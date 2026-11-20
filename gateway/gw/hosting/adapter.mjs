import fs from "node:fs";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { publish } from "../bus.mjs";
import { routeBookmarks } from "../bookmarks.mjs";
import { deleteMessage, forkSession, trashPresence, undoDelete } from "../sessionops.mjs";
import { forEachLine } from "../bigfile.mjs";
import { onPublish } from "../bus.mjs";
import { RUNS, runOfSession } from "../runs.mjs";
import { findSessionFile, listSessions } from "../sessions.mjs";
import { readSessionPage, readSessionForward, transcriptItems } from "../transcript.mjs";
import { abortRun, flushPend, permView, QUEUE, sidForItem, publishState, routeQueue, INTERRUPTED, END_HOOKS } from "../queue.mjs";
import { routeRun } from "../run.mjs";
import { PERM_MODES } from "../config.mjs";
import { SETTINGS } from "../settings-store.mjs";
import { PROVIDER, providerById, todayTokens } from "../providers.mjs";
import { effectiveModel } from "../sessmodel.mjs";
import { currentModelInfo, safeModePerm } from "../claude.mjs";
import { compactLineOf, defaultCompactLine } from "../models.mjs";
import { effectiveAutoCompact } from "../stream.mjs";
import { object, text, SID_RE, digest } from "./core.mjs";

// 适配层只使用上游已有的会话/运行接口，不把完整 HTTP 网关暴露给远端。
function safeInput(value, depth = 0, budget = { left: 24000 }) {
  if (budget.left <= 0) return "（已省略）";
  if (typeof value === "string") {
    let s = text(value, Math.min(6000, budget.left));
    if (Buffer.byteLength(JSON.stringify(s)) > budget.left) s = text(value, Math.max(0, Math.floor(budget.left / 6) - 80));
    budget.left -= Buffer.byteLength(JSON.stringify(s)); return s;
  }
  if (value == null || typeof value === "boolean" || typeof value === "number") return value;
  if (depth > 5) return "（已省略）";
  if (Array.isArray(value)) return value.slice(0, 30).map((x) => safeInput(x, depth + 1, budget));
  if (!object(value)) return null;
  const out = Object.create(null);
  for (const [k, v] of Object.entries(value).slice(0, 40)) {
    if (["__proto__", "constructor", "prototype"].includes(k)) continue;
    if (["data", "base64", "source"].includes(k)) { out[k] = "（文件/二进制内容不传输）"; continue; }
    out[k] = safeInput(v, depth + 1, budget);
  }
  return out;
}
const hostRequestId = (cid) => typeof cid === "string" && cid.length === 37 && /^host-[a-f0-9]{32}$/.test(cid) ? cid.slice(5) : "";
function requestRef(cid) {
  const requestId = hostRequestId(cid); return requestId ? { requestId } : {};
}
function cleanItem(it) {
  if (!it || !["user", "assistant", "thinking", "note", "tool_use", "tool_result"].includes(it.role)) return null;
  return { role: it.role, ...(typeof it.uuid === "string" && /^[A-Za-z0-9_-]{1,100}$/.test(it.uuid) ? { uuid: it.uuid } : {}),
    ...(typeof it.requestId === "string" && it.requestId.length === 32 && /^[a-f0-9]{32}$/.test(it.requestId) ? { requestId: it.requestId } : {}),
    ...(it.text ? { text: text(it.text, it.role === "tool_result" ? 3000 : 16000) } : {}),
    ...(it.name ? { name: text(it.name, 120), input: safeInput(it.input) } : {}), ...(it.id ? { id: text(it.id, 100) } : {}),
    ...(it.ok !== undefined ? { ok: !!it.ok } : {}) };
}
function bounded(items, max = 400000) {
  const out = []; let size = 0;
  for (let i = items.length - 1; i >= 0; i--) {
    const it = cleanItem(items[i]); if (!it) continue;
    const n = Buffer.byteLength(JSON.stringify(it));
    if (size + n > max) { out.unshift({ role: "note", text: "较早的内容已省略；请在主机查看完整记录。" }); break; }
    size += n; out.unshift(it);
  }
  return out;
}
function liveItems(r) {
  if (!r) return [];
  flushPend(r);
  const out = [{ role: "user", text: r.text || "", ...requestRef(r.cid) }];
  for (const e of (r.log || []).slice(-800)) {
    if (["delta", "assistant_text", "thinking_delta", "thinking"].includes(e.type)) {
      const role = e.type.startsWith("thinking") ? "thinking" : "assistant";
      const last = out[out.length - 1];
      if (last?.role === role && e.type.endsWith("delta")) last.text = text(last.text + (e.text || ""), 32000);
      else out.push({ role, text: e.text || "" });
    } else if (e.type === "tool_use") out.push({ role: "tool_use", name: e.name, id: e.id, input: e.input });
    else if (e.type === "tool_result") out.push({ role: "tool_result", id: e.id, ok: e.ok, text: e.text });
    else if (e.type === "inject" || e.type === "user_message") out.push({ role: "user", text: e.text || e.message || "", ...requestRef(e.cid) });
    else if (e.type === "error") out.push({ role: "note", text: e.message });
    else if (e.type === "done" && e.resultText) out.push({ role: "assistant", text: e.resultText });
  }
  return bounded(out, 300000);
}
// watch 的正文投影是每个引擎缓存项私有的；没有模块级常驻定时器。
// run.log 只在尾部追加/合并，回滚最后一条的投影再重放尾巴即可，不在每个 token 重建整轮。
const OMIT = { role: "note", text: "较早的内容已省略；请在主机查看完整记录。" };
const itemSizes = new WeakMap(), identities = new WeakMap();
let nextIdentity = 0;
function identity(value) {
  if (!value || typeof value !== "object") return 0;
  let id = identities.get(value);
  if (!id) { id = ++nextIdentity; identities.set(value, id); }
  return id;
}
function itemSize(it) {
  let n = itemSizes.get(it);
  if (n === undefined) { n = Buffer.byteLength(JSON.stringify(it)); itemSizes.set(it, n); }
  return n;
}
function fitLive(items) {
  let size = 0, from = items.length;
  while (from > 0 && size + itemSize(items[from - 1]) <= 300000) size += itemSize(items[--from]);
  return from ? [OMIT, ...items.slice(from)] : items;
}
function projectEvent(items, e) {
  let it;
  if (["delta", "assistant_text", "thinking_delta", "thinking"].includes(e.type)) {
    const role = e.type.startsWith("thinking") ? "thinking" : "assistant", last = items.at(-1);
    if (last?.role === role && e.type.endsWith("delta"))
      return fitLive([...items.slice(0, -1), cleanItem({ role, text: (last.text || "") + (e.text || "") })]);
    it = { role, text: e.text || "" };
  } else if (e.type === "tool_use") it = { role: "tool_use", name: e.name, id: e.id, input: e.input };
  else if (e.type === "tool_result") it = { role: "tool_result", id: e.id, ok: e.ok, text: e.text };
  else if (e.type === "inject" || e.type === "user_message") it = { role: "user", text: e.text || e.message || "", ...requestRef(e.cid) };
  else if (e.type === "error") it = { role: "note", text: e.message };
  else if (e.type === "done" && e.resultText) it = { role: "assistant", text: e.resultText };
  return it ? fitLive([...items, cleanItem(it)]) : items;
}
const REPLACED_META = new Set(["phase", "usage", "run_tokens"]);
const sameLogItem = (id, type, ev) => id === identity(ev) || (type === ev?.type && REPLACED_META.has(type));
function cachedLive(r, cache) {
  if (!r) { cache.live = null; return []; }
  flushPend(r);
  const log = r.log || [], start = Math.max(0, log.length - 800), old = cache.live, requestId = hostRequestId(r.cid);
  let items, at;
  let beforeLast;
  const compatible = old?.run === identity(r) && old.requestId === requestId && old.log === identity(log) && old.n <= log.length &&
    (!old.n || sameLogItem(old.first, old.firstType, log[old.start])) &&
    (!old.n || sameLogItem(old.last, old.lastType, log[old.n - 1]));
  const edited = compatible && old.n > old.start && ["delta", "thinking_delta"].includes(log[old.n - 1].type) &&
    log[old.n - 1].text?.length !== old.lastLength;
  // 阶段本身不挤掉已有正文；下一次正文更新再按800条窗口截断，避免阶段变更引起整段重放。
  const phaseTail = compatible && old.start !== start && !edited && log.length - old.n <= 800 &&
    log.slice(old.n).every(ev => ev.type === "phase");
  if (compatible && (old.start === start || phaseTail)) {
    items = edited ? old.beforeLast : old.items;
    at = edited ? old.n - 1 : old.n;
    beforeLast = old.beforeLast;
  } else { items = [cleanItem({ role: "user", text: r.text || "", ...requestRef(r.cid) })]; at = start; beforeLast = items; }
  for (; at < log.length; at++) { beforeLast = items; items = projectEvent(items, log[at]); }
  // 不可变条目/数组：引擎可复用未变条目的指纹，不必序列化旧正文。
  // 只存弱身份和长度，不把上游可能很大的 run/log/input 在轮次结束后继续强引用。
  const keptStart = phaseTail ? old.start : start;
  cache.live = { run: identity(r), requestId, log: identity(log), start: keptStart, n: log.length,
    first: identity(log[keptStart]), firstType: log[keptStart]?.type, last: identity(log.at(-1)), lastType: log.at(-1)?.type,
    lastLength: log.at(-1)?.text?.length, beforeLast, items };
  return items;
}
function queueView(sid) {
  return QUEUE.filter((q) => sidForItem(q) === sid).map((q) => ({ id: q.id, text: text(q.message, 400), held: !!q.held, ...requestRef(q.cid) }));
}
const number = (v) => typeof v === "number" && Number.isFinite(v) && v >= 0 ? v : null;
const PHASES = new Set(["prep", "boot", "connect", "wait", "retry", "think", "tool", "text"]);
const PHASE_EVENTS = new Set(["phase", "run_start", "run_busy", "run_idle", "run_end", "init", "done", "error",
  "delta", "assistant_text", "thinking_delta", "thinking", "tool_use", "tool_result", "inject", "user_message"]);
const runPhases = new WeakMap();
const phaseCount = (v, max) => Math.min(max, Math.floor(number(v) ?? 0));
const phaseId = (v) => typeof v === "string" && /^[A-Za-z0-9_-]{1,100}$/.test(v) ? v : "";
function phaseView(ev) {
  if (!PHASES.has(ev.phase)) return null;
  const out = { phase: ev.phase };
  if (ev.phase === "retry") Object.assign(out, { attempt: phaseCount(ev.attempt, 1000), max: phaseCount(ev.max, 1000),
    delayMs: phaseCount(ev.delayMs, 3600000), status: Number.isInteger(ev.status) && ev.status >= 100 && ev.status <= 599 ? ev.status : null });
  if (ev.phase === "tool") {
    if (typeof ev.name === "string" && /^[A-Za-z][A-Za-z0-9_.-]{0,119}$/.test(ev.name)) out.name = ev.name;
    const id = phaseId(ev.id); if (id) out.id = id;
    out.chars = phaseCount(ev.chars, 1000000000);
  }
  return out;
}
function setRunPhase(s, value) {
  if (JSON.stringify(s.phase) !== JSON.stringify(value)) s.phase = value;
}
function phaseSegment(s, active, phase) {
  s.active = active; s.startup = phase === "prep"; s.pending.clear(); s.tools.clear(); s.overflow = false;
  setRunPhase(s, phase ? { phase } : null);
}
function applyPhaseEvent(s, ev) {
  if (ev.parent || ev.parent_tool_use_id || ev.isSidechain) return;
  if (ev.type === "run_start") return phaseSegment(s, true, "prep");
  if (["run_busy", "inject", "user_message"].includes(ev.type)) return phaseSegment(s, true, "connect");
  if (["run_idle", "run_end", "done", "error"].includes(ev.type)) return phaseSegment(s, false, null);
  if (!s.active) return;
  if (ev.type === "init" || (ev.type === "phase" && ev.phase === "connect" && ev.from === "init")) {
    if (s.startup) { s.startup = false; setRunPhase(s, { phase: "connect" }); }
  } else if (ev.type === "phase") {
    const phase = phaseView(ev); if (!phase) return;
    if ((phase.phase === "prep" || phase.phase === "boot") && !s.startup) return;
    if (phase.phase === "tool" && phase.id && s.tools.has(phase.id)) return;
    if (phase.phase !== "prep" && phase.phase !== "boot") s.startup = false;
    setRunPhase(s, phase);
  } else if (["delta", "assistant_text", "thinking_delta", "thinking"].includes(ev.type) && ev.text) {
    s.startup = false;
    setRunPhase(s, { phase: ev.type.startsWith("thinking") ? "think" : "text" });
  } else if (ev.type === "tool_use") {
    s.startup = false; setRunPhase(s, null);
    const id = phaseId(ev.id);
    if (id) {
      if (!s.pending.has(id)) { if (s.pending.size < 128) s.pending.add(id); else s.overflow = true; }
      s.tools.add(id); if (s.tools.size > 128) s.tools.delete(s.tools.values().next().value);
    }
  } else if (ev.type === "tool_result" && s.pending.delete(phaseId(ev.id)) && !s.pending.size && !s.overflow)
    setRunPhase(s, { phase: "connect" });
}
function runPhaseState(r) {
  let s = runPhases.get(r);
  if (s) return s;
  s = { phase: null, active: true, startup: true, pending: new Set(), tools: new Set(), overflow: false };
  runPhases.set(r, s);
  // 只在首次接管时恢复日志；后续总线事件 O(1) 更新，仅留安全标量和至多128个工具编号。
  for (const ev of r.log || []) if (PHASE_EVENTS.has(ev.type)) applyPhaseEvent(s, ev);
  if (r.idle || r.ended) phaseSegment(s, false, null);
  return s;
}
const runPhase = (r) => r ? (r.idle || r.ended ? null : runPhaseState(r).phase) : null;
onPublish((ev) => {
  if (!PHASE_EVENTS.has(ev.type)) return;
  const r = RUNS.get(ev.runId);
  if (r) applyPhaseEvent(runPhaseState(r), ev);
});
const META_EVENTS = new Set(["init", "usage", "compacted", "run_tokens", "run_idle", "run_busy"]);
const runStats = new WeakMap(), completedStats = new Map(), legacyInfo = new Map();
function applyInfoEvent(s, r, ev, replay = false) {
  if (ev.type === "init" && ev.model) s.model = text(ev.model, 200);
  if (ev.type === "usage" && number(ev.contextTokens) > 0) { s.used = ev.contextTokens; s.usedKnown = true; }
  if (ev.type === "compacted") { s.used = number(ev.post); s.usedKnown = true; }
  if (ev.type === "run_tokens") s.cumulative = number(ev.out);
  if (ev.type === "run_busy") {
    s.startedAt = number(ev.startedAt) ?? (replay ? null : Date.now());
    s.active = true; s.elapsedMs = null; s.costUsd = null; s.frozenOut = null; s.split = true;
    // 重放日志的 run_tokens 会原地覆写，不能用它猜历史段的输出基数。
    s.baseOut = replay ? null : number(r.outTok) ?? s.cumulative;
  }
  if (ev.type === "run_idle" && s.active) {
    const at = number(ev.at) ?? (replay ? null : Date.now());
    s.active = false;
    s.elapsedMs = at !== null && s.startedAt !== null ? Math.max(0, at - s.startedAt) : null;
    if (!replay && number(r.outTok) !== null) s.cumulative = r.outTok;
    s.frozenOut = replay ? null : segmentOut(s);
  }
}
function segmentOut(s) {
  return s.cumulative !== null && s.baseOut !== null && s.cumulative >= s.baseOut ? s.cumulative - s.baseOut : null;
}
function runInfoState(r) {
  let s = runStats.get(r);
  if (s) return s;
  s = { startedAt: number(r.startedAt), active: true, elapsedMs: null, baseOut: 0, cumulative: null,
    costUsd: null, used: null, usedKnown: false, frozenOut: null, split: false, model: text(r.usageModel, 200) };
  runStats.set(r, s);
  for (const ev of r.log || []) if (META_EVENTS.has(ev.type)) applyInfoEvent(s, r, ev, true);
  if (number(r.outTok) !== null) s.cumulative = r.outTok;
  if (r.idle && s.active) { s.active = false; s.elapsedMs = null; }
  return s;
}
function statValue(s) {
  return { active: s.active, startedAt: s.startedAt, elapsedMs: s.elapsedMs,
    out: s.active ? segmentOut(s) : s.frozenOut,
    costUsd: s.costUsd };
}
// 弱引用只跟随活跃 run；结束钩子保留最多128个纯标量结果，没有正文、目录、配置或常驻定时器。
END_HOOKS.push((r, fields) => {
  if (!r.sessionId) return;
  const s = runInfoState(r), now = Date.now(), wasActive = s.active;
  const out = number(fields.usage?.output_tokens) ?? number(r.outTok);
  if (out !== null) s.cumulative = out;
  if (s.active) {
    s.elapsedMs = !s.split && number(fields.durationMs) !== null ? fields.durationMs :
      s.startedAt !== null ? Math.max(0, now - s.startedAt) : null;
    s.frozenOut = segmentOut(s); s.active = false;
  }
  const providerId = r.exec?.provider || r.provider, p = providerId ? providerById(providerId) : null;
  s.costUsd = p?.nativeModels && !s.split && wasActive ? number(fields.costUsd) : null;
  completedStats.delete(r.sessionId);
  completedStats.set(r.sessionId, statValue(s));
  if (completedStats.size > 128) completedStats.delete(completedStats.keys().next().value);
});
onPublish((ev) => {
  if (!META_EVENTS.has(ev.type)) return;
  const r = RUNS.get(ev.runId);
  if (!r) return;
  const old = runStats.get(r), s = runInfoState(r);
  // 首次观察也可能恰好是 idle：重放只能用日志，实时事件还能补上当前真实输出数。
  if (!old && ev.type === "run_idle" && number(ev.at) !== null) s.active = true;
  applyInfoEvent(s, r, ev);
});
function infoCache(sid) {
  let cache = legacyInfo.get(sid);
  if (!cache) {
    cache = {}; legacyInfo.set(sid, cache);
    if (legacyInfo.size > 32) legacyInfo.delete(legacyInfo.keys().next().value);
  }
  return cache;
}
function cachedInfo(sid, r, cache) {
  const now = Date.now(), s = r ? runInfoState(r) : null;
  if (s && number(r.outTok) !== null) s.cumulative = r.outTok;
  const key = JSON.stringify([r?.id, r?.provider, r?.exec?.provider, r?.exec?.model, r?.exec?.effort, s?.model,
    PROVIDER.id, SETTINGS.model, SETTINGS.effort, SETTINGS.autoCompactWindow]);
  let selected = cache.selected;
  if (!selected || selected.key !== key || now - selected.at >= 10000) {
    // effectiveModel / effectiveAutoCompact 可能查盘，仅在配置边界或10秒缓存到期取一次。
    const e = r ? null : effectiveModel(sid);
    const providerId = r ? r.exec?.provider || r.provider : e.provider;
    const p = providerId ? providerById(providerId) : null;
    const name = s?.model || (r ? (r.exec ? r.exec.model || p?.defaultModel : "") : e.model || p?.defaultModel) || "";
    const mi = p && name ? currentModelInfo(name, p) : {};
    const total = number(mi.context_window) > 0 ? mi.context_window : null;
    const compactLine = p && (r?.exec || !r) ? compactLineOf(effectiveAutoCompact(), total, p) ?? defaultCompactLine(total, p) : null;
    selected = cache.selected = { key, at: now, provider: p?.id || "", total, compactLine,
      model: { providerLabel: text(p?.label, 120), ...(p?.labelEn ? { providerLabelEn: text(p.labelEn, 120) } : {}),
        name: text(name, 200), effort: text(r ? r.exec?.effort : e.effortRun, 20) } };
  }
  const mode = r ? r.permMode : safeModePerm(SETTINGS.permissionMode);
  const stat = s ? statValue(s) : completedStats.get(sid) ||
    { active: false, startedAt: null, elapsedMs: null, out: null, costUsd: null };
  const value = { context: { used: s?.usedKnown ? s.used : number(cache.contextTokens), total: selected.total,
      compactLine: number(selected.compactLine) }, model: selected.model,
    permissionMode: PERM_MODES.includes(mode) ? mode : null,
    stat, quota: { todayTokens: todayTokens(selected.provider, now) } };
  const signature = JSON.stringify(value);
  if (cache.info?.signature === signature) return cache.info.value;
  // 不因心跳重采时间。仅内容改变时锚定耗时；engine交付时重锚，由控制端按接收后的时间续算。
  if (stat.active) value.stat = { ...stat, elapsedMs: stat.startedAt !== null ? Math.max(0, now - stat.startedAt) : null };
  cache.info = { signature, value: { ...value, sampledAt: now } };
  return cache.info.value;
}
function cachedMeta(sid, r, cache) {
  const queue = queueView(sid), interrupted = INTERRUPTED.some((q) => q.sessionId === sid), phase = runPhase(r);
  const refs = r?.perms ? [...r.perms.values()].slice(0, 12) : [], ids = refs.map(identity), old = cache.meta;
  const key = JSON.stringify([r?.id, hostRequestId(r?.cid), !!r?.idle, r?.startedAt, r?.permMode, r?.bgList || [], phase, queue, interrupted]);
  if (old && old.key === key && ids.length === old.ids.length && ids.every((v, i) => v === old.ids[i])) return old.value;
  const perms = refs.map((v) => {
    const p = permView(v), input = safeInput(p.input);
    return { id: p.id, runId: r.id, tool: text(p.tool, 120), input, reason: text(p.reason || p.description, 2000),
      cut: !!p.inputCut || JSON.stringify(p.input) !== JSON.stringify(input), at: p.at };
  });
  const value = { run: r ? { id: r.id, ...requestRef(r.cid), idle: !!r.idle, startedAt: r.startedAt, permissionMode: r.permMode,
    tasks: safeInput(r.bgList || []), phase } : null, perms, queue, interrupted };
  cache.meta = { key, ids, value }; return value;
}
function stateKey(sid) {
  const r = runOfSession(sid);
  return JSON.stringify([r?.id, hostRequestId(r?.cid), !!r?.idle, r?.permMode, r?.bgList || [], [...(r?.perms?.keys() || [])],
    [...(r?.perms?.values() || [])].slice(0, 12).map(identity), queueView(sid), INTERRUPTED.some((q) => q.sessionId === sid),
    safeModePerm(SETTINGS.permissionMode), PROVIDER.id, SETTINGS.model, SETTINGS.effort, SETTINGS.autoCompactWindow]);
}
const WATCH_EVENTS = new Set(["delta", "assistant_text", "thinking_delta", "thinking", "tool_use", "tool_result",
  "inject", "user_message", "error", "done", "run_start", "run_end", "init", "session_created", "run_idle", "run_busy", "phase",
  "perm_request", "perm_resolved", "usage", "compacted", "run_tokens"]);
const HISTORY_SCAN = 10 << 20, HISTORY_CHUNK = 64 << 10;
const historyPick = (ev) => transcriptItems(ev).map(it => cleanItem(ev.type === "assistant" ? { ...it, uuid: ev.uuid } : it)).filter(Boolean);
const historyStamp = (found, st, runKey, content = "") => JSON.stringify([found?.file, st?.dev?.toString(), st?.ino?.toString(), st?.size?.toString(),
  st?.mtimeNs?.toString(), st?.ctimeNs?.toString(), runKey, content]);
const historyChanged = () => new Error("主机记录已变化，请重新同步后再操作消息");
function checkHistoryStamp(found, expected, actual) {
  if (historyStamp(found, expected, "") !== historyStamp(found, actual, "")) throw historyChanged();
}
async function historyFingerprint(found, st) {
  if (!found || st.size > BigInt(HISTORY_SCAN)) return "";
  const fh = await fs.promises.open(found.file, "r");
  try {
    checkHistoryStamp(found, st, await fh.stat({ bigint: true }));
    const hash = createHash("sha256"), buffer = Buffer.allocUnsafe(HISTORY_CHUNK), size = Number(st.size);
    for (let at = 0; at < size;) {
      const { bytesRead } = await fh.read(buffer, 0, Math.min(buffer.length, size - at), at);
      if (!bytesRead) throw historyChanged();
      hash.update(buffer.subarray(0, bytesRead)); at += bytesRead;
    }
    checkHistoryStamp(found, st, await fh.stat({ bigint: true }));
    checkHistoryStamp(found, st, await fs.promises.stat(found.file, { bigint: true }));
    return hash.digest("hex");
  } finally { await fh.close(); }
}
function historyFingerprintSync(found, st) {
  if (!found || st.size > BigInt(HISTORY_SCAN)) return "";
  const fd = fs.openSync(found.file, "r");
  try {
    checkHistoryStamp(found, st, fs.fstatSync(fd, { bigint: true }));
    const hash = createHash("sha256"), buffer = Buffer.allocUnsafe(HISTORY_CHUNK), size = Number(st.size);
    for (let at = 0; at < size;) {
      const n = fs.readSync(fd, buffer, 0, Math.min(buffer.length, size - at), at);
      if (!n) throw historyChanged();
      hash.update(buffer.subarray(0, n)); at += n;
    }
    checkHistoryStamp(found, st, fs.fstatSync(fd, { bigint: true }));
    checkHistoryStamp(found, st, fs.statSync(found.file, { bigint: true }));
    return hash.digest("hex");
  } finally { fs.closeSync(fd); }
}
function historyArgs(args) {
  for (const key of ["before", "after", "until", "exclude"]) {
    if (Object.hasOwn(args, key) && (!Number.isSafeInteger(args[key]) || args[key] < 0)) throw new Error("无效的历史范围参数");
  }
  const before = Object.hasOwn(args, "before"), after = Object.hasOwn(args, "after");
  if ((before && after) || (Object.hasOwn(args, "until") && !after) ||
      (after && Object.hasOwn(args, "until") && args.after > args.until)) throw new Error("无效的历史范围参数");
  return { before, after, page: before || after };
}
async function watchSession(sid, cache, { verifyHistory = false } = {}) {
  const r = runOfSession(sid), runKey = r ? r.id + ":" + r.startedAt : "";
  if (!cache.history || cache.runKey !== runKey || (verifyHistory && !r)) {
    const found = checkedSession(sid);
    // 进入新轮时固定已完成历史的读边界，生成中的追加行完全由 live 接管；不重扫被 exclude 的整轮。
    const st = found ? await fs.promises.stat(found.file, { bigint: true }) : null;
    // 文件戳在部分设备上只有粗分辨率；空闲且可远程修改的记录必须核对内容，不按 token 查盘。
    const content = !r ? await historyFingerprint(found, st) : "";
    const stamp = historyStamp(found, st, runKey, content);
    if (!cache.history || cache.stamp !== stamp) {
      const exclude = r?.startedAt || 0;
      const p = found ? await readSessionPage(found.file, { limit: 30, excludeFrom: exclude,
        pick: historyPick, maxScan: HISTORY_SCAN }) : { items: [], cursor: 0, size: 0 };
      cache.history = { items: bounded(p.items), cursor: p.cursor,
        range: { from: p.cursor, to: p.size, size: p.size, exclude } }; cache.stamp = stamp;
      cache.contextTokens = p.contextTokens;
    }
    cache.historyToken = digest(stamp);
    cache.runKey = runKey;
    // 异步读盘期间轮次换了：本次快照必须来自同一个轮次，下一次重新接历史/实时边界。
    const now = runOfSession(sid);
    if (now !== r || (now && now.startedAt !== r.startedAt)) { cache.history = null; throw new Error("会话轮次正在切换，请重试同步"); }
  }
  if (!cache.marks || verifyHistory) cache.marks = await sessionMarks(sid);
  return { id: sid, ...cache.history, live: cachedLive(r, cache), ...cachedMeta(sid, r, cache), info: cachedInfo(sid, r, cache), marks: cache.marks,
    ...(cache.historyToken ? { historyToken: cache.historyToken } : {}) };
}

async function invoke(route, p, body, method = "POST", check) {
  const req = Readable.from([Buffer.from(JSON.stringify(body || {}))]);
  req.method = method; req.headers = {};
  if (check) req.hostingCheck = check;
  let code = 0, reply;
  const res = { writeHead(c) { code = c; }, end(b) { reply = JSON.parse(String(b)); } };
  const url = new URL(p, "http://127.0.0.1");
  await route(req, res, url, url.pathname);
  if (!code || code >= 400) throw new Error(text(reply?.message || reply?.error, 300) || "主机会话接口拒绝操作");
  return reply;
}
function checkedSession(sid) {
  if (!SID_RE.test(sid || "")) throw new Error("无效的会话号");
  const found = findSessionFile(sid);
  if (!found && !runOfSession(sid)) throw new Error("主机会话暂不可用");
  return found;
}
function availableSession(sid) {
  try {
    const found = checkedSession(sid);
    if (found) {
      // An existence check alone also reports unopenable or non-file transcripts as present.
      if (!fs.statSync(found.file).isFile()) return false;
      fs.accessSync(found.file, fs.constants.R_OK);
    }
    return true;
  } catch { return false; }
}
async function sessionMarks(sid) {
  const out = await invoke(routeBookmarks, "/api/bookmarks?session=" + encodeURIComponent(sid), null, "GET");
  return (out.items || []).slice(0, 50).map(({ id, uuid, para, role }) => ({ id, uuid, para, role }));
}
function messageTarget(sid, uuid, role) {
  if (typeof uuid !== "string" || !/^[A-Za-z0-9_-]{1,100}$/.test(uuid)) throw new Error("消息尚未完整存档，请重新同步后再操作");
  const found = checkedSession(sid); let matched = false;
  if (found) forEachLine(found.file, line => {
    if (!line.includes(uuid)) return;
    let ev; try { ev = JSON.parse(line); } catch { return; }
    if (ev.uuid !== uuid || ev.isSidechain || ev.isMeta || ev.isCompactSummary) return;
    if ((role === "assistant" && ev.type === "assistant" && !ev.isApiErrorMessage) || transcriptItems(ev).some(it => it.role === role)) { matched = true; return false; }
  });
  if (!matched) throw new Error("消息与主机记录不匹配，请重新同步");
}
function messageResult(result) {
  if (!result?.ok) throw new Error(text(result?.message, 300) || "主机拒绝了消息操作");
  return result;
}

function trashState(id) {
  const state = trashPresence(id);
  return state === "present" ? "deleted" : state === "missing" ? "clear" : "unavailable";
}

export const adapter = {
  infoVersion: 1,
  historyVersion: 1,
  actionVersion: 1,
  localList() { return listSessions(250).filter(({ id }) => trashState(id) === "clear")
    .map(({ id, title, mtime }) => ({ id, title: text(title, 100), mtime, running: !!runOfSession(id) })); },
  trashState,
  exists: availableSession,
  watchSession,
  subscribe(sid, changed) {
    let state = stateKey(sid);
    return onPublish((ev) => {
      if (ev.type === "state" || ev.type === "perm_mode") {
        const key = stateKey(sid);
        if (key !== state) { state = key; changed(); }
        return;
      }
      if (ev.type === "session_gone" && ev.id === sid) { changed(); return; }
      if (ev.type === "hosting_message" && ev.sessionId === sid) { changed({ history: ["delete", "undo"].includes(ev.action), marks: true }); return; }
      if (!WATCH_EVENTS.has(ev.type)) return;
      const eventSid = ev.sessionId || RUNS.get(ev.runId)?.sessionId;
      if (eventSid === sid || (ev.runId && runOfSession(sid)?.id === ev.runId)) changed();
    });
  },
  summary(sid) {
    if (!availableSession(sid)) return null;
    const r = runOfSession(sid);
    return { id: sid, running: !!r, waiting: r?.perms?.size || 0 };
  },
  async session(sid, args = {}) {
    const bounds = historyArgs(args), found = checkedSession(sid), r = runOfSession(sid);
    const exclude = args.exclude ?? r?.startedAt ?? 0;
    // 只读目标会话的字节范围；范围不在文件里时要求重接，不夹回最新页冒充旧位置。
    const options = { limit: 30, excludeFrom: exclude, pick: historyPick, maxScan: HISTORY_SCAN, strictBounds: true };
    let p;
    if (found) p = bounds.after
      ? await readSessionForward(found.file, { ...options, after: args.after, end: args.until ?? null })
      : await readSessionPage(found.file, { ...options, before: bounds.before ? args.before : null });
    else {
      if ((args.before || args.after || args.until)) throw new Error("历史范围已失效，请回到最新重新同步");
      p = { items: [], cursor: 0, from: 0, next: 0, size: 0 };
    }
    const range = bounds.after ? { from: p.from, to: p.next, size: p.size, exclude }
      : { from: p.cursor, to: bounds.before ? args.before : p.size, size: p.size, exclude };
    if (bounds.page) return { id: sid, cursor: range.from, items: bounded(p.items), live: [], run: null, perms: [], queue: [], interrupted: false, range };
    const perms = r?.perms ? [...r.perms.values()].slice(0, 12).map((v) => {
      const p = permView(v), input = safeInput(p.input), raw = JSON.stringify(p.input), shown = JSON.stringify(input);
      return { id: p.id, runId: r.id, tool: text(p.tool, 120), input, reason: text(p.reason || p.description, 2000),
        cut: !!p.inputCut || raw !== shown, at: p.at };
    }) : [];
    const cache = infoCache(sid); cache.contextTokens = p.contextTokens;
    const info = cachedInfo(sid, r, cache);
    return { id: sid, cursor: p.cursor, items: bounded(p.items), live: liveItems(r),
      run: r ? { id: r.id, ...requestRef(r.cid), idle: !!r.idle, startedAt: r.startedAt, permissionMode: r.permMode,
        tasks: safeInput(r.bgList || []), phase: runPhase(r) } : null, perms,
      queue: queueView(sid),
      interrupted: INTERRUPTED.some((q) => q.sessionId === sid), info, range, marks: await sessionMarks(sid) };
  },
  async message(sid, args, requestId, context = {}) {
    let historyToken = context.historyToken;
    const check = () => {
      context.check?.();
      if (runOfSession(sid)) throw new Error("主机正在运行，等这一轮结束再操作消息");
      const found = checkedSession(sid), st = found ? fs.statSync(found.file, { bigint: true }) : null;
      if (st?.size > BigInt(HISTORY_SCAN)) throw new Error("记录超过远程消息操作的10MiB扫描预算，请在主机操作");
      // 原写入/书签提交钩子是同步合同：分块重算后立即核对，不能在副作用前另开 await 空隙。
      const token = digest(historyStamp(found, st, "", historyFingerprintSync(found, st)));
      if (historyToken && token !== historyToken) throw historyChanged();
      historyToken = token;
      context.check?.();
    };
    check();
    checkedSession(sid);
    if (!["delete", "undo", "fork", "rewind", "mark"].includes(args.action)) throw new Error("无效的消息操作");
    if (runOfSession(sid)) throw new Error("主机正在运行，等这一轮结束再操作消息");
    let result;
    if (args.action === "undo") {
      if (typeof args.undo !== "string" || args.undo.length > 300 || !args.undo.startsWith(sid + ".")) throw new Error("无效的撤销凭据");
      result = messageResult(undoDelete(sid, args.undo));
    } else if (args.action === "mark") {
      if (!["user", "assistant"].includes(args.role) || typeof args.enabled !== "boolean" || !Number.isSafeInteger(args.para) || args.para < 0 || args.para > 10000)
        throw new Error("无效的书签参数");
      messageTarget(sid, args.uuid, args.role);
      const commit = () => {
        check();
        if (runOfSession(sid)) throw new Error("主机正在运行，等这一轮结束再操作消息");
        messageTarget(sid, args.uuid, args.role);
      };
      if (args.enabled) result = await invoke(routeBookmarks, "/api/bookmarks", { session: sid, uuid: args.uuid, role: args.role,
        para: args.para, excerpt: text(args.excerpt, 120) }, "POST", commit);
      else {
        const marks = await sessionMarks(sid), ids = marks.filter(b => b.uuid === args.uuid).map(b => b.id);
        commit(); result = ids.length ? await invoke(routeBookmarks, "/api/bookmarks/del", { ids }, "POST", commit) : { ok: true, removed: 0 };
      }
      result = { ok: true, marks: await sessionMarks(sid) };
    } else {
      messageTarget(sid, args.uuid, "user");
      if (args.action === "delete") {
        if (!["user", "bot"].includes(args.which)) throw new Error("无效的消息角色");
        if (args.which === "bot") messageTarget(sid, args.replyUuid, "assistant");
        const out = messageResult(deleteMessage(sid, "", 0, args.which, args.uuid, args.which === "bot" ? args.replyUuid : ""));
        result = { ok: true, removed: out.removed, undo: out.undo };
      } else {
        const out = messageResult(forkSession(sid, "", 0, { uuid: args.uuid, before: args.action === "rewind" }));
        result = { ok: true, id: out.id, title: out.title, kept: out.kept, hostOnly: true };
      }
    }
    publish({ type: "hosting_message", sessionId: sid, action: args.action });
    return result;
  },
  async send(sid, args, requestId, { check } = {}) {
    check?.();
    checkedSession(sid);
    if (typeof args.message !== "string" || !args.message.trim() || args.message.length > 24000) throw new Error("消息需为1至24000字文字");
    // 上游部分斜杠命令直接写全局设置；远端仅开放自然语言，不借会话入口改其他会话的配置。
    if (args.message.trimStart().startsWith("/")) throw new Error("远程托管暂不支持斜杠命令，请用普通文字；配置命令请在主机操作");
    // 只传明确白名单字段：不接受远程覆盖模型、权限模式、目录或工具限制。
    return invoke(routeQueue, "/api/chat", { sessionId: sid, message: args.message, mode: args.inject ? "inject" : undefined,
      cid: "host-" + requestId, idem: "host-" + requestId }, "POST", check);
  },
  async stop(sid, args, requestId, { check } = {}) {
    check?.();
    checkedSession(sid);
    const r = runOfSession(sid);
    if (r && r.id !== args.runId) throw new Error("运行轮次已变化，请刷新后再停止");
    let held = 0;
    for (const q of QUEUE) if (sidForItem(q) === sid) { q.held = true; held++; }
    const killed = r ? abortRun(r, { userStopped: true }) : false;
    publishState();
    return { ok: true, killed, held };
  },
  async answer(sid, args, requestId, { check } = {}) {
    check?.();
    checkedSession(sid);
    const r = RUNS.get(args.runId);
    if (!r || r.sessionId !== sid || !r.perms?.has(args.id)) throw new Error("该授权已过期或已在另一台设备回答");
    if (!["allow", "deny"].includes(args.behavior)) throw new Error("无效的授权回答");
    const pending = permView(r.perms.get(args.id));
    if (args.behavior === "allow" && (pending.inputCut || JSON.stringify(safeInput(pending.input)) !== JSON.stringify(pending.input)))
      throw new Error("该请求内容过长或包含文件数据，远端未显示完整，请在主机确认");
    const answers = Object.create(null);
    if (object(args.answers)) for (const [k, v] of Object.entries(args.answers).slice(0, 8)) {
      if (k.length > 2000 || typeof v !== "string" || v.length > 4000) throw new Error("问题回答过长");
      answers[k] = v;
    }
    return invoke(routeRun, "/api/perm/answer", { runId: r.id, id: args.id, behavior: args.behavior,
      message: text(args.message, 2000), interrupt: args.behavior === "deny" && !!args.interrupt,
      answers }, "POST", check); // 不开放“始终允许”或全局切模式。
  },
};
