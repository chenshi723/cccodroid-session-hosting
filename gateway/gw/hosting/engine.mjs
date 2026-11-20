import path from "node:path";
import { ROOT, ID_RE, SID_RE, TOKEN_RE, uid, token, digest, object, text, serverUrl, encodePair, decodePair, seal, unseal, JsonStore, request, remoteCall } from "./core.mjs";

const MUTATIONS = new Set(["send", "stop", "answer", "message"]);
const OPS = new Set(["list", "session", "watch", ...MUTATIONS]);
const REMOVAL = new Set(["pending", "failed", "confirmed"]);
const shareRevoked = () => new Error("会话已取消托管");
const age = () => Date.now();
// 长轮询而非 SSE；每个信封仍然只有一个加密回执。为发送/停止/授权留下中转槽位。
export const SYNC_LIMITS = Object.freeze({ wait: 8000, coalesce: 80, waits: 8, perSession: 2,
  entries: 16, revisions: 8, ttl: 60000, bytes: 8 << 20, snapshotBytes: 1500000 });
const encoded = new WeakMap();
function fingerprint(value) {
  if (!value || typeof value !== "object") { const s = JSON.stringify(value) ?? "undefined"; return { hash: s, bytes: Buffer.byteLength(s) }; }
  let hit = encoded.get(value);
  if (!hit) { const s = JSON.stringify(value); hit = { hash: digest(s), bytes: Buffer.byteLength(s) }; encoded.set(value, hit); }
  return hit;
}
const same = (a, b) => a === b || fingerprint(a).hash === fingerprint(b).hash;
function snapshotInfo(s) {
  const fields = [s.id, s.cursor, s.run, s.perms, s.queue, s.interrupted, s.info, s.range, s.marks, s.historyToken], hashes = [], parts = [];
  for (const v of fields) { const p = fingerprint(v); hashes.push(p.hash); parts.push(p.bytes); }
  for (const items of [s.items, s.live]) {
    hashes.push("[");
    for (const item of items) { const p = fingerprint(item); hashes.push(p.hash); parts.push(p.bytes + 1); }
    hashes.push("]");
  }
  return { hash: digest(JSON.stringify(hashes)), bytes: parts.reduce((a, b) => a + b, 120) };
}
function tailPatch(prev, next) {
  let at = 0;
  while (at < prev.length && at < next.length && same(prev[at], next[at])) at++;
  if (at === prev.length && at === next.length) return undefined;
  // 不补丁覆盖前部历史/滚动截断/不兼容改写；前端必须用 reset 对齐分页边界。
  if (at < prev.length - 1) return null;
  const a = prev[at], b = next[at];
  if (at === prev.length - 1 && b && typeof a?.text === "string" && typeof b.text === "string" &&
      b.text.startsWith(a.text) && JSON.stringify({ ...a, text: "" }) === JSON.stringify({ ...b, text: "" }))
    return { text: { index: at, from: a.text.length, append: b.text.slice(a.text.length) },
      ...(next.length > at + 1 ? { append: next.slice(at + 1) } : {}) };
  return { from: at, remove: prev.length - at, items: next.slice(at) };
}
function sessionDelta(prev, next) {
  if (prev.id !== next.id || prev.cursor !== next.cursor || prev.run?.id !== next.run?.id) return null;
  const delta = {};
  for (const field of ["items", "live"]) {
    const patch = tailPatch(prev[field], next[field]);
    if (patch === null) return null;
    if (patch) delta[field] = patch;
  }
  for (const field of ["run", "perms", "queue", "interrupted", "info", "range", "marks", "historyToken"]) if (!same(prev[field], next[field])) delta[field] = next[field] ?? null;
  return delta;
}
// 重新打开时不能把长时间无输出的旧耗时当成现在。只投影实际交付的回执，不改变缓存指纹、修订或 sampledAt。
function deliveryInfo(info) {
  if (!info?.stat?.active || !Number.isFinite(info.stat.elapsedMs) || !Number.isFinite(info.sampledAt)) return info;
  const now = age();
  return { ...info, stat: { ...info.stat, elapsedMs: info.stat.elapsedMs + Math.max(0, now - info.sampledAt) } };
}
function deliverySnapshot(snapshot) {
  return snapshot.info !== undefined ? { ...snapshot, info: deliveryInfo(snapshot.info) } : snapshot;
}
function abortError() { return new Error("同步等待已取消，请重新连接"); }
function untilAbort(promise, signal) {
  if (signal.aborted) {
    Promise.resolve(promise).catch(() => {}); // 已开始的异步读取晚到失败也必须有人收尾。
    return Promise.reject(signal.reason || abortError());
  }
  return new Promise((resolve, reject) => {
    const abort = () => { signal.removeEventListener("abort", abort); reject(signal.reason || abortError()); };
    signal.addEventListener("abort", abort, { once: true });
    Promise.resolve(promise).then((value) => { signal.removeEventListener("abort", abort); resolve(value); },
      (error) => { signal.removeEventListener("abort", abort); reject(error); });
  });
}

export class HostingEngine {
  constructor({ home, adapter, test = false }) {
    this.adapter = adapter; this.test = test; this.boot = uid(); this.controller = null; this.epoch = 0;
    this.lastPoll = 0; this.error = ""; this.outbound = new Map(); this.configBusy = false;
    this.removing = false; this.removalWriteFailed = false; this.confirmedRoom = null;
    this.sync = new Map(); this.syncWaits = new Set(); this.hostReads = new Set(); this.sessionReads = new Set();
    this.blockedShares = new Set();
    this.writeTail = Promise.resolve(); this.writeJobs = new Map();
    this.store = new JsonStore(path.join(home, "ccapp-hosting.json"), { version: 1, host: null, remotes: [] });
    this.ledger = new JsonStore(path.join(home, "ccapp-hosting-actions.json"), { actions: {} });
    const c = this.store.value;
    if (c.version !== 1 || !Array.isArray(c.remotes) || c.remotes.length > 8 || !object(this.ledger.value.actions)) throw new Error("托管配置格式不正确");
    if (c.host) {
      const h = c.host;
      if (!object(h) || !ID_RE.test(h.room) || !TOKEN_RE.test(h.hostToken) || !TOKEN_RE.test(h.clientToken) || !TOKEN_RE.test(h.secret) || !object(h.shares) ||
          (h.removal !== undefined && (!REMOVAL.has(h.removal) || h.enabled !== false))) throw new Error("主机配置格式不正确");
      serverUrl(h.url, test);
      for (const [sid, s] of Object.entries(h.shares)) if (!SID_RE.test(sid) || !object(s) || !ID_RE.test(s.grant)) throw new Error("托管会话配置格式不正确");
    }
    for (const r of c.remotes) { if (!ID_RE.test(r.id)) throw new Error("远端配置格式不正确"); decodePair(encodePair(r), test); }
  }
  hostActive(h = this.store.value.host) { return !!h?.enabled && !h.removal && !this.removing && !this.removalWriteFailed; }
  removalState(h) {
    if (this.removing) return "working";
    if (this.confirmedRoom === h.room) return "confirmed";
    if (this.removalWriteFailed) return "save-failed";
    return h.removal || null;
  }
  status() {
    this.reconcileTrash();
    const h = this.store.value.host;
    return { protocol: 1, boot: this.boot, host: h ? { name: h.name, url: h.url, enabled: this.hostActive(h),
      online: this.hostActive(h) && !!this.controller && age() - this.lastPoll < 45000 && !this.error,
      removal: this.removalState(h), removalRoom: h.room,
      error: this.error, shares: this.shares({ local: true }) } : null,
      remotes: this.store.value.remotes.map(({ id, name, url, room }) => ({ id, name, url, room })), lastPoll: this.lastPoll };
  }
  shares({ local = false } = {}) {
    const h = this.store.value.host;
    if (!h || (!local && !this.hostActive(h))) return [];
    return Object.entries(h.shares).flatMap(([id, s]) => {
      // 读不到不等于已删除，授权仍占名额；仅本机列出，允许手动清理。
      const blocked = this.blockedShares.has(id);
      const st = blocked || (this.adapter.trashState?.(id) || "clear") !== "clear" ? null : this.adapter.summary(id);
      if (st) return [{ ...st, title: s.title, grant: s.grant }];
      return local ? [{ id, title: s.title, unavailable: true, ...(blocked ? { revocationPending: true } : {}) }] : [];
    });
  }
  revokeShares(ids) {
    const h = this.store.value.host;
    if (!h) return;
    const revoked = ids.filter((id) => Object.hasOwn(h.shares, id));
    if (!revoked.length) return;
    const shares = { ...h.shares };
    for (const id of revoked) {
      // Even if persistence fails, deny in this process. The synchronous lifecycle gate
      // keeps deletion/restore from erasing the evidence until a durable retry succeeds.
      this.blockedShares.add(id); delete shares[id]; this.cancelSync(id, shareRevoked());
    }
    try { this.store.save({ ...this.store.value, host: { ...h, shares } }); }
    catch { throw new Error("未能保存托管撤销；本次运行已阻止远端访问，请检查存储后重试"); }
    for (const id of revoked) this.blockedShares.delete(id);
  }
  revokeSession(id) { this.revokeShares([id]); }
  reconcileTrash() {
    const h = this.store.value.host;
    if (!h) return;
    // Legacy trash predates the lifecycle hook. While its evidence exists, suppress the
    // grant even if a write fails; never infer deletion from absence in the live store.
    const ids = Object.keys(h.shares).filter((id) => this.blockedShares.has(id) || this.adapter.trashState?.(id) === "deleted");
    try { this.revokeShares(ids); } catch {} // status exposes cleanup pending; the lifecycle hook must not swallow this error
  }
  async config(fn) {
    if (this.configBusy) throw new Error("另一项配置正在保存，请稍后再试");
    this.configBusy = true;
    try { return await fn(); } finally { this.configBusy = false; }
  }
  async createHost(b) {
    return this.config(async () => {
      if (this.store.value.host) throw new Error("已有主机配置，请先解除旧主机");
      const url = serverUrl(b.url, this.test), name = text(b.name, 60).trim() || "我的平板";
      if (!TOKEN_RE.test(b.enrollToken)) throw new Error("注册令牌应为服务器生成的43位随机凭据");
      const rec = await request(url + ROOT + "/rooms", b.enrollToken, "POST", {});
      if (!ID_RE.test(rec.room) || !TOKEN_RE.test(rec.hostToken) || !TOKEN_RE.test(rec.clientToken)) throw new Error("中转服务返回了无效凭据");
      const h = { url, name, room: rec.room, hostToken: rec.hostToken, clientToken: rec.clientToken, secret: token(), enabled: true, shares: {} };
      try { this.store.save({ ...this.store.value, host: h }); }
      catch (e) {
        try { await request(url + ROOT + `/rooms/${rec.room}`, rec.hostToken, "DELETE"); } catch {}
        throw e;
      }
      this.start(); return this.status();
    });
  }
  toggle(b) {
    const h = this.store.value.host;
    if (!h) throw new Error("请先配置主机");
    if (this.configBusy) throw new Error("主机配置正在变更");
    if (this.removalState(h)) throw new Error("该主机正在解除，不能重新启用，请完成吊销或清除本机记录");
    if (typeof b.enabled !== "boolean") throw new Error("无效的主机开关");
    this.store.save({ ...this.store.value, host: { ...h, enabled: b.enabled, name: typeof b.name === "string" ? text(b.name, 60).trim() || h.name : h.name } });
    this.stop(); if (b.enabled) this.start(); return this.status();
  }
  saveRemoval(removal) {
    // Session trash and local pairing changes can happen while awaiting DELETE. Preserve
    // the current shares/remotes; never write back the copy captured before the request.
    this.store.save({ ...this.store.value, host: { ...this.store.value.host, enabled: false, removal } });
  }
  clearHost() {
    this.store.save({ ...this.store.value, host: null });
    this.blockedShares.clear(); this.removalWriteFailed = false; this.confirmedRoom = null; this.error = "";
  }
  async removeHost(b = {}) {
    return this.config(async () => {
      const h = this.store.value.host;
      if (!h) return { ...this.status(), removalResult: "no-host" };
      if (b.room !== undefined && b.room !== h.room) throw new Error("主机配置已变化，请刷新后再确认");
      this.removing = true;
      try {
        this.stop();
        if (h.removal !== "confirmed" && this.confirmedRoom !== h.room) {
          // No DELETE before the local non-reenableable state is durable.
          if (!h.removal) {
            try { this.saveRemoval("pending"); }
            catch {
              this.removalWriteFailed = true;
              throw new Error("本机连接已停止，但未能保存停用状态；未联系中转，重启前请检查存储并重试");
            }
          }
          this.removalWriteFailed = false;
          try {
            const out = await request(h.url + ROOT + `/rooms/${h.room}`, h.hostToken, "DELETE");
            // v1 relays acknowledge an authenticated DELETE with {ok:true}. Neither a
            // generic 2xx body nor an unauthorized/absent response proves deletion.
            if (!object(out) || out.ok !== true || Object.keys(out).length !== 1) throw new Error("invalid removal response");
          } catch {
            try { this.saveRemoval("failed"); }
            catch { throw new Error("本机已停用；无法确认中转是否已吊销，也未能保存失败状态，请检查存储后重试"); }
            throw new Error("本机已停用，无法确认中转是否已吊销。可以重试，或仅清除本机记录");
          }
          this.confirmedRoom = h.room;
        }
        try { this.clearHost(); }
        catch {
          if (this.store.value.host?.removal !== "confirmed") try { this.saveRemoval("confirmed"); } catch {}
          throw new Error("中转已确认吊销，本机已停用；未能清除本机记录，请检查存储后重试");
        }
      } finally { this.removing = false; }
      return { ...this.status(), removalResult: "revoked" };
    });
  }
  clearHostLocally(b) {
    const h = this.store.value.host;
    if (this.configBusy) throw new Error("主机配置正在变更，请稍后重试");
    if (!h || b.confirm !== true || b.room !== h.room) throw new Error("请刷新并明确确认仅清除当前主机的本机记录");
    if (h.enabled !== false || this.removalState(h) !== "failed") throw new Error("只允许为已停用且未能确认中转吊销的主机清除本机记录");
    this.stop();
    this.clearHost();
    return { ...this.status(), removalResult: "local-only" };
  }
  pairCode() {
    const h = this.store.value.host; if (!h) throw new Error("请先配置主机");
    if (this.removalState(h)) throw new Error("该主机正在解除，不能再显示配对码");
    return { code: encodePair(h) };
  }
  share(b) {
    const h = this.store.value.host;
    if (!h) throw new Error("请先配置主机");
    if (this.configBusy) throw new Error("主机配置正在变更");
    const sid = b.sessionId;
    if (typeof sid !== "string" || !SID_RE.test(sid) || typeof b.enabled !== "boolean") throw new Error("请选择已有会话");
    if (!b.enabled) {
      this.revokeSession(sid);
      return this.status();
    }
    if (this.removalState(h)) throw new Error("该主机正在解除，不能开启新托管");
    this.reconcileTrash();
    if (this.blockedShares.has(sid)) throw shareRevoked();
    this.checkAvailable(sid);
    const current = this.store.value.host, shares = { ...current.shares };
    if (!Object.hasOwn(shares, sid)) {
      if (Object.keys(shares).length >= 32) throw new Error("最多托管32个会话；可先清理暂不可用的托管记录");
      const s = this.adapter.localList().find((s) => s.id === sid);
      this.store.save({ ...this.store.value, host: { ...current, shares: { ...shares,
        [sid]: { title: text(s?.title, 100) || sid.slice(0, 8), grant: uid() } } } });
    }
    return this.status();
  }
  pair(b) {
    const p = decodePair(b.code, this.test), remotes = [...this.store.value.remotes];
    const at = remotes.findIndex((r) => r.room === p.room && r.url === p.url);
    if (at < 0 && remotes.length >= 8) throw new Error("最多保存8台远端设备");
    const r = { ...p, id: at >= 0 ? remotes[at].id : uid(), name: text(b.name, 60).trim() || "家中平板" };
    if (at >= 0) remotes[at] = r; else remotes.push(r);
    this.store.save({ ...this.store.value, remotes }); return this.status();
  }
  forget(b) {
    this.store.save({ ...this.store.value, remotes: this.store.value.remotes.filter((r) => r.id !== b.remoteId) });
    return this.status();
  }
  async rpc(b, signal) {
    const r = this.store.value.remotes.find((r) => r.id === b.remoteId);
    if (!r) throw new Error("这台设备尚未配对");
    if (!ID_RE.test(b.id || "") || !OPS.has(b.op)) throw new Error("无效的远程操作");
    for (const [k, v] of this.outbound) if (age() - v.at > 120000 && !v.pending) this.outbound.delete(k);
    if (this.outbound.size >= 256) throw new Error("操作过于频繁，请稍后再试");
    const key = r.id + ":" + b.id;
    const raw = { v: 1, id: b.id, op: b.op, sessionId: b.sessionId, boot: b.boot, grant: b.grant, args: object(b.args) ? b.args : {} };
    const hash = digest(JSON.stringify(raw));
    let entry = this.outbound.get(key);
    if (entry && entry.hash !== hash) throw new Error("同一发送编号不能用于不同内容");
    if (entry?.done) return entry.result;
    if (entry?.pending) throw new Error("这项操作正在等待主机确认");
    if (!entry) { entry = { at: age(), hash, req: { ...raw, expires: age() + 30000 } }; this.outbound.set(key, entry); }
    entry.pending = true;
    try { const result = await remoteCall(r, entry.req, signal); entry.done = true; entry.result = result; return result; }
    finally { entry.pending = false; if (!MUTATIONS.has(b.op)) this.outbound.delete(key); }
  }
  cancelSync(sid, reason = abortError()) {
    for (const wait of this.syncWaits) if (!sid || wait.sid === sid) wait.controller.abort(reason);
    for (const read of this.sessionReads) if (!sid || read.sid === sid) read.controller.abort(reason);
    for (const [key, entry] of this.sync) if (!sid || entry.sid === sid) this.dropSync(key);
  }
  dropSync(key) {
    const entry = this.sync.get(key);
    if (entry) { clearTimeout(entry.timer); entry.loadController?.abort(abortError()); }
    this.sync.delete(key);
  }
  touchSync(entry) {
    entry.at = age(); clearTimeout(entry.timer);
    entry.timer = setTimeout(() => {
      if (entry.waiters || entry.loading) this.touchSync(entry);
      else this.dropSync(entry.key);
    }, SYNC_LIMITS.ttl);
    entry.timer.unref?.();
  }
  trimSync(keep) {
    const bytes = () => [...this.sync.values()].reduce((n, entry) => n + (entry.current?.bytes || 0) + entry.records.reduce((m, r) => m + r.bytes, 0), 0);
    // 先丢旧修订，再逐出不活动的会话；正在用的当前快照不能在读到一半被替换。
    while (bytes() > SYNC_LIMITS.bytes) {
      const older = [...this.sync.values()].filter((e) => e.records.length > 1).sort((a, b) => a.records[0].at - b.records[0].at)[0];
      if (older) { older.records.shift(); continue; }
      const idle = [...this.sync.values()].filter((e) => e !== keep && !e.waiters && !e.loading).sort((a, b) => a.at - b.at)[0];
      if (!idle) throw new Error("同步缓存已满，请稍后重试");
      this.dropSync(idle.key);
    }
  }
  syncEntry(h, sid) {
    const key = [this.boot, h.room, h.shares[sid].grant, sid].join(":");
    for (const [k, e] of this.sync) if (!e.waiters && !e.loading && age() - e.at >= SYNC_LIMITS.ttl) this.dropSync(k);
    let entry = this.sync.get(key);
    if (!entry) {
      if (this.sync.size >= SYNC_LIMITS.entries) {
        const idle = [...this.sync.values()].filter((e) => !e.waiters && !e.loading).sort((a, b) => a.at - b.at)[0];
        if (!idle) throw new Error("同步会话过多，请稍后重试");
        this.dropSync(idle.key);
      }
      entry = { key, sid, cache: {}, records: [], version: 1, readVersion: 0, waiters: 0,
        loadController: new AbortController() };
      this.sync.set(key, entry);
    }
    this.touchSync(entry); return entry;
  }
  checkRead(q, h, epoch, boot, signal) {
    const sid = q.sessionId, now = this.store.value.host;
    if (!this.hostActive(now) || now.room !== h.room || this.blockedShares.has(sid) ||
        !Object.hasOwn(now.shares, sid) || now.shares[sid].grant !== h.shares[sid]?.grant) throw shareRevoked();
    if (this.epoch !== epoch || this.boot !== boot || signal?.aborted) throw signal?.reason || abortError();
    if (q.expires <= age()) throw new Error("请求已过期，请刷新后重试");
  }
  checkAvailable(sid) {
    const state = this.adapter.trashState?.(sid);
    if (state === "deleted") {
      try { this.revokeSession(sid); } catch {} // blocked even on a persistence error
      throw shareRevoked();
    }
    if ((state && state !== "clear") || this.adapter.exists?.(sid) === false) {
      const error = new Error("主机会话暂不可用");
      this.cancelSync(sid, error);
      throw error;
    }
  }
  async readSession(q, h, args, epoch, boot, signal) {
    const controller = new AbortController(), read = { sid: q.sessionId, controller };
    const combined = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
    const timer = setTimeout(() => controller.abort(new Error("请求已过期，请刷新后重试")), Math.max(0, q.expires - age()));
    this.sessionReads.add(read);
    try {
      const result = await untilAbort(this.adapter.session(q.sessionId, args), combined);
      this.checkRead(q, h, epoch, boot, combined);
      this.checkAvailable(q.sessionId);
      return deliverySnapshot(result);
    } finally {
      clearTimeout(timer); this.sessionReads.delete(read);
    }
  }
  async syncSnapshot(entry, verifyHistory, signal) {
    if (!entry.loading && (verifyHistory || entry.readVersion !== entry.version || !entry.current)) {
      const version = entry.version;
      entry.loading = untilAbort((async () => {
        const raw = this.adapter.watchSession
          ? await this.adapter.watchSession(entry.sid, entry.cache, { verifyHistory })
          : structuredClone(await this.adapter.session(entry.sid, {}));
        if (entry.loadController.signal.aborted || this.sync.get(entry.key) !== entry) throw abortError();
        const snapshot = { id: entry.sid, cursor: raw.cursor ?? 0, items: raw.items || [], live: raw.live || [],
          run: raw.run || null, perms: raw.perms || [], queue: raw.queue || [], interrupted: !!raw.interrupted,
          ...(raw.info !== undefined ? { info: raw.info } : {}), ...(raw.range !== undefined ? { range: raw.range } : {}),
          ...(raw.marks !== undefined ? { marks: raw.marks } : {}), ...(raw.historyToken !== undefined ? { historyToken: raw.historyToken } : {}) };
        const info = snapshotInfo(snapshot);
        if (info.bytes > SYNC_LIMITS.snapshotBytes) throw new Error("同步会话过大，请在主机查看完整记录");
        if (info.hash !== entry.current?.hash) {
          const record = { ...info, snapshot, revision: uid(), at: age() };
          entry.records = entry.records.filter((r) => age() - r.at < SYNC_LIMITS.ttl).slice(-(SYNC_LIMITS.revisions - 1));
          entry.records.push(record); entry.current = record;
          try { this.trimSync(entry); } catch (e) { this.dropSync(entry.key); throw e; }
        } else entry.current.at = age(); // 活跃的未变快照续租；闲置/旧修订仍按TTL淘汰。
        entry.readVersion = version; this.touchSync(entry);
      })(), entry.loadController.signal).finally(() => { entry.loading = null; });
    }
    if (entry.loading) await untilAbort(entry.loading, signal);
    return entry.current;
  }
  async watch(q, h, args, signal) {
    if (!this.adapter.subscribe) throw new Error("此主机不支持增量同步");
    if (["before", "after", "until", "exclude"].some((k) => Object.hasOwn(args, k)) ||
        (Object.hasOwn(args, "immediate") && typeof args.immediate !== "boolean") ||
        (args.revision != null && (typeof args.revision !== "string" || args.revision.length > 128)))
      throw new Error("无效的同步修订号；历史分页请使用 session");
    if (this.syncWaits.size >= SYNC_LIMITS.waits || [...this.syncWaits].filter((w) => w.sid === q.sessionId).length >= SYNC_LIMITS.perSession)
      throw new Error("同步等待过多，请稍后重试");
    const controller = new AbortController(), epoch = this.epoch, boot = this.boot;
    // 给末尾的异步文件戳核对预留25ms，整个 host 读取（包括慢盘）8秒硬封顶。
    const began = age(), deadline = Math.min(began + SYNC_LIMITS.wait - 25, q.expires);
    const entry = this.syncEntry(h, q.sessionId); entry.waiters++;
    const wait = { sid: q.sessionId, controller }; this.syncWaits.add(wait);
    // 运输 TTL 同样约束首次异步读盘，不能只约束进入 wait 之后的阶段。
    const expiresTimer = setTimeout(() => controller.abort(new Error("请求已过期，请刷新后重试")), Math.max(0, q.expires - age()));
    const hardTimer = setTimeout(() => controller.abort(new Error("同步读取超时，请重试")), SYNC_LIMITS.wait);
    const abort = () => controller.abort(signal.reason || abortError());
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    let unsubscribe, wake, timer, coalesced, notifiedAt = 0;
    const changed = (change) => {
      if (change?.history) entry.cache.history = null;
      if (change?.marks) entry.cache.marks = null;
      entry.version++;
      if (!notifiedAt) notifiedAt = age();
      wake?.();
    };
    const pause = (ms, onChange) => new Promise((resolve, reject) => {
      const done = (error) => {
        clearTimeout(timer); timer = null; wake = null;
        controller.signal.removeEventListener("abort", canceled);
        error ? reject(error) : resolve();
      };
      const canceled = () => done(controller.signal.reason || abortError());
      if (controller.signal.aborted) return canceled();
      controller.signal.addEventListener("abort", canceled, { once: true });
      wake = onChange ? () => done() : null;
      timer = setTimeout(() => done(), Math.max(0, ms));
    });
    try {
      const check = () => this.checkRead(q, h, epoch, boot, controller.signal);
      check();
      // 先订阅再读，读盘期间来的事件只标脏，绝不丢唤醒，也不在总线回调里读盘。
      unsubscribe = this.adapter.subscribe(q.sessionId, changed); check();
      let current = await this.syncSnapshot(entry, true, controller.signal); check();
      const finish = (out) => { this.checkAvailable(q.sessionId); return out; };
      const response = () => {
        const prior = entry.records.find((r) => r.revision === args.revision && age() - r.at < SYNC_LIMITS.ttl);
        if (!prior) return { sync: 1, revision: current.revision, reset: true, snapshot: deliverySnapshot(current.snapshot) };
        if (prior === current) return null;
        const delta = sessionDelta(prior.snapshot, current.snapshot);
        if (delta?.info) delta.info = deliveryInfo(delta.info);
        return delta === null ? { sync: 1, revision: current.revision, reset: true, snapshot: deliverySnapshot(current.snapshot) }
          : { sync: 1, revision: current.revision, reset: false, delta };
      };
      let out = response(); if (out) return finish(out);
      // 回访只核对一次当前修订，控制端可以立即解锁；下一次仍用原八秒事件等待。
      if (args.immediate === true) return finish({ sync: 1, revision: current.revision, reset: false,
        ...(current.snapshot.info ? { delta: { info: deliveryInfo(current.snapshot.info) } } : { unchanged: true }) });
      while (age() < deadline) {
        if (entry.version === entry.readVersion) await pause(deadline - age(), true);
        check();
        if (age() >= deadline) break;
        coalesced = notifiedAt || age();
        await pause(Math.min(deadline - age(), Math.max(0, coalesced + SYNC_LIMITS.coalesce - age())), false);
        notifiedAt = 0; check();
        current = await this.syncSnapshot(entry, false, controller.signal); check();
        out = response(); if (out) return finish(out);
      }
      // 没有事件的离线历史改写（delmsg/import）也会在有界心跳处核对文件戳，不按 token 查盘。
      current = await this.syncSnapshot(entry, true, controller.signal); check();
      return finish(response() || { sync: 1, revision: current.revision, reset: false, unchanged: true });
    } finally {
      clearTimeout(timer); clearTimeout(expiresTimer); clearTimeout(hardTimer);
      unsubscribe?.(); signal?.removeEventListener("abort", abort);
      entry.waiters--; this.syncWaits.delete(wait);
      if (!entry.waiters && entry.loading && controller.signal.aborted && this.sync.get(entry.key) === entry) this.dropSync(entry.key);
    }
  }
  start() {
    if (this.controller || !this.hostActive()) return;
    this.reconcileTrash();
    this.cancelSync();
    const controller = new AbortController(), epoch = ++this.epoch;
    this.controller = controller; this.error = "";
    this.loop(controller, epoch).catch(() => { if (this.epoch === epoch) this.error = "托管连接已停止，请重新启用"; });
  }
  stop() {
    this.epoch++; this.controller?.abort(); this.controller = null; this.lastPoll = 0;
    this.cancelSync();
  }
  async loop(controller, epoch) {
    let backoff = 1000;
    while (!controller.signal.aborted && this.epoch === epoch) {
      const h = this.store.value.host;
      if (!this.hostActive(h)) return;
      try {
        const p = await request(h.url + ROOT + `/rooms/${h.room}/poll`, h.hostToken, "GET", undefined, controller.signal, 25000);
        if (controller.signal.aborted || this.epoch !== epoch) return;
        if (!Array.isArray(p.requests) || p.requests.length > 4) throw new Error("中转队列格式无效");
        this.lastPoll = age(); this.error = ""; backoff = 1000;
        for (const env of p.requests) {
          if (controller.signal.aborted || this.epoch !== epoch) return;
          if (!ID_RE.test(env?.id || "")) continue;
          let req;
          try { req = unseal(h.secret, h.room, env.id, "request", env.packet); } catch { continue; }
          if (req?.op === "watch" && this.hostReads.size < SYNC_LIMITS.waits) {
            // 只把有界只读等待拆出去；poll 立即继续收 send/stop/answer，普通/写操作仍按到达顺序执行。
            if ([...this.hostReads].some((r) => r.id === env.id)) continue;
            const read = { id: env.id };
            this.hostReads.add(read);
            read.task = this.reply(h, req, env.id, controller, epoch).catch(() => {
              if (!controller.signal.aborted && this.epoch === epoch) this.error = "网络暂时断开，正在自动重连";
            }).finally(() => this.hostReads.delete(read));
          } else await this.reply(h, req, env.id, controller, epoch,
            req?.op === "watch" ? new Error("同步等待过多，请稍后重试") : null);
        }
      } catch (e) {
        if (controller.signal.aborted || this.epoch !== epoch) return;
        this.error = e.status === 401 || e.status === 403 || e.status === 404 ? "服务器凭据已失效，请重新配置主机" : "网络暂时断开，正在自动重连";
        await new Promise((resolve) => {
          const done = () => { clearTimeout(t); controller.signal.removeEventListener("abort", done); resolve(); };
          const t = setTimeout(done, backoff); t.unref?.(); controller.signal.addEventListener("abort", done, { once: true });
        });
        backoff = Math.min(20000, backoff * 2);
      }
    }
  }
  async reply(h, req, id, controller, epoch, rejected) {
    let reply;
    const grant = typeof req?.sessionId === "string" ? this.store.value.host?.shares[req.sessionId]?.grant : undefined;
    try {
      if (rejected) throw rejected;
      let result = await this.dispatch(req, id, { signal: controller.signal });
      // Recheck at the final synchronous delivery boundary as a deletion can occur
      // after the adapter promise resolves but before reply resumes here.
      if (req.op !== "list") {
        const now = this.store.value.host;
        if (!this.hostActive(now) || now.room !== h.room || !grant || this.blockedShares.has(req.sessionId) ||
            now.shares[req.sessionId]?.grant !== grant) throw shareRevoked();
      } else {
        const now = this.store.value.host;
        result = { ...result, sessions: result.sessions.filter(({ id: sid, grant: offered }) =>
          now?.room === h.room && !this.blockedShares.has(sid) && now.shares[sid]?.grant === offered) };
      }
      reply = { v: 1, id, ok: true, result };
    } catch (e) { reply = { v: 1, id, ok: false, error: text(e.message, 300) }; }
    if (controller.signal.aborted || this.epoch !== epoch) return;
    // Best-effort cancel a successful session/watch upload too. Bytes already
    // delivered to the controller cannot be withdrawn.
    const upload = reply.ok && (req.op === "session" || req.op === "watch")
      ? { sid: req.sessionId, controller: new AbortController() } : null;
    if (upload) this.sessionReads.add(upload);
    const signal = upload ? AbortSignal.any([controller.signal, upload.controller.signal]) : controller.signal;
    try {
      await request(h.url + ROOT + `/rooms/${h.room}/reply`, h.hostToken, "POST",
        { id, packet: seal(h.secret, h.room, id, "response", reply) }, signal, 10000);
    } catch (e) { if (e.status !== 409 && !upload?.controller.signal.aborted) throw e; }
    finally { if (upload) this.sessionReads.delete(upload); }
  }
  dispatch(q, envelopeId, context = {}) {
    context = { ...context, epoch: this.epoch };
    if (!MUTATIONS.has(q?.op)) return this.dispatchNow(q, envelopeId, context);
    // 同一个 id 的并发重试拒绝；不同写请求严格排队，不能在读等待拆分后并发改台账。
    if (this.writeJobs.has(q.id)) return Promise.reject(new Error("该操作可能已经执行，请刷新会话确认，禁止自动重复执行"));
    if (this.writeJobs.size >= 16) return Promise.reject(new Error("写操作排队过多，请稍后重试"));
    let job;
    if (!this.writeJobs.size) job = this.dispatchNow(q, envelopeId, context);
    else job = this.writeTail.then(() => this.dispatchNow(q, envelopeId, context));
    job = job.finally(() => this.writeJobs.delete(q.id));
    this.writeJobs.set(q.id, job); this.writeTail = job.catch(() => {});
    return job;
  }
  async dispatchNow(q, envelopeId, { signal, epoch: queuedEpoch } = {}) {
    const h = this.store.value.host, epoch = this.epoch, boot = this.boot;
    if (!this.hostActive(h)) throw new Error("主机已停用托管");
    if (queuedEpoch !== undefined && queuedEpoch !== this.epoch) throw abortError();
    if (!object(q) || q.v !== 1 || !ID_RE.test(q.id || "") || q.id !== envelopeId || !OPS.has(q.op)) throw new Error("无效的远程请求");
    if (!Number.isFinite(q.expires) || q.expires > age() + 45000) throw new Error("远程请求时间无效，请核对两台设备时钟");
    const write = MUTATIONS.has(q.op), args = object(q.args) ? q.args : {};
    if (q.op !== "list") {
      const sid = q.sessionId;
      if (typeof sid !== "string" || !SID_RE.test(sid) || !Object.hasOwn(h.shares, sid) || this.blockedShares.has(sid))
        throw new Error("该会话没有开启托管，或已取消托管");
      // Older controllers omitted boot/grant for plain reads; supplied authority
      // must match so a stale page request cannot cross an explicit regrant.
      if (((write || q.op === "watch" || q.boot !== undefined) && q.boot !== boot) ||
          ((write || q.op === "watch" || q.grant !== undefined) && q.grant !== h.shares[sid].grant))
        throw new Error("主机或托管授权已变化，请刷新会话再操作");
      this.checkAvailable(sid);
    }
    // expires 是运输时限，不是操作身份：控制端重启/缓存淘汰后续期同一 id，也只能拿原结果。
    const key = h.room + ":" + q.id, hash = digest(JSON.stringify({ op: q.op, sessionId: q.sessionId, boot: q.boot, grant: q.grant, args }));
    if (write) {
      const prev = this.ledger.value.actions[key];
      if (prev) {
        if (prev.hash !== hash) throw new Error("操作编号已用于不同的请求");
        if (prev.state === "done") return prev.result;
        throw new Error(prev.error || "该操作可能已经执行，请刷新会话确认，禁止自动重复执行");
      }
    }
    if (q.expires < age()) throw new Error("请求已过期，请刷新后重试");
    if (signal?.aborted) throw signal.reason || abortError();
    if (!write) {
      if (q.op === "list") {
        this.reconcileTrash();
        return { boot: this.boot, name: h.name, sync: this.adapter.subscribe ? 1 : 0,
          ...(this.adapter.subscribe ? { resume: 1 } : {}),
          ...(this.adapter.infoVersion ? { info: this.adapter.infoVersion } : {}),
          ...(this.adapter.historyVersion ? { history: this.adapter.historyVersion } : {}),
          ...(this.adapter.actionVersion ? { actions: this.adapter.actionVersion } : {}), sessions: this.shares() };
      }
      if (q.op === "watch") return this.watch(q, h, args, signal);
      return this.readSession(q, h, args, epoch, boot, signal);
    }
    const check = () => {
      this.checkRead(q, h, epoch, boot, signal);
      this.checkAvailable(q.sessionId);
    };
    let messageHistoryToken;
    if (q.op === "message") {
      if (this.adapter.actionVersion !== 1 || typeof this.adapter.message !== "function") throw new Error("主机版本不支持远程消息操作，请更新主机");
      const entry = this.sync.get([this.boot, h.room, q.grant, q.sessionId].join(":"));
      if (!ID_RE.test(args.revision || "") || entry?.current?.revision !== args.revision) throw new Error("主机记录已变化，请重新同步后再操作消息");
      const current = await this.syncSnapshot(entry, true, signal || new AbortController().signal);
      check();
      if (current.revision !== args.revision) throw new Error("主机记录已变化，请重新同步后再操作消息");
      messageHistoryToken = current.snapshot.historyToken;
    }
    // 先持久化预占再调用上游：回执丢失或进程被杀也绝不盲目再发一轮。
    // 不能按时间淘汰仍有效授权用过的 id。只清旧 boot 或已撤销 grant；它们已在上方拒绝。
    const actions = Object.fromEntries(Object.entries(this.ledger.value.actions).filter(([, v]) =>
      v.boot === this.boot && v.room === h.room && v.grant === h.shares[v.sessionId]?.grant));
    if (Object.keys(actions).length >= 10000) throw new Error("本次托管授权的操作记录已满，请在主机取消并重新托管，刷新后再操作");
    actions[key] = { at: age(), hash, state: "pending", boot: this.boot, room: h.room, sessionId: q.sessionId, grant: q.grant };
    this.ledger.save({ actions });
    try {
      const result = await this.adapter[q.op](q.sessionId, args, q.id, { check, historyToken: messageHistoryToken });
      if (q.op === "message") for (const entry of this.sync.values()) if (entry.sid === q.sessionId) {
        if (["delete", "undo"].includes(args.action)) entry.cache.history = null;
        entry.cache.marks = null; entry.version++;
      }
      this.ledger.save({ actions: { ...this.ledger.value.actions, [key]: { ...actions[key], state: "done", result } } });
      return result;
    } catch (e) {
      try { this.ledger.save({ actions: { ...this.ledger.value.actions, [key]: { ...actions[key], state: "uncertain", error: text(e.message, 300) } } }); } catch {}
      throw e;
    }
  }
}
