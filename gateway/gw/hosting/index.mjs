import { CFG, LIVE, log } from "../config.mjs";
import { NEXT, readBodyBuf, sendJson } from "../http.mjs";
import { onShutdown } from "../crashguard.mjs";
import { TRASH_HOOKS } from "../runs.mjs";
import { routeCaps } from "../caps.mjs";
import { adapter } from "./adapter.mjs";
import { HostingEngine } from "./engine.mjs";
import { object, text } from "./core.mjs";

let engine;
function getEngine() { return engine ||= new HostingEngine({ home: CFG.home, adapter }); }
// Register before startup recovery, without opening the store until the process
// owns the gateway or a real lifecycle notification is dispatched. Do not fire
// and forget: session storage must retain its journal if persistence throws.
TRASH_HOOKS.push((id) => getEngine().revokeSession(id));
export function startHosting() {
  if (!LIVE) return;
  try { getEngine().start(); } catch (e) { log("会话托管未启动：" + text(e.message, 180)); }
}
onShutdown("会话托管", () => engine?.stop());

export async function routeHosting(req, res, u, p) {
  // 原生壳划掉任务卡时以 /api/status 判断是否退出；仅对已验签的壳请求持有托管租约。
  // 不伪造 .run-active，不改普通界面的真实 running/busy，也不承诺系统强杀后自动恢复。
  if (p === "/api/status" && req.method === "GET" && req.ccBodySha && engine?.hostActive() && engine.shares().length) {
    let code = 0, body;
    const capture = { writeHead(c) { code = c; }, end(b) { body = JSON.parse(String(b)); } };
    await routeCaps(req, capture, u, p);
    return sendJson(res, code || 200, { ...body, busy: true, hostingLease: true });
  }
  if (!p.startsWith("/api/hosting/")) return NEXT;
  // 凭据和会话文字不缓存；沿用网关原有 Host/Origin/Cookie 鉴权，不增加公开入口。
  res.setHeader("Cache-Control", "no-store");
  try {
    const e = getEngine(), action = p.slice("/api/hosting/".length);
    if (req.method === "GET") {
      if (action === "status") return sendJson(res, 200, e.status());
      if (action === "local-sessions") return sendJson(res, 200, { sessions: adapter.localList() });
    }
    if (req.method !== "POST") return sendJson(res, 404, { error: "没有这个托管接口" });
    const raw = await readBodyBuf(req, 128 * 1024);
    let b;
    try { b = JSON.parse(raw.toString() || "{}"); } catch { return sendJson(res, 400, { error: "请求不是有效 JSON" }); }
    if (!object(b)) return sendJson(res, 400, { error: "请求格式不正确" });
    let out;
    switch (action) {
      case "host": out = await e.createHost(b); break;
      case "host-toggle": out = e.toggle(b); break;
      case "host-remove": out = await e.removeHost(b); break;
      case "host-remove-local": out = e.clearHostLocally(b); break;
      case "pair-code": out = e.pairCode(); break;
      case "share": out = e.share(b); break;
      case "pair": out = e.pair(b); break;
      case "forget": out = e.forget(b); break;
      case "rpc": {
        const c = new AbortController();
        const cancel = () => { if (!res.writableEnded) c.abort(); };
        res.once("close", cancel);
        req.once("aborted", cancel);
        // 取消本地 RPC 会断开到中转的请求；中转不转发 cancel，主机 watch 最多残留8秒。
        if (res.destroyed || req.aborted) c.abort();
        try { out = { result: await e.rpc(b, c.signal) }; }
        finally { res.off("close", cancel); req.off("aborted", cancel); }
        break;
      }
      default: return sendJson(res, 404, { error: "没有这个托管接口" });
    }
    if (!res.destroyed) return sendJson(res, 200, out);
  } catch (err) {
    if (!res.destroyed) return sendJson(res, 400, { error: text(err.message, 300) || "托管操作失败" });
  }
}
