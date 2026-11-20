import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export const PROTOCOL = 1;
export const ROOT = "/cc-host/v1";
export const ID_RE = /^[a-f0-9]{32}$/;
export const SID_RE = /^[A-Za-z0-9_-]{1,80}$/;
export const TOKEN_RE = /^[A-Za-z0-9_-]{43}$/;
export const uid = () => crypto.randomBytes(16).toString("hex");
export const token = () => crypto.randomBytes(32).toString("base64url");
export const digest = (s) => crypto.createHash("sha256").update(s).digest("hex");
export const object = (v) => !!v && typeof v === "object" && !Array.isArray(v);
export const text = (v, n = 16000) => typeof v === "string" ? (v.length > n ? v.slice(0, n) + "\n…（远程摘要已截短，完整内容留在主机）" : v) : "";

export function serverUrl(raw, test = false) {
  const u = new URL(String(raw || "").trim());
  const localTest = test && u.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(u.hostname);
  if ((!localTest && u.protocol !== "https:") || u.username || u.password || u.search || u.hash || (u.pathname && u.pathname !== "/"))
    throw new Error("服务器地址必须是 HTTPS 源站地址，不含路径、账号或查询参数");
  return u.origin;
}

export function encodePair(h) {
  return "CCH1." + Buffer.from(JSON.stringify({ v: 1, url: h.url, room: h.room, clientToken: h.clientToken, secret: h.secret })).toString("base64url");
}
export function decodePair(code, test = false) {
  if (typeof code !== "string" || code.length > 3000 || !/^CCH1\.[A-Za-z0-9_-]+$/.test(code.trim())) throw new Error("配对码格式不正确");
  let p;
  try { p = JSON.parse(Buffer.from(code.trim().slice(5), "base64url").toString()); } catch { throw new Error("配对码损坏"); }
  if (!object(p) || p.v !== 1 || !ID_RE.test(p.room) || !TOKEN_RE.test(p.clientToken) || !TOKEN_RE.test(p.secret)) throw new Error("配对码版本或凭据无效");
  return { url: serverUrl(p.url, test), room: p.room, clientToken: p.clientToken, secret: p.secret };
}
function aad(room, id, dir) {
  if (!ID_RE.test(room) || !ID_RE.test(id) || !["request", "response"].includes(dir)) throw new Error("无效的加密信封");
  return Buffer.from(`cc-host-v1:${room}:${id}:${dir}`);
}
export function seal(secret, room, id, dir, value) {
  if (!TOKEN_RE.test(secret)) throw new Error("加密密钥无效");
  const b = Buffer.from(JSON.stringify(value));
  if (b.length > 1900000) throw new Error("远程消息过大");
  const iv = crypto.randomBytes(12), c = crypto.createCipheriv("aes-256-gcm", Buffer.from(secret, "base64url"), iv);
  c.setAAD(aad(room, id, dir));
  const data = Buffer.concat([c.update(b), c.final()]);
  return Buffer.concat([iv, c.getAuthTag(), data]).toString("base64url");
}
export function unseal(secret, room, id, dir, packet) {
  if (!TOKEN_RE.test(secret) || typeof packet !== "string" || packet.length > 3 * 1024 * 1024 || !/^[A-Za-z0-9_-]+$/.test(packet)) throw new Error("加密消息无效");
  const b = Buffer.from(packet, "base64url");
  if (b.length < 29) throw new Error("加密消息不完整");
  const d = crypto.createDecipheriv("aes-256-gcm", Buffer.from(secret, "base64url"), b.subarray(0, 12));
  d.setAAD(aad(room, id, dir)); d.setAuthTag(b.subarray(12, 28));
  return JSON.parse(Buffer.concat([d.update(b.subarray(28)), d.final()]).toString());
}

export class JsonStore {
  constructor(file, fallback) {
    this.file = file;
    try { this.value = JSON.parse(fs.readFileSync(file, "utf8")); }
    catch (e) {
      if (e.code !== "ENOENT") throw new Error("托管配置无法读取；已停止加载，未覆盖原文件");
      this.value = structuredClone(fallback);
    }
    if (!object(this.value)) throw new Error("托管配置格式损坏，未覆盖原文件");
  }
  save(next) {
    const dir = path.dirname(this.file), tmp = this.file + "." + uid() + ".tmp";
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    let fd;
    try {
      fd = fs.openSync(tmp, "wx", 0o600);
      fs.writeFileSync(fd, JSON.stringify(next)); fs.fsyncSync(fd); fs.closeSync(fd); fd = undefined;
      fs.renameSync(tmp, this.file);
      const d = fs.openSync(dir, "r"); try { fs.fsyncSync(d); } finally { fs.closeSync(d); }
      this.value = next;
    } finally {
      if (fd !== undefined) fs.closeSync(fd);
      try { fs.unlinkSync(tmp); } catch {}
    }
  }
}

export async function request(url, auth, method = "GET", body, signal, timeout = 30000) {
  const sig = signal ? AbortSignal.any([signal, AbortSignal.timeout(timeout)]) : AbortSignal.timeout(timeout);
  const r = await fetch(url, { method, headers: { Authorization: `Bearer ${auth}`, "Content-Type": "application/json" },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}), signal: sig, redirect: "error" });
  const chunks = []; let size = 0;
  for await (const c of r.body) {
    size += c.length;
    if (size > 4 * 1024 * 1024) throw new Error("中转响应过大");
    chunks.push(c);
  }
  let data;
  try { data = JSON.parse(Buffer.concat(chunks).toString()); } catch { throw new Error("中转服务未返回有效 JSON"); }
  // DELETE v1 confirms completion with HTTP 200 plus {ok:true}; a generic
  // 2xx (for example 202 Accepted) is not a completed revocation receipt.
  if (!r.ok || (method === "DELETE" && r.status !== 200)) {
    const e = new Error(r.status === 504 ? "主机未在时限内回应，请检查平板网络和后台运行" : `中转请求失败（${r.status}）`);
    e.status = r.status; throw e;
  }
  return data;
}

export async function remoteCall(pair, req, signal) {
  const id = req.id;
  const out = await request(pair.url + ROOT + `/rooms/${pair.room}/rpc`, pair.clientToken, "POST",
    { id, packet: seal(pair.secret, pair.room, id, "request", req) }, signal);
  if (out.id !== id) throw new Error("中转回复编号不匹配");
  const reply = unseal(pair.secret, pair.room, id, "response", out.packet);
  if (!object(reply) || reply.v !== 1 || reply.id !== id) throw new Error("主机回复无效");
  if (!reply.ok) throw new Error(text(reply.error, 300) || "主机拒绝了操作");
  return reply.result;
}
