import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { uid, token, seal, unseal, encodePair, decodePair, serverUrl, JsonStore } from "../gateway/gw/hosting/core.mjs";
import { HostingEngine } from "../gateway/gw/hosting/engine.mjs";

function fixture(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "hosting-unit-"));
  t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  const calls = [], sid = "session_A", grant = uid();
  const adapter = {
    localList: () => [{ id: sid, title: "测试会话" }], exists: (id) => { assert.equal(id, sid); return true; },
    summary: (id) => id === sid ? { id, running: true, waiting: 1 } : null,
    session: async (id) => ({ id, items: [{ role: "assistant", text: "你好" }] }),
    send: async (...args) => { calls.push(["send", ...args]); return { ok: true, id: "run-a" }; },
    stop: async (...args) => { calls.push(["stop", ...args]); return { ok: true }; },
    answer: async (...args) => { calls.push(["answer", ...args]); return { ok: true }; },
  };
  const e = new HostingEngine({ home, adapter });
  e.store.save({ version: 1, remotes: [], host: { url: "https://relay.example.invalid", room: uid(), hostToken: token(), clientToken: token(), secret: token(),
    name: "测试平板", enabled: true, shares: { [sid]: { grant, title: "测试会话" } } } });
  const req = (op = "send") => ({ v: 1, id: uid(), expires: Date.now() + 30000, op, sessionId: sid, boot: e.boot, grant, args: { message: "你好" } });
  t.after(() => e.stop());
  return { e, sid, grant, req, home, calls, adapter };
}

test("AES-GCM authenticates payload, room, request ID and direction", () => {
  const secret = token(), room = uid(), id = uid(), q = { message: "中文🙂", v: 1 };
  const p = seal(secret, room, id, "request", q);
  assert.deepEqual(unseal(secret, room, id, "request", p), q);
  assert.notEqual(seal(secret, room, id, "request", q), p);
  for (const args of [[token(), room, id, "request", p], [secret, uid(), id, "request", p], [secret, room, uid(), "request", p],
    [secret, room, id, "response", p], [secret, room, id, "request", "abc"]]) assert.throws(() => unseal(...args));
  const bytes = Buffer.from(p, "base64url"); bytes[30] ^= 1;
  assert.throws(() => unseal(secret, room, id, "request", bytes.toString("base64url")));
});
test("pair codes validate origin, version and credential lengths", () => {
  const h = { url: "https://relay.example.invalid", room: uid(), clientToken: token(), secret: token() };
  assert.deepEqual(decodePair(encodePair(h)), h);
  for (const url of ["http://relay.example.invalid", "https://user:pass@relay.example.invalid", "https://relay.example.invalid/path", "https://relay.example.invalid?q=x", "https://relay.example.invalid/#key"]) assert.throws(() => serverUrl(url));
  assert.equal(serverUrl("http://127.0.0.1:1234", true), "http://127.0.0.1:1234");
  assert.throws(() => decodePair("bad"));
  assert.throws(() => decodePair(encodePair({ ...h, clientToken: "short" })));
});
test("store saves atomically with private permissions and fails closed on corruption", (t) => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "hosting-store-"));
  t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  const file = path.join(home, "store.json"), s = new JsonStore(file, { a: 1 });
  s.save({ a: 2 }); assert.equal(new JsonStore(file, {}).value.a, 2);
  assert.equal(fs.statSync(file).mode & 0o777, 0o600);
  assert.equal(fs.readdirSync(home).length, 1);
  fs.writeFileSync(file, "{broken"); assert.throws(() => new JsonStore(file, {}));
  assert.equal(fs.readFileSync(file, "utf8"), "{broken");
});
test("status never exposes pairing credentials; unshared sessions are invisible", (t) => {
  const { e } = fixture(t), h = e.store.value.host, s = JSON.stringify(e.status());
  for (const value of [h.hostToken, h.clientToken, h.secret]) assert.equal(s.includes(value), false);
  assert.equal(e.shares().length, 1);
  const p = decodePair(e.pairCode().code); assert.equal(p.secret, h.secret);
});
test("list and session work but unknown operations and sessions fail closed", async (t) => {
  const { e, req } = fixture(t);
  const q = req("list"); assert.equal((await e.dispatch(q, q.id)).sessions.length, 1);
  const r = req("session"); assert.equal((await e.dispatch(r, r.id)).items[0].text, "你好");
  for (const patch of [{ op: "files" }, { op: "abortAll" }, { sessionId: "other" }, { sessionId: "../bad" }, { v: 2 }]) {
    const x = { ...req(), ...patch }; await assert.rejects(e.dispatch(x, x.id));
  }
});
test("mutations are at-most-once, including after reopening stored ledger", async (t) => {
  const { e, req, home, adapter, calls } = fixture(t), q = req();
  const a = await e.dispatch(q, q.id), b = await e.dispatch(q, q.id);
  assert.deepEqual(a, b); assert.equal(calls.length, 1);
  const reopened = new HostingEngine({ home, adapter });
  // A real restart changes boot and rejects all old write authority.
  await assert.rejects(reopened.dispatch(q, q.id), /变化/);
  reopened.boot = e.boot;
  assert.deepEqual(await reopened.dispatch(q, q.id), a); assert.equal(calls.length, 1);
});
test("old mutation IDs stay consumed beyond all client/upstream TTLs, even with renewed expiry", async (t) => {
  const { e, req, calls } = fixture(t);
  const first = req(); await e.dispatch(first, first.id);
  const clock = Date.now; Date.now = () => clock() + 601000;
  try {
    const second = req(); await e.dispatch(second, second.id);
    const retry = { ...first, expires: Date.now() + 30000 };
    assert.deepEqual(await e.dispatch(retry, retry.id), { ok: true, id: "run-a" });
    assert.equal(calls.length, 2);
    assert.equal(Object.keys(e.ledger.value.actions).length, 2);
    await assert.rejects(e.dispatch({ ...retry, args: { message: "different" } }, retry.id), /不同/);
    assert.equal(calls.length, 2);
  } finally { Date.now = clock; }
});
test("conflicting, replayed, expired and future requests are rejected", async (t) => {
  const { e, req, calls } = fixture(t), q = req();
  await e.dispatch(q, q.id);
  await assert.rejects(e.dispatch({ ...q, args: { message: "different" } }, q.id), /不同/);
  for (const patch of [{ boot: uid() }, { grant: uid() }, { expires: Date.now() - 1 }, { expires: Date.now() + 60000 }]) {
    const x = { ...req(), ...patch }; await assert.rejects(e.dispatch(x, x.id));
  }
  const x = req(); await assert.rejects(e.dispatch(x, uid()));
  assert.equal(calls.length, 1);
});
test("revocation and disabling immediately deny reads and writes", async (t) => {
  const { e, sid, req, calls } = fixture(t), old = req();
  e.share({ sessionId: sid, enabled: false });
  await assert.rejects(e.dispatch(old, old.id), /取消托管/);
  e.share({ sessionId: sid, enabled: true });
  await assert.rejects(e.dispatch(old, old.id), /变化/);
  e.toggle({ enabled: false });
  const q = req("list"); await assert.rejects(e.dispatch(q, q.id), /停用/);
  assert.equal(calls.length, 0);
});
test("pending preclaim prevents concurrent duplicate effects", async (t) => {
  const { e, req, adapter } = fixture(t); let release, n = 0;
  adapter.send = async () => { n++; await new Promise((r) => { release = r; }); return { ok: true }; };
  const q = req(), first = e.dispatch(q, q.id);
  await assert.rejects(e.dispatch(q, q.id), /可能已经执行/);
  release(); await first; assert.equal(n, 1);
});
test("a failed/uncertain operation is not executed again", async (t) => {
  const { e, req, adapter } = fixture(t); let n = 0;
  adapter.send = async () => { n++; throw new Error("upstream failed after effect"); };
  const q = req(); await assert.rejects(e.dispatch(q, q.id)); await assert.rejects(e.dispatch(q, q.id));
  assert.equal(n, 1);
});
test("storage failure prevents execution before effects", async (t) => {
  const { e, req, calls } = fixture(t);
  e.ledger.save = () => { throw new Error("disk full"); };
  const q = req(); await assert.rejects(e.dispatch(q, q.id), /disk full/); assert.equal(calls.length, 0);
});
test("read completion rechecks cancellation and never leaks canceled session", async (t) => {
  const { e, sid, req, adapter } = fixture(t); let release;
  adapter.session = async () => { await new Promise((r) => { release = r; }); return { text: "secret" }; };
  const q = req("session"), result = e.dispatch(q, q.id);
  e.share({ sessionId: sid, enabled: false }); release();
  await assert.rejects(result, /取消托管/);
});
test("pair/forget operations preserve private data and do not stop host", (t) => {
  const { e } = fixture(t), h = e.store.value.host;
  e.pair({ code: encodePair(h), name: "自己测试" });
  assert.equal(e.store.value.remotes.length, 1);
  assert.equal(JSON.stringify(e.status()).includes(h.secret), false);
  e.forget({ remoteId: e.store.value.remotes[0].id });
  assert.equal(e.store.value.remotes.length, 0); assert.equal(e.store.value.host.enabled, true);
});
