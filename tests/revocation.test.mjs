import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { HostingEngine } from "../gateway/gw/hosting/engine.mjs";
import { ROOT, token, uid } from "../gateway/gw/hosting/core.mjs";

const url = "https://hosting.invalid";
const json = (data, status = 200) => new Response(JSON.stringify(data), { status });
const deferred = () => Promise.withResolvers();
function fixture(t, adapter = {}) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "hosting-revoke-"));
  const engines = [], open = () => { const e = new HostingEngine({ home, adapter: { summary: () => null, ...adapter } }); engines.push(e); return e; };
  t.after(() => { for (const e of engines) e.stop(); fs.rmSync(home, { recursive: true, force: true }); });
  const e = open(), sid = "sample", grant = uid();
  const host = { url, room: uid(), hostToken: token(), clientToken: token(), secret: token(), enabled: true, name: "test", shares: { [sid]: { grant, title: "test" } } };
  e.store.save({ ...e.store.value, host });
  const request = (op, args = {}, s = sid) => ({ v: 1, id: uid(), op, args, sessionId: s, grant: host.shares[s]?.grant, boot: e.boot, expires: Date.now() + 30000 });
  return { e, open, host, sid, grant, request };
}
const run = (e, q) => e.dispatch(q, q.id);
function deletionFetch(t, host, reply) {
  return t.mock.method(globalThis, "fetch", (where, init) => {
    assert.equal(where, url + ROOT + "/rooms/" + host.room);
    assert.equal(init.method, "DELETE");
    assert.equal(init.headers.Authorization, "Bearer " + host.hostToken);
    return reply();
  });
}

test("stop and durable revoking state precede DELETE; legacy success is sufficient", async (t) => {
  const { e, open, sid, host, request } = fixture(t, { summary: (id) => ({ id }) });
  const gate = deferred();
  const mock = deletionFetch(t, host, () => gate.promise);
  const controller = e.controller = new AbortController();
  const removal = e.removeHost({ room: host.room });
  assert.equal(controller.signal.aborted, true);
  assert.equal(open().store.value.host.removal, "pending");
  assert.equal(e.status().host.removal, "working");
  await assert.rejects(run(e, request("list")), /停用/);
  assert.throws(() => e.toggle({ enabled: true }), /变更/);
  assert.throws(() => e.pairCode(), /正在解除/);
  assert.throws(() => e.clearHostLocally({ room: host.room, confirm: true }), /变更/);
  e.revokeSession(sid); // local lifecycle changes while waiting must not be overwritten
  e.pair({ code: "CCH1." + Buffer.from(JSON.stringify({ v: 1, url, room: host.room, clientToken: host.clientToken, secret: host.secret })).toString("base64url") });
  gate.resolve(json({ ok: true }));
  const result = await removal;
  assert.equal(result.removalResult, "revoked");
  assert.equal(open().store.value.host, null);
  assert.equal(result.remotes.length, 1);
  assert.equal(mock.mock.callCount(), 1);
});

test("transport failure and old-relay 401 survive restart without showing confirmation; fallback is explicit and local", async (t) => {
  const { e, open, sid, host } = fixture(t);
  let attempts = 0;
  const mock = deletionFetch(t, host, () => {
    if (++attempts === 1) return Promise.reject(new Error("transport lost acknowledgement"));
    return json({ ok: true, error: "unauthorized" }, 401); // status is not proof, regardless of body
  });
  assert.throws(() => e.clearHostLocally({ room: host.room, confirm: true }), /只允许/);
  e.revokeSession(sid);
  await assert.rejects(e.removeHost(), /无法确认/);
  const restarted = open(); restarted.start();
  assert.equal(restarted.status().host.removal, "failed");
  assert.equal(restarted.controller, null);
  assert.throws(() => restarted.toggle({ enabled: true }), /不能重新启用/);
  assert.throws(() => restarted.pairCode(), /不能再显示/);
  assert.throws(() => restarted.share({ sessionId: sid, enabled: true }), /不能开启/);
  await assert.rejects(restarted.createHost({}), /已有主机/);
  await assert.rejects(restarted.removeHost(), /无法确认/);
  assert.equal(restarted.status().host.removal, "failed");
  assert.throws(() => restarted.clearHostLocally({ room: host.room, confirm: "true" }), /明确确认/);
  assert.throws(() => restarted.clearHostLocally({ room: uid(), confirm: true }), /当前主机/);
  const out = restarted.clearHostLocally({ room: host.room, confirm: true });
  assert.equal(out.removalResult, "local-only");
  assert.equal(out.host, null);
  assert.equal(open().store.value.host, null);
  assert.equal((await restarted.removeHost()).removalResult, "no-host");
  assert.equal(mock.mock.callCount(), 2);
  mock.mock.restore();
  const fresh = { room: uid(), hostToken: token(), clientToken: token() };
  t.mock.method(globalThis, "fetch", (where, init) => {
    assert.equal(where, url + ROOT + "/rooms");
    assert.equal(init.method, "POST");
    return json(fresh);
  });
  restarted.start = () => {}; // do not open a background connection from this test
  await restarted.createHost({ url, enrollToken: token() });
  assert.equal(open().store.value.host.room, fresh.room);
  assert.deepEqual(open().store.value.host.shares, {});
});

test("a paused host or unknown/malformed success cannot bypass the removal gates", async (t) => {
  const { e, host, open } = fixture(t);
  e.toggle({ enabled: false });
  assert.throws(() => e.clearHostLocally({ room: host.room, confirm: true }), /只允许/);
  let calls = 0;
  const mock = deletionFetch(t, host, () => ++calls === 1 ? json({ health: "up" }) : json({ ok: true }, 202));
  await assert.rejects(e.removeHost({ room: uid() }), /配置已变化/);
  assert.equal(mock.mock.callCount(), 0);
  await assert.rejects(e.removeHost(), /无法确认/);
  await assert.rejects(e.removeHost(), /无法确认/);
  assert.equal(open().status().host.removal, "failed");
});

test("a failed durable stop never sends DELETE; retry recovers", async (t) => {
  const { e, open, host } = fixture(t);
  const fetch = deletionFetch(t, host, () => json({ ok: true }));
  const original = e.store.save.bind(e.store);
  e.store.save = () => { throw new Error("write failed"); };
  await assert.rejects(e.removeHost(), /未能保存停用状态/);
  assert.equal(e.status().host.enabled, false);
  assert.equal(e.status().host.removal, "save-failed");
  assert.equal(open().store.value.host.enabled, true); // never claim this stop reached disk
  assert.equal(fetch.mock.callCount(), 0);
  e.store.save = original;
  assert.equal((await e.removeHost()).removalResult, "revoked");
  assert.equal(fetch.mock.callCount(), 1);
});

test("confirmed relay success survives a local cleanup failure without issuing a second DELETE", async (t) => {
  const { e, open, host } = fixture(t);
  const fetch = deletionFetch(t, host, () => json({ ok: true }));
  const original = e.store.save.bind(e.store);
  e.store.save = (next) => { if (!next.host) throw new Error("write failed"); original(next); };
  await assert.rejects(e.removeHost(), /中转已确认吊销.*未能清除/);
  const recovered = open();
  assert.equal(recovered.status().host.removal, "confirmed");
  assert.equal((await recovered.removeHost()).removalResult, "revoked");
  assert.equal(fetch.mock.callCount(), 1);
});

test("definite session revocation aborts reads, drops revisions and rejects stale write commits", async (t) => {
  const slowRead = deferred(), lettingWriteCommit = deferred();
  let effects = 0, slow = false, subscribers = 0;
  const adapter = {
    summary: id => ({ id }), exists: () => true, localList: () => [],
    subscribe: () => { subscribers++; return () => subscribers--; },
    session: (id) => slow ? slowRead.promise : Promise.resolve({ id, items: [{ role: "assistant", text: "fixture" }] }),
    send: async (sid, args, id, { check }) => { await lettingWriteCommit.promise; check(); effects++; },
    stop: async () => { effects++; },
  };
  const { e, open, sid, grant, request } = fixture(t, adapter);
  const first = await run(e, request("watch"));
  slow = true;
  const oldPage = request("session"), read = assert.rejects(run(e, oldPage), /取消托管/);
  const watch = assert.rejects(run(e, request("watch", { revision: first.revision })), /取消托管|取消/);
  const send = assert.rejects(run(e, request("send")), /取消托管/);
  const queued = assert.rejects(run(e, request("stop")), /变化|取消托管/);
  assert.equal(subscribers, 1);
  e.revokeSession(sid);
  await Promise.all([read, watch]);
  assert.equal(e.sync.size, 0);
  assert.equal(e.sessionReads.size, 0);
  assert.equal(subscribers, 0);
  const restarted = open();
  assert.deepEqual(restarted.status().host.shares, []);
  await assert.rejects(run(restarted, { ...oldPage, boot: restarted.boot }), /取消托管/);
  assert.notEqual(e.share({ sessionId: sid, enabled: true }).host.shares[0].grant, grant);
  await assert.rejects(run(e, oldPage), /变化/);
  lettingWriteCommit.resolve();
  slowRead.resolve({ id: sid, items: [{ role: "assistant", text: "late fixture" }] });
  await Promise.all([send, queued]);
  assert.equal(effects, 0);
});

test("session revocation cancels a still-pending successful reply upload", async (t) => {
  const { e, host, sid, request } = fixture(t, { session: async id => ({ id, items: [] }) });
  const uploading = deferred();
  t.mock.method(globalThis, "fetch", (where, init) => {
    assert.equal(where, url + ROOT + "/rooms/" + host.room + "/reply");
    const sent = new Promise((_, reject) => init.signal.addEventListener("abort", () => reject(init.signal.reason), { once: true }));
    uploading.resolve(init.signal);
    return sent;
  });
  const q = request("session");
  const done = e.reply(host, q, q.id, new AbortController(), e.epoch);
  const signal = await uploading.promise;
  e.revokeSession(sid);
  await done;
  assert.equal(signal.aborted, true);
  assert.equal(e.sessionReads.size, 0);
});

test("missing and uncertain shares are local-only and removable; definite evidence frees quota", async (t) => {
  const states = new Map(), adapter = {
    trashState: (id) => states.get(id) || "clear", exists: (id) => !id.startsWith("old_"),
    summary: (id) => id.startsWith("old_") ? null : { id }, localList: () => [],
  };
  const { e, open, sid, grant, request } = fixture(t, adapter);
  const h = e.store.value.host;
  for (let i = 1; i < 32; i++) h.shares["old_" + i] = { title: "old " + i, grant: uid() };
  e.store.save({ ...e.store.value, host: h });
  states.set(sid, "unavailable");
  const out = e.status().host.shares;
  assert.equal(out.length, 32);
  assert.ok(out.every((s) => s.unavailable && !s.grant));
  assert.equal(open().store.value.host.shares[sid].grant, grant);
  assert.deepEqual((await run(e, request("list"))).sessions, []);
  await assert.rejects(run(e, request("session")), /暂不可用/);
  assert.throws(() => e.share({ sessionId: "next", enabled: true }), /32/);
  states.set(sid, "clear");
  assert.equal((await run(e, request("list"))).sessions[0].grant, grant);
  e.share({ sessionId: "old_1", enabled: false });
  states.set("old_2", "deleted");
  e.status();
  assert.equal(open().store.value.host.shares.old_2, undefined);
  e.share({ sessionId: "next", enabled: true });
  assert.equal(e.status().host.shares.length, 31);
});
