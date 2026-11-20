import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { getEventListeners } from "node:events";
import { HostingEngine, SYNC_LIMITS } from "../gateway/gw/hosting/engine.mjs";
import { uid, token, seal } from "../gateway/gw/hosting/core.mjs";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
function fixture(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "hosting-sync-")), listeners = new Map();
  const states = new Map(), effects = [], sid = "session_A", sid2 = "session_B";
  for (const id of [sid, sid2]) states.set(id, { id, cursor: 0, items: [{ role: "assistant", text: "history" }],
    live: [{ role: "user", text: "question" }, { role: "assistant", text: "A" }],
    run: { id: "run-" + id, idle: false }, perms: [], queue: [], interrupted: false });
  let snapshots = 0;
  const adapter = {
    localList: () => [...states.keys()].map((id) => ({ id, title: id })),
    exists: (id) => assert.ok(states.has(id)), summary: (id) => states.has(id) ? { id, running: true } : null,
    session: async (id) => { snapshots++; return structuredClone(states.get(id)); },
    subscribe: (id, fn) => { let set = listeners.get(id); if (!set) listeners.set(id, set = new Set()); set.add(fn); return () => set.delete(fn); },
    send: async (...args) => { effects.push(["send", ...args]); return { ok: true }; },
    stop: async (...args) => { effects.push(["stop", ...args]); return { ok: true }; },
    answer: async (...args) => { effects.push(["answer", ...args]); return { ok: true }; },
  };
  const e = new HostingEngine({ home, adapter, test: true });
  const shares = Object.fromEntries([...states.keys()].map((id) => [id, { grant: uid(), title: id }]));
  e.store.save({ version: 1, remotes: [], host: { url: "http://127.0.0.1:1", room: uid(), hostToken: token(),
    clientToken: token(), secret: token(), name: "test", enabled: true, shares } });
  const req = (op = "watch", args = {}, id = sid) => ({ v: 1, id: uid(), expires: Date.now() + 30000, op,
    sessionId: id, boot: e.boot, grant: shares[id].grant, args });
  const call = (args = {}, id = sid, context) => { const q = req("watch", args, id); return e.dispatch(q, q.id, context); };
  const emit = (id = sid) => { for (const fn of listeners.get(id) || []) fn(); };
  t.after(() => { e.stop(); fs.rmSync(home, { recursive: true, force: true }); });
  return { e, adapter, req, call, emit, states, sid, sid2, listeners, effects, count: () => snapshots };
}
function apply(snapshot, reply) {
  if (reply.reset) return structuredClone(reply.snapshot);
  const out = structuredClone(snapshot);
  for (const [field, patch] of Object.entries(reply.delta || {})) {
    if (!["items", "live"].includes(field)) { out[field] = patch; continue; }
    if (patch.text) {
      const { index, from, append } = patch.text;
      assert.equal(out[field][index].text.length, from);
      out[field][index].text += append;
      if (patch.append) out[field].push(...patch.append);
    } else out[field].splice(patch.from, patch.remove, ...patch.items);
  }
  return out;
}

test("list advertises sync; initial and lost revisions reset exact full snapshot shape", async (t) => {
  const f = fixture(t), q = f.req("list");
  assert.equal((await f.e.dispatch(q, q.id)).sync, 1);
  const first = await f.call();
  assert.equal(first.reset, true); assert.match(first.revision, /^[a-f0-9]{32}$/);
  assert.deepEqual(first.snapshot, f.states.get(f.sid));
  assert.deepEqual(Object.keys(first.snapshot), ["id", "cursor", "items", "live", "run", "perms", "queue", "interrupted"]);
  const lost = await f.call({ revision: "not-in-history" });
  assert.equal(lost.reset, true); assert.deepEqual(lost.snapshot, first.snapshot);
  assert.equal(f.listeners.get(f.sid).size, 0);
});

test("optional info has its own revision and metadata-only changes do not resend history or live", async (t) => {
  const f = fixture(t), state = f.states.get(f.sid); f.adapter.infoVersion = 1;
  const q = f.req("list"); assert.equal((await f.e.dispatch(q, q.id)).info, 1);
  state.info = { context: { used: 205000, total: 500000, compactLine: 396000 },
    model: { providerLabel: "host", name: "model", effort: "high" }, permissionMode: "plan",
    stat: { active: false, startedAt: 1000, elapsedMs: 2000, out: 0, costUsd: null },
    quota: { todayTokens: 60 }, sampledAt: 3000 };
  const first = await f.call(); assert.deepEqual(first.snapshot.info, state.info);
  const saved = structuredClone(first.snapshot.info), waiting = f.call({ revision: first.revision });
  await sleep(10); state.info.stat.out = 42; state.info.sampledAt = 4000; f.emit();
  const update = await waiting;
  assert.equal(update.reset, false); assert.deepEqual(Object.keys(update.delta), ["info"]);
  assert.deepEqual(update.delta.info, state.info); assert.deepEqual(first.snapshot.info, saved);
  assert.deepEqual(apply(first.snapshot, update), state);
  const signal = new AbortController(), unchanged = assert.rejects(f.call({ revision: update.revision }, f.sid, { signal: signal.signal }), /cancel/);
  await sleep(10); f.emit(); await sleep(SYNC_LIMITS.coalesce + 20);
  assert.equal(f.e.syncWaits.size, 1, "identical metadata must remain in long poll");
  signal.abort(new Error("cancel")); await unchanged;
});

test("reset reanchors elapsed time on the host without changing canonical info or revision", async (t) => {
  const f = fixture(t), state = f.states.get(f.sid), sampledAt = Date.now() - 10000;
  state.info = { stat: { active: true, startedAt: sampledAt - 2000, elapsedMs: 2000, out: 3, costUsd: null }, sampledAt };
  const first = await f.call();
  assert.ok(first.snapshot.info.stat.elapsedMs >= 12000);
  assert.equal(state.info.stat.elapsedMs, 2000); assert.equal(state.info.sampledAt, sampledAt);
  const canonical = [...f.e.sync.values()][0].current.snapshot.info;
  assert.equal(canonical.stat.elapsedMs, 2000); assert.equal(canonical.sampledAt, sampledAt);
  const second = await f.call({ revision: "lost-revision" });
  assert.equal(second.revision, first.revision);
  assert.ok(second.snapshot.info.stat.elapsedMs >= first.snapshot.info.stat.elapsedMs);
  state.info.stat.active = false;
  const idle = await f.call({ revision: first.revision });
  assert.equal(idle.delta.info.stat.elapsedMs, 2000); assert.equal(idle.delta.info.sampledAt, sampledAt);
});

test("watch boot, grant, share, envelope, operation and TTL checks fail closed", async (t) => {
  const f = fixture(t);
  for (const patch of [{ boot: undefined }, { grant: undefined }, { boot: uid() }, { grant: uid() },
    { sessionId: "private" }, { expires: Date.now() - 1 }, { expires: Date.now() + 46000 }, { op: "events" }]) {
    const q = { ...f.req(), ...patch };
    await assert.rejects(f.e.dispatch(q, q.id));
  }
  for (const args of [{ revision: {} }, { revision: "x".repeat(129) }, { before: 10 }]) await assert.rejects(f.call(args));
  assert.equal(f.e.syncWaits.size, 0);
});

test("token changes coalesce and transfer only text suffix; unrelated session never wakes", async (t) => {
  const f = fixture(t), first = await f.call(), before = f.count(), start = Date.now();
  let done = false;
  const waiting = f.call({ revision: first.revision }).then((r) => { done = true; return r; });
  await sleep(10);
  for (let i = 0; i < 10; i++) f.emit(f.sid2);
  await sleep(20); assert.equal(done, false); assert.equal(f.count(), before + 1);
  f.states.get(f.sid).live[1].text += "BC"; f.emit();
  await sleep(20); f.states.get(f.sid).live[1].text += "DE"; f.emit();
  const result = await waiting;
  assert.ok(Date.now() - start >= 95); assert.equal(result.reset, false); assert.equal(result.snapshot, undefined);
  assert.deepEqual(result.delta, { live: { text: { index: 1, from: 1, append: "BCDE" } } });
  assert.equal(f.count(), before + 2); assert.deepEqual(apply(first.snapshot, result), f.states.get(f.sid));
  assert.equal(f.listeners.get(f.sid).size, 0);
});

test("resume from earlier revision gives aggregate text tail and new tool metadata without full payload", async (t) => {
  const f = fixture(t), first = await f.call();
  f.states.get(f.sid).live[1].text += "BC";
  const second = await f.call({ revision: first.revision });
  f.states.get(f.sid).live.push({ role: "tool_use", name: "Read", id: "tool", input: { file_path: "/tmp/x" } });
  Object.assign(f.states.get(f.sid), { perms: [{ id: "p", tool: "Read" }], queue: [{ id: "q", held: true }], interrupted: true });
  const third = await f.call({ revision: second.revision });
  assert.deepEqual(third.delta.live, { from: 2, remove: 0, items: f.states.get(f.sid).live.slice(2) });
  const resumed = await f.call({ revision: first.revision });
  assert.equal(resumed.reset, false); assert.equal(resumed.delta.items, undefined);
  assert.equal(resumed.delta.live.text.append, "BC"); assert.equal(resumed.delta.live.append[0].id, "tool");
  assert.deepEqual(apply(first.snapshot, resumed), f.states.get(f.sid));
  assert.ok(JSON.stringify(resumed).length < 500);
});

test("last-item edits use suffix splice; incompatible non-tail edit, cursor, run transition reset", async (t) => {
  const f = fixture(t); let prev = await f.call();
  f.states.get(f.sid).live[1].text = "replacement";
  let next = await f.call({ revision: prev.revision });
  assert.deepEqual(next.delta.live, { from: 1, remove: 1, items: [{ role: "assistant", text: "replacement" }] });
  for (const mutate of [
    (s) => { s.live[0].text = "changed question"; },
    (s) => { s.cursor = 123; },
    (s) => { s.run = null; s.live = []; },
  ]) {
    prev = next; mutate(f.states.get(f.sid)); next = await f.call({ revision: prev.revision });
    assert.equal(next.reset, true); assert.equal(next.delta, undefined);
  }
});

test("unchanged heartbeat really waits 8s without rebuilding on timer ticks; no listeners left", async (t) => {
  const f = fixture(t), stalled = fixture(t);
  stalled.adapter.session = () => new Promise(() => {});
  const stallBegin = Date.now();
  const stalledDone = assert.rejects(stalled.call(), /超时/).then(() => {
    assert.ok(Date.now() - stallBegin >= SYNC_LIMITS.wait - 25);
    assert.equal(stalled.e.syncWaits.size, 0); assert.equal(stalled.e.sync.size, 0);
    assert.equal(stalled.listeners.get(stalled.sid).size, 0);
  });
  const first = await f.call(), count = f.count(), begin = Date.now();
  const signal = new AbortController();
  const heartbeat = await f.call({ revision: first.revision }, f.sid, { signal: signal.signal });
  const elapsed = Date.now() - begin;
  assert.ok(elapsed >= SYNC_LIMITS.wait - 80 && elapsed < SYNC_LIMITS.wait + 1000, elapsed);
  assert.deepEqual(heartbeat, { sync: 1, revision: first.revision, reset: false, unchanged: true });
  assert.equal(f.count(), count + 2); // entry check + terminal file check, never periodic snapshots
  assert.equal(f.listeners.get(f.sid).size, 0); assert.equal(f.e.syncWaits.size, 0);
  assert.equal(getEventListeners(signal.signal, "abort").length, 0);
  await stalledDone;
});

test("subscribe-before-snapshot preserves events during async disk read", async (t) => {
  const f = fixture(t), first = await f.call(); let release;
  f.adapter.session = async (id) => {
    const old = structuredClone(f.states.get(id));
    await new Promise((r) => { release = r; }); return old;
  };
  const waiting = f.call({ revision: first.revision });
  assert.equal(f.listeners.get(f.sid).size, 1);
  f.states.get(f.sid).live[1].text += "raced"; f.emit();
  f.adapter.session = async (id) => structuredClone(f.states.get(id)); release();
  const result = await waiting;
  assert.equal(result.delta.live.text.append, "raced");
});

test("watch revocation, engine stop, epoch and boot changes cancel or reject read after wait", async (t) => {
  for (const act of [
    (f) => f.e.share({ sessionId: f.sid, enabled: false }),
    (f) => f.e.stop(),
    (f) => { f.e.epoch++; f.emit(); },
    (f) => { f.e.boot = uid(); f.emit(); },
  ]) {
    const f = fixture(t), first = await f.call(), start = Date.now();
    const outcome = assert.rejects(f.call({ revision: first.revision }), /取消|变化/);
    await sleep(5); act(f); await outcome;
    assert.ok(Date.now() - start < 300); assert.equal(f.listeners.get(f.sid).size, 0); assert.equal(f.e.syncWaits.size, 0);
  }
});

test("late snapshot after revocation cannot return secret; cancellation also bounds stalled snapshots", async (t) => {
  const f = fixture(t); let release;
  f.adapter.session = () => new Promise((r) => { release = r; });
  const result = assert.rejects(f.call(), /取消/); f.e.share({ sessionId: f.sid, enabled: false }); await result;
  release(f.states.get(f.sid)); await sleep(1);
  assert.equal(f.e.sync.size, 0); assert.equal(f.listeners.get(f.sid).size, 0);
  f.e.share({ sessionId: f.sid, enabled: true });
  const q = f.req(); q.grant = f.e.store.value.host.shares[f.sid].grant; q.expires = Date.now() + 40;
  await assert.rejects(f.e.dispatch(q, q.id), /过期/);
  release(f.states.get(f.sid)); await sleep(1); assert.equal(f.e.syncWaits.size, 0);
});

test("bounded concurrent waits reject excess and do not block send stop answer; writes serialize", async (t) => {
  const f = fixture(t), first = await f.call(), controllers = [new AbortController(), new AbortController()];
  const waits = controllers.map((c) => assert.rejects(f.call({ revision: first.revision }, f.sid, { signal: c.signal }), /cancel/));
  await sleep(5); await assert.rejects(f.call({ revision: first.revision }), /过多/);
  const start = Date.now();
  for (const op of ["send", "stop", "answer"]) { const q = f.req(op); await f.e.dispatch(q, q.id); }
  assert.ok(Date.now() - start < 100); assert.equal(f.effects.length, 3);
  for (const c of controllers) c.abort(new Error("cancel")); await Promise.all(waits);
  let release; const seen = [];
  f.adapter.send = async () => { seen.push("start"); await new Promise((r) => { release = r; }); seen.push("end"); return { ok: true }; };
  f.adapter.stop = async () => { seen.push("stop"); return { ok: true }; };
  const a = f.req("send"), b = f.req("stop"), pending = f.e.dispatch(a, a.id), queued = f.e.dispatch(b, b.id);
  await assert.rejects(f.e.dispatch(a, a.id), /可能已经执行/);
  assert.deepEqual(seen, ["start"]); release(); await Promise.all([pending, queued]);
  assert.deepEqual(seen, ["start", "end", "stop"]);
  assert.equal(f.e.ledger.value.actions[f.e.store.value.host.room + ":" + a.id].state, "done");
});

test("global wait cap reserves capacity for writes and cancellation removes every listener", async (t) => {
  const f = fixture(t), h = f.e.store.value.host, ids = [f.sid, f.sid2];
  for (let n = 0; n < 3; n++) {
    const id = "cap_" + n; ids.push(id); f.states.set(id, { ...structuredClone(f.states.get(f.sid)), id });
    h.shares[id] = { grant: uid(), title: id };
  }
  const initial = [];
  for (const id of ids) { const q = f.req(); q.sessionId = id; q.grant = h.shares[id].grant; initial.push(await f.e.dispatch(q, q.id)); }
  const controllers = [], waits = [];
  for (let n = 0; n < SYNC_LIMITS.waits; n++) {
    const id = ids[Math.floor(n / 2)], c = new AbortController(), q = f.req("watch", { revision: initial[Math.floor(n / 2)].revision });
    q.sessionId = id; q.grant = h.shares[id].grant;
    controllers.push(c); waits.push(assert.rejects(f.e.dispatch(q, q.id, { signal: c.signal }), /stop cap/));
  }
  assert.equal(f.e.syncWaits.size, SYNC_LIMITS.waits);
  const excess = f.req(); excess.sessionId = ids.at(-1); excess.grant = h.shares[excess.sessionId].grant;
  await assert.rejects(f.e.dispatch(excess, excess.id), /过多/);
  const write = f.req("send"); await f.e.dispatch(write, write.id); assert.equal(f.effects.length, 1);
  for (const c of controllers) c.abort(new Error("stop cap")); await Promise.all(waits);
  assert.equal(f.e.syncWaits.size, 0); for (const set of f.listeners.values()) assert.equal(set.size, 0);
});

test("two watches share a snapshot read; aborting one does not poison the other", async (t) => {
  const f = fixture(t), initial = await f.call(); let release;
  f.adapter.session = () => new Promise((r) => { release = r; });
  const c = new AbortController(), one = assert.rejects(f.call({ revision: initial.revision }, f.sid, { signal: c.signal }), /cancel first/);
  const two = f.call({ revision: initial.revision });
  c.abort(new Error("cancel first")); await one;
  const snapshot = structuredClone(f.states.get(f.sid)); snapshot.live[1].text += "second survived";
  release(snapshot); const reply = await two;
  assert.equal(reply.delta.live.text.append, "second survived"); assert.equal(f.e.syncWaits.size, 0);
});

test("queued writes recheck revoke, TTL and stopped epoch instead of applying stale authority", async (t) => {
  for (const mode of ["revoke", "expire", "stop"]) {
    const f = fixture(t); let release;
    f.adapter.send = () => new Promise((r) => { release = r; });
    const a = f.req("send"), pending = f.e.dispatch(a, a.id), b = f.req("stop");
    if (mode === "expire") b.expires = Date.now() + 15;
    const queued = assert.rejects(f.e.dispatch(b, b.id), /取消托管|过期|取消/);
    if (mode === "revoke") f.e.share({ sessionId: f.sid, enabled: false });
    else if (mode === "stop") f.e.stop();
    else await sleep(25);
    release({ ok: true }); await pending; await queued; assert.equal(f.effects.length, 0);
  }
});

test("revision scope and bounded revision history reset safely after eviction/regrant", async (t) => {
  const f = fixture(t), initial = await f.call(); let last = initial;
  for (let n = 0; n < SYNC_LIMITS.revisions + 2; n++) {
    f.states.get(f.sid).live[1].text += "X";
    last = await f.call({ revision: last.revision });
  }
  assert.equal([...f.e.sync.values()][0].records.length, SYNC_LIMITS.revisions);
  assert.equal((await f.call({ revision: initial.revision })).reset, true);
  assert.equal((await f.call({ revision: last.revision }, f.sid2)).reset, true);
  f.e.share({ sessionId: f.sid, enabled: false }); f.e.share({ sessionId: f.sid, enabled: true });
  const q = f.req("watch", { revision: last.revision }); q.grant = f.e.store.value.host.shares[f.sid].grant;
  assert.equal((await f.e.dispatch(q, q.id)).reset, true);
  const clock = Date.now; Date.now = () => clock() + SYNC_LIMITS.ttl + 1;
  try { assert.equal((await f.call({}, f.sid2)).reset, true); } finally { Date.now = clock; }
});

test("snapshot, cache bytes, sessions and revision count have hard caps; seal fits relay budget", async (t) => {
  const f = fixture(t);
  f.states.get(f.sid).live[1].text = "汉".repeat(500000);
  await assert.rejects(f.call(), /过大/); assert.equal(f.e.syncWaits.size, 0);
  f.states.get(f.sid).live[1].text = "汉".repeat(16000);
  const first = await f.call(); assert.doesNotThrow(() => seal(token(), uid(), uid(), "response", { result: first }));
  f.states.get(f.sid).items = [{ role: "assistant", text: "a".repeat(1100000) }];
  for (let n = 0; n < 20; n++) { f.states.get(f.sid).live[1].text = String(n); await f.call(); }
  const sum = [...f.e.sync.values()].reduce((n, e) => n + e.current.bytes + e.records.reduce((m, r) => m + r.bytes, 0), 0);
  assert.ok(sum <= SYNC_LIMITS.bytes); assert.ok([...f.e.sync.values()][0].records.length < SYNC_LIMITS.revisions);
  const h = f.e.store.value.host;
  for (let n = 0; n < 20; n++) {
    const id = "extra_" + n; f.states.set(id, { ...structuredClone(f.states.get(f.sid2)), id }); h.shares[id] = { grant: uid(), title: id };
    const q = f.req("watch", {}, f.sid); q.sessionId = id; q.grant = h.shares[id].grant; await f.e.dispatch(q, q.id);
  }
  assert.equal(f.e.sync.size, SYNC_LIMITS.entries);
});
