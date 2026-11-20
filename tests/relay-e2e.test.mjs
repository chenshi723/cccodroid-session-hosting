import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { HostingEngine, SYNC_LIMITS } from "../gateway/gw/hosting/engine.mjs";
import { ROOT, uid, token, request } from "../gateway/gw/hosting/core.mjs";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(check, timeout = 2000) {
  const deadline = Date.now() + timeout;
  while (!check()) { if (Date.now() >= deadline) throw new Error("local condition timed out"); await sleep(5); }
}
// Start the real relay application on an OS-assigned loopback port. The
// fixture owns the state directory and never loads another instance's config.
const relayFixture = String.raw`
import asyncio
import signal
from aiohttp import web
from server import create_app

async def main():
    stopped = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopped.set)
    runner = web.AppRunner(create_app(), access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        print(runner.addresses[0][1], flush=True)
        await stopped.wait()
    finally:
        await runner.cleanup()

asyncio.run(main())
`;

async function localRelay(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "hosting-sync-relay-"));
  const relayDir = fileURLToPath(new URL("../relay/", import.meta.url));
  const python = process.env.HOSTING_TEST_PYTHON ? path.resolve(process.env.HOSTING_TEST_PYTHON) : "python3";
  const enroll = token();
  const runtimeEnv = Object.fromEntries(Object.entries(process.env).filter(([k]) => /^(?:ANDROID_|LD_|TERMUX_)/.test(k) || k === "PATH"));
  const proc = spawn(python, ["-B", "-u", "-c", relayFixture], {
    cwd: relayDir, env: { ...runtimeEnv, HOME: home, TMPDIR: home, CC_HOST_STATE: path.join(home, "rooms.json"), CC_HOST_ENROLL_TOKEN: enroll },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const closed = once(proc, "close").catch((error) => error.code || "spawn failed");
  const lines = createInterface({ input: proc.stdout });
  let stderr = ""; proc.stderr.on("data", (c) => { stderr = (stderr + c).slice(-4000); });
  t.after(async () => {
    if (proc.pid && proc.exitCode == null && proc.signalCode == null) {
      proc.kill("SIGTERM");
      if (await Promise.race([closed, delay(1500, false, { ref: false })]) === false) proc.kill("SIGKILL");
    }
    await closed;
    lines.close();
    fs.rmSync(home, { recursive: true, force: true });
  });
  const line = await Promise.race([
    once(lines, "line", { signal: AbortSignal.timeout(5000) }).then(([line]) => line),
    closed.then((result) => { throw new Error(`isolated relay exited before binding (${result}): ${stderr}`); }),
  ]);
  assert.match(line, /^\d{1,5}$/);
  const port = Number(line); assert.ok(port > 0 && port <= 65535);
  const url = `http://127.0.0.1:${port}`;
  assert.equal((await request(url + ROOT + "/health", "", "GET", undefined, undefined, 1000)).protocol, 1);
  return { url, enroll, home };
}

test("encrypted local relay watch: waits, delta resume, responsive writes, disconnect ceiling, revocation", { timeout: 20000 }, async (t) => {
  const { url, enroll, home } = await localRelay(t), sid = "shared", privateSid = "private", listeners = new Map();
  let reads = 0;
  const effects = [], snapshots = new Map([[sid, { id: sid, cursor: 0, items: [{ role: "user", text: "history" }],
    live: [{ role: "assistant", text: "A" }], run: { id: "r1", idle: false }, perms: [], queue: [], interrupted: false }]]);
  const adapter = {
    localList: () => [...snapshots.keys()].map((id) => ({ id, title: id })), exists: (id) => assert.ok(snapshots.has(id)),
    summary: (id) => snapshots.has(id) ? { id, running: true } : null,
    session: async (id) => { reads++; return structuredClone(snapshots.get(id)); },
    subscribe: (id, fn) => { let set = listeners.get(id); if (!set) listeners.set(id, set = new Set()); set.add(fn); return () => set.delete(fn); },
    send: async (id, args, key) => { effects.push(["send", id, args, key]); return { ok: true }; },
    stop: async () => { effects.push(["stop"]); return { ok: true }; },
    answer: async () => { effects.push(["answer"]); return { ok: true }; },
  };
  const host = new HostingEngine({ home: path.join(home, "host"), adapter, test: true });
  const client = new HostingEngine({ home: path.join(home, "client"), adapter: {}, test: true });
  t.after(() => { client.stop(); host.stop(); });
  await host.createHost({ url, enrollToken: enroll }); host.share({ sessionId: sid, enabled: true });
  client.pair({ code: host.pairCode().code }); const remoteId = client.status().remotes[0].id;
  const list = await client.rpc({ remoteId, op: "list", id: uid() }); assert.equal(list.sync, 1);
  const auth = { sessionId: sid, boot: list.boot, grant: list.sessions[0].grant };
  const call = (op, args = {}, id = uid(), signal) => client.rpc({ remoteId, id, op, ...auth, args }, signal);
  const emit = () => { for (const fn of listeners.get(sid) || []) fn(); };
  const first = await call("watch"); assert.equal(first.reset, true); assert.equal(first.snapshot.live[0].text, "A");
  let done = false; const waiting = call("watch", { revision: first.revision }).then((r) => { done = true; return r; });
  await waitFor(() => host.syncWaits.size === 1); await sleep(30); assert.equal(done, false);
  const start = Date.now(), sendId = uid();
  await call("send", { message: "text" }, sendId); await call("stop", { runId: "r1" });
  await call("answer", { runId: "r1", id: "p", behavior: "allow" });
  assert.ok(Date.now() - start < 1000, "watch blocked mutation dispatch");
  assert.deepEqual(effects.map((e) => e[0]), ["send", "stop", "answer"]);
  client.outbound.get(remoteId + ":" + sendId).done = false;
  await call("send", { message: "text" }, sendId); assert.equal(effects.length, 3);
  snapshots.get(sid).live[0].text += "中文"; emit();
  const second = await waiting;
  assert.deepEqual(second.delta, { live: { text: { index: 0, from: 1, append: "中文" } } });
  snapshots.get(sid).live[0].text += "tail";
  const resumed = await call("watch", { revision: first.revision });
  assert.equal(resumed.delta.live.text.append, "中文tail"); assert.equal(resumed.snapshot, undefined);
  await assert.rejects(client.rpc({ remoteId, op: "watch", id: uid(), ...auth, sessionId: privateSid }), /没有开启/);
  await assert.rejects(call("watch", {}, uid(), AbortSignal.abort(new Error("already canceled"))), /already canceled/);

  // Relay has no cancel forwarding. The controller socket closes promptly; the host keeps only one bounded wait.
  const canceled = new AbortController(), begun = Date.now();
  const disconnected = assert.rejects(call("watch", { revision: resumed.revision }, uid(), canceled.signal));
  await waitFor(() => host.syncWaits.size === 1); const count = reads;
  canceled.abort(new Error("controller closed")); await disconnected;
  assert.equal(client.outbound.size, 3); // completed write ledger cache only
  assert.equal(host.syncWaits.size, 1);
  await call("stop", { runId: "r1" }); assert.equal(reads, count);
  await waitFor(() => host.syncWaits.size === 0 && host.hostReads.size === 0, SYNC_LIMITS.wait + 1000);
  assert.ok(Date.now() - begun <= SYNC_LIMITS.wait + 700); assert.equal(listeners.get(sid).size, 0);
  assert.equal(reads, count + 1); // only terminal heartbeat check, no polling snapshots

  const revoking = assert.rejects(call("watch", { revision: resumed.revision }), /取消托管|已取消/);
  await waitFor(() => host.syncWaits.size === 1); host.share({ sessionId: sid, enabled: false }); await revoking;
  assert.equal(host.sync.size, 0); assert.equal(listeners.get(sid).size, 0);
  host.share({ sessionId: sid, enabled: true }); await assert.rejects(call("watch"), /变化/);
  const newer = await client.rpc({ remoteId, id: uid(), op: "list" }); auth.grant = newer.sessions[0].grant;
  const reset = await call("watch", { revision: resumed.revision }); assert.equal(reset.reset, true);
  const stopping = new AbortController();
  const stopped = assert.rejects(call("watch", { revision: reset.revision }, uid(), stopping.signal));
  await waitFor(() => host.syncWaits.size === 1); host.stop(); await waitFor(() => !host.syncWaits.size && !host.hostReads.size);
  assert.equal(listeners.get(sid).size, 0); stopping.abort(); await stopped;
  const state = fs.readFileSync(path.join(home, "rooms.json"), "utf8");
  for (const value of ["中文", "history", host.store.value.host.secret, host.store.value.host.clientToken]) assert.equal(state.includes(value), false);
  await host.removeHost();
});
