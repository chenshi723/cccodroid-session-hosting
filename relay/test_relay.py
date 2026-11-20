"""Run with: python -m unittest discover -s <relay directory> -v"""

import asyncio
import base64
import hashlib
import io
import json
from pathlib import Path
import secrets
import stat
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, ClientTimeout
from aiohttp.test_utils import TestClient, TestServer

from server import Config, MAX_BODY, MAX_PACKET, MAX_REVOCATIONS, PREFIX, RELAY_KEY, StateStore, create_app, digest


def auth(token):
    return {"Authorization": "Bearer " + token}


def packet(size=48):
    return base64.urlsafe_b64encode(secrets.token_bytes(size)).rstrip(b"=").decode()


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "rooms.json"
        self.store = StateStore(self.path)
        self.records = {secrets.token_hex(16): {"hostSha256": "a" * 64, "clientSha256": "b" * 64}}

    def tearDown(self):
        self.temp.cleanup()

    def test_atomic_private_roundtrip(self):
        self.assertEqual(self.store.load(), {})
        self.store.save(self.records)
        self.assertEqual(self.store.load(), self.records)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(list(self.path.parent.glob(".rooms-*")), [])

    def test_load_repairs_mode(self):
        self.store.save(self.records)
        self.path.chmod(0o644)
        self.store.load()
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    def test_failed_replace_keeps_previous_state_and_no_temp(self):
        self.store.save(self.records)
        with patch("server.os.replace", side_effect=OSError("write failed")):
            with self.assertRaises(OSError):
                self.store.save({})
        self.assertEqual(self.store.load(), self.records)
        self.assertEqual(list(self.path.parent.glob(".rooms-*")), [])

    def test_corrupt_state_fails_closed(self):
        examples = ["bad", "null", '{"v":1,"rooms":[]}', '{"v":2,"rooms":{}}',
                    '{"v":1,"v":1,"rooms":{}}', '{"v":1,"rooms":{"bad":{}}}']
        for text in examples:
            with self.subTest(text=text):
                self.path.write_text(text)
                with self.assertRaises(ValueError):
                    self.store.load()

    def test_raw_or_equal_tokens_rejected(self):
        for record in [dict(hostSha256="raw", clientSha256="x" * 43),
                       dict(hostSha256="a" * 64, clientSha256="a" * 64)]:
            self.store.save({secrets.token_hex(16): record})
            with self.assertRaises(ValueError):
                self.store.load()

    def test_symlink_state_rejected(self):
        actual = self.path.parent / "actual.json"
        actual.write_text('{"v":1,"rooms":{}}')
        self.path.symlink_to(actual)
        with self.assertRaises(OSError):
            self.store.load()

    def test_config_validates_secrets_paths_and_limits(self):
        args = dict(enroll_token=secrets.token_urlsafe(32), state_path=self.path)
        for replacement in [dict(enroll_token="short"), dict(state_path="rooms.json"),
                            dict(max_rooms=33), dict(room_pending=17), dict(global_pending=65),
                            dict(poll_timeout=21), dict(rpc_timeout=26), dict(pending_bytes=0)]:
            with self.subTest(replacement=replacement):
                with self.assertRaises(ValueError):
                    Config(**(args | replacement))


class RelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state_path = Path(self.temp.name) / "rooms.json"
        self.enroll = secrets.token_urlsafe(32)
        self.config = Config(self.enroll, self.state_path, poll_timeout=0.12, rpc_timeout=0.7)
        self.app = create_app(self.config)
        self.relay = self.app[RELAY_KEY]
        self.server = TestServer(self.app)
        self.client = TestClient(self.server)
        await self.client.start_server()
        self.background = []

    async def asyncTearDown(self):
        for task in self.background:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.background, return_exceptions=True)
        await self.client.close()
        self.temp.cleanup()

    async def request(self, method, url, token=None, **kwargs):
        response = await self.client.request(method, url, headers=auth(token) if token else {}, **kwargs)
        data = await response.json()
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.content_type, "application/json")
        return response.status, data

    async def room(self):
        status, data = await self.request("POST", PREFIX + "/rooms", self.enroll, json={})
        self.assertEqual(status, 200)
        return data

    def url(self, room, operation=""):
        return PREFIX + "/rooms/" + room["room"] + ("/" + operation if operation else "")

    async def poll(self, room):
        return await self.request("GET", self.url(room, "poll"), room["hostToken"])

    async def rpc(self, room, request_id=None, payload=None):
        request_id = request_id or secrets.token_hex(16)
        return await self.request("POST", self.url(room, "rpc"), room["clientToken"],
                                  json={"id": request_id, "packet": payload or packet()})

    async def answer(self, room, request_id, payload=None):
        return await self.request("POST", self.url(room, "reply"), room["hostToken"],
                                  json={"id": request_id, "packet": payload or packet()})

    def launch(self, awaitable):
        task = asyncio.create_task(awaitable)
        self.background.append(task)
        return task

    async def until(self, predicate):
        async with asyncio.timeout(3):
            while not predicate():
                await asyncio.sleep(0.005)

    async def test_health_and_json_errors(self):
        self.assertEqual(await self.request("GET", PREFIX + "/health"), (200, {"ok": True, "protocol": 1}))
        self.assertEqual((await self.request("GET", PREFIX + "/unknown"))[0], 404)
        self.assertEqual((await self.request("POST", PREFIX + "/health"))[0], 405)

    async def test_room_credentials_are_private_and_hashed_on_disk(self):
        room = await self.room()
        self.assertRegex(room["room"], r"^[a-f0-9]{32}$")
        for key in ("hostToken", "clientToken"):
            self.assertRegex(room[key], r"^[A-Za-z0-9_-]{43}$")
        raw = self.state_path.read_text()
        for secret in (self.enroll, room["hostToken"], room["clientToken"]):
            self.assertNotIn(secret, raw)
        self.assertIn(hashlib.sha256(room["hostToken"].encode()).hexdigest(), raw)
        self.assertEqual(stat.S_IMODE(self.state_path.stat().st_mode), 0o600)

    async def test_enrollment_authentication_and_body(self):
        for token in (None, secrets.token_urlsafe(32), "short"):
            status, data = await self.request("POST", PREFIX + "/rooms", token, json={})
            self.assertEqual((status, data), (401, {"error": "unauthorized"}))
        self.assertEqual((await self.request("POST", PREFIX + "/rooms", self.enroll, json={"x": 1}))[0], 400)

    async def test_origin_and_query_rejected(self):
        response = await self.client.get(PREFIX + "/health", headers={"Origin": "null"})
        self.assertEqual(response.status, 403)
        self.assertEqual(await response.json(), {"error": "origin_forbidden"})
        status, _ = await self.request("GET", PREFIX + "/health?token=" + self.enroll)
        self.assertEqual(status, 400)

    async def test_roles_and_cross_room_tokens_rejected(self):
        room, other = await self.room(), await self.room()
        for token in (room["clientToken"], other["hostToken"], self.enroll):
            self.assertEqual((await self.request("GET", self.url(room, "poll"), token))[0], 401)
        for token in (room["hostToken"], other["clientToken"], self.enroll):
            self.assertEqual((await self.request("POST", self.url(room, "rpc"), token,
                                                 json={"id": secrets.token_hex(16), "packet": packet()}))[0], 401)
        self.assertEqual((await self.request("DELETE", self.url(room), room["clientToken"]))[0], 401)

    async def test_full_roundtrip_preserves_only_opaque_packets(self):
        room = await self.room()
        request_id, request_packet, response_packet = secrets.token_hex(16), packet(), packet()
        task = self.launch(self.rpc(room, request_id, request_packet))
        await self.until(lambda: self.relay.total_pending == 1)
        status, data = await self.poll(room)
        self.assertEqual((status, data), (200, {"requests": [{"id": request_id, "packet": request_packet}]}))
        self.assertEqual(await self.answer(room, request_id, response_packet), (200, {"ok": True}))
        self.assertEqual(await task, (200, {"id": request_id, "packet": response_packet}))
        self.assertEqual((self.relay.total_pending, self.relay.total_bytes), (0, 0))
        self.assertNotIn(request_packet, self.state_path.read_text())
        self.assertNotIn(response_packet, self.state_path.read_text())

    async def test_poll_wakes_when_request_arrives(self):
        room = await self.room()
        task = self.launch(self.poll(room))
        await self.until(lambda: self.relay.rooms[room["room"]].polling)
        rid = secrets.token_hex(16)
        rpc_task = self.launch(self.rpc(room, rid))
        self.assertEqual((await task)[1]["requests"][0]["id"], rid)
        await self.answer(room, rid)
        self.assertEqual((await rpc_task)[0], 200)

    async def test_empty_poll_and_duplicate_poll(self):
        room = await self.room()
        task = self.launch(self.poll(room))
        await self.until(lambda: self.relay.rooms[room["room"]].polling)
        self.assertEqual(await self.poll(room), (409, {"error": "poll_in_progress"}))
        self.assertEqual(await task, (200, {"requests": []}))
        self.assertFalse(self.relay.rooms[room["room"]].polling)

    async def test_duplicate_active_rpc_does_not_enqueue_twice(self):
        room = await self.room()
        rid = secrets.token_hex(16)
        task = self.launch(self.rpc(room, rid))
        await self.until(lambda: self.relay.total_pending == 1)
        self.assertEqual(await self.rpc(room, rid), (409, {"error": "duplicate_request"}))
        self.assertEqual(len(self.relay.rooms[room["room"]].queue), 1)
        await self.poll(room)
        await self.answer(room, rid)
        self.assertEqual((await task)[0], 200)

    async def test_poll_batch_limit_and_fifo(self):
        room = await self.room()
        ids = [secrets.token_hex(16) for _ in range(6)]
        tasks = []
        for i, rid in enumerate(ids):
            tasks.append(self.launch(self.rpc(room, rid)))
            await self.until(lambda: self.relay.total_pending == i + 1)
        first, second = (await self.poll(room))[1], (await self.poll(room))[1]
        self.assertEqual([x["id"] for x in first["requests"]], ids[:4])
        self.assertEqual([x["id"] for x in second["requests"]], ids[4:])
        for rid in reversed(ids):
            await self.answer(room, rid)
        self.assertTrue(all(status == 200 for status, _ in await asyncio.gather(*tasks)))

    async def test_unknown_duplicate_and_undelivered_reply(self):
        room = await self.room()
        rid = secrets.token_hex(16)
        self.assertEqual(await self.answer(room, rid), (409, {"error": "request_gone"}))
        task = self.launch(self.rpc(room, rid))
        await self.until(lambda: self.relay.total_pending == 1)
        self.assertEqual(await self.answer(room, rid), (409, {"error": "request_not_delivered"}))
        await self.poll(room)
        await self.answer(room, rid)
        self.assertEqual((await task)[0], 200)
        self.assertEqual(await self.answer(room, rid), (409, {"error": "request_gone"}))

    async def test_timeout_removes_queued_and_delivered_requests(self):
        for delivered in (False, True):
            room = await self.room()
            rid = secrets.token_hex(16)
            task = self.launch(self.rpc(room, rid))
            await self.until(lambda: self.relay.total_pending == 1)
            if delivered:
                await self.poll(room)
            self.assertEqual(await task, (504, {"error": "host_timeout"}))
            state = self.relay.rooms[room["room"]]
            self.assertFalse(state.pending)
            self.assertFalse(state.queue)
            self.assertFalse(state.available.is_set())
            self.assertEqual((self.relay.total_pending, self.relay.total_bytes), (0, 0))
            self.assertEqual((await self.answer(room, rid))[0], 409)

    async def test_delete_revokes_tokens_and_wakes_pending_rpc(self):
        room = await self.room()
        task = self.launch(self.rpc(room))
        await self.until(lambda: self.relay.total_pending == 1)
        self.assertEqual(await self.request("DELETE", self.url(room), room["hostToken"]), (200, {"ok": True}))
        self.assertEqual(await task, (503, {"error": "room_closed"}))
        self.assertEqual((await self.poll(room))[0], 401)
        self.assertEqual((await self.rpc(room))[0], 401)
        self.assertEqual(json.loads(self.state_path.read_text())["rooms"], {})
        self.assertEqual(self.relay.total_pending, 0)

    async def test_authenticated_delete_receipt_survives_restart_and_preserves_existing_rooms(self):
        room = await self.room()  # state v1 written before any deletion
        other = await self.room()
        self.assertEqual(json.loads(self.state_path.read_text())["v"], 1)
        async with TestClient(TestServer(create_app(self.config))) as upgraded:
            async with upgraded.delete(self.url(room), headers=auth(room["hostToken"])) as response:
                self.assertEqual(response.status, 200)
                await response.read()  # the client need not retain this acknowledgement
        raw = self.state_path.read_text()
        self.assertNotIn(room["hostToken"], raw)
        self.assertNotIn(room["clientToken"], raw)
        self.assertEqual(json.loads(raw)["revoked"], {room["room"]: digest(room["hostToken"])})
        async with TestClient(TestServer(create_app(self.config))) as restarted:
            for key, status in ((room["hostToken"], 200), (room["clientToken"], 401), (other["hostToken"], 401)):
                async with restarted.delete(self.url(room), headers=auth(key)) as response:
                    self.assertEqual(response.status, status)
                    self.assertEqual(await response.json(), {"ok": True} if status == 200 else {"error": "unauthorized"})
            async with restarted.get(self.url(room, "poll"), headers=auth(room["hostToken"])) as response:
                self.assertEqual(response.status, 401)
            async with restarted.get(self.url(other, "poll"), headers=auth(other["hostToken"])) as response:
                self.assertEqual(response.status, 200)
            async with restarted.post(PREFIX + "/rooms", headers=auth(self.enroll), json={}) as response:
                self.assertEqual(response.status, 200)
        self.assertEqual(StateStore(self.state_path).load_state()[1], {room["room"]: digest(room["hostToken"])})

    async def test_receipts_have_separate_bounded_capacity(self):
        room = await self.room()
        oldest, oldest_token = secrets.token_hex(16), secrets.token_urlsafe(32)
        revoked = {oldest: digest(oldest_token)}
        revoked.update({secrets.token_hex(16): digest(secrets.token_urlsafe(32)) for _ in range(MAX_REVOCATIONS - 1)})
        self.relay.revoked = revoked
        self.assertEqual(await self.request("DELETE", self.url(room), room["hostToken"]), (200, {"ok": True}))
        self.assertEqual(len(self.relay.revoked), MAX_REVOCATIONS)
        self.assertEqual(await self.request("DELETE", PREFIX + "/rooms/" + oldest, oldest_token), (401, {"error": "unauthorized"}))
        self.assertEqual(await self.request("DELETE", self.url(room), room["hostToken"]), (200, {"ok": True}))
        self.assertIn((await self.room())["room"], self.relay.rooms)

    async def test_delete_wakes_idle_poll(self):
        room = await self.room()
        task = self.launch(self.poll(room))
        await self.until(lambda: self.relay.rooms[room["room"]].polling)
        await self.request("DELETE", self.url(room), room["hostToken"])
        self.assertEqual(await task, (503, {"error": "room_closed"}))

    async def test_room_limit(self):
        self.relay.config = Config(self.enroll, self.state_path, max_rooms=2)
        await self.room()
        await self.room()
        self.assertEqual(await self.request("POST", PREFIX + "/rooms", self.enroll, json={}),
                         (429, {"error": "room_limit"}))

    async def test_room_and_global_pending_limits(self):
        self.relay.config = Config(self.enroll, self.state_path, poll_timeout=0.12,
                                   rpc_timeout=0.7, room_pending=2, global_pending=3)
        a, b = await self.room(), await self.room()
        for i in range(2):
            self.launch(self.rpc(a))
            await self.until(lambda: self.relay.total_pending == i + 1)
        self.assertEqual(await self.rpc(a), (429, {"error": "room_busy"}))
        self.launch(self.rpc(b))
        await self.until(lambda: self.relay.total_pending == 3)
        self.assertEqual(await self.rpc(b), (429, {"error": "relay_busy"}))

    async def test_full_global_capacity_recovers_after_batch_completion(self):
        self.relay.config = Config(self.enroll, self.state_path)
        rooms = [await self.room() for _ in range(5)]
        tasks = []
        for room in rooms[:4]:
            tasks.extend(self.launch(self.rpc(room)) for _ in range(16))
        await self.until(lambda: self.relay.total_pending == 64)
        self.assertEqual(await self.rpc(rooms[4]), (429, {"error": "relay_busy"}))
        for room in rooms[:4]:
            for _ in range(4):
                status, data = await self.poll(room)
                self.assertEqual(status, 200)
                self.assertEqual(len(data["requests"]), 4)
                for item in data["requests"]:
                    self.assertEqual((await self.answer(room, item["id"]))[0], 200)
        self.assertTrue(all(status == 200 for status, _ in await asyncio.gather(*tasks)))
        self.assertEqual((self.relay.total_pending, self.relay.total_bytes), (0, 0))
        task = self.launch(self.rpc(rooms[4]))
        await self.until(lambda: self.relay.total_pending == 1)
        item = (await self.poll(rooms[4]))[1]["requests"][0]
        await self.answer(rooms[4], item["id"])
        self.assertEqual((await task)[0], 200)

    async def test_duplicate_authorization_header_is_rejected(self):
        response = await self.client.post(PREFIX + "/rooms", json={}, headers=[
            ("Authorization", "Bearer " + self.enroll),
            ("Authorization", "Bearer " + self.enroll),
        ])
        self.assertEqual(response.status, 401)
        self.assertEqual(await response.json(), {"error": "unauthorized"})

    async def test_byte_capacity(self):
        self.relay.config = Config(self.enroll, self.state_path, pending_bytes=64)
        room = await self.room()
        self.launch(self.rpc(room, payload="A" * 64))
        await self.until(lambda: self.relay.total_pending == 1)
        self.assertEqual(await self.rpc(room, payload="AAAA"), (429, {"error": "relay_busy"}))

    async def test_bad_json_packet_and_content_type(self):
        room = await self.room()
        url = self.url(room, "rpc")
        self.assertEqual((await self.request("POST", url, room["clientToken"], data="{}"))[0], 415)
        for body in ([], None, {"id": "wrong", "packet": packet()},
                     {"id": secrets.token_hex(16), "packet": "x"},
                     {"id": secrets.token_hex(16), "packet": "a=b+"},
                     {"id": secrets.token_hex(16), "packet": {"plaintext": "no"}},
                     {"id": secrets.token_hex(16), "packet": packet(), "extra": True}):
            response = await self.client.post(url, headers=auth(room["clientToken"]) | {"Content-Type": "application/json"},
                                              data=json.dumps(body))
            self.assertEqual(response.status, 400, body)
            await response.read()
        response = await self.client.post(url, headers=auth(room["clientToken"]) | {"Content-Type": "application/json"},
                                          data='{"id":"x","id":"y","packet":"AAAA"}')
        self.assertEqual(response.status, 400)
        await response.read()
        self.assertEqual(self.relay.total_pending, 0)

    async def test_oversized_packet_and_body(self):
        room = await self.room()
        for raw in (json.dumps({"id": secrets.token_hex(16), "packet": "A" * (MAX_PACKET + 1)}).encode(),
                    b" " * (MAX_BODY + 1)):
            response = await self.client.post(self.url(room, "rpc"),
                                              headers=auth(room["clientToken"]) | {"Content-Type": "application/json"},
                                              data=io.BytesIO(raw))
            self.assertEqual(response.status, 413)
            await response.read()
        self.assertEqual(self.relay.total_pending, 0)

    async def test_compressed_json_rejected(self):
        response = await self.client.post(PREFIX + "/rooms", data=b"not gzip",
            headers=auth(self.enroll) | {"Content-Type": "application/json", "Content-Encoding": "gzip"})
        self.assertEqual(response.status, 415)
        await response.read()

    async def test_store_failure_never_returns_credentials_or_deletes_room(self):
        with patch.object(self.relay.store, "save", side_effect=OSError("sensitive details")):
            status, data = await self.request("POST", PREFIX + "/rooms", self.enroll, json={})
            self.assertEqual((status, data), (500, {"error": "internal_error"}))
            self.assertEqual(len(self.relay.rooms), 0)
        room = await self.room()
        with patch.object(self.relay.store, "save", side_effect=OSError("sensitive details")):
            status, _ = await self.request("DELETE", self.url(room), room["hostToken"])
            self.assertEqual(status, 500)
        self.assertIn(room["room"], self.relay.rooms)
        self.assertEqual((await self.poll(room))[0], 200)

    async def test_chunked_oversized_body_is_rejected(self):
        room = await self.room()

        async def chunks():
            for _ in range(65):
                yield b" " * 65536
                await asyncio.sleep(0)

        response = await self.client.post(self.url(room, "rpc"), data=chunks(),
            headers=auth(room["clientToken"]) | {"Content-Type": "application/json"})
        self.assertEqual(response.status, 413)
        await response.read()
        self.assertEqual(self.relay.total_pending, 0)

    async def test_room_deleted_while_rpc_body_is_arriving(self):
        room = await self.room()
        entered, proceed = asyncio.Event(), asyncio.Event()
        raw = json.dumps({"id": secrets.token_hex(16), "packet": packet()}).encode()

        async def chunks():
            yield raw[:1]
            entered.set()
            await proceed.wait()
            yield raw[1:]

        task = self.launch(self.client.post(self.url(room, "rpc"), data=chunks(),
            headers=auth(room["clientToken"]) | {"Content-Type": "application/json"}))
        await entered.wait()
        await asyncio.sleep(0.02)
        await self.request("DELETE", self.url(room), room["hostToken"])
        proceed.set()
        response = await task
        self.assertEqual(response.status, 503)
        self.assertEqual(await response.json(), {"error": "room_closed"})
        self.assertEqual((self.relay.total_pending, self.relay.total_bytes), (0, 0))

    async def test_no_redirects_or_query_auth(self):
        room = await self.room()
        status, _ = await self.request("GET", self.url(room, "poll") + "?token=" + room["hostToken"])
        self.assertEqual(status, 400)
        response = await self.client.get(PREFIX + "/health/", allow_redirects=False)
        self.assertEqual(response.status, 404)
        self.assertNotIn("Location", response.headers)
        await response.read()

    async def test_restart_preserves_only_rooms_and_credentials(self):
        room = await self.room()
        self.launch(self.rpc(room))
        await self.until(lambda: self.relay.total_pending == 1)
        reloaded = create_app(self.config)[RELAY_KEY]
        self.assertEqual(reloaded.rooms[room["room"]].tokens, self.relay.rooms[room["room"]].tokens)
        self.assertEqual(reloaded.total_pending, 0)
        self.assertFalse(reloaded.rooms[room["room"]].pending)

    async def test_disconnecting_client_releases_pending(self):
        room = await self.room()
        base = str(self.server.make_url(self.url(room, "rpc")))
        async with ClientSession(timeout=ClientTimeout(total=5)) as session:
            task = self.launch(session.post(base, headers=auth(room["clientToken"]),
                              json={"id": secrets.token_hex(16), "packet": packet()}))
            await self.until(lambda: self.relay.total_pending == 1)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.until(lambda: self.relay.total_pending == 0)
        self.assertFalse(self.relay.rooms[room["room"]].queue)
        self.assertEqual(self.relay.total_bytes, 0)

    async def test_disconnecting_host_releases_poll_slot(self):
        self.relay.config = Config(self.enroll, self.state_path, poll_timeout=5)
        room = await self.room()
        async with ClientSession(timeout=ClientTimeout(total=5)) as session:
            task = self.launch(session.get(str(self.server.make_url(self.url(room, "poll"))),
                                           headers=auth(room["hostToken"])))
            await self.until(lambda: self.relay.rooms[room["room"]].polling)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.until(lambda: not self.relay.rooms[room["room"]].polling)

    async def test_shutdown_cancels_waiters_without_losing_credentials(self):
        room, polling_room = await self.room(), await self.room()
        rpc_task, poll_task = self.launch(self.rpc(room)), self.launch(self.poll(polling_room))
        await self.until(lambda: self.relay.total_pending == 1 and self.relay.rooms[polling_room["room"]].polling)
        await self.relay.shutdown(self.app)
        self.assertEqual(await rpc_task, (503, {"error": "relay_stopping"}))
        self.assertEqual((await poll_task)[0], 503)
        self.assertEqual((self.relay.total_pending, self.relay.total_bytes), (0, 0))
        self.assertEqual(len(StateStore(self.state_path).load()), 2)


if __name__ == "__main__":
    unittest.main()
