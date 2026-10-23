"""Regression tests for listeners that died silently (Oct 2026).

A channel Gateway on a laptop ran for 16 days with an expired token, closed by
Discord after every identify, re-registering its source with exocortexd every
few seconds (211k log lines), while nothing told anyone that messages had
stopped arriving.  These cover reconnecting, registration, supervision,
backfill and the singleton/log bounds that let that happen.
"""

import io
import json
import os
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src import gateway, listener_state, notify, supervisor
from src.channel_notifications import PREFIX
from src.gateway import GatewayListener

C = "709232473783140375"
G = "709232473783140370"
DM = "1031060518775038003"
SELF = "1000000000000000001"


def snowflake(epoch_seconds):
    return GatewayListener._snowflake_at(epoch_seconds)


class ListenerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for target in ("src.listener_state.listener_dir", "src.listener_state.account_root"):
            patcher = patch(target, lambda alias=None: self.root)
            patcher.start()
            self.addCleanup(patcher.stop)

    def listener(self, key=PREFIX + C):
        listener = object.__new__(GatewayListener)
        listener.channel_id = key
        listener.notification_channel = key[len(PREFIX):] if key.startswith(PREFIX) else None
        listener.notification_source = {"id": "account:paramount:channel:%s:messages" % C, "label": "x"}
        listener.account = {"alias": "paramount", "owner": "assistant"}
        listener.running = True
        listener._stopping = threading.Event()
        listener._notification_queue = []
        listener._notification_lock = threading.Lock()
        listener._notification_active = False
        listener._notification_seen = {}
        listener._pending_ids = set()
        listener._init_health()
        listener._channel_notify_paths = listener_state.gateway_paths(key)
        listener.output_file = str(self.root / f"{key}.log")
        listener._log = Mock()
        listener._write = Mock()
        listener.my_id = SELF
        listener._guilds = {G: "The Toolbar Cult"}
        listener._channels = {C: {"name": "storage"}}
        listener._private_channels = {}
        listener._failed_connects = 0
        listener.ws = Mock()
        listener._queue_notification = Mock()
        return listener

    def queued(self, listener):
        return [call.args[0] for call in listener._queue_notification.call_args_list]


class ReconnectTests(ListenerCase):
    def test_reconnect_closes_the_old_socket_resumably(self):
        listener = self.listener()
        old = listener.ws
        with patch.object(gateway.websocket, "WebSocket") as new:
            listener._ws_open("wss://gateway.discord.gg")
        old.close.assert_called_once_with(status=gateway.RESUMABLE_CLOSE_CODE, timeout=1)
        new.return_value.settimeout.assert_called_once_with(gateway.CONNECT_TIMEOUT_SECONDS)

    def test_missed_heartbeat_drops_the_socket_without_a_close_frame(self):
        listener = self.listener()
        ours, theirs = socket.socketpair()
        self.addCleanup(ours.close)
        self.addCleanup(theirs.close)
        listener.ws = Mock(sock=ours)
        listener.heartbeat_acked = False
        listener.heartbeat_interval = 0
        listener._hb_gen = 1
        listener._heartbeat_loop(1)
        listener.ws.close.assert_not_called()
        self.assertEqual(theirs.recv(1), b"")  # peer sees the shutdown

    def test_invalid_session_keeps_a_resumable_session(self):
        for resumable, expected in ((True, "session"), (False, None)):
            with self.subTest(resumable=resumable):
                listener = self.listener()
                listener.session_id = "session"
                payload = json.dumps({"op": 9, "d": resumable}).encode()
                listener.ws.recv_data.return_value = (1, payload)
                listener._decompress = lambda data: data
                listener._stopping = Mock(wait=Mock(return_value=False))
                listener._receive_loop()
                self.assertEqual(listener.session_id, expected)

    def test_frames_update_liveness(self):
        listener = self.listener()
        frames = iter([(1, json.dumps({"op": 11}).encode())])

        def recv():
            try:
                return next(frames)
            except StopIteration:
                raise ConnectionResetError("gone")

        listener.ws.recv_data.side_effect = recv
        listener._decompress = lambda data: data
        listener.heartbeat_acked = False
        listener._receive_loop()
        self.assertTrue(listener.heartbeat_acked)
        self.assertIsNotNone(listener._health["last_event"])

    def test_connect_failures_never_register_with_exocortex(self):
        # The old loop registered before every connect attempt: 211k times.
        listener = self.listener()
        attempts = []

        def connect():
            attempts.append(1)
            if len(attempts) == 5:
                listener.running = False
            raise OSError("[Errno 8] nodename nor servname provided")

        listener._connect = connect
        listener._stopping = Mock(wait=Mock(return_value=False))
        with (patch.object(listener, "_reconnect_delay", return_value=0),
              patch("src.exocortex.register_external_notification_source") as register):
            listener.run()
        self.assertEqual(len(attempts), 5)
        register.assert_not_called()
        state = json.loads(listener._channel_notify_paths["state"].read_text())
        self.assertEqual(state["status"], "stopped")
        self.assertIn("nodename", state["detail"])


class RegistrationTests(ListenerCase):
    def ready(self, listener, identity):
        listener._exocortex_identity = Mock(return_value=identity)
        listener._backfill = Mock()
        listener._on_session_ready(fresh=True)

    def test_registers_once_per_exocortexd_instance(self):
        listener = self.listener()
        with patch("src.exocortex.register_external_notification_source") as register:
            for _ in range(20):  # many Discord reconnects
                self.ready(listener, ("dev", 1, 100))
                listener._ready = False
            self.assertEqual(register.call_count, 1)
            self.ready(listener, ("dev", 2, 200))  # exocortexd restarted
            self.assertEqual(register.call_count, 2)
        self.assertEqual(listener._health["reconnects"], 20)

    def test_waits_for_a_live_session_and_a_socket(self):
        listener = self.listener()
        listener._exocortex_identity = Mock(return_value=("dev", 1, 100))
        with patch("src.exocortex.register_external_notification_source") as register:
            listener._maybe_register(time.monotonic())
            register.assert_not_called()  # not ready: registering would claim "online"
            listener._ready = True
            listener._exocortex_identity.return_value = None  # exocortexd is not running
            listener._maybe_register(time.monotonic())
            register.assert_not_called()

    def test_failed_registration_backs_off_and_logs_once(self):
        listener = self.listener()
        listener._ready = True
        listener._exocortex_identity = Mock(return_value=("dev", 1, 100))
        error = RuntimeError("Timed out waiting for external_notification_source from exocortexd")
        with patch("src.exocortex.register_external_notification_source", side_effect=error) as register:
            now = 1000.0
            delays = []
            for _ in range(len(gateway.REGISTER_BACKOFF_SECONDS) + 1):
                listener._maybe_register(now)
                listener._maybe_register(now + 0.5)  # inside the backoff: no attempt
                delays.append(listener._next_register - now)
                now = listener._next_register
        self.assertEqual(register.call_count, len(gateway.REGISTER_BACKOFF_SECONDS) + 1)
        self.assertEqual(delays, list(gateway.REGISTER_BACKOFF_SECONDS) + [gateway.REGISTER_BACKOFF_SECONDS[-1]])
        self.assertEqual(listener._log.call_count, 1)  # repeated failures are throttled
        with patch("src.exocortex.register_external_notification_source") as register:
            listener._maybe_register(now)
        register.assert_called_once()
        self.assertEqual(listener._register_failures, 0)

    def test_publisher_backs_off_while_exocortexd_is_down(self):
        listener = self.listener()
        del listener._queue_notification
        waits = []
        listener._stopping = Mock(wait=lambda seconds: waits.append(seconds))
        outcomes = iter([RuntimeError("exocortexd is not running")] * 3 + [None])

        def publish(notif):
            outcome = next(outcomes)
            if outcome:
                raise outcome

        listener._publish_notification = publish
        with patch.object(gateway.threading, "Thread") as thread:
            listener._queue_notification({"msg_id": "5", "type": "dm"})
        self.assertEqual(listener._pending_ids, {"5"})
        thread.return_value.start.assert_called_once()
        listener._notification_sender()
        self.assertEqual(waits, list(gateway.PUBLISH_BACKOFF_SECONDS[:3]))
        self.assertEqual(listener._pending_ids, set())
        logged = [call.args[0] for call in listener._log.call_args_list]
        self.assertEqual(sum("publish failed" in line for line in logged), 1)
        self.assertIn("Notification publishing recovered", logged)


class BackfillTests(ListenerCase):
    def history(self, *ids, channel=C, author="friend"):
        return [{"id": i, "channel_id": channel, "type": 0, "content": f"post {i}",
                 "author": {"id": author, "username": author}, "mentions": []}
                for i in sorted(ids, key=int, reverse=True)]

    def test_first_session_starts_the_cursor_without_fetching(self):
        listener = self.listener()
        with patch("src.api.get_messages") as fetch:
            listener._backfill()
        fetch.assert_not_called()
        self.assertGreater(int(listener._seen_max), int(snowflake(time.time() - 60)))

    def test_channel_messages_missed_while_down_are_delivered_once(self):
        listener = self.listener()
        cursor = snowflake(time.time() - 3600)
        listener._seen_max = cursor
        listener._channel_guilds[C] = G
        older, missed_a, missed_b, own = (str(int(cursor) - 5), str(int(cursor) + 10),
                                          str(int(cursor) + 20), str(int(cursor) + 30))
        batch = self.history(older, missed_a, missed_b) + self.history(own, author=SELF)
        with patch("src.api.get_messages", return_value=batch) as fetch:
            listener._backfill()
        fetch.assert_called_once_with(C, limit=gateway.BACKFILL_CHANNEL_LIMIT)
        events = self.queued(listener)
        self.assertEqual([event["msg_id"] for event in events], [missed_a, missed_b])
        self.assertTrue(all(event["backfilled"] and event["guild_id"] == G for event in events))
        # The same message arriving live afterwards is not delivered twice.
        listener._on_dispatch("MESSAGE_CREATE", {**self.history(missed_b)[0], "guild_id": G})
        self.assertEqual(len(self.queued(listener)), 2)
        self.assertEqual(listener._seen_max, own)

    def test_ready_metadata_skips_a_fetch_when_nothing_was_missed(self):
        listener = self.listener()
        listener._seen_max = snowflake(time.time() - 60)
        listener._last_message_ids[C] = str(int(listener._seen_max) - 1)
        with patch("src.api.get_messages") as fetch:
            listener._backfill()
        fetch.assert_not_called()

    def test_truncated_or_failed_backfill_is_an_explicit_notice(self):
        listener = self.listener()
        cursor = snowflake(time.time() - 3600)
        listener._seen_max = cursor
        listener._channel_guilds[C] = G
        full = self.history(*(str(int(cursor) + n) for n in range(1, gateway.BACKFILL_CHANNEL_LIMIT + 1)))
        with patch("src.api.get_messages", return_value=full):
            listener._backfill()
        notices = [event for event in self.queued(listener) if event["type"] == "notice"]
        self.assertEqual(len(notices), 1)
        self.assertIn(f"[ch:{C}]", notices[0]["text"])

        listener = self.listener()
        listener._seen_max = cursor
        with patch("src.api.get_messages", side_effect=RuntimeError("HTTP 503")):
            listener._backfill()
        (notice,) = self.queued(listener)
        self.assertEqual(notice["event_id"], f"backfill-failed:{cursor}")
        self.assertIn("HTTP 503", notice["text"])

    def test_dm_listener_backfills_newer_dms_and_mentions(self):
        listener = self.listener("__notify__")
        listener.notification_source = {"id": "account:paramount:notifications", "label": "x"}
        cursor = snowflake(time.time() - 3600)
        listener._seen_max = cursor
        quiet = "1031060518775038004"
        listener._private_channels = {DM: {"channel_type": "dm", "channel_name": "owner", "participants": ["owner"]},
                                      quiet: {"channel_type": "dm", "channel_name": "quiet"}}
        listener._last_message_ids = {DM: str(int(cursor) + 50), quiet: str(int(cursor) - 50)}
        listener._channel_guilds = {C: G}
        dm_id, mention_id, stale_mention = str(int(cursor) + 50), str(int(cursor) + 40), str(int(cursor) - 1)
        mentions = [
            {**self.history(mention_id)[0], "mentions": [{"id": SELF}]},
            {**self.history(stale_mention)[0], "mentions": [{"id": SELF}]},
            {**self.history(str(int(cursor) + 41), channel="1999999999999999999")[0], "mentions": [{"id": SELF}]},
        ]
        with (patch("src.api.get_messages", return_value=self.history(dm_id, channel=DM)) as fetch,
              patch("src.api.get", return_value=mentions) as get):
            listener._backfill()
        fetch.assert_called_once_with(DM, limit=gateway.BACKFILL_DM_LIMIT)
        self.assertEqual(get.call_args.args[0], "/users/@me/mentions")
        events = self.queued(listener)
        # Chronological; the mention in an unknown server is dropped rather
        # than misread as a DM.
        self.assertEqual([(event["msg_id"], event["type"]) for event in events],
                         [(mention_id, "mention"), (dm_id, "dm")])

    def test_cursor_never_passes_an_undelivered_message(self):
        listener = self.listener()
        listener._seen_max = "1000"
        listener._pending_ids = {"990", "995"}
        self.assertEqual(listener._durable_cursor(), "989")
        listener._save_cursor(force=True)
        self.assertEqual(listener_state.load_cursor(listener.channel_id), "989")
        listener._pending_ids = set()
        listener._save_cursor()  # throttled
        self.assertEqual(listener_state.load_cursor(listener.channel_id), "989")
        listener._save_cursor(force=True)
        self.assertEqual(listener_state.load_cursor(listener.channel_id), "1000")

    def test_late_events_say_so(self):
        listener = self.listener()
        listener._format_notification_batch = Mock(return_value=("post", {}))
        with patch("src.exocortex.publish_external_notification", return_value={"deliveries": []}) as publish:
            listener._publish_notification({"msg_id": "7", "type": "channel_message", "backfilled": True})
            listener._publish_notification({"type": "notice", "event_id": "backfill-gap:1", "text": "gap"})
        late, notice = publish.call_args_list
        self.assertIn("delivered late", late.args[3])
        self.assertTrue(late.kwargs["data"]["backfilled"])
        self.assertEqual(notice.args[2], "backfill-gap:1")
        self.assertEqual(notice.kwargs["data"]["kind"], "listener_notice")
        self.assertIsNotNone(listener._health["last_delivery"])


class SingletonAndLogTests(ListenerCase):
    def test_deleted_lock_and_pid_files_are_restored(self):
        listener = self.listener()
        with patch("atexit.register"):
            listener._acquire_notify_singleton()
        self.addCleanup(listener._release_notify_singleton)
        files = listener._channel_notify_paths
        files["lock"].unlink()  # macOS /tmp cleanup
        files["pid"].unlink()
        listener._verify_notify_singleton()
        self.assertEqual(files["pid"].read_text().strip(), str(os.getpid()))
        with self.assertRaises(SystemExit):  # the re-created lock excludes again
            other = self.listener()
            other._acquire_notify_singleton()

    def test_a_gateway_that_lost_its_lock_to_another_exits(self):
        listener = self.listener()
        with patch("atexit.register"):
            listener._acquire_notify_singleton()
        self.addCleanup(listener._release_notify_singleton)
        files = listener._channel_notify_paths
        files["lock"].unlink()
        winner = GatewayListener._try_lock(files["lock"])
        self.addCleanup(os.close, winner)
        listener._verify_notify_singleton()
        self.assertFalse(listener.running)

    def test_error_log_rotates_onto_stderr(self):
        err = self.root / "x.err"
        saved = os.dup(2)
        self.addCleanup(os.close, saved)
        fd = os.open(err, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        os.dup2(fd, 2)
        os.close(fd)
        try:
            os.write(2, b"x" * 200)
            self.assertFalse(listener_state.rotate_stderr(err, limit=1000))
            self.assertTrue(listener_state.rotate_stderr(err, limit=100))
            os.write(2, b"after")
        finally:
            os.dup2(saved, 2)
        self.assertEqual(err.read_bytes(), b"after")
        self.assertEqual(len((self.root / "x.err.1").read_bytes()), 200)

    def test_status_line_shows_liveness(self):
        now = 1_000_000.0
        state = {"pid": 5, "status": "ready", "connected_since": now - 3700, "last_event": now - 8,
                 "last_delivery": now - 7200, "reconnects": 3, "updated": now - 5}
        self.assertEqual(
            listener_state.describe(state, 5, now),
            "ready (PID 5) · connected 61m · last Gateway event 8s ago · last delivery 2h00m ago · 3 reconnects",
        )
        stuck = {**state, "status": "reconnecting", "since": now - 900, "updated": now - 400,
                 "detail": "timed out"}
        self.assertIn("STALE", listener_state.describe(stuck, 5, now))
        self.assertIn("last error: timed out", listener_state.describe(stuck, 5, now))
        self.assertEqual(listener_state.describe(state, None, now), "stopped")


class FakeProc:
    def __init__(self, pid):
        self.pid = pid
        self.returncode = None
        self.killed = False

    def poll(self):
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9


class SupervisorHealthTests(unittest.TestCase):
    def setUp(self):
        self.states = {}
        patcher = patch.object(supervisor, "read_state", lambda key, alias=None: self.states.get(key))
        patcher.start()
        self.addCleanup(patcher.stop)
        for target, value in (("_log", lambda message: None),):
            patcher = patch.object(supervisor, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.published = []

    def publish(self, source_id, event_id, text, data):
        self.published.append((source_id, event_id, text, data["status"]))

    def gateway(self, key=PREFIX + C):
        gw = supervisor.Gateway("paramount", key)
        gw.proc = FakeProc(4242)
        gw.progress_at = 0.0
        return gw

    def check(self, gw, now):
        gw.next_health = 0.0
        gw.check_health(now, self.publish)

    def test_wedged_gateway_is_killed(self):
        gw = self.gateway()
        self.states[gw.key] = {"pid": 4242, "status": "ready", "updated": 1.0}
        self.check(gw, 10.0)
        self.check(gw, 10.0 + supervisor.STALL_SECONDS - 1)
        self.assertFalse(gw.proc.killed)
        self.states[gw.key] = {"pid": 4242, "status": "ready", "updated": 2.0}  # still progressing
        self.check(gw, 10.0 + supervisor.STALL_SECONDS + 1)
        self.assertFalse(gw.proc.killed)
        self.check(gw, 20.0 + 2 * supervisor.STALL_SECONDS)
        self.assertTrue(gw.proc.killed)

    def test_long_outage_alerts_subscribers_once_then_recovers(self):
        gw = self.gateway()
        self.states[gw.key] = {"pid": 4242, "status": "reconnecting", "detail": "[Errno 51] Network is unreachable"}
        for t in range(0, int(supervisor.ALERT_SECONDS), 30):
            self.states[gw.key]["updated"] = t
            self.check(gw, float(t))
        self.assertEqual(self.published, [])  # a short blip stays quiet
        for t in (supervisor.ALERT_SECONDS, supervisor.ALERT_SECONDS + 30, supervisor.ALERT_SECONDS + 60):
            self.states[gw.key]["updated"] = t
            self.check(gw, t)
        self.assertEqual(len(self.published), 1)
        source, event_id, text, status = self.published[0]
        self.assertEqual(source, f"account:paramount:channel:{C}:messages")
        self.assertTrue(event_id.startswith(f"listener-down:{PREFIX}{C}:"))
        self.assertIn("Network is unreachable", text)
        self.assertEqual(status, "reconnecting")
        self.states[gw.key] = {"pid": 4242, "status": "ready", "updated": 999.0}
        self.check(gw, supervisor.ALERT_SECONDS + 90)
        self.assertEqual([item[3] for item in self.published], ["reconnecting", "recovered"])
        self.assertIsNone(gw.unready_since)

    def test_rejected_token_alerts_at_once_and_retries_when_exocortexd_is_down(self):
        gw = self.gateway("__notify__")
        self.states[gw.key] = {"pid": 4242, "status": "token-rejected", "updated": 1.0,
                               "detail": "close code 4004: run 'discord -a paramount login'"}
        failing = Mock(side_effect=RuntimeError("exocortexd is not running"))
        gw.check_health(5.0, failing)
        self.assertFalse(gw.alerted)
        gw.next_health = 0.0
        gw.check_health(6.0, self.publish)  # inside ALERT_RETRY_SECONDS
        self.assertEqual(self.published, [])
        self.check(gw, 5.0 + supervisor.ALERT_RETRY_SECONDS)
        self.assertEqual(self.published[0][0], "account:paramount:notifications")
        self.assertIn("discord -a paramount login", self.published[0][2])

    def test_a_crash_looping_gateway_counts_as_down(self):
        gw = self.gateway()
        gw.proc = None  # exited, waiting for its backoff restart
        self.check(gw, 0.0)
        self.check(gw, supervisor.ALERT_SECONDS)
        self.assertEqual([item[3] for item in self.published], ["stopped"])


class DaemonAccountTests(unittest.TestCase):
    def run_daemon(self, registry_notify):
        with (patch.object(notify, "_migrate_relay_targets"),
              patch.object(notify, "supervised_notify_alias", return_value=registry_notify),
              patch("src.accounts.account_count", return_value=2),
              patch("src.supervisor.Supervisor") as sup):
            notify._run_daemon_mode([])
        return sup.call_args.args[0]

    def test_only_the_chosen_account_always_listens_for_dms(self):
        self.assertEqual(self.run_daemon("paramount"), "paramount")
        # 'discord account notify --none': no default-account fallback, so a
        # machine hosting only channel subscriptions runs no DM Gateway.
        self.assertIsNone(self.run_daemon(None))

    def test_account_notify_none_survives_reloading_the_registry(self):
        from src import accounts
        with tempfile.TemporaryDirectory() as tmp:
            registry = Path(tmp) / "accounts.json"
            registry.write_text(json.dumps({
                "default_account": "paramount", "notify_account": "paramount",
                "accounts": {"paramount": {"user_id": "1", "owner": "assistant", "access": "full"}},
            }))
            with (patch.object(accounts, "REGISTRY_FILE", registry),
                  patch("sys.stdout", new_callable=io.StringIO)):
                accounts.set_notify_account(["--none"])
                self.assertIsNone(accounts.supervised_notify_alias())
                accounts.set_notify_account(["paramount"])
                self.assertEqual(accounts.supervised_notify_alias(), "paramount")


if __name__ == "__main__":
    unittest.main()
