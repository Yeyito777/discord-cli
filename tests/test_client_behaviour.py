import base64
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src import api, fingerprint, gateway


def _pb_field(number, payload):
    """A length-delimited protobuf field (enough for test fixtures)."""
    assert len(payload) < 128
    return bytes([(number << 3) | 2, len(payload)]) + payload


def settings_proto(status):
    status_value = _pb_field(1, status.encode())          # StringValue.value
    status_settings = _pb_field(1, status_value)            # StatusSettings.status
    other = _pb_field(14, _pb_field(2, b"\x01"))            # unrelated guild_folders
    return base64.b64encode(other + _pb_field(11, status_settings)).decode()


class FingerprintTests(unittest.TestCase):
    def test_timezone_comes_from_the_host_not_a_hardcoded_city(self):
        with patch.dict(os.environ, {"TZ": "Europe/Zurich"}):
            self.assertEqual(fingerprint.timezone(), "Europe/Zurich")
        with patch.dict(os.environ, {"TZ": ""}), \
                patch.object(fingerprint.os.path, "realpath", return_value="/usr/share/zoneinfo/America/Panama"):
            self.assertEqual(fingerprint.timezone(), "America/Panama")
        with patch.dict(os.environ, {"TZ": "America/Panama"}):
            self.assertEqual(fingerprint.rest_headers("t")["X-Discord-Timezone"], "America/Panama")

    def test_locale_uses_the_client_format(self):
        with patch.dict(os.environ, {"LC_ALL": "", "LANG": "en_US.UTF-8"}):
            self.assertEqual(fingerprint.system_locale(), "en-US")
        with patch.dict(os.environ, {"LC_ALL": "C", "LANG": ""}):
            self.assertEqual(fingerprint.system_locale(), "en-US")

    def test_rest_and_gateway_share_one_identity(self):
        listener = object.__new__(gateway.GatewayListener)
        props = listener._build_properties()
        headers = fingerprint.rest_headers("t")
        decoded = json.loads(base64.b64decode(headers["X-Super-Properties"]))
        self.assertEqual(props["client_launch_id"], decoded["client_launch_id"])
        self.assertEqual(headers["User-Agent"], props["browser_user_agent"])
        self.assertEqual(gateway._USER_AGENT, api.USER_AGENT)

    def test_build_number_is_cached_on_disk(self):
        with tempfile.TemporaryDirectory() as cache, \
                patch.dict(os.environ, {"XDG_CACHE_HOME": cache}), \
                patch.object(fingerprint, "_build_number", None), \
                patch.object(fingerprint, "_fetch_build_number", return_value=700001) as fetch:
            self.assertEqual(fingerprint.build_number(), 700001)
            fingerprint._build_number = None
            self.assertEqual(fingerprint.build_number(), 700001)
            self.assertEqual(fetch.call_count, 1)
            fingerprint._build_number = None
            with patch.object(fingerprint.time, "time", return_value=time.time() + fingerprint.BUILD_NUMBER_TTL + 1):
                fetch.return_value = None  # discord.com unreachable: keep the last known value
                self.assertEqual(fingerprint.build_number(), 700001)
            self.assertEqual(fetch.call_count, 2)


class RateLimitTests(unittest.TestCase):
    def setUp(self):
        for item in (
            patch.object(api, "_headers", return_value={"Authorization": "t"}),
            patch("src.accounts.ensure_mutation_allowed"),
            patch("src.accounts.audit_event"),
            patch("src.typing_keeper.stop_quietly"),
            patch.object(api.time, "sleep"),
        ):
            item.start()
            self.addCleanup(item.stop)

    def serve(self, *responses):
        class Conn:
            sock = None
            sent = []

            def request(self, *args):
                Conn.sent.append(args)

            def getresponse(self):
                status, body = responses[len(Conn.sent) - 1]
                return Mock(status=status, read=Mock(return_value=json.dumps(body).encode()),
                            getheader=Mock(return_value=None))

            def close(self):
                pass

        patcher = patch.object(api, "_get_connection", return_value=[Conn(), True, 0])
        patcher.start()
        self.addCleanup(patcher.stop)
        return Conn

    def test_short_rate_limit_is_waited_out_once(self):
        conn = self.serve((429, {"retry_after": 1.5, "global": False}), (200, {"id": "9"}))
        self.assertEqual(api.send_message("1", "hi")["id"], "9")
        api.time.sleep.assert_called_once()
        self.assertAlmostEqual(api.time.sleep.call_args.args[0], 1.55)
        bodies = [json.loads(args[2]) for args in conn.sent]
        self.assertEqual(bodies[0]["nonce"], bodies[1]["nonce"])  # same message, deduplicated

    def test_long_rate_limit_is_reported_not_slept(self):
        conn = self.serve((429, {"retry_after": 120}))
        with self.assertRaisesRegex(api.RateLimited, "Retry after 120.0 s") as caught:
            api.get("/users/@me")
        self.assertEqual(caught.exception.retry_after, 120)
        api.time.sleep.assert_not_called()
        self.assertEqual(len(conn.sent), 1)

    def test_second_rate_limit_is_not_retried_again(self):
        conn = self.serve((429, {"retry_after": 0.5}), (429, {"retry_after": 0.5}))
        with self.assertRaises(api.RateLimited):
            api.get("/users/@me")
        self.assertEqual(len(conn.sent), 2)

    def test_thread_listing_reports_channels_it_could_not_read(self):
        channels = [{"id": "1", "name": "a", "type": 0}, {"id": "2", "name": "b", "type": 0}]

        def get(path, **_kwargs):
            if path.startswith("/channels/2"):
                raise api.RateLimited("Rate limited", 30)
            return {"threads": [{"id": "t1"}]}

        with patch.object(api, "get_guild_channels", return_value=channels), patch.object(api, "get", side_effect=get):
            result = api.get_active_threads("g")
        self.assertEqual([t["id"] for t in result["threads"]], ["t1"])
        self.assertEqual([ch["id"] for ch, _ in result["failed"]], ["2"])


class GatewayBehaviourTests(unittest.TestCase):
    def listener(self):
        listener = object.__new__(gateway.GatewayListener)
        listener.token = "old-token"
        listener.account = {"alias": "personal"}
        listener.running = True
        listener._stopping = threading.Event()
        listener._failed_connects = 0
        listener.session_id = "s"
        listener._log = Mock()
        listener._channel_notify_state = Mock()
        return listener

    def test_identify_lets_the_account_status_setting_apply(self):
        listener = self.listener()
        listener._ws_send = Mock()
        listener._identify()
        presence = listener._ws_send.call_args.args[0]["d"]["presence"]
        self.assertEqual(presence["status"], "unknown")

    def test_ready_logs_setting_and_session_status(self):
        listener = self.listener()
        listener.session_id = "mine"
        listener._log_presence({
            "user_settings_proto": settings_proto("invisible"),
            "sessions": [{"session_id": "other", "status": "online"}, {"session_id": "mine", "status": "invisible"}],
        })
        listener._log.assert_called_with("Presence: account setting invisible, this session invisible")

    def test_settings_status_decoding(self):
        self.assertEqual(gateway.settings_status(settings_proto("dnd")), "dnd")
        self.assertIsNone(gateway.settings_status(base64.b64encode(_pb_field(14, b"")).decode()))
        self.assertIsNone(gateway.settings_status("not base64!"))
        self.assertIsNone(gateway.settings_status(None))

    def test_rejected_token_stops_reconnecting_until_credentials_change(self):
        listener = self.listener()
        attempts = []
        mtimes = iter([1, 1, 1, 2])

        def connect():
            attempts.append(listener.token)
            if len(attempts) == 2:
                listener.running = False

        def receive():
            listener._close_code = 4004

        listener._connect = connect
        listener._receive_loop = receive
        listener.notification_source = None
        listener._credentials_mtime = lambda: next(mtimes)
        waits = []
        listener._stopping = Mock(wait=lambda seconds: waits.append(seconds))
        with (patch("src.auth.get_token", side_effect=["old-token", "new-token"]),
              patch("src.api.reset_token_cache") as reset):
            listener.run()
        self.assertEqual(attempts, ["old-token", "new-token"])
        self.assertEqual(waits, [gateway.CREDENTIALS_RECHECK_SECONDS] * 2)
        listener._channel_notify_state.assert_any_call(
            "token-rejected", "close code 4004: run 'discord -a personal login'")
        reset.assert_called_once_with()

    def test_rejected_token_uses_a_token_file_refreshed_while_running(self):
        # A Gateway that outlived a login holds the old token in memory: the
        # file already has a new one, so it reconnects at once instead of
        # waiting for another change (the Mac's 16-day 4004 loop).
        listener = self.listener()
        attempts = []

        def connect():
            attempts.append(listener.token)
            if len(attempts) == 2:
                listener.running = False

        listener._connect = connect
        listener._receive_loop = lambda: setattr(listener, "_close_code", 4004)
        listener.notification_source = None
        listener._credentials_mtime = Mock(return_value=1)
        listener._stopping = Mock(wait=Mock(return_value=False))
        with (patch("src.auth.get_token", return_value="refreshed-token"),
              patch("src.api.reset_token_cache") as reset):
            listener.run()
        self.assertEqual(attempts, ["old-token", "refreshed-token"])
        listener._stopping.wait.assert_not_called()
        reset.assert_called_once_with()

    def test_ordinary_close_still_reconnects_with_backoff(self):
        listener = self.listener()
        listener.notification_source = None
        calls = []

        def connect():
            calls.append(1)
            if len(calls) == 3:
                listener.running = False

        listener._connect = connect
        listener._receive_loop = lambda: setattr(listener, "_close_code", 1006)
        listener._stopping = Mock(wait=Mock())
        listener.run()
        self.assertEqual(len(calls), 3)
        self.assertEqual(listener._stopping.wait.call_count, 2)
