import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from src import api, managing, typing_keeper


class TypingKeeperTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        dir_patch = patch.object(typing_keeper, "listener_dir", return_value=Path(self.tmp.name))
        dir_patch.start()
        self.addCleanup(dir_patch.stop)

    def pid_file(self, channel="42"):
        return Path(self.tmp.name) / f"typing-{channel}.pid"

    def test_start_triggers_now_and_records_detached_keeper(self):
        proc = MagicMock(pid=4321)
        with patch.object(typing_keeper.api, "trigger_typing") as trigger, \
                patch.object(typing_keeper.subprocess, "Popen", return_value=proc) as popen:
            typing_keeper.start("42", 30)
        trigger.assert_called_once_with("42")
        argv = popen.call_args.args[0]
        self.assertIn("discord-typing-keeper", argv)
        self.assertIn("42", argv)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        self.assertEqual(self.pid_file().read_text().strip(), "4321")

    def test_start_replaces_running_keeper(self):
        with patch.object(typing_keeper, "stop") as stop, \
                patch.object(typing_keeper.api, "trigger_typing"), \
                patch.object(typing_keeper.subprocess, "Popen", return_value=MagicMock(pid=1)):
            typing_keeper.start("42", 30)
        stop.assert_called_once_with("42")

    def test_stop_kills_only_a_verified_keeper(self):
        self.pid_file().write_text("4321\n")
        alive = iter([True, True, False])
        with patch.object(typing_keeper, "_is_keeper", side_effect=lambda pid, ch: next(alive)), \
                patch.object(typing_keeper.os, "kill") as kill:
            self.assertTrue(typing_keeper.stop("42"))
        kill.assert_called_once_with(4321, typing_keeper.signal.SIGTERM)
        self.assertFalse(self.pid_file().exists())

    def test_stop_ignores_stale_or_reused_pid(self):
        self.pid_file().write_text("4321\n")
        with patch.object(typing_keeper, "_is_keeper", return_value=False), \
                patch.object(typing_keeper.os, "kill") as kill:
            self.assertFalse(typing_keeper.stop("42"))
        kill.assert_not_called()
        self.assertFalse(self.pid_file().exists())

    def test_stop_without_keeper_is_a_no_op(self):
        self.assertFalse(typing_keeper.stop("42"))

    def test_is_keeper_rejects_other_processes(self):
        self.assertFalse(typing_keeper._is_keeper(os.getpid(), "42"))

    def test_run_retriggers_until_deadline_then_cleans_up(self):
        self.pid_file().write_text(f"{os.getpid()}\n")
        clock = [1000.0]
        with patch.object(typing_keeper.time, "time", side_effect=lambda: clock[0]), \
                patch.object(typing_keeper.time, "sleep", side_effect=lambda s: clock.__setitem__(0, clock[0] + s)), \
                patch.object(typing_keeper.signal, "signal"), \
                patch.object(typing_keeper.api, "post") as post:
            typing_keeper.run(["discord-typing-keeper", "42", "1020"])
        # Beats at +8 and +16; the last 4 s is covered by the +16 trigger.
        self.assertEqual(post.call_args_list, [
            call("/channels/42/typing", allow_captcha_fallback=False),
            call("/channels/42/typing", allow_captcha_fallback=False),
        ])
        self.assertFalse(self.pid_file().exists())

    def test_run_stops_on_access_error_but_tolerates_rate_limits(self):
        clock = [0.0]
        common = dict(
            time=patch.object(typing_keeper.time, "time", side_effect=lambda: clock[0]),
            sleep=patch.object(typing_keeper.time, "sleep", side_effect=lambda s: clock.__setitem__(0, clock[0] + s)),
            sig=patch.object(typing_keeper.signal, "signal"),
        )
        with common["time"], common["sleep"], common["sig"], \
                patch.object(typing_keeper.api, "post", side_effect=RuntimeError("HTTP 403: Missing Access")) as post:
            typing_keeper.run(["m", "42", "100"])
        self.assertEqual(post.call_count, 1)

        clock[0] = 0.0
        with common["time"], common["sleep"], common["sig"], \
                patch.object(typing_keeper.api, "post", side_effect=RuntimeError("HTTP 429: slow down")) as post:
            typing_keeper.run(["m", "42", "100"])
        self.assertEqual(post.call_count, typing_keeper.MAX_CONSECUTIVE_ERRORS)


class SendStopsTypingTests(unittest.TestCase):
    def test_send_message_stops_keeper_before_posting(self):
        order = []
        with patch("src.typing_keeper.stop_quietly", side_effect=lambda ch: order.append(("stop", ch))), \
                patch.object(api, "post", side_effect=lambda *a, **k: order.append(("post", a[0])) or {"id": "1"}):
            api.send_message("42", "hi")
        self.assertEqual(order, [("stop", "42"), ("post", "/channels/42/messages")])

    def test_keeper_failure_never_blocks_send(self):
        with patch.object(typing_keeper, "stop", side_effect=OSError("boom")), \
                patch.object(api, "post", return_value={"id": "1"}) as post:
            api.send_message("42", "hi")
        post.assert_called_once()


class TypingCommandTests(unittest.TestCase):
    def run_cmd(self, argv):
        out = io.StringIO()
        with redirect_stdout(out):
            managing.typing(argv)
        return out.getvalue()

    def test_default_starts_keeper(self):
        with patch.object(managing.typing_keeper, "start") as start:
            self.assertIn("next send", self.run_cmd(["42"]))
        start.assert_called_once_with("42", typing_keeper.DEFAULT_SECONDS)

    def test_for_sets_deadline_and_is_bounded(self):
        with patch.object(managing.typing_keeper, "start") as start:
            self.run_cmd(["42", "--for", "45"])
        start.assert_called_once_with("42", 45.0)
        with self.assertRaises(SystemExit), patch("sys.stderr", io.StringIO()):
            self.run_cmd(["42", "--for", str(typing_keeper.MAX_SECONDS + 1)])

    def test_once_and_stop(self):
        with patch.object(managing.api, "trigger_typing") as trigger:
            self.run_cmd(["42", "--once"])
        trigger.assert_called_once_with("42")
        with patch.object(managing.typing_keeper, "stop", return_value=False):
            self.assertIn("No typing", self.run_cmd(["42", "--stop"]))


if __name__ == "__main__":
    unittest.main()
