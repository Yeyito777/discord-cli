import json
import unittest
from pathlib import Path
from unittest.mock import patch

from src import api
from src import vimbrowser_captcha as captcha


CAPTCHA_PAYLOAD = {
    "captcha_key": ["verification required"],
    "captcha_service": "hcaptcha",
    "captcha_sitekey": "site-key",
    "captcha_session_id": "session-id",
    "captcha_rqdata": "request-data",
    "captcha_rqtoken": "request-token",
}


class FakeIpc:
    def __init__(self):
        self.opened = None
        self.closed = None
        self.required = False

    def require_context_tabs(self):
        self.required = True

    def open_context_tab(self, context, url):
        self.opened = (context, url)
        return 17

    def eval(self, tab_id, script):
        if "document.readyState" in script:
            return json.dumps({"origin": "https://discord.com", "ready": "complete"})
        if "const challenge" in script:
            self.bootstrap_script = script
            return "started"
        if "window.__discordCliCaptcha" in script:
            return json.dumps({"status": "solved", "token": "solution-token"})
        raise AssertionError(f"Unexpected script: {script[:80]}")

    def close_tab(self, tab_id):
        self.closed = tab_id


class FakeInviteIpc:
    def __init__(self):
        self.closed = None
        self.cleared = None
        self.clicked = False
        self.calls = []
        self.page_error = None

    def require_context_tabs(self):
        pass

    def open_context_tab(self, context, url):
        self.opened = (context, url)
        return 23

    def eval(self, tab_id, script):
        if "document.readyState" in script and "has_action" not in script:
            return json.dumps({"origin": "https://discord.com", "ready": "complete"})
        if "const authToken" in script:
            self.bootstrap = script
            return "opening invite"
        if "has_action" in script:
            return json.dumps({"has_code": True, "has_action": True, "body": "Invite"})
        if "action.click" in script:
            self.calls.append("click")
            self.clicked = True
            return True
        if "unable to accept invite" in script:
            return json.dumps({
                "error": self.page_error,
                "captcha": False,
                "body": f"bbchallenge\n{self.page_error}" if self.page_error else "",
            })
        raise AssertionError(f"Unexpected invite script: {script[:80]}")

    def network_capture_on(self, tab_id, url_prefix):
        self.calls.append(("capture", tab_id, url_prefix))

    def network_clear(self, tab_id):
        self.calls.append("clear")
        self.cleared = tab_id

    def network_list(self, tab_id):
        return [{
            "id": 5,
            "url": "https://discord.com/api/v9/invites/example",
            "method": "POST",
            "status": 200,
            "complete": True,
        }]

    def network_body(self, tab_id, request_id):
        return '{"guild":{"id":"1","name":"Example"}}'

    def close_tab(self, tab_id):
        self.closed = tab_id


class VimbrowserCaptchaTests(unittest.TestCase):
    def test_solves_in_account_specific_isolated_context(self):
        ipc = FakeIpc()
        with patch.object(captcha, "_context_name", return_value="discord-paramount"):
            token = captcha.solve_captcha(CAPTCHA_PAYLOAD, ipc=ipc, timeout_secs=2)

        self.assertEqual(token, "solution-token")
        self.assertTrue(ipc.required)
        self.assertEqual(
            ipc.opened,
            ("discord-paramount", "https://discord.com/login"),
        )
        self.assertIn("request-data", ipc.bootstrap_script)
        self.assertEqual(ipc.closed, 17)

    def test_missing_browser_error_mentions_install_and_open_states(self):
        missing = Path("/definitely/not/a/vimbrowser/socket")
        with patch.object(captcha, "_candidate_socket_paths", return_value=[missing]):
            with self.assertRaises(captcha.VimbrowserUnavailable) as raised:
                captcha.VimbrowserIpc.detect()
        message = str(raised.exception)
        self.assertIn("could not be found", message)
        self.assertIn("not be installed", message)
        self.assertIn("not be open", message)

    def test_context_tab_feature_is_required(self):
        client = captcha.VimbrowserIpc(Path("/tmp/unused.sock"))
        with patch.object(client, "send_json", return_value={"commands": []}):
            with self.assertRaisesRegex(
                captcha.VimbrowserUnavailable,
                "isolated context tabs",
            ):
                client.require_context_tabs()

    def test_account_alias_is_converted_to_valid_context_name(self):
        with patch.object(captcha, "selected_alias", return_value="Personal.Profile"):
            self.assertEqual(captcha._context_name(), "discord-personal-profile")

        long_alias = "a" * 64
        with patch.object(captcha, "selected_alias", return_value=long_alias):
            context = captcha._context_name()
        self.assertLessEqual(len(context), 48)
        self.assertRegex(context, r"^[a-z0-9][a-z0-9_-]*$")

    def test_invite_uses_real_discord_ui_in_isolated_context(self):
        ipc = FakeInviteIpc()
        with patch.object(captcha, "_context_name", return_value="discord-paramount"):
            result = captcha.complete_invite(
                "example",
                "account-token",
                ipc=ipc,
                timeout_secs=2,
            )
        self.assertEqual(result.status, 200)
        self.assertEqual(result.body["guild"]["name"], "Example")
        self.assertEqual(ipc.opened, ("discord-paramount", "https://discord.com/login"))
        self.assertIn("account-token", ipc.bootstrap)
        self.assertEqual(ipc.cleared, 23)
        self.assertTrue(ipc.clicked)
        self.assertEqual(ipc.closed, 23)

    def test_invite_turns_network_capture_on_before_clicking(self):
        # vimbrowser records nothing until capture is on, which left the CLI
        # blind to Discord's answer and waiting out its whole timeout.
        ipc = FakeInviteIpc()
        with patch.object(captcha, "_context_name", return_value="discord-paramount"):
            captcha.complete_invite("example", "account-token", ipc=ipc, timeout_secs=2)
        self.assertEqual(
            ipc.calls,
            [("capture", 23, "https://discord.com/api/"), "clear", "click"],
        )

    def test_invite_refusal_is_returned_from_any_api_version(self):
        ipc = FakeInviteIpc()
        ipc.network_list = lambda tab_id: [
            {
                "id": 3,
                "url": "https://discord.com/api/v9/invites/examplelonger",
                "method": "POST",
                "status": 200,
                "complete": True,
            },
            {
                "id": 4,
                "url": "https://discord.com/api/v10/invites/example?inputValue=example",
                "method": "POST",
                "status": 403,
                "complete": True,
            },
        ]
        ipc.network_body = lambda tab_id, request_id: (
            '{"message":"Missing Access","code":50001}'
        )
        with patch.object(captcha, "_context_name", return_value="discord-paramount"):
            result = captcha.complete_invite("example", "account-token", ipc=ipc, timeout_secs=2)
        self.assertEqual(result.status, 403)
        self.assertEqual(result.body["code"], 50001)

    def test_invite_page_error_fails_fast_when_response_is_not_captured(self):
        ipc = FakeInviteIpc()
        ipc.network_list = lambda tab_id: []
        ipc.page_error = "Unable to accept invite"
        with (
            patch.object(captcha, "_context_name", return_value="discord-paramount"),
            patch.object(captcha, "_PAGE_ERROR_GRACE_SECS", 0.0),
        ):
            with self.assertRaisesRegex(
                captcha.VimbrowserCaptchaError,
                "refused the invite.*Unable to accept invite",
            ):
                captcha.complete_invite("example", "account-token", ipc=ipc, timeout_secs=30)
        self.assertEqual(ipc.closed, 23)

    def test_invite_membership_is_not_polled_every_second(self):
        ipc = FakeInviteIpc()
        ipc.network_list = lambda tab_id: []
        clock = [1000.0]
        checks = []

        def membership_check():
            checks.append(clock[0])
            return None

        def fake_sleep(seconds):
            clock[0] += seconds

        with (
            patch.object(captcha, "_context_name", return_value="discord-paramount"),
            patch.object(captcha.time, "monotonic", side_effect=lambda: clock[0]),
            patch.object(captcha.time, "sleep", side_effect=fake_sleep),
        ):
            with self.assertRaisesRegex(captcha.VimbrowserCaptchaError, "timed out"):
                captcha.complete_invite(
                    "example",
                    "account-token",
                    ipc=ipc,
                    timeout_secs=30,
                    membership_check=membership_check,
                )
        self.assertLessEqual(len(checks), 30 / captcha._MEMBERSHIP_POLL_SECS + 1)
        gaps = [b - a for a, b in zip(checks, checks[1:])]
        self.assertTrue(all(gap >= captcha._MEMBERSHIP_POLL_SECS for gap in gaps))

    def test_invite_redirect_success_is_detected_from_membership(self):
        ipc = FakeInviteIpc()
        ipc.network_list = lambda tab_id: []
        checks = []

        def membership_check():
            checks.append(True)
            return {"id": "guild-1", "name": "Example"}

        with patch.object(captcha, "_context_name", return_value="discord-paramount"):
            result = captcha.complete_invite(
                "example",
                "account-token",
                ipc=ipc,
                timeout_secs=2,
                expected_guild_id="guild-1",
                expected_guild_name="Example",
                membership_check=membership_check,
            )
        self.assertEqual(result.status, 200)
        self.assertEqual(result.body, {"guild": {"id": "guild-1", "name": "Example"}})
        self.assertTrue(checks)
        self.assertEqual(ipc.closed, 23)


class ApiCaptchaFallbackTests(unittest.TestCase):
    def setUp(self):
        # Never read the real account registry or token.
        headers = patch.object(api, "_headers", return_value={"Authorization": "test-token"})
        headers.start()
        self.addCleanup(headers.stop)

    def test_replays_request_in_vimbrowser_and_returns_response(self):
        raw = json.dumps(CAPTCHA_PAYLOAD).encode()
        with (
            patch.object(
                captcha,
                "complete_invite",
                return_value=captcha.BrowserReplayResult(
                    status=200,
                    text='{"guild":{"id":"1"}}',
                    body={"guild": {"id": "1"}},
                ),
            ) as complete,
            patch.object(
                api,
                "get",
                return_value={"guild": {"id": "1", "name": "Example"}},
            ),
            patch("src.accounts.audit_event") as audit,
        ):
            handled, result = api._maybe_retry_with_vimbrowser_captcha(
                "POST",
                "/invites/example",
                body={},
                status=400,
                raw=raw,
            )

        self.assertTrue(handled)
        self.assertEqual(result, {"guild": {"id": "1"}})
        complete.assert_called_once()
        self.assertEqual(complete.call_args.args[0], "example")
        self.assertTrue(complete.call_args.args[1])
        self.assertEqual(complete.call_args.kwargs["expected_guild_id"], "1")
        self.assertEqual(complete.call_args.kwargs["expected_guild_name"], "Example")
        self.assertTrue(callable(complete.call_args.kwargs["membership_check"]))
        audit.assert_called_once_with(
            "discord-api:POST",
            target="/invites/example",
            result_id=None,
        )

    def test_rejected_browser_replay_is_reported(self):
        raw = json.dumps(CAPTCHA_PAYLOAD).encode()
        rejected = captcha.BrowserReplayResult(
            status=400,
            text='{"captcha_key":["invalid-response"]}',
            body={"captcha_key": ["invalid-response"]},
        )
        with (
            patch.object(captcha, "complete_invite", return_value=rejected),
            patch.object(api, "get", return_value={}),
        ):
            with self.assertRaisesRegex(RuntimeError, "replayed by vimbrowser"):
                api._maybe_retry_with_vimbrowser_captcha(
                    "POST", "/invites/example", body={}, status=400, raw=raw,
                )

    def test_unavailable_browser_error_retains_captcha_reason(self):
        raw = json.dumps(CAPTCHA_PAYLOAD).encode()
        unavailable = captcha.VimbrowserUnavailable(
            "vimbrowser could not be found; it may not be installed or may not be open"
        )
        with (
            patch.object(captcha, "complete_invite", side_effect=unavailable),
            patch.object(api, "get", return_value={}),
        ):
            with self.assertRaises(RuntimeError) as raised:
                api._maybe_retry_with_vimbrowser_captcha(
                    "POST",
                    "/invites/example",
                    body={},
                    status=400,
                    raw=raw,
                )
        message = str(raised.exception)
        self.assertIn("Discord requires a captcha (verification required)", message)
        self.assertIn("vimbrowser could not be found", message)
        self.assertIn("not be installed", message)
        self.assertIn("not be open", message)

    def test_non_captcha_response_is_not_handled(self):
        handled, result = api._maybe_retry_with_vimbrowser_captcha(
            "POST",
            "/messages",
            status=400,
            raw=b'{"message":"bad request"}',
        )
        self.assertFalse(handled)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
