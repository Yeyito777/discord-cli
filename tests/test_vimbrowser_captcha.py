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
            self.clicked = True
            return True
        raise AssertionError(f"Unexpected invite script: {script[:80]}")

    def network_clear(self, tab_id):
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
