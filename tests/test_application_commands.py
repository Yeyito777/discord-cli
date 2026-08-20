import contextlib
import http.client
import io
import unittest
from unittest.mock import patch

from src import api, application_commands as commands


INDEX = {
    "applications": [{"id": "1528294349442650202", "name": "Better Rhythm"}],
    "application_commands": [{
        "id": "1528316728344580186",
        "application_id": "1528294349442650202",
        "version": "1528316728629657802",
        "type": 1,
        "name": "play",
        "description": "Play a YouTube URL",
        "options": [{
            "type": 3,
            "name": "url",
            "description": "YouTube URL",
            "required": True,
        }],
    }, {
        "id": "1538287556641427638",
        "application_id": "1528294349442650202",
        "version": "1538287556641427640",
        "type": 1,
        "name": "autoplay",
        "description": "Configure autoplay",
        "options": [{
            "type": 1,
            "name": "on",
            "description": "Enable autoplay",
            "options": [],
        }],
    }],
}


@contextlib.contextmanager
def gateway_session():
    yield "gateway-session"


class ApplicationCommandTests(unittest.TestCase):
    def test_invokes_command_from_live_schema(self):
        output = io.StringIO()
        with (
            patch.object(commands, "resolve_channel", return_value={
                "id": "1492275915546427633",
                "guild_id": "1389428023832608861",
            }),
            patch.object(commands.api, "get", return_value=INDEX),
            patch.object(commands, "_gateway_session", gateway_session),
            patch.object(commands, "_nonce", return_value="nonce-1"),
            patch.object(commands.api, "post_once") as post,
            patch.object(commands, "audit_event") as audit,
            contextlib.redirect_stdout(output),
        ):
            commands.command([
                "1492275915546427633", "Better Rhythm", "play",
                "--url", "https://www.youtube.com/watch?v=q-74HTjRbuY",
            ])

        payload = post.call_args.kwargs["body"]
        self.assertEqual(post.call_args.args, ("/interactions",))
        self.assertEqual(payload["session_id"], "gateway-session")
        self.assertEqual(payload["application_id"], "1528294349442650202")
        self.assertEqual(payload["data"]["options"], [{
            "type": 3,
            "name": "url",
            "value": "https://www.youtube.com/watch?v=q-74HTjRbuY",
        }])
        audit.assert_called_once()
        self.assertIn("Invoked /play from Better Rhythm", output.getvalue())

    def test_wraps_subcommands(self):
        command = INDEX["application_commands"][1]
        self.assertEqual(commands._interaction_options(command, ["on"]), [{
            "type": 1,
            "name": "on",
        }])

    def test_rejects_missing_required_option_before_post(self):
        with (
            patch.object(commands, "resolve_channel", return_value={
                "id": "1492275915546427633",
                "guild_id": "1389428023832608861",
            }),
            patch.object(commands.api, "get", return_value=INDEX),
            patch.object(commands.api, "post_once") as post,
            self.assertRaises(RuntimeError),
        ):
            commands.command([
                "1492275915546427633", "Better Rhythm", "play",
            ])
        post.assert_not_called()

    def test_lists_apps_then_app_commands(self):
        with (
            patch.object(commands, "resolve_channel", return_value={
                "id": "1492275915546427633",
                "guild_id": "1389428023832608861",
            }),
            patch.object(commands.api, "get", return_value=INDEX),
        ):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                commands.commands(["1492275915546427633"])
            self.assertIn("Better Rhythm [1528294349442650202] — 2 commands", output.getvalue())

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                commands.commands(["1492275915546427633", "Better Rhythm"])
            self.assertIn("play --url VALUE", output.getvalue())
            self.assertIn("autoplay {on}", output.getvalue())

    def test_interaction_post_does_not_retry_an_ambiguous_disconnect(self):
        class BrokenConnection:
            def __init__(self):
                self.requests = 0

            def request(self, *_args, **_kwargs):
                self.requests += 1
                raise http.client.RemoteDisconnected("closed after request")

            def close(self):
                return None

        connection = BrokenConnection()
        entry = [connection, True, 0]
        with (
            patch.object(api, "_get_connection", return_value=entry),
            patch.object(api, "_build_headers", return_value={}),
            self.assertRaisesRegex(RuntimeError, "may have accepted"),
        ):
            api.post_once("/interactions", body={"type": 2})
        self.assertEqual(connection.requests, 1)


if __name__ == "__main__":
    unittest.main()
