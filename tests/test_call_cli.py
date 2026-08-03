import os
import unittest
from unittest.mock import patch

from src.calls import cli


class DiscordCallCliTests(unittest.TestCase):
    @patch.object(cli, "_spawn_detached_call")
    @patch.object(cli, "_resolve_call_target", return_value=("123", "456", "#voice"))
    def test_join_uses_exocortex_media_by_default(self, _resolve, spawn):
        cli.join([
            "voice",
            "--conversation", "conv-1",
            "--voice", "sol",
        ])

        spawn.assert_called_once()
        args, kwargs = spawn.call_args
        self.assertEqual(args, ("123", "456", "#voice"))
        self.assertFalse(kwargs["self_mute"])
        self.assertFalse(kwargs["self_deaf"])
        self.assertEqual(kwargs["exocortex_conversation"], "conv-1")
        self.assertEqual(kwargs["call_voice"], "sol")

    @patch.object(cli, "_spawn_detached_call")
    @patch.object(cli, "_resolve_call_target", return_value=("123", "456", "#voice"))
    def test_join_defaults_to_the_invoking_conversation_and_socket(self, _resolve, spawn):
        with patch.dict(os.environ, {
            "EXOCORTEX_PARENT_CONV_ID": "parent-conversation",
            "EXOCORTEX_SOCKET": "/tmp/exocortex-test.sock",
        }, clear=False):
            cli.join(["voice"])

        kwargs = spawn.call_args.kwargs
        self.assertEqual(kwargs["exocortex_conversation"], "parent-conversation")
        self.assertEqual(kwargs["exocortex_socket"], "/tmp/exocortex-test.sock")

    @patch.object(cli, "_resolve_call_target", return_value=("123", "456", "#voice"))
    def test_bidi_flag_is_not_a_public_mode(self, _resolve):
        with self.assertRaises(SystemExit):
            cli.join(["voice", "--bidi"])


if __name__ == "__main__":
    unittest.main()
