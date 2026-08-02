import unittest
from unittest.mock import patch

from src.calls import cli


class DiscordBidiCliTests(unittest.TestCase):
    @patch.object(cli, "_spawn_detached_call")
    @patch.object(cli, "_configured_notify_targets", return_value=[])
    @patch.object(cli, "_resolve_call_target", return_value=("123", "456", "#voice"))
    def test_bidi_join_uses_unmuted_media_without_local_transcription(self, _resolve, _targets, spawn):
        cli.join([
            "voice",
            "--bidi",
            "--exo-conversation", "conv-1",
            "--voice", "sol",
        ])

        spawn.assert_called_once()
        args, kwargs = spawn.call_args
        self.assertEqual(args, ("123", "456", "#voice"))
        self.assertFalse(kwargs["self_mute"])
        self.assertFalse(kwargs["self_deaf"])
        self.assertFalse(kwargs["transcribe"])
        self.assertTrue(kwargs["bidi"])
        self.assertEqual(kwargs["exocortex_conversation"], "conv-1")
        self.assertEqual(kwargs["bidi_voice"], "sol")

    @patch.object(cli, "_resolve_call_target", return_value=("123", "456", "#voice"))
    def test_bidi_rejects_discord_self_deaf(self, _resolve):
        with self.assertRaisesRegex(SystemExit, "cannot be used with --deafened"):
            cli.join(["voice", "--bidi", "--deafened"])


if __name__ == "__main__":
    unittest.main()
