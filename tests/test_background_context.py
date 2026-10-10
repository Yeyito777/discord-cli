import unittest
from pathlib import Path
from unittest.mock import patch

from src.vimbrowser_captcha import VimbrowserIpc, VimbrowserCaptchaError


class BackgroundContextTests(unittest.TestCase):
    def test_does_not_use_unchanged_active_tab(self):
        client = VimbrowserIpc(Path("/tmp/not-used"))
        before = {"active_tabid": 8, "tabs": [{"id": 8, "context": None}]}
        after = {"active_tabid": 8, "tabs": [
            {"id": 8, "context": None},
            {"id": 11, "context": "discord-paramount"},
        ]}
        with patch.object(client, "send_json", side_effect=[
            before, {"ok": True, "active_tabid": 8}, after,
        ]) as send:
            self.assertEqual(client.open_context_tab("discord-paramount", "https://discord.com"), 11)
        self.assertEqual(send.call_args_list[1].args[0],
                         "open-background-context-tab discord-paramount https://discord.com")

    def test_rejects_ambiguous_new_tabs(self):
        client = VimbrowserIpc(Path("/tmp/not-used"))
        with patch.object(client, "send_json", side_effect=[
            {"tabs": []}, {"ok": True},
            {"tabs": [{"id": 1, "context": "discord-paramount"},
                      {"id": 2, "context": "discord-paramount"}]},
        ]):
            with self.assertRaises(VimbrowserCaptchaError):
                client.open_context_tab("discord-paramount", "https://discord.com")

    def test_rejects_wrong_context(self):
        client = VimbrowserIpc(Path("/tmp/not-used"))
        with patch.object(client, "send_json", side_effect=[
            {"tabs": []}, {"ok": True}, {"tabs": [{"id": 1, "context": "personal"}]},
        ]):
            with self.assertRaises(VimbrowserCaptchaError):
                client.open_context_tab("discord-paramount", "https://discord.com")
