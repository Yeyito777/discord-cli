import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src import accounts, api


REGISTRY = {
    "version": 1,
    "default_account": "bot",
    "notify_account": "bot",
    "accounts": {
        "bot": {"user_id": "1", "username": "bot", "owner": "assistant", "access": "full"},
        "me": {"user_id": "2", "username": "me", "owner": "user", "access": "read-only"},
    },
}


class MutationGateTests(unittest.TestCase):
    """The account policy is enforced where requests are sent, not from argv."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        (root / "accounts.json").write_text(json.dumps(REGISTRY))
        for item in (
            patch.object(accounts, "CONFIG_ROOT", root),
            patch.object(accounts, "ACCOUNTS_ROOT", root / "accounts"),
            patch.object(accounts, "REGISTRY_FILE", root / "accounts.json"),
            patch.object(accounts, "LEGACY_CREDENTIALS_FILE", root / "credentials.json"),
            patch.dict(os.environ, {}),
            patch.object(api, "_headers", return_value={"Authorization": "test-token"}),
            patch.object(api, "_get_connection"),
        ):
            item.start()
            self.addCleanup(item.stop)
        os.environ.pop(accounts.ACCOUNT_EXPLICIT_ENV, None)
        os.environ[accounts.ACCOUNT_ENV] = "bot"

    def assert_refused(self, call, message):
        with self.assertRaisesRegex(accounts.AccountError, message):
            call()
        api._get_connection.assert_not_called()

    def test_writes_without_an_explicit_account_never_reach_discord(self):
        for call in (
            lambda: api.send_message("123456789012345678", "hi"),
            lambda: api.add_reaction("1", "2", "👍"),
            lambda: api.delete_message("1", "2"),
            lambda: api.edit_message("1", "2", "x"),
            lambda: api.trigger_typing("1"),
            lambda: api.post_once("/interactions", body={}),
        ):
            with self.subTest(call=call):
                with patch("src.typing_keeper.stop_quietly"):
                    self.assert_refused(call, "-a/--account")

    def test_read_only_account_is_refused_even_when_explicit(self):
        os.environ[accounts.ACCOUNT_ENV] = "me"
        os.environ[accounts.ACCOUNT_EXPLICIT_ENV] = "1"
        with patch("src.typing_keeper.stop_quietly"):
            self.assert_refused(lambda: api.send_message("1", "hi"), "read-only")

    def test_explicit_full_account_may_write(self):
        os.environ[accounts.ACCOUNT_EXPLICIT_ENV] = "1"
        accounts.ensure_mutation_allowed("POST /channels/1/messages")

    def test_reads_and_url_refreshes_are_not_gated(self):
        conn = Mock()
        conn.getresponse.return_value = Mock(status=200, read=Mock(return_value=b'{"refreshed_urls": []}'))
        api._get_connection.return_value = [conn, True, 0]
        api.refresh_attachment_urls(["https://cdn.discordapp.com/attachments/1/2/a.gif"])
        api.get("/users/@me")
        self.assertEqual(conn.request.call_count, 2)
