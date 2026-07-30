import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DISCORD = PROJECT_ROOT / "bin" / "discord"


class AccountCliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config = Path(self.tmp.name)
        accounts = {
            "version": 1,
            "default_account": "paramount",
            "accounts": {
                "paramount": {
                    "user_id": "1",
                    "username": "paramount.available",
                    "global_name": "Paramount",
                    "owner": "assistant",
                    "access": "full",
                },
                "personal": {
                    "user_id": "2",
                    "username": "yeyito777",
                    "global_name": "Yeyito",
                    "owner": "user",
                    "access": "read-only",
                },
            },
        }
        (self.config / "accounts.json").write_text(json.dumps(accounts))

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args):
        env = os.environ.copy()
        env["DISCORD_CONFIG_DIR"] = str(self.config)
        return subprocess.run(
            [str(DISCORD), *args],
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
        )

    def test_lists_identity_and_ownership(self):
        result = self.run_cli("accounts")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("paramount — Paramount (@paramount.available) [assistant]", result.stdout)
        self.assertIn("personal — Yeyito (@yeyito777) [user]", result.stdout)

    def test_write_requires_explicit_account(self):
        result = self.run_cli("send", "12345678901234567", "hello")
        self.assertEqual(result.returncode, 1)
        self.assertIn("requires '-a/--account <alias>'", result.stderr)

    def test_thread_creation_requires_explicit_account(self):
        result = self.run_cli(
            "thread", "create", "12345678901234567", "example thread",
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("requires '-a/--account <alias>'", result.stderr)

    def test_notification_subscription_requires_explicit_account(self):
        result = self.run_cli("notify", "subscribe", "conv-1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("requires '-a/--account <alias>'", result.stderr)

    def test_abbreviated_dm_send_requires_explicit_account(self):
        result = self.run_cli("dm", "somebody", "--sen", "hello")
        self.assertEqual(result.returncode, 1)
        self.assertIn("requires '-a/--account <alias>'", result.stderr)

    def test_read_only_account_rejects_write_before_network(self):
        result = self.run_cli("send", "-a", "personal", "12345678901234567", "hello")
        self.assertEqual(result.returncode, 1)
        self.assertIn("is read-only", result.stderr)

    def test_explicit_read_prints_identity_banner(self):
        result = self.run_cli("me", "-a", "personal")
        self.assertEqual(result.returncode, 1)  # credentials are intentionally absent
        self.assertIn("Discord account: personal — Yeyito (@yeyito777) [user]", result.stderr)

    def test_help_does_not_require_account_or_write_access(self):
        result = self.run_cli("send", "--help")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Send a message", result.stdout)

    def test_global_account_option_is_supported(self):
        result = self.run_cli("-a", "personal", "me")
        self.assertEqual(result.returncode, 1)  # credentials are intentionally absent
        self.assertIn("Discord account: personal", result.stderr)

    def test_duplicate_account_selectors_are_rejected(self):
        result = self.run_cli("-a", "paramount", "me", "-a", "personal")
        self.assertEqual(result.returncode, 2)
        self.assertIn("account may only be selected once", result.stderr)

    def test_write_access_elevation_requires_confirmation(self):
        result = self.run_cli("account", "access", "personal", "full")
        self.assertEqual(result.returncode, 1)
        self.assertIn("requires --yes", result.stderr)


if __name__ == "__main__":
    unittest.main()
