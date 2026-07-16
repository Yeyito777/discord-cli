import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import notify


ACCOUNT = {
    "alias": "paramount",
    "user_id": "123",
    "username": "paramount.available",
    "global_name": "Paramount",
    "owner": "assistant",
    "access": "full",
}
SOURCE = {
    "id": "account:paramount:notifications",
    "label": "paramount — Paramount (@paramount.available) [assistant] · DMs and @mentions",
}


class NotifyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config_dir = Path(self.tmp.name)
        self.config_file = self.config_dir / "notify.json"
        self.config_patches = [
            patch.object(notify, "CONFIG_DIR", self.config_dir),
            patch.object(notify, "CONFIG_FILE", self.config_file),
        ]
        for config_patch in self.config_patches:
            config_patch.start()

    def tearDown(self):
        for config_patch in reversed(self.config_patches):
            config_patch.stop()
        self.tmp.cleanup()

    def _write_config(self, value):
        self.config_file.write_text(json.dumps(value))

    def test_source_is_account_scoped_and_uses_account_label(self):
        self.assertEqual(notify.notification_source(ACCOUNT), SOURCE)

    def test_notify_help_describes_new_subscription_ux(self):
        with patch("sys.stdout", new_callable=io.StringIO) as stdout:
            notify.dispatch("notify", ["--help"])
        output = stdout.getvalue()
        self.assertIn("subscribe|unsubscribe|list", output)
        self.assertIn("--delivery wake|inbox", output)
        self.assertIn("Aliases for subscribe/unsubscribe", output)

    def test_subscribe_uses_core_registry_and_wake_default(self):
        with (
            patch.object(notify, "notification_source", return_value=SOURCE),
            patch.object(
                notify,
                "subscribe_external_notification",
                return_value={"id": "sub-1"},
            ) as subscribe,
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            notify.subscribe(["conv-1"])

        subscribe.assert_called_once_with(
            "discord",
            SOURCE["id"],
            "conv-1",
            "wake",
            source_label=SOURCE["label"],
        )
        self.assertIn("Subscribed conv-1 (wake) [sub-1]", stdout.getvalue())

    def test_add_and_remove_remain_aliases(self):
        with (
            patch.object(notify, "notification_source", return_value=SOURCE),
            patch.object(notify, "subscribe_external_notification", return_value={}) as subscribe,
            patch.object(notify, "unsubscribe_external_notification", return_value=[]) as unsubscribe,
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            notify.dispatch("notify", ["add", "conv-1", "--delivery", "inbox"])
            notify.dispatch("notify", ["remove", "conv-1"])

        self.assertEqual(subscribe.call_args.args[3], "inbox")
        unsubscribe.assert_called_once_with(
            tool_name="discord", source_id=SOURCE["id"], conv_id="conv-1"
        )

    def test_list_reads_subscriptions_from_core_but_labels_from_file(self):
        self._write_config({
            "labels": {
                "42": {"label": "friend", "username": "someone", "name": "Someone"}
            }
        })
        with (
            patch.object(notify, "notification_source", return_value=SOURCE),
            patch.object(
                notify,
                "list_external_notification_subscriptions",
                return_value=[{"id": "sub-1", "convId": "conv-1", "delivery": "inbox"}],
            ) as list_subscriptions,
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            notify.list_config([])

        list_subscriptions.assert_called_once_with(
            tool_name="discord", source_id=SOURCE["id"]
        )
        output = stdout.getvalue()
        self.assertIn("conv-1 (inbox) [sub-1]", output)
        self.assertIn("Sender labels (local)", output)
        self.assertIn("@someone (Someone) [42] → friend", output)

    def test_migration_imports_only_missing_targets_then_removes_key(self):
        self._write_config({
            "relay_targets": ["conv-existing", "conv-new", "conv-new"],
            "labels": {"42": {"label": "friend"}},
        })
        with (
            patch.object(notify, "notification_source", return_value=SOURCE),
            patch.object(
                notify,
                "list_external_notification_subscriptions",
                return_value=[{"convId": "conv-existing", "delivery": "inbox"}],
            ),
            patch.object(notify, "subscribe_external_notification", return_value={"id": "sub-new"}) as subscribe,
        ):
            imported = notify._migrate_relay_targets()

        self.assertEqual(imported, 1)
        subscribe.assert_called_once_with(
            "discord",
            SOURCE["id"],
            "conv-new",
            "wake",
            source_label=SOURCE["label"],
        )
        migrated = json.loads(self.config_file.read_text())
        self.assertNotIn("relay_targets", migrated)
        self.assertEqual(migrated["labels"], {"42": {"label": "friend"}})

    def test_migration_retains_entire_key_after_partial_import_failure(self):
        original = {
            "relay_targets": ["conv-one", "conv-two"],
            "labels": {"42": {"label": "friend"}},
        }
        self._write_config(original)
        with (
            patch.object(notify, "notification_source", return_value=SOURCE),
            patch.object(notify, "list_external_notification_subscriptions", return_value=[]),
            patch.object(
                notify,
                "subscribe_external_notification",
                side_effect=[{"id": "sub-one"}, RuntimeError("core unavailable")],
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "core unavailable"):
                notify._migrate_relay_targets()

        self.assertEqual(json.loads(self.config_file.read_text()), original)

    def test_migration_retry_skips_already_imported_target(self):
        self._write_config({"relay_targets": ["conv-one"], "labels": {}})
        with (
            patch.object(notify, "notification_source", return_value=SOURCE),
            patch.object(
                notify,
                "list_external_notification_subscriptions",
                return_value=[{"convId": "conv-one", "delivery": "wake"}],
            ),
            patch.object(notify, "subscribe_external_notification") as subscribe,
        ):
            imported = notify._migrate_relay_targets()

        self.assertEqual(imported, 0)
        subscribe.assert_not_called()
        self.assertNotIn("relay_targets", json.loads(self.config_file.read_text()))

    def test_migration_keeps_empty_labels_config_external(self):
        self._write_config({"relay_targets": []})
        with (
            patch.object(notify, "notification_source", return_value=SOURCE),
            patch.object(notify, "list_external_notification_subscriptions", return_value=[]),
        ):
            notify._migrate_relay_targets()
        self.assertEqual(json.loads(self.config_file.read_text()), {"labels": {}})


if __name__ == "__main__":
    unittest.main()
