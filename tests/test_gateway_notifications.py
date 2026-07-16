import unittest
from collections import defaultdict
from unittest.mock import Mock, patch

from src.gateway import GatewayListener


class GatewayNotificationTests(unittest.TestCase):
    def test_registers_account_source_with_core(self):
        listener = object.__new__(GatewayListener)
        listener.notification_source = {
            "id": "account:paramount:notifications",
            "label": "Paramount · DMs and @mentions",
        }
        listener._log = Mock()

        with patch("src.exocortex.register_external_notification_source") as register:
            listener._register_notification_source()

        register.assert_called_once_with("discord", listener.notification_source)

    def test_publishes_event_to_core_and_marks_context_seen(self):
        listener = object.__new__(GatewayListener)
        listener.notification_source = {
            "id": "account:paramount:notifications",
            "label": "Paramount · DMs and @mentions",
        }
        listener._format_notification_batch = Mock(
            return_value=("[Discord/paramount Notification] hello", defaultdict(set, {"ch-1": {"old-1"}}))
        )
        listener._mark_notification_seen = Mock()
        listener._log = Mock()
        notification = {
            "msg_id": "message-1",
            "ts": "2026-07-15T12:00:00Z",
            "channel_id": "ch-1",
            "type": "dm",
        }

        with patch(
            "src.exocortex.publish_external_notification", return_value={"delivered": 1}
        ) as publish:
            result = listener._publish_notification(notification)

        publish.assert_called_once_with(
            "discord",
            "account:paramount:notifications",
            "message-1",
            "[Discord/paramount Notification] hello",
            occurred_at=1784116800000,
        )
        listener._mark_notification_seen.assert_called_once_with("ch-1", {"old-1"})
        self.assertEqual(result, {"delivered": 1})

    def test_failed_core_delivery_is_retryable_and_does_not_mark_context_seen(self):
        listener = object.__new__(GatewayListener)
        listener.notification_source = {
            "id": "account:paramount:notifications",
            "label": "Paramount · DMs and @mentions",
        }
        listener._format_notification_batch = Mock(
            return_value=("notification", defaultdict(set, {"ch-1": {"old-1"}}))
        )
        listener._mark_notification_seen = Mock()
        listener._log = Mock()
        notification = {
            "msg_id": "message-1",
            "ts": "2026-07-15T12:00:00Z",
            "channel_id": "ch-1",
            "type": "dm",
        }

        with patch(
            "src.exocortex.publish_external_notification",
            return_value={"deliveries": [{"status": "failed", "message": "missing"}]},
        ):
            with self.assertRaisesRegex(RuntimeError, "rejected 1"):
                listener._publish_notification(notification)

        listener._mark_notification_seen.assert_not_called()

    def test_converts_discord_timestamps_to_epoch_milliseconds(self):
        self.assertEqual(
            GatewayListener._notification_occurred_at_ms("2026-07-15T12:00:00Z"),
            1784116800000,
        )
        self.assertIsNone(GatewayListener._notification_occurred_at_ms("not-a-time"))

    def test_call_events_have_stable_explicit_ids(self):
        self.assertEqual(
            GatewayListener._notification_event_id({"event_id": "call:123"}),
            "call:123",
        )
        self.assertEqual(
            GatewayListener._notification_event_id({
                "type": "call", "channel_id": "ch-1", "ts": "2026-07-15T12:00:00Z"
            }),
            "call:ch-1:2026-07-15T12:00:00Z",
        )

    def test_dm_event_is_queued_without_gateway_owned_targets(self):
        listener = object.__new__(GatewayListener)
        listener.my_id = "me"
        listener._ensure_private_channel_meta = Mock(
            return_value={"channel_type": "dm", "channel_name": "friend"}
        )
        listener._write = Mock()
        listener._queue_notification = Mock()
        listener._guilds = {}
        listener._channels = {}

        listener._on_notify("MESSAGE_CREATE", {
            "id": "message-1",
            "timestamp": "2026-07-15T12:00:00Z",
            "channel_id": "ch-1",
            "author": {"id": "friend-1", "username": "friend", "global_name": "Friend"},
            "content": "hello",
            "mentions": [],
        })

        listener._queue_notification.assert_called_once()
        queued = listener._queue_notification.call_args.args[0]
        self.assertEqual(queued["msg_id"], "message-1")
        self.assertEqual(queued["type"], "dm")


if __name__ == "__main__":
    unittest.main()
