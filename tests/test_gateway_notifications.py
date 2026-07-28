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
        listener.account = {"alias": "paramount"}
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
        listener.account = {"alias": "paramount"}
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
            "channel_type": "dm",
            "author_id": "friend-1",
            "author": "friend",
            "display_name": "Friend",
            "content": "hello",
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
            data={
                "schemaVersion": 1,
                "accountAlias": "paramount",
                "kind": "dm",
                "channel": {
                    "id": "ch-1",
                    "type": "dm",
                    "name": "",
                    "participants": [],
                    "participantIds": [],
                },
                "guild": None,
                "messageId": "message-1",
                "author": {
                    "id": "friend-1",
                    "username": "friend",
                    "displayName": "Friend",
                },
                "content": "hello",
                "mentionsAssistant": False,
                "replyTo": None,
            },
        )
        listener._mark_notification_seen.assert_called_once_with("ch-1", {"old-1"})
        self.assertEqual(result, {"delivered": 1})

    def test_failed_core_delivery_is_retryable_and_does_not_mark_context_seen(self):
        listener = object.__new__(GatewayListener)
        listener.notification_source = {
            "id": "account:paramount:notifications",
            "label": "Paramount · DMs and @mentions",
        }
        listener.account = {"alias": "paramount"}
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

    def test_structured_dm_data_preserves_multiline_content_and_reply_fields(self):
        listener = object.__new__(GatewayListener)
        listener.account = {"alias": "paramount"}

        data = listener._notification_data({
            "type": "dm",
            "channel_type": "dm",
            "channel_id": "ch-1",
            "channel_name": "Yeyito",
            "channel_participants": ["yeyito777"],
            "channel_participant_ids": ["owner-1"],
            "msg_id": "message-1",
            "author_id": "owner-1",
            "author": "yeyito777",
            "display_name": "Yeyito",
            "content": "first line\nsecond line",
            "mentions_assistant": False,
            "reply_to": {
                "msg_id": "old-1",
                "author_id": "assistant-1",
                "author": "paramount.available",
                "display_name": "Paramount",
                "content": "the complete referenced message",
            },
        })

        self.assertEqual(data["kind"], "dm")
        self.assertEqual(data["content"], "first line\nsecond line")
        self.assertEqual(data["author"]["id"], "owner-1")
        self.assertEqual(data["channel"]["participantIds"], ["owner-1"])
        self.assertEqual(data["replyTo"]["author"]["id"], "assistant-1")
        self.assertEqual(data["replyTo"]["content"], "the complete referenced message")

    def test_structured_server_mention_and_call_are_discriminated(self):
        listener = object.__new__(GatewayListener)
        listener.account = {"alias": "paramount"}

        mention = listener._notification_data({
            "type": "mention",
            "channel_type": "guild_text",
            "channel_id": "channel-1",
            "channel_name": "general",
            "guild_id": "guild-1",
            "guild_name": "raw mutton",
            "msg_id": "message-1",
            "author_id": "owner-1",
            "author": "yeyito777",
            "display_name": "Yeyito",
            "content": "@Paramount hello",
            "mentions_assistant": True,
        })
        call = listener._notification_data({
            "type": "call",
            "channel_type": "dm",
            "channel_id": "dm-1",
            "channel_name": "Yeyito",
            "caller": "yeyito777",
            "ringing_user_ids": ["assistant-1"],
            "voice_state_user_ids": ["owner-1"],
            "region": "us-east",
        })

        self.assertEqual(mention["kind"], "server_mention")
        self.assertEqual(mention["guild"], {"id": "guild-1", "name": "raw mutton"})
        self.assertTrue(mention["mentionsAssistant"])
        self.assertEqual(call["kind"], "call")
        self.assertEqual(call["call"]["voiceStateUserIds"], ["owner-1"])
        self.assertNotIn("content", call)


if __name__ == "__main__":
    unittest.main()
