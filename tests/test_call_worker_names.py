import unittest
from unittest.mock import MagicMock, patch

from src.calls.worker import DiscordCallWorker, _record_style_display_name


class CallWorkerDisplayNameTests(unittest.TestCase):
    def test_record_style_display_name_prefers_global_name_over_username(self):
        self.assertEqual(
            _record_style_display_name({"username": "ryu", "global_name": "Felipe Toro"}),
            "Felipe Toro",
        )

    def test_voice_state_ignores_guild_nick_for_display_name(self):
        worker = object.__new__(DiscordCallWorker)
        worker._participant_names = {}

        user_id = DiscordCallWorker._remember_voice_state_name(worker, {
            "user_id": "123",
            "member": {
                "nick": "server nickname",
                "user": {"id": "123", "username": "account_name", "global_name": "Display Name"},
            },
        })

        self.assertEqual(user_id, "123")
        self.assertEqual(worker._participant_names["123"], "Display Name")

    def test_voice_state_falls_back_to_username_when_global_name_absent(self):
        worker = object.__new__(DiscordCallWorker)
        worker._participant_names = {}

        user_id = DiscordCallWorker._remember_voice_state_name(worker, {
            "user_id": "456",
            "member": {
                "nick": "server nickname",
                "user": {"id": "456", "username": "account_name", "global_name": None},
            },
        })

        self.assertEqual(user_id, "456")
        self.assertEqual(worker._participant_names["456"], "account_name")

    def test_call_participants_use_immutable_ids_and_local_trust_labels(self):
        worker = object.__new__(DiscordCallWorker)
        worker._participant_names = {"123": "Owner Name", "456": "Friend Name", "789": "Unknown"}
        worker._active_participant_ids = {"123", "456", "789"}

        with patch("src.calls.worker.get_labels", return_value={
            "123": {"label": "owner"},
            "456": {"label": "friend"},
            "789": {"label": "anything-else"},
        }):
            participants = worker._call_participants()

        self.assertEqual(participants, [
            {"id": "123", "displayName": "Owner Name", "trust": "owner"},
            {"id": "456", "displayName": "Friend Name", "trust": "friend"},
            {"id": "789", "displayName": "Unknown", "trust": "untrusted"},
        ])

    def test_remote_voice_leave_republishes_the_reduced_participant_roster(self):
        worker = object.__new__(DiscordCallWorker)
        worker.my_id = "self"
        worker.channel_id = "voice-channel"
        worker._participant_names = {"123": "Owner", "456": "Friend"}
        worker._active_participant_ids = {"123", "456"}
        worker._sync_call_participants = MagicMock()

        worker._handle_voice_state_update({
            "user_id": "456",
            "channel_id": None,
            "user": {"id": "456", "username": "friend"},
        })

        worker._sync_call_participants.assert_called_once_with({"123"})

if __name__ == "__main__":
    unittest.main()
