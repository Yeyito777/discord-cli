import unittest

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

if __name__ == "__main__":
    unittest.main()
