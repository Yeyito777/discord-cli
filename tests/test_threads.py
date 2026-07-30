import contextlib
import io
import unittest
from unittest.mock import patch

from src import api, writing


class ThreadApiTests(unittest.TestCase):
    def test_create_public_thread_without_message(self):
        response = {"id": "20", "guild_id": "10", "name": "topic"}
        with patch.object(api, "post", return_value=response) as post:
            result = api.create_thread(
                "15",
                "topic",
                auto_archive_duration=4320,
                rate_limit_per_user=7,
            )

        self.assertEqual(result, response)
        post.assert_called_once_with(
            "/channels/15/threads",
            body={
                "name": "topic",
                "auto_archive_duration": 4320,
                "rate_limit_per_user": 7,
                "type": 11,
            },
        )

    def test_create_private_thread(self):
        with patch.object(api, "post", return_value={"id": "20"}) as post:
            api.create_thread("15", "private", thread_type=12, invitable=False)

        post.assert_called_once_with(
            "/channels/15/threads",
            body={
                "name": "private",
                "auto_archive_duration": 1440,
                "rate_limit_per_user": 0,
                "type": 12,
                "invitable": False,
            },
        )

    def test_create_message_thread_uses_message_endpoint(self):
        with patch.object(api, "post", return_value={"id": "20"}) as post:
            api.create_thread("15", "replies", message_id="18")

        post.assert_called_once_with(
            "/channels/15/messages/18/threads",
            body={
                "name": "replies",
                "auto_archive_duration": 1440,
                "rate_limit_per_user": 0,
            },
        )


class ThreadCliTests(unittest.TestCase):
    def test_create_resolves_parent_and_prints_result(self):
        response = {"id": "20", "guild_id": "10", "name": "topic"}
        output = io.StringIO()
        with (
            patch.object(writing, "resolve_guild", return_value={"id": "10"}),
            patch.object(writing, "resolve_channel", return_value={"id": "15"}) as resolve,
            patch.object(writing.api, "create_thread", return_value=response) as create,
            contextlib.redirect_stdout(output),
        ):
            writing.thread([
                "create", "general", "topic", "--guild", "server",
                "--auto-archive", "60", "--slowmode", "3",
            ])

        resolve.assert_called_once_with("general", "10")
        create.assert_called_once_with(
            "15",
            "topic",
            message_id=None,
            auto_archive_duration=60,
            thread_type=11,
            invitable=True,
            rate_limit_per_user=3,
        )
        self.assertIn("Created thread #topic. Thread ID: 20", output.getvalue())
        self.assertIn("https://discord.com/channels/10/20", output.getvalue())

    def test_private_message_thread_is_rejected(self):
        with self.assertRaises(SystemExit):
            writing.thread([
                "create", "12345678901234567", "topic",
                "--private", "--message", "12345678901234568",
            ])

    def test_forum_parent_is_rejected_as_a_post_not_a_thread(self):
        with (
            patch.object(writing, "resolve_channel", return_value={"id": "15", "type": 15}),
            patch.object(writing.api, "create_thread") as create,
            self.assertRaises(SystemExit),
        ):
            writing.thread(["create", "12345678901234567", "topic"])

        create.assert_not_called()


if __name__ == "__main__":
    unittest.main()
