import unittest
from unittest.mock import patch

from src import invite


class InviteTests(unittest.TestCase):
    def test_extracts_supported_invite_forms(self):
        for value in (
            "example",
            "discord.gg/example",
            "https://discord.gg/example",
            "https://discord.com/invite/example",
            "https://ptb.discord.com/invite/example",
        ):
            with self.subTest(value=value):
                self.assertEqual(invite._extract_code(value), "example")

    def test_join_uses_rest_api(self):
        response = {"guild": {"id": "1", "name": "Example"}}
        with patch.object(invite.api, "post", return_value=response) as post:
            self.assertEqual(invite.join_server("discord.gg/example"), response)
        post.assert_called_once_with("/invites/example", body={})

    def test_rejects_unexpected_response(self):
        with patch.object(invite.api, "post", return_value={"message": "nope"}):
            with self.assertRaisesRegex(RuntimeError, "Unexpected response"):
                invite.join_server("example")


if __name__ == "__main__":
    unittest.main()
