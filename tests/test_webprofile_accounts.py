import unittest
from unittest.mock import Mock, patch

from src import webprofile


class WebProfileAccountTests(unittest.TestCase):
    def test_matching_browser_identity_is_reused(self):
        page = Mock()
        with (
            patch.object(webprofile, "selected_account", return_value={"user_id": "1"}),
            patch.object(webprofile, "authenticated_user_id", return_value="1"),
            patch.object(webprofile, "inject_token") as inject,
        ):
            changed = webprofile.ensure_logged_in(page, "token")
        self.assertFalse(changed)
        inject.assert_not_called()

    def test_mismatched_browser_identity_is_replaced_and_rechecked(self):
        page = Mock()
        with (
            patch.object(webprofile, "selected_account", return_value={"user_id": "1"}),
            patch.object(webprofile, "authenticated_user_id", side_effect=["2", "1"]),
            patch.object(webprofile, "inject_token") as inject,
        ):
            changed = webprofile.ensure_logged_in(page, "token")
        self.assertTrue(changed)
        inject.assert_called_once_with(page, "token")


if __name__ == "__main__":
    unittest.main()
