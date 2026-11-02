import contextlib
import io
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from src import api, channels, writing

FORUM = {"id": "15", "type": 15, "name": "ideas", "guild_id": "7",
         "available_tags": [{"id": "1", "name": "BB(6)"}, {"id": "2", "name": "Cryptid"}]}


def run_post(argv, text="hello", channel=FORUM):
    with tempfile.TemporaryFile("w+b") as stdin:
        stdin.write(text.encode())
        stdin.seek(0)
        out = io.StringIO()
        with (
            patch.object(writing, "resolve_channel", return_value=channel),
            patch.object(writing, "resolve_guild", return_value={"id": "7"}),
            patch.object(writing.api, "create_forum_post", return_value={"id": "99", "name": "Title"}) as create,
            patch("sys.stdin", io.TextIOWrapper(stdin)),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            writing.post(argv)
    return create, out.getvalue()


class ForumPostApiTests(unittest.TestCase):
    def test_text_post_nests_the_first_message_and_is_never_retried(self):
        with patch.object(api, "post_once", return_value={"id": "9"}) as post:
            api.create_forum_post("15", "Title", "hello", applied_tags=["1"])
        post.assert_called_once_with(
            "/channels/15/threads",
            body={"name": "Title", "message": {"content": "hello"}, "applied_tags": ["1"]},
            params={"use_nested_fields": "true"},
        )

    def test_files_go_in_the_nested_message_and_are_never_retried(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "card.png")
            with open(path, "wb") as f:
                f.write(b"png")
            with patch.object(api, "_request", return_value={"id": "9"}) as request:
                api.create_forum_post("15", "Title", "hello", file_paths=[path])
        kwargs = request.call_args.kwargs
        self.assertEqual(request.call_args.args, ("POST", "/channels/15/threads"))
        self.assertFalse(kwargs["retry_connection_failure"])
        self.assertEqual(kwargs["params"], {"use_nested_fields": "true"})
        self.assertIn(b'name="files[0]"; filename="card.png"', kwargs["body_bytes"])
        start = kwargs["body_bytes"].index(b"{")
        payload = json.JSONDecoder().raw_decode(kwargs["body_bytes"][start:].decode())[0]
        self.assertEqual(payload["message"]["attachments"], [{"id": 0, "filename": "card.png"}])


class PostCommandTests(unittest.TestCase):
    def test_posts_with_tags_by_name_and_prints_the_link(self):
        create, out = run_post(["ideas", "Title", "-g", "srv", "--tag", "cryptid"])
        create.assert_called_once_with("15", "Title", "hello", file_paths=None, applied_tags=["2"])
        self.assertIn("Thread ID: 99", out)
        self.assertIn("https://discord.com/channels/7/99", out)

    def test_unknown_tag_is_refused_before_anything_is_sent(self):
        with patch.object(writing.api, "create_forum_post") as create, self.assertRaises(SystemExit):
            run_post(["ideas", "Title", "--tag", "nope"])
        create.assert_not_called()

    def test_required_tag_is_enforced(self):
        with self.assertRaises(SystemExit):
            run_post(["ideas", "Title"], channel={**FORUM, "flags": 16})

    def test_text_channel_is_refused(self):
        with self.assertRaises(SystemExit):
            run_post(["general", "Title"], channel={"id": "3", "type": 0})

    def test_long_title_is_refused(self):
        with self.assertRaises(SystemExit):
            run_post(["ideas", "x" * 101])

    def test_inline_text_is_refused(self):
        with self.assertRaises(SystemExit):
            run_post(["ideas", "Title", "inline text"])


class ForumChannelTests(unittest.TestCase):
    def create(self, argv):
        out = io.StringIO()
        with (
            patch.object(channels, "resolve_guild", return_value={"id": "7"}),
            patch.object(channels.api, "get_guild_channels", return_value=[]),
            patch.object(channels.api, "create_guild_channel", return_value={"id": "5", "name": "ideas"}) as create,
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            channels.create(argv)
        return create

    def test_forum_takes_guidelines_and_tags(self):
        create = self.create(["srv", "ideas", "--type", "forum", "--topic", "be kind", "--tag", "BB(6)", "--tag", "Cryptid"])
        create.assert_called_once_with("7", "ideas", channel_type=15, topic="be kind", parent_id=None,
                                       tags=["BB(6)", "Cryptid"])

    def test_guidelines_can_be_longer_than_a_topic(self):
        self.assertEqual(self.create(["srv", "ideas", "--type", "forum", "--topic", "x" * 4096]).call_count, 1)
        with self.assertRaises(SystemExit):
            self.create(["srv", "ideas", "--type", "text", "--topic", "x" * 1025])

    def test_tags_are_forum_only(self):
        with self.assertRaises(SystemExit):
            self.create(["srv", "ideas", "--tag", "x"])

    def test_available_tags_are_sent(self):
        with patch.object(api, "post_once", return_value={"id": "5"}) as post:
            api.create_guild_channel("7", "ideas", channel_type=15, tags=["a"])
        self.assertEqual(post.call_args.kwargs["body"]["available_tags"], [{"name": "a"}])


if __name__ == "__main__":
    unittest.main()
