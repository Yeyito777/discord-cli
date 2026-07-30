import contextlib
import io
import tempfile
import unittest
from unittest.mock import patch

from src import reading, writing
from src.payload import read_stdin_text


class StdinPayloadTests(unittest.TestCase):
    def test_reads_stdin_exactly_without_trimming_or_escape_decoding(self):
        payload = "líne one\r\n\r\n`code` $HOME \\n trailing space "
        parser = writing.argparse.ArgumentParser()
        stdin = io.TextIOWrapper(io.BytesIO(payload.encode("utf-8")), encoding="utf-8")
        with patch("sys.stdin", stdin):
            self.assertEqual(read_stdin_text(parser), payload)

    def test_required_empty_stdin_is_rejected(self):
        parser = writing.argparse.ArgumentParser()
        stderr = io.StringIO()
        with (
            patch("sys.stdin", io.StringIO("")),
            contextlib.redirect_stderr(stderr),
            self.assertRaises(SystemExit),
        ):
            read_stdin_text(parser, label="message text")
        self.assertIn("message text is required on stdin", stderr.getvalue())

    def test_send_reads_message_from_stdin(self):
        output = io.StringIO()
        with (
            patch("sys.stdin", io.StringIO("hello\nworld")),
            patch.object(writing, "resolve_channel", return_value={"id": "15"}),
            patch.object(writing.api, "send_message", return_value={"id": "20"}) as send,
            contextlib.redirect_stdout(output),
        ):
            writing.send(["12345678901234567"])

        send.assert_called_once_with("15", "hello\nworld", reply_to=None)
        self.assertIn("Message ID: 20", output.getvalue())

    def test_send_rejects_inline_message_argument(self):
        stderr = io.StringIO()
        with (
            patch.object(writing.api, "send_message") as send,
            contextlib.redirect_stderr(stderr),
            self.assertRaises(SystemExit),
        ):
            writing.send(["12345678901234567", "inline text"])

        send.assert_not_called()
        self.assertIn(
            "message text must be provided via stdin; inline text is not accepted",
            stderr.getvalue(),
        )

    def test_file_only_send_does_not_require_stdin(self):
        with tempfile.NamedTemporaryFile() as attachment:
            attachment_name = attachment.name
            with (
                patch("sys.stdin", io.StringIO("")),
                patch.object(writing, "resolve_channel", return_value={"id": "15"}),
                patch.object(
                    writing.api,
                    "send_message_with_files",
                    return_value={"id": "20"},
                ) as send,
            ):
                writing.send(["12345678901234567", "--file", attachment_name])

        send.assert_called_once_with("15", [attachment_name], content=None, reply_to=None)

    def test_reply_and_edit_read_stdin(self):
        with (
            patch("sys.stdin", io.StringIO("reply body")),
            patch.object(writing, "resolve_channel", return_value={"id": "15"}),
            patch.object(writing.api, "send_message", return_value={"id": "20"}) as reply,
        ):
            writing.reply(["12345678901234567", "12345678901234568"])
        reply.assert_called_once_with("15", "reply body", reply_to="12345678901234568")

        with (
            patch("sys.stdin", io.StringIO("replacement")),
            patch.object(writing.api, "edit_message") as edit,
        ):
            writing.edit(["12345678901234567", "12345678901234568"])
        edit.assert_called_once_with("12345678901234567", "12345678901234568", "replacement")

    def test_dm_send_reads_stdin_and_rejects_inline_value(self):
        with (
            patch("sys.stdin", io.StringIO("DM body")),
            patch.object(reading, "resolve_dm", return_value={"id": "15"}),
            patch.object(reading.api, "send_message", return_value={"id": "20"}) as send,
        ):
            reading.dm(["friend", "--send"])
        send.assert_called_once_with("15", "DM body")

        stderr = io.StringIO()
        with (
            patch.object(reading.api, "send_message") as send,
            contextlib.redirect_stderr(stderr),
            self.assertRaises(SystemExit),
        ):
            reading.dm(["friend", "--send", "inline text"])
        send.assert_not_called()
        self.assertIn(
            "DM text must be provided via stdin; inline text is not accepted",
            stderr.getvalue(),
        )


if __name__ == "__main__":
    unittest.main()
