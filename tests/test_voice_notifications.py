import json
import os
import subprocess
import unittest
from unittest.mock import Mock, patch

from src import voice
from src.gateway import GatewayListener

SIGNED = "https://cdn.discordapp.com/attachments/1/2/voice-message.ogg?ex=signed"


def listener():
    listener = object.__new__(GatewayListener)
    listener.account = {"alias": "paramount", "owner": "assistant"}
    listener.my_id = "assistant-1"
    listener._ensure_private_channel_meta = Mock(return_value={"channel_type": "dm", "channel_name": "Yeyito"})
    listener._write = Mock()
    listener._queue_notification = Mock()
    listener._log = Mock()
    listener.notification_source = {"id": "account:paramount:notifications"}
    listener._mark_notification_seen = Mock()
    listener._guilds = {}
    listener._channels = {}
    return listener


def dm(**fields):
    return {
        "id": "message-1",
        "timestamp": "2026-10-20T12:00:00Z",
        "channel_id": "dm-1",
        "author": {"id": "owner-1", "username": "yeyito777", "global_name": "Yeyito"},
        "content": "",
        "mentions": [],
        **fields,
    }


VOICE = {
    "id": "att-1", "filename": "voice-message.ogg", "content_type": "audio/ogg",
    "size": 9000, "duration_secs": 3.2, "waveform": "AAAA", "url": SIGNED,
}


def publish_text(l, event, transcript=None, fail=False):
    def fake(urls, content_type=None):
        if fail:
            raise RuntimeError("boom")
        return transcript

    with (patch("src.notify.get_labels", return_value={"owner-1": {"label": "owner"}}),
          patch("src.voice.transcribe_voice", side_effect=fake) as transcribe,
          patch("src.exocortex.publish_external_notification", return_value={"deliveries": []}) as publish):
        l._publish_notification(event)
        l._publish_notification(event)  # a retry reuses the first attempt
    return publish.call_args.args[3], transcribe


class VoiceNotificationTests(unittest.TestCase):
    def queued(self, l, message):
        l._on_notify("MESSAGE_CREATE", message)
        return l._queue_notification.call_args.args[0]

    def test_voice_message_is_transcribed_once_and_url_never_logged(self):
        l = listener()
        event = self.queued(l, dm(flags=8192, attachments=[VOICE]))
        self.assertNotIn("signed", l._write.call_args.args[0])
        text, transcribe = publish_text(l, event, transcript="call me when you land")
        transcribe.assert_called_once_with([SIGNED], "audio/ogg")
        self.assertEqual(text, "\n".join([
            "Yeyito [ch:dm-1]",
            "",
            "→ Yeyito <@owner-1> [owner]:",
            "🎤 call me when you land",
            "[msg:message-1]",
        ]))
        self.assertNotIn("[message]", text)
        self.assertNotIn("_voice_urls", event)

    def test_failed_transcription_keeps_attachment_line(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                l = listener()
                event = self.queued(l, dm(flags=8192, attachments=[VOICE]))
                text, _ = publish_text(l, event, transcript=None, fail=fail)
                self.assertIn(
                    "→ Yeyito <@owner-1> [owner]:\n"
                    "🎤 voice message (transcription failed)\n"
                    "[attachment:att-1] voice-message.ogg\n"
                    "[msg:message-1]",
                    text,
                )

    def test_image_only_dm_has_no_placeholder(self):
        l = listener()
        image = {"id": "att-2", "filename": "photo.png", "content_type": "image/png", "url": SIGNED}
        event = self.queued(l, dm(attachments=[image]))
        text, transcribe = publish_text(l, event)
        transcribe.assert_not_called()
        self.assertIn("→ Yeyito <@owner-1> [owner]:\n[attachment:att-2] photo.png\n[msg:message-1]", text)
        self.assertNotIn("[message]", text)
        self.assertNotIn("signed", json.dumps(event))

    def test_text_with_attachment_and_sticker(self):
        l = listener()
        image = {"id": "att-2", "filename": "photo.png", "content_type": "image/png"}
        event = self.queued(l, dm(content="look", attachments=[image], sticker_items=[{"id": "s", "name": "wave"}]))
        text, _ = publish_text(l, event)
        self.assertIn("→ Yeyito <@owner-1> [owner]:\nlook\n[attachment:att-2] photo.png\n[sticker] wave\n", text)

    def test_ordinary_audio_file_is_not_a_voice_message(self):
        song = {"id": "a", "filename": "song.ogg", "content_type": "audio/ogg"}
        self.assertFalse(voice.is_voice_attachment(song, 0))
        self.assertTrue(voice.is_voice_attachment(song, 8192))
        self.assertTrue(voice.is_voice_attachment({"waveform": "x"}, 0))


class TranscribeVoiceTests(unittest.TestCase):
    def fetch_ok(self, url, path, deadline):
        with open(path, "wb") as f:
            f.write(b"OggS")
        self.paths.append(path)

    def setUp(self):
        self.paths = []

    def test_runs_transcribe_and_cleans_temp_audio(self):
        runner = Mock(return_value=subprocess.CompletedProcess([], 0, stdout="hello\nthere\n", stderr=""))
        with patch("src.voice._fetch", side_effect=self.fetch_ok):
            self.assertEqual(voice.transcribe_voice([SIGNED], "audio/ogg; codecs=opus", runner=runner), "hello there")
        cmd = runner.call_args.args[0]
        self.assertEqual(cmd[:3], ["transcribe", "--mime-type", "audio/ogg"])
        self.assertLessEqual(runner.call_args.kwargs["timeout"], voice.TRANSCRIBE_TIMEOUT_SECONDS)
        self.assertFalse(os.path.exists(self.paths[0]))

    def test_timeout_failure_and_download_failure_return_none(self):
        timeout = Mock(side_effect=subprocess.TimeoutExpired("transcribe", 45))
        failed = Mock(return_value=subprocess.CompletedProcess([], 1, stdout="", stderr="err"))
        with patch("src.voice._fetch", side_effect=self.fetch_ok):
            self.assertIsNone(voice.transcribe_voice([SIGNED], runner=timeout))
            self.assertIsNone(voice.transcribe_voice([SIGNED], runner=failed))
        never = Mock()
        with patch("src.voice._fetch", side_effect=OSError("403")):
            self.assertIsNone(voice.transcribe_voice([SIGNED, SIGNED], runner=never))
        never.assert_not_called()
        self.assertTrue(all(not os.path.exists(p) for p in self.paths))


if __name__ == "__main__":
    unittest.main()
