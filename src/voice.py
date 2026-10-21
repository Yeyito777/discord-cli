"""Voice-message transcription for relayed notifications.

Discord marks voice messages with the IS_VOICE_MESSAGE message flag and gives
their (single, audio/ogg) attachment ``waveform``/``duration_secs`` fields.
The audio is fetched into a temporary directory, transcribed with the
``transcribe`` CLI, and deleted; the whole attempt has one hard deadline so a
notification is never held up indefinitely.
"""

import os
import subprocess
import tempfile
import time
import urllib.request

VOICE_MESSAGE_FLAG = 1 << 13
TRANSCRIBE_TIMEOUT_SECONDS = 45
MAX_VOICE_BYTES = 25 * 1024 * 1024


def is_voice_attachment(attachment, message_flags=0):
    if not isinstance(attachment, dict):
        return False
    if "waveform" in attachment or "duration_secs" in attachment:
        return True
    try:
        flagged = bool(int(message_flags or 0) & VOICE_MESSAGE_FLAG)
    except (TypeError, ValueError):
        flagged = False
    return flagged and str(attachment.get("content_type") or "").startswith("audio/")


def _fetch(url, dest_path, deadline):
    from src.downloading import _CDN_HEADERS

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("voice download deadline passed")
    req = urllib.request.Request(url, headers=_CDN_HEADERS)
    total = 0
    with urllib.request.urlopen(req, timeout=remaining) as resp, open(dest_path, "wb") as out:
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError("voice download deadline passed")
            chunk = resp.read(64 * 1024)
            if not chunk:
                return
            total += len(chunk)
            if total > MAX_VOICE_BYTES:
                raise ValueError("voice message is too large to transcribe")
            out.write(chunk)


def transcribe_voice(urls, content_type=None, timeout=TRANSCRIBE_TIMEOUT_SECONDS, runner=subprocess.run):
    """Return the transcript of the first downloadable URL, or None on any failure."""
    deadline = time.monotonic() + timeout
    with tempfile.TemporaryDirectory(prefix="discord-voice-") as tmp:
        path = os.path.join(tmp, "voice-message.ogg")
        for url in urls:
            try:
                _fetch(url, path, deadline)
                break
            except Exception:
                continue
        else:
            return None

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        cmd = ["transcribe"]
        if content_type:
            cmd += ["--mime-type", str(content_type).split(";")[0].strip()]
        cmd.append(path)
        try:
            result = runner(cmd, capture_output=True, text=True, timeout=remaining, stdin=subprocess.DEVNULL)
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            return None
        transcript = " ".join((result.stdout or "").split())
        return transcript or None
