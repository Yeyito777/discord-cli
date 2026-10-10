"""Keep a channel's typing indicator alive until the next send or a deadline.

Discord shows typing for ~10 s per trigger. ``discord typing`` starts one
detached keeper per (account, channel) that re-triggers every few seconds.
Sending through this CLI stops the keeper first, so the indicator never
outlives the reply; otherwise it ends at its deadline.
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from src import api
from src.accounts import listener_dir

INTERVAL_SECONDS = 8
DEFAULT_SECONDS = 120
MAX_SECONDS = 900
MAX_CONSECUTIVE_ERRORS = 3

_MARKER = "discord-typing-keeper"
_ENTRY = "import sys; from src.typing_keeper import run; run(sys.argv[1:])"


def _pid_path(channel_id: str) -> Path:
    return listener_dir() / f"typing-{channel_id}.pid"


def _read_pid(channel_id: str) -> int | None:
    try:
        return int(_pid_path(channel_id).read_text().strip())
    except (OSError, ValueError):
        return None


def _is_keeper(pid: int, channel_id: str) -> bool:
    """True only for a live keeper process for this channel (never a reused PID or zombie)."""
    try:
        argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    except OSError:
        return False
    return _MARKER.encode() in argv and channel_id.encode() in argv


def _remove_pid_file(channel_id: str, pid: int) -> None:
    if _read_pid(channel_id) == pid:
        _pid_path(channel_id).unlink(missing_ok=True)


def stop(channel_id: str, *, wait_seconds: float = 2.0) -> bool:
    """Stop this channel's keeper if one is running. Returns whether one was stopped."""
    pid = _read_pid(channel_id)
    if pid is None:
        return False
    if not _is_keeper(pid, channel_id):
        _remove_pid_file(channel_id, pid)
        return False
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline and _is_keeper(pid, channel_id):
        time.sleep(0.05)
    _remove_pid_file(channel_id, pid)
    return True


def stop_quietly(channel_id: str) -> None:
    """Stop before sending; a keeper problem must never block the send itself."""
    try:
        stop(channel_id)
    except Exception:
        pass


def start(channel_id: str, seconds: float) -> None:
    """Show typing now and keep it alive for up to ``seconds`` or until the next send."""
    stop(channel_id)
    api.trigger_typing(channel_id)
    proc = subprocess.Popen(
        [sys.executable, "-c", _ENTRY, _MARKER, channel_id, f"{time.time() + seconds:.3f}"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    path = _pid_path(channel_id)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(f"{proc.pid}\n")
    os.replace(tmp, path)


def run(argv: list[str]) -> None:
    """Keeper process body: re-trigger until the deadline, SIGTERM or repeated errors."""
    _marker, channel_id, deadline = argv[0], argv[1], float(argv[2])
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    errors = 0
    try:
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return
            time.sleep(min(INTERVAL_SECONDS, remaining))
            if time.time() >= deadline:
                return
            try:
                # Never open an interactive CAPTCHA tab from a background loop.
                api.post(f"/channels/{channel_id}/typing", allow_captcha_fallback=False)
                errors = 0
            except RuntimeError as exc:
                errors += 1
                # 429 just skips this beat; anything else (auth, access) ends the keeper.
                if "HTTP 429" not in str(exc) or errors >= MAX_CONSECUTIVE_ERRORS:
                    return
    finally:
        _remove_pid_file(channel_id, os.getpid())
