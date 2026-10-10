"""Detached, account+channel-scoped Gateway lifecycle; no conversation routing."""

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from src.accounts import listener_dir

PREFIX = "__channel_notify__"
PROJECT_DIR = Path(__file__).resolve().parent.parent


def paths(channel_id):
    if not re.fullmatch(r"\d{17,20}", channel_id):
        raise ValueError("Expected a channel snowflake")
    root = listener_dir()
    return {suffix: root / f"{PREFIX}{channel_id}.{suffix}"
            for suffix in ("pid", "lock", "log", "err", "state")}


def owned_pid(channel_id):
    files = paths(channel_id)
    try:
        pid = int(files["pid"].read_text().strip())
        # macOS has no /proc; ps works on both supported Unix platforms.
        command = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "command="], text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        expected = f"{PROJECT_DIR / 'src' / 'gateway.py'} {PREFIX}{channel_id} {files['log']}"
        return pid if expected in command else None
    except (ValueError, OSError, subprocess.CalledProcessError):
        return None


def listener_status(channel_id):
    pid = owned_pid(channel_id)
    if not pid:
        return "stopped"
    try:
        state = json.loads(paths(channel_id)["state"].read_text())
        status = state["status"] if state.get("pid") == pid else "starting"
    except (OSError, ValueError, KeyError):
        status = "starting"
    return f"{status} (PID {pid})"


def start_listener(channel_id, *, timeout=30):
    files = paths(channel_id)
    pid = owned_pid(channel_id)
    proc = None
    if not pid:
        with files["err"].open("a") as errors:
            proc = subprocess.Popen(
                [sys.executable, str(PROJECT_DIR / "src" / "gateway.py"),
                 f"{PREFIX}{channel_id}", str(files["log"])],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=errors,
                start_new_session=True,
            )
        # The child owns its PID file under an exclusive lock. Concurrent starts
        # cannot overwrite the winner's identity.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = listener_status(channel_id)
        if status.startswith("ready "):
            print(f"  Channel {channel_id}: {status}")
            return
        if proc and proc.poll() is not None and not owned_pid(channel_id):
            raise RuntimeError(f"Channel listener exited; inspect {files['err']}")
        time.sleep(0.1)
    raise RuntimeError(
        f"Channel listener not ready ({listener_status(channel_id)}); "
        f"it may still be reconnecting. Inspect {files['err']} or retry start."
    )


def stop_listener(channel_id):
    from src.notify import _stop_notify_pids
    pid = owned_pid(channel_id)
    if pid:
        _, alive = _stop_notify_pids([pid])
        if alive:
            raise RuntimeError(f"Failed to stop channel listener PID {pid}")
    print(f"  Stopped channel {channel_id} listener; core subscriptions retained.")
