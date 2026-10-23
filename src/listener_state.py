"""Notification Gateway health: state files, delivery cursors and bounded logs.

Every notification Gateway (``__notify__`` and ``__channel_notify__<id>``)
keeps a JSON state file next to its PID file.  Only the Gateway's main thread
writes it, at least every ``STATE_WRITE_SECONDS``, so a stale ``updated`` means
a wedged process, not a quiet channel.  The supervisor and ``discord notify
list|status`` read it.

The delivery cursor is the highest Discord message ID the Gateway has fully
handled.  It lives in the durable account config (not the runtime directory)
so a fresh Gateway session can fetch what arrived while nothing was listening.
"""

import json
import os
import subprocess
import time
from pathlib import Path

from src.accounts import account_root, listener_dir

PROJECT_DIR = Path(__file__).resolve().parent.parent
STATE_WRITE_SECONDS = 30
LOG_LIMIT_BYTES = 2 * 1024 * 1024


def gateway_paths(key, alias=None):
    root = listener_dir(alias)
    return {suffix: root / f"{key}.{suffix}" for suffix in ("pid", "lock", "log", "err", "state")}


def write_json_atomic(path, value):
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(value))
    tmp.replace(path)


def read_state(key, alias=None):
    try:
        value = json.loads(gateway_paths(key, alias)["state"].read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def owned_pid(key, alias=None):
    """The PID in ``<key>.pid`` if that process is this checkout's Gateway for ``key``."""
    files = gateway_paths(key, alias)
    try:
        pid = int(files["pid"].read_text().strip())
        # macOS has no /proc; ps works on both supported Unix platforms.
        command = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "command="], text=True, stderr=subprocess.DEVNULL,
        ).strip()
    except (ValueError, OSError, subprocess.CalledProcessError):
        return None
    expected = f"{PROJECT_DIR / 'src' / 'gateway.py'} {key} {files['log']}"
    return pid if expected in command else None


# ─── Delivery cursor ──────────────────────────────────────────────────────────


def cursor_path(key, alias=None):
    return account_root(alias) / "listener-cursors" / f"{key}.json"


def load_cursor(key, alias=None):
    try:
        value = json.loads(cursor_path(key, alias).read_text()).get("after")
    except (OSError, ValueError, AttributeError):
        return None
    return str(value) if str(value or "").isdigit() else None


def save_cursor(key, after, alias=None):
    path = cursor_path(key, alias)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, {"after": str(after), "saved": time.time()})


def snowflake_ms(snowflake):
    """Unix epoch milliseconds encoded in a Discord snowflake."""
    return (int(snowflake) >> 22) + 1420070400000


# ─── Bounded logs ─────────────────────────────────────────────────────────────


def rotate_if_large(path, limit=LOG_LIMIT_BYTES):
    """Move ``path`` to ``path.1`` once it exceeds ``limit``; return True if rotated."""
    try:
        if path.stat().st_size <= limit:
            return False
        os.replace(path, path.with_name(path.name + ".1"))
        return True
    except OSError:
        return False


def rotate_stderr(path, limit=LOG_LIMIT_BYTES):
    """Rotate this process's stderr file if it is ``path`` and too large.

    The supervisor opens ``<key>.err`` in append mode as the Gateway's stderr;
    after the rename a fresh file is reopened onto fd 2.
    """
    try:
        held = os.fstat(2)
        current = os.stat(path)
    except OSError:
        return False
    if (held.st_ino, held.st_dev) != (current.st_ino, current.st_dev):
        return False  # stderr is a terminal or the daemon's own log
    if not rotate_if_large(path, limit):
        return False
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        os.dup2(fd, 2)
    finally:
        os.close(fd)
    return True


# ─── Human-readable status ────────────────────────────────────────────────────


def ago(seconds):
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 90:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours}h{minutes:02d}m"
    return f"{hours // 24}d"


def describe(state, pid, now=None):
    """One line: status, PID, uptime, last Gateway event, last delivery, last error."""
    if not pid:
        return "stopped"
    if not state or state.get("pid") != pid:
        return f"starting (PID {pid})"
    now = time.time() if now is None else now
    parts = [f"{state.get('status', '?')} (PID {pid})"]
    if state.get("status") == "ready" and state.get("connected_since"):
        parts.append(f"connected {ago(now - state['connected_since'])}")
    elif state.get("since"):
        parts.append(f"for {ago(now - state['since'])}")
    if state.get("last_event"):
        parts.append(f"last Gateway event {ago(now - state['last_event'])} ago")
    if state.get("last_delivery"):
        parts.append(f"last delivery {ago(now - state['last_delivery'])} ago")
    if state.get("reconnects"):
        parts.append(f"{state['reconnects']} reconnects")
    if state.get("updated") and now - state["updated"] > 3 * STATE_WRITE_SECONDS:
        parts.append(f"STALE: no state update for {ago(now - state['updated'])}")
    if state.get("status") != "ready" and state.get("detail"):
        parts.append(f"last error: {state['detail']}")
    return " · ".join(parts)


def gateway_status(key, alias=None):
    pid = owned_pid(key, alias)
    return describe(read_state(key, alias), pid)
