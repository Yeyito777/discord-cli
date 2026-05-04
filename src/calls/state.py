"""Detached Discord call worker state files."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import time

CALL_STATE_DIR = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state")) / "discord-cli" / "calls"
CALL_LOG_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "discord-cli" / "calls"
CALL_META_ENV = "DISCORD_CALL_META_PATH"
CALL_NOTIFY_TARGETS_ENV = "DISCORD_CALL_NOTIFY_TARGETS"


def pid_alive(pid):
    try:
        pid = int(pid)
        os.kill(pid, 0)
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
            if ") Z" in stat:
                return False
        except Exception:
            pass
        return True
    except (ProcessLookupError, ValueError):
        return False
    except PermissionError:
        return True


def call_paths(channel_id):
    CALL_STATE_DIR.mkdir(parents=True, exist_ok=True)
    CALL_LOG_DIR.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(channel_id))
    return {
        "meta": CALL_STATE_DIR / f"{safe}.json",
        "log": CALL_LOG_DIR / f"{safe}.log",
        "segments": CALL_LOG_DIR / f"{safe}.segments",
    }


def read_call_meta(path):
    try:
        meta = json.loads(Path(path).read_text())
    except Exception:
        return None
    pid = meta.get("pid")
    if not pid or not pid_alive(pid):
        try:
            Path(path).unlink(missing_ok=True)
        except Exception:
            pass
        return None
    return meta


def running_call_metas():
    CALL_STATE_DIR.mkdir(parents=True, exist_ok=True)
    metas = []
    for path in sorted(CALL_STATE_DIR.glob("*.json")):
        meta = read_call_meta(path)
        if meta:
            metas.append(meta)
    return metas


def write_call_meta(path, meta):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def update_call_meta_env(**updates):
    meta_path = os.environ.get(CALL_META_ENV)
    if not meta_path:
        return
    path = Path(meta_path)
    try:
        meta = json.loads(path.read_text()) if path.exists() else {}
        meta.update(updates)
        write_call_meta(path, meta)
    except Exception:
        pass


def remove_call_meta_env():
    meta_path = os.environ.get(CALL_META_ENV)
    if not meta_path:
        return
    try:
        Path(meta_path).unlink(missing_ok=True)
    except Exception:
        pass


def bump_control_seq(current):
    try:
        current["control_seq"] = int(current.get("control_seq") or 0) + 1
    except (TypeError, ValueError):
        current["control_seq"] = 1
    current["updated_at"] = time.time()
