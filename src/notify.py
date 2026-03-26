"""Notify subcommands — manage Discord notification relay.

Configuration is stored in PROJECT_ROOT/config/notify.json:
{
    "relay_targets": ["conv_id_1", "conv_id_2"],
    "labels": {
        "username": "label",
        ...
    }
}

The notify listener sends DM and @mention notifications to all
configured relay targets via `exo send`.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
CONFIG_FILE = CONFIG_DIR / "notify.json"
LISTENER_DIR = Path("/tmp/discord-listeners")
PROJECT_DIR = Path(__file__).resolve().parent.parent


# ─── Config management ───────────────────────────────────────────────────────

def _load_config():
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {"relay_targets": [], "labels": {}}


def _save_config(cfg):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2) + "\n")


def get_relay_targets():
    """Return list of exo conversation IDs to relay notifications to."""
    return _load_config().get("relay_targets", [])


def _find_notify_gateway_pid():
    """Return PID of any running __notify__ gateway process, or None.

    Matches both daemon-managed processes (started by exocortexd with relative
    paths) and manually started ones (started via 'discord notify start' with
    absolute paths).  We key on 'gateway.py __notify__' which appears in both.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-f", r"gateway\.py\s+__notify__"],
            capture_output=True, text=True,
        )
        pids = [int(p) for p in result.stdout.strip().split() if p.strip()]
        return pids[0] if pids else None
    except Exception:
        return None


def get_labels():
    """Return dict of user_id → {label, username, display_name}."""
    return _load_config().get("labels", {})


# ─── Commands ─────────────────────────────────────────────────────────────────

def add(argv):
    p = argparse.ArgumentParser(prog="discord notify add",
        description="Add an exo conversation as a relay target.")
    p.add_argument("conv_id", help="Exo conversation ID")
    args = p.parse_args(argv)

    cfg = _load_config()
    targets = cfg.setdefault("relay_targets", [])
    if args.conv_id in targets:
        print(f"  Already added: {args.conv_id}")
        return
    targets.append(args.conv_id)
    _save_config(cfg)
    print(f"  Added relay target: {args.conv_id}")
    print(f"  Restart notify listener for changes to take effect.")


def remove(argv):
    p = argparse.ArgumentParser(prog="discord notify remove",
        description="Remove a relay target.")
    p.add_argument("conv_id", help="Exo conversation ID")
    args = p.parse_args(argv)

    cfg = _load_config()
    targets = cfg.get("relay_targets", [])
    if args.conv_id not in targets:
        print(f"  Not found: {args.conv_id}")
        return
    targets.remove(args.conv_id)
    _save_config(cfg)
    print(f"  Removed relay target: {args.conv_id}")
    print(f"  Restart notify listener for changes to take effect.")


def label(argv):
    p = argparse.ArgumentParser(prog="discord notify label",
        description="Set a label for a Discord user (keyed by user ID for security).")
    p.add_argument("user_id", help="Discord user ID (snowflake)")
    p.add_argument("label_value", nargs="?", metavar="label",
                   help="Label (e.g. owner, friend). Omit to remove.")
    p.add_argument("--username", "-u", help="Username (for display, not matching)")
    p.add_argument("--name", "-n", help="Display name / nickname (for display)")
    args = p.parse_args(argv)

    cfg = _load_config()
    labels = cfg.setdefault("labels", {})

    if args.label_value:
        entry = {"label": args.label_value}
        if args.username:
            entry["username"] = args.username
        if args.name:
            entry["name"] = args.name
        labels[args.user_id] = entry
        _save_config(cfg)
        display = f"@{args.username}" if args.username else args.user_id
        print(f"  {display} → {args.label_value}")
    else:
        if args.user_id in labels:
            del labels[args.user_id]
            _save_config(cfg)
            print(f"  Removed label for {args.user_id}")
        else:
            print(f"  No label set for {args.user_id}")


def list_config(argv):
    p = argparse.ArgumentParser(prog="discord notify list",
        description="Show notification relay configuration.")
    p.parse_args(argv)

    cfg = _load_config()
    targets = cfg.get("relay_targets", [])
    labels = cfg.get("labels", {})

    print("  Relay targets:")
    if targets:
        for t in targets:
            print(f"    • {t}")
    else:
        print("    (none)")

    print("  Labels:")
    if labels:
        for user_id, entry in sorted(labels.items(), key=lambda x: x[1].get("label", "")):
            if isinstance(entry, dict):
                lbl = entry.get("label", "?")
                uname = entry.get("username", "")
                name = entry.get("name", "")
                display = f"@{uname}" if uname else user_id
                extra = f" ({name})" if name and name != uname else ""
                print(f"    {display}{extra} [{user_id}] → {lbl}")
            else:
                # Legacy format (plain string)
                print(f"    {user_id} → {entry}")
    else:
        print("    (none)")


def start(argv):
    p = argparse.ArgumentParser(prog="discord notify start",
        description="Start the notification listener.")
    p.parse_args(argv)

    targets = get_relay_targets()
    if not targets:
        print("  No relay targets configured. Run: discord notify add <conv_id>")
        return

    pid_file = LISTENER_DIR / "__notify__.pid"
    log_file = LISTENER_DIR / "__notify__.log"
    LISTENER_DIR.mkdir(parents=True, exist_ok=True)

    # Check if ANY __notify__ gateway is already running — this catches both
    # daemon-managed processes (started by exocortexd, no PID file written) and
    # manually started ones.  Without this check, calling 'notify start' while
    # the daemon is already running the gateway causes a duplicate connection.
    existing_pid = _find_notify_gateway_pid()
    if existing_pid:
        # Keep PID file in sync so 'notify stop' can find it
        pid_file.write_text(str(existing_pid))
        print(f"  Notify gateway already running (PID {existing_pid})")
        print(f"  Relay targets read from config/notify.json at startup.")
        print(f"  (Managed by exocortexd daemon — lifecycle is automatic)")
        return

    # Stale PID file cleanup
    if pid_file.exists():
        pid_file.unlink(missing_ok=True)

    gateway_script = PROJECT_DIR / "src" / "gateway.py"
    err_file = LISTENER_DIR / "__notify__.err"

    # Pass relay targets as additional args after channel_id and output_file
    cmd = [sys.executable, str(gateway_script), "__notify__", str(log_file)] + targets

    proc = subprocess.Popen(
        cmd,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=open(err_file, "a"),
    )

    pid_file.write_text(str(proc.pid))
    meta = {
        "channel_id": "__notify__",
        "channel_name": "Notifications",
        "type": "notify",
        "relay_targets": targets,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    (LISTENER_DIR / "__notify__.meta").write_text(json.dumps(meta))

    print(f"  Notify listener started (PID {proc.pid})")
    print(f"  Relaying to: {', '.join(targets)}")
    print(f"  Output: {log_file}")


def stop(argv):
    p = argparse.ArgumentParser(prog="discord notify stop",
        description="Stop the notification listener.")
    p.parse_args(argv)

    pid_file = LISTENER_DIR / "__notify__.pid"
    meta_file = LISTENER_DIR / "__notify__.meta"

    # Resolve actual PID: try PID file first, then fall back to pgrep.
    # The daemon-managed process won't have an up-to-date PID file after a
    # restart, so pgrep is the reliable fallback.
    pid = None
    if pid_file.exists():
        try:
            candidate = int(pid_file.read_text().strip())
            os.kill(candidate, 0)   # check still alive
            pid = candidate
        except (ProcessLookupError, ValueError):
            pid_file.unlink(missing_ok=True)

    if pid is None:
        pid = _find_notify_gateway_pid()

    if pid is None:
        print("  Notify listener not running")
        return

    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(10):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            os.kill(pid, signal.SIGKILL)
        print(f"  Stopped notify listener (PID {pid})")
        print(f"  Note: if managed by exocortexd daemon it will restart automatically.")
    except ProcessLookupError:
        print(f"  Notify listener already stopped")

    pid_file.unlink(missing_ok=True)
    meta_file.unlink(missing_ok=True)


# ─── Dispatch ─────────────────────────────────────────────────────────────────

_COMMANDS = {
    "add": add,
    "remove": remove,
    "label": label,
    "list": list_config,
    "start": start,
    "stop": stop,
}


def dispatch(cmd, argv):
    # cmd is "notify", argv is ["add", "conv_id", ...] or ["start", ...]
    if not argv:
        list_config([])
        return

    subcmd = argv[0]
    fn = _COMMANDS.get(subcmd)
    if fn is None:
        print(f"  Unknown notify subcommand: {subcmd}")
        print(f"  Available: {', '.join(_COMMANDS.keys())}")
        sys.exit(1)
    fn(argv[1:])
