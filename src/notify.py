"""Notify subcommands — manage Discord notification relay.

Configuration is stored in PROJECT_ROOT/config/notify.json:
{
    "relay_targets": ["conv_id_1", "conv_id_2"],
    "labels": {
        "username": "label",
        ...
    }
}

The notify listener relays DM and @mention notifications to configured targets
via `exo send`. If the target conversation is currently busy, exo's send path
falls back to next-turn queuing; otherwise delivery is immediate.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from src.exocortex import manage_external_tool_daemon

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


def _find_notify_gateway_pids():
    """Return PIDs of any running __notify__ gateway processes.

    Matches both daemon-managed processes (started by exocortexd with relative
    paths) and manually started ones (started via 'discord notify start' with
    absolute paths). We key on 'gateway.py __notify__' which appears in both.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-f", r"gateway\.py\s+__notify__"],
            capture_output=True, text=True,
        )
        pids = []
        for raw in result.stdout.strip().split():
            if not raw.strip():
                continue
            try:
                pid = int(raw)
            except ValueError:
                continue
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                continue
            pids.append(pid)
        return pids
    except Exception:
        return []


def _find_notify_gateway_pid():
    """Return PID of any running __notify__ gateway process, or None."""
    pids = _find_notify_gateway_pids()
    return pids[0] if pids else None


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _listener_paths():
    return {
        "pid": LISTENER_DIR / "__notify__.pid",
        "log": LISTENER_DIR / "__notify__.log",
        "err": LISTENER_DIR / "__notify__.err",
        "meta": LISTENER_DIR / "__notify__.meta",
    }


def _write_pid_hint(pid):
    LISTENER_DIR.mkdir(parents=True, exist_ok=True)
    _listener_paths()["pid"].write_text(f"{pid}\n")


def _collect_notify_pids():
    """Return all live notify gateway PIDs, preferring the PID file first."""
    paths = _listener_paths()
    pids = []
    seen = set()

    if paths["pid"].exists():
        try:
            candidate = int(paths["pid"].read_text().strip())
            os.kill(candidate, 0)
            pids.append(candidate)
            seen.add(candidate)
        except (ProcessLookupError, ValueError):
            paths["pid"].unlink(missing_ok=True)

    for pid in _find_notify_gateway_pids():
        if pid in seen:
            continue
        pids.append(pid)
        seen.add(pid)

    return pids


def _stop_notify_pids(pids):
    """Terminate notify gateway PIDs, returning (stopped, still_alive)."""
    pids = list(dict.fromkeys(pids))
    if not pids:
        return [], []

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    alive = list(pids)
    for _ in range(10):
        if not alive:
            break
        time.sleep(0.5)
        alive = [pid for pid in alive if _pid_alive(pid)]

    for pid in alive:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    if alive:
        time.sleep(0.2)
        alive = [pid for pid in alive if _pid_alive(pid)]

    stopped = [pid for pid in pids if pid not in set(alive)]
    return stopped, alive


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

    status = manage_external_tool_daemon("discord", "start")
    print(f"  {status.get('message', 'Requested start for supervised Discord daemon')}")


def stop(argv):
    p = argparse.ArgumentParser(prog="discord notify stop",
        description="Stop the notification listener.")
    p.parse_args(argv)

    status = manage_external_tool_daemon("discord", "stop")
    print(f"  {status.get('message', 'Requested stop for supervised Discord daemon')}")


def _run_daemon_mode(argv):
    if not argv:
        raise SystemExit("usage: python -m src.notify __daemon__ <log_file> [conv_id ...]")

    log_file = argv[0]
    explicit_targets = argv[1:]
    gateway_script = PROJECT_DIR / "src" / "gateway.py"

    while True:
        existing = [pid for pid in _find_notify_gateway_pids() if pid != os.getpid()]
        if existing:
            _stopped, alive = _stop_notify_pids(existing)
            if alive:
                _write_pid_hint(alive[0])
                time.sleep(5)
                continue

        relay_targets = explicit_targets or get_relay_targets()
        os.execv(
            sys.executable,
            [sys.executable, str(gateway_script), "__notify__", str(log_file)] + relay_targets,
        )


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


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "__daemon__":
        _run_daemon_mode(argv[1:])
    else:
        dispatch("notify", argv)


if __name__ == "__main__":
    main()
