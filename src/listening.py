"""Listening subcommands — listen, unlisten, listeners.

Manages background gateway processes that stream real-time events
from Discord channels to files in /tmp/discord-listeners/.
"""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

LISTENER_DIR = Path("/tmp/discord-listeners")
PROJECT_DIR = Path(__file__).resolve().parent.parent


def listen(argv):
    p = argparse.ArgumentParser(
        prog="discord listen",
        description="Start listening to a channel or DM in real-time.",
    )
    p.add_argument("target", nargs="?", help="Channel name/ID, or username for DMs")
    p.add_argument("-g", "--guild", "--server", dest="guild",
                   help="Server name or ID (required for channel names)")
    p.add_argument("--dm", action="store_true",
                   help="Listen to a DM conversation")
    p.add_argument("--notify", action="store_true",
                   help="Listen for all DMs and @mentions (legacy alias; prefer 'discord notify start')")
    p.add_argument("--relay-conv", dest="relay_conv", metavar="ID",
                   help="Exo conversation ID for instant notification relay")
    args = p.parse_args(argv)

    if args.notify:
        _start_notify_listener(relay_conv=args.relay_conv)
        return

    if not args.target:
        p.error("target is required (or use --notify)")

    from src.resolve import resolve_channel, resolve_dm, resolve_guild

    # Resolve target to a channel ID
    channel_name = args.target
    guild_name = ""

    if args.dm:
        ch = resolve_dm(args.target)
    elif args.guild:
        g = resolve_guild(args.guild)
        guild_name = g.get("name", args.guild)
        ch = resolve_channel(args.target, g["id"])
    elif re.match(r"^\d{17,20}$", args.target):
        from src import api
        ch = api.get_channel(args.target)
    else:
        # No guild given and not a snowflake — try DM resolution
        try:
            ch = resolve_dm(args.target)
            args.dm = True
        except RuntimeError:
            p.error(
                "Use -g/--guild for server channels, --dm for DMs, "
                "or pass a channel ID directly."
            )

    channel_id = ch["id"]

    # Friendly name for display
    if args.dm or ch.get("type") in (1, 3):
        for r in ch.get("recipients", []):
            channel_name = r.get("global_name") or r.get("username", "DM")
            break
        label = f"DM: {channel_name}"
        ch_type = "dm"
    else:
        channel_name = ch.get("name", args.target)
        label = f"#{channel_name}" + (f" ({guild_name})" if guild_name else "")
        ch_type = "server"

    # Check for existing listener
    LISTENER_DIR.mkdir(parents=True, exist_ok=True)
    pid_file = LISTENER_DIR / f"{channel_id}.pid"
    log_file = LISTENER_DIR / f"{channel_id}.log"

    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)
            print(f"  Already listening to {label}")
            print(f"  Output: {log_file}")
            return
        except (ProcessLookupError, ValueError):
            pid_file.unlink(missing_ok=True)

    # Spawn background gateway process
    gateway_script = PROJECT_DIR / "src" / "gateway.py"
    err_file = LISTENER_DIR / f"{channel_id}.err"

    proc = subprocess.Popen(
        [sys.executable, str(gateway_script), channel_id, str(log_file)],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=open(err_file, "a"),
    )

    pid_file.write_text(str(proc.pid))

    # Save metadata
    meta = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "guild_name": guild_name,
        "type": ch_type,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    (LISTENER_DIR / f"{channel_id}.meta").write_text(json.dumps(meta))

    print(f"  Listening to {label}")
    print(f"  Output: {log_file}")
    print(f"  PID: {proc.pid}")


def _start_notify_listener(relay_conv=None):
    """Start the notification listener (DMs + @mentions)."""
    from src.notify import _find_notify_gateway_pids

    LISTENER_DIR.mkdir(parents=True, exist_ok=True)
    pid_file = LISTENER_DIR / "__notify__.pid"
    log_file = LISTENER_DIR / "__notify__.log"

    existing_pids = _find_notify_gateway_pids()
    if existing_pids:
        pid = existing_pids[0]
        pid_file.write_text(str(pid))
        print(f"  Notify listener already running (PID {pid})")
        if len(existing_pids) > 1:
            extras = ", ".join(str(existing) for existing in existing_pids[1:])
            print(f"  Warning: found {len(existing_pids)} notify gateways already running (extra PIDs: {extras})")
            print(f"  Run 'discord notify stop' to clean up duplicates.")
        print(f"  Output: {log_file}")
        return

    if pid_file.exists():
        pid_file.unlink(missing_ok=True)

    gateway_script = PROJECT_DIR / "src" / "gateway.py"
    err_file = LISTENER_DIR / "__notify__.err"

    cmd = [sys.executable, str(gateway_script), "__notify__", str(log_file)]
    if relay_conv:
        cmd.append(relay_conv)

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
        "relay_conv": relay_conv,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    (LISTENER_DIR / "__notify__.meta").write_text(json.dumps(meta))

    relay_str = f" → relaying to exo conv {relay_conv}" if relay_conv else ""
    print(f"  Notify listener started{relay_str}")
    print(f"  Output: {log_file}")
    print(f"  PID: {proc.pid}")


def unlisten(argv):
    p = argparse.ArgumentParser(
        prog="discord unlisten",
        description="Stop listening to a channel or DM.",
    )
    p.add_argument("target", nargs="?", help="Channel name/ID, username, or PID")
    p.add_argument("-g", "--guild", "--server", dest="guild",
                   help="Server name or ID")
    p.add_argument("--dm", action="store_true", help="DM conversation")
    p.add_argument("--notify", action="store_true", help="Stop notify listener (legacy alias; prefer 'discord notify stop')")
    p.add_argument("--all", action="store_true", dest="stop_all",
                   help="Stop all listeners")
    args = p.parse_args(argv)

    if args.notify:
        from src.notify import stop as stop_notify
        stop_notify([])
        return

    if args.stop_all:
        _stop_all()
        return

    if not args.target:
        p.error("Specify a target or use --all")

    channel_id = _resolve_target(args.target, args.guild, args.dm)
    _stop_one(channel_id)


def _stop_one(channel_id):
    """Stop a single listener by channel ID."""
    pid_file = LISTENER_DIR / f"{channel_id}.pid"
    meta_file = LISTENER_DIR / f"{channel_id}.meta"

    if not pid_file.exists():
        print(f"  Not listening to {channel_id}")
        return

    # Load name for display
    label = channel_id
    if meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text())
            name = meta.get("channel_name", "")
            if meta.get("type") == "dm":
                label = f"DM: {name}"
            else:
                guild = meta.get("guild_name", "")
                label = f"#{name}" + (f" ({guild})" if guild else "")
        except Exception:
            pass

    pid = int(pid_file.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
        # Wait briefly for graceful shutdown, then force-kill
        for _ in range(10):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            os.kill(pid, signal.SIGKILL)
        print(f"  Stopped listening to {label} (PID {pid})")
    except ProcessLookupError:
        print(f"  Listener for {label} already stopped")

    pid_file.unlink(missing_ok=True)
    meta_file.unlink(missing_ok=True)


def _stop_all():
    """Stop all active listeners."""
    if not LISTENER_DIR.exists():
        print("  No active listeners")
        return

    count = 0
    for pid_file in LISTENER_DIR.glob("*.pid"):
        pid = int(pid_file.read_text().strip())
        try:
            os.kill(pid, signal.SIGTERM)
            count += 1
        except (ProcessLookupError, ValueError):
            pass
        pid_file.unlink(missing_ok=True)
        pid_file.with_suffix(".meta").unlink(missing_ok=True)

    print(f"  Stopped {count} listener(s)")


def _resolve_target(target, guild, dm):
    """Resolve a target argument to a channel ID."""
    # Direct channel ID
    if re.match(r"^\d{17,20}$", target):
        return target

    # Might be a PID
    if re.match(r"^\d{1,7}$", target) and LISTENER_DIR.exists():
        for pf in LISTENER_DIR.glob("*.pid"):
            if pf.read_text().strip() == target:
                return pf.stem

    # Resolve via Discord
    from src.resolve import resolve_channel, resolve_dm, resolve_guild

    if dm:
        return resolve_dm(target)["id"]
    elif guild:
        g = resolve_guild(guild)
        return resolve_channel(target, g["id"])["id"]
    else:
        try:
            return resolve_dm(target)["id"]
        except RuntimeError:
            raise RuntimeError(
                f"Can't resolve '{target}'. Use --guild or --dm to clarify."
            )


def listeners(argv):
    p = argparse.ArgumentParser(
        prog="discord listeners",
        description="List active listeners.",
    )
    p.parse_args(argv)

    if not LISTENER_DIR.exists():
        print("  No active listeners")
        return

    active = []
    for pid_file in sorted(LISTENER_DIR.glob("*.pid")):
        channel_id = pid_file.stem
        try:
            pid = int(pid_file.read_text().strip())
        except (ValueError, OSError):
            pid_file.unlink(missing_ok=True)
            continue

        # Check if alive
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            pid_file.unlink(missing_ok=True)
            pid_file.with_suffix(".meta").unlink(missing_ok=True)
            continue

        meta = {}
        meta_file = pid_file.with_suffix(".meta")
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text())
            except Exception:
                pass

        log_file = pid_file.with_suffix(".log")
        log_size = log_file.stat().st_size if log_file.exists() else 0

        active.append((pid, channel_id, meta, log_file, log_size))

    if not active:
        print("  No active listeners")
        return

    for pid, channel_id, meta, log_file, log_size in active:
        name = meta.get("channel_name", "?")
        guild = meta.get("guild_name", "")
        ch_type = meta.get("type", "?")

        if ch_type == "notify":
            label = "🔔 Notifications (DMs + @mentions)"
        elif ch_type == "dm":
            label = f"DM: {name}"
        else:
            label = f"#{name}" + (f" ({guild})" if guild else "")

        size_str = _fmt_size(log_size)
        print(f"  {pid:>7}  {label:30}  {size_str:>8}  {log_file}")


def _fmt_size(n):
    if n < 1024:
        return f"{n}B"
    elif n < 1024 * 1024:
        return f"{n / 1024:.1f}KB"
    return f"{n / (1024 * 1024):.1f}MB"


# ─── Dispatch ────────────────────────────────────────────────────────────────

_COMMANDS = {
    "listen": listen,
    "unlisten": unlisten,
    "listeners": listeners,
}


def dispatch(cmd, argv):
    fn = _COMMANDS.get(cmd)
    if fn is None:
        raise RuntimeError(f"Unknown listening command: {cmd}")
    fn(argv)
