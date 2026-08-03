"""CLI entrypoints for Discord call commands."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

from src import api
from src.calls.state import (
    CALL_META_ENV,
    bump_control_seq as _bump_control_seq,
    call_paths as _call_paths,
    pid_alive as _pid_alive,
    read_call_meta as _read_call_meta,
    remove_call_meta_env as _remove_call_meta_env,
    running_call_metas as _running_call_metas,
    update_call_meta_env as _update_call_meta_env,
    write_call_meta as _write_call_meta,
)
from src.calls.worker import DiscordCallWorker
from src.private_channels import private_channel_label_for_type, private_channel_name, private_channel_type

PROJECT_DIR = Path(__file__).resolve().parents[2]


def _resolve_call_target(args):
    ch = _resolve_call_channel(args)
    ch_type = ch.get("type")
    if ch_type in (1, 3):
        return ch["id"], None, private_channel_label_for_type(private_channel_type(ch), private_channel_name(ch))
    guild_id = ch.get("guild_id")
    name = ch.get("name", getattr(args, "target", ch.get("id", "")))
    return ch["id"], guild_id, f"#{name}" if guild_id else name


def _resolve_call_channel(args):
    from src.resolve import resolve_channel, resolve_dm, resolve_guild

    if args.dm:
        return resolve_dm(args.target)

    guild_arg = getattr(args, "guild", None)
    if guild_arg:
        guild = resolve_guild(guild_arg)
        return resolve_channel(args.target, guild["id"])

    if re.match(r"^\d{17,20}$", args.target):
        return api.get_channel(args.target)

    # Most call testing is done in DMs, so try those first when no guild is given.
    try:
        return resolve_dm(args.target)
    except RuntimeError:
        raise SystemExit("Use --dm for DMs, --guild/-g for server voice channels, or pass a channel ID.")


def _recipient_ids_for_private_call(channel):
    ch_type = channel.get("type")
    if ch_type not in (1, 3):
        raise SystemExit("Ringing is only supported for DMs and group DMs.")
    return [str(r.get("id")) for r in channel.get("recipients") or [] if isinstance(r, dict) and r.get("id")]



def _join_foreground_channel(channel_id, guild_id, label, *, self_mute=False, self_deaf=False, ring_recipient_ids=None, exocortex_conversation=None, exocortex_socket=None, call_voice=None):
    joiner = DiscordCallWorker(
        channel_id,
        guild_id=guild_id,
        label=label,
        self_mute=self_mute,
        self_deaf=self_deaf,
        ring_recipient_ids=ring_recipient_ids,
        exocortex_conversation=exocortex_conversation,
        exocortex_socket=exocortex_socket,
        call_voice=call_voice,
    )
    try:
        _update_call_meta_env(status="joining", updated_at=time.time())
        joiner.run()
    finally:
        _remove_call_meta_env()


def _join_child(argv):
    p = argparse.ArgumentParser(prog="python -m src.calls.cli __join_foreground")
    p.add_argument("channel_id")
    p.add_argument("guild_id")
    p.add_argument("label")
    p.add_argument("--muted", action="store_true")
    p.add_argument("--deafened", action="store_true")
    p.add_argument("--ring", action="append", default=[], metavar="USER_ID")
    p.add_argument("--exo-conversation")
    p.add_argument("--exo-socket")
    p.add_argument("--call-voice")
    args = p.parse_args(argv)
    return _join_foreground_channel(
        args.channel_id,
        args.guild_id or None,
        args.label,
        self_mute=args.muted,
        self_deaf=args.deafened,
        ring_recipient_ids=args.ring,
        exocortex_conversation=args.exo_conversation,
        exocortex_socket=args.exo_socket,
        call_voice=args.call_voice,
    )


def _invoking_conversation(explicit=None):
    return str(explicit or os.environ.get("EXOCORTEX_PARENT_CONV_ID") or "").strip() or None


def _current_exocortex_socket(explicit=None):
    return str(explicit or os.environ.get("EXOCORTEX_SOCKET") or "").strip() or None


def _spawn_detached_call(channel_id, guild_id, label, *, self_mute=False, self_deaf=False, ring_recipient_ids=None, exocortex_conversation=None, exocortex_socket=None, call_voice=None):
    paths = _call_paths(channel_id)
    existing = _read_call_meta(paths["meta"])
    if existing:
        print(f"Already joining {existing.get('label') or label} detached (pid {existing.get('pid')}).")
        print(f"Log: {existing.get('log')}")
        return

    log_file = paths["log"]
    log = open(log_file, "a", buffering=1)
    ring_recipient_ids = [str(user_id) for user_id in (ring_recipient_ids or []) if user_id]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env[CALL_META_ENV] = str(paths["meta"])

    cmd = [
        sys.executable,
        "-m", "src.calls.cli",
        "__join_foreground",
        str(channel_id),
        str(guild_id or ""),
        str(label),
    ]
    if self_mute:
        cmd.append("--muted")
    if self_deaf:
        cmd.append("--deafened")
    if exocortex_conversation:
        cmd.extend(["--exo-conversation", str(exocortex_conversation)])
    if exocortex_socket:
        cmd.extend(["--exo-socket", str(exocortex_socket)])
    if call_voice:
        cmd.extend(["--call-voice", str(call_voice)])
    for user_id in ring_recipient_ids:
        cmd.extend(["--ring", user_id])

    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_DIR),
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env=env,
    )
    log.close()

    meta = {
        "pid": proc.pid,
        "channel_id": str(channel_id),
        "guild_id": str(guild_id) if guild_id else None,
        "label": label,
        "status": "starting",
        "self_mute": self_mute,
        "self_deaf": self_deaf,
        "exocortex_conversation": str(exocortex_conversation) if exocortex_conversation else None,
        "control_seq": 0,
        "started_at": time.time(),
        "updated_at": time.time(),
        "log": str(log_file),
        "ring_recipient_ids": ring_recipient_ids,
    }
    _write_call_meta(paths["meta"], meta)

    time.sleep(0.35)
    if proc.poll() is not None:
        _read_call_meta(paths["meta"])
        print(f"Failed to start detached call join for {label} (exit {proc.returncode}).")
        print(f"Log: {log_file}")
        try:
            tail = log_file.read_text(errors="replace").splitlines()[-8:]
            if tail:
                print("Last log lines:")
                for line in tail:
                    print(f"  {line}")
        except Exception:
            pass
        raise SystemExit(proc.returncode or 1)

    print(f"Started detached Discord call join for {label} (pid {proc.pid}).")
    print(f"Log: {log_file}")
    print("Use `discord call leave` to leave, or `discord call join --foreground ...` to run in the foreground.")

def _add_call_adapter_options(parser):
    parser.add_argument("--muted", action="store_true", help="Join with Discord self-mute enabled")
    parser.add_argument("--deafened", action="store_true", help="Join with Discord self-deaf enabled")
    parser.add_argument("--foreground", action="store_true", help="Run in the foreground until Ctrl+C instead of detaching")
    parser.add_argument("--detach", "--background", action="store_true", help="Detach and return immediately (default)")
    parser.add_argument(
        "--conversation", "--exo-conversation",
        dest="exocortex_conversation",
        metavar="CONV_ID",
        help="Owning Exocortex conversation; defaults to the invoking conversation",
    )
    parser.add_argument("--exo-socket", metavar="PATH", help=argparse.SUPPRESS)
    parser.add_argument("--voice", dest="call_voice", help="OpenAI realtime voice")


def join(argv):
    p = argparse.ArgumentParser(
        prog="discord call join",
        description="Join a Discord call as an Exocortex media adapter. Detaches by default; use --foreground to block.",
    )
    p.add_argument("target", help="DM/group name, channel ID, or voice channel name with --guild")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name/ID for a voice channel")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    _add_call_adapter_options(p)
    args = p.parse_args(argv)

    channel_id, guild_id, label = _resolve_call_target(args)
    conversation = _invoking_conversation(args.exocortex_conversation)
    socket_path = _current_exocortex_socket(args.exo_socket)
    options = dict(
        self_mute=args.muted,
        self_deaf=args.deafened,
        exocortex_conversation=conversation,
        exocortex_socket=socket_path,
        call_voice=args.call_voice,
    )
    if args.foreground:
        return _join_foreground_channel(channel_id, guild_id, label, **options)
    return _spawn_detached_call(channel_id, guild_id, label, **options)


def start(argv):
    p = argparse.ArgumentParser(
        prog="discord call start",
        description="Start a Discord DM/group call as an Exocortex media adapter. Detaches by default; use --foreground to block.",
    )
    p.add_argument("target", help="DM/group name or channel ID")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    _add_call_adapter_options(p)
    args = p.parse_args(argv)

    channel = _resolve_call_channel(args)
    recipient_ids = _recipient_ids_for_private_call(channel)
    if not recipient_ids:
        raise SystemExit("No recipients to ring for this DM/group DM.")
    channel_id = channel["id"]
    label = private_channel_label_for_type(private_channel_type(channel), private_channel_name(channel))
    conversation = _invoking_conversation(args.exocortex_conversation)
    socket_path = _current_exocortex_socket(args.exo_socket)
    options = dict(
        self_mute=args.muted,
        self_deaf=args.deafened,
        ring_recipient_ids=recipient_ids,
        exocortex_conversation=conversation,
        exocortex_socket=socket_path,
        call_voice=args.call_voice,
    )
    if args.foreground:
        return _join_foreground_channel(channel_id, None, label, **options)
    return _spawn_detached_call(channel_id, None, label, **options)

def list_calls(argv):
    p = argparse.ArgumentParser(prog="discord call list", description="List detached Discord call sessions.")
    p.parse_args(argv)
    metas = _running_call_metas()
    if not metas:
        print("No detached Discord call sessions.")
        return
    for meta in metas:
        status = meta.get("status") or "running"
        mute = "muted" if meta.get("self_mute", True) else "unmuted"
        deaf = "deafened" if meta.get("self_deaf", True) else "undeafened"
        print(f"{meta.get('channel_id')}  pid {meta.get('pid')}  {status}  {mute}/{deaf}  {meta.get('label')}")
        print(f"  log: {meta.get('log')}")
        if meta.get("exocortex_conversation"):
            print(f"  exocortex: {meta.get('exocortex_conversation')}  call {meta.get('exocortex_call_id') or 'starting'}")


_STATE_WORDS = {
    "on": True,
    "true": True,
    "yes": True,
    "1": True,
    "off": False,
    "false": False,
    "no": False,
    "0": False,
    "toggle": None,
}


def _parse_call_voice_state_args(prog, argv, *, default_value=None):
    p = argparse.ArgumentParser(prog=prog)
    p.add_argument("args", nargs="*", help="optional target and on/off/toggle state")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name/ID for resolving a voice channel target")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    p.add_argument("--all", action="store_true", help="Apply to all detached calls")
    parsed = p.parse_args(argv)

    target = None
    value = default_value
    for token in parsed.args:
        lower = token.lower()
        if lower in _STATE_WORDS and value == default_value:
            value = _STATE_WORDS[lower]
        elif target is None:
            target = token
        elif lower in _STATE_WORDS:
            value = _STATE_WORDS[lower]
        else:
            p.error(f"unexpected argument: {token}")
    parsed.target = target
    parsed.value = value
    return parsed


def _target_call_metas(args):
    metas = _running_call_metas()
    if args.all or not args.target:
        return metas
    target = str(args.target)
    direct = [m for m in metas if str(m.get("channel_id")) == target or str(m.get("label") or "") == target]
    if direct:
        return direct
    channel_id, _guild_id, _label = _resolve_call_target(args)
    return [m for m in metas if str(m.get("channel_id")) == str(channel_id)]


def _control_call_voice_state(argv, *, field, label, default_value=None):
    args = _parse_call_voice_state_args(f"discord call {label}", argv, default_value=default_value)
    targets = _target_call_metas(args)
    if not targets:
        print("No matching detached Discord call sessions.")
        return

    for meta in targets:
        channel_id = meta.get("channel_id")
        paths = _call_paths(channel_id)
        current = _read_call_meta(paths["meta"]) or meta
        old_value = bool(current.get(field, True))
        next_value = (not old_value) if args.value is None else bool(args.value)
        current[field] = next_value
        _bump_control_seq(current)
        _write_call_meta(paths["meta"], current)
        mute = "muted" if current.get("self_mute", True) else "unmuted"
        deaf = "deafened" if current.get("self_deaf", True) else "undeafened"
        print(f"Set {current.get('label') or channel_id} to {mute}/{deaf} (pid {current.get('pid')}).")


def _terminate_call_meta(meta, *, timeout=5):
    pid = int(meta["pid"])
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    time.sleep(0.2)
    return not _pid_alive(pid)


def leave(argv):
    p = argparse.ArgumentParser(prog="discord call leave", description="Leave detached Discord call sessions.")
    p.add_argument("target", nargs="?", help="channel ID / DM / voice channel to leave; omit with --all to leave every detached call")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name/ID for resolving a voice channel target")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    p.add_argument("--all", action="store_true", help="Leave all detached calls")
    args = p.parse_args(argv)

    metas = _running_call_metas()
    if args.all or not args.target:
        targets = metas
    else:
        channel_id, _guild_id, _label = _resolve_call_target(args)
        targets = [m for m in metas if str(m.get("channel_id")) == str(channel_id)]

    if not targets:
        print("No matching detached Discord call sessions.")
        return

    for meta in targets:
        ok = _terminate_call_meta(meta)
        paths = _call_paths(meta.get("channel_id"))
        paths["meta"].unlink(missing_ok=True)
        if ok:
            print(f"Left {meta.get('label') or meta.get('channel_id')} (pid {meta.get('pid')}).")
        else:
            print(f"Failed to stop {meta.get('label') or meta.get('channel_id')} (pid {meta.get('pid')}).")


def dispatch(cmd, argv):
    if cmd in {"call", "voice"}:
        if not argv or argv[0] in {"-h", "--help", "help"}:
            print("usage: discord call <start|join|leave|mute|unmute|deafen|undeafen|list> ...")
            print("  start <dm> [--dm] [--conversation CONV_ID] [--voice VOICE] [--foreground]")
            print("  join <target> [--dm|-g SERVER] [--conversation CONV_ID] [--voice VOICE] [--foreground]")
            print("  mute [target] [on|off|toggle] [--all]")
            print("  unmute [target] [--all]")
            print("  deafen [target] [on|off|toggle] [--all]")
            print("  undeafen [target] [--all]")
            print("  leave [target|--all]")
            print("  list")
            return
        subcmd, rest = argv[0], argv[1:]
        if subcmd == "join":
            return join(rest)
        if subcmd in {"start", "call", "ring"}:
            return start(rest)
        if subcmd in {"leave", "stop", "hangup"}:
            return leave(rest)
        if subcmd in {"mute", "muted"}:
            return _control_call_voice_state(rest, field="self_mute", label="mute")
        if subcmd in {"unmute", "unmuted"}:
            return _control_call_voice_state(rest, field="self_mute", label="unmute", default_value=False)
        if subcmd in {"deafen", "deaf", "deafened"}:
            return _control_call_voice_state(rest, field="self_deaf", label="deafen")
        if subcmd in {"undeafen", "undeaf", "undeafened"}:
            return _control_call_voice_state(rest, field="self_deaf", label="undeafen", default_value=False)
        if subcmd in {"list", "ls", "status"}:
            return list_calls(rest)
        raise SystemExit(f"discord call: unknown subcommand '{subcmd}'")
    if cmd == "join-call":
        return join(argv)
    raise SystemExit(f"discord: unknown call command '{cmd}'")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "__join_foreground":
        return _join_child(argv[1:])
    return dispatch("call", argv)


if __name__ == "__main__":
    main()
