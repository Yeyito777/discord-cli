"""CLI entrypoints for Discord call commands."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import time
import uuid

from src import api
from src.calls.state import (
    CALL_LOG_DIR,
    CALL_META_ENV,
    CALL_NOTIFY_TARGETS_ENV,
    bump_control_seq as _bump_control_seq,
    call_paths as _call_paths,
    pid_alive as _pid_alive,
    read_call_meta as _read_call_meta,
    remove_call_meta_env as _remove_call_meta_env,
    running_call_metas as _running_call_metas,
    update_call_meta_env as _update_call_meta_env,
    write_call_meta as _write_call_meta,
)
from src.calls.receive import diagnose_saved_voice_segment
from src.calls.worker import NoAudioCallJoiner
from src.private_channels import private_channel_label_for_type, private_channel_name, private_channel_type


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



def _join_foreground_channel(channel_id, guild_id, label, *, self_mute=True, self_deaf=False, ring_recipient_ids=None, transcribe=True, save_audio=False, audio_dir=None):
    joiner = NoAudioCallJoiner(
        channel_id,
        guild_id=guild_id,
        label=label,
        self_mute=self_mute,
        self_deaf=self_deaf,
        ring_recipient_ids=ring_recipient_ids,
        transcribe=transcribe,
        save_audio=save_audio,
        audio_dir=audio_dir,
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
    p.add_argument("--unmuted", action="store_true")
    p.add_argument("--deafened", action="store_true")
    p.add_argument("--undeafened", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--ring", action="append", default=[], metavar="USER_ID")
    p.add_argument("--no-transcribe", action="store_true")
    p.add_argument("--save-audio", "--keep-audio", action="store_true")
    p.add_argument("--audio-dir")
    args = p.parse_args(argv)
    return _join_foreground_channel(
        args.channel_id,
        args.guild_id or None,
        args.label,
        self_mute=not args.unmuted,
        self_deaf=bool(args.deafened and not args.undeafened),
        ring_recipient_ids=args.ring,
        transcribe=not args.no_transcribe,
        save_audio=args.save_audio,
        audio_dir=args.audio_dir,
    )


def _configured_notify_targets():
    parent = os.environ.get("EXOCORTEX_PARENT_CONV_ID", "").strip()
    if parent:
        return [parent]
    try:
        from src.notify import get_relay_targets
        return list(get_relay_targets())
    except Exception:
        return []


def _normalize_notify_targets(targets):
    seen = set()
    result = []
    for target in targets or []:
        target = str(target).strip()
        if target and target not in seen:
            result.append(target)
            seen.add(target)
    return result


def _spawn_detached_call(channel_id, guild_id, label, *, self_mute=True, self_deaf=False, notify_targets=None, ring_recipient_ids=None, transcribe=True, save_audio=False):
    paths = _call_paths(channel_id)
    existing = _read_call_meta(paths["meta"])
    if existing:
        print(f"Already joining {existing.get('label') or label} detached (pid {existing.get('pid')}).")
        print(f"Log: {existing.get('log')}")
        return

    log_file = paths["log"]
    log = open(log_file, "a", buffering=1)
    notify_targets = _normalize_notify_targets(notify_targets)
    ring_recipient_ids = [str(user_id) for user_id in (ring_recipient_ids or []) if user_id]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env[CALL_META_ENV] = str(paths["meta"])
    env[CALL_NOTIFY_TARGETS_ENV] = ",".join(notify_targets)
    if save_audio:
        paths["segments"].mkdir(parents=True, exist_ok=True)
        env["DISCORD_CALL_TRANSCRIBE_KEEP_AUDIO"] = "1"
        env["DISCORD_CALL_TRANSCRIBE_AUDIO_DIR"] = str(paths["segments"])

    cmd = [
        sys.executable,
        "-m", "src.calls.cli",
        "__join_foreground",
        str(channel_id),
        str(guild_id or ""),
        str(label),
    ]
    if not self_mute:
        cmd.append("--unmuted")
    if self_deaf:
        cmd.append("--deafened")
    if not transcribe:
        cmd.append("--no-transcribe")
    if save_audio:
        cmd.extend(["--save-audio", "--audio-dir", str(paths["segments"])])
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
        "transcribe": bool(transcribe and not self_deaf),
        "save_audio": bool(save_audio),
        "segments_dir": str(paths["segments"]) if save_audio else None,
        "control_seq": 0,
        "started_at": time.time(),
        "updated_at": time.time(),
        "log": str(log_file),
        "notify_targets": notify_targets,
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


def join(argv):
    p = argparse.ArgumentParser(
        prog="discord call join",
        description="Join a DM/group/server voice call muted and undeafened by default. Detaches by default; use --foreground to block.",
    )
    p.add_argument("target", help="DM/group name, channel ID, or voice channel name with --guild")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name/ID for a voice channel")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    p.add_argument("--unmuted", action="store_true", help="Join with Discord self-mute off (no audio is still sent)")
    p.add_argument("--deafened", action="store_true", help="Join with Discord self-deaf on")
    p.add_argument("--undeafened", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--no-transcribe", action="store_true", help="Join without receiving/transcribing call audio")
    p.add_argument("--save-audio", "--keep-audio", action="store_true", help="Keep per-segment WAV files for transcription diagnostics")
    p.add_argument("--foreground", action="store_true", help="Run in the foreground until Ctrl+C instead of detaching")
    p.add_argument("--detach", "--background", action="store_true", help="Detach and return immediately (default)")
    p.add_argument("--notify-parent", metavar="CONV_ID", action="append", help="Relay call activity to an Exocortex conversation; defaults to EXOCORTEX_PARENT_CONV_ID")
    p.add_argument("--no-notify", action="store_true", help="Disable detached call activity notifications")
    args = p.parse_args(argv)

    channel_id, guild_id, label = _resolve_call_target(args)
    self_mute = not args.unmuted
    self_deaf = bool(args.deafened and not args.undeafened)
    notify_targets = [] if args.no_notify else _normalize_notify_targets(args.notify_parent or _configured_notify_targets())
    transcribe = not args.no_transcribe
    if args.foreground:
        if notify_targets:
            os.environ[CALL_NOTIFY_TARGETS_ENV] = ",".join(notify_targets)
        audio_dir = str(_call_paths(channel_id)["segments"]) if args.save_audio else None
        return _join_foreground_channel(channel_id, guild_id, label, self_mute=self_mute, self_deaf=self_deaf, transcribe=transcribe, save_audio=args.save_audio, audio_dir=audio_dir)
    return _spawn_detached_call(channel_id, guild_id, label, self_mute=self_mute, self_deaf=self_deaf, notify_targets=notify_targets, transcribe=transcribe, save_audio=args.save_audio)


def start(argv):
    p = argparse.ArgumentParser(
        prog="discord call start",
        description="Start/ring a DM or group DM call muted and undeafened by default. Detaches by default; use --foreground to block.",
    )
    p.add_argument("target", help="DM/group name or channel ID")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    p.add_argument("--unmuted", action="store_true", help="Join with Discord self-mute off (no audio is still sent)")
    p.add_argument("--deafened", action="store_true", help="Join with Discord self-deaf on")
    p.add_argument("--undeafened", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--no-transcribe", action="store_true", help="Join without receiving/transcribing call audio")
    p.add_argument("--save-audio", "--keep-audio", action="store_true", help="Keep per-segment WAV files for transcription diagnostics")
    p.add_argument("--foreground", action="store_true", help="Run in the foreground until Ctrl+C instead of detaching")
    p.add_argument("--detach", "--background", action="store_true", help="Detach and return immediately (default)")
    p.add_argument("--notify-parent", metavar="CONV_ID", action="append", help="Relay call activity to an Exocortex conversation; defaults to EXOCORTEX_PARENT_CONV_ID")
    p.add_argument("--no-notify", action="store_true", help="Disable detached call activity notifications")
    args = p.parse_args(argv)

    channel = _resolve_call_channel(args)
    recipient_ids = _recipient_ids_for_private_call(channel)
    if not recipient_ids:
        raise SystemExit("No recipients to ring for this DM/group DM.")
    channel_id = channel["id"]
    guild_id = None
    label = private_channel_label_for_type(private_channel_type(channel), private_channel_name(channel))
    self_mute = not args.unmuted
    self_deaf = bool(args.deafened and not args.undeafened)
    notify_targets = [] if args.no_notify else _normalize_notify_targets(args.notify_parent or _configured_notify_targets())
    transcribe = not args.no_transcribe
    if args.foreground:
        if notify_targets:
            os.environ[CALL_NOTIFY_TARGETS_ENV] = ",".join(notify_targets)
        audio_dir = str(_call_paths(channel_id)["segments"]) if args.save_audio else None
        return _join_foreground_channel(channel_id, guild_id, label, self_mute=self_mute, self_deaf=self_deaf, ring_recipient_ids=recipient_ids, transcribe=transcribe, save_audio=args.save_audio, audio_dir=audio_dir)
    return _spawn_detached_call(channel_id, guild_id, label, self_mute=self_mute, self_deaf=self_deaf, notify_targets=notify_targets, ring_recipient_ids=recipient_ids, transcribe=transcribe, save_audio=args.save_audio)


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
        notify = meta.get("notify_targets") or []
        notify_text = f"  notify: {', '.join(notify)}" if notify else "  notify: off"
        transcribe = "transcribe:on" if meta.get("transcribe", True) and not meta.get("self_deaf", False) else "transcribe:off"
        save_audio = "save-audio:on" if meta.get("save_audio") else "save-audio:off"
        print(f"{meta.get('channel_id')}  pid {meta.get('pid')}  {status}  {mute}/{deaf}  {transcribe}  {save_audio}  {meta.get('label')}")
        print(f"  log: {meta.get('log')}")
        if meta.get("segments_dir"):
            print(f"  segments: {meta.get('segments_dir')}")
        print(notify_text)


def diagnose_segment(argv):
    p = argparse.ArgumentParser(prog="discord call diagnose-audio", description="Analyze a saved call transcription WAV/sidecar/packet trace and write decode variants.")
    p.add_argument("segment", help="Path to a saved .wav, .json sidecar, or .packets.jsonl trace")
    p.add_argument("--no-variants", action="store_true", help="Only print metrics; do not write libopus decode variant WAVs")
    args = p.parse_args(argv)
    print(diagnose_saved_voice_segment(args.segment, write_variants=not args.no_variants))


def list_segments(argv):
    p = argparse.ArgumentParser(prog="discord call segments", description="List saved call transcription WAV segments.")
    p.add_argument("target", nargs="?", help="Call channel ID/label; defaults to active call when unambiguous")
    p.add_argument("-n", "--limit", type=int, default=20)
    args = p.parse_args(argv)

    metas = _target_call_metas(argparse.Namespace(target=args.target, all=False))
    dirs = []
    if metas:
        for meta in metas:
            if meta.get("segments_dir"):
                dirs.append(Path(meta["segments_dir"]))
            elif meta.get("channel_id"):
                dirs.append(_call_paths(meta["channel_id"])["segments"])
    elif args.target:
        dirs.append(_call_paths(args.target)["segments"])
    else:
        dirs = sorted(CALL_LOG_DIR.glob("*.segments"))

    files = []
    for directory in dirs:
        if directory.exists():
            files.extend(directory.glob("*.wav"))
    files = sorted(set(files), key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)
    if args.limit > 0:
        files = files[:args.limit]
    if not files:
        print("No saved call transcription audio segments.")
        return
    for path in files:
        sidecar = path.with_suffix(".json")
        transcript = ""
        if sidecar.exists():
            try:
                data = json.loads(sidecar.read_text(errors="replace"))
                transcript = str(data.get("transcript") or "").replace("\n", " ")
            except Exception:
                pass
        print(path)
        if transcript:
            print(f"  transcript: {transcript}")


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
        if field == "self_deaf" and next_value:
            current["transcribe"] = False
        elif field == "self_deaf" and not next_value:
            current["transcribe"] = True
        _bump_control_seq(current)
        _write_call_meta(paths["meta"], current)
        mute = "muted" if current.get("self_mute", True) else "unmuted"
        deaf = "deafened" if current.get("self_deaf", True) else "undeafened"
        transcribe = "transcribe:on" if current.get("transcribe", True) and not current.get("self_deaf", False) else "transcribe:off"
        print(f"Set {current.get('label') or channel_id} to {mute}/{deaf} {transcribe} (pid {current.get('pid')}).")


def _control_call_transcription(argv, *, default_value=None):
    args = _parse_call_voice_state_args("discord call transcribe", argv, default_value=default_value)
    targets = _target_call_metas(args)
    if not targets:
        print("No matching detached Discord call sessions.")
        return

    for meta in targets:
        channel_id = meta.get("channel_id")
        paths = _call_paths(channel_id)
        current = _read_call_meta(paths["meta"]) or meta
        old_value = bool(current.get("transcribe", True)) and not bool(current.get("self_deaf", False))
        next_value = (not old_value) if args.value is None else bool(args.value)
        current["transcribe"] = bool(next_value)
        if next_value:
            current["self_deaf"] = False
        _bump_control_seq(current)
        _write_call_meta(paths["meta"], current)
        mute = "muted" if current.get("self_mute", True) else "unmuted"
        deaf = "deafened" if current.get("self_deaf", True) else "undeafened"
        transcribe = "transcribe:on" if current.get("transcribe", True) and not current.get("self_deaf", False) else "transcribe:off"
        print(f"Set {current.get('label') or channel_id} to {mute}/{deaf} {transcribe} (pid {current.get('pid')}).")


def say(argv):
    p = argparse.ArgumentParser(
        prog="discord call say",
        description="Play an audio file into an already-running detached Discord call.",
    )
    p.add_argument("audio_file", help="audio file to send (mp3/ogg/wav/etc.; decoded by ffmpeg)")
    p.add_argument("target", nargs="?", help="optional active call channel ID / DM label when more than one call is active")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name/ID for resolving a voice channel target")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    args = p.parse_args(argv)
    args.all = False

    audio_path = Path(args.audio_file).expanduser().resolve()
    if not audio_path.exists() or not audio_path.is_file():
        raise SystemExit(f"Audio file not found: {audio_path}")
    if not shutil.which("ffmpeg"):
        raise SystemExit("ffmpeg is required for discord call say")

    metas = _target_call_metas(args)
    if not metas:
        raise SystemExit("No active detached Discord call session. Join a call first with `discord call join ...`.")
    if len(metas) > 1:
        raise SystemExit("More than one detached call is active; pass the target/channel ID as the second argument.")

    meta = metas[0]
    if str(meta.get("status") or "") != "joined":
        raise SystemExit(f"Call is not joined yet (status: {meta.get('status') or 'unknown'}). Try again once `discord call list` shows joined.")
    channel_id = meta.get("channel_id")
    paths = _call_paths(channel_id)
    current = _read_call_meta(paths["meta"]) or meta
    queue_items = current.get("say_queue") if isinstance(current.get("say_queue"), list) else []
    request_id = str(uuid.uuid4())
    queue_items.append({"id": request_id, "path": str(audio_path), "requested_at": time.time()})
    current["say_queue"] = queue_items
    _bump_control_seq(current)
    _write_call_meta(paths["meta"], current)
    print(f"Queued audio for {current.get('label') or channel_id}: {audio_path}")


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
            print("usage: discord call <start|join|say|leave|mute|unmute|deafen|undeafen|transcribe|segments|diagnose-audio|list> ...")
            print("  start <dm> [--dm] [--foreground] [--unmuted] [--deafened] [--no-transcribe] [--save-audio] [--notify-parent CONV_ID|--no-notify]")
            print("  join <target> [--dm|-g SERVER] [--foreground] [--unmuted] [--deafened] [--no-transcribe] [--save-audio] [--notify-parent CONV_ID|--no-notify]")
            print("  mute [target] [on|off|toggle] [--all]")
            print("  unmute [target] [--all]")
            print("  deafen [target] [on|off|toggle] [--all]        # also disables transcription")
            print("  undeafen [target] [--all]                      # also enables transcription")
            print("  transcribe [target] [on|off|toggle] [--all]")
            print("  say <audio-file> [target]                         # requires an already-joined detached call")
            print("  segments [target] [-n LIMIT]")
            print("  diagnose-audio <segment.wav|segment.json|segment.packets.jsonl> [--no-variants]")
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
        if subcmd in {"transcribe", "listen", "listening"}:
            return _control_call_transcription(rest)
        if subcmd in {"no-transcribe", "unlisten"}:
            return _control_call_transcription(rest, default_value=False)
        if subcmd in {"say", "play", "send-audio"}:
            return say(rest)
        if subcmd in {"segments", "clips", "audio", "recordings"}:
            return list_segments(rest)
        if subcmd in {"diagnose-audio", "diagnose", "analyze-audio", "analyse-audio"}:
            return diagnose_segment(rest)
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
