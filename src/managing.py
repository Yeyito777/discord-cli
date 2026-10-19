"""Managing subcommands — join, leave, typing, read."""

import argparse

from src import api, typing_keeper
from src.private_channels import private_channel_close_message
from src.resolve import resolve_dm, resolve_guild
from src.invite import join_server


def join(argv):
    p = argparse.ArgumentParser(allow_abbrev=False, prog="discord join", description="Join a server via invite link.")
    p.add_argument("invite", help="Invite link or code (e.g. discord.gg/abc123 or abc123)")
    args = p.parse_args(argv)

    result = join_server(args.invite)
    name = result.get("guild", {}).get("name", "Unknown")
    guild_id = result.get("guild", {}).get("id", "")
    print(f"Joined {name}. Guild ID: {guild_id}")


def _close_private_channel(target: str) -> None:
    ch = resolve_dm(target)
    api.close_private_channel(ch["id"])
    print(private_channel_close_message(ch))


def leave(argv):
    p = argparse.ArgumentParser(
        allow_abbrev=False,
        prog="discord leave",
        description="Leave a server, or close/leave a DM or group DM.",
    )
    p.add_argument("target", help="Server name/ID, or DM/group-DM name/ID with --dm")
    p.add_argument("--dm", action="store_true", help="Treat target as a DM or group-DM conversation to close/leave")
    args = p.parse_args(argv)

    if args.dm:
        _close_private_channel(args.target)
        return

    try:
        g = resolve_guild(args.target)
    except RuntimeError:
        _close_private_channel(args.target)
        return

    api.leave_guild(g["id"])
    print(f"Left {g.get('name', g['id'])}.")


def typing(argv):
    p = argparse.ArgumentParser(
        allow_abbrev=False,
        prog="discord typing",
        description="Show the typing indicator until this account's next send in the channel, or a timeout.",
    )
    p.add_argument("channel", help="Channel ID")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--for", dest="seconds", type=float, metavar="SECONDS",
                      help=f"keep typing for at most SECONDS (default {typing_keeper.DEFAULT_SECONDS}, "
                           f"max {typing_keeper.MAX_SECONDS})")
    mode.add_argument("--once", action="store_true", help="trigger once (~10 s) without a keeper")
    mode.add_argument("--stop", action="store_true", help="stop typing in this channel without sending")
    args = p.parse_args(argv)

    if args.stop:
        stopped = typing_keeper.stop(args.channel)
        print("Typing stopped." if stopped else "No typing indicator was running.")
        return
    if args.once:
        api.trigger_typing(args.channel)
        print("Typing indicator sent.")
        return
    seconds = typing_keeper.DEFAULT_SECONDS if args.seconds is None else args.seconds
    if not 0 < seconds <= typing_keeper.MAX_SECONDS:
        p.error(f"--for must be between 0 and {typing_keeper.MAX_SECONDS} seconds")
    typing_keeper.start(args.channel, seconds)
    print(f"Typing until the next send here, or {seconds:g}s.")


def read(argv):
    p = argparse.ArgumentParser(allow_abbrev=False, prog="discord read", description="Mark channel as read.")
    p.add_argument("channel", help="Channel ID")
    p.add_argument("message", nargs="?", help="Message ID to mark read up to (default: latest)")
    args = p.parse_args(argv)

    channel_id = args.channel
    message_id = args.message
    if not message_id:
        msgs = api.get_messages(channel_id, limit=1)
        if msgs:
            message_id = msgs[0]["id"]
        else:
            print("  No messages to mark as read.")
            return

    api.ack_message(channel_id, message_id)
    print("Marked as read.")


_COMMANDS = {
    "join": join,
    "leave": leave,
    "typing": typing,
    "read": read,
}


def dispatch(cmd, argv):
    """Dispatch managing subcommands."""
    fn = _COMMANDS.get(cmd)
    if fn is None:
        raise RuntimeError(f"Unknown managing command: {cmd}")
    fn(argv)
