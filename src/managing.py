"""Managing subcommands — join, leave, typing, read."""

import argparse

from src import api
from src.resolve import resolve_guild
from src.invite import join_server


def join(argv):
    p = argparse.ArgumentParser(prog="discord join", description="Join a server via invite link.")
    p.add_argument("invite", help="Invite link or code (e.g. discord.gg/abc123 or abc123)")
    args = p.parse_args(argv)

    result = join_server(args.invite)
    name = result.get("guild", {}).get("name", "Unknown")
    guild_id = result.get("guild", {}).get("id", "")
    print(f"Joined {name}. Guild ID: {guild_id}")


def leave(argv):
    p = argparse.ArgumentParser(prog="discord leave", description="Leave a server.")
    p.add_argument("server", help="Server name or ID")
    args = p.parse_args(argv)

    g = resolve_guild(args.server)
    api.leave_guild(g["id"])
    print(f"Left {g.get('name', g['id'])}.")


def typing(argv):
    p = argparse.ArgumentParser(prog="discord typing", description="Trigger typing indicator.")
    p.add_argument("channel", help="Channel ID")
    args = p.parse_args(argv)

    api.trigger_typing(args.channel)
    print("Typing indicator sent.")


def read(argv):
    p = argparse.ArgumentParser(prog="discord read", description="Mark channel as read.")
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
