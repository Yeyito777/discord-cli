"""Writing subcommands — send, reply, edit, delete, react, unreact."""

import argparse
import os
import re

from src import api
from src.resolve import resolve_guild, resolve_channel


def _validate_files(file_paths):
    """Validate that all file paths exist and are readable."""
    for fp in file_paths:
        if not os.path.isfile(fp):
            raise RuntimeError(f"File not found: {fp}")
    return file_paths


def _resolve_mentions(text, guild_id=None):
    """Replace @username with <@user_id> in outgoing messages.

    Checks notify config labels first (no API call), then tries guild
    member search for server channels. Skips @everyone, @here, and
    already-resolved mentions like <@123>.
    """
    if not text or "@" not in text:
        return text

    # Build username → user_id map from notify config
    known = {}
    try:
        from src.notify import get_labels
        for user_id, entry in get_labels().items():
            if isinstance(entry, dict) and entry.get("username"):
                known[entry["username"].lower()] = user_id
    except Exception:
        pass

    def replacer(match):
        username = match.group(1)
        if username.lower() in ("everyone", "here"):
            return match.group(0)  # leave @everyone/@here as-is

        # Check known users first (from notify config)
        uid = known.get(username.lower())
        if uid:
            return f"<@{uid}>"

        # Try guild member search for server channels
        if guild_id:
            try:
                members = api.search_guild_members(guild_id, username, limit=1)
                if members:
                    member = members[0]
                    user = member.get("user", member)
                    if user.get("username", "").lower() == username.lower():
                        return f"<@{user['id']}>"
            except Exception:
                pass

        return match.group(0)  # no match — leave as-is

    # Match @username but not <@id> or email-like patterns
    return re.sub(r"(?<![<\w])@([\w.]{2,32})(?![\w.])", replacer, text)


def send(argv):
    p = argparse.ArgumentParser(prog="discord send", description="Send a message.")
    p.add_argument("channel", help="Channel name or ID")
    p.add_argument("text", nargs="?", default=None, help="Message text (optional when using --file)")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server (required if using channel name)")
    p.add_argument("--reply", help="Message ID to reply to")
    p.add_argument("-f", "--file", nargs="+", dest="files", metavar="PATH", help="File(s) to attach")
    args = p.parse_args(argv)

    if not args.text and not args.files:
        p.error("must provide text and/or --file")

    guild_id = None
    if args.guild:
        g = resolve_guild(args.guild)
        guild_id = g["id"]
    ch = resolve_channel(args.channel, guild_id)

    text = _resolve_mentions(args.text, guild_id)

    if args.files:
        _validate_files(args.files)
        data = api.send_message_with_files(
            ch["id"], args.files, content=text, reply_to=args.reply,
        )
    else:
        data = api.send_message(ch["id"], text, reply_to=args.reply)
    print(f"Sent. Message ID: {data['id']}")


def reply(argv):
    p = argparse.ArgumentParser(prog="discord reply", description="Reply to a message.")
    p.add_argument("channel", help="Channel name or ID")
    p.add_argument("message", help="Message ID to reply to")
    p.add_argument("text", nargs="?", default=None, help="Reply text (optional when using --file)")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server (required if using channel name)")
    p.add_argument("-f", "--file", nargs="+", dest="files", metavar="PATH", help="File(s) to attach")
    args = p.parse_args(argv)

    if not args.text and not args.files:
        p.error("must provide text and/or --file")

    guild_id = None
    if args.guild:
        g = resolve_guild(args.guild)
        guild_id = g["id"]
    ch = resolve_channel(args.channel, guild_id)

    text = _resolve_mentions(args.text, guild_id)

    if args.files:
        _validate_files(args.files)
        data = api.send_message_with_files(
            ch["id"], args.files, content=text, reply_to=args.message,
        )
    else:
        data = api.send_message(ch["id"], text, reply_to=args.message)
    print(f"Replied. Message ID: {data['id']}")


def edit(argv):
    p = argparse.ArgumentParser(prog="discord edit", description="Edit a message.")
    p.add_argument("channel", help="Channel ID")
    p.add_argument("message", help="Message ID")
    p.add_argument("text", help="New message text")
    args = p.parse_args(argv)

    api.edit_message(args.channel, args.message, args.text)
    print(f"Edited.")


def delete(argv):
    p = argparse.ArgumentParser(prog="discord delete", description="Delete a message.")
    p.add_argument("channel", help="Channel ID")
    p.add_argument("message", help="Message ID")
    args = p.parse_args(argv)

    api.delete_message(args.channel, args.message)
    print(f"Deleted.")


def react(argv):
    p = argparse.ArgumentParser(prog="discord react", description="React to a message.")
    p.add_argument("channel", help="Channel ID")
    p.add_argument("message", help="Message ID")
    p.add_argument("emoji", help="Emoji (e.g. 👍 or custom_name:123456)")
    args = p.parse_args(argv)

    api.add_reaction(args.channel, args.message, args.emoji)
    print(f"Reacted {args.emoji}.")


def unreact(argv):
    p = argparse.ArgumentParser(prog="discord unreact", description="Remove a reaction.")
    p.add_argument("channel", help="Channel ID")
    p.add_argument("message", help="Message ID")
    p.add_argument("emoji", help="Emoji to remove")
    args = p.parse_args(argv)

    api.remove_reaction(args.channel, args.message, args.emoji)
    print(f"Removed {args.emoji}.")


_ALIASES = {
    "del": "delete",
}

_COMMANDS = {
    "send": send,
    "reply": reply,
    "edit": edit,
    "delete": delete,
    "react": react,
    "unreact": unreact,
}


def dispatch(cmd, argv):
    """Dispatch writing subcommands, resolving aliases."""
    canonical = _ALIASES.get(cmd, cmd)
    fn = _COMMANDS.get(canonical)
    if fn is None:
        raise RuntimeError(f"Unknown writing command: {cmd}")
    fn(argv)
