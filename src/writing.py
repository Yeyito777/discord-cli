"""Writing subcommands — send, reply, edit, delete, react, unreact."""

import argparse
import os
import re
import sys

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

    # Build name → user_id map from notify config.
    # Maps usernames, label names, and display names — all case-insensitive.
    # Ambiguous names (shared by multiple users) are removed.
    known = {}
    _seen = {}  # lowercase name → user_id (to detect conflicts)
    try:
        from src.notify import get_labels
        for user_id, entry in get_labels().items():
            if not isinstance(entry, dict):
                continue
            names = []
            if entry.get("username"):
                names.append(entry["username"])
            if entry.get("name"):
                names.append(entry["name"])
            if entry.get("display_name"):
                names.append(entry["display_name"])
            if entry.get("global_name"):
                names.append(entry["global_name"])
            for name in names:
                key = name.lower()
                if key in _seen:
                    if _seen[key] != user_id:
                        # Conflict — two users share this name; remove it
                        known.pop(key, None)
                        _seen[key] = None  # mark as conflicted
                else:
                    _seen[key] = user_id
                    known[key] = user_id
    except Exception:
        pass

    unresolved = []

    def replacer(match):
        mention = match.group(1)
        if mention.lower() in ("everyone", "here"):
            return match.group(0)  # leave @everyone/@here as-is

        # Check known users first (from notify config)
        uid = known.get(mention.lower())
        if uid:
            return f"<@{uid}>"

        # Try guild member search for server channels
        if guild_id:
            try:
                members = api.search_guild_members(guild_id, mention, limit=5)
                for member in members:
                    user = member.get("user", member)
                    nick = member.get("nick") or ""
                    uname = user.get("username") or ""
                    global_name = user.get("global_name") or ""
                    if mention.lower() in (
                        uname.lower(), global_name.lower(), nick.lower(),
                    ):
                        return f"<@{user['id']}>"
            except Exception:
                pass

        unresolved.append(mention)
        return match.group(0)

    # Match @username but not <@id> or email-like patterns
    result = re.sub(r"(?<![<\w])@([\w.]{2,32})(?![\w.])", replacer, text)

    if unresolved:
        names = ", ".join(f"@{n}" for n in unresolved)
        print(
            f"Error: could not resolve mention(s): {names}\n"
            f"Hint: not found in notify.json labels or guild member search",
            file=sys.stderr,
        )
        sys.exit(1)

    return result


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
