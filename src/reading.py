"""Reading subcommands — guilds, channels, messages, DMs, search, etc."""

import argparse
import os
import sys

from src import api
from src import format as fmt
from src.resolve import resolve_guild, resolve_channel, resolve_dm


def guilds(argv):
    p = argparse.ArgumentParser(prog="discord guilds", description="List your servers.")
    p.parse_args(argv)
    data = api.get_guilds()
    print(fmt.format_guilds(data))


def guild(argv):
    p = argparse.ArgumentParser(prog="discord guild", description="Show server details.")
    p.add_argument("target", help="Server name or ID")
    args = p.parse_args(argv)
    g = resolve_guild(args.target)
    data = api.get_guild(g["id"])
    print(fmt.format_guild_detail(data))


def channels(argv):
    p = argparse.ArgumentParser(prog="discord channels", description="List channels in a server.")
    p.add_argument("server", help="Server name or ID")
    args = p.parse_args(argv)
    g = resolve_guild(args.server)
    data = api.get_guild_channels(g["id"])
    print(fmt.format_channels(data))


def messages(argv):
    p = argparse.ArgumentParser(prog="discord messages", description="Read messages from a channel.")
    p.add_argument("channel", help="Channel name or ID")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name or ID (required if using channel name)")
    p.add_argument("-n", "--limit", type=int, default=20, help="Number of messages (default: 20)")
    p.add_argument("--before", help="Get messages before this message ID")
    p.add_argument("--after", help="Get messages after this message ID")
    args = p.parse_args(argv)

    guild_id = None
    if args.guild:
        g = resolve_guild(args.guild)
        guild_id = g["id"]
    ch = resolve_channel(args.channel, guild_id)

    data = api.get_messages(ch["id"], limit=args.limit, before=args.before, after=args.after)
    if not data:
        print("  No messages.")
    else:
        print(fmt.format_messages(data))


def dms(argv):
    p = argparse.ArgumentParser(prog="discord dms", description="List DM conversations.")
    p.parse_args(argv)
    data = api.get_dm_channels()
    print(fmt.format_dms(data))


def dm(argv):
    p = argparse.ArgumentParser(prog="discord dm", description="Read or send DMs.")
    p.add_argument("target", help="Username or DM channel ID")
    p.add_argument("-n", "--limit", type=int, default=20, help="Number of messages")
    p.add_argument("--send", dest="send_text", nargs="?", const="", default=None,
                   help="Send a DM (text optional when using --file)")
    p.add_argument("-f", "--file", nargs="+", dest="files", metavar="PATH",
                   help="File(s) to attach (requires --send)")
    p.add_argument("--before", help="Get messages before this message ID")
    args = p.parse_args(argv)

    if args.files and args.send_text is None:
        p.error("--file requires --send")

    d = resolve_dm(args.target)
    channel_id = d["id"]

    if args.send_text is not None:
        text = args.send_text or None  # convert empty string to None
        if args.files:
            for fp in args.files:
                if not os.path.isfile(fp):
                    raise RuntimeError(f"File not found: {fp}")
            data = api.send_message_with_files(channel_id, args.files, content=text)
        else:
            if not text:
                p.error("must provide text with --send or use --file")
            data = api.send_message(channel_id, text)
        print(f"Sent. Message ID: {data['id']}")
        return

    data = api.get_messages(channel_id, limit=args.limit, before=args.before)
    if not data:
        print("  No messages.")
    else:
        print(fmt.format_messages(data))


def search(argv):
    p = argparse.ArgumentParser(prog="discord search", description="Search messages.")
    p.add_argument("query", help="Search query")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server to search in")
    p.add_argument("-c", "--channel", help="Channel to search in")
    p.add_argument("-n", "--limit", type=int, default=25, help="Max results")
    args = p.parse_args(argv)

    if args.guild:
        g = resolve_guild(args.guild)
        data = api.search_guild(g["id"], content=args.query, channel_id=args.channel, limit=args.limit)
    elif args.channel:
        data = api.search_channel(args.channel, content=args.query, limit=args.limit)
    else:
        raise RuntimeError(
            "Search requires --guild/-g or --channel/-c. "
            "Example: discord search 'hello' -g 'My Server'"
        )
    print(fmt.format_search_results(data))


def pins(argv):
    p = argparse.ArgumentParser(prog="discord pins", description="Get pinned messages.")
    p.add_argument("channel", help="Channel ID")
    args = p.parse_args(argv)

    data = api.get_pins(args.channel)
    if not data:
        print("  No pinned messages.")
    else:
        print(fmt.format_messages(data, reverse=False))


def threads(argv):
    p = argparse.ArgumentParser(prog="discord threads", description="List active threads in a server.")
    p.add_argument("server", help="Server name or ID")
    args = p.parse_args(argv)

    g = resolve_guild(args.server)
    data = api.get_active_threads(g["id"])
    thread_list = data.get("threads", [])
    if not thread_list:
        print("  No active threads.")
        return
    lines = []
    for t in thread_list:
        name = t.get("name", "?")
        tid = t["id"]
        count = t.get("message_count", "?")
        lines.append(f"  {tid:20}  🧵 {name}  ({count} messages)")
    print("\n".join(lines))


def members(argv):
    p = argparse.ArgumentParser(prog="discord members", description="List or search server members.")
    p.add_argument("server", help="Server name or ID")
    p.add_argument("-q", "--query", help="Search for members by name")
    p.add_argument("-n", "--limit", type=int, default=50, help="Max results")
    args = p.parse_args(argv)

    g = resolve_guild(args.server)
    if args.query:
        data = api.search_guild_members(g["id"], args.query, limit=args.limit)
    else:
        data = api.get_guild_members(g["id"], limit=args.limit)
    print(fmt.format_members(data))


def me(argv):
    p = argparse.ArgumentParser(prog="discord me", description="Show current user info.")
    p.parse_args(argv)

    data = api.get_me()
    name = data.get("global_name") or data["username"]
    lines = [
        f"  {name} (@{data['username']})",
        f"  ID: {data['id']}",
    ]
    if data.get("email"):
        lines.append(f"  Email: {data['email']}")
    if data.get("phone"):
        lines.append(f"  Phone: {data['phone']}")
    print("\n".join(lines))


_ALIASES = {
    "servers": "guilds",
    "server": "guild",
    "chs": "channels",
    "msgs": "messages",
    "s": "search",
}

_COMMANDS = {
    "guilds": guilds,
    "guild": guild,
    "channels": channels,
    "messages": messages,
    "dms": dms,
    "dm": dm,
    "search": search,
    "pins": pins,
    "threads": threads,
    "members": members,
    "me": me,
}


def dispatch(cmd, argv):
    """Dispatch reading subcommands, resolving aliases."""
    canonical = _ALIASES.get(cmd, cmd)
    fn = _COMMANDS.get(canonical)
    if fn is None:
        raise RuntimeError(f"Unknown reading command: {cmd}")
    fn(argv)
