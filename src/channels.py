"""Channel management — create a server channel, move it in the channel list."""

import argparse

from src import api
from src.resolve import resolve_category, resolve_channel, resolve_guild

CHANNEL_TYPES = {"text": 0, "voice": 2, "category": 4, "announcement": 5, "forum": 15}
TOPIC_LIMITS = {"text": 1024, "announcement": 1024, "forum": 4096}


def parse_position(value):
    """``top`` or a 0-based index among the channel's siblings."""
    if value == "top":
        return 0
    try:
        position = int(value)
    except ValueError:
        position = -1
    if position < 0:
        raise argparse.ArgumentTypeError("must be 'top' or a number from 0")
    return position


def _siblings(channels, channel):
    parent = channel.get("parent_id")
    return sorted(
        (c for c in channels if c.get("parent_id") == parent),
        key=lambda c: (c.get("position", 0), int(c["id"])),
    )


def reorder(guild_id, channel_id, position):
    """Place a channel at ``position`` among its siblings; return the final index.

    Siblings are renumbered 0..n-1 and only entries whose number changes are
    sent, so the result does not depend on how Discord shifts the others.
    """
    channels = api.get_guild_channels(guild_id)
    channel = next((c for c in channels if c["id"] == channel_id), None)
    if channel is None:
        raise RuntimeError(f"Channel {channel_id} is not in server {guild_id}.")
    order = [c for c in _siblings(channels, channel) if c["id"] != channel_id]
    index = min(position, len(order))
    order.insert(index, channel)
    changes = [
        {"id": c["id"], "position": new}
        for new, c in enumerate(order)
        if c.get("position") != new
    ]
    if changes:
        api.set_channel_positions(guild_id, changes)
    return index


def create(argv):
    p = argparse.ArgumentParser(
        allow_abbrev=False,
        prog="discord channel create",
        description="Create a channel in a server (needs Manage Channels).",
    )
    p.add_argument("server", help="Server name or ID")
    p.add_argument("name", help="Channel name (1-100 characters)")
    p.add_argument("--type", choices=sorted(CHANNEL_TYPES), default="text", help="Channel type (default: text)")
    p.add_argument("--topic", help="Channel topic: text and announcement channels up to 1024 characters, "
                                   "or a forum's post guidelines up to 4096")
    p.add_argument("--tag", action="append", dest="tags", metavar="NAME",
                   help="Tag posts can use, repeatable (forum channels only, up to 20, 20 characters each)")
    p.add_argument("--position", type=parse_position, metavar="top|N",
                   help="Place it first ('top') or at index N among its siblings (default: Discord's, at the end)")
    p.add_argument("--parent", metavar="CATEGORY", help="Category name or ID to create it in")
    args = p.parse_args(argv)

    if not 1 <= len(args.name) <= 100:
        p.error("channel name must be between 1 and 100 characters")
    if args.topic is not None:
        if args.type not in TOPIC_LIMITS:
            p.error("--topic is only valid for text, announcement and forum channels")
        if len(args.topic) > TOPIC_LIMITS[args.type]:
            p.error(f"--topic must be at most {TOPIC_LIMITS[args.type]} characters")
    if args.tags:
        if args.type != "forum":
            p.error("--tag is only valid for forum channels")
        if len(args.tags) > 20 or not all(1 <= len(tag) <= 20 for tag in args.tags):
            p.error("a forum takes up to 20 tags of 1-20 characters each")
    if args.parent and args.type == "category":
        p.error("a category cannot have a parent")

    guild = resolve_guild(args.server)
    parent_id = resolve_category(args.parent, guild["id"])["id"] if args.parent else None
    channel_type = CHANNEL_TYPES[args.type]

    # Names are not unique in Discord; a repeated create would silently duplicate.
    for existing in api.get_guild_channels(guild["id"]):
        if (existing.get("type") == channel_type and existing.get("parent_id") == parent_id
                and str(existing.get("name", "")).lower() == args.name.lower()):
            raise RuntimeError(
                f"#{existing['name']} ({existing['id']}) already exists here; nothing created."
            )

    result = api.create_guild_channel(
        guild["id"], args.name, channel_type=channel_type, topic=args.topic, parent_id=parent_id,
        tags=args.tags,
    )
    channel_id = result["id"]
    name = result.get("name") or args.name
    print(f"Created {args.type} channel #{name}. Channel ID: {channel_id}")
    print(f"https://discord.com/channels/{guild['id']}/{channel_id}")

    if args.position is not None:
        try:
            index = reorder(guild["id"], channel_id, args.position)
        except RuntimeError as e:
            raise RuntimeError(
                f"The channel was created (ID {channel_id}) but moving it failed: {e}\n"
                f"Retry with: discord channel move {channel_id} --position {args.position}"
            )
        print(f"Moved to position {index}{' (top)' if index == 0 else ''}.")


def move(argv):
    p = argparse.ArgumentParser(
        allow_abbrev=False,
        prog="discord channel move",
        description="Move a channel within its category (or the top level) of the channel list.",
    )
    p.add_argument("channel", help="Channel name or ID")
    p.add_argument("-g", "--guild", "--server", dest="guild",
                   help="Server (required if using a channel name)")
    p.add_argument("--position", type=parse_position, required=True, metavar="top|N",
                   help="'top' or the 0-based index among its siblings")
    args = p.parse_args(argv)

    guild_id = resolve_guild(args.guild)["id"] if args.guild else None
    channel = resolve_channel(args.channel, guild_id)
    guild_id = channel.get("guild_id") or guild_id
    if not guild_id:
        raise RuntimeError(f"{args.channel} is not a server channel.")
    index = reorder(guild_id, channel["id"], args.position)
    name = channel.get("name") or channel["id"]
    print(f"Moved #{name} to position {index}{' (top)' if index == 0 else ''}.")


_SUBCOMMANDS = {"create": create, "move": move}


def dispatch(cmd, argv):
    if cmd != "channel":
        raise RuntimeError(f"Unknown channel command: {cmd}")
    if not argv or argv[0] in ("-h", "--help"):
        print("usage: discord channel {create,move} ...\n\n"
              "  create SERVER NAME [--type text] [--topic T] [--tag NAME ...] [--position top|N] [--parent CATEGORY]\n"
              "  move CHANNEL --position top|N [-g SERVER]")
        return
    fn = _SUBCOMMANDS.get(argv[0])
    if fn is None:
        raise RuntimeError(f"Unknown channel subcommand '{argv[0]}'. Use: create, move.")
    fn(argv[1:])
