"""Format parsed Discord data for terminal output."""

import json
from src.parse import (
    parse_message, parse_channel, parse_guild, parse_member, parse_user,
    _parse_timestamp,
)


# ─── Messages ────────────────────────────────────────────────────────────────

def format_message(msg, indent=0):
    """Format a single parsed message for terminal display."""
    pad = " " * indent
    lines = []

    author = msg["author"]
    name = author["display_name"]
    username = author["username"]
    time = msg["timestamp_fmt"]
    msg_id = msg["id"]

    # Bot badge
    badge = " [BOT]" if author["bot"] else ""

    # Reply context
    if msg["reply_to"]:
        ref = msg["reply_to"]
        ref_name = ref["author"]["display_name"]
        ref_text = ref["content"][:80]
        if ref_text:
            lines.append(f"{pad}  ↩ {ref_name}: {ref_text}")
        else:
            lines.append(f"{pad}  ↩ {ref_name}")

    # Header
    if username != name:
        lines.append(f"{pad}{name} (@{username}){badge}  {time}  [{msg_id}]")
    else:
        lines.append(f"{pad}{name}{badge}  {time}  [{msg_id}]")

    # Content
    content = msg["content"]
    if content:
        for line in content.split("\n"):
            lines.append(f"{pad}  {line}")

    # Attachments
    for a in msg["attachments"]:
        size = _format_size(a["size"])
        lines.append(f"{pad}  📎 {a['filename']} ({size}) {a['url']}")

    # Embeds
    for e in msg["embeds"]:
        if e["title"]:
            lines.append(f"{pad}  ┌─ {e['title']}")
        if e["description"]:
            desc = e["description"][:200]
            lines.append(f"{pad}  │ {desc}")
        if e["url"]:
            lines.append(f"{pad}  │ {e['url']}")
        if e["title"] or e["description"]:
            lines.append(f"{pad}  └─")

    # Stickers
    for s in msg["stickers"]:
        lines.append(f"{pad}  🏷️ sticker: {s['name']}")

    # Reactions
    if msg["reactions"]:
        rxns = "  ".join(
            f"{r['emoji']} {r['count']}" for r in msg["reactions"]
        )
        lines.append(f"{pad}  {rxns}")

    # Edited indicator
    if msg["edited"]:
        lines[-1] += "  (edited)"

    return "\n".join(lines)


def format_messages(raw_messages, reverse=True):
    """Format a list of raw messages. Reverses to chronological by default."""
    parsed = [parse_message(m) for m in raw_messages]
    if reverse:
        parsed.reverse()
    blocks = [format_message(m) for m in parsed]
    return "\n\n".join(blocks)


# ─── Channels ────────────────────────────────────────────────────────────────

def format_channels(raw_channels):
    """Format a list of channels, grouped by category."""
    parsed = [parse_channel(c) for c in raw_channels]

    # Separate categories and channels
    categories = {c["id"]: c for c in parsed if c["type"] == 4}
    channels = [c for c in parsed if c["type"] != 4]

    # Group by parent
    groups = {}
    for ch in channels:
        parent = ch["parent_id"]
        if parent not in groups:
            groups[parent] = []
        groups[parent].append(ch)

    # Sort categories by position
    sorted_cats = sorted(categories.values(), key=lambda c: c["position"])

    lines = []

    # Channels with no category
    if None in groups:
        for ch in sorted(groups[None], key=lambda c: c["position"]):
            icon = _channel_icon(ch["type_name"])
            lines.append(f"  {ch['id']:20}  {icon} {ch['name']}")

    # Each category
    for cat in sorted_cats:
        lines.append(f"\n  ── {cat['name'].upper()} ──")
        for ch in sorted(groups.get(cat["id"], []), key=lambda c: c["position"]):
            icon = _channel_icon(ch["type_name"])
            topic = f"  — {ch['topic'][:60]}" if ch.get("topic") else ""
            lines.append(f"  {ch['id']:20}  {icon} {ch['name']}{topic}")

    return "\n".join(lines)


def _channel_icon(type_name):
    """Return an icon for a channel type."""
    icons = {
        "text": "#",
        "voice": "🔊",
        "announcement": "📢",
        "stage": "🎭",
        "forum": "💬",
        "media": "📷",
        "thread": "🧵",
        "category": "📁",
    }
    return icons.get(type_name, "?")


# ─── Guilds (Servers) ────────────────────────────────────────────────────────

def format_guilds(raw_guilds):
    """Format a list of guilds."""
    lines = []
    for g in raw_guilds:
        parsed = parse_guild(g)
        owner = " 👑" if parsed["owner"] else ""
        lines.append(f"  {parsed['id']:20}  {parsed['name']}{owner}")
    return "\n".join(lines)


# ─── DMs ──────────────────────────────────────────────────────────────────────

def format_dms(raw_dms):
    """Format DM channel list."""
    lines = []
    for d in raw_dms:
        parsed = parse_channel(d)
        if parsed["type"] == 1:  # DM
            if parsed["recipients"]:
                r = parsed["recipients"][0]
                name = r["display_name"]
                uname = r["username"]
                label = f"{name} (@{uname})" if name != uname else name
            else:
                label = "Unknown"
            lines.append(f"  {parsed['id']:20}  DM     {label}")
        elif parsed["type"] == 3:  # Group DM
            name = parsed["name"] or "Group DM"
            count = len(parsed["recipients"])
            lines.append(f"  {parsed['id']:20}  GROUP  {name} ({count + 1} members)")
    return "\n".join(lines)


# ─── Members ─────────────────────────────────────────────────────────────────

def format_members(raw_members):
    """Format a list of guild members."""
    lines = []
    for m in raw_members:
        parsed = parse_member(m)
        name = parsed["display_name"]
        nick = parsed["nick"]
        label = f"{nick} ({name})" if nick and nick != name else name
        bot = " [BOT]" if parsed["bot"] else ""
        lines.append(f"  {parsed['id']:20}  {label}{bot}")
    return "\n".join(lines)


# ─── Search Results ───────────────────────────────────────────────────────────

def format_search_results(data):
    """Format search results."""
    total = data.get("total_results", 0)
    lines = [f"  {total} result(s)\n"]

    for msg_group in data.get("messages", []):
        # Each result is a list of messages (context), the matching one has hit=true
        for m in msg_group:
            parsed = parse_message(m)
            hit = "▶ " if m.get("hit") else "  "
            lines.append(hit + format_message(parsed, indent=2))
        lines.append("")

    return "\n".join(lines)


# ─── Utility ─────────────────────────────────────────────────────────────────

def _format_size(size_bytes):
    """Format file size."""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f}KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f}MB"


def format_guild_detail(guild):
    """Format detailed guild info."""
    lines = [
        f"  Name:     {guild.get('name', '')}",
        f"  ID:       {guild.get('id', '')}",
        f"  Owner:    {guild.get('owner_id', '')}",
    ]
    if guild.get("description"):
        lines.append(f"  About:    {guild['description']}")
    if guild.get("approximate_member_count"):
        lines.append(f"  Members:  {guild['approximate_member_count']}")
    if guild.get("approximate_presence_count"):
        lines.append(f"  Online:   {guild['approximate_presence_count']}")
    return "\n".join(lines)


def format_channel_detail(ch):
    """Format detailed channel info."""
    parsed = parse_channel(ch)
    icon = _channel_icon(parsed["type_name"])
    lines = [
        f"  Channel:  {icon} {parsed['name']}",
        f"  ID:       {parsed['id']}",
        f"  Type:     {parsed['type_name']}",
    ]
    if parsed.get("topic"):
        lines.append(f"  Topic:    {parsed['topic']}")
    if parsed.get("parent_id"):
        lines.append(f"  Category: {parsed['parent_id']}")
    return "\n".join(lines)
