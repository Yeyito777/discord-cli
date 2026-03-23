"""Parse Discord API responses into clean dicts."""

from datetime import datetime, timezone


def _parse_timestamp(ts):
    """Parse ISO timestamp to human-readable format."""
    if not ts:
        return ""
    try:
        dt = datetime.fromisoformat(ts.replace("+00:00", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - dt

        if delta.days == 0:
            return dt.strftime("%I:%M %p")
        elif delta.days == 1:
            return "Yesterday " + dt.strftime("%I:%M %p")
        elif delta.days < 7:
            return dt.strftime("%a %I:%M %p")
        elif dt.year == now.year:
            return dt.strftime("%b %d %I:%M %p")
        else:
            return dt.strftime("%b %d, %Y %I:%M %p")
    except (ValueError, TypeError):
        return ts[:19] if ts else ""


def parse_user(user):
    """Parse a user object."""
    return {
        "id": user["id"],
        "username": user.get("username", ""),
        "display_name": user.get("global_name") or user.get("username", ""),
        "discriminator": user.get("discriminator", "0"),
        "avatar": user.get("avatar"),
        "bot": user.get("bot", False),
    }


def parse_message(msg):
    """Parse a message object into a clean dict."""
    author = parse_user(msg["author"])

    # Parse attachments
    attachments = []
    for a in msg.get("attachments", []):
        attachments.append({
            "id": a["id"],
            "filename": a["filename"],
            "size": a["size"],
            "url": a["url"],
            "content_type": a.get("content_type", ""),
        })

    # Parse embeds
    embeds = []
    for e in msg.get("embeds", []):
        embeds.append({
            "type": e.get("type", ""),
            "title": e.get("title", ""),
            "description": e.get("description", ""),
            "url": e.get("url", ""),
            "color": e.get("color"),
        })

    # Parse reactions
    reactions = []
    for r in msg.get("reactions", []):
        emoji = r["emoji"]
        reactions.append({
            "emoji": emoji.get("name", "?"),
            "emoji_id": emoji.get("id"),
            "count": r.get("count", 0),
            "me": r.get("me", False),
        })

    # Parse reply reference
    reply_to = None
    ref = msg.get("referenced_message")
    if ref:
        ref_author = parse_user(ref["author"])
        reply_to = {
            "id": ref["id"],
            "author": ref_author,
            "content": ref.get("content", ""),
        }

    # Stickers
    stickers = []
    for s in msg.get("sticker_items", []):
        stickers.append({
            "id": s["id"],
            "name": s["name"],
        })

    return {
        "id": msg["id"],
        "channel_id": msg.get("channel_id", ""),
        "author": author,
        "content": msg.get("content", ""),
        "timestamp": msg.get("timestamp", ""),
        "timestamp_fmt": _parse_timestamp(msg.get("timestamp")),
        "edited": msg.get("edited_timestamp") is not None,
        "type": msg.get("type", 0),
        "attachments": attachments,
        "embeds": embeds,
        "reactions": reactions,
        "reply_to": reply_to,
        "stickers": stickers,
        "pinned": msg.get("pinned", False),
        "mentions": [parse_user(u) for u in msg.get("mentions", [])],
    }


def parse_channel(ch):
    """Parse a channel object."""
    TYPE_NAMES = {
        0: "text", 1: "dm", 2: "voice", 3: "group_dm", 4: "category",
        5: "announcement", 10: "thread", 11: "thread", 12: "thread",
        13: "stage", 14: "directory", 15: "forum", 16: "media",
    }
    return {
        "id": ch["id"],
        "name": ch.get("name", ""),
        "type": ch.get("type", 0),
        "type_name": TYPE_NAMES.get(ch.get("type", 0), "unknown"),
        "position": ch.get("position", 0),
        "parent_id": ch.get("parent_id"),
        "topic": ch.get("topic", ""),
        "nsfw": ch.get("nsfw", False),
        "last_message_id": ch.get("last_message_id"),
        # DM-specific
        "recipients": [parse_user(r) for r in ch.get("recipients", [])],
    }


def parse_guild(guild):
    """Parse a guild object."""
    return {
        "id": guild["id"],
        "name": guild.get("name", ""),
        "icon": guild.get("icon"),
        "owner": guild.get("owner", False),
        "member_count": guild.get("approximate_member_count"),
        "online_count": guild.get("approximate_presence_count"),
    }


def parse_member(member):
    """Parse a guild member object."""
    user = parse_user(member.get("user", {}))
    return {
        **user,
        "nick": member.get("nick"),
        "roles": member.get("roles", []),
        "joined_at": member.get("joined_at", ""),
        "joined_at_fmt": _parse_timestamp(member.get("joined_at")),
    }
