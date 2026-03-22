"""Resolve fuzzy targets (server names, channel names, user names) to IDs."""

import re
from src import api


def _fuzzy_match(query, candidates, key):
    """Find best fuzzy match from candidates.

    Returns the best matching candidate or None.
    Priority: exact match > starts with > contains (case-insensitive).
    """
    query_lower = query.lower()

    exact = [c for c in candidates if key(c).lower() == query_lower]
    if exact:
        return exact[0]

    starts = [c for c in candidates if key(c).lower().startswith(query_lower)]
    if len(starts) == 1:
        return starts[0]

    contains = [c for c in candidates if query_lower in key(c).lower()]
    if len(contains) == 1:
        return contains[0]

    # If multiple matches, prefer shortest name (most specific)
    if contains:
        return min(contains, key=lambda c: len(key(c)))

    return None


def resolve_guild(target):
    """Resolve a guild by name or ID.

    Returns guild dict with at least 'id' and 'name', or raises RuntimeError.
    """
    # If it looks like a snowflake ID, use it directly
    if re.match(r'^\d{17,20}$', target):
        try:
            return api.get_guild(target)
        except RuntimeError:
            return {"id": target, "name": target}

    guilds = api.get_guilds()
    match = _fuzzy_match(target, guilds, lambda g: g.get("name", ""))
    if match:
        return match

    raise RuntimeError(
        f'Cannot resolve server "{target}". '
        f"Use 'discord guilds' to list your servers."
    )


def resolve_channel(target, guild_id=None):
    """Resolve a channel by name or ID.

    If guild_id is provided, searches that guild's channels.
    Returns channel dict, or raises RuntimeError.
    """
    # Snowflake ID
    if re.match(r'^\d{17,20}$', target):
        try:
            return api.get_channel(target)
        except RuntimeError:
            return {"id": target, "name": target}

    if not guild_id:
        raise RuntimeError(
            f'Cannot resolve channel "{target}" without a server. '
            f'Use --guild/-g to specify the server, or use a channel ID.'
        )

    channels = api.get_guild_channels(guild_id)
    # Filter to text-like channels (text, announcement, thread, forum)
    text_types = {0, 5, 10, 11, 12, 15, 16}
    text_channels = [c for c in channels if c.get("type", 0) in text_types]

    match = _fuzzy_match(target, text_channels, lambda c: c.get("name", ""))
    if match:
        return match

    # Also try all channels
    match = _fuzzy_match(target, channels, lambda c: c.get("name", ""))
    if match:
        return match

    raise RuntimeError(
        f'Cannot resolve channel "{target}" in the server. '
        f"Use 'discord channels <server>' to list channels."
    )


def resolve_dm(target):
    """Resolve a DM channel by username or ID.

    Returns channel dict, or raises RuntimeError.
    """
    # Snowflake ID
    if re.match(r'^\d{17,20}$', target):
        try:
            return api.get_channel(target)
        except RuntimeError:
            return {"id": target, "name": target}

    dms = api.get_dm_channels()

    # Search DM recipients by username or display name
    for dm in dms:
        if dm.get("type") == 1:  # Direct DM
            for r in dm.get("recipients", []):
                name = r.get("global_name", "") or ""
                uname = r.get("username", "") or ""
                if (target.lower() in name.lower() or
                    target.lower() in uname.lower() or
                    target.lstrip("@").lower() == uname.lower()):
                    return dm
        elif dm.get("type") == 3:  # Group DM
            group_name = dm.get("name", "") or ""
            if target.lower() in group_name.lower():
                return dm

    raise RuntimeError(
        f'Cannot resolve DM target "{target}". '
        f"Use 'discord dms' to list your conversations."
    )


def parse_message_link(link):
    """Parse a Discord message link into (guild_id, channel_id, message_id).

    Supports:
      https://discord.com/channels/GUILD/CHANNEL/MESSAGE
      CHANNEL/MESSAGE  (just IDs)
    """
    m = re.match(
        r'(?:https?://(?:ptb\.|canary\.)?discord\.com/channels/)?'
        r'(\d{17,20})/(\d{17,20})/(\d{17,20})',
        link,
    )
    if m:
        return m.group(1), m.group(2), m.group(3)

    # channel_id/message_id
    m = re.match(r'(\d{17,20})/(\d{17,20})', link)
    if m:
        return None, m.group(1), m.group(2)

    return None, None, None
