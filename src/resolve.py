"""Resolve fuzzy targets (server names, channel names, user names) to IDs."""

import re
from src import api
from src.private_channels import (
    private_channel_name,
    private_channel_participants,
    summarize_participants,
)


class AmbiguousTarget(RuntimeError):
    """A name matched more than one possible target; nothing was chosen."""


def _pick(query, candidates, key, *, what, describe):
    """Return the one candidate a name means, or raise listing the choices.

    Tiers: exact (case-insensitive) > starts with > contains.  The first tier
    with any match decides; more than one match there is an error, never a
    guess.  Messages must not go to "the closest" channel or person.
    """
    query_lower = query.lower()
    tiers = (
        lambda name: name == query_lower,
        lambda name: name.startswith(query_lower),
        lambda name: query_lower in name,
    )
    for matches_tier in tiers:
        matches = [c for c in candidates if matches_tier(str(key(c) or "").lower())]
        if len(matches) == 1:
            return matches[0]
        if matches:
            listing = "\n".join(f"  {describe(c)}" for c in matches[:15])
            more = f"\n  … and {len(matches) - 15} more" if len(matches) > 15 else ""
            raise AmbiguousTarget(
                f'"{query}" matches {len(matches)} {what}; use an ID instead:\n{listing}{more}'
            )
    return None


def _describe_guild(guild):
    return f"{guild.get('name', '?')} ({guild.get('id', '?')})"


def _describe_named_channel(channel):
    return f"#{channel.get('name', '?')} ({channel.get('id', '?')})"


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
    match = _pick(target, guilds, lambda g: g.get("name", ""),
                  what="servers", describe=_describe_guild)
    if match:
        return match

    raise RuntimeError(
        f'Cannot resolve server "{target}". '
        f"Use 'discord guilds' to list your servers."
    )


TEXT_CHANNEL_TYPES = {0, 5, 10, 11, 12, 15, 16}


def resolve_channel(target, guild_id=None):
    """Resolve a channel by name or ID.

    If guild_id is provided, searches that guild's text-capable channels.
    Returns channel dict, or raises RuntimeError (AmbiguousTarget when a name
    matches several channels).
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
    text_channels = [c for c in channels if c.get("type", 0) in TEXT_CHANNEL_TYPES]
    match = _pick(target.lstrip("#"), text_channels, lambda c: c.get("name", ""),
                  what="channels", describe=_describe_named_channel)
    if match:
        return match

    raise RuntimeError(
        f'Cannot resolve channel "{target}" in the server. '
        f"Use 'discord channels <server>' to list channels."
    )


def resolve_category(target, guild_id):
    """Resolve a channel category (type 4) in a guild by name or ID."""
    categories = [c for c in api.get_guild_channels(guild_id) if c.get("type") == 4]
    if re.match(r'^\d{17,20}$', target):
        match = next((c for c in categories if c["id"] == target), None)
    else:
        match = _pick(target, categories, lambda c: c.get("name", ""),
                      what="categories", describe=_describe_named_channel)
    if match:
        return match
    raise RuntimeError(
        f'Cannot resolve category "{target}" in the server. '
        f"Use 'discord channels <server>' to list categories."
    )


def _recipient_names(recipient):
    return [
        str(recipient.get(field) or "").lower()
        for field in ("username", "global_name", "display_name")
        if recipient.get(field)
    ]


def _describe_private(channel):
    if channel.get("type") == 1:
        recipient = (channel.get("recipients") or [{}])[0]
        username = recipient.get("username") or "?"
        shown = recipient.get("global_name") or recipient.get("display_name")
        who = f"{shown} (@{username})" if shown and shown != username else f"@{username}"
        return f"DM with {who} ({channel.get('id', '?')})"
    members = summarize_participants(private_channel_participants(channel))
    if channel.get("name"):
        return f'group DM "{channel["name"]}" with {members} ({channel.get("id", "?")})'
    return f'group DM with {members} ({channel.get("id", "?")})'


def resolve_dm(target):
    """Resolve a DM channel by username or ID.

    Exact 1:1 matches win: exact username, then exact display name, then an
    exact group DM name.  Otherwise a name must match exactly one DM or group
    DM by prefix or substring.  Anything ambiguous raises AmbiguousTarget with
    the candidates; a group DM is never chosen over an exact 1:1 match.
    """
    # Snowflake ID
    if re.match(r'^\d{17,20}$', target):
        try:
            return api.get_channel(target)
        except RuntimeError:
            return {"id": target, "name": target}

    dms = [dm for dm in api.get_dm_channels() if dm.get("type") in (1, 3)]
    direct = [dm for dm in dms if dm.get("type") == 1]
    groups = [dm for dm in dms if dm.get("type") == 3]
    query = target.lstrip("@").lower()

    def unique(matches, tier):
        if len(matches) > 1:
            listing = "\n".join(f"  {_describe_private(m)}" for m in matches[:15])
            raise AmbiguousTarget(
                f'"{target}" {tier} {len(matches)} conversations; use a channel ID instead:\n{listing}'
            )
        return matches[0] if matches else None

    def recipients(dm):
        return [r for r in dm.get("recipients", []) if isinstance(r, dict)]

    exact_tiers = (
        ([dm for dm in direct if any(str(r.get("username") or "").lower() == query for r in recipients(dm))],
         "is the exact username in"),
        ([dm for dm in direct if any(
            query in (str(r.get("global_name") or "").lower(), str(r.get("display_name") or "").lower())
            for r in recipients(dm))],
         "is the exact display name in"),
        ([dm for dm in groups if private_channel_name(dm).lower() == query], "is the exact name of"),
    )
    for matches, tier in exact_tiers:
        match = unique(matches, tier)
        if match:
            return match

    def loosely(dm):
        names = (
            [name for r in recipients(dm) for name in _recipient_names(r)]
            if dm.get("type") == 1
            else [private_channel_name(dm).lower()] + [p.lower() for p in private_channel_participants(dm)]
        )
        return any(query in name for name in names)

    match = unique([dm for dm in dms if loosely(dm)], "partly matches")
    if match:
        return match

    raise RuntimeError(
        f'Cannot resolve DM target "{target}". '
        f"Use 'discord dms' to list your conversations."
    )


def describe_destination(channel, guild=None):
    """One line naming where a message went: name, ID, and server or DM partner."""
    if channel.get("type") in (1, 3):
        return _describe_private(channel)
    name = channel.get("name") or channel.get("id", "?")
    where = f"#{name} ({channel.get('id', '?')})"
    guild_id = channel.get("guild_id") or (guild or {}).get("id")
    if guild and guild.get("name") and str(guild.get("id")) == str(guild_id):
        return f"{where} in {guild['name']} ({guild_id})"
    if guild_id:
        return f"{where} in server {guild_id}"
    return where


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
