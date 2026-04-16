"""Helpers for naming and labeling Discord private channels."""

from __future__ import annotations


def private_channel_type(ch: dict) -> str | None:
    """Return the normalized private-channel type, if any."""
    ch_type = ch.get("type")
    if ch_type == 1:
        return "dm"
    if ch_type == 3:
        return "group_dm"
    return None


def private_channel_participants(ch: dict) -> list[str]:
    """Return display names for any known private-channel participants."""
    recipients = ch.get("recipients", ch.get("recipient_ids", [])) or []
    participants: list[str] = []
    for recipient in recipients:
        if not isinstance(recipient, dict):
            continue
        name = (
            recipient.get("display_name")
            or recipient.get("global_name")
            or recipient.get("username")
            or "?"
        )
        participants.append(name)
    return participants


def summarize_participants(participants: list[str], *, limit: int = 3) -> str:
    """Return a compact participant preview like 'a, b, c, +2'."""
    if not participants:
        return ""
    preview = ", ".join(participants[:limit])
    remaining = len(participants) - limit
    if remaining > 0:
        preview += f", +{remaining}"
    return preview


def private_channel_name(ch: dict, *, default: str | None = None) -> str:
    """Return a stable friendly name for a DM or group DM."""
    channel_type = private_channel_type(ch)
    channel_id = ch.get("id", "")

    if channel_type == "dm":
        recipients = ch.get("recipients", []) or []
        if recipients:
            recipient = recipients[0]
            if isinstance(recipient, dict):
                return (
                    recipient.get("display_name")
                    or recipient.get("global_name")
                    or recipient.get("username")
                    or default
                    or channel_id
                    or "DM"
                )
        return default or channel_id or "DM"

    if channel_type == "group_dm":
        name = ch.get("name")
        if name:
            return name
        preview = summarize_participants(private_channel_participants(ch))
        return preview or default or "Group DM"

    return ch.get("name") or default or channel_id or ""


def private_channel_meta(ch: dict) -> dict | None:
    """Build normalized metadata for a DM/group-DM channel object."""
    channel_type = private_channel_type(ch)
    if channel_type is None:
        return None
    return {
        "channel_type": channel_type,
        "channel_name": private_channel_name(ch),
        "participants": private_channel_participants(ch),
    }


def private_channel_label_for_type(channel_type: str | None, name: str) -> str:
    """Render a stable user-facing label from a normalized private-channel type."""
    if channel_type == "group_dm":
        return f"Group DM: {name or 'Group DM'}"
    return f"DM: {name or 'DM'}"


def private_channel_listener_label(ch: dict) -> str:
    """Return a listener/status label for a DM or group DM."""
    return private_channel_label_for_type(private_channel_type(ch), private_channel_name(ch))


def private_channel_close_message(ch: dict) -> str:
    """Return the user-facing result message for closing/leaving a private channel."""
    channel_type = private_channel_type(ch)
    name = private_channel_name(ch)
    if channel_type == "group_dm":
        return f"Left group DM {name}."
    return f"Closed DM {name}."
