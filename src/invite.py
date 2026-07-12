"""Join Discord servers via invite links using the REST API."""

import re

from src import api


def _extract_code(invite):
    """Extract invite code from a URL or bare code.

    Accepts:
      discord.gg/abc123
      https://discord.gg/abc123
      https://discord.com/invite/abc123
      https://ptb.discord.com/invite/abc123
      abc123
    """
    m = re.match(
        r'(?:https?://)?(?:(?:ptb|canary)\.)?discord(?:\.gg|\.com/invite)/([A-Za-z0-9\-_]+)',
        invite,
    )
    if m:
        return m.group(1)
    if re.match(r'^[A-Za-z0-9\-_]+$', invite):
        return invite
    raise RuntimeError(f'Invalid invite: "{invite}"')


def join_server(invite):
    """Join a server via invite link/code and return the API response."""
    code = _extract_code(invite)
    result = api.post(f"/invites/{code}", body={})
    if result and isinstance(result, dict) and "guild" in result:
        return result
    raise RuntimeError(f"Unexpected response: {result}")
