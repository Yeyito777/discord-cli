"""Join Discord servers via invite links.

Uses qutebrowser to accept invites — executes a fetch() from inside an existing
Discord tab, which inherits the browser's full session context (cookies, headers,
captcha state). This is indistinguishable from the user clicking "Accept Invite"
in the web app.

Falls back to the REST API only if qb is unavailable and the API doesn't
require captcha (rare — typically only for accounts with high trust).
"""

import json
import re
import subprocess
import time


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


def _find_discord_tab(profile="yeyito"):
    """Find a Discord tab ID in qutebrowser."""
    try:
        result = subprocess.run(
            ["qb", "tabs", "-b", profile],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0:
            return None, profile

        for line in result.stdout.strip().split("\n"):
            if "discord.com/channels" in line:
                parts = line.split()
                return parts[0], profile

        return None, profile
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None, profile


def join_server(invite, profile=None):
    """Join a server via invite link/code. Returns the API response dict.

    Primary method: uses qutebrowser to execute a fetch() from inside
    the Discord web app, which inherits full browser session context.

    Args:
        invite: Invite link or code
        profile: qutebrowser profile to use (default: tries mnemo, then yeyito)
    """
    code = _extract_code(invite)

    # Find a Discord tab
    tab_id = None
    if profile:
        tab_id, profile = _find_discord_tab(profile)
    else:
        # Try exocortex first, then yeyito
        for p in ("exocortex", "yeyito"):
            tab_id, profile = _find_discord_tab(p)
            if tab_id:
                break

    if tab_id:
        return _join_via_qb(code, tab_id, profile)

    # No Discord tab found — try raw API as last resort
    try:
        from src import api
        result = api.post(f"/invites/{code}", body={})
        if result and isinstance(result, dict) and "guild" in result:
            return result
        raise RuntimeError(f"Unexpected response: {result}")
    except RuntimeError as e:
        if "captcha" in str(e).lower():
            raise RuntimeError(
                "Discord requires captcha verification to join this server. "
                "Open discord.com in qutebrowser first, then retry."
            )
        raise


def _join_via_qb(code, tab_id, profile):
    """Accept an invite by running fetch() inside a Discord tab in qutebrowser."""
    # Sanitize the invite code for JS injection safety
    if not re.match(r'^[A-Za-z0-9\-_]+$', code):
        raise RuntimeError(f"Invalid invite code: {code}")

    # Step 1: Extract token and execute fetch from the Discord app's context
    js = (
        'void(('
        'async()=>{'
        'try{'
        'let f=document.createElement("iframe");'
        'f.style.display="none";'
        'document.body.appendChild(f);'
        'let tk=f.contentWindow.localStorage.getItem("token");'
        'f.remove();'
        'let r=await fetch("/api/v9/invites/' + code + '",{'
        'method:"POST",'
        'headers:{"Content-Type":"application/json","Authorization":JSON.parse(tk)},'
        'body:"{}"'
        '});'
        'window.__dcli_join=await r.text()'
        '}catch(e){'
        'window.__dcli_join=JSON.stringify({error:e.message})'
        '}'
        '})())'
    )

    result = subprocess.run(
        ["qb", "console", "-b", profile, tab_id, js],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to execute join in browser: {result.stderr.strip()}")

    # Step 2: Wait for the async fetch to complete
    time.sleep(3)

    # Step 3: Read the result
    result = subprocess.run(
        ["qb", "console", "-b", profile, tab_id, "window.__dcli_join"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to read join result: {result.stderr.strip()}")

    raw = result.stdout.strip()
    if not raw or raw == "undefined":
        raise RuntimeError(
            "Join request timed out. Make sure Discord is loaded in the browser."
        )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise RuntimeError(f"Unexpected response: {raw[:200]}")

    if "error" in data:
        raise RuntimeError(f"Join failed: {data['error']}")

    if "captcha_key" in data:
        raise RuntimeError(
            "Discord requires captcha even in browser context. "
            "Join the server manually in the browser."
        )

    if "guild" not in data and "message" in data:
        raise RuntimeError(f"Join failed: {data['message']}")

    if "guild" not in data:
        raise RuntimeError(f"Unexpected response: {raw[:200]}")

    return data
