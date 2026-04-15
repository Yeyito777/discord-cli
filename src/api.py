"""Discord REST API client — mirrors Discord desktop client requests.

Impersonates the Discord desktop client (Electron) rather than the web client.
This avoids cookie/TLS fingerprint mismatches that come with pretending to be
a browser. Uses http.client for keepalive connection pooling.

Header fingerprinting based on endcord (https://github.com/sparklost/endcord).
"""

import base64
import http.client
import json
import os
import re
import ssl
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid

from src.auth import get_token

API_HOST = "discord.com"
API_BASE = "/api/v9"

# ─── Desktop client fingerprint ──────────────────────────────────────────────
# Mimics the Discord desktop client (Electron) on Linux.
# This is what endcord uses and it's less suspicious than pretending to be
# a browser, because there are no cookies or TLS fingerprint to mismatch.

_CLIENT_VERSION = "0.0.115"
_ELECTRON_VERSION = "37.6.0"
_CHROME_VERSION = "138.0.7204.251"
_USER_AGENT = (
    f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    f"(KHTML, like Gecko) discord/{_CLIENT_VERSION} "
    f"Chrome/{_CHROME_VERSION} Electron/{_ELECTRON_VERSION} Safari/537.36"
)

# ─── Build number ────────────────────────────────────────────────────────────

_build_number = None


def _get_build_number():
    """Fetch Discord's current client build number from their JS bundle."""
    global _build_number
    if _build_number is not None:
        return _build_number

    try:
        conn = http.client.HTTPSConnection(API_HOST, 443, timeout=10)
        conn.request("GET", "/app", headers={"User-Agent": _USER_AGENT})
        resp = conn.getresponse()
        html = resp.read().decode("utf-8", errors="replace")
        conn.close()

        match = re.search(r'src="/assets/(web\.[a-f0-9]+\.js)"', html)
        if not match:
            _build_number = 510733
            return _build_number

        conn = http.client.HTTPSConnection(API_HOST, 443, timeout=10)
        conn.request("GET", f"/assets/{match.group(1)}", headers={"User-Agent": _USER_AGENT})
        resp = conn.getresponse()
        js = resp.read().decode("utf-8", errors="replace")
        conn.close()

        match = re.search(r'buildNumber["\s:]*(\d+)', js)
        _build_number = int(match.group(1)) if match else 510733
    except Exception:
        _build_number = 510733

    return _build_number


def _get_os_version():
    """Get Linux kernel version."""
    try:
        return subprocess.check_output(["uname", "-r"], text=True).strip()
    except Exception:
        return ""


def _get_system_locale():
    """Get system locale."""
    locale = os.environ.get("LC_ALL") or os.environ.get("LANG")
    if locale:
        return locale.split(".")[0]
    return "en_US"


def _build_super_properties():
    """Build X-Super-Properties matching the Discord desktop client."""
    props = {
        "os": "Linux",
        "browser": "Discord Client",
        "release_channel": "stable",
        "os_version": _get_os_version(),
        "os_arch": "x64",
        "app_arch": "x64",
        "system_locale": _get_system_locale(),
        "has_client_mods": False,
        "browser_user_agent": _USER_AGENT,
        "browser_version": "",
        "runtime_environment": "native",
        "client_build_number": _get_build_number(),
        "native_build_number": None,
        "client_event_source": None,
        # UUIDs generated once per session, like the real client
        "client_launch_id": str(uuid.uuid4()),
        "client_heartbeat_session_id": str(uuid.uuid4()),
        "client_version": _CLIENT_VERSION,
    }
    wm = os.environ.get("XDG_CURRENT_DESKTOP", "unknown")
    session = os.environ.get("GDMSESSION", "unknown")
    props["window_manager"] = f"{wm},{session}"

    return base64.b64encode(
        json.dumps(props, separators=(",", ":")).encode()
    ).decode()


# ─── Connection pool ─────────────────────────────────────────────────────────
# Reuses HTTPS connections like a real client (keepalive).

_pool_lock = threading.Lock()
_pool = []  # list of [connection, in_use, created_time]
_MAX_POOL = 3
_MAX_AGE = 55 * 60  # Discord closes keepalive after 60 min


def _get_connection(host=API_HOST, port=443, timeout=10):
    """Get an HTTPS connection, reusing from pool if possible."""
    now = int(time.time())

    with _pool_lock:
        for entry in _pool:
            if not entry[1]:  # not in use
                conn = entry[0]
                # Recreate if too old
                if now - entry[2] > _MAX_AGE or conn.sock is None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = http.client.HTTPSConnection(host, port, timeout=timeout)
                    entry[0] = conn
                entry[1] = True
                entry[2] = now
                return entry

        if len(_pool) < _MAX_POOL:
            conn = http.client.HTTPSConnection(host, port, timeout=timeout)
            entry = [conn, True, now]
            _pool.append(entry)
            return entry

    # Pool full — create a non-pooled connection
    conn = http.client.HTTPSConnection(host, port, timeout=timeout)
    return [conn, True, now]


def _release_connection(entry):
    """Release a connection back to the pool."""
    entry[1] = False


# ─── Headers ─────────────────────────────────────────────────────────────────

_cached_token = None
_cached_super_props = None
_cached_headers = None


def _headers(token=None):
    """Build request headers matching the Discord desktop client."""
    global _cached_token, _cached_super_props, _cached_headers

    if token is None:
        if _cached_token is None:
            _cached_token = get_token()
        token = _cached_token

    if _cached_headers is not None and token == _cached_token:
        return _cached_headers

    if _cached_super_props is None:
        _cached_super_props = _build_super_properties()

    _cached_headers = {
        "Accept": "*/*",
        "Authorization": token,
        "Content-Type": "application/json",
        "Priority": "u=1",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": _USER_AGENT,
        "X-Super-Properties": _cached_super_props,
        "X-Discord-Locale": "en-US",
        "X-Discord-Timezone": "America/Chicago",
    }
    _cached_token = token
    return _cached_headers


# ─── Core request ────────────────────────────────────────────────────────────

def _build_headers(token=None, extra_headers=None):
    headers = dict(_headers(token))
    if extra_headers:
        headers.update(extra_headers)
    return headers


def _maybe_retry_with_captcha(method, path, *, body=None, body_bytes=None,
                              token=None, params=None, extra_headers=None,
                              status=None, raw=None, allow_captcha_retry=True):
    """Retry a challenged request after solving Discord's hCaptcha flow."""
    if not allow_captcha_retry or status != 400 or not raw:
        return None

    try:
        err = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(err, dict) or "captcha_key" not in err:
        return None

    if os.environ.get("DISCORD_CAPTCHA_DEBUG"):
        try:
            print(
                "DISCORD_CAPTCHA_DEBUG payload:",
                json.dumps(err, ensure_ascii=False),
                file=sys.stderr,
            )
        except Exception:
            pass

    from src.captcha import CaptchaChallenge, solve_hcaptcha

    challenge = CaptchaChallenge.from_discord_error(err)
    solution = solve_hcaptcha(challenge)

    retry_headers = dict(extra_headers or {})
    retry_headers["X-Captcha-Key"] = solution.token
    if challenge.session_id:
        retry_headers["X-Captcha-Session-Id"] = challenge.session_id
    if challenge.rqtoken:
        retry_headers["X-Captcha-Rqtoken"] = challenge.rqtoken

    return _request(
        method,
        path,
        body=body,
        body_bytes=body_bytes,
        token=token,
        params=params,
        extra_headers=retry_headers,
        allow_captcha_retry=False,
    )


def _request(method, path, body=None, body_bytes=None, token=None, params=None,
             extra_headers=None, allow_captcha_retry=True):
    """Make an API request with connection pooling. Returns parsed JSON."""
    if body is not None and body_bytes is not None:
        raise ValueError("Provide either body or body_bytes, not both")

    url = f"{API_BASE}{path}"

    if params:
        qs = urllib.parse.urlencode(params, doseq=True)
        url = f"{url}?{qs}"

    data = body_bytes
    if body is not None:
        data = json.dumps(body).encode("utf-8")

    headers = _build_headers(token, extra_headers)
    entry = _get_connection()
    conn = entry[0]

    try:
        try:
            conn.request(method, url, data, headers)
            resp = conn.getresponse()
            raw = resp.read()
        except (BrokenPipeError, ConnectionResetError, http.client.RemoteDisconnected, TimeoutError):
            # Server closed keepalive — reconnect
            try:
                conn.close()
            except Exception:
                pass
            conn = http.client.HTTPSConnection(API_HOST, 443, timeout=10)
            entry[0] = conn
            conn.request(method, url, data, headers)
            resp = conn.getresponse()
            raw = resp.read()

        status = resp.status

        if status == 204:
            return None

        if 200 <= status < 300:
            if not raw:
                return None
            return json.loads(raw)

        retried = _maybe_retry_with_captcha(
            method,
            path,
            body=body,
            body_bytes=body_bytes,
            token=token,
            params=params,
            extra_headers=extra_headers,
            status=status,
            raw=raw,
            allow_captcha_retry=allow_captcha_retry,
        )
        if retried is not None:
            return retried

        # Error handling
        try:
            err = json.loads(raw)
            msg = err.get("message", raw.decode("utf-8", errors="replace"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            msg = raw.decode("utf-8", errors="replace") if raw else f"HTTP {status}"

        raise RuntimeError(f"HTTP {status}: {msg}")

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        # Network error — remove from pool
        try:
            conn.close()
        except Exception:
            pass
        with _pool_lock:
            if entry in _pool:
                _pool.remove(entry)
        raise RuntimeError(f"Network error: {e}")

    finally:
        _release_connection(entry)


def get(path, **kwargs):
    return _request("GET", path, **kwargs)


def post(path, body=None, **kwargs):
    return _request("POST", path, body=body, **kwargs)


def put(path, body=None, **kwargs):
    return _request("PUT", path, body=body, **kwargs)


def patch(path, body=None, **kwargs):
    return _request("PATCH", path, body=body, **kwargs)


def delete(path, **kwargs):
    return _request("DELETE", path, **kwargs)


# ─── Users ────────────────────────────────────────────────────────────────────

def get_me():
    """Get current user info."""
    return get("/users/@me")


def get_user(user_id):
    """Get a user's profile."""
    return get(f"/users/{user_id}/profile", params={"with_mutual_guilds": "true"})


# ─── Guilds (Servers) ─────────────────────────────────────────────────────────

def get_guilds():
    """Get all guilds the user is in."""
    return get("/users/@me/guilds")


def get_guild(guild_id):
    """Get guild details."""
    return get(f"/guilds/{guild_id}", params={"with_counts": "true"})


def leave_guild(guild_id):
    """Leave a guild."""
    return _request("DELETE", f"/users/@me/guilds/{guild_id}", body={"lurking": False})


def get_guild_channels(guild_id):
    """Get all channels in a guild."""
    return get(f"/guilds/{guild_id}/channels")


def get_guild_members(guild_id, limit=100):
    """Get members of a guild (limited by permissions)."""
    return get(f"/guilds/{guild_id}/members", params={"limit": str(limit)})


def search_guild_members(guild_id, query, limit=20):
    """Search for members in a guild by name."""
    return get(
        f"/guilds/{guild_id}/members/search",
        params={"query": query, "limit": str(limit)},
    )


# ─── Channels ────────────────────────────────────────────────────────────────

def get_channel(channel_id):
    """Get channel details."""
    return get(f"/channels/{channel_id}")


def get_dm_channels():
    """Get all DM/group DM channels."""
    return get("/users/@me/channels")


def create_dm(recipient_id):
    """Open a DM channel with a user."""
    return post("/users/@me/channels", body={"recipients": [recipient_id]})


# ─── Messages ────────────────────────────────────────────────────────────────

def get_messages(channel_id, limit=50, before=None, after=None, around=None):
    """Get messages from a channel."""
    params = {"limit": str(min(limit, 100))}
    if before:
        params["before"] = before
    if after:
        params["after"] = after
    if around:
        params["around"] = around
    return get(f"/channels/{channel_id}/messages", params=params)


def get_message(channel_id, message_id):
    """Get a single message."""
    return get(f"/channels/{channel_id}/messages/{message_id}")


def send_message(channel_id, content, reply_to=None, tts=False):
    """Send a message to a channel."""
    body = {"content": content, "tts": tts}
    if reply_to:
        body["message_reference"] = {"message_id": reply_to}
    return post(f"/channels/{channel_id}/messages", body=body)


def send_message_with_files(channel_id, file_paths, content=None, reply_to=None, tts=False):
    """Send a message with file attachments using multipart/form-data.

    Discord expects:
      - A 'payload_json' part with the message body (application/json)
      - One or more 'files[N]' parts with the actual file data
    """
    import mimetypes
    import os

    boundary = f"----ExoDiscord{uuid.uuid4().hex}"

    # Build payload_json
    payload = {"tts": tts}
    if content:
        payload["content"] = content
    if reply_to:
        payload["message_reference"] = {"message_id": reply_to}

    # Populate attachments metadata so Discord knows about each file
    attachments = []
    for i, fp in enumerate(file_paths):
        attachments.append({
            "id": i,
            "filename": os.path.basename(fp),
        })
    payload["attachments"] = attachments

    # Build multipart body
    parts = []

    # payload_json part
    parts.append(
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="payload_json"\r\n'
        f"Content-Type: application/json\r\n"
        f"\r\n"
        f"{json.dumps(payload)}\r\n"
    )

    # File parts
    for i, fp in enumerate(file_paths):
        filename = os.path.basename(fp)
        mime_type = mimetypes.guess_type(fp)[0] or "application/octet-stream"
        with open(fp, "rb") as f:
            file_data = f.read()

        # Header portion (text)
        header = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="files[{i}]"; filename="{filename}"\r\n'
            f"Content-Type: {mime_type}\r\n"
            f"\r\n"
        )
        parts.append(header.encode("utf-8") + file_data + b"\r\n")

    # Closing boundary
    parts.append(f"--{boundary}--\r\n")

    # Combine into a single bytes body
    body_parts = []
    for part in parts:
        if isinstance(part, str):
            body_parts.append(part.encode("utf-8"))
        else:
            body_parts.append(part)
    body = b"".join(body_parts)

    return _request(
        "POST",
        f"/channels/{channel_id}/messages",
        body_bytes=body,
        extra_headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(body)),
        },
    )


def edit_message(channel_id, message_id, content):
    """Edit a message."""
    return patch(
        f"/channels/{channel_id}/messages/{message_id}",
        body={"content": content},
    )


def delete_message(channel_id, message_id):
    """Delete a message."""
    return delete(f"/channels/{channel_id}/messages/{message_id}")


# ─── Reactions ────────────────────────────────────────────────────────────────

def add_reaction(channel_id, message_id, emoji):
    """Add a reaction. emoji is URL-encoded (e.g. %F0%9F%91%8D or custom:id)."""
    encoded = urllib.parse.quote(emoji, safe=":")
    return put(
        f"/channels/{channel_id}/messages/{message_id}/reactions/{encoded}/@me",
        body={},
    )


def remove_reaction(channel_id, message_id, emoji):
    """Remove own reaction."""
    encoded = urllib.parse.quote(emoji, safe=":")
    return delete(
        f"/channels/{channel_id}/messages/{message_id}/reactions/{encoded}/@me"
    )


# ─── Pins ─────────────────────────────────────────────────────────────────────

def get_pins(channel_id):
    """Get pinned messages in a channel."""
    return get(f"/channels/{channel_id}/pins")


# ─── Search ───────────────────────────────────────────────────────────────────

def search_guild(guild_id, content=None, author_id=None, channel_id=None,
                 has=None, before=None, after=None, limit=25, offset=0):
    """Search messages in a guild."""
    params = {"limit": str(limit), "offset": str(offset)}
    if content:
        params["content"] = content
    if author_id:
        params["author_id"] = author_id
    if channel_id:
        params["channel_id"] = channel_id
    if has:
        params["has"] = has
    if before:
        params["max_id"] = before
    if after:
        params["min_id"] = after
    return get(f"/guilds/{guild_id}/messages/search", params=params)


def search_channel(channel_id, content=None, author_id=None,
                   has=None, limit=25, offset=0):
    """Search messages in a DM channel."""
    params = {"limit": str(limit), "offset": str(offset)}
    if content:
        params["content"] = content
    if author_id:
        params["author_id"] = author_id
    if has:
        params["has"] = has
    return get(f"/channels/{channel_id}/messages/search", params=params)


# ─── Typing ───────────────────────────────────────────────────────────────────

def trigger_typing(channel_id):
    """Show typing indicator (lasts ~10 seconds)."""
    return post(f"/channels/{channel_id}/typing")


# ─── Read state ───────────────────────────────────────────────────────────────

def ack_message(channel_id, message_id):
    """Mark a channel as read up to a message."""
    return post(
        f"/channels/{channel_id}/messages/{message_id}/ack",
        body={"token": None},
    )


# ─── Threads ──────────────────────────────────────────────────────────────────

def get_active_threads(guild_id):
    """Get active threads in a guild.

    The /guilds/{id}/threads/active endpoint is bot-only.
    For user accounts, we fetch threads via channels.
    """
    channels = get_guild_channels(guild_id)
    text_channels = [c for c in channels if c.get("type", 0) in (0, 5, 15, 16)]

    all_threads = []
    for ch in text_channels:
        try:
            data = get(
                f"/channels/{ch['id']}/threads/search",
                params={
                    "archived": "false",
                    "sort_by": "last_message_time",
                    "sort_order": "desc",
                    "limit": "25",
                },
            )
            all_threads.extend(data.get("threads", []))
        except RuntimeError:
            pass

    return {"threads": all_threads}


def get_thread_messages(thread_id, limit=50, before=None):
    """Get messages from a thread (threads are just channels)."""
    return get_messages(thread_id, limit=limit, before=before)
