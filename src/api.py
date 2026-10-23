"""Discord REST API client — mirrors Discord desktop client requests.

Presents the Discord desktop client identity from ``fingerprint``.  Uses
http.client for keepalive connection pooling.
"""

import base64
import http.client
import json
import os
import re
import ssl
import subprocess
import threading
import time
import urllib.parse
import uuid

from src import fingerprint
from src.auth import get_token

API_HOST = "discord.com"
API_BASE = "/api/v9"

USER_AGENT = fingerprint.USER_AGENT


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
_cached_headers = None


def _headers(token=None):
    """Build request headers matching the Discord desktop client."""
    global _cached_token, _cached_headers

    if token is None:
        if _cached_token is None:
            _cached_token = get_token()
        token = _cached_token

    if _cached_headers is not None and token == _cached_token:
        return _cached_headers

    _cached_headers = fingerprint.rest_headers(token)
    _cached_token = token
    return _cached_headers


def reset_token_cache():
    """Forget the cached token so a long-lived process re-reads credentials."""
    global _cached_token, _cached_headers
    _cached_token = None
    _cached_headers = None


# ─── Core request ────────────────────────────────────────────────────────────

def _build_headers(token=None, extra_headers=None):
    headers = dict(_headers(token))
    if extra_headers:
        headers.update(extra_headers)
    return headers


def _maybe_retry_with_vimbrowser_captcha(
        method, path, *, body=None, body_bytes=None, token=None, params=None,
        extra_headers=None, status=None, raw=None, allow_captcha_fallback=True):
    """Open a Discord hCaptcha in vimbrowser and retry the request once."""
    if not allow_captcha_fallback or status != 400 or not raw:
        return False, None

    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return False, None
    if not isinstance(payload, dict) or "captcha_key" not in payload:
        return False, None

    from src.vimbrowser_captcha import (
        BrowserReplayResult,
        VimbrowserCaptchaError,
        captcha_reason,
        complete_invite,
        solve_captcha,
    )

    reason = captcha_reason(payload)
    try:
        retry_headers = dict(extra_headers or {})
        if payload.get("captcha_session_id"):
            retry_headers["X-Captcha-Session-Id"] = payload["captcha_session_id"]
        if payload.get("captcha_rqtoken"):
            retry_headers["X-Captcha-Rqtoken"] = payload["captcha_rqtoken"]
        request_headers = _build_headers(token, retry_headers)
        invite_match = re.fullmatch(r"/invites/([A-Za-z0-9_-]+)", path)
        if method.upper() == "POST" and invite_match and body_bytes is None:
            invite_code = invite_match.group(1)
            expected_guild_id = None
            expected_guild_name = None
            try:
                preview = get(
                    f"/invites/{invite_code}",
                    params={"with_counts": "true", "with_expiration": "true"},
                )
                preview_guild = preview.get("guild", {}) if isinstance(preview, dict) else {}
                expected_guild_id = str(preview_guild.get("id") or "") or None
                expected_guild_name = str(preview_guild.get("name") or "") or None
            except RuntimeError:
                pass

            def membership_check():
                if not expected_guild_id:
                    return None
                for guild in get_guilds():
                    if str(guild.get("id") or "") == expected_guild_id:
                        return guild
                return None

            replay = complete_invite(
                invite_code,
                request_headers["Authorization"],
                expected_guild_id=expected_guild_id,
                expected_guild_name=expected_guild_name,
                membership_check=membership_check,
            )
        else:
            replay = solve_captcha(
                payload,
                replay_request={
                    "method": method,
                    "path": path,
                    "body": body,
                    "body_bytes": body_bytes,
                    "params": params,
                    "headers": request_headers,
                },
            )
    except VimbrowserCaptchaError as e:
        raise RuntimeError(
            f"Discord requires a captcha ({reason}), but it could not be "
            f"completed in vimbrowser: {e}"
        ) from e

    if not isinstance(replay, BrowserReplayResult):
        raise RuntimeError("vimbrowser captcha fallback did not replay the Discord request")
    if not (200 <= replay.status < 300):
        raise RuntimeError(
            "Discord rejected the request replayed by vimbrowser after captcha "
            f"completion (HTTP {replay.status}): {replay.text[:500]}"
        )

    if method.upper() not in {"GET", "HEAD", "OPTIONS"}:
        from src.accounts import audit_event
        result_id = replay.body.get("id") if isinstance(replay.body, dict) else None
        audit_event(
            f"discord-api:{method.upper()}",
            target=path,
            result_id=result_id,
        )
    return True, replay.body


_READ_METHODS = {"GET", "HEAD", "OPTIONS"}
# Only requests that are safe to repeat are re-sent after a dropped keepalive.
# A POST (send, reply, thread, interaction) that may have reached Discord is
# never repeated: its outcome is reported as unknown instead.
_RETRYABLE_METHODS = _READ_METHODS | {"PUT", "PATCH", "DELETE"}
DISCORD_EPOCH_MS = 1_420_070_400_000


def new_nonce():
    """A snowflake-shaped nonce, as the client sends with every message."""
    return str((int(time.time() * 1000) - DISCORD_EPOCH_MS) << 22 | uuid.uuid4().int % (1 << 22))


RATE_LIMIT_MAX_WAIT = 10.0


class RateLimited(RuntimeError):
    """Discord answered 429; the request was not processed."""

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


def _retry_after(resp, raw):
    try:
        body = json.loads(raw)
        if isinstance(body, dict) and body.get("retry_after") is not None:
            return max(0.0, float(body["retry_after"]))
    except (ValueError, TypeError):
        pass
    for name in ("Retry-After", "X-RateLimit-Reset-After"):
        try:
            value = resp.getheader(name)
            if value is not None:
                return max(0.0, float(value))
        except (ValueError, TypeError, AttributeError):
            continue
    return None


class UnknownOutcome(RuntimeError):
    """The request was sent but no answer arrived; Discord may have applied it."""


def _request(method, path, body=None, body_bytes=None, token=None, params=None,
             extra_headers=None, allow_captcha_fallback=True,
             retry_connection_failure=None, audit=True, mutating=None, _rate_limited=False):
    """Make an API request with connection pooling. Returns parsed JSON.

    ``audit=False`` skips the mutation receipt for non-GET requests that do not
    change Discord state (e.g. attachment URL refreshes).  Every request that
    may change Discord state (``mutating``, default: any non-read method) must
    pass the account policy first; nothing is sent otherwise.
    """
    if body is not None and body_bytes is not None:
        raise ValueError("Provide either body or body_bytes, not both")
    if mutating is None:
        mutating = method.upper() not in _READ_METHODS
    if mutating:
        from src.accounts import ensure_mutation_allowed
        ensure_mutation_allowed(f"{method.upper()} {path}")
    if retry_connection_failure is None:
        retry_connection_failure = method.upper() in _RETRYABLE_METHODS

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

    sent = False
    try:
        try:
            conn.request(method, url, data, headers)
            sent = True
            resp = conn.getresponse()
            raw = resp.read()
        except (BrokenPipeError, ConnectionResetError, http.client.RemoteDisconnected, TimeoutError) as e:
            if not retry_connection_failure:
                try:
                    conn.close()
                except Exception:
                    pass
                with _pool_lock:
                    if entry in _pool:
                        _pool.remove(entry)
                raise UnknownOutcome(
                    f"Outcome unknown: the connection failed after sending {method.upper()} {path} "
                    f"({type(e).__name__}); Discord may have accepted it. Check before retrying."
                ) from e
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
            if audit and method.upper() not in {"GET", "HEAD", "OPTIONS"}:
                from src.accounts import audit_event
                audit_event(f"discord-api:{method.upper()}", target=path)
            return None

        if 200 <= status < 300:
            if not raw:
                if audit and method.upper() not in {"GET", "HEAD", "OPTIONS"}:
                    from src.accounts import audit_event
                    audit_event(f"discord-api:{method.upper()}", target=path)
                return None
            result = json.loads(raw)
            if audit and method.upper() not in {"GET", "HEAD", "OPTIONS"}:
                from src.accounts import audit_event
                result_id = result.get("id") if isinstance(result, dict) else None
                audit_event(f"discord-api:{method.upper()}", target=path, result_id=result_id)
            return result

        if status == 429:
            wait = _retry_after(resp, raw)
            if not _rate_limited and wait is not None and wait <= RATE_LIMIT_MAX_WAIT:
                # A 429 means Discord did not process the request, so one
                # identical retry after the advised wait cannot duplicate it.
                time.sleep(wait + 0.05)
                return _request(
                    method, path, body=body, body_bytes=body_bytes, token=token, params=params,
                    extra_headers=extra_headers, allow_captcha_fallback=allow_captcha_fallback,
                    retry_connection_failure=retry_connection_failure, audit=audit,
                    mutating=mutating, _rate_limited=True,
                )
            advised = f"{wait:.1f} s" if wait is not None else "a while"
            raise RateLimited(
                f"Rate limited by Discord on {method.upper()} {path}; nothing was done. "
                f"Retry after {advised}.",
                wait,
            )

        handled, result = _maybe_retry_with_vimbrowser_captcha(
            method,
            path,
            body=body,
            body_bytes=body_bytes,
            token=token,
            params=params,
            extra_headers=extra_headers,
            status=status,
            raw=raw,
            allow_captcha_fallback=allow_captcha_fallback,
        )
        if handled:
            return result

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
        if sent and method.upper() not in _READ_METHODS:
            raise UnknownOutcome(
                f"Outcome unknown: network error after sending {method.upper()} {path} ({e}); "
                "Discord may have accepted it. Check before retrying."
            ) from e
        raise RuntimeError(f"Network error: {e}")

    finally:
        _release_connection(entry)


def get(path, **kwargs):
    return _request("GET", path, **kwargs)


def post(path, body=None, **kwargs):
    return _request("POST", path, body=body, **kwargs)


def post_once(path, body=None, **kwargs):
    """POST without an ambiguous automatic retry after a connection failure."""
    return _request(
        "POST",
        path,
        body=body,
        retry_connection_failure=False,
        **kwargs,
    )


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


def create_guild_channel(guild_id, name, *, channel_type=0, topic=None, parent_id=None):
    """Create a channel in a guild; never resent after an ambiguous failure."""
    body = {"name": name, "type": channel_type}
    if topic is not None:
        body["topic"] = topic
    if parent_id is not None:
        body["parent_id"] = parent_id
    return post_once(f"/guilds/{guild_id}/channels", body=body)


def set_channel_positions(guild_id, positions):
    """Reorder guild channels: ``positions`` is a list of {"id", "position"}."""
    return patch(f"/guilds/{guild_id}/channels", body=positions)


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


def close_private_channel(channel_id):
    """Close a 1:1 DM or leave a group DM by deleting the private channel."""
    return delete(f"/channels/{channel_id}")


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


def _stop_typing(channel_id):
    """End a `discord typing` keeper first so the indicator can't outlive this message."""
    from src.typing_keeper import stop_quietly
    stop_quietly(channel_id)


def send_message(channel_id, content, reply_to=None, tts=False):
    """Send a message to a channel."""
    _stop_typing(channel_id)
    # enforce_nonce makes Discord return the existing message instead of
    # creating a second one if the same nonce arrives twice.
    body = {"content": content, "tts": tts, "nonce": new_nonce(), "enforce_nonce": True, "flags": 0}
    if reply_to:
        body["message_reference"] = {"message_id": reply_to}
    return post(f"/channels/{channel_id}/messages", body=body)


def send_message_with_files(channel_id, file_paths, content=None, reply_to=None, tts=False):
    """Send a message with file attachments using multipart/form-data.

    Discord expects:
      - A 'payload_json' part with the message body (application/json)
      - One or more 'files[N]' parts with the actual file data
    """
    _stop_typing(channel_id)
    import mimetypes
    import os

    boundary = f"----ExoDiscord{uuid.uuid4().hex}"

    # Build payload_json
    payload = {"tts": tts, "nonce": new_nonce(), "enforce_nonce": True, "flags": 0}
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


# ─── Attachments ──────────────────────────────────────────────────────────────

def refresh_attachment_urls(urls):
    """Re-sign CDN attachment URLs, as the client does for expired links.

    Returns {original: refreshed URL or None}.  Discord signs any well-formed
    attachment path, even for deleted files, so callers must still fetch the
    refreshed URL to know the file exists.  Changes nothing on Discord.
    """
    result = {}
    urls = list(dict.fromkeys(urls))
    for i in range(0, len(urls), 50):  # Discord accepts 1-50 URLs per request
        data = post(
            "/attachments/refresh-urls",
            body={"attachment_urls": urls[i:i + 50]},
            audit=False,
            allow_captcha_fallback=False,
            mutating=False,
        )
        for item in (data or {}).get("refreshed_urls", []):
            result[item.get("original")] = item.get("refreshed")
    return result


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

def create_thread(channel_id, name, *, message_id=None,
                  auto_archive_duration=1440, thread_type=11,
                  invitable=True, rate_limit_per_user=0):
    """Create a thread in a channel, optionally anchored to a message.

    Message-anchored threads are always public (or announcement threads when
    the parent is an announcement channel), so Discord does not accept
    ``type`` or ``invitable`` for that endpoint.
    """
    body = {
        "name": name,
        "auto_archive_duration": auto_archive_duration,
        "rate_limit_per_user": rate_limit_per_user,
    }
    if message_id:
        return post(
            f"/channels/{channel_id}/messages/{message_id}/threads",
            body=body,
        )

    body["type"] = thread_type
    if thread_type == 12:
        body["invitable"] = invitable
    return post(f"/channels/{channel_id}/threads", body=body)

def get_active_threads(guild_id):
    """Get active threads in a guild.

    The /guilds/{id}/threads/active endpoint is bot-only.
    For user accounts, we fetch threads via channels.
    """
    channels = get_guild_channels(guild_id)
    text_channels = [c for c in channels if c.get("type", 0) in (0, 5, 15, 16)]

    all_threads = []
    failed = []
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
        except RuntimeError as e:
            failed.append((ch, str(e)))

    return {"threads": all_threads, "failed": failed}


def get_thread_messages(thread_id, limit=50, before=None):
    """Get messages from a thread (threads are just channels)."""
    return get_messages(thread_id, limit=limit, before=before)
