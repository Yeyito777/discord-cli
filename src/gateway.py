"""Discord Gateway WebSocket client for real-time event listening.

Connects to Discord's gateway, authenticates, handles heartbeating and
reconnection, and streams events for a specific channel to a file.

Gateway protocol adapted from endcord (https://github.com/sparklost/endcord).
"""

import atexit
import fcntl
import http.client
import json
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
import zlib
from collections import defaultdict, deque
from pathlib import Path

import websocket

# ─── Client fingerprint (matches api.py) ─────────────────────────────────────

_CLIENT_VERSION = "0.0.115"
_ELECTRON_VERSION = "37.6.0"
_CHROME_VERSION = "138.0.7204.251"
_USER_AGENT = (
    f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    f"(KHTML, like Gecko) discord/{_CLIENT_VERSION} "
    f"Chrome/{_CHROME_VERSION} Electron/{_ELECTRON_VERSION} Safari/537.36"
)

GATEWAY_HOST = "discord.com"
ZLIB_SUFFIX = b"\x00\x00\xff\xff"
DEFAULT_CAPABILITIES = 30717
NOTIFY_LISTENER_DIR = Path("/tmp/discord-listeners")
NOTIFY_LOCK_PATH = NOTIFY_LISTENER_DIR / "__notify__.lock"
NOTIFY_PID_PATH = NOTIFY_LISTENER_DIR / "__notify__.pid"
RELAY_SEEN_LIMIT = 200

# ─── Build number ────────────────────────────────────────────────────────────

_build_number = None


def _get_build_number():
    """Fetch Discord's current client build number (cached)."""
    global _build_number
    if _build_number is not None:
        return _build_number
    try:
        from src.api import _get_build_number as _api_build
        _build_number = _api_build()
    except Exception:
        _build_number = 510733
    return _build_number


# ─── Gateway listener ────────────────────────────────────────────────────────


class GatewayListener:
    """Connects to Discord's gateway and writes events for a channel to a file.

    Each instance maintains one WebSocket connection with heartbeating,
    automatic reconnection with resume, and graceful shutdown on SIGTERM.
    """

    def __init__(self, channel_id, output_file, relay_targets=None):
        self.channel_id = channel_id
        self.output_file = output_file
        self.relay_targets = relay_targets or []   # exo conversation IDs for instant relay

        from src.auth import get_token
        self.token = get_token()

        self.running = True
        self.ws = None
        self.sequence = None
        self.session_id = None
        self.resume_url = None
        self.heartbeat_interval = 41250
        self.heartbeat_acked = True
        self.my_id = None
        self._hb_gen = 0       # incremented each connect to retire old heartbeat threads
        self._inflator = zlib.decompressobj()
        self._guilds = {}      # guild_id → name (populated from READY, used in notify mode)
        self._channels = {}    # channel_id → {name, guild_name} (same)
        self._private_channels = {}  # channel_id → private-channel metadata for DMs / group DMs
        self._notify_lock_fd = None

        # Notification relay queue (notify mode with relay_conv)
        self._relay_queue = []
        self._relay_lock = threading.Lock()
        self._relay_active = False
        self._relay_seen = defaultdict(dict)  # relay_target -> channel_id -> {ids, order}

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        if self.channel_id == "__notify__":
            self._acquire_notify_singleton()

    def _shutdown(self, signum=None, frame=None):
        sig_name = signal.Signals(signum).name if signum else "?"
        self._log(f"Received {sig_name}, shutting down")
        self.running = False
        # Don't close ws here — we may be inside recv_data() on the main
        # thread and ws.close() can deadlock.  The 1s socket timeout will
        # break us out of recv, the loop checks self.running, and exits.

    def _acquire_notify_singleton(self):
        NOTIFY_LISTENER_DIR.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(NOTIFY_LOCK_PATH, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(lock_fd)
            existing_pid = "?"
            try:
                existing_pid = NOTIFY_PID_PATH.read_text().strip() or "?"
            except OSError:
                pass
            raise SystemExit(f"Notify listener already running (PID {existing_pid})")

        os.ftruncate(lock_fd, 0)
        os.write(lock_fd, f"{os.getpid()}\n".encode())
        self._notify_lock_fd = lock_fd
        NOTIFY_PID_PATH.write_text(f"{os.getpid()}\n")
        atexit.register(self._release_notify_singleton)

    def _release_notify_singleton(self):
        try:
            if NOTIFY_PID_PATH.exists():
                current = NOTIFY_PID_PATH.read_text().strip()
                if current == str(os.getpid()):
                    NOTIFY_PID_PATH.unlink(missing_ok=True)
        except OSError:
            pass

        if self._notify_lock_fd is not None:
            try:
                fcntl.flock(self._notify_lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(self._notify_lock_fd)
            except OSError:
                pass
            self._notify_lock_fd = None

    def _get_relay_seen_ids(self, relay_target, channel_id):
        bucket = self._relay_seen.get(relay_target, {}).get(channel_id)
        if not bucket:
            return set()
        return set(bucket["ids"])

    def _mark_relay_seen(self, relay_target, channel_id, msg_ids):
        if not relay_target or not channel_id or not msg_ids:
            return
        buckets = self._relay_seen[relay_target]
        bucket = buckets.get(channel_id)
        if bucket is None:
            bucket = {"ids": set(), "order": deque()}
            buckets[channel_id] = bucket

        seen_ids = bucket["ids"]
        order = bucket["order"]
        for msg_id in msg_ids:
            if not msg_id or msg_id in seen_ids:
                continue
            seen_ids.add(msg_id)
            order.append(msg_id)
            while len(order) > RELAY_SEEN_LIMIT:
                old = order.popleft()
                seen_ids.discard(old)

    # ─── Main loop ───────────────────────────────────────────────────────────

    def run(self):
        """Connect and run until stopped. Auto-reconnects on failure."""
        while self.running:
            try:
                self._connect()
                self._receive_loop()
            except Exception as e:
                self._log(f"Error: {e}")
            if self.running:
                time.sleep(random.uniform(1, 5))

    # ─── Connection ──────────────────────────────────────────────────────────

    def _connect(self):
        """Connect to gateway — tries resume first, falls back to fresh."""
        self._hb_gen += 1

        if self.session_id and self.resume_url:
            try:
                self._ws_open(self.resume_url)
                self.heartbeat_interval = self._recv_hello()
                self._ws_send({
                    "op": 6,
                    "d": {
                        "token": self.token,
                        "session_id": self.session_id,
                        "seq": self.sequence,
                    },
                })
                self._start_heartbeat()
                self._log("Resumed session")
                return
            except Exception:
                self._log("Resume failed, fresh connect")
                self.session_id = None

        url = self._get_gateway_url()
        self._ws_open(url)
        self.heartbeat_interval = self._recv_hello()
        self._identify()
        self._start_heartbeat()

    def _ws_open(self, url):
        """Open a new WebSocket connection, closing any existing one."""
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

        self._inflator = zlib.decompressobj()
        self.ws = websocket.WebSocket()
        self.ws.settimeout(1)   # short timeout so SIGTERM is handled promptly
        self.ws.connect(
            f"{url}/?v=9&encoding=json&compress=zlib-stream",
            header=[
                "Connection: keep-alive, Upgrade",
                "Sec-WebSocket-Extensions: permessage-deflate",
                f"User-Agent: {_USER_AGENT}",
            ],
        )

    def _get_gateway_url(self):
        conn = http.client.HTTPSConnection(GATEWAY_HOST, 443, timeout=10)
        conn.request("GET", "/api/v9/gateway", headers={"User-Agent": _USER_AGENT})
        resp = conn.getresponse()
        data = json.loads(resp.read())
        conn.close()
        return data["url"]

    def _identify(self):
        self._ws_send({
            "op": 2,
            "d": {
                "token": self.token,
                "capabilities": DEFAULT_CAPABILITIES,
                "properties": self._build_properties(),
                "presence": {
                    "activities": [],
                    "status": "online",
                    "since": None,
                    "afk": False,
                },
            },
        })

    def _build_properties(self):
        try:
            os_ver = subprocess.check_output(["uname", "-r"], text=True).strip()
        except Exception:
            os_ver = ""
        locale = os.environ.get("LC_ALL") or os.environ.get("LANG")
        locale = locale.split(".")[0] if locale else "en_US"

        return {
            "os": "Linux",
            "browser": "Discord Client",
            "release_channel": "stable",
            "os_version": os_ver,
            "os_arch": "x64",
            "app_arch": "x64",
            "system_locale": locale,
            "has_client_mods": False,
            "browser_user_agent": _USER_AGENT,
            "browser_version": "",
            "runtime_environment": "native",
            "client_build_number": _get_build_number(),
            "native_build_number": None,
            "client_event_source": None,
            "client_launch_id": str(uuid.uuid4()),
            "client_heartbeat_session_id": str(uuid.uuid4()),
            "client_version": _CLIENT_VERSION,
            "window_manager": (
                f"{os.environ.get('XDG_CURRENT_DESKTOP', 'unknown')},"
                f"{os.environ.get('GDMSESSION', 'unknown')}"
            ),
        }

    # ─── Heartbeat ───────────────────────────────────────────────────────────

    def _start_heartbeat(self):
        self.heartbeat_acked = True
        gen = self._hb_gen
        threading.Thread(target=self._heartbeat_loop, daemon=True, args=(gen,)).start()

    def _heartbeat_loop(self, gen):
        # Initial jitter
        time.sleep(self.heartbeat_interval * random.random() / 1000)

        while self.running and self._hb_gen == gen:
            if not self.heartbeat_acked:
                self._log("Heartbeat ACK missed")
                try:
                    self.ws.close()
                except Exception:
                    pass
                return
            self.heartbeat_acked = False
            self._ws_send({"op": 1, "d": self.sequence})

            # Sleep in small increments so we can exit promptly
            wait = self.heartbeat_interval * (0.8 + 0.4 * random.random()) / 1000
            deadline = time.time() + wait
            while time.time() < deadline and self.running and self._hb_gen == gen:
                time.sleep(0.5)

    # ─── WebSocket I/O ───────────────────────────────────────────────────────

    def _ws_send(self, payload):
        try:
            self.ws.send(json.dumps(payload))
        except Exception:
            pass

    def _recv_hello(self):
        """Receive the HELLO (op 10) payload and return heartbeat_interval."""
        data = self.ws.recv()
        if isinstance(data, bytes):
            data = self._decompress(data)
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        msg = json.loads(data)
        return msg["d"]["heartbeat_interval"]

    def _decompress(self, data):
        if len(data) >= 4 and data[-4:] == ZLIB_SUFFIX:
            return self._inflator.decompress(data)
        return data

    # ─── Event processing ────────────────────────────────────────────────────

    def _receive_loop(self):
        """Process gateway events until the connection drops."""
        while self.running:
            try:
                ws_op, data = self.ws.recv_data()
            except websocket.WebSocketTimeoutException:
                continue   # timeout is expected — lets signals run
            except (ConnectionError, websocket.WebSocketException, OSError):
                return

            if ws_op == 8:   # close frame
                return

            data = self._decompress(data)
            if not data:
                continue
            try:
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                event = json.loads(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            op = event.get("op")

            if op == 11:     # Heartbeat ACK
                self.heartbeat_acked = True
            elif op == 10:   # Hello (mid-session)
                self.heartbeat_interval = event["d"]["heartbeat_interval"]
            elif op == 7:    # Reconnect requested
                return
            elif op == 9:    # Invalid session
                self.session_id = None
                time.sleep(random.uniform(1, 5))
                return
            elif op == 0:    # Dispatch
                self.sequence = event.get("s")
                try:
                    self._on_dispatch(event.get("t"), event.get("d", {}))
                except Exception as e:
                    self._log(f"Event error ({event.get('t')}): {e}")

    def _on_dispatch(self, event_type, d):
        if not isinstance(d, dict):
            return  # some events (SESSIONS_REPLACE) have list payloads

        if event_type == "READY":
            self.session_id = d["session_id"]
            self.resume_url = d.get("resume_gateway_url")
            self.my_id = d["user"]["id"]
            name = d["user"].get("global_name") or d["user"]["username"]

            if self.channel_id == "__notify__":
                self._build_name_maps(d)
                self._write(f"--- Listening for DMs and @mentions as {name} ---\n\n")
            else:
                self._write(f"--- Listening as {name} · channel {self.channel_id} ---\n\n")
            return

        if event_type == "RESUMED":
            return

        if self.channel_id == "__notify__":
            self._on_notify(event_type, d)
        else:
            self._on_channel(event_type, d)

    def _on_channel(self, event_type, d):
        """Handle events for a specific channel listener."""
        if d.get("channel_id") != self.channel_id:
            return

        if event_type == "MESSAGE_CREATE":
            if d.get("author", {}).get("id") == self.my_id:
                return
            self._write_message(d)
        elif event_type == "MESSAGE_UPDATE":
            if "author" in d and "content" in d:
                self._write_message(d)
        elif event_type == "MESSAGE_DELETE":
            self._write(f"  [message {d.get('id', '?')} deleted]\n\n")

    def _on_notify(self, event_type, d):
        """Handle events for the notification listener — DMs and @mentions."""
        if event_type in {"CHANNEL_CREATE", "CHANNEL_UPDATE"}:
            if d.get("type") in (1, 3):
                self._remember_private_channel(d)
            return
        if event_type == "CHANNEL_DELETE":
            ch_id = d.get("id", "")
            if ch_id:
                self._private_channels.pop(ch_id, None)
            return
        if event_type != "MESSAGE_CREATE":
            return
        if d.get("author", {}).get("id") == self.my_id:
            return

        is_dm = d.get("guild_id") is None
        mentions_me = any(
            m.get("id") == self.my_id for m in d.get("mentions", [])
        )

        if not is_dm and not mentions_me:
            return

        author = d.get("author", {})
        channel_id = d.get("channel_id", "")
        priv = self._private_channels.get(channel_id, {}) if is_dm else {}
        private_type = priv.get("channel_type") or ("dm" if is_dm else None)
        notif = {
            "ts": d.get("timestamp", ""),
            "type": private_type if is_dm else "mention",
            "channel_type": private_type if is_dm else "guild_text",
            "is_group_dm": bool(is_dm and private_type == "group_dm"),
            "author_id": author.get("id", ""),
            "author": author.get("username", ""),
            "display_name": author.get("global_name") or author.get("username", ""),
            "content": (d.get("content") or "")[:300],
            "channel_id": channel_id,
            "msg_id": d.get("id", ""),
        }

        if is_dm:
            if priv.get("channel_name"):
                notif["channel_name"] = priv["channel_name"]
            participants = priv.get("participants") or []
            if participants:
                notif["channel_participants"] = participants

        # Extract reply reference if present
        ref = d.get("referenced_message")
        if ref and isinstance(ref, dict):
            ref_author = ref.get("author", {})
            notif["reply_to"] = {
                "msg_id": ref.get("id", ""),
                "author": ref_author.get("username", ""),
                "display_name": ref_author.get("global_name") or ref_author.get("username", ""),
                "content": (ref.get("content") or "")[:100],
            }

        if not is_dm:
            guild_id = d.get("guild_id", "")
            notif["guild_id"] = guild_id
            notif["guild_name"] = self._guilds.get(guild_id, "?")
            ch_info = self._channels.get(d.get("channel_id", ""), {})
            notif["channel_name"] = ch_info.get("name", "?")

        # Always log to file
        self._write(json.dumps(notif) + "\n")

        # If relay is configured, queue for instant delivery
        if self.relay_targets:
            self._queue_relay(notif)

    # ─── Instant relay ───────────────────────────────────────────────────────

    def _queue_relay(self, notif):
        """Add a notification to the relay queue and trigger the sender."""
        with self._relay_lock:
            self._relay_queue.append(notif)
            if self._relay_active:
                return  # sender thread will pick it up
            self._relay_active = True

        threading.Thread(target=self._relay_sender, daemon=True).start()

    def _relay_sender(self):
        """Process the relay queue — sends batched notifications via exo.

        Uses `exo send` which auto-queues if the conversation is busy.
        """
        while self.running:
            with self._relay_lock:
                if not self._relay_queue:
                    self._relay_active = False
                    return
                batch = list(self._relay_queue)
                self._relay_queue.clear()

            self._log(f"Relaying {len(batch)} notification(s) to {len(self.relay_targets)} target(s)")

            for conv_id in self.relay_targets:
                msg, seen_updates = self._format_relay(batch, relay_target=conv_id)
                try:
                    result = subprocess.run(
                        ["exo", "send", msg, "-c", conv_id, "--timeout", "600", "--no-notify"],
                        capture_output=True, text=True, timeout=660,
                    )
                    if result.returncode != 0:
                        self._log(f"Relay to {conv_id} failed: {result.stderr[:200]}")
                    else:
                        for channel_id, msg_ids in seen_updates.items():
                            self._mark_relay_seen(conv_id, channel_id, msg_ids)
                        out = result.stdout.strip()
                        if "queued" in out.lower():
                            self._log(f"Relay to {conv_id}: auto-queued (conversation busy)")
                        else:
                            self._log(f"Relay to {conv_id}: delivered")
                except subprocess.TimeoutExpired:
                    self._log(f"Relay to {conv_id} timed out")
                except Exception as e:
                    self._log(f"Relay to {conv_id} error: {e}")

            # Brief pause before checking queue again (lets batching happen)
            time.sleep(2)

    def _format_relay(self, batch, *, relay_target=None):
        """Format notification batch into a human-readable message.

        Returns (message_text, seen_updates) where seen_updates maps channel_id
        to message IDs that were actually shown to this relay target.
        """
        from src.private_channels import summarize_participants

        try:
            from src.notify import get_labels
            labels = get_labels()  # user_id → {label, username, ...}
        except Exception:
            labels = {}

        # Cache channel history fetches within this batch
        history_cache = {}  # channel_id → raw message list (chronological)
        seen_updates = defaultdict(set)
        local_seen = {}

        parts = []
        for n in batch:
            name = n.get("display_name") or n.get("author", "?")
            username = n.get("author", "?")
            author_id = n.get("author_id", "")
            entry = labels.get(author_id, {})
            label = entry.get("label", "") if isinstance(entry, dict) else entry
            label_str = f" [{label}]" if label else ""
            content = n.get("content", "")[:200]

            msg_id = n.get("msg_id", "")
            id_tag = f" [msg:{msg_id}]" if msg_id else ""

            # Format reply context if present
            reply_ctx = ""
            reply_to = n.get("reply_to")
            if reply_to:
                ref_name = reply_to.get("display_name") or reply_to.get("author", "?")
                ref_preview = reply_to.get("content", "")[:80]
                reply_ctx = f' (replying to {ref_name}: "{ref_preview}")'

            if n.get("type") in {"dm", "group_dm"}:
                ch_id = n.get("channel_id", "")
                channel_type = n.get("channel_type") or n.get("type") or "dm"
                channel_name = n.get("channel_name") or ch_id
                participants = n.get("channel_participants") or []

                if channel_type == "group_dm":
                    preview = summarize_participants(participants)
                    summary = channel_name or preview or "Group DM"
                    participant_suffix = f" [group: {preview}]" if preview and preview != summary else ""
                    ch_tag = f" [ch:{ch_id}]" if ch_id else ""
                    parts.append(
                        f'Group DM [{summary}] from {name} (@{username}){label_str}{participant_suffix}{ch_tag}{id_tag}{reply_ctx}: "{content}"'
                    )
                else:
                    ch_tag = f" [ch:{ch_id}]" if ch_id else ""
                    conv_tag = f" [dm:{channel_name}]" if channel_name else ""
                    parts.append(
                        f'DM from {name} (@{username}){label_str}{conv_tag}{ch_tag}{id_tag}{reply_ctx}: "{content}"'
                    )
            else:
                guild = n.get("guild_name", "?")
                channel = n.get("channel_name", "?")
                channel_id = n.get("channel_id", "")
                current_seen = local_seen.get(channel_id)
                if current_seen is None:
                    current_seen = self._get_relay_seen_ids(relay_target, channel_id)
                    local_seen[channel_id] = current_seen

                # Fetch recent channel history for server mentions
                history_lines, shown_history_ids = self._fetch_channel_history(
                    channel_id, msg_id, history_cache, labels, seen_ids=current_seen
                )
                if shown_history_ids:
                    current_seen.update(shown_history_ids)
                    seen_updates[channel_id].update(shown_history_ids)

                # Notification line — ⟶ prefix distinguishes from history
                if history_lines:
                    # Server/channel in header, don't repeat in notification line
                    mention_line = (
                        f'\u27F6 @mention from {name} (@{username}){label_str}'
                        f'{id_tag}{reply_ctx}: "{content}"'
                    )
                    header = f"Server: {guild} | Channel: #{channel}"
                    history_block = "Recent history:\n" + "\n".join(history_lines)
                    parts.append(f'{header}\n{history_block}\n{mention_line}')
                else:
                    # Fallback: no history available, include server/channel in notification line
                    parts.append(
                        f'\u27F6 @mention from {name} (@{username}){label_str}'
                        f' in #{channel} ({guild}){id_tag}{reply_ctx}: "{content}"'
                    )

                if msg_id:
                    current_seen.add(msg_id)
                    seen_updates[channel_id].add(msg_id)

        if len(parts) == 1:
            # Server mentions with history are multiline; DMs stay on one line
            if "\n" in parts[0]:
                return f"[Discord Notification]\n{parts[0]}", seen_updates
            else:
                return f"[Discord Notification] {parts[0]}", seen_updates
        else:
            header = f"[Discord Notification] {len(parts)} new:"
            body = "\n".join(f"  \u2022 {p}" for p in parts)
            return f"{header}\n{body}", seen_updates

    def _fetch_channel_history(self, channel_id, exclude_msg_id, cache, labels=None, seen_ids=None):
        """Fetch recent messages from a channel for context.

        Returns (history_lines, shown_message_ids), with history lines in
        chronological order. Each line includes [msg:id] and reply context if
        applicable. Resolves <@id> mentions to readable names and shows
        attachments/embeds.
        """
        if not channel_id:
            return [], []

        if channel_id not in cache:
            try:
                from src.api import get_messages
                msgs = get_messages(channel_id, limit=7)
                cache[channel_id] = list(reversed(msgs)) if msgs else []
            except Exception as e:
                self._log(f"Failed to fetch history for {channel_id}: {e}")
                cache[channel_id] = []

        messages = cache[channel_id]
        if not messages:
            return [], []

        # Build user_id → display_name map for mention resolution
        user_names = {}

        # 1. From labels config
        if labels:
            for uid, entry in labels.items():
                if isinstance(entry, dict):
                    name = entry.get("name") or entry.get("username")
                else:
                    name = str(entry)
                if name:
                    user_names[uid] = name

        # 2. From message authors and mentions in this history batch
        for m in messages:
            author = m.get("author", {})
            aid = author.get("id")
            if aid and aid not in user_names:
                user_names[aid] = author.get("global_name") or author.get("username", "?")
            for mention in m.get("mentions", []):
                mid = mention.get("id")
                if mid and mid not in user_names:
                    user_names[mid] = mention.get("global_name") or mention.get("username", "?")

        def _resolve_mention(match):
            uid = match.group(1)
            name = user_names.get(uid)
            return f"@{name}" if name else match.group(0)

        seen_ids = seen_ids or set()
        lines = []
        shown_message_ids = []
        for m in messages:
            mid = m.get("id", "?")
            if mid == exclude_msg_id or mid in seen_ids:
                continue
            author = m.get("author", {})
            author_name = author.get("global_name") or author.get("username", "?")
            body = (m.get("content") or "")[:150]

            # Resolve user mentions (<@id> and <@!id>) in body
            if body:
                body = re.sub(r'<@!?(\d+)>', _resolve_mention, body)

            # Build attachment / embed / sticker indicators
            extras = []

            attachments = m.get("attachments") or []
            if attachments:
                filenames = [a.get("filename", "file") for a in attachments]
                extras.append(f"[📎 {', '.join(filenames)}]")

            for embed in (m.get("embeds") or []):
                title = embed.get("title")
                if title:
                    extras.append(f"[🔗 {title}]")
                elif embed.get("url"):
                    extras.append(f"[🔗 {embed['url']}]")

            for sticker in (m.get("sticker_items") or []):
                extras.append(f"[sticker: {sticker.get('name', '?')}]")

            extra_str = " ".join(extras)

            # Combine body text and extras
            if body and extra_str:
                content = f"{body} {extra_str}"
            elif body:
                content = body
            elif extra_str:
                content = extra_str
            else:
                continue  # nothing to show

            # Check if this message is a reply
            ref = m.get("referenced_message")
            if ref and isinstance(ref, dict):
                ref_author = ref.get("author", {})
                ref_name = ref_author.get("global_name") or ref_author.get("username", "?")
                lines.append(f"  [msg:{mid}] (reply to {ref_name}) {author_name}: {content}")
            else:
                lines.append(f"  [msg:{mid}] {author_name}: {content}")
            shown_message_ids.append(mid)

        return lines, shown_message_ids

    def _remember_private_channel(self, ch):
        """Cache private-channel metadata for DM / group-DM notifications."""
        from src.private_channels import private_channel_meta

        ch_id = ch.get("id", "")
        meta = private_channel_meta(ch)
        if not ch_id or meta is None:
            return
        self._private_channels[ch_id] = meta

    def _build_name_maps(self, ready_data):
        """Build guild/channel name mappings from the READY event."""
        self._guilds = {}
        self._channels = {}
        for guild in ready_data.get("guilds", []):
            props = guild.get("properties", guild)
            gid = guild.get("id") or props.get("id", "")
            gname = props.get("name", "?")
            self._guilds[gid] = gname
            for ch in guild.get("channels", []):
                self._channels[ch["id"]] = {
                    "name": ch.get("name", "?"),
                    "guild_name": gname,
                }

        # Build metadata for private channels
        self._private_channels = {}
        for ch in ready_data.get("private_channels", []):
            self._remember_private_channel(ch)

    # ─── Output ──────────────────────────────────────────────────────────────

    def _write_message(self, msg_data):
        """Format a message using the existing formatters and append to file."""
        try:
            from src.parse import parse_message
            from src.format import format_message
            parsed = parse_message(msg_data)
            text = format_message(parsed)
            self._write(text + "\n\n")
        except Exception:
            # Fallback: write raw
            author = msg_data.get("author", {})
            name = author.get("global_name") or author.get("username", "?")
            content = msg_data.get("content", "")
            msg_id = msg_data.get("id", "?")
            self._write(f"{name}  [{msg_id}]\n  {content}\n\n")

    def _write(self, text):
        try:
            with open(self.output_file, "a") as f:
                f.write(text)
        except Exception:
            pass

    def _log(self, msg):
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts} gateway:{self.channel_id[:8]}] {msg}", file=sys.stderr, flush=True)


# ─── Subprocess entry point ─────────────────────────────────────────────────

if __name__ == "__main__":
    # Ensure project root is importable
    project_root = str(Path(__file__).resolve().parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    if len(sys.argv) < 3:
        print("Usage: gateway.py <channel_id> <output_file> [relay_target ...]", file=sys.stderr)
        sys.exit(1)

    targets = sys.argv[3:] if len(sys.argv) > 3 else []

    # If no relay targets on CLI, try loading from config/notify.json
    if not targets:
        config_path = Path(project_root) / "config" / "notify.json"
        try:
            with open(config_path) as f:
                targets = json.load(f).get("relay_targets", [])
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            pass

    GatewayListener(sys.argv[1], sys.argv[2], relay_targets=targets).run()
