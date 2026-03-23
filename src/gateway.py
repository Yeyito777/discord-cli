"""Discord Gateway WebSocket client for real-time event listening.

Connects to Discord's gateway, authenticates, handles heartbeating and
reconnection, and streams events for a specific channel to a file.

Gateway protocol adapted from endcord (https://github.com/sparklost/endcord).
"""

import http.client
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
import uuid
import zlib
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

        # Notification relay queue (notify mode with relay_conv)
        self._relay_queue = []
        self._relay_lock = threading.Lock()
        self._relay_active = False

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum=None, frame=None):
        sig_name = signal.Signals(signum).name if signum else "?"
        self._log(f"Received {sig_name}, shutting down")
        self.running = False
        # Don't close ws here — we may be inside recv_data() on the main
        # thread and ws.close() can deadlock.  The 1s socket timeout will
        # break us out of recv, the loop checks self.running, and exits.

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
        notif = {
            "ts": d.get("timestamp", ""),
            "type": "dm" if is_dm else "mention",
            "author_id": author.get("id", ""),
            "author": author.get("username", ""),
            "display_name": author.get("global_name") or author.get("username", ""),
            "content": (d.get("content") or "")[:300],
            "channel_id": d.get("channel_id", ""),
            "msg_id": d.get("id", ""),
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

            msg = self._format_relay(batch)
            self._log(f"Relaying {len(batch)} notification(s) to {len(self.relay_targets)} target(s)")

            for conv_id in self.relay_targets:
                try:
                    result = subprocess.run(
                        ["exo", "send", msg, "-c", conv_id, "--timeout", "600"],
                        capture_output=True, text=True, timeout=660,
                    )
                    if result.returncode != 0:
                        self._log(f"Relay to {conv_id} failed: {result.stderr[:200]}")
                    else:
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

    def _format_relay(self, batch):
        """Format notification batch into a human-readable message."""
        try:
            from src.notify import get_labels
            labels = get_labels()  # user_id → {label, username, ...}
        except Exception:
            labels = {}

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

            if n.get("type") == "dm":
                parts.append(
                    f'DM from {name} (@{username}){label_str}{id_tag}: "{content}"'
                )
            else:
                guild = n.get("guild_name", "?")
                channel = n.get("channel_name", "?")
                parts.append(
                    f'@mention from {name} (@{username}){label_str}'
                    f' in #{channel} ({guild}){id_tag}: "{content}"'
                )

        if len(parts) == 1:
            return f"[Discord Notification] {parts[0]}"
        else:
            header = f"[Discord Notification] {len(parts)} new:"
            body = "\n".join(f"  • {p}" for p in parts)
            return f"{header}\n{body}"

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
