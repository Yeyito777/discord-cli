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
import socket
import subprocess
import sys
import threading
import time
import uuid
import zlib
from collections import defaultdict, deque
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import websocket

from src import fingerprint, listener_state
from src.accounts import listener_dir, selected_account

_USER_AGENT = fingerprint.USER_AGENT

GATEWAY_HOST = "discord.com"
ZLIB_SUFFIX = b"\x00\x00\xff\xff"
DEFAULT_CAPABILITIES = 30717
NOTIFY_LISTENER_DIR = listener_dir()
NOTIFY_LOCK_PATH = NOTIFY_LISTENER_DIR / "__notify__.lock"
NOTIFY_PID_PATH = NOTIFY_LISTENER_DIR / "__notify__.pid"
NOTIFICATION_SEEN_LIMIT = 200
# Close codes after which reconnecting with the same token cannot work:
# 4004 authentication failed (token revoked or changed), 4010 invalid shard,
# 4011 sharding required, 4012 invalid API version, 4013/4014 invalid or
# disallowed intents.  Identifying again only adds failed logins.
FATAL_CLOSE_CODES = {4004, 4010, 4011, 4012, 4013, 4014}
CREDENTIALS_RECHECK_SECONDS = 30
# Closing with 1000/1001 ends the Discord session; any other code (or no close
# frame) keeps it resumable, so a reconnect replays missed events.
RESUMABLE_CLOSE_CODE = 4000
CONNECT_TIMEOUT_SECONDS = 15  # TLS + HELLO on a slow or just-woken network
RECV_TIMEOUT_SECONDS = 1      # short so SIGTERM is handled promptly
# Re-register the notification source once per exocortexd instance; while
# exocortexd is unreachable, retry on this schedule instead of every reconnect.
REGISTER_BACKOFF_SECONDS = (5, 10, 30, 60, 120, 300)
REGISTER_CHECK_SECONDS = 5
PUBLISH_BACKOFF_SECONDS = (2, 5, 10, 30, 60)
LOG_REPEAT_SECONDS = 300
CURSOR_SAVE_SECONDS = 10
HANDLED_IDS_LIMIT = 1000
# Backfill after a fresh session (a resume replays events itself).
BACKFILL_CHANNEL_LIMIT = 50
BACKFILL_DM_CHANNELS = 10
BACKFILL_DM_LIMIT = 20
BACKFILL_MENTIONS_LIMIT = 25
BACKFILL_MAX_AGE_SECONDS = 3 * 24 * 3600
PRESENCE_STATUSES = {"online", "idle", "dnd", "invisible"}

# ─── Account status setting ─────────────────────────────────────────────────


def _protobuf_fields(data):
    """Top-level (field number, value) pairs of a protobuf message."""
    i, out = 0, []

    def varint():
        nonlocal i
        result = shift = 0
        while True:
            byte = data[i]
            i += 1
            result |= (byte & 0x7F) << shift
            shift += 7
            if byte < 0x80:
                return result

    while i < len(data):
        key = varint()
        number, wire = key >> 3, key & 7
        if wire == 0:
            value = varint()
        elif wire == 1:
            value, i = data[i:i + 8], i + 8
        elif wire == 2:
            length = varint()
            value, i = data[i:i + length], i + length
        elif wire == 5:
            value, i = data[i:i + 4], i + 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")
        out.append((number, value))
    return out


def settings_status(proto_b64):
    """PreloadedUserSettings.status.status (online/idle/dnd/invisible), or None."""
    import base64

    try:
        settings = _protobuf_fields(base64.b64decode(proto_b64 or ""))
        status_settings = next(v for n, v in settings if n == 11)          # StatusSettings
        wrapper = next(v for n, v in _protobuf_fields(status_settings) if n == 1)  # StringValue
        return next(v for n, v in _protobuf_fields(wrapper) if n == 1).decode()
    except (StopIteration, ValueError, IndexError, TypeError, UnicodeDecodeError):
        return None


# ─── Gateway listener ────────────────────────────────────────────────────────


class GatewayListener:
    """Connects to Discord's gateway and writes events for a channel to a file.

    Each instance maintains one WebSocket connection with heartbeating,
    automatic reconnection with resume, and graceful shutdown on SIGTERM.
    """

    def __init__(self, channel_id, output_file):
        self.channel_id = channel_id
        self.output_file = output_file

        from src.auth import get_token
        self.token = get_token()
        self.account = selected_account()

        self.notification_source = None
        self.notification_channel = None
        # pid/lock/state/err files of a notification Gateway (either kind).
        self._channel_notify_paths = None
        from src.channel_notifications import PREFIX, paths
        if self.channel_id.startswith(PREFIX):
            self.notification_channel = self.channel_id[len(PREFIX):]
            self._channel_notify_paths = paths(self.notification_channel)
            from src.notify import channel_notification_source
            self.notification_source = channel_notification_source(self.notification_channel, self.account)
        if self.channel_id == "__notify__":
            from src.notify import notification_source
            self.notification_source = notification_source(self.account)
            self._channel_notify_paths = listener_state.gateway_paths("__notify__")

        self.running = True
        # Set from the signal handler so waits end at once on shutdown.
        self._stopping = threading.Event()
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
        self._notified_calls = set()  # channel IDs with an active incoming-call notification
        self._call_event_baseline_until = 0.0  # suppress call-state hydration after READY
        self._notify_lock_fd = None
        self._failed_connects = 0  # reconnect attempts since the last READY/RESUMED

        # Notification IPC publisher queue (notify mode only).
        self._notification_queue = []
        self._notification_lock = threading.Lock()
        self._notification_active = False
        self._notification_seen = {}  # channel_id -> {ids, order}
        self._pending_ids = set()      # queued message IDs not yet published

        self._init_health()

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        if self.notification_source:
            self._acquire_notify_singleton()
            self._seen_max = listener_state.load_cursor(self.channel_id)

    def _init_health(self):
        """Liveness, registration, cursor and log-bound bookkeeping."""
        now = time.time()
        self._health = {
            "status": "starting", "since": now, "connected_since": None,
            "last_event": None, "last_delivery": None, "reconnects": 0, "detail": None,
        }
        self._ready = False
        self._sessions = 0
        self._next_housekeeping = 0.0
        self._registered_identity = None
        self._register_failures = 0
        self._next_register = 0.0
        self._seen_max = None
        self._cursor_saved = None
        self._next_cursor_save = 0.0
        self._handled_ids = set()
        self._handled_order = deque()
        self._last_message_ids = {}  # channel_id → last_message_id from READY
        self._channel_guilds = {}    # channel_id → guild_id from READY
        self._repeats = {}           # throttled log key → [next allowed, suppressed count]

    def _shutdown(self, signum=None, frame=None):
        sig_name = signal.Signals(signum).name if signum else "?"
        self._log(f"Received {sig_name}, shutting down")
        self.running = False
        self._stopping.set()
        # Don't close ws here — we may be inside recv_data() on the main
        # thread and ws.close() can deadlock.  Shutting the raw socket down is
        # one syscall: the pending recv then returns at once instead of after
        # its 1s timeout (which held up every Exocortex daemon restart), and
        # the loop sees self.running and exits.
        sock = getattr(self.ws, "sock", None)
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _singleton_paths(self):
        files = getattr(self, "_channel_notify_paths", None)
        return (files["lock"], files["pid"]) if files else (NOTIFY_LOCK_PATH, NOTIFY_PID_PATH)

    @staticmethod
    def _try_lock(lock_path):
        """An exclusively locked fd for ``lock_path``; raises BlockingIOError if held."""
        lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(lock_fd)
            raise
        os.ftruncate(lock_fd, 0)
        os.write(lock_fd, f"{os.getpid()}\n".encode())
        return lock_fd

    def _acquire_notify_singleton(self):
        lock_path, pid_path = self._singleton_paths()
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            lock_fd = self._try_lock(lock_path)
        except BlockingIOError:
            existing_pid = "?"
            try:
                existing_pid = pid_path.read_text().strip() or "?"
            except OSError:
                pass
            raise SystemExit(f"Notify listener already running (PID {existing_pid})")
        self._notify_lock_fd = lock_fd
        listener_state.write_json_atomic(pid_path, os.getpid())
        atexit.register(self._release_notify_singleton)

    def _verify_notify_singleton(self):
        """Repair the lock and PID files if something deleted them.

        macOS cleans /tmp of files untouched for days; a lock file unlinked
        while held excludes nobody, so a second Gateway could start beside
        this one.  Re-create both, or exit if another Gateway already holds
        the replacement lock.
        """
        if getattr(self, "_notify_lock_fd", None) is None:
            return
        lock_path, pid_path = self._singleton_paths()
        try:
            held = os.fstat(self._notify_lock_fd)
            try:
                current = os.stat(lock_path)
                same = (current.st_ino, current.st_dev) == (held.st_ino, held.st_dev)
            except FileNotFoundError:
                same = False
            if not same:
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    new_fd = self._try_lock(lock_path)
                except BlockingIOError:
                    self._log("Another Gateway holds the replacement lock; exiting")
                    self.running = False
                    self._stopping.set()
                    return
                os.close(self._notify_lock_fd)
                self._notify_lock_fd = new_fd
                self._log("Lock file was missing; re-created it")
            try:
                pid_ok = pid_path.read_text().strip() == str(os.getpid())
            except OSError:
                pid_ok = False
            if not pid_ok:
                listener_state.write_json_atomic(pid_path, os.getpid())
                self._log("PID file was missing or replaced; restored it")
        except OSError as exc:
            self._log_repeated("singleton", f"Singleton check failed: {exc}")

    def _release_notify_singleton(self):
        files = getattr(self, "_channel_notify_paths", None)
        pid_path = files["pid"] if files else NOTIFY_PID_PATH
        try:
            if pid_path.exists():
                current = pid_path.read_text().strip()
                if current == str(os.getpid()):
                    pid_path.unlink(missing_ok=True)
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

    def _get_notification_seen_ids(self, channel_id):
        bucket = self._notification_seen.get(channel_id)
        if not bucket:
            return set()
        return set(bucket["ids"])

    def _mark_notification_seen(self, channel_id, msg_ids):
        if not channel_id or not msg_ids:
            return
        bucket = self._notification_seen.get(channel_id)
        if bucket is None:
            bucket = {"ids": set(), "order": deque()}
            self._notification_seen[channel_id] = bucket

        seen_ids = bucket["ids"]
        order = bucket["order"]
        for msg_id in msg_ids:
            if not msg_id or msg_id in seen_ids:
                continue
            seen_ids.add(msg_id)
            order.append(msg_id)
            while len(order) > NOTIFICATION_SEEN_LIMIT:
                old = order.popleft()
                seen_ids.discard(old)

    # ─── Main loop ───────────────────────────────────────────────────────────

    def run(self):
        """Connect and run until stopped.

        Reconnects forever.  Consecutive attempts that never reach READY or
        RESUMED (network down, channel inaccessible) back off up to about a
        minute instead of re-identifying every few seconds.  Exocortex being
        unreachable never stops the Discord connection: registration and
        publishing retry on their own schedules while events keep queueing.
        """
        try:
            while self.running:
                self._close_code = None
                try:
                    self._channel_notify_state("connecting")
                    self._connect()
                    self._receive_loop()
                except Exception as e:
                    self._log(f"Error: {e}")
                    if hasattr(self, "_health"):
                        self._health["detail"] = str(e)[:300]
                self._ready = False
                if self.running and self._close_code in FATAL_CLOSE_CODES:
                    self._wait_for_new_credentials(self._close_code)
                    continue
                self._channel_notify_state("reconnecting" if self.running else "stopped")
                if self.running:
                    self._wait(self._reconnect_delay())
        finally:
            self._save_cursor(force=True)
            self._channel_notify_state("stopped")

    def _wait(self, seconds):
        """Sleep up to ``seconds``, ending at once on shutdown, keeping state fresh."""
        deadline = time.monotonic() + seconds
        while self.running:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            if self._stopping.wait(min(remaining, listener_state.STATE_WRITE_SECONDS)):
                return
            self._housekeeping()

    def _credentials_mtime(self):
        from src.accounts import credentials_file

        try:
            alias = (self.account or {}).get("alias")
            path = credentials_file(None if alias in (None, "legacy") else alias)
            return path.stat().st_mtime_ns
        except (OSError, RuntimeError):
            return None

    def _use_token(self, token):
        from src import api

        self.token = token
        api.reset_token_cache()  # REST calls in this process use it too
        self.session_id = None
        self._failed_connects = 0

    def _wait_for_new_credentials(self, code):
        """Stop identifying with a token Discord rejected until it is replaced.

        The token file may already have been refreshed while this process kept
        the old token in memory (a long-lived Gateway outlives logins), so the
        file is compared first.
        """
        from src.auth import get_token

        alias = (self.account or {}).get("alias", "?")
        try:
            on_disk = get_token()
        except RuntimeError:
            on_disk = None
        if on_disk and on_disk != self.token:
            self._log(f"Discord rejected this session (close code {code}); the token file "
                      "has a different token, reconnecting with it")
            self._use_token(on_disk)
            return
        self._log(
            f"Discord rejected this session for good (close code {code}); not reconnecting "
            f"until the token changes ('discord -a {alias} login')"
        )
        self._channel_notify_state("token-rejected", f"close code {code}: run 'discord -a {alias} login'")
        self.session_id = None
        rejected = self._credentials_mtime()
        while self.running and self._credentials_mtime() == rejected:
            self._stopping.wait(CREDENTIALS_RECHECK_SECONDS)
            self._housekeeping()
        if not self.running:
            return
        try:
            token = get_token()
        except RuntimeError as e:
            self._log(f"Could not read the new token: {e}")
            return
        self._use_token(token)
        self._log("Token changed; reconnecting")

    def _reconnect_delay(self):
        failures = getattr(self, "_failed_connects", 0)
        self._failed_connects = failures + 1
        return min(60, random.uniform(1, 5) * 2 ** min(failures, 4))

    # ─── Health, registration and housekeeping ───────────────────────────────

    def _channel_notify_state(self, status, detail=None):
        """Record a status transition (any notification Gateway) and write it now."""
        health = getattr(self, "_health", None)
        if health is None or not getattr(self, "_channel_notify_paths", None):
            return
        if status != health["status"]:
            health["status"] = status
            health["since"] = time.time()
        if detail is not None:
            health["detail"] = detail
        self._write_state()

    def _write_state(self):
        files = getattr(self, "_channel_notify_paths", None)
        if not files or not hasattr(self, "_health"):
            return
        try:
            listener_state.write_json_atomic(files["state"], {
                "pid": os.getpid(), **self._health, "updated": time.time(),
                "registered": self._registered_identity is not None,
                "pending": len(getattr(self, "_pending_ids", ())),
            })
        except OSError as exc:
            self._log_repeated("state", f"Could not write state: {exc}")

    def _housekeeping(self):
        """Periodic duties of the main thread; cheap enough to call every second."""
        if not hasattr(self, "_health") or not self.notification_source:
            return
        now = time.monotonic()
        self._maybe_register(now)
        self._save_cursor()
        if now < self._next_housekeeping:
            return
        self._next_housekeeping = now + listener_state.STATE_WRITE_SECONDS
        self._verify_notify_singleton()
        files = self._channel_notify_paths or {}
        if files.get("err") and listener_state.rotate_stderr(files["err"]):
            self._log("Rotated the error log")
        listener_state.rotate_if_large(Path(self.output_file))
        self._write_state()

    def _exocortex_identity(self):
        """Identity of the exocortexd socket: changes whenever exocortexd restarts."""
        path = getattr(self, "_socket_file", None)
        if path is None:
            from src.exocortex import _socket_path
            path = self._socket_file = _socket_path()  # resolving it runs git
        try:
            info = os.stat(path)
        except OSError:
            return None
        return (info.st_dev, info.st_ino, info.st_ctime_ns)

    def _maybe_register(self, now):
        """Register the source once per exocortexd instance, after Discord is ready.

        Registration marks the source online in Exocortex, so it waits for a
        live Gateway session; failures back off instead of repeating every
        reconnect.
        """
        if not self._ready or now < self._next_register:
            return
        identity = self._exocortex_identity()
        if identity is None or identity == self._registered_identity:
            self._next_register = now + REGISTER_CHECK_SECONDS
            return
        try:
            self._register_notification_source()
        except Exception as exc:
            delay = REGISTER_BACKOFF_SECONDS[min(self._register_failures, len(REGISTER_BACKOFF_SECONDS) - 1)]
            self._register_failures += 1
            self._next_register = now + delay
            self._log_repeated("register", f"Could not register with exocortexd ({exc}); retrying in {delay}s")
            return
        self._registered_identity = identity
        self._register_failures = 0
        self._repeats.pop("register", None)
        self._write_state()

    def _log_repeated(self, key, message):
        """Log the first occurrence, then at most every LOG_REPEAT_SECONDS."""
        repeats = getattr(self, "_repeats", None)
        if repeats is None:
            self._log(message)
            return
        now = time.monotonic()
        entry = repeats.get(key)
        if entry and now < entry[0]:
            entry[1] += 1
            return
        suppressed = entry[1] if entry else 0
        repeats[key] = [now + LOG_REPEAT_SECONDS, 0]
        self._log(message + (f" (repeated {suppressed} more times)" if suppressed else ""))

    # ─── Delivery cursor and backfill ────────────────────────────────────────

    @staticmethod
    def _snowflake_at(epoch_seconds):
        return str(max(0, int(epoch_seconds * 1000) - 1420070400000) << 22)

    def _note_handled(self, msg_id):
        """Advance the cursor past a message; False if it was already handled."""
        if not msg_id or not str(msg_id).isdigit():
            return True
        handled = getattr(self, "_handled_ids", None)
        if handled is None:
            return True
        if msg_id in handled:
            return False
        handled.add(msg_id)
        self._handled_order.append(msg_id)
        while len(self._handled_order) > HANDLED_IDS_LIMIT:
            handled.discard(self._handled_order.popleft())
        if self._seen_max is None or int(msg_id) > int(self._seen_max):
            self._seen_max = str(msg_id)
        return True

    def _durable_cursor(self):
        """Highest ID such that every message up to it has been delivered or skipped."""
        if self._seen_max is None:
            return None
        bound = int(self._seen_max)
        with self._notification_lock:
            pending = [int(i) for i in self._pending_ids if str(i).isdigit()]
        if pending:
            bound = min(bound, min(pending) - 1)
        return str(bound)

    def _save_cursor(self, force=False):
        if not getattr(self, "notification_source", None) or not hasattr(self, "_health"):
            return
        now = time.monotonic()
        if not force and now < self._next_cursor_save:
            return
        self._next_cursor_save = now + CURSOR_SAVE_SECONDS
        cursor = self._durable_cursor()
        if cursor is None or cursor == self._cursor_saved:
            return
        try:
            listener_state.save_cursor(self.channel_id, cursor)
            self._cursor_saved = cursor
        except OSError as exc:
            self._log_repeated("cursor", f"Could not save the delivery cursor: {exc}")

    def _backfill(self):
        """Deliver messages that arrived while no Gateway session was listening.

        Runs after a fresh READY (a RESUMED replays missed events itself).
        Bounded: the latest BACKFILL_* messages per source; anything beyond
        that, or a failed fetch, becomes one explicit notice instead of a
        silent gap.  Exocortex deduplicates by message ID, so overlap with
        events already delivered is harmless.
        """
        after = self._seen_max
        if after is None:
            # First run: nothing to catch up on; start the cursor now.
            self._seen_max = self._snowflake_at(time.time())
            return
        floor = max(int(after), int(self._snowflake_at(time.time() - BACKFILL_MAX_AGE_SECONDS)))
        messages, gaps = [], []

        def fetch(channel_id, limit, guild_id=None):
            known = self._last_message_ids.get(channel_id)
            if known and str(known).isdigit() and int(known) <= floor:
                return  # READY says nothing newer was posted
            batch = api.get_messages(channel_id, limit=limit) or []
            newer = [m for m in batch if isinstance(m, dict) and str(m.get("id", "")).isdigit()
                     and int(m["id"]) > floor]
            if len(batch) >= limit and len(newer) == len(batch):
                gaps.append(channel_id)
            for message in newer:
                message.setdefault("channel_id", channel_id)
                if guild_id:
                    message.setdefault("guild_id", guild_id)
            messages.extend(newer)

        from src import api
        try:
            if self.notification_channel:
                guild_id = self._channel_guilds.get(self.notification_channel)
                fetch(self.notification_channel, BACKFILL_CHANNEL_LIMIT, guild_id)
            else:
                channels = sorted(
                    (cid for cid, last in self._last_message_ids.items()
                     if cid in self._private_channels and str(last or "").isdigit() and int(last) > floor),
                    key=lambda cid: int(self._last_message_ids[cid]), reverse=True,
                )
                for channel_id in channels[:BACKFILL_DM_CHANNELS]:
                    fetch(channel_id, BACKFILL_DM_LIMIT)
                if len(channels) > BACKFILL_DM_CHANNELS:
                    gaps.extend(channels[BACKFILL_DM_CHANNELS:])
                mentions = api.get("/users/@me/mentions", params={
                    "limit": str(BACKFILL_MENTIONS_LIMIT), "roles": "false", "everyone": "false",
                }) or []
                for message in mentions:
                    if not isinstance(message, dict) or not str(message.get("id", "")).isdigit():
                        continue
                    if int(message["id"]) <= floor:
                        continue
                    guild_id = message.get("guild_id") or self._channel_guilds.get(message.get("channel_id"))
                    if guild_id:  # without it a server mention would look like a DM
                        messages.append({**message, "guild_id": guild_id})
        except Exception as exc:
            self._log(f"Backfill failed: {exc}")
            self._queue_notice(
                f"backfill-failed:{after}",
                f"The Discord listener reconnected after a gap but could not fetch messages sent "
                f"while it was down ({exc}). Messages since "
                f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(listener_state.snowflake_ms(after) / 1000))} "
                "may be missing; check the source directly.",
            )
            return

        delivered = 0
        for message in sorted({m["id"]: m for m in messages}.values(), key=lambda m: int(m["id"])):
            if self._handle_message(message, backfilled=True):
                delivered += 1
        if delivered:
            self._log(f"Backfilled {delivered} message(s) missed while disconnected")
        if gaps:
            self._queue_notice(
                f"backfill-gap:{after}",
                f"The Discord listener was disconnected long enough that only the latest messages "
                f"were delivered late; older missed messages exist in {len(gaps)} channel(s): "
                + ", ".join(f"[ch:{cid}]" for cid in gaps[:10]),
            )

    def _queue_notice(self, event_id, text):
        self._queue_notification({"type": "notice", "event_id": event_id, "text": text})

    def _handle_message(self, d, *, backfilled=False):
        """Route one MESSAGE_CREATE exactly once (live or backfilled)."""
        if not self._note_handled(str(d.get("id") or "")):
            return False
        self._on_notify("MESSAGE_CREATE", d, backfilled=backfilled)
        return True

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
                self._log("Resuming session")
                return
            except Exception as exc:
                self._log(f"Resume failed ({exc}), fresh connect")
                self.session_id = None

        url = self._get_gateway_url()
        self._ws_open(url)
        self.heartbeat_interval = self._recv_hello()
        self._identify()
        self._start_heartbeat()

    def _ws_open(self, url):
        """Open a new WebSocket connection, closing any existing one resumably."""
        if self.ws:
            try:
                self.ws.close(status=RESUMABLE_CLOSE_CODE, timeout=1)
            except Exception:
                pass

        self._inflator = zlib.decompressobj()
        self.ws = websocket.WebSocket()
        self.ws.settimeout(CONNECT_TIMEOUT_SECONDS)
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
                # "unknown" lets Discord apply the account's own status
                # setting (Invisible, Idle, DND), as other clients do by
                # default; "online" here made an Invisible user show online.
                "presence": {
                    "activities": [],
                    "status": "unknown",
                    "since": 0,
                    "afk": False,
                },
            },
        })

    def _build_properties(self):
        return fingerprint.super_properties()

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
                # Drop the socket without a close frame: the session stays
                # resumable and the blocked receive returns at once.
                sock = getattr(self.ws, "sock", None)
                try:
                    if sock is not None:
                        sock.shutdown(socket.SHUT_RDWR)
                except OSError:
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
        self.ws.settimeout(RECV_TIMEOUT_SECONDS)
        return msg["d"]["heartbeat_interval"]

    def _decompress(self, data):
        if len(data) >= 4 and data[-4:] == ZLIB_SUFFIX:
            return self._inflator.decompress(data)
        return data

    # ─── Event processing ────────────────────────────────────────────────────

    def _receive_loop(self):
        """Process gateway events until the connection drops."""
        while self.running:
            self._housekeeping()
            try:
                ws_op, data = self.ws.recv_data()
            except websocket.WebSocketTimeoutException:
                continue   # timeout is expected — lets signals run
            except (ConnectionError, websocket.WebSocketException, OSError) as e:
                if self.running:
                    self._log(f"Connection lost: {e or type(e).__name__}")
                return

            if ws_op == 8:   # close frame
                code = int.from_bytes(data[:2], "big") if len(data) >= 2 else None
                self._log(f"Gateway closed the connection (code {code})")
                self._close_code = code
                return

            if hasattr(self, "_health"):
                self._health["last_event"] = time.time()
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
                self._log("Gateway requested a reconnect")
                return
            elif op == 9:    # Invalid session
                resumable = event.get("d") is True
                self._log(f"Gateway invalidated the session ({'resumable' if resumable else 'not resumable'})")
                if not resumable:
                    self.session_id = None
                self._stopping.wait(random.uniform(1, 5))
                return
            elif op == 0:    # Dispatch
                self.sequence = event.get("s")
                try:
                    self._on_dispatch(event.get("t"), event.get("d", {}))
                except Exception as e:
                    self._log(f"Event error ({event.get('t')}): {e}")
                    if (getattr(self, "notification_channel", None)
                            and event.get("t") in {"READY", "RESUMED"}):
                        raise  # retry activation rather than staying silently unready

    def _on_dispatch(self, event_type, d):
        if not isinstance(d, dict):
            return  # some events (SESSIONS_REPLACE) have list payloads

        if event_type == "READY":
            self.session_id = d["session_id"]
            self.resume_url = d.get("resume_gateway_url")
            self.my_id = d["user"]["id"]
            self._log_presence(d)
            name = d["user"].get("global_name") or d["user"]["username"]
            if getattr(self, "notification_channel", None):
                self._build_name_maps(d)
                self._subscribe_notification_channel()

            if self.channel_id == "__notify__":
                self._build_name_maps(d)
                # Discord may hydrate existing DM call state immediately after a
                # fresh identify. That is level state, not a new ringing edge.
                self._call_event_baseline_until = time.time() + 15
                self._write(f"--- Listening for DMs and @mentions as {name} ---\n\n")
            else:
                self._write(f"--- Listening as {name} · channel {self.channel_id} ---\n\n")
            self._failed_connects = 0
            self._on_session_ready(fresh=True)
            return

        if event_type == "RESUMED":
            if getattr(self, "notification_channel", None):
                self._subscribe_notification_channel()
            self._failed_connects = 0
            self._on_session_ready(fresh=False)
            return

        if self.channel_id == "__notify__" or getattr(self, "notification_channel", None):
            if event_type == "MESSAGE_CREATE":
                self._handle_message(d)
            else:
                self._on_notify(event_type, d)
        else:
            self._on_channel(event_type, d)

    def _on_session_ready(self, *, fresh):
        """A live Gateway session: mark ready, register, catch up after a gap."""
        if not getattr(self, "notification_source", None):
            return
        health = getattr(self, "_health", None)
        if health is not None:
            self._ready = True
            self._sessions += 1
            if self._sessions > 1:
                health["reconnects"] += 1
            health["connected_since"] = time.time()
            health["detail"] = None
        self._channel_notify_state("ready")
        if health is None:
            return
        self._log(f"Session {'ready' if fresh else 'resumed'}")
        if fresh:
            self._backfill()
        if not self._register_failures:
            self._next_register = 0.0
        self._maybe_register(time.monotonic())

    def _log_presence(self, ready):
        """Record the account's status setting and what Discord shows for this session."""
        setting = settings_status(ready.get("user_settings_proto"))
        shown = next(
            (s.get("status") for s in ready.get("sessions") or []
             if isinstance(s, dict) and s.get("session_id") == self.session_id),
            None,
        )
        self._log(f"Presence: account setting {setting or 'unset'}, this session {shown or 'not listed'}")

    def _subscribe_notification_channel(self):
        """Enable the user Gateway guild stream on identify and resume.

        This is a session-local Gateway subscription, not a Discord permission,
        channel follow, read acknowledgement, or account notification setting.
        """
        from src import api
        channel = api.get_channel(self.notification_channel)
        guild_id = channel.get("guild_id")
        if not guild_id:
            raise RuntimeError("Notification channel is not a server channel")
        self._channels[self.notification_channel] = {"name": channel.get("name", "?")}
        if hasattr(self, "_channel_guilds"):
            self._channel_guilds[self.notification_channel] = guild_id
        # Send directly: a failed send must not be reported as ready.
        self.ws.send(json.dumps({
            "op": 14,
            "d": {"guild_id": guild_id, "typing": False, "activities": False,
                  "threads": True, "channels": {self.notification_channel: [[0, 99]]}},
        }))

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

    def _on_notify(self, event_type, d, *, backfilled=False):
        """Handle events for the notification listener — DMs and @mentions."""
        channel_scope = getattr(self, "notification_channel", None)
        if channel_scope and (
            event_type != "MESSAGE_CREATE" or d.get("channel_id") != channel_scope
            or not d.get("guild_id") or not d.get("id")
        ):
            return
        if event_type in {"CHANNEL_CREATE", "CHANNEL_UPDATE"}:
            if d.get("type") in (1, 3):
                self._remember_private_channel(d)
            return
        if event_type == "CHANNEL_DELETE":
            ch_id = d.get("id", "")
            if ch_id:
                self._private_channels.pop(ch_id, None)
                self._notified_calls.discard(ch_id)
            return
        if event_type in {"CALL_CREATE", "CALL_UPDATE"}:
            self._on_notify_call(event_type, d)
            return
        if event_type == "CALL_DELETE":
            ch_id = d.get("channel_id", "")
            if ch_id:
                self._notified_calls.discard(ch_id)
            return
        if event_type != "MESSAGE_CREATE":
            return
        if d.get("type") == 6:  # CHANNEL_PINNED_MESSAGE system notice
            return
        if d.get("author", {}).get("id") == self.my_id:
            return

        is_dm = d.get("guild_id") is None
        if is_dm and self._is_call_message(d):
            if not backfilled:  # a call that rang while disconnected is over
                self._on_notify_call_message(d)
            return

        mentions_me = any(
            m.get("id") == self.my_id for m in d.get("mentions", [])
        )

        if not is_dm and not mentions_me and not channel_scope:
            return

        author = d.get("author", {})
        channel_id = d.get("channel_id", "")
        priv = self._ensure_private_channel_meta(channel_id) if is_dm else {}
        private_type = priv.get("channel_type") or ("dm" if is_dm else None)
        notif = {
            "ts": d.get("timestamp", ""),
            "type": private_type if is_dm else ("channel_message" if channel_scope else "mention"),
            "channel_type": private_type if is_dm else "guild_text",
            "is_group_dm": bool(is_dm and private_type == "group_dm"),
            "author_id": author.get("id", ""),
            "author": author.get("username", ""),
            "display_name": author.get("global_name") or author.get("username", ""),
            "content": d.get("content") or "",
            "mentions_assistant": mentions_me,
            "channel_id": channel_id,
            "msg_id": d.get("id", ""),
        }
        if backfilled:
            notif["backfilled"] = True
        # Keep actionable attachment IDs/names, never expiring signed URLs.
        # Voice-message URLs live only in memory (``_voice_urls``) until the
        # publisher transcribes them; they are never logged or published.
        from src.voice import is_voice_attachment

        notif["attachments"] = []
        voice_urls = {}
        for attachment in d.get("attachments", []):
            if not isinstance(attachment, dict):
                continue
            entry = {key: attachment[key] for key in ("id", "filename", "size", "content_type")
                     if key in attachment}
            if is_voice_attachment(attachment, d.get("flags")):
                entry["voice"] = True
                urls = [attachment[key] for key in ("url", "proxy_url") if attachment.get(key)]
                if urls:
                    voice_urls[str(attachment.get("id", ""))] = urls
            notif["attachments"].append(entry)
        if voice_urls:
            notif["_voice_urls"] = voice_urls
        stickers = [s.get("name") or "sticker" for s in d.get("sticker_items") or [] if isinstance(s, dict)]
        if stickers:
            notif["stickers"] = stickers
        if channel_scope:
            reference = d.get("message_reference") or {}
            if reference.get("message_id"):
                notif["reply_to"] = {"msg_id": reference["message_id"]}

        if is_dm:
            if priv.get("channel_name"):
                notif["channel_name"] = priv["channel_name"]
            participants = priv.get("participants") or []
            if participants:
                notif["channel_participants"] = participants
            participant_ids = priv.get("participant_ids") or []
            if participant_ids:
                notif["channel_participant_ids"] = participant_ids

        # Extract reply reference if present
        ref = d.get("referenced_message")
        if ref and isinstance(ref, dict):
            ref_author = ref.get("author", {})
            notif["reply_to"] = {
                "msg_id": ref.get("id", ""),
                "author_id": ref_author.get("id", ""),
                "author": ref_author.get("username", ""),
                "display_name": ref_author.get("global_name") or ref_author.get("username", ""),
                "content": ref.get("content") or "",
            }

        if not is_dm:
            guild_id = d.get("guild_id", "")
            notif["guild_id"] = guild_id
            notif["guild_name"] = self._guilds.get(guild_id, "?")
            ch_info = self._channels.get(d.get("channel_id", ""), {})
            notif["channel_name"] = ch_info.get("name", "?")

        # Always log to file
        self._write(json.dumps({k: v for k, v in notif.items() if not k.startswith("_")}) + "\n")

        # Exocortex owns subscription routing and delivery.
        self._queue_notification(notif)

    def _is_call_message(self, d):
        """Return True for Discord's call system message in a DM/group DM."""
        return d.get("type") == 3 or isinstance(d.get("call"), dict)

    def _on_notify_call_message(self, d):
        """Use the call system MESSAGE_CREATE as the incoming-call notification."""
        channel_id = d.get("channel_id", "")
        if not channel_id or channel_id in self._notified_calls:
            return
        author = d.get("author") if isinstance(d.get("author"), dict) else {}
        caller = author.get("username") or author.get("global_name") or "someone"
        priv = self._ensure_private_channel_meta(channel_id)
        self._emit_call_notification(
            channel_id,
            priv,
            caller,
            ringing={str(self.my_id)} if self.my_id else set(),
            voice_state_user_ids={str(author.get("id"))} if author.get("id") else set(),
            region=(d.get("call") or {}).get("region") if isinstance(d.get("call"), dict) else None,
            timestamp=d.get("timestamp") or time.strftime("%Y-%m-%dT%H:%M:%S"),
            event_id=f"call:{d.get('id')}" if d.get("id") else None,
        )

    def _on_notify_call(self, event_type, d):
        """Notify on call-start edges, not ongoing call/ring state.

        Discord CALL_UPDATE is level-triggered and can repeat while a call is
        still ringing or while another client reconnects. Treating it as an
        incoming-call edge causes stale notification spam. The call system
        MESSAGE_CREATE handler above is the primary edge-triggered path; CALL_CREATE
        remains only as a best-effort fallback and is suppressed during READY
        hydration.
        """
        channel_id = d.get("channel_id", "")
        if not channel_id:
            return
        if event_type == "CALL_UPDATE":
            return
        if event_type != "CALL_CREATE":
            return
        if time.time() < self._call_event_baseline_until:
            self._log(f"Suppressing hydrated CALL_CREATE for {channel_id}")
            return
        ringing = {str(user_id) for user_id in (d.get("ringing") or []) if user_id}
        voice_state_user_ids = set()
        for state in d.get("voice_states") or []:
            if isinstance(state, dict) and state.get("user_id"):
                voice_state_user_ids.add(str(state.get("user_id")))

        if self.my_id and ringing and str(self.my_id) not in ringing:
            return
        if channel_id in self._notified_calls:
            return

        priv = self._ensure_private_channel_meta(channel_id)
        caller = self._call_caller_name(ringing, voice_state_user_ids, priv)
        self._emit_call_notification(
            channel_id,
            priv,
            caller,
            ringing=ringing,
            voice_state_user_ids=voice_state_user_ids,
            region=d.get("region"),
            event_id=f"call:{d.get('id')}" if d.get("id") else None,
        )

    def _emit_call_notification(self, channel_id, priv, caller, *, ringing=None, voice_state_user_ids=None, region=None, timestamp=None, event_id=None):
        participants = priv.get("participants") or []
        private_type = priv.get("channel_type") or "dm"
        channel_name = priv.get("channel_name") or channel_id
        occurred_at = timestamp or time.strftime("%Y-%m-%dT%H:%M:%S")
        notif = {
            "ts": occurred_at,
            "event_id": event_id or f"call:{channel_id}:{occurred_at}",
            "type": "call",
            "channel_type": private_type,
            "is_group_dm": private_type == "group_dm",
            "channel_id": channel_id,
            "channel_name": channel_name,
            "channel_participants": participants,
            "channel_participant_ids": priv.get("participant_ids") or [],
            "caller": caller,
            "ringing_user_ids": sorted(ringing or []),
            "voice_state_user_ids": sorted(voice_state_user_ids or []),
            "region": region,
        }
        self._notified_calls.add(channel_id)
        self._write(json.dumps(notif) + "\n")
        self._queue_notification(notif)

    def _call_caller_name(self, ringing, voice_state_user_ids, priv):
        # Prefer the non-self participant username/display name from channel metadata.
        participants = priv.get("participants") or []
        if participants:
            return ", ".join(participants[:3])
        for user_id in voice_state_user_ids:
            if user_id != str(self.my_id):
                return user_id
        for user_id in ringing:
            if user_id != str(self.my_id):
                return user_id
        return "someone"

    # ─── Exocortex notification IPC ──────────────────────────────────────────

    def _register_notification_source(self):
        from src.exocortex import register_external_notification_source

        register_external_notification_source("discord", self.notification_source)
        self._log(f"Registered notification source {self.notification_source['id']}")

    def _queue_notification(self, notif):
        """Add an event to the asynchronous, retryable IPC publisher queue."""
        with self._notification_lock:
            self._notification_queue.append(notif)
            pending = getattr(self, "_pending_ids", None)
            if pending is not None and notif.get("msg_id"):
                pending.add(str(notif["msg_id"]))
            if self._notification_active:
                return
            self._notification_active = True

        threading.Thread(target=self._notification_sender, daemon=True).start()

    def _notification_sender(self):
        """Publish queued events; event IDs make uncertain retries deduplicable.

        While exocortexd is unreachable, events stay queued (in order) and
        retries back off up to a minute with one log line per few minutes.
        """
        failures = 0
        while self.running:
            with self._notification_lock:
                if not self._notification_queue:
                    self._notification_active = False
                    return
                batch = list(self._notification_queue)
                self._notification_queue.clear()

            retry_from = None
            for index, notif in enumerate(batch):
                try:
                    self._publish_notification(notif)
                except Exception as exc:
                    self._log_repeated("publish", f"Notification publish failed: {exc}")
                    retry_from = index
                    break
                with self._notification_lock:
                    getattr(self, "_pending_ids", set()).discard(str(notif.get("msg_id") or ""))

            if retry_from is not None:
                with self._notification_lock:
                    self._notification_queue[0:0] = batch[retry_from:]
                delay = PUBLISH_BACKOFF_SECONDS[min(failures, len(PUBLISH_BACKOFF_SECONDS) - 1)]
                failures += 1
                self._stopping.wait(delay)
            elif failures:
                failures = 0
                getattr(self, "_repeats", {}).pop("publish", None)
                self._log("Notification publishing recovered")

    def _publish_notice(self, notif):
        from src.exocortex import publish_external_notification

        alias = str((getattr(self, "account", None) or {}).get("alias") or "")
        return publish_external_notification(
            "discord", self.notification_source["id"], notif["event_id"],
            f"[Discord listener notice] {notif['text']}",
            data={"schemaVersion": 1, "accountAlias": alias, "kind": "listener_notice",
                  "content": notif["text"]},
        )

    def _publish_notification(self, notif):
        from src.exocortex import publish_external_notification

        if notif.get("type") == "notice":
            result = self._publish_notice(notif)
            self._log(f"Published listener notice {notif['event_id']}")
            return result
        self._transcribe_voice_attachments(notif)
        text, seen_updates = self._format_notification_batch([notif])
        if not text:
            self._log("Skipped stale notification event")
            return None
        if notif.get("backfilled"):
            text += "\n(sent while the Discord listener was disconnected; delivered late)"

        event_id = self._notification_event_id(notif)
        data = self._notification_data(notif)
        if notif.get("backfilled"):
            data["backfilled"] = True
        result = publish_external_notification(
            "discord",
            self.notification_source["id"],
            event_id,
            text,
            occurred_at=self._notification_occurred_at_ms(notif.get("ts")),
            data=data,
        )
        failures = [
            delivery for delivery in (result.get("deliveries") or [])
            if isinstance(delivery, dict) and delivery.get("status") == "failed"
        ]
        if failures:
            raise RuntimeError(
                f"Exocortex rejected {len(failures)} notification delivery target(s)"
            )
        for channel_id, msg_ids in seen_updates.items():
            self._mark_notification_seen(channel_id, msg_ids)
        if hasattr(self, "_health"):
            self._health["last_delivery"] = time.time()
        self._log(f"Published notification event {event_id}")
        return result

    def _transcribe_voice_attachments(self, notif):
        """Attach transcripts to voice attachments once; retries reuse them."""
        voice_urls = notif.pop("_voice_urls", None) or {}
        for attachment in notif.get("attachments") or []:
            if not attachment.get("voice") or "transcript" in attachment:
                continue
            urls = voice_urls.get(str(attachment.get("id", "")))
            transcript = None
            if urls:
                from src.voice import transcribe_voice
                try:
                    transcript = transcribe_voice(urls, attachment.get("content_type"))
                except Exception as exc:
                    self._log(f"Voice transcription failed: {exc}")
            attachment["transcript"] = transcript

    @staticmethod
    def _notification_message_content(n):
        """Message text plus one line per voice note, sticker and attachment."""
        lines = [n["content"]] if n.get("content") else []
        for attachment in n.get("attachments") or []:
            if attachment.get("voice"):
                transcript = attachment.get("transcript")
                if transcript:
                    lines.append(f"🎤 {transcript}")
                    continue
                lines.append("🎤 voice message (transcription failed)")
            lines.append(f"[attachment:{attachment.get('id', '?')}] {attachment.get('filename', 'file')}")
        lines.extend(f"[sticker] {name}" for name in n.get("stickers") or [])
        return "\n".join(lines) or "[empty message]"

    @staticmethod
    def _notification_occurred_at_ms(value):
        if isinstance(value, (int, float)):
            return int(value)
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            from datetime import datetime
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return int(parsed.timestamp() * 1000)
        except (TypeError, ValueError, OverflowError):
            return None

    @staticmethod
    def _notification_event_id(notif):
        explicit = str(notif.get("event_id") or "").strip()
        if explicit:
            return explicit
        msg_id = str(notif.get("msg_id") or "").strip()
        if msg_id:
            return msg_id
        event_type = str(notif.get("type") or "notification")
        channel_id = str(notif.get("channel_id") or "unknown")
        occurred_at = str(notif.get("ts") or "unknown")
        return f"{event_type}:{channel_id}:{occurred_at}"

    def _notification_data(self, notif):
        """Return the stable, machine-readable representation of an event.

        ``text`` is intentionally optimized for people and model context.  This
        payload is the routing/filtering API and therefore must not depend on
        punctuation, truncation, or line wrapping in the rendered text.
        """
        event_type = str(notif.get("type") or "notification")
        kind = "server_mention" if event_type == "mention" else event_type
        account_alias = ""
        if isinstance(getattr(self, "account", None), dict):
            account_alias = str(self.account.get("alias") or "")

        data = {
            "schemaVersion": 1,
            "accountAlias": account_alias,
            "kind": kind,
            "channel": {
                "id": str(notif.get("channel_id") or ""),
                "type": str(notif.get("channel_type") or ""),
                "name": str(notif.get("channel_name") or ""),
                "participants": list(notif.get("channel_participants") or []),
                "participantIds": [
                    str(value) for value in (notif.get("channel_participant_ids") or [])
                ],
            },
        }

        if event_type == "call":
            data["call"] = {
                "callerName": str(notif.get("caller") or ""),
                "ringingUserIds": [
                    str(value) for value in (notif.get("ringing_user_ids") or [])
                ],
                "voiceStateUserIds": [
                    str(value) for value in (notif.get("voice_state_user_ids") or [])
                ],
                "region": notif.get("region"),
            }
            return data

        reply = notif.get("reply_to")
        reply_data = None
        if isinstance(reply, dict):
            reply_data = {
                "messageId": str(reply.get("msg_id") or ""),
                "author": {
                    "id": str(reply.get("author_id") or ""),
                    "username": str(reply.get("author") or ""),
                    "displayName": str(reply.get("display_name") or ""),
                },
                "content": str(reply.get("content") or ""),
            }

        data.update({
            "guild": {
                "id": str(notif.get("guild_id") or ""),
                "name": str(notif.get("guild_name") or ""),
            } if event_type in {"mention", "channel_message"} else None,
            "messageId": str(notif.get("msg_id") or ""),
            "author": {
                "id": str(notif.get("author_id") or ""),
                "username": str(notif.get("author") or ""),
                "displayName": str(notif.get("display_name") or ""),
            },
            "content": str(notif.get("content") or ""),
            "mentionsAssistant": bool(notif.get("mentions_assistant")),
            "replyTo": reply_data,
        })
        if event_type == "channel_message":
            data["attachments"] = notif.get("attachments", [])
        return data

    def _notification_actor(self, user_id, display_name, labels):
        user_id = str(user_id or "")
        name = str(display_name or "?")
        mention = f" <@{user_id}>" if user_id else ""
        entry = labels.get(user_id, {}) if labels else {}
        label = entry.get("label", "") if isinstance(entry, dict) else entry
        if not label and user_id and user_id == str(getattr(self, "my_id", "")):
            label = (getattr(self, "account", {}) or {}).get("owner", "")
        label_tag = f" [{label}]" if label else ""
        return f"{name}{mention}{label_tag}"

    @staticmethod
    def _notification_reply_marker(reply_to):
        if not isinstance(reply_to, dict):
            return ""
        message_id = str(reply_to.get("msg_id") or "")
        return f" ↳ [reply-to:{message_id}]" if message_id else ""

    def _format_notification_batch(self, batch):
        """Format notification events into human-readable text.

        Returns (message_text, seen_updates) where seen_updates maps channel IDs
        to contextual message IDs included in the published event.
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
            if n.get("type") == "call":
                ch_id = n.get("channel_id", "")
                channel_type = n.get("channel_type") or "dm"
                channel_name = n.get("channel_name") or n.get("channel_id", "")
                participants = n.get("channel_participants") or []
                caller = n.get("caller") or "someone"
                ch_tag = f" [ch:{ch_id}]" if ch_id else ""
                if channel_type == "group_dm":
                    preview = summarize_participants(participants)
                    summary = channel_name or preview or "Group DM"
                    parts.append(f"Group DM › {summary}{ch_tag}\n\n→ Incoming call from {caller}")
                else:
                    parts.append(f"{channel_name or 'DM'}{ch_tag}\n\n→ Incoming call from {caller}")
                continue

            name = n.get("display_name") or n.get("author", "?")
            author_id = n.get("author_id", "")
            actor = self._notification_actor(author_id, name, labels)
            content = self._notification_message_content(n)
            msg_id = n.get("msg_id", "")
            id_line = f"\n[msg:{msg_id}]" if msg_id else ""
            reply_marker = self._notification_reply_marker(n.get("reply_to"))
            event_line = f"→ {actor}{reply_marker}:\n{content}{id_line}"

            if n.get("type") in {"dm", "group_dm"}:
                ch_id = n.get("channel_id", "")
                channel_type = n.get("channel_type") or n.get("type") or "dm"
                channel_name = n.get("channel_name") or ch_id
                participants = n.get("channel_participants") or []
                ch_tag = f" [ch:{ch_id}]" if ch_id else ""

                if channel_type == "group_dm":
                    preview = summarize_participants(participants)
                    summary = channel_name or preview or "Group DM"
                    parts.append(f"Group DM › {summary}{ch_tag}\n\n{event_line}")
                else:
                    parts.append(f"{channel_name or name}{ch_tag}\n\n{event_line}")
            else:
                guild = n.get("guild_name", "?")
                channel = n.get("channel_name", "?")
                channel_id = n.get("channel_id", "")
                current_seen = local_seen.get(channel_id)
                if current_seen is None:
                    current_seen = self._get_notification_seen_ids(channel_id)
                    local_seen[channel_id] = current_seen

                if n.get("type") == "channel_message":
                    history_lines, shown_history_ids = [], set()
                else:
                    history_lines, shown_history_ids = self._fetch_channel_history(
                        channel_id, msg_id, history_cache, labels, seen_ids=current_seen
                    )
                if shown_history_ids:
                    current_seen.update(shown_history_ids)
                    seen_updates[channel_id].update(shown_history_ids)

                location = f"{guild} › #{channel}"
                if channel_id:
                    location += f" [ch:{channel_id}]"
                if history_lines:
                    history_block = "Context:\n" + "\n".join(history_lines)
                    parts.append(f"{location}\n\n{history_block}\n\n{event_line}")
                else:
                    parts.append(f"{location}\n\n{event_line}")

                if msg_id:
                    current_seen.add(msg_id)
                    seen_updates[channel_id].add(msg_id)

        if not parts:
            return "", seen_updates
        return "\n\n".join(parts), seen_updates

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

        seen_ids = seen_ids or set()
        lines = []
        shown_message_ids = []
        for m in messages:
            mid = m.get("id", "?")
            if mid == exclude_msg_id or mid in seen_ids:
                continue
            author = m.get("author", {})
            author_name = author.get("global_name") or author.get("username", "?")
            author_id = author.get("id", "")
            actor = self._notification_actor(author_id, author_name, labels or {})
            body = re.sub(r"\s+", " ", m.get("content") or "").strip()[:200]

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

            ref = m.get("referenced_message")
            if ref and isinstance(ref, dict):
                ref_id = str(ref.get("id") or "")
                reply_marker = f" ↳ [reply-to:{ref_id}]" if ref_id else ""
                lines.append(f"{actor}{reply_marker}: {content} [msg:{mid}]")
            else:
                lines.append(f"{actor}: {content} [msg:{mid}]")
            shown_message_ids.append(mid)

        return lines, shown_message_ids

    def _remember_private_channel(self, ch):
        """Cache private-channel metadata for DM / group-DM notifications."""
        from src.private_channels import private_channel_meta

        ch_id = ch.get("id", "")
        meta = private_channel_meta(ch)
        if not ch_id or meta is None:
            return
        recipients = ch.get("recipients") or []
        meta["raw_recipients"] = recipients
        meta["participants"] = self._participant_names_from_recipients(recipients) or meta.get("participants") or []
        meta["participant_ids"] = [str(r.get("id")) for r in recipients if isinstance(r, dict) and r.get("id")]
        if not meta.get("channel_name") and meta["participants"]:
            meta["channel_name"] = ", ".join(meta["participants"][:3])
        self._private_channels[ch_id] = meta

    def _participant_names_from_recipients(self, recipients):
        names = []
        for recipient in recipients or []:
            if not isinstance(recipient, dict):
                continue
            if recipient.get("id") and str(recipient.get("id")) == str(self.my_id):
                continue
            name = recipient.get("username") or recipient.get("global_name") or recipient.get("display_name")
            if name:
                names.append(name)
        return names

    def _ensure_private_channel_meta(self, channel_id):
        meta = self._private_channels.get(channel_id)
        if meta and (meta.get("participants") or meta.get("channel_name") != channel_id):
            return meta
        try:
            from src import api
            ch = api.get_channel(channel_id)
            self._remember_private_channel(ch)
        except Exception as e:
            self._log(f"Failed to fetch private channel {channel_id}: {e}")
        return self._private_channels.get(channel_id, {})

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
                if hasattr(self, "_channel_guilds"):
                    self._channel_guilds[ch["id"]] = gid
                    self._last_message_ids[ch["id"]] = ch.get("last_message_id")

        # Build metadata for private channels
        self._private_channels = {}
        for ch in ready_data.get("private_channels", []):
            self._remember_private_channel(ch)
            if hasattr(self, "_last_message_ids") and ch.get("id"):
                self._last_message_ids[ch["id"]] = ch.get("last_message_id")

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

    if len(sys.argv) != 3:
        print("Usage: gateway.py <channel_id> <output_file>", file=sys.stderr)
        sys.exit(1)

    GatewayListener(sys.argv[1], sys.argv[2]).run()
