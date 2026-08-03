"""Detached Discord call worker and voice gateway lifecycle."""

from __future__ import annotations

import json
import os
from pathlib import Path
import random
import re
import signal
import struct
import subprocess
import threading
import time
import uuid
import zlib

import websocket

from src import api
from src.accounts import selected_account
from src.auth import get_token
from src.calls.receive import VoiceReceiveMedia
from src.calls.send import send_outgoing_opus_payload
from src.calls.adapter import DiscordCallAdapter
from src.calls.exocortex import ExocortexCallClient
from src.calls.state import CALL_META_ENV, update_call_meta_env as _update_call_meta_env, write_call_meta as _write_call_meta
from src.calls.transport import OPUS_PAYLOAD_TYPE, select_encryption_mode, udp_discovery

GATEWAY_HOST = "discord.com"
ZLIB_SUFFIX = b"\x00\x00\xff\xff"
DEFAULT_CAPABILITIES = 30717
VOICE_FLAGS = 3
VOICE_GATEWAY_VERSION = 8
VOICE_CONNECT_TIMEOUT = 20
VOICE_GATEWAY_RECONNECT_DELAY = 1.0
VOICE_GATEWAY_RECONNECT_MAX_DELAY = 30.0
VOICE_GATEWAY_APP_RECONNECT_EVERY = 3
VOICE_GATEWAY_RECOVERABLE_CLOSE_CODES = {1006, 4006, 4009, 4015}
VOICE_GATEWAY_TERMINAL_CLOSE_CODES = {4014, 4022}
DAVE_PROTOCOL_VERSION = 1

_CLIENT_VERSION = "0.0.115"
_ELECTRON_VERSION = "37.6.0"
_CHROME_VERSION = "138.0.7204.251"
_USER_AGENT = (
    f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    f"(KHTML, like Gecko) discord/{_CLIENT_VERSION} "
    f"Chrome/{_CHROME_VERSION} Electron/{_ELECTRON_VERSION} Safari/537.36"
)

_build_number = None


def _get_build_number():
    global _build_number
    if _build_number is not None:
        return _build_number
    try:
        _build_number = api._get_build_number()
    except Exception:
        _build_number = 510733
    return _build_number


def _build_properties():
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


def _gateway_url():
    data = api.get("/gateway")
    return data["url"]


def _record_style_display_name(user):
    """Return the same user-facing display name scheme Record uses.

    Record intentionally ignores guild/server nicknames and formats Discord
    users as global display name when present, falling back to username.  Keep
    call participant identity aligned with that behavior so a guild voice
    speaker is identified by their Discord display name rather than
    a per-server nickname.
    """
    if not isinstance(user, dict):
        return None
    return user.get("global_name") or user.get("display_name") or user.get("username")


class DiscordCallWorker:
    def __init__(self, channel_id, *, guild_id=None, label=None, self_mute=False, self_deaf=False, ring_recipient_ids=None, exocortex_conversation=None, exocortex_socket=None, call_voice=None):
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.label = label or channel_id
        self.self_mute = self_mute
        self.self_deaf = self_deaf
        self.exocortex_conversation = str(exocortex_conversation).strip() if exocortex_conversation else None
        self.exocortex_socket = str(exocortex_socket).strip() if exocortex_socket else None
        self.call_voice = str(call_voice).strip() if call_voice else None
        self.ring_recipient_ids = [str(user_id) for user_id in (ring_recipient_ids or []) if user_id]
        self.token = get_token()
        self.account_alias = selected_account()["alias"]

        self.running = True
        self.app_ws = None
        self.voice_ws = None
        self.voice_udp = None
        self._app_inflator = zlib.decompressobj()
        self._app_hb_gen = 0
        self._voice_hb_gen = 0
        self._app_heartbeat_interval = 41250
        self._voice_heartbeat_interval = 5000
        self._app_heartbeat_acked = True
        self._voice_heartbeat_acked = True
        self._app_sequence = None
        self._voice_sequence = 0
        self._voice_reconnect_attempts = 0

        self.my_id = None
        self.session_id = None
        self.voice_token = None
        self.voice_endpoint = None
        self.voice_ssrc = None
        self.voice_mode = None
        self.voice_secret_key = None
        self.voice_ready = False
        self._send_sequence = random.randrange(0, 0x10000)
        self._send_timestamp = random.randrange(0, 0x100000000)
        self._send_counter = 0
        self._requested_leave = False
        self._participant_names = {}
        self._active_participant_ids = set()
        self._control_seq = 0
        self._voice_media = None
        self._ssrc_cache = []
        self._speaking_cache = {}
        self._pending_voice_session_description = None
        self._call_client = None
        self._call_adapter = None
        self._call_id = None
        self._call_monitor_thread = None
        self._voice_send_lock = threading.Lock()

    def run(self):
        old_int = signal.getsignal(signal.SIGINT)
        old_term = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self._signal_shutdown)
        signal.signal(signal.SIGTERM, self._signal_shutdown)
        try:
            self._connect_app_gateway()
            self._request_voice_state(self.channel_id)
            print(
                f"Joining {self.label} {'muted' if self.self_mute else 'unmuted'}/"
                f"{'deafened' if self.self_deaf else 'undeafened'}…",
                flush=True,
            )

            deadline = time.time() + VOICE_CONNECT_TIMEOUT
            while self.running and not self.voice_ready:
                self._pump_app_gateway_once()
                self._poll_control()
                self._maybe_connect_voice_gateway()
                if self.voice_ws and not self.voice_ready:
                    self._pump_voice_gateway_once()
                if time.time() > deadline:
                    raise RuntimeError("Timed out joining Discord voice call")

            if not self.running:
                return
            _update_call_meta_env(status="joined", updated_at=time.time())
            print(f"Joined {self.label}. Press Ctrl+C to leave.", flush=True)
            if self.ring_recipient_ids:
                self._ring_recipients()
            self._start_exocortex_call()

            while self.running:
                self._pump_app_gateway_once()
                self._poll_control()
                self._maybe_connect_voice_gateway()
                if self.voice_ws:
                    self._pump_voice_gateway_once()
        finally:
            self.running = False
            self._stop_exocortex_call()
            self._leave_voice()
            self._close()
            signal.signal(signal.SIGINT, old_int)
            signal.signal(signal.SIGTERM, old_term)

    def _signal_shutdown(self, signum=None, frame=None):
        self.running = False

    def _poll_control(self):
        meta_path = os.environ.get(CALL_META_ENV)
        if not meta_path:
            return
        try:
            meta = json.loads(Path(meta_path).read_text())
        except Exception:
            return
        try:
            seq = int(meta.get("control_seq") or 0)
        except (TypeError, ValueError):
            return
        if seq <= self._control_seq:
            return
        self._control_seq = seq
        changed = False
        if "self_mute" in meta:
            next_mute = bool(meta.get("self_mute"))
            if self.self_mute != next_mute:
                self.self_mute = next_mute
                if self.self_mute:
                    self._send_speaking(False)
                changed = True
        if "self_deaf" in meta:
            next_deaf = bool(meta.get("self_deaf"))
            if self.self_deaf != next_deaf:
                self.self_deaf = next_deaf
                changed = True
        if changed:
            self._request_voice_state(self.channel_id)
            _update_call_meta_env(status="joined" if self.voice_ready else "joining", updated_at=time.time())
            print(f"Voice state: {'muted' if self.self_mute else 'unmuted'}/{'deafened' if self.self_deaf else 'undeafened'}", flush=True)

    # ─── App gateway ──────────────────────────────────────────────────────────

    def _connect_app_gateway(self):
        self._close_app_gateway()
        self._app_hb_gen += 1
        self._app_inflator = zlib.decompressobj()
        self._app_sequence = None
        self.my_id = None
        url = _gateway_url()
        self.app_ws = websocket.WebSocket()
        self.app_ws.settimeout(1)
        self.app_ws.connect(
            f"{url}/?v=9&encoding=json&compress=zlib-stream",
            header=[
                "Connection: keep-alive, Upgrade",
                "Sec-WebSocket-Extensions: permessage-deflate",
                f"User-Agent: {_USER_AGENT}",
            ],
        )
        hello = self._recv_app_json()
        self._app_heartbeat_interval = hello["d"]["heartbeat_interval"]
        self._send_app({
            "op": 2,
            "d": {
                "token": self.token,
                "capabilities": DEFAULT_CAPABILITIES,
                "properties": _build_properties(),
                "presence": {"activities": [], "status": "online", "since": None, "afk": False},
            },
        })
        self._start_app_heartbeat()

        deadline = time.time() + 15
        while self.running and not self.my_id:
            self._pump_app_gateway_once()
            if time.time() > deadline:
                raise RuntimeError("Timed out waiting for Discord gateway READY")

    def _close_app_gateway(self):
        self._app_hb_gen += 1
        ws = self.app_ws
        self.app_ws = None
        if ws:
            try:
                ws.close()
            except Exception:
                pass

    def _reconnect_app_gateway(self, reason: str):
        print(f"Discord gateway {reason}; reconnecting…", flush=True)
        last_error = None
        for attempt in range(1, 6):
            if not self.running:
                return
            try:
                self._connect_app_gateway()
                if self.running and self.channel_id:
                    self._request_voice_state(self.channel_id)
                print("Discord gateway reconnected.", flush=True)
                return
            except Exception as exc:
                last_error = exc
                print(f"Discord gateway reconnect attempt {attempt} failed: {exc}", flush=True)
                time.sleep(min(10, attempt * 2))
        raise RuntimeError(f"Discord gateway reconnect failed: {last_error}")

    def _recv_app_json(self):
        data = self.app_ws.recv()
        if isinstance(data, bytes):
            data = self._decompress_app(data)
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    def _decompress_app(self, data):
        if len(data) >= 4 and data[-4:] == ZLIB_SUFFIX:
            return self._app_inflator.decompress(data)
        return data

    def _start_app_heartbeat(self):
        self._app_heartbeat_acked = True
        gen = self._app_hb_gen
        threading.Thread(target=self._app_heartbeat_loop, daemon=True, args=(gen,)).start()

    def _app_heartbeat_loop(self, gen):
        time.sleep(self._app_heartbeat_interval * random.random() / 1000)
        while self.running and self._app_hb_gen == gen:
            if not self._app_heartbeat_acked:
                try:
                    self.app_ws.close()
                except Exception:
                    pass
                return
            self._app_heartbeat_acked = False
            self._send_app({"op": 1, "d": self._app_sequence})
            deadline = time.time() + self._app_heartbeat_interval / 1000
            while time.time() < deadline and self.running and self._app_hb_gen == gen:
                time.sleep(0.5)

    def _pump_app_gateway_once(self):
        if not self.app_ws:
            self._reconnect_app_gateway("was disconnected")
            return
        try:
            ws_op, data = self.app_ws.recv_data()
        except websocket.WebSocketTimeoutException:
            return
        except Exception as exc:
            if self.running:
                self._reconnect_app_gateway(f"disconnected ({exc})")
            return
        if ws_op == 8:
            if self.running:
                self._reconnect_app_gateway("closed")
            return
        data = self._decompress_app(data)
        if not data:
            return
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        event = json.loads(data)
        op = event.get("op")
        if op == 11:
            self._app_heartbeat_acked = True
        elif op == 7:
            self._reconnect_app_gateway("requested reconnect")
        elif op == 9:
            self._reconnect_app_gateway("invalidated the session")
        elif op == 0:
            self._app_sequence = event.get("s")
            self._on_app_dispatch(event.get("t"), event.get("d") or {})

    def _on_app_dispatch(self, event_type, data):
        if event_type == "READY":
            self.my_id = data.get("user", {}).get("id")
            self._seed_participant_names()
            return
        if event_type == "VOICE_STATE_UPDATE":
            self._handle_voice_state_update(data)
            return
        if event_type == "VOICE_SERVER_UPDATE":
            # DMs can have null guild_id; server voice events should match guild_id.
            event_guild = data.get("guild_id")
            if self.guild_id and event_guild and str(event_guild) != str(self.guild_id):
                return
            endpoint = data.get("endpoint")
            token = data.get("token")
            if endpoint and token:
                self.voice_endpoint = endpoint
                self.voice_token = token
            return
        if event_type in {"CALL_CREATE", "CALL_UPDATE"} and data.get("channel_id") == self.channel_id:
            self._handle_call_voice_states(data.get("voice_states") or [])
            return
        if event_type == "CALL_DELETE" and data.get("channel_id") == self.channel_id:
            for user_id in sorted(self._active_participant_ids):
                self._remove_media_user(user_id)
            self._active_participant_ids.clear()
            print("Call ended by Discord.", flush=True)
            self.running = False

    def _seed_participant_names(self):
        try:
            ch = api.get_channel(self.channel_id)
        except Exception:
            return
        for recipient in ch.get("recipients") or []:
            if not isinstance(recipient, dict) or not recipient.get("id"):
                continue
            user_id = str(recipient.get("id"))
            name = _record_style_display_name(recipient)
            if name:
                self._participant_names[user_id] = name

    def _display_name_for_user(self, user_id):
        return self._participant_names.get(str(user_id)) or str(user_id)

    def _remember_voice_state_name(self, state):
        user_id = state.get("user_id") or (state.get("user") or {}).get("id")
        if not user_id:
            return None
        user_id = str(user_id)
        member = state.get("member") if isinstance(state.get("member"), dict) else {}
        user = state.get("user") if isinstance(state.get("user"), dict) else {}
        member_user = member.get("user") if isinstance(member.get("user"), dict) else {}
        name = _record_style_display_name(member_user) or _record_style_display_name(user)
        if name:
            self._participant_names[user_id] = name
        return user_id

    def _handle_voice_state_update(self, data):
        user_id = self._remember_voice_state_name(data)
        if not user_id:
            return
        if user_id == str(self.my_id):
            if data.get("channel_id") == self.channel_id:
                self.session_id = data.get("session_id")
            elif data.get("channel_id") is None:
                self.session_id = None
            return

        if data.get("channel_id") == self.channel_id:
            current = set(self._active_participant_ids)
            current.add(user_id)
            self._sync_call_participants(current)
            return

        if user_id in self._active_participant_ids:
            self._active_participant_ids.discard(user_id)
            self._remove_media_user(user_id)

    def _handle_call_voice_states(self, states):
        current = set()
        saw_voice_state = False
        for state in states:
            if not isinstance(state, dict):
                continue
            saw_voice_state = True
            user_id = self._remember_voice_state_name(state)
            if user_id and user_id != str(self.my_id):
                current.add(user_id)
        if saw_voice_state:
            self._sync_call_participants(current)

    def _sync_call_participants(self, current):
        current = set(current)
        removed = self._active_participant_ids - current
        for user_id in sorted(removed):
            self._remove_media_user(user_id)
        self._active_participant_ids = current
        self._sync_media_participants()

    def _sync_media_participants(self):
        if self._voice_media:
            self._voice_media.set_active_remote_users(self._active_participant_ids)

    def _log_voice_media(self, message):
        print(f"[voice-media] {message}", flush=True)

    def _receive_call_pcm(self, user_id, pcm, sample_rate, channels):
        bridge = self._call_adapter
        if bridge:
            bridge.push_pcm(user_id, pcm, sample_rate, channels)

    def send_call_opus(self, opus_payload):
        media = self._voice_media
        if self.self_mute or not media or not media.can_encode_outgoing():
            return False
        with self._voice_send_lock:
            return send_outgoing_opus_payload(self, opus_payload, media)

    def _start_exocortex_call(self):
        client = ExocortexCallClient(self.exocortex_socket)
        client.connect()
        conv_id = self.exocortex_conversation or client.create_conversation(f"Discord call · {self.label}")
        adapter = {
            "type": "external",
            "id": f"discord:{self.account_alias}:{self.channel_id}",
            "toolName": "discord",
            "accountAlias": self.account_alias,
            "endpointId": str(self.channel_id),
            "label": self.label,
        }
        call_id, _state = client.start_call(conv_id, adapter, voice=self.call_voice)
        bridge = DiscordCallAdapter(self, client, conv_id, call_id, log=self._log_voice_media)
        self._call_client = client
        self._call_adapter = bridge
        self._call_id = call_id
        self.exocortex_conversation = conv_id
        bridge.start()
        self.update_call_meta(
            exocortex_conversation=conv_id,
            exocortex_call_id=call_id,
            updated_at=time.time(),
        )
        self._call_monitor_thread = threading.Thread(
            target=self._monitor_exocortex_call,
            name="discord-call-control",
            daemon=True,
        )
        self._call_monitor_thread.start()
        print(f"Discord media attached to Exocortex conversation {conv_id} (call {call_id}).", flush=True)

    def _monitor_exocortex_call(self):
        client = self._call_client
        call_id = self._call_id
        while self.running and client is self._call_client:
            try:
                event = client.receive(timeout=1)
            except TimeoutError:
                continue
            except Exception:
                if self.running:
                    self.running = False
                return
            if event.get("type") == "call_state" and event.get("callId") == call_id:
                if event.get("state") in {"closed", "error"}:
                    self.running = False
                    return

    def _stop_exocortex_call(self):
        bridge, self._call_adapter = self._call_adapter, None
        if bridge:
            bridge.stop()
        client, self._call_client = self._call_client, None
        call_id, self._call_id = self._call_id, None
        if client:
            if call_id and self.exocortex_conversation:
                try:
                    client.send({
                        "type": "stop_call",
                        "reqId": f"discord-call-stop-{uuid.uuid4()}",
                        "convId": self.exocortex_conversation,
                        "callId": call_id,
                    })
                except Exception:
                    pass
            client.close()

    def _remove_media_user(self, user_id):
        if self._voice_media:
            self._voice_media.remove_user(user_id)

    def _ring_recipients(self):
        try:
            api.post(f"/channels/{self.channel_id}/call/ring", body={"recipients": self.ring_recipient_ids})
            print(f"Ringing {len(self.ring_recipient_ids)} recipient(s)…", flush=True)
        except Exception as exc:
            print(f"Failed to ring recipient(s): {exc}", flush=True)

    def _request_voice_state(self, channel_id):
        self._send_app({
            "op": 4,
            "d": {
                "guild_id": self.guild_id,
                "channel_id": channel_id,
                "self_mute": self.self_mute,
                "self_deaf": self.self_deaf,
                "self_video": False,
                "preferred_regions": ["automatic"],
                "preferred_region": "automatic",
                "flags": VOICE_FLAGS,
            },
        })

    def _leave_voice(self):
        if self._requested_leave:
            return
        self._requested_leave = True
        self._request_voice_disconnect()

    def _request_voice_disconnect(self):
        try:
            if self.app_ws:
                self._send_app({
                    "op": 4,
                    "d": {
                        "guild_id": None,
                        "channel_id": None,
                        "self_mute": False,
                        "self_deaf": False,
                        "self_video": False,
                        "flags": VOICE_FLAGS,
                    },
                })
                time.sleep(0.2)
        except Exception:
            pass

    def _send_app(self, payload):
        if not self.app_ws:
            return
        try:
            self.app_ws.send(json.dumps(payload))
        except Exception:
            pass

    # ─── Voice gateway ────────────────────────────────────────────────────────

    def _maybe_connect_voice_gateway(self):
        if self.voice_ws or not (self.session_id and self.voice_token and self.voice_endpoint):
            return
        self._connect_voice_gateway()

    def _connect_voice_gateway(self):
        self.voice_ready = False
        self._voice_sequence = 0
        self._ensure_voice_media_object()
        endpoint = re.sub(r"^wss?://", "", self.voice_endpoint or "")
        self.voice_ws = websocket.WebSocket()
        self.voice_ws.settimeout(1)
        self.voice_ws.connect(f"wss://{endpoint}/?v={VOICE_GATEWAY_VERSION}")

    def _pump_voice_gateway_once(self):
        try:
            ws_op, data = self.voice_ws.recv_data()
        except websocket.WebSocketTimeoutException:
            return
        except Exception as exc:
            if self.running:
                self._recover_voice_gateway(f"disconnected ({exc})")
            return
        if ws_op == 8:
            if self.running:
                code, reason = self._parse_voice_gateway_close(data)
                if self._is_terminal_voice_gateway_close(code, reason):
                    print(f"Discord voice gateway closed ({code or 'unknown'}: {reason or 'unknown reason'}); call ended.", flush=True)
                    self.running = False
                    return
                if self._is_recoverable_voice_gateway_close(code, reason):
                    self._recover_voice_gateway(f"closed ({code or 'unknown'}: {reason or 'unknown reason'})")
                    return
                raise RuntimeError(f"Discord voice gateway closed ({code or 'unknown'}: {reason or 'unknown reason'})")
            return
        if isinstance(data, bytes):
            if len(data) >= 3:
                sequence = int.from_bytes(data[:2], "big")
                self._voice_sequence = max(self._voice_sequence, sequence)
                opcode = data[2]
                if self._voice_media and self._voice_media.handle_binary_opcode(opcode, data[3:]):
                    return
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                return
        payload = json.loads(data)
        seq = payload.get("seq")
        if isinstance(seq, int):
            self._voice_sequence = max(self._voice_sequence, seq)
        op = payload.get("op")
        if op == 8:
            interval = (payload.get("d") or {}).get("heartbeat_interval")
            if interval:
                self._voice_heartbeat_interval = interval
            # Identify before starting the heartbeat thread. Discord can close
            # the voice gateway as "Not authenticated" if a heartbeat races
            # ahead of op 0 identify on a fresh voice websocket.
            self._voice_identify()
            self._start_voice_heartbeat()
        elif op == 6:
            self._voice_heartbeat_acked = True
        elif op == 3:
            self._send_voice_heartbeat()
        elif op == 2:
            self._handle_voice_ready(payload.get("d") or {})
        elif op == 4:
            self._handle_voice_session_description(payload.get("d") or {})
            self.voice_ready = True
            self._voice_reconnect_attempts = 0
            _update_call_meta_env(status="joined", updated_at=time.time())
        elif op == 5:
            self._handle_voice_speaking(payload.get("d") or {})
        elif op == 11:
            data = payload.get("d") or {}
            if isinstance(data, dict):
                user_ids = [str(user_id) for user_id in data.get("user_ids") or [] if user_id]
                if self._voice_media:
                    self._voice_media.dave.add_known_users(user_ids)
        elif op == 13:
            data = payload.get("d") or {}
            if isinstance(data, dict) and data.get("user_id"):
                self._remove_media_user(str(data.get("user_id")))
        elif op == 9:
            self._recover_voice_gateway("invalidated the session")
        elif self._voice_media:
            self._voice_media.handle_json_opcode(op, payload.get("d"))

    def _parse_voice_gateway_close(self, data):
        code = getattr(self.voice_ws, "status", None)
        reason = ""
        if isinstance(data, bytes):
            if len(data) >= 2:
                try:
                    code = struct.unpack("!H", data[:2])[0]
                    reason = data[2:].decode("utf-8", errors="replace")
                except Exception:
                    reason = data.decode("utf-8", errors="replace")
            else:
                reason = data.decode("utf-8", errors="replace")
        elif data:
            reason = str(data)
        return code, reason

    def _is_recoverable_voice_gateway_close(self, code, reason):
        reason = (reason or "").lower()
        return (
            code in VOICE_GATEWAY_RECOVERABLE_CLOSE_CODES
            or "session is no longer valid" in reason
            or "invalidated" in reason
            or "server crashed" in reason
            or "connection ended" in reason
            or "abnormal" in reason
        )

    def _is_terminal_voice_gateway_close(self, code, reason):
        reason = (reason or "").lower()
        return code in VOICE_GATEWAY_TERMINAL_CLOSE_CODES or "call terminated" in reason

    def _recover_voice_gateway(self, reason: str):
        if not self.running:
            return
        self._voice_reconnect_attempts += 1
        attempt = self._voice_reconnect_attempts
        print(f"Discord voice gateway {reason}; reconnecting (attempt {attempt})…", flush=True)
        _update_call_meta_env(status="reconnecting", updated_at=time.time())
        self._reset_voice_gateway_state(stop_media=True)
        if attempt % VOICE_GATEWAY_APP_RECONNECT_EVERY == 0:
            try:
                self._reconnect_app_gateway(f"refreshing after voice {reason}")
            except Exception as exc:
                print(f"Discord gateway refresh after voice reconnect failed: {exc}", flush=True)
        elif self.app_ws and self.channel_id:
            self._request_voice_disconnect()
            self._request_voice_state(self.channel_id)
        time.sleep(min(VOICE_GATEWAY_RECONNECT_DELAY * attempt, VOICE_GATEWAY_RECONNECT_MAX_DELAY))

    def _reset_voice_gateway_state(self, *, stop_media: bool):
        self._voice_hb_gen += 1
        ws = self.voice_ws
        self.voice_ws = None
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        if self.voice_udp:
            try:
                self.voice_udp.close()
            except Exception:
                pass
            self.voice_udp = None
        self.voice_ssrc = None
        self.voice_mode = None
        self.voice_secret_key = None
        self._send_sequence = random.randrange(0, 0x10000)
        self._send_timestamp = random.randrange(0, 0x100000000)
        self._send_counter = 0
        if stop_media and self._voice_media:
            try:
                self._voice_media.stop()
            except Exception:
                pass
            self._voice_media = None
        self._pending_voice_session_description = None
        self.voice_ready = False
        self.voice_token = None
        self.voice_endpoint = None
        self.session_id = None
        self._voice_sequence = 0

    def _start_voice_heartbeat(self):
        self._voice_heartbeat_acked = True
        self._voice_hb_gen += 1
        gen = self._voice_hb_gen
        threading.Thread(target=self._voice_heartbeat_loop, daemon=True, args=(gen,)).start()

    def _voice_heartbeat_loop(self, gen):
        while self.running and self._voice_hb_gen == gen:
            if not self._voice_heartbeat_acked:
                try:
                    self.voice_ws.close()
                except Exception:
                    pass
                return
            self._voice_heartbeat_acked = False
            self._send_voice_heartbeat()
            deadline = time.time() + max(1, self._voice_heartbeat_interval / 1000)
            while time.time() < deadline and self.running and self._voice_hb_gen == gen:
                time.sleep(0.5)

    def _voice_identify(self):
        advertised_dave = DAVE_PROTOCOL_VERSION
        try:
            advertised_dave = max(advertised_dave, int(VoiceReceiveMedia.advertised_dave_protocol_version_static()))
        except Exception:
            pass
        self._send_voice({
            "op": 0,
            "d": {
                "server_id": self.guild_id or self.channel_id,
                "channel_id": self.channel_id,
                "user_id": self.my_id,
                "session_id": self.session_id,
                "token": self.voice_token,
                "video": False,
                "max_dave_protocol_version": advertised_dave,
            },
        })

    def _handle_voice_ready(self, data):
        ip = data.get("ip")
        port = data.get("port")
        ssrc = data.get("ssrc")
        if not ip or not port or not ssrc:
            raise RuntimeError("Discord voice gateway sent incomplete UDP details")
        modes = [m for m in (data.get("modes") or []) if isinstance(m, str)]
        mode = select_encryption_mode(modes)
        self.voice_ssrc = int(ssrc)
        udp, address, discovered_port = udp_discovery(ip, int(port), int(ssrc))
        udp.settimeout(0.5)
        self.voice_udp = udp
        self._ensure_voice_media()
        self._send_voice({
            "op": 1,
            "d": {
                "protocol": "udp",
                "data": {"address": address, "port": discovered_port, "mode": mode},
                "codecs": [
                    {"name": "opus", "type": "audio", "priority": 1000, "payload_type": OPUS_PAYLOAD_TYPE},
                ],
            },
        })

    def _handle_voice_session_description(self, data):
        if not isinstance(data, dict):
            return
        self._pending_voice_session_description = data
        self._ensure_voice_media()

    def _ensure_voice_media_object(self):
        if self._voice_media or not self.my_id:
            return self._voice_media
        self._voice_media = VoiceReceiveMedia(
            self_user_id=str(self.my_id),
            channel_id=str(self.channel_id),
            send_json=self._send_voice,
            send_binary=self._send_voice_binary,
            name_for_user=self._display_name_for_user,
            log=self._log_voice_media,
            pcm_sink=self._receive_call_pcm,
        )
        self._sync_media_participants()
        for ssrc, user_id in self._ssrc_cache:
            self._voice_media.add_ssrc_mapping(ssrc, user_id)
        self._ssrc_cache.clear()
        for user_id, speaking in self._speaking_cache.items():
            self._voice_media.set_user_speaking(user_id, speaking)
        return self._voice_media

    def _ensure_voice_media(self):
        data = self._pending_voice_session_description
        if not isinstance(data, dict) or not self.voice_udp:
            return
        secret_key = data.get("secret_key")
        mode = data.get("mode")
        if not secret_key or not mode:
            return
        self.voice_secret_key = bytes(secret_key)
        self.voice_mode = str(mode)
        media = self._ensure_voice_media_object()
        if not media:
            return
        media.configure_media(udp=self.voice_udp, mode=str(mode), secret_key=bytes(secret_key))
        media.set_self_ssrc(self.voice_ssrc)
        media.handle_session_description(data)
        media.start()

    def _handle_voice_speaking(self, data):
        if not isinstance(data, dict):
            return
        user_id = data.get("user_id")
        ssrc = data.get("ssrc")
        speaking = bool(int(data.get("speaking") or 0))
        if user_id is None or ssrc is None:
            return
        item = (int(ssrc), str(user_id))
        self._speaking_cache[str(user_id)] = speaking
        if self._voice_media:
            self._voice_media.set_user_speaking(str(user_id), speaking)
            self._voice_media.add_ssrc_mapping(*item)
        else:
            self._ssrc_cache.append(item)

    def _send_voice_heartbeat(self):
        self._send_voice({"op": 3, "d": {"t": int(time.time() * 1000), "seq_ack": self._voice_sequence}})

    def _send_voice(self, payload):
        if not self.voice_ws:
            return
        try:
            self.voice_ws.send(json.dumps(payload))
        except Exception:
            pass

    def _send_voice_binary(self, opcode, payload):
        if not self.voice_ws:
            return
        try:
            self.voice_ws.send_binary(bytes([int(opcode) & 0xFF]) + bytes(payload))
        except Exception:
            pass

    def update_call_meta(self, **updates):
        _update_call_meta_env(**updates)

    def next_send_counter(self):
        self._send_counter = (self._send_counter + 1) & 0xFFFFFFFF
        return struct.pack("!I", self._send_counter)

    def _send_speaking(self, speaking):
        if self.voice_ssrc is None:
            return
        self._send_voice({"op": 5, "d": {"speaking": 1 if speaking else 0, "delay": 0, "ssrc": int(self.voice_ssrc)}})

    def _close(self):
        if self._voice_media:
            try:
                self._voice_media.stop()
            except Exception:
                pass
            self._voice_media = None
        self._pending_voice_session_description = None
        if self.voice_ws:
            try:
                self.voice_ws.close()
            except Exception:
                pass
        self.voice_ws = None
        self._close_app_gateway()
        if self.voice_udp:
            try:
                self.voice_udp.close()
            except Exception:
                pass
            self.voice_udp = None
