"""Voice call helpers for Discord CLI.

This intentionally joins voice/call sessions without local audio capture or
playback yet. It is meant as a lightweight test peer for Record's call flow: keep
a Discord voice gateway session alive, appear in the call, and leave cleanly when
interrupted. Detached calls are muted but undeafened by default so receive-side
features can be layered on; use `discord call deafen` to opt out of listening.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import random
import re
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
import zlib

import websocket

from src import api
from src.voice_receive import VoiceReceiveTranscription
from src.auth import get_token
from src.private_channels import private_channel_label_for_type, private_channel_name, private_channel_type

GATEWAY_HOST = "discord.com"
ZLIB_SUFFIX = b"\x00\x00\xff\xff"
DEFAULT_CAPABILITIES = 30717
VOICE_FLAGS = 3
VOICE_GATEWAY_VERSION = 8
VOICE_CONNECT_TIMEOUT = 20
VOICE_UDP_TIMEOUT = 5
OPUS_PAYLOAD_TYPE = 120
DAVE_PROTOCOL_VERSION = 1
PROJECT_DIR = Path(__file__).resolve().parents[1]
CALL_STATE_DIR = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state")) / "discord-cli" / "calls"
CALL_LOG_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "discord-cli" / "calls"
CALL_META_ENV = "DISCORD_CALL_META_PATH"
CALL_NOTIFY_TARGETS_ENV = "DISCORD_CALL_NOTIFY_TARGETS"

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


def _snowflake(value):
    if value is None:
        return None
    text = str(value)
    return text if re.match(r"^\d{17,20}$", text) else None


def _select_encryption_mode(modes):
    if "aead_aes256_gcm_rtpsize" in modes:
        return "aead_aes256_gcm_rtpsize"
    if "aead_xchacha20_poly1305_rtpsize" in modes:
        return "aead_xchacha20_poly1305_rtpsize"
    return modes[0] if modes else "aead_aes256_gcm_rtpsize"


def _udp_discovery(host, port, ssrc):
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.settimeout(VOICE_UDP_TIMEOUT)
    udp.connect((host, int(port)))
    packet = bytearray(74)
    struct.pack_into(">HHI", packet, 0, 1, 70, int(ssrc))
    udp.send(packet)
    response = udp.recv(74)
    if len(response) < 74:
        udp.close()
        raise RuntimeError("Discord voice UDP discovery returned a short packet")
    packet_type, length = struct.unpack_from(">HH", response, 0)
    if packet_type != 2 or length != 70:
        udp.close()
        raise RuntimeError("Discord voice UDP discovery returned an invalid packet")
    address = response[8:72].split(b"\x00", 1)[0].decode("ascii", errors="replace")
    discovered_port = struct.unpack_from(">H", response, 72)[0]
    return udp, address, discovered_port


class NoAudioCallJoiner:
    def __init__(self, channel_id, *, guild_id=None, label=None, self_mute=True, self_deaf=False, ring_recipient_ids=None, transcribe=True):
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.label = label or channel_id
        self.self_mute = self_mute
        self.self_deaf = self_deaf
        self.transcribe_enabled = bool(transcribe and not self_deaf)
        self.ring_recipient_ids = [str(user_id) for user_id in (ring_recipient_ids or []) if user_id]
        self.token = get_token()

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

        self.my_id = None
        self.session_id = None
        self.voice_token = None
        self.voice_endpoint = None
        self.voice_ready = False
        self._requested_leave = False
        self._participant_names = {}
        self._active_participant_ids = set()
        self._participant_audio_states = {}
        self._notified_leave_ids = set()
        self._participants_seeded = False
        self._control_seq = 0
        self._voice_transcription = None
        self._ssrc_cache = []
        self._pending_voice_session_description = None

    def run(self):
        old_int = signal.getsignal(signal.SIGINT)
        old_term = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self._signal_shutdown)
        signal.signal(signal.SIGTERM, self._signal_shutdown)
        try:
            self._connect_app_gateway()
            self._request_voice_state(self.channel_id)
            print(
                f"Joining {self.label} {'muted' if self.self_mute else 'unmuted'}/{'deafened' if self.self_deaf else 'undeafened'} "
                f"({'transcribing' if self.transcribe_enabled else 'not transcribing'})…",
                flush=True,
            )

            deadline = time.time() + VOICE_CONNECT_TIMEOUT
            while self.running and not self.voice_ready:
                self._pump_app_gateway_once()
                self._poll_control()
                if self.session_id and self.voice_token and self.voice_endpoint and not self.voice_ws:
                    self._connect_voice_gateway()
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

            while self.running:
                self._pump_app_gateway_once()
                self._poll_control()
                if self.voice_ws:
                    self._pump_voice_gateway_once()
        finally:
            self.running = False
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
                changed = True
        if "self_deaf" in meta:
            next_deaf = bool(meta.get("self_deaf"))
            if self.self_deaf != next_deaf:
                self.self_deaf = next_deaf
                self.transcribe_enabled = not self.self_deaf and bool(meta.get("transcribe", True))
                self._set_transcription_enabled(self.transcribe_enabled)
                changed = True
        if "transcribe" in meta:
            next_transcribe = bool(meta.get("transcribe")) and not self.self_deaf
            if self.transcribe_enabled != next_transcribe:
                self.transcribe_enabled = next_transcribe
                self._set_transcription_enabled(self.transcribe_enabled)
        if changed:
            self._request_voice_state(self.channel_id)
            _update_call_meta_env(status="joined" if self.voice_ready else "joining", updated_at=time.time())
            print(f"Voice state: {'muted' if self.self_mute else 'unmuted'}/{'deafened' if self.self_deaf else 'undeafened'}", flush=True)

    # ─── App gateway ──────────────────────────────────────────────────────────

    def _connect_app_gateway(self):
        self._app_hb_gen += 1
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
        try:
            ws_op, data = self.app_ws.recv_data()
        except websocket.WebSocketTimeoutException:
            return
        except Exception as exc:
            if self.running:
                raise RuntimeError(f"Discord gateway disconnected: {exc}")
            return
        if ws_op == 8:
            if self.running:
                raise RuntimeError("Discord gateway closed")
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
            raise RuntimeError("Discord gateway requested reconnect")
        elif op == 9:
            raise RuntimeError("Discord gateway invalidated the session")
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
            self._participants_seeded = True
            for user_id in sorted(self._active_participant_ids):
                if user_id not in self._notified_leave_ids:
                    self._notified_leave_ids.add(user_id)
                    self._notify_call_event(f"☎ {self._display_name_for_user(user_id)} left {self.label}")
                self._remove_transcription_user(user_id)
            self._active_participant_ids.clear()
            self._participant_audio_states.clear()
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
            name = recipient.get("username") or recipient.get("global_name") or recipient.get("display_name")
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
        name = (
            member.get("nick")
            or user.get("username")
            or user.get("global_name")
            or member_user.get("username")
            or member_user.get("global_name")
        )
        if name:
            self._participant_names[user_id] = name
        return user_id

    def _voice_audio_state_from_state(self, state):
        audio = {}
        if "self_mute" in state or "mute" in state:
            audio["muted"] = bool(state.get("self_mute") or state.get("mute"))
        if "self_deaf" in state or "deaf" in state:
            audio["deafened"] = bool(state.get("self_deaf") or state.get("deaf"))
        return audio

    def _remember_participant_audio_state(self, user_id, state):
        if not user_id or user_id == str(self.my_id):
            return
        audio = self._voice_audio_state_from_state(state)
        if not audio:
            return
        user_id = str(user_id)
        previous = self._participant_audio_states.get(user_id, {})
        current = dict(previous)
        changes = []
        for key, value in audio.items():
            old_value = previous.get(key)
            current[key] = value
            if old_value is not None and old_value != value:
                if key == "muted":
                    changes.append("muted" if value else "unmuted")
                elif key == "deafened":
                    changes.append("deafened" if value else "undeafened")
        self._participant_audio_states[user_id] = current
        if changes:
            self._notify_call_event(f"☎ {self._display_name_for_user(user_id)} {' and '.join(changes)} in {self.label}")

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
            self._remember_participant_audio_state(user_id, data)
            current = set(self._active_participant_ids)
            current.add(user_id)
            self._sync_call_participants(current)
            return

        if user_id in self._active_participant_ids:
            self._active_participant_ids.discard(user_id)
            self._participant_audio_states.pop(user_id, None)
            self._remove_transcription_user(user_id)
            if user_id not in self._notified_leave_ids:
                self._notified_leave_ids.add(user_id)
                self._notify_call_event(f"☎ {self._display_name_for_user(user_id)} left {self.label}")

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
                self._remember_participant_audio_state(user_id, state)
        if saw_voice_state:
            self._sync_call_participants(current)

    def _sync_call_participants(self, current):
        current = set(current)
        if not self._participants_seeded:
            self._participants_seeded = True
            # For a plain `join`, the first CALL_UPDATE/VOICE_STATE_UPDATE is a
            # baseline and should not announce existing participants. For
            # `start`/`ring`, however, the first remote participant we observe is
            # the callee answering our outbound call; announce it instead of
            # swallowing it as baseline.
            if self.ring_recipient_ids and current:
                for user_id in sorted(current):
                    self._notified_leave_ids.discard(user_id)
                    self._notify_call_event(f"☎ {self._display_name_for_user(user_id)} joined {self.label}")
            self._active_participant_ids = current
            self._notified_leave_ids.difference_update(current)
            return
        joined = current - self._active_participant_ids
        removed = self._active_participant_ids - current
        for user_id in sorted(joined):
            self._notified_leave_ids.discard(user_id)
            self._notify_call_event(f"☎ {self._display_name_for_user(user_id)} joined {self.label}")
        for user_id in sorted(removed):
            self._participant_audio_states.pop(user_id, None)
            self._remove_transcription_user(user_id)
            if user_id not in self._notified_leave_ids:
                self._notified_leave_ids.add(user_id)
                self._notify_call_event(f"☎ {self._display_name_for_user(user_id)} left {self.label}")
        self._active_participant_ids = current
        self._notified_leave_ids.difference_update(current)

    def _notify_call_event(self, message):
        self._notify_exo(message, prefix="Discord Call")

    def _notify_voice_transcript(self, message, prefix="Discord Voice"):
        self._notify_exo(message, prefix=prefix)

    def _notify_exo(self, message, *, prefix):
        targets = [target for target in os.environ.get(CALL_NOTIFY_TARGETS_ENV, "").split(",") if target]
        if not targets:
            return
        print(message, flush=True)
        for target in targets:
            threading.Thread(target=self._send_notification, args=(target, prefix, message), daemon=True).start()

    def _send_notification(self, target, prefix, message):
        try:
            subprocess.run(
                ["exo", "send", f"[{prefix}] {message}", "-c", target, "--timeout", "600", "--no-notify"],
                capture_output=True,
                text=True,
                timeout=660,
            )
        except Exception:
            pass

    def _log_voice_transcription(self, message):
        print(f"[voice-transcribe] {message}", flush=True)

    def _set_transcription_enabled(self, enabled):
        if self._voice_transcription:
            self._voice_transcription.set_enabled(enabled)

    def _remove_transcription_user(self, user_id):
        if self._voice_transcription:
            self._voice_transcription.remove_user(user_id)

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

    def _connect_voice_gateway(self):
        self._ensure_voice_transcription_object()
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
                raise RuntimeError(f"Discord voice gateway disconnected: {exc}")
            return
        if ws_op == 8:
            if self.running:
                code = getattr(self.voice_ws, "status", None)
                reason = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else str(data or "")
                raise RuntimeError(f"Discord voice gateway closed ({code or 'unknown'}: {reason or 'unknown reason'})")
            return
        if isinstance(data, bytes):
            if len(data) >= 3:
                sequence = int.from_bytes(data[:2], "big")
                self._voice_sequence = max(self._voice_sequence, sequence)
                opcode = data[2]
                if self._voice_transcription and self._voice_transcription.handle_binary_opcode(opcode, data[3:]):
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
        elif op == 5:
            self._handle_voice_speaking(payload.get("d") or {})
        elif op == 11:
            data = payload.get("d") or {}
            if isinstance(data, dict):
                user_ids = [str(user_id) for user_id in data.get("user_ids") or [] if user_id]
                if self._voice_transcription:
                    self._voice_transcription.dave.add_known_users(user_ids)
        elif op == 13:
            data = payload.get("d") or {}
            if isinstance(data, dict) and data.get("user_id"):
                self._remove_transcription_user(str(data.get("user_id")))
        elif op == 9:
            raise RuntimeError("Discord voice gateway invalidated the session")
        elif self._voice_transcription:
            self._voice_transcription.handle_json_opcode(op, payload.get("d"))

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
            advertised_dave = max(advertised_dave, int(VoiceReceiveTranscription.advertised_dave_protocol_version_static()))
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
        mode = _select_encryption_mode(modes)
        udp, address, discovered_port = _udp_discovery(ip, int(port), int(ssrc))
        udp.settimeout(0.5)
        self.voice_udp = udp
        self._ensure_voice_transcription()
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
        self._ensure_voice_transcription()

    def _ensure_voice_transcription_object(self):
        if self._voice_transcription or not self.my_id:
            return self._voice_transcription
        self._voice_transcription = VoiceReceiveTranscription(
            self_user_id=str(self.my_id),
            channel_id=str(self.channel_id),
            label=self.label,
            send_json=self._send_voice,
            send_binary=self._send_voice_binary,
            notify=self._notify_voice_transcript,
            name_for_user=self._display_name_for_user,
            log=self._log_voice_transcription,
        )
        self._voice_transcription.set_enabled(self.transcribe_enabled)
        for ssrc, user_id in self._ssrc_cache:
            self._voice_transcription.add_ssrc_mapping(ssrc, user_id)
        self._ssrc_cache.clear()
        return self._voice_transcription

    def _ensure_voice_transcription(self):
        data = self._pending_voice_session_description
        if not isinstance(data, dict) or not self.voice_udp:
            return
        secret_key = data.get("secret_key")
        mode = data.get("mode")
        if not secret_key or not mode:
            return
        transcription = self._ensure_voice_transcription_object()
        if not transcription:
            return
        transcription.configure_media(udp=self.voice_udp, mode=str(mode), secret_key=bytes(secret_key))
        transcription.handle_session_description(data)
        transcription.set_enabled(self.transcribe_enabled)
        transcription.start()

    def _handle_voice_speaking(self, data):
        if not isinstance(data, dict):
            return
        user_id = data.get("user_id")
        ssrc = data.get("ssrc")
        if user_id is None or ssrc is None:
            return
        item = (int(ssrc), str(user_id))
        if self._voice_transcription:
            self._voice_transcription.add_ssrc_mapping(*item)
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

    def _close(self):
        if self._voice_transcription:
            try:
                self._voice_transcription.stop()
            except Exception:
                pass
            self._voice_transcription = None
        self._pending_voice_session_description = None
        for ws in (self.voice_ws, self.app_ws):
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
        self.voice_ws = None
        self.app_ws = None
        if self.voice_udp:
            try:
                self.voice_udp.close()
            except Exception:
                pass
            self.voice_udp = None


def _resolve_call_target(args):
    ch = _resolve_call_channel(args)
    ch_type = ch.get("type")
    if ch_type in (1, 3):
        return ch["id"], None, private_channel_label_for_type(private_channel_type(ch), private_channel_name(ch))
    guild_id = ch.get("guild_id")
    name = ch.get("name", getattr(args, "target", ch.get("id", "")))
    return ch["id"], guild_id, f"#{name}" if guild_id else name


def _resolve_call_channel(args):
    from src.resolve import resolve_channel, resolve_dm, resolve_guild

    if args.dm:
        return resolve_dm(args.target)

    guild_arg = getattr(args, "guild", None)
    if guild_arg:
        guild = resolve_guild(guild_arg)
        return resolve_channel(args.target, guild["id"])

    if re.match(r"^\d{17,20}$", args.target):
        return api.get_channel(args.target)

    # Most call testing is done in DMs, so try those first when no guild is given.
    try:
        return resolve_dm(args.target)
    except RuntimeError:
        raise SystemExit("Use --dm for DMs, --guild/-g for server voice channels, or pass a channel ID.")


def _recipient_ids_for_private_call(channel):
    ch_type = channel.get("type")
    if ch_type not in (1, 3):
        raise SystemExit("Ringing is only supported for DMs and group DMs.")
    return [str(r.get("id")) for r in channel.get("recipients") or [] if isinstance(r, dict) and r.get("id")]



def _pid_alive(pid):
    try:
        pid = int(pid)
        os.kill(pid, 0)
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
            if ") Z" in stat:
                return False
        except Exception:
            pass
        return True
    except (ProcessLookupError, ValueError):
        return False
    except PermissionError:
        return True


def _call_paths(channel_id):
    CALL_STATE_DIR.mkdir(parents=True, exist_ok=True)
    CALL_LOG_DIR.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(channel_id))
    return {
        "meta": CALL_STATE_DIR / f"{safe}.json",
        "log": CALL_LOG_DIR / f"{safe}.log",
    }


def _read_call_meta(path):
    try:
        meta = json.loads(path.read_text())
    except Exception:
        return None
    pid = meta.get("pid")
    if not pid or not _pid_alive(pid):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        return None
    return meta


def _running_call_metas():
    CALL_STATE_DIR.mkdir(parents=True, exist_ok=True)
    metas = []
    for path in sorted(CALL_STATE_DIR.glob("*.json")):
        meta = _read_call_meta(path)
        if meta:
            metas.append(meta)
    return metas


def _write_call_meta(path, meta):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def _update_call_meta_env(**updates):
    meta_path = os.environ.get(CALL_META_ENV)
    if not meta_path:
        return
    path = Path(meta_path)
    try:
        meta = json.loads(path.read_text()) if path.exists() else {}
        meta.update(updates)
        _write_call_meta(path, meta)
    except Exception:
        pass


def _remove_call_meta_env():
    meta_path = os.environ.get(CALL_META_ENV)
    if not meta_path:
        return
    try:
        Path(meta_path).unlink(missing_ok=True)
    except Exception:
        pass


def _join_foreground_channel(channel_id, guild_id, label, *, self_mute=True, self_deaf=False, ring_recipient_ids=None, transcribe=True):
    joiner = NoAudioCallJoiner(
        channel_id,
        guild_id=guild_id,
        label=label,
        self_mute=self_mute,
        self_deaf=self_deaf,
        ring_recipient_ids=ring_recipient_ids,
        transcribe=transcribe,
    )
    try:
        _update_call_meta_env(status="joining", updated_at=time.time())
        joiner.run()
    finally:
        _remove_call_meta_env()


def _join_child(argv):
    p = argparse.ArgumentParser(prog="python -m src.calling __join_foreground")
    p.add_argument("channel_id")
    p.add_argument("guild_id")
    p.add_argument("label")
    p.add_argument("--unmuted", action="store_true")
    p.add_argument("--deafened", action="store_true")
    p.add_argument("--undeafened", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--ring", action="append", default=[], metavar="USER_ID")
    p.add_argument("--no-transcribe", action="store_true")
    args = p.parse_args(argv)
    return _join_foreground_channel(
        args.channel_id,
        args.guild_id or None,
        args.label,
        self_mute=not args.unmuted,
        self_deaf=bool(args.deafened and not args.undeafened),
        ring_recipient_ids=args.ring,
        transcribe=not args.no_transcribe,
    )


def _configured_notify_targets():
    parent = os.environ.get("EXOCORTEX_PARENT_CONV_ID", "").strip()
    if parent:
        return [parent]
    try:
        from src.notify import get_relay_targets
        return list(get_relay_targets())
    except Exception:
        return []


def _normalize_notify_targets(targets):
    seen = set()
    result = []
    for target in targets or []:
        target = str(target).strip()
        if target and target not in seen:
            result.append(target)
            seen.add(target)
    return result


def _spawn_detached_call(channel_id, guild_id, label, *, self_mute=True, self_deaf=False, notify_targets=None, ring_recipient_ids=None, transcribe=True):
    paths = _call_paths(channel_id)
    existing = _read_call_meta(paths["meta"])
    if existing:
        print(f"Already joining {existing.get('label') or label} detached (pid {existing.get('pid')}).")
        print(f"Log: {existing.get('log')}")
        return

    log_file = paths["log"]
    log = open(log_file, "a", buffering=1)
    notify_targets = _normalize_notify_targets(notify_targets)
    ring_recipient_ids = [str(user_id) for user_id in (ring_recipient_ids or []) if user_id]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env[CALL_META_ENV] = str(paths["meta"])
    env[CALL_NOTIFY_TARGETS_ENV] = ",".join(notify_targets)

    cmd = [
        sys.executable,
        "-m", "src.calling",
        "__join_foreground",
        str(channel_id),
        str(guild_id or ""),
        str(label),
    ]
    if not self_mute:
        cmd.append("--unmuted")
    if self_deaf:
        cmd.append("--deafened")
    if not transcribe:
        cmd.append("--no-transcribe")
    for user_id in ring_recipient_ids:
        cmd.extend(["--ring", user_id])

    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_DIR),
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env=env,
    )
    log.close()

    meta = {
        "pid": proc.pid,
        "channel_id": str(channel_id),
        "guild_id": str(guild_id) if guild_id else None,
        "label": label,
        "status": "starting",
        "self_mute": self_mute,
        "self_deaf": self_deaf,
        "transcribe": bool(transcribe and not self_deaf),
        "control_seq": 0,
        "started_at": time.time(),
        "updated_at": time.time(),
        "log": str(log_file),
        "notify_targets": notify_targets,
        "ring_recipient_ids": ring_recipient_ids,
    }
    _write_call_meta(paths["meta"], meta)

    time.sleep(0.35)
    if proc.poll() is not None:
        _read_call_meta(paths["meta"])
        print(f"Failed to start detached call join for {label} (exit {proc.returncode}).")
        print(f"Log: {log_file}")
        try:
            tail = log_file.read_text(errors="replace").splitlines()[-8:]
            if tail:
                print("Last log lines:")
                for line in tail:
                    print(f"  {line}")
        except Exception:
            pass
        raise SystemExit(proc.returncode or 1)

    print(f"Started detached Discord call join for {label} (pid {proc.pid}).")
    print(f"Log: {log_file}")
    print("Use `discord call leave` to leave, or `discord call join --foreground ...` to run in the foreground.")


def join(argv):
    p = argparse.ArgumentParser(
        prog="discord call join",
        description="Join a DM/group/server voice call muted and undeafened by default. Detaches by default; use --foreground to block.",
    )
    p.add_argument("target", help="DM/group name, channel ID, or voice channel name with --guild")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name/ID for a voice channel")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    p.add_argument("--unmuted", action="store_true", help="Join with Discord self-mute off (no audio is still sent)")
    p.add_argument("--deafened", action="store_true", help="Join with Discord self-deaf on")
    p.add_argument("--undeafened", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--no-transcribe", action="store_true", help="Join without receiving/transcribing call audio")
    p.add_argument("--foreground", action="store_true", help="Run in the foreground until Ctrl+C instead of detaching")
    p.add_argument("--detach", "--background", action="store_true", help="Detach and return immediately (default)")
    p.add_argument("--notify-parent", metavar="CONV_ID", action="append", help="Relay call activity to an Exocortex conversation; defaults to EXOCORTEX_PARENT_CONV_ID")
    p.add_argument("--no-notify", action="store_true", help="Disable detached call activity notifications")
    args = p.parse_args(argv)

    channel_id, guild_id, label = _resolve_call_target(args)
    self_mute = not args.unmuted
    self_deaf = bool(args.deafened and not args.undeafened)
    notify_targets = [] if args.no_notify else _normalize_notify_targets(args.notify_parent or _configured_notify_targets())
    transcribe = not args.no_transcribe
    if args.foreground:
        if notify_targets:
            os.environ[CALL_NOTIFY_TARGETS_ENV] = ",".join(notify_targets)
        return _join_foreground_channel(channel_id, guild_id, label, self_mute=self_mute, self_deaf=self_deaf, transcribe=transcribe)
    return _spawn_detached_call(channel_id, guild_id, label, self_mute=self_mute, self_deaf=self_deaf, notify_targets=notify_targets, transcribe=transcribe)


def start(argv):
    p = argparse.ArgumentParser(
        prog="discord call start",
        description="Start/ring a DM or group DM call muted and undeafened by default. Detaches by default; use --foreground to block.",
    )
    p.add_argument("target", help="DM/group name or channel ID")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    p.add_argument("--unmuted", action="store_true", help="Join with Discord self-mute off (no audio is still sent)")
    p.add_argument("--deafened", action="store_true", help="Join with Discord self-deaf on")
    p.add_argument("--undeafened", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--no-transcribe", action="store_true", help="Join without receiving/transcribing call audio")
    p.add_argument("--foreground", action="store_true", help="Run in the foreground until Ctrl+C instead of detaching")
    p.add_argument("--detach", "--background", action="store_true", help="Detach and return immediately (default)")
    p.add_argument("--notify-parent", metavar="CONV_ID", action="append", help="Relay call activity to an Exocortex conversation; defaults to EXOCORTEX_PARENT_CONV_ID")
    p.add_argument("--no-notify", action="store_true", help="Disable detached call activity notifications")
    args = p.parse_args(argv)

    channel = _resolve_call_channel(args)
    recipient_ids = _recipient_ids_for_private_call(channel)
    if not recipient_ids:
        raise SystemExit("No recipients to ring for this DM/group DM.")
    channel_id = channel["id"]
    guild_id = None
    label = private_channel_label_for_type(private_channel_type(channel), private_channel_name(channel))
    self_mute = not args.unmuted
    self_deaf = bool(args.deafened and not args.undeafened)
    notify_targets = [] if args.no_notify else _normalize_notify_targets(args.notify_parent or _configured_notify_targets())
    transcribe = not args.no_transcribe
    if args.foreground:
        if notify_targets:
            os.environ[CALL_NOTIFY_TARGETS_ENV] = ",".join(notify_targets)
        return _join_foreground_channel(channel_id, guild_id, label, self_mute=self_mute, self_deaf=self_deaf, ring_recipient_ids=recipient_ids, transcribe=transcribe)
    return _spawn_detached_call(channel_id, guild_id, label, self_mute=self_mute, self_deaf=self_deaf, notify_targets=notify_targets, ring_recipient_ids=recipient_ids, transcribe=transcribe)


def list_calls(argv):
    p = argparse.ArgumentParser(prog="discord call list", description="List detached Discord call sessions.")
    p.parse_args(argv)
    metas = _running_call_metas()
    if not metas:
        print("No detached Discord call sessions.")
        return
    for meta in metas:
        status = meta.get("status") or "running"
        mute = "muted" if meta.get("self_mute", True) else "unmuted"
        deaf = "deafened" if meta.get("self_deaf", True) else "undeafened"
        notify = meta.get("notify_targets") or []
        notify_text = f"  notify: {', '.join(notify)}" if notify else "  notify: off"
        transcribe = "transcribe:on" if meta.get("transcribe", True) and not meta.get("self_deaf", False) else "transcribe:off"
        print(f"{meta.get('channel_id')}  pid {meta.get('pid')}  {status}  {mute}/{deaf}  {transcribe}  {meta.get('label')}")
        print(f"  log: {meta.get('log')}")
        print(notify_text)


_STATE_WORDS = {
    "on": True,
    "true": True,
    "yes": True,
    "1": True,
    "off": False,
    "false": False,
    "no": False,
    "0": False,
    "toggle": None,
}


def _parse_call_voice_state_args(prog, argv, *, default_value=None):
    p = argparse.ArgumentParser(prog=prog)
    p.add_argument("args", nargs="*", help="optional target and on/off/toggle state")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name/ID for resolving a voice channel target")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    p.add_argument("--all", action="store_true", help="Apply to all detached calls")
    parsed = p.parse_args(argv)

    target = None
    value = default_value
    for token in parsed.args:
        lower = token.lower()
        if lower in _STATE_WORDS and value == default_value:
            value = _STATE_WORDS[lower]
        elif target is None:
            target = token
        elif lower in _STATE_WORDS:
            value = _STATE_WORDS[lower]
        else:
            p.error(f"unexpected argument: {token}")
    parsed.target = target
    parsed.value = value
    return parsed


def _target_call_metas(args):
    metas = _running_call_metas()
    if args.all or not args.target:
        return metas
    target = str(args.target)
    direct = [m for m in metas if str(m.get("channel_id")) == target or str(m.get("label") or "") == target]
    if direct:
        return direct
    channel_id, _guild_id, _label = _resolve_call_target(args)
    return [m for m in metas if str(m.get("channel_id")) == str(channel_id)]


def _bump_control_seq(current):
    try:
        current["control_seq"] = int(current.get("control_seq") or 0) + 1
    except (TypeError, ValueError):
        current["control_seq"] = 1
    current["updated_at"] = time.time()


def _control_call_voice_state(argv, *, field, label, default_value=None):
    args = _parse_call_voice_state_args(f"discord call {label}", argv, default_value=default_value)
    targets = _target_call_metas(args)
    if not targets:
        print("No matching detached Discord call sessions.")
        return

    for meta in targets:
        channel_id = meta.get("channel_id")
        paths = _call_paths(channel_id)
        current = _read_call_meta(paths["meta"]) or meta
        old_value = bool(current.get(field, True))
        next_value = (not old_value) if args.value is None else bool(args.value)
        current[field] = next_value
        if field == "self_deaf" and next_value:
            current["transcribe"] = False
        elif field == "self_deaf" and not next_value:
            current["transcribe"] = True
        _bump_control_seq(current)
        _write_call_meta(paths["meta"], current)
        mute = "muted" if current.get("self_mute", True) else "unmuted"
        deaf = "deafened" if current.get("self_deaf", True) else "undeafened"
        transcribe = "transcribe:on" if current.get("transcribe", True) and not current.get("self_deaf", False) else "transcribe:off"
        print(f"Set {current.get('label') or channel_id} to {mute}/{deaf} {transcribe} (pid {current.get('pid')}).")


def _control_call_transcription(argv, *, default_value=None):
    args = _parse_call_voice_state_args("discord call transcribe", argv, default_value=default_value)
    targets = _target_call_metas(args)
    if not targets:
        print("No matching detached Discord call sessions.")
        return

    for meta in targets:
        channel_id = meta.get("channel_id")
        paths = _call_paths(channel_id)
        current = _read_call_meta(paths["meta"]) or meta
        old_value = bool(current.get("transcribe", True)) and not bool(current.get("self_deaf", False))
        next_value = (not old_value) if args.value is None else bool(args.value)
        current["transcribe"] = bool(next_value)
        if next_value:
            current["self_deaf"] = False
        _bump_control_seq(current)
        _write_call_meta(paths["meta"], current)
        mute = "muted" if current.get("self_mute", True) else "unmuted"
        deaf = "deafened" if current.get("self_deaf", True) else "undeafened"
        transcribe = "transcribe:on" if current.get("transcribe", True) and not current.get("self_deaf", False) else "transcribe:off"
        print(f"Set {current.get('label') or channel_id} to {mute}/{deaf} {transcribe} (pid {current.get('pid')}).")


def _terminate_call_meta(meta, *, timeout=5):
    pid = int(meta["pid"])
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    time.sleep(0.2)
    return not _pid_alive(pid)


def leave(argv):
    p = argparse.ArgumentParser(prog="discord call leave", description="Leave detached Discord call sessions.")
    p.add_argument("target", nargs="?", help="channel ID / DM / voice channel to leave; omit with --all to leave every detached call")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name/ID for resolving a voice channel target")
    p.add_argument("--dm", action="store_true", help="Resolve target as a DM/group DM")
    p.add_argument("--all", action="store_true", help="Leave all detached calls")
    args = p.parse_args(argv)

    metas = _running_call_metas()
    if args.all or not args.target:
        targets = metas
    else:
        channel_id, _guild_id, _label = _resolve_call_target(args)
        targets = [m for m in metas if str(m.get("channel_id")) == str(channel_id)]

    if not targets:
        print("No matching detached Discord call sessions.")
        return

    for meta in targets:
        ok = _terminate_call_meta(meta)
        paths = _call_paths(meta.get("channel_id"))
        paths["meta"].unlink(missing_ok=True)
        if ok:
            print(f"Left {meta.get('label') or meta.get('channel_id')} (pid {meta.get('pid')}).")
        else:
            print(f"Failed to stop {meta.get('label') or meta.get('channel_id')} (pid {meta.get('pid')}).")


def dispatch(cmd, argv):
    if cmd in {"call", "voice"}:
        if not argv or argv[0] in {"-h", "--help", "help"}:
            print("usage: discord call <start|join|leave|mute|unmute|deafen|undeafen|transcribe|list> ...")
            print("  start <dm> [--dm] [--foreground] [--unmuted] [--deafened] [--no-transcribe] [--notify-parent CONV_ID|--no-notify]")
            print("  join <target> [--dm|-g SERVER] [--foreground] [--unmuted] [--deafened] [--no-transcribe] [--notify-parent CONV_ID|--no-notify]")
            print("  mute [target] [on|off|toggle] [--all]")
            print("  unmute [target] [--all]")
            print("  deafen [target] [on|off|toggle] [--all]        # also disables transcription")
            print("  undeafen [target] [--all]                      # also enables transcription")
            print("  transcribe [target] [on|off|toggle] [--all]")
            print("  leave [target|--all]")
            print("  list")
            return
        subcmd, rest = argv[0], argv[1:]
        if subcmd == "join":
            return join(rest)
        if subcmd in {"start", "call", "ring"}:
            return start(rest)
        if subcmd in {"leave", "stop", "hangup"}:
            return leave(rest)
        if subcmd in {"mute", "muted"}:
            return _control_call_voice_state(rest, field="self_mute", label="mute")
        if subcmd in {"unmute", "unmuted"}:
            return _control_call_voice_state(rest, field="self_mute", label="unmute", default_value=False)
        if subcmd in {"deafen", "deaf", "deafened"}:
            return _control_call_voice_state(rest, field="self_deaf", label="deafen")
        if subcmd in {"undeafen", "undeaf", "undeafened"}:
            return _control_call_voice_state(rest, field="self_deaf", label="undeafen", default_value=False)
        if subcmd in {"transcribe", "listen", "listening"}:
            return _control_call_transcription(rest)
        if subcmd in {"no-transcribe", "unlisten"}:
            return _control_call_transcription(rest, default_value=False)
        if subcmd in {"list", "ls", "status"}:
            return list_calls(rest)
        raise SystemExit(f"discord call: unknown subcommand '{subcmd}'")
    if cmd == "join-call":
        return join(argv)
    raise SystemExit(f"discord: unknown call command '{cmd}'")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "__join_foreground":
        return _join_child(argv[1:])
    return dispatch("call", argv)


if __name__ == "__main__":
    main()
