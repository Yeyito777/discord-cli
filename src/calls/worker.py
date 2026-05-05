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
from src.auth import get_token
from src.calls.receive import VoiceReceiveTranscription
from src.calls.send import send_audio_file
from src.calls.state import CALL_META_ENV, CALL_NOTIFY_TARGETS_ENV, update_call_meta_env as _update_call_meta_env, write_call_meta as _write_call_meta
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
VOICE_GATEWAY_RECOVERABLE_CLOSE_CODES = {4006, 4009, 4015}
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


def _timing_delta_ms(start, end=None):
    try:
        start = float(start)
        end = time.time() if end is None else float(end)
    except (TypeError, ValueError):
        return "n/a"
    if start <= 0 or end <= 0:
        return "n/a"
    return f"{max(0, int(round((end - start) * 1000)))}ms"


class NoAudioCallJoiner:
    def __init__(self, channel_id, *, guild_id=None, label=None, self_mute=True, self_deaf=False, ring_recipient_ids=None, transcribe=True, save_audio=False, audio_dir=None, notify_audio_state=False):
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.label = label or channel_id
        self.self_mute = self_mute
        self.self_deaf = self_deaf
        self.transcribe_enabled = bool(transcribe and not self_deaf)
        self.save_audio = bool(save_audio)
        self.audio_dir = str(audio_dir) if audio_dir else None
        self.notify_audio_state = bool(notify_audio_state)
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
        self._say_ids_seen = set()
        self._say_lock = threading.Lock()
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
                f"({'transcribing' if self.transcribe_enabled else 'not transcribing'}"
                f"{', saving audio' if self.save_audio else ''})…",
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

            while self.running:
                self._pump_app_gateway_once()
                self._poll_control()
                self._maybe_connect_voice_gateway()
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
        self._poll_say_queue(meta)
        if changed:
            self._request_voice_state(self.channel_id)
            _update_call_meta_env(status="joined" if self.voice_ready else "joining", updated_at=time.time())
            print(f"Voice state: {'muted' if self.self_mute else 'unmuted'}/{'deafened' if self.self_deaf else 'undeafened'}", flush=True)

    def _poll_say_queue(self, meta):
        queue_items = meta.get("say_queue")
        if not isinstance(queue_items, list) or not queue_items:
            return
        meta_path = os.environ.get(CALL_META_ENV)
        pending = []
        for item in queue_items:
            if not isinstance(item, dict):
                continue
            request_id = str(item.get("id") or "")
            path = str(item.get("path") or "")
            if not request_id or request_id in self._say_ids_seen or not path:
                continue
            self._say_ids_seen.add(request_id)
            pending.append((request_id, path))
        if meta_path:
            try:
                current = json.loads(Path(meta_path).read_text())
                current["say_queue"] = []
                current["updated_at"] = time.time()
                _write_call_meta(Path(meta_path), current)
            except Exception:
                pass
        for request_id, path in pending:
            threading.Thread(target=self._send_audio_file, daemon=True, args=(request_id, path)).start()

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
        if changes and self.notify_audio_state:
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
            self._sync_transcription_participants()
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
        self._sync_transcription_participants()

    def _sync_transcription_participants(self):
        if self._voice_transcription:
            self._voice_transcription.set_active_remote_users(self._active_participant_ids)

    def _notify_call_event(self, message):
        self._notify_exo(message, prefix="Discord Call")

    def _notify_voice_transcript(self, message, prefix="Discord Voice", timing=None):
        self._notify_exo(message, prefix=prefix, timing=timing)

    def _notify_exo(self, message, *, prefix, timing=None):
        targets = [target for target in os.environ.get(CALL_NOTIFY_TARGETS_ENV, "").split(",") if target]
        if not targets:
            return
        print(message, flush=True)
        for target in targets:
            threading.Thread(target=self._send_notification, args=(target, prefix, message, timing), daemon=True).start()

    def _send_notification(self, target, prefix, message, timing=None):
        send_started_at = time.time()
        try:
            proc = subprocess.run(
                ["exo", "send", f"[{prefix}] {message}", "-c", target, "--timeout", "600", "--no-notify"],
                capture_output=True,
                text=True,
                timeout=660,
            )
            send_finished_at = time.time()
            if timing and prefix == "Discord Voice":
                self._log_voice_transcription(
                    "notification timing: "
                    f"target={target} returncode={proc.returncode} "
                    f"speech_start_to_exo_return={_timing_delta_ms(timing.get('speech_started_at'), send_finished_at)} "
                    f"speech_end_to_exo_return={_timing_delta_ms(timing.get('speech_ended_at'), send_finished_at)} "
                    f"transcript_ready_to_exo_return={_timing_delta_ms(timing.get('transcript_ready_at'), send_finished_at)} "
                    f"exo_send={_timing_delta_ms(send_started_at, send_finished_at)}"
                )
        except Exception as exc:
            if timing and prefix == "Discord Voice":
                self._log_voice_transcription(f"notification timing failed: target={target} error={exc}")

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
            self._voice_reconnect_attempts = 0
            _update_call_meta_env(status="joined", updated_at=time.time())
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
            self._recover_voice_gateway("invalidated the session")
        elif self._voice_transcription:
            self._voice_transcription.handle_json_opcode(op, payload.get("d"))

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
        self._reset_voice_gateway_state(stop_transcription=True)
        if attempt % VOICE_GATEWAY_APP_RECONNECT_EVERY == 0:
            try:
                self._reconnect_app_gateway(f"refreshing after voice {reason}")
            except Exception as exc:
                print(f"Discord gateway refresh after voice reconnect failed: {exc}", flush=True)
        elif self.app_ws and self.channel_id:
            self._request_voice_disconnect()
            self._request_voice_state(self.channel_id)
        time.sleep(min(VOICE_GATEWAY_RECONNECT_DELAY * attempt, VOICE_GATEWAY_RECONNECT_MAX_DELAY))

    def _reset_voice_gateway_state(self, *, stop_transcription: bool):
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
        if stop_transcription and self._voice_transcription:
            try:
                self._voice_transcription.stop()
            except Exception:
                pass
            self._voice_transcription = None
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
        mode = select_encryption_mode(modes)
        self.voice_ssrc = int(ssrc)
        udp, address, discovered_port = udp_discovery(ip, int(port), int(ssrc))
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
            keep_audio=self.save_audio,
            audio_dir=self.audio_dir,
        )
        self._voice_transcription.set_enabled(self.transcribe_enabled)
        self._sync_transcription_participants()
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
        self.voice_secret_key = bytes(secret_key)
        self.voice_mode = str(mode)
        transcription = self._ensure_voice_transcription_object()
        if not transcription:
            return
        transcription.configure_media(udp=self.voice_udp, mode=str(mode), secret_key=bytes(secret_key))
        transcription.set_self_ssrc(self.voice_ssrc)
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

    # ─── Outgoing one-shot audio ────────────────────────────────────────────────

    def _send_audio_file(self, request_id, path):
        with self._say_lock:
            path = str(path)
            try:
                send_audio_file(self, path)
                print(f"Finished call audio send: {path}", flush=True)
            except Exception as exc:
                print(f"Failed to send call audio {path}: {exc}", flush=True)

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
        if self._voice_transcription:
            try:
                self._voice_transcription.stop()
            except Exception:
                pass
            self._voice_transcription = None
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
