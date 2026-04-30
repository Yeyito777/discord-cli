"""Receive Discord voice audio and asynchronously transcribe speaker segments.

This module deliberately does not implement speech-to-text itself.  It receives
Discord RTP/Opus media, segments decoded PCM by speaker using a simple RMS
threshold, writes finalized WAV files, and delegates ASR to the external
`transcribe` CLI.
"""

from __future__ import annotations

from collections import deque
import math
import os
from pathlib import Path
import queue
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import time
import wave

try:  # Optional runtime dependencies; callers can degrade cleanly.
    import av  # type: ignore
except Exception:  # pragma: no cover - depends on deployment venv
    av = None

try:
    import davey  # type: ignore
except Exception:  # pragma: no cover - depends on deployment venv
    davey = None

try:
    import nacl.bindings  # type: ignore
except Exception:  # pragma: no cover - depends on deployment venv
    nacl = None

OPUS_PAYLOAD_TYPE = 120
RTP_HEADER_LENGTH = 12
TRANSCRIBE_WORKERS = 2
DEFAULT_SPEECH_THRESHOLD_DB = -42.0
DEFAULT_SILENCE_MS = 900
DEFAULT_MIN_SPEECH_MS = 450
DEFAULT_MAX_SEGMENT_MS = 18_000
DEFAULT_PRE_ROLL_MS = 250
DEFAULT_MAX_QUEUE = 24


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def db_to_linear(db: float) -> float:
    return 10 ** (db / 20.0)


def format_ms(seconds: float | None) -> str:
    if not isinstance(seconds, (int, float)) or not math.isfinite(seconds):
        return "n/a"
    return f"{seconds * 1000:.0f}ms"


def elapsed_since(start: float | None, end: float | None = None) -> float | None:
    if not isinstance(start, (int, float)) or start <= 0:
        return None
    if end is None:
        end = time.time()
    if not isinstance(end, (int, float)) or end <= 0:
        return None
    return max(0.0, end - start)


def pcm16_rms(samples: bytes) -> float:
    if len(samples) < 2:
        return 0.0
    usable = len(samples) - (len(samples) % 2)
    if usable <= 0:
        return 0.0
    count = usable // 2
    total = 0.0
    for (sample,) in struct.iter_unpack("<h", samples[:usable]):
        value = sample / 32768.0
        total += value * value
    return math.sqrt(total / count) if count else 0.0


def parse_rtp_packet(packet: bytes):
    if len(packet) < RTP_HEADER_LENGTH or packet[0] >> 6 != 2:
        return None
    csrc_count = packet[0] & 0x0F
    has_extension = bool(packet[0] & 0x10)
    header_length = RTP_HEADER_LENGTH + csrc_count * 4
    if len(packet) < header_length:
        return None
    extension_body_length = 0
    if has_extension:
        if len(packet) < header_length + 4:
            return None
        # In Discord rtpsize AEAD packets, the 4-byte RTP extension prelude is
        # authenticated with the header, but the extension body is encrypted and
        # must be stripped after transport decryption.
        extension_body_length = int.from_bytes(packet[header_length + 2:header_length + 4], "big") * 4
        header_length += 4
    payload_type = packet[1] & 0x7F
    if len(packet) <= header_length + 4 + 16:
        return None
    return {
        "payload_type": payload_type,
        "sequence": int.from_bytes(packet[2:4], "big"),
        "timestamp": int.from_bytes(packet[4:8], "big"),
        "ssrc": int.from_bytes(packet[8:12], "big"),
        "header_length": header_length,
        "extension_body_length": extension_body_length,
    }


def decrypt_transport(packet: bytes, parsed: dict, mode: str, secret_key: bytes):
    header_length = parsed["header_length"]
    header = packet[:header_length]
    encrypted = packet[header_length:-4]
    counter = packet[-4:]
    try:
        if mode == "aead_aes256_gcm_rtpsize":
            if nacl is None:
                return None
            nonce = counter + (b"\x00" * 8)
            return nacl.bindings.crypto_aead_aes256gcm_decrypt(encrypted, header, nonce, secret_key)
        if mode == "aead_xchacha20_poly1305_rtpsize":
            if nacl is None:
                return None
            nonce = counter + (b"\x00" * 20)
            return nacl.bindings.crypto_aead_xchacha20poly1305_ietf_decrypt(encrypted, header, nonce, secret_key)
    except Exception:
        return None
    return None


class DavePassthroughDecryptor:
    """Small wrapper around davey.DaveSession matching Record's behavior."""

    def __init__(self, *, user_id: str, channel_id: str, send_json, send_binary, on_error=None):
        self.user_id = str(user_id)
        self.channel_id = str(channel_id)
        self.send_json = send_json
        self.send_binary = send_binary
        self.on_error = on_error
        self.session = None
        self.protocol_version = 0
        self.pending_transitions = {}
        self.known_user_ids = {self.user_id}
        self.external_sender = None
        self.downgraded = False
        self.reinitializing = False
        self.last_transition_id = None
        self.passthrough_recovery_enabled = False

    @property
    def advertised_protocol_version(self):
        return getattr(davey, "DAVE_PROTOCOL_VERSION", 1) if davey is not None else 0

    def handle_session_description(self, data: dict):
        self.protocol_version = int(data.get("dave_protocol_version") or 0)
        self.reinit()

    def add_known_users(self, user_ids):
        for user_id in user_ids or []:
            if user_id is not None:
                self.known_user_ids.add(str(user_id))

    def remove_known_user(self, user_id):
        self.known_user_ids.discard(str(user_id))

    def handle_json_opcode(self, opcode, data):
        if opcode == 21:
            self.handle_prepare_transition(data or {})
            return True
        if opcode == 22:
            transition_id = (data or {}).get("transition_id") if isinstance(data, dict) else None
            if transition_id is not None:
                self.execute_pending_transition(int(transition_id))
            return True
        if opcode == 24:
            self.handle_prepare_epoch(data or {})
            return True
        return False

    def handle_binary_opcode(self, opcode, payload: bytes):
        if opcode == 25:
            self.external_sender = bytes(payload)
            self.apply_external_sender()
            return True
        if opcode == 27:
            self.handle_proposals(payload)
            return True
        if opcode == 29:
            self.handle_announce_commit_transition(payload)
            return True
        if opcode == 30:
            self.handle_welcome(payload)
            return True
        return False

    def decode_incoming_opus(self, user_id, payload: bytes):
        if payload == b"\xf8\xff\xfe":
            return payload
        if self.protocol_version == 0:
            return payload
        if self.session is None or user_id is None:
            return None
        try:
            can_passthrough = bool(self.session.can_passthrough(int(user_id)))
        except Exception:
            can_passthrough = False
        if not (bool(getattr(self.session, "ready", False)) or can_passthrough):
            return None
        try:
            return self.session.decrypt(int(user_id), davey.MediaType.audio, payload)
        except Exception as exc:
            # Mixed encrypted/unencrypted transition windows are common during DAVE
            # setup. Match Record's passthrough recovery behavior.
            if "UnencryptedWhenPassthroughDisabled" in repr(exc) or "Unencrypted" in repr(exc):
                self.enable_passthrough_recovery()
                return payload
            self.report_error(f"DAVE decrypt failed for {user_id}: {exc}")
            return None

    def handle_prepare_transition(self, data: dict):
        if not isinstance(data, dict):
            return
        transition_id = data.get("transition_id")
        protocol_version = data.get("protocol_version", data.get("dave_protocol_version"))
        if transition_id is None or protocol_version is None:
            return
        transition_id = int(transition_id)
        protocol_version = int(protocol_version)
        self.pending_transitions[transition_id] = protocol_version
        if transition_id == 0:
            self.execute_pending_transition(transition_id)
            return
        if protocol_version == 0 and self.session is not None:
            self._set_passthrough(True, 30)
        self.send_json({"op": 23, "d": {"transition_id": transition_id}})

    def handle_prepare_epoch(self, data: dict):
        if not isinstance(data, dict):
            return
        if int(data.get("epoch") or 0) != 1:
            return
        protocol_version = data.get("protocol_version", data.get("dave_protocol_version"))
        if protocol_version is None:
            return
        self.protocol_version = int(protocol_version)
        self.reinit()

    def handle_proposals(self, payload: bytes):
        if self.session is None or not payload:
            return
        try:
            op_byte = payload[0]
            operation_type = davey.ProposalsOperationType.append if op_byte == 0 else davey.ProposalsOperationType.revoke
            result = self.session.process_proposals(operation_type, payload[1:], [int(u) for u in self.known_user_ids])
            commit = getattr(result, "commit", None) if result is not None else None
            if not commit:
                return
            welcome = getattr(result, "welcome", None)
            self.send_binary(28, bytes(commit) + (bytes(welcome) if welcome else b""))
        except Exception as exc:
            self.report_error(f"DAVE proposals failed: {exc}")
            self.recover_from_invalid_transition(self.last_transition_id)

    def handle_announce_commit_transition(self, payload: bytes):
        if self.session is None or len(payload) < 2:
            return
        transition_id = int.from_bytes(payload[:2], "big")
        try:
            self.session.process_commit(payload[2:])
            self.finish_commit_or_welcome_transition(transition_id)
        except Exception as exc:
            self.report_error(f"DAVE commit failed: {exc}")
            self.recover_from_invalid_transition(transition_id)

    def handle_welcome(self, payload: bytes):
        if self.session is None or len(payload) < 2:
            return
        transition_id = int.from_bytes(payload[:2], "big")
        try:
            self.session.process_welcome(payload[2:])
            self.finish_commit_or_welcome_transition(transition_id)
        except Exception as exc:
            self.report_error(f"DAVE welcome failed: {exc}")
            self.recover_from_invalid_transition(transition_id)

    def finish_commit_or_welcome_transition(self, transition_id: int):
        if transition_id == 0:
            self.last_transition_id = 0
            self.reinitializing = False
            return
        self.pending_transitions[transition_id] = self.protocol_version
        self.send_json({"op": 23, "d": {"transition_id": transition_id}})

    def execute_pending_transition(self, transition_id: int):
        next_version = self.pending_transitions.pop(transition_id, None)
        if next_version is None:
            return False
        self.protocol_version = int(next_version)
        if self.session is not None:
            if next_version == 0:
                self.downgraded = True
                self._set_passthrough(True, 10)
            elif self.downgraded and transition_id > 0:
                self.downgraded = False
                self._set_passthrough(True, 10)
        self.reinitializing = False
        self.last_transition_id = transition_id
        return True

    def reinit(self):
        if davey is None:
            return
        if self.protocol_version <= 0:
            if self.session is not None:
                self._set_passthrough(True, 10)
                try:
                    self.session.reset()
                except Exception:
                    pass
            return
        try:
            if self.session is None:
                self.session = davey.DaveSession(self.protocol_version, int(self.user_id), int(self.channel_id))
            else:
                self.session.reinit(self.protocol_version, int(self.user_id), int(self.channel_id))
            self.apply_external_sender()
            self.send_key_package()
        except Exception as exc:
            self.report_error(f"DAVE init failed: {exc}")

    def apply_external_sender(self):
        if self.session is None or self.external_sender is None:
            return
        try:
            self.session.set_external_sender(self.external_sender)
        except Exception as exc:
            self.report_error(f"DAVE external sender failed: {exc}")

    def send_key_package(self):
        if self.session is None or self.protocol_version <= 0:
            return
        try:
            self.send_binary(26, self.session.get_serialized_key_package())
        except Exception as exc:
            self.report_error(f"DAVE key package failed: {exc}")

    def recover_from_invalid_transition(self, transition_id):
        if transition_id is None or self.reinitializing:
            return
        self.reinitializing = True
        self.send_json({"op": 31, "d": {"transition_id": int(transition_id)}})
        self.reinit()

    def enable_passthrough_recovery(self):
        if self.passthrough_recovery_enabled:
            return
        self.passthrough_recovery_enabled = True
        self._set_passthrough(True, 120)

    def _set_passthrough(self, enabled: bool, expiry: int):
        if self.session is None:
            return
        try:
            self.session.set_passthrough_mode(enabled, expiry)
        except Exception:
            pass

    def report_error(self, message: str):
        if self.on_error:
            self.on_error(message)


class SpeakerSegmenter:
    def __init__(self, user_id: str, name_for_user, submit_segment, *, sample_rate=16000, channels=1):
        self.user_id = str(user_id)
        self.name_for_user = name_for_user
        self.submit_segment = submit_segment
        self.sample_rate = sample_rate
        self.channels = channels
        self.threshold = db_to_linear(env_float("DISCORD_CALL_TRANSCRIBE_THRESHOLD_DB", DEFAULT_SPEECH_THRESHOLD_DB))
        self.silence_seconds = env_int("DISCORD_CALL_TRANSCRIBE_SILENCE_MS", DEFAULT_SILENCE_MS) / 1000.0
        self.min_speech_seconds = env_int("DISCORD_CALL_TRANSCRIBE_MIN_SPEECH_MS", DEFAULT_MIN_SPEECH_MS) / 1000.0
        self.max_segment_seconds = env_int("DISCORD_CALL_TRANSCRIBE_MAX_SEGMENT_MS", DEFAULT_MAX_SEGMENT_MS) / 1000.0
        pre_roll_frames = max(0, int((env_int("DISCORD_CALL_TRANSCRIBE_PRE_ROLL_MS", DEFAULT_PRE_ROLL_MS) / 1000.0) * sample_rate))
        self.pre_roll = deque(maxlen=pre_roll_frames * channels * 2)
        self.active = False
        self.frames = bytearray()
        self.speech_seconds = 0.0
        self.silence_seconds_seen = 0.0
        self.segment_started_at = 0.0
        self.last_speech_at = 0.0
        self.max_rms = 0.0
        self.last_audio_at = time.time()

    def add_pcm(self, pcm: bytes, duration: float):
        now = time.time()
        self.last_audio_at = now
        rms = pcm16_rms(pcm)
        if rms > self.max_rms:
            self.max_rms = rms
        speaking = rms >= self.threshold
        if speaking:
            if not self.active:
                self.active = True
                self.frames = bytearray(self.pre_roll)
                self.speech_seconds = 0.0
                self.silence_seconds_seen = 0.0
                self.segment_started_at = now
                self.last_speech_at = now
                self.max_rms = rms
            self.last_speech_at = now
            self.speech_seconds += duration
            self.silence_seconds_seen = 0.0
        elif self.active:
            self.silence_seconds_seen += duration

        if self.active:
            self.frames.extend(pcm)
            current_len_seconds = len(self.frames) / (self.sample_rate * self.channels * 2)
            if (
                (self.silence_seconds_seen >= self.silence_seconds and self.speech_seconds >= self.min_speech_seconds)
                or current_len_seconds >= self.max_segment_seconds
            ):
                self.finalize()
        else:
            self.pre_roll.extend(pcm)

    def flush_if_stale(self, stale_after=2.5):
        if self.active and time.time() - self.last_audio_at >= stale_after:
            self.finalize()

    def finalize(self):
        if not self.active:
            return
        finalized_at = time.time()
        frames = bytes(self.frames)
        speech_seconds = self.speech_seconds
        max_rms = self.max_rms
        speech_started_at = self.segment_started_at
        speech_ended_at = self.last_speech_at or finalized_at
        duration_seconds = len(frames) / (self.sample_rate * self.channels * 2) if frames else 0.0
        self.active = False
        self.frames = bytearray()
        self.speech_seconds = 0.0
        self.silence_seconds_seen = 0.0
        self.segment_started_at = 0.0
        self.last_speech_at = 0.0
        self.max_rms = 0.0
        self.pre_roll.clear()
        if speech_seconds < self.min_speech_seconds:
            return
        self.submit_segment(self.user_id, self.name_for_user(self.user_id), frames, self.sample_rate, self.channels, {
            "duration_seconds": duration_seconds,
            "speech_seconds": speech_seconds,
            "speech_started_at": speech_started_at,
            "speech_ended_at": speech_ended_at,
            "finalized_at": finalized_at,
            "max_db": 20 * math.log10(max_rms) if max_rms > 0 else -math.inf,
        })


class VoiceTranscriber:
    def __init__(self, *, label: str, notify, log=print):
        self.label = label
        self.notify = notify
        self.log = log
        self.enabled = True
        self.queue = queue.Queue(maxsize=env_int("DISCORD_CALL_TRANSCRIBE_QUEUE", DEFAULT_MAX_QUEUE))
        self.running = True
        self.keep_audio = os.environ.get("DISCORD_CALL_TRANSCRIBE_KEEP_AUDIO") == "1"
        self.tmpdir = Path(tempfile.mkdtemp(prefix="discord-call-transcribe-"))
        self.workers = []
        for index in range(TRANSCRIBE_WORKERS):
            thread = threading.Thread(target=self._worker, name=f"discord-transcribe-{index}", daemon=True)
            thread.start()
            self.workers.append(thread)

    def set_enabled(self, enabled: bool):
        self.enabled = bool(enabled)

    def submit(self, user_id: str, name: str, pcm: bytes, sample_rate: int, channels: int, stats=None):
        if not self.enabled or not pcm:
            return
        stats = stats or {}
        queued_at = time.time()
        stats["queued_at"] = queued_at
        max_db = stats.get("max_db")
        duration = stats.get("duration_seconds")
        queue_wait = elapsed_since(stats.get("finalized_at"), queued_at)
        speech_to_queue = elapsed_since(stats.get("speech_started_at"), queued_at)
        self.log(
            f"queue transcription for {name or user_id}: "
            f"duration={duration:.2f}s max_db={max_db:.1f} "
            f"speech_to_queue={format_ms(speech_to_queue)} finalize_to_queue={format_ms(queue_wait)}"
            if isinstance(duration, (int, float)) and isinstance(max_db, (int, float)) and math.isfinite(max_db)
            else f"queue transcription for {name or user_id}: speech_to_queue={format_ms(speech_to_queue)}"
        )
        try:
            self.queue.put_nowait({
                "user_id": str(user_id),
                "name": name or str(user_id),
                "pcm": pcm,
                "sample_rate": sample_rate,
                "channels": channels,
                "stats": stats,
                "created_at": queued_at,
            })
        except queue.Full:
            self.log("Transcription queue full; dropping voice segment")

    def stop(self):
        self.running = False
        for _ in self.workers:
            try:
                self.queue.put_nowait(None)
            except queue.Full:
                pass
        for thread in self.workers:
            thread.join(timeout=1)
        if not self.keep_audio:
            try:
                shutil.rmtree(self.tmpdir, ignore_errors=True)
            except Exception:
                pass

    def _worker(self):
        while self.running:
            try:
                item = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                return
            try:
                self._transcribe_item(item)
            finally:
                self.queue.task_done()

    def _transcribe_item(self, item: dict):
        name = item["name"]
        stats = dict(item.get("stats") or {})
        worker_started_at = time.time()
        wav_path = self.tmpdir / f"segment-{int(time.time() * 1000)}-{os.getpid()}-{threading.get_ident()}.wav"
        with wave.open(str(wav_path), "wb") as wf:
            wf.setnchannels(int(item["channels"]))
            wf.setsampwidth(2)
            wf.setframerate(int(item["sample_rate"]))
            wf.writeframes(item["pcm"])
        transcribe_started_at = time.time()
        try:
            proc = subprocess.run(
                ["transcribe", str(wav_path), "--mime-type", "audio/wav"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            transcript_ready_at = time.time()
            timing = self._build_timing(stats, worker_started_at, transcribe_started_at, transcript_ready_at)
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or "transcribe failed").strip().splitlines()[-1:]
                self.log(f"transcribe failed for {name}: {err[0] if err else proc.returncode}; {self._format_timing(timing)}")
                return
            text = (proc.stdout or "").strip()
            if not text:
                self.log(f"transcribe empty for {name}; {self._format_timing(timing)}")
                return
            self.log(f"transcribe ready for {name}; {self._format_timing(timing)}")
            self.notify(f"🎙 {name}: {text}", prefix="Discord Voice", timing=timing)
        except subprocess.TimeoutExpired:
            timed_out_at = time.time()
            timing = self._build_timing(stats, worker_started_at, transcribe_started_at, timed_out_at)
            self.log(f"transcribe timed out for {name}; {self._format_timing(timing)}")
        except Exception as exc:
            failed_at = time.time()
            timing = self._build_timing(stats, worker_started_at, transcribe_started_at, failed_at)
            self.log(f"transcribe failed for {name}: {exc}; {self._format_timing(timing)}")
        finally:
            if not self.keep_audio:
                try:
                    wav_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def _build_timing(self, stats: dict, worker_started_at: float, transcribe_started_at: float, transcript_ready_at: float):
        speech_started_at = stats.get("speech_started_at")
        speech_ended_at = stats.get("speech_ended_at")
        finalized_at = stats.get("finalized_at")
        queued_at = stats.get("queued_at") or stats.get("created_at")
        return {
            "speech_started_at": speech_started_at,
            "speech_ended_at": speech_ended_at,
            "finalized_at": finalized_at,
            "queued_at": queued_at,
            "worker_started_at": worker_started_at,
            "transcribe_started_at": transcribe_started_at,
            "transcript_ready_at": transcript_ready_at,
            "speech_start_to_ready_ms": self._elapsed_ms(speech_started_at, transcript_ready_at),
            "speech_end_to_ready_ms": self._elapsed_ms(speech_ended_at, transcript_ready_at),
            "finalize_to_ready_ms": self._elapsed_ms(finalized_at, transcript_ready_at),
            "queue_wait_ms": self._elapsed_ms(queued_at, worker_started_at),
            "asr_ms": self._elapsed_ms(transcribe_started_at, transcript_ready_at),
        }

    def _format_timing(self, timing: dict) -> str:
        return (
            f"speech_start_to_ready={self._format_timing_ms(timing.get('speech_start_to_ready_ms'))} "
            f"speech_end_to_ready={self._format_timing_ms(timing.get('speech_end_to_ready_ms'))} "
            f"finalize_to_ready={self._format_timing_ms(timing.get('finalize_to_ready_ms'))} "
            f"queue_wait={self._format_timing_ms(timing.get('queue_wait_ms'))} "
            f"asr={self._format_timing_ms(timing.get('asr_ms'))}"
        )

    @staticmethod
    def _elapsed_ms(start, end) -> int | None:
        seconds = elapsed_since(start, end)
        return int(round(seconds * 1000)) if seconds is not None else None

    @staticmethod
    def _format_timing_ms(value) -> str:
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            return "n/a"
        return f"{int(round(value))}ms"


class VoiceReceiveTranscription:
    @staticmethod
    def advertised_dave_protocol_version_static():
        return getattr(davey, "DAVE_PROTOCOL_VERSION", 1) if davey is not None else 0

    def __init__(self, *, udp=None, mode: str | None = None, secret_key=None, self_user_id: str, channel_id: str, label: str, send_json, send_binary, notify, name_for_user, log=print):
        self.udp = udp
        self.mode = mode
        self.secret_key = bytes(secret_key or b"")
        self.self_user_id = str(self_user_id)
        self.channel_id = str(channel_id)
        self.label = label
        self.send_json = send_json
        self.send_binary = send_binary
        self.notify = notify
        self.name_for_user = name_for_user
        self.log = log
        self.running = False
        self.thread = None
        self.ssrc_to_user_id = {}
        self.segmenters = {}
        self.decoders = {}
        self.resamplers = {}
        self.packet_count = 0
        self.decrypt_count = 0
        self.decode_frame_count = 0
        self.decode_error_count = 0
        self.last_decode_error_log_at = 0.0
        self.last_stats_at = time.time()
        self.transcriber = VoiceTranscriber(label=label, notify=notify, log=log)
        self.dave = DavePassthroughDecryptor(
            user_id=self.self_user_id,
            channel_id=self.channel_id,
            send_json=send_json,
            send_binary=send_binary,
            on_error=log,
        )

    @property
    def advertised_dave_protocol_version(self):
        return self.dave.advertised_protocol_version

    def configure_media(self, *, udp, mode: str, secret_key):
        self.udp = udp
        self.mode = mode
        self.secret_key = bytes(secret_key or b"")

    def start(self):
        if self.running:
            return
        if not self.udp or not self.mode or not self.secret_key:
            self.log("Voice transcription waiting for Discord media session")
            return
        if av is None:
            self.log("Voice transcription disabled: PyAV is not installed")
            return
        if nacl is None:
            self.log("Voice transcription disabled: PyNaCl is not installed")
            return
        if not shutil.which("transcribe"):
            self.log("Voice transcription disabled: transcribe CLI is not in PATH")
            return
        self.running = True
        self.thread = threading.Thread(target=self._recv_loop, name="discord-voice-receive", daemon=True)
        self.thread.start()
        self.log("Voice transcription receiver started")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
            self.thread = None
        for segmenter in list(self.segmenters.values()):
            segmenter.finalize()
        self.transcriber.stop()

    def set_enabled(self, enabled: bool):
        self.transcriber.set_enabled(enabled)
        if not enabled:
            for segmenter in list(self.segmenters.values()):
                segmenter.finalize()

    def add_ssrc_mapping(self, ssrc, user_id):
        if ssrc is None or user_id is None:
            return
        ssrc = int(ssrc)
        user_id = str(user_id)
        previous = self.ssrc_to_user_id.get(ssrc)
        self.ssrc_to_user_id[ssrc] = user_id
        self.dave.add_known_users([user_id])
        if previous != user_id:
            self.log(f"Voice transcription mapped SSRC {ssrc} to {self.name_for_user(user_id)}")

    def remove_user(self, user_id):
        user_id = str(user_id)
        self.dave.remove_known_user(user_id)
        for ssrc, mapped_user in list(self.ssrc_to_user_id.items()):
            if mapped_user == user_id:
                del self.ssrc_to_user_id[ssrc]
        segmenter = self.segmenters.pop(user_id, None)
        if segmenter:
            segmenter.finalize()

    def handle_session_description(self, data: dict):
        self.dave.handle_session_description(data)

    def handle_json_opcode(self, opcode, data):
        return self.dave.handle_json_opcode(opcode, data)

    def handle_binary_opcode(self, opcode, payload: bytes):
        return self.dave.handle_binary_opcode(opcode, payload)

    def _recv_loop(self):
        while self.running:
            try:
                packet = self.udp.recv(4096)
            except SOCKET_TIMEOUT_EXCEPTIONS:
                self._flush_stale()
                continue
            except OSError:
                break
            except Exception as exc:
                self.log(f"Voice UDP receive error: {exc}")
                break
            self._handle_packet(packet)
            self._flush_stale()

    def _flush_stale(self):
        for segmenter in list(self.segmenters.values()):
            segmenter.flush_if_stale()
        now = time.time()
        if now - self.last_stats_at >= 10:
            self.last_stats_at = now
            if self.packet_count or self.decrypt_count or self.decode_frame_count or self.ssrc_to_user_id:
                self.log(
                    f"Voice transcription stats: packets={self.packet_count} decrypted={self.decrypt_count} "
                    f"frames={self.decode_frame_count} decode_errors={self.decode_error_count} "
                    f"speakers={len(self.segmenters)} ssrcs={len(self.ssrc_to_user_id)}"
                )

    def _handle_packet(self, packet: bytes):
        parsed = parse_rtp_packet(packet)
        if not parsed or parsed["payload_type"] != OPUS_PAYLOAD_TYPE:
            return
        self.packet_count += 1
        user_id = self.ssrc_to_user_id.get(parsed["ssrc"])
        if not user_id or user_id == self.self_user_id:
            return
        payload = decrypt_transport(packet, parsed, self.mode, self.secret_key)
        if not payload:
            return
        self.decrypt_count += 1
        ext_len = int(parsed.get("extension_body_length") or 0)
        if ext_len:
            payload = payload[ext_len:]
        if not payload:
            return
        payload = self.dave.decode_incoming_opus(user_id, payload)
        if not payload:
            return
        self._decode_and_segment(user_id, payload)

    def _decode_and_segment(self, user_id: str, opus_payload: bytes):
        decoder = self.decoders.get(user_id)
        if decoder is None:
            decoder = av.codec.CodecContext.create("opus", "r")
            self.decoders[user_id] = decoder
        try:
            frames = decoder.decode(av.packet.Packet(opus_payload))
        except Exception as exc:
            self.decode_error_count += 1
            now = time.time()
            if now - self.last_decode_error_log_at >= 5:
                self.last_decode_error_log_at = now
                self.log(f"Opus decode failed for {self.name_for_user(user_id)} ({self.decode_error_count} total): {exc}")
            return
        for frame in frames:
            self.decode_frame_count += 1
            for pcm, sample_rate, channels in self._frame_to_pcm16_mono_16k(user_id, frame):
                if not pcm:
                    continue
                duration = len(pcm) / (sample_rate * channels * 2)
                segmenter = self.segmenters.get(user_id)
                if segmenter is None:
                    segmenter = SpeakerSegmenter(user_id, self.name_for_user, self.transcriber.submit, sample_rate=sample_rate, channels=channels)
                    self.segmenters[user_id] = segmenter
                segmenter.add_pcm(pcm, duration)

    def _frame_to_pcm16_mono_16k(self, user_id: str, frame):
        resampler = self.resamplers.get(user_id)
        if resampler is None:
            resampler = av.audio.resampler.AudioResampler(format="s16", layout="mono", rate=16000)
            self.resamplers[user_id] = resampler
        try:
            frames = resampler.resample(frame)
        except Exception as exc:
            self.log(f"Audio resample failed for {self.name_for_user(user_id)}: {exc}")
            frames = [frame]
        result = []
        for out in frames:
            sample_rate = int(getattr(out, "sample_rate", None) or getattr(out, "rate", None) or 16000)
            channels = 1
            valid_bytes = int(out.samples or 0) * channels * 2
            plane = bytes(out.planes[0])
            pcm = plane[:valid_bytes] if valid_bytes > 0 else plane
            result.append((pcm, sample_rate, channels))
        return result


SOCKET_TIMEOUT_EXCEPTIONS = (socket.timeout, TimeoutError)
