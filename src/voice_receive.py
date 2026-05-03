"""Receive Discord voice audio and asynchronously transcribe speaker segments.

This module deliberately does not implement speech-to-text itself.  It receives
Discord RTP/Opus media, segments decoded PCM by speaker using the same RMS
speaking gate as Record's call widget, writes finalized WAV files, and
delegates ASR to `exo transcribe` so exocortexd owns OpenAI auth.
"""

from __future__ import annotations

from collections import deque
import base64
import ctypes
import ctypes.util
import json
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
    import dave  # type: ignore
except Exception:  # pragma: no cover - depends on deployment venv
    dave = None

try:
    import nacl.bindings  # type: ignore
except Exception:  # pragma: no cover - depends on deployment venv
    nacl = None

OPUS_PAYLOAD_TYPE = 120
RTP_HEADER_LENGTH = 12
TRANSCRIBE_WORKERS = 2
DEFAULT_SPEECH_START_THRESHOLD_DB = -40.0
DEFAULT_SPEECH_STOP_THRESHOLD_DB = -40.0
DEFAULT_SILENCE_MS = 700
DEFAULT_MIN_SPEECH_MS = 450
DEFAULT_MAX_SEGMENT_MS = 0
DEFAULT_PRE_ROLL_MS = 250
DEFAULT_MAX_QUEUE = 24
DEFAULT_JITTER_PACKETS = 12
OPUS_SAMPLE_RATE = 48_000
TRANSCRIBE_SAMPLE_RATE = 16_000
OPUS_FRAME_SAMPLES = 960  # 20 ms at 48 kHz.
OPUS_MAX_FRAME_SAMPLES = 5_760
OPUS_SILENCE_FRAME = b"\xf8\xff\xfe"


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


_OPUS_LIB = None
_OPUS_LOAD_ATTEMPTED = False


def load_opus_lib():
    global _OPUS_LIB, _OPUS_LOAD_ATTEMPTED
    if _OPUS_LOAD_ATTEMPTED:
        return _OPUS_LIB
    _OPUS_LOAD_ATTEMPTED = True
    names = []
    found = ctypes.util.find_library("opus")
    if found:
        names.append(found)
    names.extend(["libopus.so.0", "libopus.so"])
    for name in names:
        try:
            lib = ctypes.CDLL(name)
            lib.opus_decoder_create.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
            lib.opus_decoder_create.restype = ctypes.c_void_p
            lib.opus_decode.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.POINTER(ctypes.c_int16), ctypes.c_int, ctypes.c_int]
            lib.opus_decode.restype = ctypes.c_int
            lib.opus_decoder_destroy.argtypes = [ctypes.c_void_p]
            lib.opus_decoder_destroy.restype = None
            lib.opus_packet_get_nb_frames.argtypes = [ctypes.c_void_p, ctypes.c_int]
            lib.opus_packet_get_nb_frames.restype = ctypes.c_int
            lib.opus_packet_get_nb_samples.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
            lib.opus_packet_get_nb_samples.restype = ctypes.c_int
            _OPUS_LIB = lib
            return lib
        except Exception:
            continue
    _OPUS_LIB = None
    return None


def has_opus_decoder() -> bool:
    return load_opus_lib() is not None


def opus_packet_frame_count(payload: bytes | None) -> int:
    if not payload:
        return -1
    lib = load_opus_lib()
    if lib is None:
        return -1
    data = (ctypes.c_ubyte * len(payload)).from_buffer_copy(payload)
    try:
        return int(lib.opus_packet_get_nb_frames(ctypes.cast(data, ctypes.c_void_p), len(payload)))
    except Exception:
        return -1


def opus_packet_sample_count(payload: bytes | None) -> int:
    if not payload:
        return -1
    lib = load_opus_lib()
    if lib is None:
        return -1
    data = (ctypes.c_ubyte * len(payload)).from_buffer_copy(payload)
    try:
        return int(lib.opus_packet_get_nb_samples(ctypes.cast(data, ctypes.c_void_p), len(payload), OPUS_SAMPLE_RATE))
    except Exception:
        return -1


def opus_packet_is_valid(payload: bytes | None) -> bool:
    return opus_packet_frame_count(payload) > 0


def is_dave_encrypted_payload(payload: bytes | None) -> bool:
    if not payload or len(payload) < 2:
        return False
    marker = payload.rfind(b"\xfa\xfa")
    if marker < 0:
        return False
    suffix = payload[marker + 2:]
    # DAVE-encrypted media usually ends in FAFA, but Discord/davey can leave
    # padding bytes after the marker (often one byte value repeated). Treat that
    # as encrypted too; feeding these bytes to Opus causes metallic/clipped noise.
    if not suffix:
        return True
    if marker < len(payload) - 256:
        return False
    return all(byte == suffix[0] for byte in suffix)


def strip_dave_padding(payload: bytes | None) -> tuple[bytes | None, int]:
    """Remove Discord/DAVE rtpsize padding that follows the FAFA marker.

    @snazzah/davey emits media ending in FAFA. Discord can relay those packets
    with repeated-byte padding after the marker for RTP-size obfuscation. dave-py
    expects the DAVE frame to end at FAFA, so normalize before decrypting instead
    of treating the packet as a missing audio frame.
    """
    if not payload or len(payload) < 2:
        return payload, 0
    marker = payload.rfind(b"\xfa\xfa")
    if marker < 0:
        return payload, 0
    suffix = payload[marker + 2:]
    if not suffix:
        return payload, 0
    if marker < len(payload) - 256:
        return payload, 0
    if not all(byte == suffix[0] for byte in suffix):
        return payload, 0
    return payload[:marker + 2], len(suffix)


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
    """Endcord-style DAVE handler using dave-py's per-SSRC key ratchets.

    davey exposes a convenient session.decrypt(user_id, payload) API, but in
    practice it leaves some encrypted/padded packets in the media path. Endcord
    uses dave-py Session + one Decryptor per SSRC and transitions each decryptor
    onto the sender's key ratchet; mirror that shape here so transcription sees
    real Opus once instead of attempting heuristic recovery after decode damage.
    """

    def __init__(self, *, user_id: str, channel_id: str, send_json, send_binary, on_error=None):
        self.user_id = str(user_id)
        self.channel_id = str(channel_id)
        self.send_json = send_json
        self.send_binary = send_binary
        self.on_error = on_error
        self.session = dave.Session() if dave is not None else None
        self.protocol_version = 0
        self.pending_transition_id = None
        self.known_user_ids = {self.user_id}
        self.ssrc_to_user_id = {}
        self.ssrc_to_decryptor = {}
        self.self_ssrc = None
        self.encryptor = dave.Encryptor() if dave is not None else None
        self.external_sender = None
        self.passthrough_count = 0
        self.decrypt_failure_count = 0
        self.encrypted_drop_count = 0
        self.padding_trim_count = 0
        self.padding_trim_bytes = 0
        if self.session is not None:
            self._init_session(self.advertised_protocol_version)

    @property
    def advertised_protocol_version(self):
        return int(dave.get_max_supported_protocol_version()) if dave is not None else 0

    def handle_session_description(self, data: dict):
        self.protocol_version = int(data.get("dave_protocol_version") or 0)

    def add_known_users(self, user_ids):
        for user_id in user_ids or []:
            if user_id is not None:
                self.known_user_ids.add(str(user_id))

    def remove_known_user(self, user_id):
        user_id = str(user_id)
        self.known_user_ids.discard(user_id)
        for ssrc, mapped_user_id in list(self.ssrc_to_user_id.items()):
            if mapped_user_id == user_id:
                self.ssrc_to_user_id.pop(ssrc, None)
                self.ssrc_to_decryptor.pop(ssrc, None)

    def add_ssrc_mapping(self, ssrc, user_id):
        if ssrc is None or user_id is None:
            return
        ssrc = int(ssrc)
        user_id = str(user_id)
        self.ssrc_to_user_id[ssrc] = user_id
        self.add_known_users([user_id])
        self._transition_decryptor(ssrc, user_id)

    def set_self_ssrc(self, ssrc):
        if ssrc is None:
            return
        self.self_ssrc = int(ssrc)
        self._transition_encryptor()

    def handle_json_opcode(self, opcode, data):
        if opcode == 21:
            self.handle_prepare_transition(data or {})
            return True
        if opcode == 22:
            if self.session is not None:
                self.update_ratchets()
            return True
        if opcode == 24:
            self.handle_prepare_epoch(data or {})
            return True
        return False

    def handle_binary_opcode(self, opcode, payload: bytes):
        if opcode == 25:
            self.external_sender = bytes(payload)
            self.apply_external_sender()
            self.send_key_package()
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

    def decode_incoming_opus(self, ssrc, payload: bytes):
        if payload == OPUS_SILENCE_FRAME:
            return payload
        if self.protocol_version <= 0:
            return payload
        if self.session is None:
            return None
        if not self.session.has_established_group():
            return None
        decryptor = self.ssrc_to_decryptor.get(int(ssrc))
        if decryptor is None:
            user_id = self.ssrc_to_user_id.get(int(ssrc))
            if user_id is not None:
                self._transition_decryptor(int(ssrc), user_id)
                decryptor = self.ssrc_to_decryptor.get(int(ssrc))
        if decryptor is None:
            return None
        payload, trimmed = strip_dave_padding(payload)
        if trimmed:
            self.padding_trim_count += 1
            self.padding_trim_bytes += trimmed
        encrypted = is_dave_encrypted_payload(payload)
        try:
            decoded = decryptor.decrypt(dave.MediaType.audio, bytes(payload))
        except Exception:
            decoded = None
        if decoded is None:
            if encrypted:
                self.decrypt_failure_count += 1
                return None
            self.passthrough_count += 1
            return payload
        if is_dave_encrypted_payload(decoded):
            self.encrypted_drop_count += 1
            self.report_error(f"DAVE decrypt returned encrypted-looking audio for SSRC {ssrc}; dropping packet")
            return None
        return bytes(decoded)

    def encode_outgoing_opus(self, payload: bytes):
        if payload == OPUS_SILENCE_FRAME:
            return payload
        if self.protocol_version <= 0:
            return payload
        if self.session is None or self.encryptor is None or self.self_ssrc is None:
            return None
        if not self.session.has_established_group():
            return None
        self._transition_encryptor()
        try:
            encoded = self.encryptor.encrypt(dave.MediaType.audio, int(self.self_ssrc), bytes(payload))
        except Exception as exc:
            self.report_error(f"DAVE encrypt failed for SSRC {self.self_ssrc}: {exc}")
            return None
        return bytes(encoded) if encoded is not None else None

    def can_encode_outgoing(self) -> bool:
        if self.protocol_version <= 0:
            return True
        if self.session is None or self.encryptor is None or self.self_ssrc is None:
            return False
        if not self.session.has_established_group():
            return False
        self._transition_encryptor()
        try:
            return bool(self.encryptor.has_key_ratchet())
        except Exception:
            return False

    def handle_prepare_transition(self, data: dict):
        if not isinstance(data, dict):
            return
        transition_id = data.get("transition_id")
        if transition_id is None:
            return
        self.pending_transition_id = int(transition_id)
        self.send_json({"op": 23, "d": {"transition_id": self.pending_transition_id}})

    def handle_prepare_epoch(self, data: dict):
        if not isinstance(data, dict) or int(data.get("epoch") or 0) != 1:
            return
        protocol_version = int(data.get("protocol_version", data.get("dave_protocol_version", 1)) or 1)
        if self.session is None:
            return
        try:
            self.session.reset()
            self._init_session(protocol_version)
            self.apply_external_sender()
            self.send_key_package()
        except Exception as exc:
            self.report_error(f"DAVE epoch init failed: {exc}")

    def handle_proposals(self, payload: bytes):
        if self.session is None or not payload:
            return
        try:
            result = self.session.process_proposals(bytes(payload), self.known_user_ids)
            if result is not None:
                self.send_commit_welcome(result)
        except Exception as exc:
            self.report_error(f"DAVE proposals failed: {exc}")
            self.send_invalid_commit_welcome(self.pending_transition_id)

    def handle_announce_commit_transition(self, payload: bytes):
        if self.session is None or len(payload) < 2:
            return
        transition_id = int.from_bytes(payload[:2], "big")
        try:
            result = self.session.process_commit(bytes(payload[2:]))
            if dave is not None and isinstance(result, dave.RejectType):
                self.report_error(f"DAVE rejected commit: {result}")
                self.send_invalid_commit_welcome(transition_id)
                self.send_key_package()
                return
            self.update_ratchets()
            if transition_id != 0:
                self.send_json({"op": 23, "d": {"transition_id": transition_id}})
        except Exception as exc:
            self.report_error(f"DAVE commit failed: {exc}")
            self.send_invalid_commit_welcome(transition_id)
            self.send_key_package()

    def handle_welcome(self, payload: bytes):
        if self.session is None or len(payload) < 2:
            return
        transition_id = int.from_bytes(payload[:2], "big")
        try:
            result = self.session.process_welcome(bytes(payload[2:]), self.known_user_ids)
            if result is None:
                self.report_error("DAVE welcome was invalid")
                self.send_invalid_commit_welcome(transition_id)
                self.send_key_package()
                return
            self.update_ratchets()
            self.send_json({"op": 23, "d": {"transition_id": transition_id}})
        except Exception as exc:
            self.report_error(f"DAVE welcome failed: {exc}")
            self.send_invalid_commit_welcome(transition_id)
            self.send_key_package()

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
            self.send_binary(26, self.session.get_marshalled_key_package())
        except Exception as exc:
            self.report_error(f"DAVE key package failed: {exc}")

    def send_commit_welcome(self, result):
        if result is None:
            return
        commit = getattr(result, "commit", None)
        if commit is not None:
            welcome = getattr(result, "welcome", None)
            self.send_binary(28, bytes(commit) + (bytes(welcome) if welcome is not None else b""))
            return
        self.send_binary(28, bytes(result))

    def send_invalid_commit_welcome(self, transition_id):
        if transition_id is None:
            return
        self.send_json({"op": 31, "d": {"transition_id": int(transition_id)}})

    def update_ratchets(self):
        for ssrc, user_id in list(self.ssrc_to_user_id.items()):
            self._transition_decryptor(ssrc, user_id)
        self._transition_encryptor()

    def _transition_encryptor(self):
        if self.session is None or self.encryptor is None or dave is None or self.self_ssrc is None:
            return
        try:
            ratchet = self.session.get_key_ratchet(str(self.user_id))
        except Exception:
            ratchet = None
        if ratchet is None:
            return
        try:
            self.encryptor.set_key_ratchet(ratchet)
            self.encryptor.assign_ssrc_to_codec(int(self.self_ssrc), dave.Codec.opus)
        except Exception as exc:
            self.report_error(f"DAVE encryptor ratchet transition failed: {exc}")

    def _transition_decryptor(self, ssrc: int, user_id: str):
        if self.session is None or dave is None:
            return
        try:
            ratchet = self.session.get_key_ratchet(str(user_id))
        except Exception:
            ratchet = None
        if ratchet is None:
            return
        decryptor = self.ssrc_to_decryptor.get(ssrc)
        if decryptor is None:
            decryptor = dave.Decryptor()
            self.ssrc_to_decryptor[ssrc] = decryptor
        try:
            decryptor.transition_to_key_ratchet(ratchet, transition_expiry=10.0)
        except Exception as exc:
            self.report_error(f"DAVE ratchet transition failed for {user_id}: {exc}")

    def _init_session(self, protocol_version: int):
        if self.session is None:
            return
        self.protocol_version = int(protocol_version or 0)
        if self.protocol_version <= 0:
            return
        self.session.init(
            version=self.protocol_version,
            group_id=int(self.channel_id),
            self_user_id=str(self.user_id),
        )
        self._transition_encryptor()

    def report_error(self, message: str):
        if self.on_error:
            self.on_error(message)


def sequence_distance(seq: int, expected: int) -> int:
    return ((int(seq) - int(expected) + 32768) & 0xFFFF) - 32768


class RtpJitterBuffer:
    def __init__(self, *, max_packets: int = DEFAULT_JITTER_PACKETS):
        self.max_packets = max(0, int(max_packets))
        self.expected = None
        self.buffer = {}

    def add(self, sequence: int, item):
        sequence = int(sequence) & 0xFFFF
        if self.expected is None:
            self.expected = sequence
        if sequence_distance(sequence, self.expected) < 0:
            return []
        self.buffer[sequence] = item
        return self.drain()

    def drain(self):
        if self.expected is None:
            return []
        ready = []
        while True:
            payload = self.buffer.pop(self.expected, None)
            if payload is None:
                if self.max_packets <= 0 or len(self.buffer) <= self.max_packets:
                    break
                # Give up on one missing packet and let the decoder perform PLC.
                ready.append(None)
            else:
                ready.append(payload)
            self.expected = (self.expected + 1) & 0xFFFF
        return ready

    def flush(self, limit: int | None = None):
        if self.expected is None:
            return []
        ready = []
        max_steps = max(0, int(limit)) if limit is not None else len(self.buffer) + self.max_packets + 1
        steps = 0
        while self.buffer and steps < max_steps:
            payload = self.buffer.pop(self.expected, None)
            ready.append(payload)
            self.expected = (self.expected + 1) & 0xFFFF
            steps += 1
        return ready


class LibOpusPcmDecoder:
    def __init__(self, *, channels: int | None = None):
        lib = load_opus_lib()
        if lib is None:
            raise RuntimeError("libopus is not available")
        self.lib = lib
        if channels is None:
            channels = env_int("DISCORD_CALL_TRANSCRIBE_OPUS_CHANNELS", 1)
        self.channels = max(1, min(2, int(channels)))
        error = ctypes.c_int(0)
        self.ptr = lib.opus_decoder_create(OPUS_SAMPLE_RATE, self.channels, ctypes.byref(error))
        if error.value != 0 or not self.ptr:
            raise RuntimeError(f"failed to create Opus decoder ({error.value})")
        self.buffer = (ctypes.c_int16 * (OPUS_MAX_FRAME_SAMPLES * self.channels))()

    def close(self):
        if self.ptr:
            self.lib.opus_decoder_destroy(self.ptr)
            self.ptr = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def decode(self, payload: bytes | None):
        if not self.ptr:
            return None
        data_ptr = None
        data_len = 0
        frame_size = OPUS_FRAME_SAMPLES
        if payload == OPUS_SILENCE_FRAME:
            return b"\x00\x00" * (OPUS_FRAME_SAMPLES * self.channels), OPUS_SAMPLE_RATE, self.channels
        if payload is not None:
            data_len = len(payload)
            data = (ctypes.c_ubyte * data_len).from_buffer_copy(payload)
            data_ptr = ctypes.cast(data, ctypes.c_void_p)
            frame_size = OPUS_MAX_FRAME_SAMPLES
        decoded = self.lib.opus_decode(self.ptr, data_ptr, data_len, self.buffer, frame_size, 0)
        if decoded < 0:
            return None
        valid_bytes = decoded * self.channels * 2
        return bytes(self.buffer)[:valid_bytes], OPUS_SAMPLE_RATE, self.channels

    def decode_missing(self):
        return self.decode(None)


def probe_libopus_payload(payload: bytes | None) -> str:
    if not payload:
        return "payload=missing"
    frame_count = opus_packet_frame_count(payload)
    sample_count = opus_packet_sample_count(payload)
    parts = [f"len={len(payload)}", f"frames={frame_count}", f"samples={sample_count}"]
    for channels in (1, 2):
        try:
            decoder = LibOpusPcmDecoder(channels=channels)
            decoded = decoder.decode(payload)
            decoder.close()
            if decoded is not None:
                _pcm, sample_rate, out_channels = decoded
                parts.append(f"fresh{channels}ch=ok/{sample_rate}Hz/{out_channels}ch")
            else:
                parts.append(f"fresh{channels}ch=reject")
        except Exception as exc:
            parts.append(f"fresh{channels}ch=err:{exc}")
    for trim in range(1, min(32, len(payload)) + 1):
        trimmed = payload[:-trim]
        try:
            decoder = LibOpusPcmDecoder(channels=1)
            decoded = decoder.decode(trimmed)
            decoder.close()
        except Exception:
            decoded = None
        if decoded is not None:
            parts.append(f"trim_tail={trim}:ok")
            break
    for trim in range(1, min(16, len(payload)) + 1):
        trimmed = payload[trim:]
        try:
            decoder = LibOpusPcmDecoder(channels=1)
            decoded = decoder.decode(trimmed)
            decoder.close()
        except Exception:
            decoded = None
        if decoded is not None:
            parts.append(f"trim_head={trim}:ok")
            break
    parts.append(f"head={payload[:8].hex()}")
    parts.append(f"tail={(payload[-8:] if len(payload) >= 8 else payload).hex()}")
    return " ".join(parts)


class VoicePacketTrace:
    def __init__(self, *, max_packets: int | None = None):
        self.max_packets = env_int("DISCORD_CALL_TRANSCRIBE_TRACE_PACKETS", 2_000) if max_packets is None else int(max_packets)
        self.items = []
        self.dropped = 0

    def append(self, info: dict | None):
        if info is None:
            return
        if self.max_packets <= 0:
            return
        if len(self.items) >= self.max_packets:
            self.dropped += 1
            return
        item = dict(info)
        payload = item.pop("payload", None)
        if payload is not None:
            item["payload_b64"] = base64.b64encode(bytes(payload)).decode("ascii")
        self.items.append(item)

    def snapshot(self) -> dict:
        return {"packets": list(self.items), "dropped": self.dropped}

    def clear(self):
        self.items.clear()
        self.dropped = 0


class SpeakerSegmenter:
    def __init__(self, user_id: str, name_for_user, submit_segment, *, sample_rate=16000, channels=1):
        self.user_id = str(user_id)
        self.name_for_user = name_for_user
        self.submit_segment = submit_segment
        self.sample_rate = sample_rate
        self.channels = channels
        legacy_threshold_db = env_float("DISCORD_CALL_TRANSCRIBE_THRESHOLD_DB", DEFAULT_SPEECH_START_THRESHOLD_DB)
        record_threshold_db = env_float("RECORD_VOICE_SPEAKING_THRESHOLD_DB", legacy_threshold_db)
        self.start_threshold_db = env_float("DISCORD_CALL_TRANSCRIBE_START_THRESHOLD_DB", record_threshold_db)
        self.stop_threshold_db = env_float("DISCORD_CALL_TRANSCRIBE_STOP_THRESHOLD_DB", self.start_threshold_db)
        if self.stop_threshold_db > self.start_threshold_db:
            self.stop_threshold_db = self.start_threshold_db
        self.start_threshold = db_to_linear(self.start_threshold_db)
        self.stop_threshold = db_to_linear(self.stop_threshold_db)
        self.silence_seconds = env_int("DISCORD_CALL_TRANSCRIBE_SILENCE_MS", DEFAULT_SILENCE_MS) / 1000.0
        self.min_speech_seconds = env_int("DISCORD_CALL_TRANSCRIBE_MIN_SPEECH_MS", DEFAULT_MIN_SPEECH_MS) / 1000.0
        self.max_segment_seconds = max(0, env_int("DISCORD_CALL_TRANSCRIBE_MAX_SEGMENT_MS", DEFAULT_MAX_SEGMENT_MS)) / 1000.0
        pre_roll_frames = max(0, int((env_int("DISCORD_CALL_TRANSCRIBE_PRE_ROLL_MS", DEFAULT_PRE_ROLL_MS) / 1000.0) * sample_rate))
        self.pre_roll = deque(maxlen=pre_roll_frames * channels * 2)
        pre_roll_packets = max(0, int(math.ceil(env_int("DISCORD_CALL_TRANSCRIBE_PRE_ROLL_MS", DEFAULT_PRE_ROLL_MS) / 20)))
        self.pre_roll_packet_trace = deque(maxlen=pre_roll_packets)
        self.packet_trace = VoicePacketTrace()
        self.active = False
        self.frames = bytearray()
        self.speech_seconds = 0.0
        self.silence_seconds_seen = 0.0
        self.segment_started_at = 0.0
        self.last_speech_at = 0.0
        self.max_rms = 0.0
        self.last_audio_at = time.time()

    def add_pcm(self, pcm: bytes, duration: float, packet_info: dict | None = None):
        now = time.time()
        self.last_audio_at = now
        rms = pcm16_rms(pcm)
        if rms > self.max_rms:
            self.max_rms = rms
        speaking = rms >= (self.stop_threshold if self.active else self.start_threshold)
        if speaking:
            if not self.active:
                self.active = True
                self.frames = bytearray(self.pre_roll)
                self.packet_trace.clear()
                for traced in self.pre_roll_packet_trace:
                    self.packet_trace.append(traced)
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
            self.packet_trace.append(packet_info)
            current_len_seconds = len(self.frames) / (self.sample_rate * self.channels * 2)
            if self.silence_seconds_seen >= self.silence_seconds and self.speech_seconds >= self.min_speech_seconds:
                self.finalize()
            elif self.max_segment_seconds > 0 and current_len_seconds >= self.max_segment_seconds:
                self.finalize()
        else:
            self.pre_roll.extend(pcm)
            if packet_info is not None:
                self.pre_roll_packet_trace.append(packet_info)

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
        packet_trace = self.packet_trace.snapshot()
        self.active = False
        self.frames = bytearray()
        self.speech_seconds = 0.0
        self.silence_seconds_seen = 0.0
        self.segment_started_at = 0.0
        self.last_speech_at = 0.0
        self.max_rms = 0.0
        self.packet_trace.clear()
        self.pre_roll.clear()
        self.pre_roll_packet_trace.clear()
        if speech_seconds < self.min_speech_seconds:
            return
        self.submit_segment(self.user_id, self.name_for_user(self.user_id), frames, self.sample_rate, self.channels, {
            "duration_seconds": duration_seconds,
            "speech_seconds": speech_seconds,
            "speech_started_at": speech_started_at,
            "speech_ended_at": speech_ended_at,
            "finalized_at": finalized_at,
            "max_db": 20 * math.log10(max_rms) if max_rms > 0 else -math.inf,
            "packet_trace": packet_trace,
        })


class VoiceTranscriber:
    def __init__(self, *, label: str, notify, log=print, keep_audio: bool | None = None, audio_dir: str | os.PathLike | None = None):
        self.label = label
        self.notify = notify
        self.log = log
        self.enabled = True
        self.queue = queue.Queue(maxsize=env_int("DISCORD_CALL_TRANSCRIBE_QUEUE", DEFAULT_MAX_QUEUE))
        self.running = True
        self.keep_audio = (os.environ.get("DISCORD_CALL_TRANSCRIBE_KEEP_AUDIO") == "1") if keep_audio is None else bool(keep_audio)
        configured_audio_dir = audio_dir or os.environ.get("DISCORD_CALL_TRANSCRIBE_AUDIO_DIR")
        if configured_audio_dir:
            self.tmpdir = Path(configured_audio_dir).expanduser()
            self.tmpdir.mkdir(parents=True, exist_ok=True)
        else:
            self.tmpdir = Path(tempfile.mkdtemp(prefix="discord-call-transcribe-"))
        if self.keep_audio:
            self.log(f"keeping transcription audio segments in {self.tmpdir}")
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
        packet_trace = stats.pop("packet_trace", None)
        packet_trace_path = None
        worker_started_at = time.time()
        wav_path = self.tmpdir / f"segment-{int(time.time() * 1000)}-{os.getpid()}-{threading.get_ident()}.wav"
        with wave.open(str(wav_path), "wb") as wf:
            wf.setnchannels(int(item["channels"]))
            wf.setsampwidth(2)
            wf.setframerate(int(item["sample_rate"]))
            wf.writeframes(item["pcm"])
        if self.keep_audio:
            self.log(f"saved transcription audio segment for {name}: {wav_path}")
            packet_trace_path = self._write_packet_trace(wav_path, packet_trace)
            if packet_trace_path:
                self.log(f"saved transcription Opus packet trace for {name}: {packet_trace_path}")
        transcribe_started_at = time.time()
        try:
            proc = subprocess.run(
                ["exo", "transcribe", str(wav_path), "--mime-type", "audio/wav", "--timeout", "120"],
                capture_output=True,
                text=True,
                timeout=150,
            )
            transcript_ready_at = time.time()
            timing = self._build_timing(stats, worker_started_at, transcribe_started_at, transcript_ready_at)
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or "exo transcribe failed").strip().splitlines()[-1:]
                self.log(f"exo transcribe failed for {name}: {err[0] if err else proc.returncode}; {self._format_timing(timing)}")
                return
            text = (proc.stdout or "").strip()
            if not text:
                self.log(f"exo transcribe empty for {name}; {self._format_timing(timing)}")
                return
            self.log(f"exo transcribe ready for {name}; {self._format_timing(timing)}")
            if self.keep_audio:
                self._write_sidecar(wav_path, item, text, timing, packet_trace_path=packet_trace_path)
            self.notify(f"🎙 {name}: {text}", prefix="Discord Voice", timing=timing)
        except subprocess.TimeoutExpired:
            timed_out_at = time.time()
            timing = self._build_timing(stats, worker_started_at, transcribe_started_at, timed_out_at)
            self.log(f"exo transcribe timed out for {name}; {self._format_timing(timing)}")
        except Exception as exc:
            failed_at = time.time()
            timing = self._build_timing(stats, worker_started_at, transcribe_started_at, failed_at)
            self.log(f"exo transcribe failed for {name}: {exc}; {self._format_timing(timing)}")
        finally:
            if not self.keep_audio:
                try:
                    wav_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def _write_packet_trace(self, wav_path: Path, packet_trace) -> Path | None:
        if not packet_trace or not isinstance(packet_trace, dict):
            return None
        packets = packet_trace.get("packets") or []
        if not packets:
            return None
        path = wav_path.with_suffix(".packets.jsonl")
        try:
            with path.open("w", encoding="utf-8") as fh:
                header = {"type": "header", "dropped": packet_trace.get("dropped", 0), "packet_count": len(packets)}
                fh.write(json.dumps(header, sort_keys=True) + "\n")
                for packet in packets:
                    fh.write(json.dumps(packet, sort_keys=True) + "\n")
            return path
        except Exception as exc:
            self.log(f"failed to write transcription Opus packet trace for {wav_path}: {exc}")
            return None

    def _write_sidecar(self, wav_path: Path, item: dict, text: str, timing: dict, *, packet_trace_path: Path | None = None):
        sidecar = wav_path.with_suffix(".json")
        try:
            stats = dict(item.get("stats") or {})
            stats.pop("packet_trace", None)
            payload = {
                "wav": str(wav_path),
                "user_id": item.get("user_id"),
                "name": item.get("name"),
                "sample_rate": item.get("sample_rate"),
                "channels": item.get("channels"),
                "stats": stats,
                "packet_trace": str(packet_trace_path) if packet_trace_path else None,
                "timing": timing,
                "transcript": text,
            }
            sidecar.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except Exception as exc:
            self.log(f"failed to write transcription audio sidecar for {wav_path}: {exc}")

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



def _read_wav_metrics(path: Path) -> dict:
    try:
        with wave.open(str(path), "rb") as wf:
            channels = wf.getnchannels()
            sample_rate = wf.getframerate()
            frames = wf.getnframes()
            pcm = wf.readframes(frames)
    except Exception as exc:
        return {"path": str(path), "error": str(exc)}
    sample_count = len(pcm) // 2
    if sample_count <= 0:
        return {"path": str(path), "channels": channels, "sample_rate": sample_rate, "duration_seconds": 0.0}
    samples = struct.unpack("<" + "h" * sample_count, pcm[:sample_count * 2])
    rms = math.sqrt(sum((sample / 32768.0) ** 2 for sample in samples) / sample_count)
    peak = max(abs(sample) for sample in samples) / 32768.0
    clipped = sum(1 for sample in samples if abs(sample) >= 32760) / sample_count
    zero_crossing = 0.0
    diff_rms = 0.0
    if sample_count > 1:
        zero_crossing = sum(1 for a, b in zip(samples, samples[1:]) if (a < 0 <= b) or (a >= 0 > b)) / (sample_count - 1)
        diff_rms = math.sqrt(sum(((b - a) / 32768.0) ** 2 for a, b in zip(samples, samples[1:])) / (sample_count - 1))
    return {
        "path": str(path),
        "channels": channels,
        "sample_rate": sample_rate,
        "duration_seconds": frames / sample_rate if sample_rate else 0.0,
        "rms_db": 20 * math.log10(rms) if rms > 0 else -math.inf,
        "peak_db": 20 * math.log10(peak) if peak > 0 else -math.inf,
        "clipped_percent": clipped * 100,
        "zero_crossing_rate": zero_crossing,
        "diff_rms_db": 20 * math.log10(diff_rms) if diff_rms > 0 else -math.inf,
    }


def _format_metric_db(value) -> str:
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        return "n/a"
    return f"{value:.1f}dB"


def _format_wav_metrics(metrics: dict) -> str:
    if metrics.get("error"):
        return f"error={metrics['error']}"
    return (
        f"{metrics.get('duration_seconds', 0):.2f}s {metrics.get('sample_rate')}Hz/{metrics.get('channels')}ch "
        f"rms={_format_metric_db(metrics.get('rms_db'))} peak={_format_metric_db(metrics.get('peak_db'))} "
        f"clip={metrics.get('clipped_percent', 0):.3f}% zc={metrics.get('zero_crossing_rate', 0):.3f} "
        f"diff={_format_metric_db(metrics.get('diff_rms_db'))}"
    )


def _default_packet_trace_path(path: Path) -> Path:
    if path.suffix == ".json":
        try:
            data = json.loads(path.read_text(errors="replace"))
            trace = data.get("packet_trace")
            if trace:
                return Path(trace)
            wav = data.get("wav")
            if wav:
                return Path(wav).with_suffix(".packets.jsonl")
        except Exception:
            pass
    if path.suffix == ".jsonl":
        return path
    return path.with_suffix(".packets.jsonl")


def _default_wav_path(path: Path) -> Path:
    if path.suffix == ".json":
        try:
            data = json.loads(path.read_text(errors="replace"))
            wav = data.get("wav")
            if wav:
                return Path(wav)
        except Exception:
            pass
    if path.suffix == ".jsonl" and path.name.endswith(".packets.jsonl"):
        return Path(str(path)[:-len(".packets.jsonl")] + ".wav")
    return path


def _iter_packet_trace(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            if item.get("type") == "header":
                yield item
                continue
            payload_b64 = item.get("payload_b64")
            if payload_b64:
                item["payload"] = base64.b64decode(payload_b64)
            yield item


def _try_decode_with_trim(payload: bytes, channels: int):
    for trim in range(1, min(32, len(payload)) + 1):
        trimmed = payload[:-trim]
        decoder = LibOpusPcmDecoder(channels=channels)
        decoded = decoder.decode(trimmed)
        decoder.close()
        if decoded is not None:
            return decoded, f"tail:{trim}"
    for trim in range(1, min(16, len(payload)) + 1):
        trimmed = payload[trim:]
        decoder = LibOpusPcmDecoder(channels=channels)
        decoded = decoder.decode(trimmed)
        decoder.close()
        if decoded is not None:
            return decoded, f"head:{trim}"
    return None, None


def _decode_packet_trace_to_wav(trace_path: Path, out_path: Path, *, channels: int = 1, salvage: bool = False, drop_dave_encrypted: bool = False) -> dict:
    decoder = LibOpusPcmDecoder(channels=channels)
    pcm_parts = []
    packet_count = 0
    missing = 0
    errors = 0
    salvaged = 0
    dropped_dave_encrypted = 0
    first_errors = []
    try:
        for item in _iter_packet_trace(trace_path):
            if item.get("type") == "header":
                continue
            packet_count += 1
            payload = item.get("payload")
            if item.get("missing") or item.get("dave_drop") or payload is None:
                missing += 1
                decoded = decoder.decode_missing()
            elif drop_dave_encrypted and is_dave_encrypted_payload(payload):
                dropped_dave_encrypted += 1
                decoded = decoder.decode_missing()
            else:
                decoded = decoder.decode(payload)
                if decoded is None and salvage:
                    decoded, how = _try_decode_with_trim(payload, channels)
                    if decoded is not None:
                        salvaged += 1
                        if len(first_errors) < 5:
                            first_errors.append({"sequence": item.get("sequence"), "salvage": how, "probe": probe_libopus_payload(payload)})
                if decoded is None:
                    errors += 1
                    if len(first_errors) < 5:
                        first_errors.append({"sequence": item.get("sequence"), "probe": probe_libopus_payload(payload)})
                    decoded = decoder.decode_missing()
            if decoded is None:
                continue
            pcm, sample_rate, out_channels = decoded
            pcm_parts.append(pcm)
        pcm = b"".join(pcm_parts)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with wave.open(str(out_path), "wb") as wf:
            wf.setnchannels(channels)
            wf.setsampwidth(2)
            wf.setframerate(OPUS_SAMPLE_RATE)
            wf.writeframes(pcm)
        metrics = _read_wav_metrics(out_path)
        return {
            "path": str(out_path),
            "packets": packet_count,
            "missing": missing,
            "decode_errors": errors,
            "salvaged": salvaged,
            "dropped_dave_encrypted": dropped_dave_encrypted,
            "first_errors": first_errors,
            "metrics": metrics,
        }
    finally:
        decoder.close()


def diagnose_saved_voice_segment(path: str | os.PathLike, *, write_variants: bool = True) -> str:
    input_path = Path(path).expanduser()
    wav_path = _default_wav_path(input_path)
    trace_path = _default_packet_trace_path(input_path)
    lines = [f"segment: {wav_path}"]
    metrics = _read_wav_metrics(wav_path)
    lines.append(f"wav: {_format_wav_metrics(metrics)}")
    sidecar = wav_path.with_suffix(".json")
    if sidecar.exists():
        try:
            data = json.loads(sidecar.read_text(errors="replace"))
            transcript = str(data.get("transcript") or "").replace("\n", " ")
            if transcript:
                lines.append(f"transcript: {transcript}")
        except Exception:
            pass
    if not trace_path.exists():
        lines.append(f"packet trace: missing ({trace_path})")
        return "\n".join(lines)
    try:
        header = None
        packet_rows = []
        for item in _iter_packet_trace(trace_path):
            if item.get("type") == "header":
                header = item
            else:
                packet_rows.append(item)
        dave_drops = sum(1 for item in packet_rows if item.get("dave_drop"))
        missing_packets = sum(1 for item in packet_rows if item.get("missing"))
        invalid_parser = sum(1 for item in packet_rows if not item.get("dave_drop") and not item.get("missing") and int(item.get("opus_frames") or -1) <= 0)
        sample_counts = {}
        lengths = {}
        for item in packet_rows:
            sample_counts[item.get("opus_samples", item.get("pre_dave_opus_samples"))] = sample_counts.get(item.get("opus_samples", item.get("pre_dave_opus_samples")), 0) + 1
            lengths[item.get("payload_len", item.get("pre_dave_payload_len"))] = lengths.get(item.get("payload_len", item.get("pre_dave_payload_len")), 0) + 1
        top_samples = ", ".join(f"{k}:{v}" for k, v in sorted(sample_counts.items(), key=lambda kv: (-kv[1], str(kv[0])))[:5])
        top_lengths = ", ".join(f"{k}:{v}" for k, v in sorted(lengths.items(), key=lambda kv: (-kv[1], str(kv[0])))[:5])
        lines.append(f"packet trace: {trace_path}")
        lines.append(f"packets={len(packet_rows)} dropped={(header or {}).get('dropped', 0)} missing={missing_packets} dave_drop={dave_drops} parser_invalid={invalid_parser} opus_samples={top_samples} payload_lens={top_lengths}")
    except Exception as exc:
        lines.append(f"packet trace read failed: {exc}")
        return "\n".join(lines)
    if write_variants:
        stem = wav_path.with_suffix("")
        variants = [
            (1, False, False, ".diag-libopus-1ch.wav"),
            (2, False, False, ".diag-libopus-2ch.wav"),
            (1, True, False, ".diag-libopus-1ch-salvage.wav"),
            (2, True, False, ".diag-libopus-2ch-salvage.wav"),
            (1, False, True, ".diag-drop-dave-padded.wav"),
        ]
        for channels, salvage, drop_dave, suffix in variants:
            result = _decode_packet_trace_to_wav(trace_path, Path(str(stem) + suffix), channels=channels, salvage=salvage, drop_dave_encrypted=drop_dave)
            lines.append(
                f"variant: {result['path']} packets={result['packets']} missing={result['missing']} "
                f"errors={result['decode_errors']} salvaged={result['salvaged']} dave_dropped={result['dropped_dave_encrypted']} {_format_wav_metrics(result['metrics'])}"
            )
            for err in result.get("first_errors") or []:
                lines.append(f"  decode_issue seq={err.get('sequence')} {err.get('salvage', 'reject')} {err.get('probe')}")
    return "\n".join(lines)



class VoiceReceiveTranscription:
    @staticmethod
    def advertised_dave_protocol_version_static():
        return int(dave.get_max_supported_protocol_version()) if dave is not None else 0

    def __init__(self, *, udp=None, mode: str | None = None, secret_key=None, self_user_id: str, channel_id: str, label: str, send_json, send_binary, notify, name_for_user, log=print, keep_audio: bool | None = None, audio_dir: str | os.PathLike | None = None):
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
        self.fallback_user_ids = set()
        self.segmenters = {}
        self.decoders = {}
        self.resamplers = {}
        self.pre_dave_jitter_buffers = {}
        self.jitter_buffers = {}
        self.jitter_missing_count = 0
        self.packet_count = 0
        self.unknown_ssrc_count = 0
        self.self_packet_count = 0
        self.transport_decrypt_fail_count = 0
        self.extension_packet_count = 0
        self.extension_bytes_total = 0
        self.decrypt_count = 0
        self.pre_dave_opus_valid_count = 0
        self.pre_dave_opus_invalid_count = 0
        self.dave_drop_count = 0
        self.post_dave_opus_valid_count = 0
        self.post_dave_opus_invalid_count = 0
        self.decode_frame_count = 0
        self.decode_error_count = 0
        self.last_decode_error_log_at = 0.0
        self.last_invalid_opus_log_at = 0.0
        self.last_stats_at = time.time()
        self.transcriber = VoiceTranscriber(label=label, notify=notify, log=log, keep_audio=keep_audio, audio_dir=audio_dir)
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
        if not has_opus_decoder() and av is None:
            self.log("Voice transcription disabled: neither libopus nor PyAV is available")
            return
        if nacl is None:
            self.log("Voice transcription disabled: PyNaCl is not installed")
            return
        if not shutil.which("exo"):
            self.log("Voice transcription disabled: exo CLI is not in PATH")
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

    def set_active_remote_users(self, user_ids):
        self.fallback_user_ids = {str(user_id) for user_id in (user_ids or []) if user_id is not None and str(user_id) != self.self_user_id}
        self.dave.add_known_users(self.fallback_user_ids)

    def set_self_ssrc(self, ssrc):
        self.dave.set_self_ssrc(ssrc)

    def encode_outgoing_opus(self, payload: bytes):
        return self.dave.encode_outgoing_opus(payload)

    def can_encode_outgoing(self) -> bool:
        return self.dave.can_encode_outgoing()

    def add_ssrc_mapping(self, ssrc, user_id):
        if ssrc is None or user_id is None:
            return
        ssrc = int(ssrc)
        user_id = str(user_id)
        previous = self.ssrc_to_user_id.get(ssrc)
        self.ssrc_to_user_id[ssrc] = user_id
        self.fallback_user_ids.add(user_id)
        self.dave.add_ssrc_mapping(ssrc, user_id)
        if previous != user_id:
            self.log(f"Voice transcription mapped SSRC {ssrc} to {self.name_for_user(user_id)}")

    def remove_user(self, user_id):
        user_id = str(user_id)
        self.fallback_user_ids.discard(user_id)
        self.dave.remove_known_user(user_id)
        for ssrc, mapped_user in list(self.ssrc_to_user_id.items()):
            if mapped_user == user_id:
                del self.ssrc_to_user_id[ssrc]
        segmenter = self.segmenters.pop(user_id, None)
        if segmenter:
            segmenter.finalize()
        decoder = self.decoders.pop(user_id, None)
        if hasattr(decoder, "close"):
            try:
                decoder.close()
            except Exception:
                pass
        self.resamplers.pop(user_id, None)
        self.jitter_buffers.pop(user_id, None)

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
                self._flush_stale(flush_jitter=True)
                continue
            except OSError:
                break
            except Exception as exc:
                self.log(f"Voice UDP receive error: {exc}")
                break
            self._handle_packet(packet)
            self._flush_stale(flush_jitter=False)

    def _flush_stale(self, *, flush_jitter: bool = False):
        if flush_jitter:
            for user_id, jitter in list(self.pre_dave_jitter_buffers.items()):
                for item in jitter.flush():
                    self._handle_ordered_pre_dave_item(user_id, item)
            for user_id, jitter in list(self.jitter_buffers.items()):
                for item in jitter.flush():
                    if item is None:
                        self.jitter_missing_count += 1
                        self._decode_and_segment(user_id, None, packet_info={"missing": True, "stage": "post_dave_jitter"})
                    else:
                        opus_payload, traced_packet = item
                        if opus_payload is None:
                            self.jitter_missing_count += 1
                        self._decode_and_segment(user_id, opus_payload, packet_info=traced_packet)
        for segmenter in list(self.segmenters.values()):
            segmenter.flush_if_stale()
        now = time.time()
        if now - self.last_stats_at >= 10:
            self.last_stats_at = now
            if self.packet_count or self.decrypt_count or self.decode_frame_count or self.ssrc_to_user_id:
                self.log(
                    f"Voice transcription stats: packets={self.packet_count} unknown_ssrc={self.unknown_ssrc_count} "
                    f"self={self.self_packet_count} transport_fail={self.transport_decrypt_fail_count} "
                    f"ext={self.extension_packet_count}/{self.extension_bytes_total}B decrypted={self.decrypt_count} "
                    f"pre_dave_opus={self.pre_dave_opus_valid_count}/{self.pre_dave_opus_invalid_count} "
                    f"dave_drop={self.dave_drop_count} dave_passthrough={self.dave.passthrough_count} "
                    f"dave_decrypt_fail={self.dave.decrypt_failure_count} dave_encrypted_drop={self.dave.encrypted_drop_count} "
                    f"dave_padding_trim={self.dave.padding_trim_count}/{self.dave.padding_trim_bytes}B "
                    f"post_dave_opus={self.post_dave_opus_valid_count}/{self.post_dave_opus_invalid_count} "
                    f"frames={self.decode_frame_count} decode_errors={self.decode_error_count} "
                    f"jitter_missing={self.jitter_missing_count} speakers={len(self.segmenters)} ssrcs={len(self.ssrc_to_user_id)}"
                )

    def _handle_packet(self, packet: bytes):
        parsed = parse_rtp_packet(packet)
        if not parsed or parsed["payload_type"] != OPUS_PAYLOAD_TYPE:
            return
        self.packet_count += 1
        user_id = self.ssrc_to_user_id.get(parsed["ssrc"])
        if not user_id and len(self.fallback_user_ids) == 1:
            user_id = next(iter(self.fallback_user_ids))
            self.add_ssrc_mapping(parsed["ssrc"], user_id)
            self.log(f"Voice transcription inferred SSRC {parsed['ssrc']} for {self.name_for_user(user_id)}")
        if not user_id:
            self.unknown_ssrc_count += 1
            return
        if user_id == self.self_user_id:
            self.self_packet_count += 1
            return
        ext_len = int(parsed.get("extension_body_length") or 0)
        if ext_len:
            self.extension_packet_count += 1
            self.extension_bytes_total += ext_len
        payload = decrypt_transport(packet, parsed, self.mode, self.secret_key)
        if not payload:
            self.transport_decrypt_fail_count += 1
            return
        self.decrypt_count += 1
        if ext_len:
            payload = payload[ext_len:]
        if not payload:
            return
        jitter = self.pre_dave_jitter_buffers.get(user_id)
        if jitter is None:
            jitter = RtpJitterBuffer(max_packets=env_int("DISCORD_CALL_TRANSCRIBE_PRE_DAVE_JITTER_PACKETS", DEFAULT_JITTER_PACKETS))
            self.pre_dave_jitter_buffers[user_id] = jitter
        for item in jitter.add(parsed["sequence"], (dict(parsed), payload)):
            self._handle_ordered_pre_dave_item(user_id, item)

    def _handle_ordered_pre_dave_item(self, user_id: str, item):
        if item is None:
            self.jitter_missing_count += 1
            self._decode_and_segment(user_id, None, packet_info={"missing": True, "stage": "pre_dave_jitter"})
            return
        parsed, payload = item
        pre_dave_payload = payload
        if opus_packet_is_valid(payload):
            self.pre_dave_opus_valid_count += 1
        else:
            self.pre_dave_opus_invalid_count += 1
        payload = self.dave.decode_incoming_opus(parsed["ssrc"], payload)
        jitter = self.jitter_buffers.get(user_id)
        if jitter is None:
            jitter = RtpJitterBuffer(max_packets=env_int("DISCORD_CALL_TRANSCRIBE_JITTER_PACKETS", DEFAULT_JITTER_PACKETS))
            self.jitter_buffers[user_id] = jitter
        if not payload:
            self.dave_drop_count += 1
            packet_info = {
                "sequence": parsed.get("sequence"),
                "timestamp": parsed.get("timestamp"),
                "ssrc": parsed.get("ssrc"),
                "stage": "dave_drop",
                "dave_drop": True,
                "pre_dave_payload_len": len(pre_dave_payload),
                "pre_dave_opus_frames": opus_packet_frame_count(pre_dave_payload),
                "pre_dave_opus_samples": opus_packet_sample_count(pre_dave_payload),
                "pre_dave_encrypted_marker": is_dave_encrypted_payload(pre_dave_payload),
                "payload": pre_dave_payload,
            }
            for item in jitter.add(parsed["sequence"], (None, packet_info)):
                if item is None:
                    self.jitter_missing_count += 1
                    self._decode_and_segment(user_id, None, packet_info={"missing": True, "stage": "post_dave_jitter"})
                else:
                    opus_payload, traced_packet = item
                    if opus_payload is None:
                        self.jitter_missing_count += 1
                    self._decode_and_segment(user_id, opus_payload, packet_info=traced_packet)
            return
        if opus_packet_is_valid(payload):
            self.post_dave_opus_valid_count += 1
        else:
            self.post_dave_opus_invalid_count += 1
            self._log_invalid_opus(user_id, parsed, payload)
        packet_info = {
            "sequence": parsed.get("sequence"),
            "timestamp": parsed.get("timestamp"),
            "ssrc": parsed.get("ssrc"),
            "stage": "post_dave",
            "payload_len": len(payload),
            "opus_frames": opus_packet_frame_count(payload),
            "opus_samples": opus_packet_sample_count(payload),
            "payload": payload,
        }
        for item in jitter.add(parsed["sequence"], (payload, packet_info)):
            if item is None:
                self.jitter_missing_count += 1
                self._decode_and_segment(user_id, None, packet_info={"missing": True, "stage": "post_dave_jitter"})
            else:
                opus_payload, traced_packet = item
                if opus_payload is None:
                    self.jitter_missing_count += 1
                self._decode_and_segment(user_id, opus_payload, packet_info=traced_packet)

    def _decode_and_segment(self, user_id: str, opus_payload: bytes | None, *, packet_info: dict | None = None):
        decoder = self.decoders.get(user_id)
        if decoder is None:
            decoder = self._create_decoder()
            self.decoders[user_id] = decoder
        if isinstance(decoder, LibOpusPcmDecoder):
            decoded = decoder.decode(opus_payload)
            if decoded is None:
                self._log_decode_error(user_id, f"libopus rejected packet; inserting PLC; {probe_libopus_payload(opus_payload)}")
                decoded = decoder.decode_missing()
            if decoded is None:
                return
            self.decode_frame_count += 1
            self._segment_pcm(user_id, *decoded, packet_info=packet_info)
            return

        if opus_payload is None:
            return
        try:
            frames = decoder.decode(av.packet.Packet(opus_payload))
        except Exception as exc:
            self._log_decode_error(user_id, exc)
            return
        for frame in frames:
            self.decode_frame_count += 1
            for pcm, sample_rate, channels in self._frame_to_pcm16_mono_16k(user_id, frame):
                self._segment_pcm(user_id, pcm, sample_rate, channels, packet_info=packet_info)

    def _log_invalid_opus(self, user_id: str, parsed: dict, payload: bytes):
        now = time.time()
        if now - self.last_invalid_opus_log_at < 5:
            return
        self.last_invalid_opus_log_at = now
        head = payload[:8].hex()
        tail = payload[-8:].hex() if len(payload) >= 8 else payload.hex()
        self.log(
            f"Invalid post-DAVE Opus packet for {self.name_for_user(user_id)}: "
            f"seq={parsed.get('sequence')} ts={parsed.get('timestamp')} len={len(payload)} head={head} tail={tail} "
            f"pre_valid/invalid={self.pre_dave_opus_valid_count}/{self.pre_dave_opus_invalid_count} "
            f"post_valid/invalid={self.post_dave_opus_valid_count}/{self.post_dave_opus_invalid_count}"
        )

    def _create_decoder(self):
        if has_opus_decoder():
            return LibOpusPcmDecoder()
        if av is None:
            raise RuntimeError("No Opus decoder available")
        return av.codec.CodecContext.create("opus", "r")

    def _log_decode_error(self, user_id: str, error):
        self.decode_error_count += 1
        now = time.time()
        if now - self.last_decode_error_log_at >= 5:
            self.last_decode_error_log_at = now
            self.log(f"Opus decode failed for {self.name_for_user(user_id)} ({self.decode_error_count} total): {error}")

    def _segment_pcm(self, user_id: str, pcm: bytes, sample_rate: int, channels: int, *, packet_info: dict | None = None):
        if not pcm:
            return
        duration = len(pcm) / (sample_rate * channels * 2)
        segmenter = self.segmenters.get(user_id)
        if segmenter is None:
            segmenter = SpeakerSegmenter(user_id, self.name_for_user, self.transcriber.submit, sample_rate=sample_rate, channels=channels)
            self.segmenters[user_id] = segmenter
        segmenter.add_pcm(pcm, duration, packet_info=packet_info)

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
