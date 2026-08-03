"""Receive, decrypt, decode, and emit Discord voice media."""

from __future__ import annotations

from collections import deque
import base64
import ctypes
import ctypes.util
import os
import socket
import struct
import threading
import time

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
DEFAULT_JITTER_PACKETS = 12
DEFAULT_JITTER_RESYNC_GAP = 120
DEFAULT_UNKNOWN_SSRC_BUFFER_PACKETS = 750
OPUS_SAMPLE_RATE = 48_000
OPUS_FRAME_SAMPLES = 960  # 20 ms at 48 kHz.
OPUS_MAX_FRAME_SAMPLES = 5_760
OPUS_SILENCE_FRAME = b"\xf8\xff\xfe"


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default

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
    onto the sender's key ratchet; mirror that shape here so the media adapter sees
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
        self.outgoing_encrypt_count = 0
        self.outgoing_passthrough_count = 0
        self.outgoing_encrypt_failure_count = 0
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
            if is_dave_encrypted_payload(payload):
                return None
            self.passthrough_count += 1
            return payload
        if not self.session.has_established_group():
            if is_dave_encrypted_payload(payload):
                return None
            self.passthrough_count += 1
            return payload
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
            # This happens during Discord/DAVE transition windows when a packet
            # is still encrypted/padded after the current decryptor ratchet. It
            # is accounted for in periodic stats as dave_encrypted_drop; do not
            # print one line per packet into the live call log.
            return None
        return bytes(decoded)

    def _outgoing_dave_mode(self) -> str:
        raw = (os.environ.get("DISCORD_CALL_SAY_DAVE_MODE") or "auto").strip().lower()
        if raw in {"off", "plain", "plaintext", "passthrough"}:
            return "plaintext"
        if raw in {"on", "encrypt", "encrypted", "dave"}:
            return "encrypt"
        return "auto"

    def _can_encrypt_outgoing(self) -> bool:
        if self.protocol_version <= 0 or self.session is None or self.encryptor is None or dave is None or self.self_ssrc is None:
            return False
        try:
            if not self.session.has_established_group():
                return False
            if not self.encryptor.has_key_ratchet():
                self._transition_encryptor()
            return bool(self.encryptor.has_key_ratchet())
        except Exception:
            return False

    def encode_outgoing_opus(self, payload: bytes):
        mode = self._outgoing_dave_mode()
        if mode == "plaintext" or self.protocol_version <= 0:
            self.outgoing_passthrough_count += 1
            return payload
        if not self._can_encrypt_outgoing():
            if mode == "auto":
                self.outgoing_passthrough_count += 1
                return payload
            self.outgoing_encrypt_failure_count += 1
            return None
        try:
            encoded = self.encryptor.encrypt(dave.MediaType.audio, int(self.self_ssrc), bytes(payload))
        except Exception as exc:
            self.outgoing_encrypt_failure_count += 1
            self.report_error(f"DAVE outgoing audio encryption failed: {exc}")
            return None
        if encoded is None:
            self.outgoing_encrypt_failure_count += 1
            return None
        self.outgoing_encrypt_count += 1
        return bytes(encoded)

    def can_encode_outgoing(self) -> bool:
        mode = self._outgoing_dave_mode()
        if mode == "plaintext" or self.protocol_version <= 0:
            return True
        if mode == "encrypt":
            return self._can_encrypt_outgoing()
        return True if self.protocol_version <= 0 else self._can_encrypt_outgoing()

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


def sequence_forward_distance(seq: int, expected: int) -> int:
    return (int(seq) - int(expected)) & 0xFFFF


class RtpJitterBuffer:
    def __init__(self, *, max_packets: int = DEFAULT_JITTER_PACKETS, max_resync_gap: int = DEFAULT_JITTER_RESYNC_GAP):
        self.max_packets = max(0, int(max_packets))
        self.max_resync_gap = max(0, int(max_resync_gap))
        self.expected = None
        self.buffer = {}
        self.resync_count = 0

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
                next_sequence = min(self.buffer, key=lambda seq: sequence_forward_distance(seq, self.expected))
                gap = sequence_forward_distance(next_sequence, self.expected)
                if self.max_resync_gap > 0 and gap > self.max_resync_gap:
                    # Large jumps happen at join/DAVE transition boundaries and
                    # after Discord drops idle/DTX packets.  Backfilling the whole
                    # gap with PLC creates seconds of synthetic silence, delaying
                    # or suppressing real speech. Treat those jumps
                    # as a stream boundary instead of as thousands of lost frames.
                    self.expected = next_sequence
                    self.resync_count += 1
                    continue
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
            channels = env_int("DISCORD_CALL_MEDIA_OPUS_CHANNELS", 1)
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


class VoiceReceiveMedia:
    @staticmethod
    def advertised_dave_protocol_version_static():
        return int(dave.get_max_supported_protocol_version()) if dave is not None else 0

    def __init__(self, *, udp=None, mode: str | None = None, secret_key=None, self_user_id: str, channel_id: str, send_json, send_binary, name_for_user, pcm_sink, log=print):
        self.udp = udp
        self.mode = mode
        self.secret_key = bytes(secret_key or b"")
        self.self_user_id = str(self_user_id)
        self.channel_id = str(channel_id)
        self.send_json = send_json
        self.send_binary = send_binary
        self.name_for_user = name_for_user
        self.log = log
        self.pcm_sink = pcm_sink
        self.running = False
        self.thread = None
        self.ssrc_to_user_id = {}
        self.fallback_user_ids = set()
        self.unknown_ssrc_buffers = {}
        self.unknown_ssrc_buffer_limit = env_int("DISCORD_CALL_MEDIA_UNKNOWN_SSRC_BUFFER_PACKETS", DEFAULT_UNKNOWN_SSRC_BUFFER_PACKETS)
        self.decoders = {}
        self.resamplers = {}
        self.pre_dave_jitter_buffers = {}
        self.jitter_buffers = {}
        self.jitter_missing_count = 0
        self.packet_count = 0
        self.unknown_ssrc_count = 0
        self.unknown_ssrc_buffered_count = 0
        self.unknown_ssrc_replayed_count = 0
        self.unknown_ssrc_dropped_count = 0
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
            self.log("Discord media receiver waiting for a voice session")
            return
        if not has_opus_decoder() and av is None:
            self.log("Discord media receiver disabled: neither libopus nor PyAV is available")
            return
        if nacl is None:
            self.log("Discord media receiver disabled: PyNaCl is not installed")
            return
        self.running = True
        self.thread = threading.Thread(target=self._recv_loop, name="discord-voice-receive", daemon=True)
        self.thread.start()
        self.log("Discord media receiver started")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
            self.thread = None
    def set_active_remote_users(self, user_ids):
        self.fallback_user_ids = {str(user_id) for user_id in (user_ids or []) if user_id is not None and str(user_id) != self.self_user_id}
        self.dave.add_known_users(self.fallback_user_ids)

    def set_user_speaking(self, user_id, speaking):
        if user_id is None or str(user_id) == self.self_user_id:
            return
        user_id = str(user_id)
        self.fallback_user_ids.add(user_id)
        self.dave.add_known_users([user_id])

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
        stale_user_ssrcs = [mapped_ssrc for mapped_ssrc, mapped_user in self.ssrc_to_user_id.items() if mapped_user == user_id and mapped_ssrc != ssrc]
        if stale_user_ssrcs:
            self._reset_user_media_state(user_id)
            for mapped_ssrc in stale_user_ssrcs:
                self.ssrc_to_user_id.pop(mapped_ssrc, None)
                self.dave.ssrc_to_user_id.pop(mapped_ssrc, None)
                self.dave.ssrc_to_decryptor.pop(mapped_ssrc, None)
        self.ssrc_to_user_id[ssrc] = user_id
        self.fallback_user_ids.add(user_id)
        self.dave.add_ssrc_mapping(ssrc, user_id)
        if previous != user_id:
            self.log(f"Discord media mapped SSRC {ssrc} to {self.name_for_user(user_id)}")
        if previous != user_id:
            self._drain_unknown_ssrc_buffer(ssrc, user_id)

    def remove_user(self, user_id):
        user_id = str(user_id)
        self.fallback_user_ids.discard(user_id)
        self.dave.remove_known_user(user_id)
        for ssrc, mapped_user in list(self.ssrc_to_user_id.items()):
            if mapped_user == user_id:
                del self.ssrc_to_user_id[ssrc]
        self._reset_user_media_state(user_id)

    def _reset_user_media_state(self, user_id):
        user_id = str(user_id)
        decoder = self.decoders.pop(user_id, None)
        if hasattr(decoder, "close"):
            try:
                decoder.close()
            except Exception:
                pass
        self.resamplers.pop(user_id, None)
        self.pre_dave_jitter_buffers.pop(user_id, None)
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
                        self._decode_and_emit(user_id, None, packet_info={"missing": True, "stage": "post_dave_jitter"})
                    else:
                        opus_payload, traced_packet = item
                        if opus_payload is None:
                            self.jitter_missing_count += 1
                        self._decode_and_emit(user_id, opus_payload, packet_info=traced_packet)
        now = time.time()
        if now - self.last_stats_at >= 10:
            self.last_stats_at = now
            if self.packet_count or self.decrypt_count or self.decode_frame_count or self.ssrc_to_user_id:
                self.log(
                    f"Discord media stats: packets={self.packet_count} unknown_ssrc={self.unknown_ssrc_count} "
                    f"unknown_buffered={self.unknown_ssrc_buffered_count}/{self.unknown_ssrc_replayed_count}/{self.unknown_ssrc_dropped_count} "
                    f"self={self.self_packet_count} transport_fail={self.transport_decrypt_fail_count} "
                    f"ext={self.extension_packet_count}/{self.extension_bytes_total}B decrypted={self.decrypt_count} "
                    f"pre_dave_opus={self.pre_dave_opus_valid_count}/{self.pre_dave_opus_invalid_count} "
                    f"dave_drop={self.dave_drop_count} dave_passthrough={self.dave.passthrough_count} "
                    f"dave_decrypt_fail={self.dave.decrypt_failure_count} dave_encrypted_drop={self.dave.encrypted_drop_count} "
                    f"dave_padding_trim={self.dave.padding_trim_count}/{self.dave.padding_trim_bytes}B "
                    f"dave_out={self.dave.outgoing_encrypt_count}/{self.dave.outgoing_passthrough_count}/{self.dave.outgoing_encrypt_failure_count} "
                    f"post_dave_opus={self.post_dave_opus_valid_count}/{self.post_dave_opus_invalid_count} "
                    f"frames={self.decode_frame_count} decode_errors={self.decode_error_count} "
                    f"jitter_missing={self.jitter_missing_count} "
                    f"jitter_resync={self._pre_dave_jitter_resync_count()}/{self._post_dave_jitter_resync_count()} "
                    f"ssrcs={len(self.ssrc_to_user_id)}"
                )

    def _pre_dave_jitter_resync_count(self) -> int:
        return sum(getattr(jitter, "resync_count", 0) for jitter in self.pre_dave_jitter_buffers.values())

    def _post_dave_jitter_resync_count(self) -> int:
        return sum(getattr(jitter, "resync_count", 0) for jitter in self.jitter_buffers.values())

    def _handle_packet(self, packet: bytes):
        parsed = parse_rtp_packet(packet)
        if not parsed or parsed["payload_type"] != OPUS_PAYLOAD_TYPE:
            return
        self.packet_count += 1
        user_id = self.ssrc_to_user_id.get(parsed["ssrc"])
        if not user_id and len(self.fallback_user_ids) == 1:
            user_id = next(iter(self.fallback_user_ids))
            self.add_ssrc_mapping(parsed["ssrc"], user_id)
            self.log(f"Discord media inferred SSRC {parsed['ssrc']} for {self.name_for_user(user_id)}")
        if not user_id:
            self.unknown_ssrc_count += 1
            self._buffer_unknown_ssrc_packet(parsed, packet)
            return
        self._handle_mapped_packet(parsed, packet, user_id)

    def _buffer_unknown_ssrc_packet(self, parsed: dict, packet: bytes):
        if self.unknown_ssrc_buffer_limit <= 0:
            return
        ssrc = int(parsed["ssrc"])
        buffer = self.unknown_ssrc_buffers.get(ssrc)
        if buffer is None:
            buffer = deque(maxlen=self.unknown_ssrc_buffer_limit)
            self.unknown_ssrc_buffers[ssrc] = buffer
        before = len(buffer)
        buffer.append((dict(parsed), bytes(packet)))
        self.unknown_ssrc_buffered_count += 1
        if len(buffer) == before:
            self.unknown_ssrc_dropped_count += 1

    def _drain_unknown_ssrc_buffer(self, ssrc: int, user_id: str):
        buffer = self.unknown_ssrc_buffers.pop(int(ssrc), None)
        if not buffer:
            return
        count = len(buffer)
        self.unknown_ssrc_replayed_count += count
        self.log(f"Discord media replaying {count} buffered packet(s) for {self.name_for_user(user_id)} after SSRC {ssrc} mapping")
        for parsed, packet in buffer:
            self._handle_mapped_packet(parsed, packet, user_id)

    def _handle_mapped_packet(self, parsed: dict, packet: bytes, user_id: str):
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
            jitter = RtpJitterBuffer(
                max_packets=env_int("DISCORD_CALL_MEDIA_PRE_DAVE_JITTER_PACKETS", DEFAULT_JITTER_PACKETS),
                max_resync_gap=env_int("DISCORD_CALL_MEDIA_JITTER_RESYNC_GAP", DEFAULT_JITTER_RESYNC_GAP),
            )
            self.pre_dave_jitter_buffers[user_id] = jitter
        for item in jitter.add(parsed["sequence"], (dict(parsed), payload)):
            self._handle_ordered_pre_dave_item(user_id, item)

    def _handle_ordered_pre_dave_item(self, user_id: str, item):
        if item is None:
            self.jitter_missing_count += 1
            self._decode_and_emit(user_id, None, packet_info={"missing": True, "stage": "pre_dave_jitter"})
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
            jitter = RtpJitterBuffer(
                max_packets=env_int("DISCORD_CALL_MEDIA_JITTER_PACKETS", DEFAULT_JITTER_PACKETS),
                max_resync_gap=env_int("DISCORD_CALL_MEDIA_JITTER_RESYNC_GAP", DEFAULT_JITTER_RESYNC_GAP),
            )
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
                    self._decode_and_emit(user_id, None, packet_info={"missing": True, "stage": "post_dave_jitter"})
                else:
                    opus_payload, traced_packet = item
                    if opus_payload is None:
                        self.jitter_missing_count += 1
                    self._decode_and_emit(user_id, opus_payload, packet_info=traced_packet)
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
                self._decode_and_emit(user_id, None, packet_info={"missing": True, "stage": "post_dave_jitter"})
            else:
                opus_payload, traced_packet = item
                if opus_payload is None:
                    self.jitter_missing_count += 1
                self._decode_and_emit(user_id, opus_payload, packet_info=traced_packet)

    def _decode_and_emit(self, user_id: str, opus_payload: bytes | None, *, packet_info: dict | None = None):
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
            self._emit_pcm(user_id, *decoded, packet_info=packet_info)
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
            for pcm, sample_rate, channels in self._frame_to_pcm16_mono_48k(user_id, frame):
                self._emit_pcm(user_id, pcm, sample_rate, channels, packet_info=packet_info)

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

    def _emit_pcm(self, user_id: str, pcm: bytes, sample_rate: int, channels: int, *, packet_info: dict | None = None):
        if pcm:
            self.pcm_sink(user_id, pcm, sample_rate, channels)

    def _frame_to_pcm16_mono_48k(self, user_id: str, frame):
        resampler = self.resamplers.get(user_id)
        if resampler is None:
            resampler = av.audio.resampler.AudioResampler(format="s16", layout="mono", rate=48000)
            self.resamplers[user_id] = resampler
        try:
            frames = resampler.resample(frame)
        except Exception as exc:
            self.log(f"Audio resample failed for {self.name_for_user(user_id)}: {exc}")
            frames = [frame]
        result = []
        for out in frames:
            sample_rate = int(getattr(out, "sample_rate", None) or getattr(out, "rate", None) or 48000)
            channels = 1
            valid_bytes = int(out.samples or 0) * channels * 2
            plane = bytes(out.planes[0])
            pcm = plane[:valid_bytes] if valid_bytes > 0 else plane
            result.append((pcm, sample_rate, channels))
        return result


SOCKET_TIMEOUT_EXCEPTIONS = (socket.timeout, TimeoutError)
