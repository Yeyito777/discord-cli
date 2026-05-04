"""Discord voice RTP transport helpers."""

from __future__ import annotations

import socket
import struct

try:
    import nacl.bindings  # type: ignore
except Exception:  # pragma: no cover - optional deployment dependency
    nacl = None

VOICE_UDP_TIMEOUT = 5
OPUS_PAYLOAD_TYPE = 120
OPUS_RTP_CLOCK_INCREMENT = 960


def select_encryption_mode(modes):
    if "aead_aes256_gcm_rtpsize" in modes:
        return "aead_aes256_gcm_rtpsize"
    if "aead_xchacha20_poly1305_rtpsize" in modes:
        return "aead_xchacha20_poly1305_rtpsize"
    return modes[0] if modes else "aead_aes256_gcm_rtpsize"


def udp_discovery(host, port, ssrc):
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


def encrypt_voice_transport(header, payload, *, mode, secret_key, counter):
    if nacl is None:
        raise RuntimeError("PyNaCl is required for Discord voice transport encryption")
    header = bytes(header)
    payload = bytes(payload)
    secret_key = bytes(secret_key)
    if mode == "aead_aes256_gcm_rtpsize":
        nonce = counter + (b"\x00" * 8)
        encrypted = nacl.bindings.crypto_aead_aes256gcm_encrypt(payload, header, nonce, secret_key)
    elif mode == "aead_xchacha20_poly1305_rtpsize":
        nonce = counter + (b"\x00" * 20)
        encrypted = nacl.bindings.crypto_aead_xchacha20poly1305_ietf_encrypt(payload, header, nonce, secret_key)
    else:
        raise RuntimeError(f"unsupported Discord voice encryption mode: {mode}")
    return header + encrypted + counter


def parse_plain_rtp_packet(packet):
    if len(packet) < 12 or packet[0] >> 6 != 2:
        return None
    csrc_count = packet[0] & 0x0F
    has_extension = bool(packet[0] & 0x10)
    payload_type = packet[1] & 0x7F
    offset = 12 + csrc_count * 4
    if len(packet) < offset:
        return None
    if has_extension:
        if len(packet) < offset + 4:
            return None
        offset += 4 + int.from_bytes(packet[offset + 2:offset + 4], "big") * 4
    if len(packet) <= offset:
        return None
    return {
        "payload_type": payload_type,
        "payload": packet[offset:],
    }
