"""Outgoing Discord Opus/RTP media transport."""

from __future__ import annotations

import struct

from src.calls.transport import OPUS_PAYLOAD_TYPE, OPUS_RTP_CLOCK_INCREMENT, encrypt_voice_transport


def send_outgoing_opus_payload(worker, opus_payload, media):
    encoded = media.encode_outgoing_opus(opus_payload)
    if not encoded:
        return False
    sequence = worker._send_sequence & 0xFFFF
    timestamp = worker._send_timestamp & 0xFFFFFFFF
    header = bytearray(12)
    header[0] = 0x80
    header[1] = OPUS_PAYLOAD_TYPE
    struct.pack_into("!HII", header, 2, sequence, timestamp, int(worker.voice_ssrc))
    worker._send_sequence = (worker._send_sequence + 1) & 0xFFFF
    worker._send_timestamp = (worker._send_timestamp + OPUS_RTP_CLOCK_INCREMENT) & 0xFFFFFFFF
    counter = worker.next_send_counter()
    worker.voice_udp.send(encrypt_voice_transport(
        bytes(header),
        encoded,
        mode=worker.voice_mode,
        secret_key=worker.voice_secret_key,
        counter=counter,
    ))
    return True
