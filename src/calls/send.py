"""One-shot outgoing call audio helpers used by `discord call say`."""

from __future__ import annotations

from pathlib import Path
import shutil
import socket
import struct
import subprocess
import time

from src.calls.transport import OPUS_PAYLOAD_TYPE, OPUS_RTP_CLOCK_INCREMENT, encrypt_voice_transport, parse_plain_rtp_packet


def send_audio_file(worker, path):
    """Decode an audio file with ffmpeg and send it through an active call worker.

    `worker` is the call worker object. Keeping this code here isolates file send
    media plumbing from the voice-gateway/control lifecycle in calling.py without
    introducing another large class hierarchy.
    """
    audio_path = Path(path).expanduser()
    if not audio_path.exists() or not audio_path.is_file():
        raise RuntimeError(f"audio file not found: {audio_path}")
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg is required for discord call say")
    if not worker.voice_ready or not worker.voice_udp or worker.voice_ssrc is None or not worker.voice_secret_key or not worker.voice_mode:
        raise RuntimeError("call is not voice-ready yet")
    transcription = worker._voice_transcription
    if not transcription:
        raise RuntimeError("voice media state is not initialized")
    deadline = time.time() + 10
    while worker.running and time.time() < deadline and not transcription.can_encode_outgoing():
        time.sleep(0.1)
    if not transcription.can_encode_outgoing():
        raise RuntimeError("voice media send path is not ready yet")

    previous_mute = worker.self_mute
    if previous_mute:
        worker.self_mute = False
        worker._request_voice_state(worker.channel_id)
        worker.update_call_meta(self_mute=False, updated_at=time.time())
        time.sleep(0.35)

    relay = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    relay.settimeout(0.5)
    relay.bind(("127.0.0.1", 0))
    relay_port = relay.getsockname()[1]
    proc = None
    sent = 0
    dropped = 0
    stderr_chunks = []
    try:
        worker._send_speaking(True)
        proc = subprocess.Popen([
            "ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-loglevel", "error",
            "-re",
            "-i", str(audio_path),
            "-vn",
            "-ac", "1",
            "-ar", "48000",
            "-c:a", "libopus",
            "-application", "voip",
            "-frame_duration", "20",
            "-payload_type", str(OPUS_PAYLOAD_TYPE),
            "-f", "rtp",
            f"rtp://127.0.0.1:{relay_port}",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        while worker.running:
            try:
                packet, _addr = relay.recvfrom(4096)
                if forward_outgoing_rtp_packet(worker, packet, transcription):
                    sent += 1
                else:
                    dropped += 1
            except socket.timeout:
                if proc.poll() is not None:
                    break
            if proc.poll() is not None:
                # Drain any packet already queued by ffmpeg before exiting.
                relay.settimeout(0.05)
        if proc.stderr:
            try:
                stderr_chunks.append(proc.stderr.read().decode("utf-8", errors="replace"))
            except Exception:
                pass
        code = proc.wait(timeout=2)
        if code != 0:
            raise RuntimeError(f"ffmpeg exited with {code}: {''.join(stderr_chunks).strip()}")
        print(f"Sent call audio {audio_path} ({sent} RTP packet(s), {dropped} dropped).", flush=True)
    finally:
        worker._send_speaking(False)
        try:
            relay.close()
        except Exception:
            pass
        if proc and proc.poll() is None:
            proc.terminate()
        if previous_mute and worker.running:
            worker.self_mute = True
            worker._request_voice_state(worker.channel_id)
            worker.update_call_meta(self_mute=True, updated_at=time.time())


def forward_outgoing_rtp_packet(worker, packet, transcription):
    parsed = parse_plain_rtp_packet(packet)
    if not parsed or parsed["payload_type"] != OPUS_PAYLOAD_TYPE:
        return False
    opus_payload = parsed["payload"]
    if not opus_payload:
        return False
    encoded = transcription.encode_outgoing_opus(opus_payload)
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
    worker.voice_udp.send(encrypt_voice_transport(bytes(header), encoded, mode=worker.voice_mode, secret_key=worker.voice_secret_key, counter=counter))
    return True
