"""One-shot outgoing call audio helpers used by `discord call say`."""

from __future__ import annotations

from pathlib import Path
import os
import shutil
import socket
import struct
import subprocess
import time

from src.calls.transport import OPUS_PAYLOAD_TYPE, OPUS_RTP_CLOCK_INCREMENT, encrypt_voice_transport, parse_plain_rtp_packet


DEFAULT_VOICE_ENGINE = "discord-voice-engine"
SPEAKING_KEEPALIVE_SECONDS = 1.0
OPUS_SILENCE_FRAME = b"\xf8\xff\xfe"
CALL_SAY_PREROLL_MS = max(0, int(os.environ.get("DISCORD_CALL_SAY_PREROLL_MS", "1000") or "0"))
CALL_SAY_PREROLL_PACKETS = CALL_SAY_PREROLL_MS // 20
CALL_SAY_VOLUME_PERCENT = None


def parse_call_say_volume_percent():
    raw = os.environ.get("DISCORD_CALL_SAY_VOLUME_PERCENT", os.environ.get("DISCORD_CALL_SAY_VOLUME", "50"))
    try:
        percent = float(str(raw).strip().rstrip("%"))
    except Exception:
        percent = 50.0
    return max(0.0, min(200.0, percent))


def call_say_volume_percent():
    global CALL_SAY_VOLUME_PERCENT
    if CALL_SAY_VOLUME_PERCENT is None:
        CALL_SAY_VOLUME_PERCENT = parse_call_say_volume_percent()
    return CALL_SAY_VOLUME_PERCENT


def format_volume_gain(percent):
    return f"{max(0.0, float(percent)) / 100.0:.6g}"


def find_voice_engine():
    configured = os.environ.get("DISCORD_VOICE_ENGINE")
    if configured:
        expanded = Path(configured).expanduser()
        if expanded.exists():
            return str(expanded)
        return configured
    return shutil.which(DEFAULT_VOICE_ENGINE)


def build_voice_engine_file_command(engine, audio_path, relay_port):
    return [
        engine,
        "encode-file",
        "--input", str(audio_path),
        "--rtp", f"127.0.0.1:{relay_port}",
        "--mode", "music",
        "--channels", "2",
        "--bitrate", "192000",
        "--payload-type", str(OPUS_PAYLOAD_TYPE),
    ]


def build_ffmpeg_file_command(audio_path, relay_port, volume_percent=100.0):
    command = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "error",
        "-re",
        "-i", str(audio_path),
        "-vn",
        "-ac", "2",
        "-ar", "48000",
    ]
    if float(volume_percent) != 100.0:
        command.extend(["-filter:a", f"volume={format_volume_gain(volume_percent)}"])
    command.extend([
        "-c:a", "libopus",
        "-application", "audio",
        "-b:a", "192000",
        "-frame_duration", "20",
        "-payload_type", str(OPUS_PAYLOAD_TYPE),
        "-f", "rtp",
        f"rtp://127.0.0.1:{relay_port}",
    ])
    return command


def build_file_command(voice_engine, audio_path, relay_port, volume_percent):
    ffmpeg = shutil.which("ffmpeg")
    if float(volume_percent) != 100.0:
        if not ffmpeg:
            raise RuntimeError("ffmpeg is required for discord call say volume adjustment")
        return "ffmpeg", build_ffmpeg_file_command(audio_path, relay_port, volume_percent)
    if voice_engine:
        return "discord-voice-engine", build_voice_engine_file_command(voice_engine, audio_path, relay_port)
    return "ffmpeg", build_ffmpeg_file_command(audio_path, relay_port, volume_percent)


def send_audio_file(worker, path):
    """Decode an audio file with ffmpeg and send it through an active call worker.

    `worker` is the call worker object. Keeping this code here isolates file send
    media plumbing from the voice-gateway/control lifecycle in calling.py without
    introducing another large class hierarchy.
    """
    audio_path = Path(path).expanduser()
    if not audio_path.exists() or not audio_path.is_file():
        raise RuntimeError(f"audio file not found: {audio_path}")
    voice_engine = find_voice_engine()
    volume_percent = call_say_volume_percent()
    if not voice_engine and not shutil.which("ffmpeg"):
        raise RuntimeError("discord-voice-engine or ffmpeg is required for discord call say")
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
    preroll_sent = 0
    dropped = 0
    stderr_chunks = []
    try:
        worker._send_speaking(True)
        next_speaking_keepalive = time.monotonic() + SPEAKING_KEEPALIVE_SECONDS
        preroll_sent = send_outgoing_silence_preroll(worker, transcription, CALL_SAY_PREROLL_PACKETS)
        backend, command = build_file_command(voice_engine, audio_path, relay_port, volume_percent)
        proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        while worker.running:
            now = time.monotonic()
            if now >= next_speaking_keepalive:
                worker._send_speaking(True)
                next_speaking_keepalive = now + SPEAKING_KEEPALIVE_SECONDS
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
            raise RuntimeError(f"{backend} exited with {code}: {''.join(stderr_chunks).strip()}")
        dave = getattr(transcription, "dave", None)
        dave_suffix = ""
        if dave is not None:
            dave_suffix = (
                f", dave_out={getattr(dave, 'outgoing_encrypt_count', 0)}/"
                f"{getattr(dave, 'outgoing_passthrough_count', 0)}/"
                f"{getattr(dave, 'outgoing_encrypt_failure_count', 0)}"
            )
        print(f"Sent call audio {audio_path} ({sent} RTP packet(s), {dropped} dropped, {preroll_sent} preroll silence packet(s), volume={volume_percent:g}%, backend={backend}{dave_suffix}).", flush=True)
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
    return send_outgoing_opus_payload(worker, opus_payload, transcription)


def send_outgoing_silence_preroll(worker, transcription, packet_count):
    sent = 0
    start = time.monotonic()
    for index in range(max(0, int(packet_count))):
        if not worker.running:
            break
        if send_outgoing_opus_payload(worker, OPUS_SILENCE_FRAME, transcription):
            sent += 1
        target = start + ((index + 1) * OPUS_RTP_CLOCK_INCREMENT / 48_000)
        delay = target - time.monotonic()
        if delay > 0:
            time.sleep(delay)
    return sent


def send_outgoing_opus_payload(worker, opus_payload, transcription):
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
