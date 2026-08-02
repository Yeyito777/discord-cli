"""WebRTC bridge between Discord voice media and an Exocortex Bidi call."""

from __future__ import annotations

from array import array
import asyncio
from collections import defaultdict, deque
import fractions
import math
import threading
import time

try:  # Optional until Bidi mode is selected.
    import av  # type: ignore
    from aiortc import AudioStreamTrack, RTCPeerConnection, RTCSessionDescription  # type: ignore
except Exception:  # pragma: no cover - deployment dependency validation
    av = None
    AudioStreamTrack = object
    RTCPeerConnection = None
    RTCSessionDescription = None

from src.calls.send import send_outgoing_opus_payload


SAMPLE_RATE = 48_000
FRAME_SAMPLES = 960
FRAME_SECONDS = FRAME_SAMPLES / SAMPLE_RATE
MAX_QUEUED_FRAMES_PER_USER = 12
OUTPUT_SILENCE_THRESHOLD_DB = -58.0
OUTPUT_SPEECH_HANGOVER_SECONDS = 0.3


def _mono_48k(pcm: bytes, sample_rate: int, channels: int) -> bytes:
    """Normalize decoded Discord PCM to signed 16-bit mono at 48 kHz."""
    if not pcm:
        return b""
    channels = max(1, int(channels or 1))
    sample_rate = int(sample_rate or SAMPLE_RATE)
    if sample_rate == SAMPLE_RATE:
        samples = array("h")
        samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
        if channels == 1:
            return samples.tobytes()
        mono = array("h")
        usable = len(samples) - (len(samples) % channels)
        for offset in range(0, usable, channels):
            mono.append(int(sum(samples[offset:offset + channels]) / channels))
        return mono.tobytes()
    if av is None:
        raise RuntimeError("PyAV is required to resample Discord Bidi audio")
    layout = "mono" if channels == 1 else "stereo"
    sample_count = len(pcm) // (2 * channels)
    frame = av.AudioFrame(format="s16", layout=layout, samples=sample_count)
    frame.planes[0].update(pcm)
    frame.sample_rate = sample_rate
    resampler = av.AudioResampler(format="s16", layout="mono", rate=SAMPLE_RATE)
    chunks = []
    for output in resampler.resample(frame):
        chunks.append(bytes(output.planes[0])[: int(output.samples) * 2])
    return b"".join(chunks)


def _frame_rms_db(frame) -> float:
    channels = len(getattr(frame.layout, "channels", ()) or ()) or 1
    valid = int(frame.samples or 0) * channels * 2
    raw = bytes(frame.planes[0])[:valid]
    samples = array("h")
    samples.frombytes(raw[: len(raw) - (len(raw) % 2)])
    if not samples:
        return -120.0
    mean_square = sum((sample / 32768.0) ** 2 for sample in samples) / len(samples)
    return 20 * math.log10(max(mean_square ** 0.5, 1e-6))


class DiscordInputAudioTrack(AudioStreamTrack):
    """Paced mono track mixing one 20 ms frame from every Discord speaker."""

    kind = "audio"

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self._queues = defaultdict(lambda: deque(maxlen=MAX_QUEUED_FRAMES_PER_USER))
        self._remainders = defaultdict(bytearray)
        self._start = None
        self._timestamp = 0

    def push_pcm(self, user_id: str, pcm: bytes, sample_rate: int, channels: int):
        normalized = _mono_48k(pcm, sample_rate, channels)
        if not normalized:
            return
        frame_bytes = FRAME_SAMPLES * 2
        with self._lock:
            remainder = self._remainders[str(user_id)]
            remainder.extend(normalized)
            queue = self._queues[str(user_id)]
            while len(remainder) >= frame_bytes:
                queue.append(bytes(remainder[:frame_bytes]))
                del remainder[:frame_bytes]

    def _mixed_frame(self) -> bytes:
        with self._lock:
            chunks = [queue.popleft() for queue in self._queues.values() if queue]
        if not chunks:
            return bytes(FRAME_SAMPLES * 2)
        mixed = array("h", [0]) * FRAME_SAMPLES
        divisor = max(1.0, len(chunks) ** 0.5)
        for chunk in chunks:
            samples = array("h")
            samples.frombytes(chunk)
            for index, sample in enumerate(samples[:FRAME_SAMPLES]):
                value = mixed[index] + int(sample / divisor)
                mixed[index] = max(-32768, min(32767, value))
        return mixed.tobytes()

    async def recv(self):
        if self._start is None:
            self._start = time.time()
            self._timestamp = 0
        else:
            self._timestamp += FRAME_SAMPLES
            delay = self._start + self._timestamp / SAMPLE_RATE - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
        frame = av.AudioFrame(format="s16", layout="mono", samples=FRAME_SAMPLES)
        frame.planes[0].update(self._mixed_frame())
        frame.pts = self._timestamp
        frame.sample_rate = SAMPLE_RATE
        frame.time_base = fractions.Fraction(1, SAMPLE_RATE)
        return frame


class DiscordBidiBridge:
    """Own one aiortc peer and route its audio through an active Discord worker."""

    def __init__(self, worker, exocortex, conv_id: str, call_id: str, *, log=print):
        if RTCPeerConnection is None or av is None:
            raise RuntimeError("aiortc and PyAV are required for Discord Bidi calls")
        self.worker = worker
        self.exocortex = exocortex
        self.conv_id = str(conv_id)
        self.call_id = str(call_id)
        self.log = log
        self.input_track = DiscordInputAudioTrack()
        self.loop = None
        self.thread = None
        self.peer = None
        self.output_task = None
        self.started = threading.Event()
        self.start_error = None
        self.stopping = False

    def start(self, timeout=30):
        if self.thread:
            return
        self.thread = threading.Thread(target=self._thread_main, name="discord-bidi-webrtc", daemon=True)
        self.thread.start()
        if not self.started.wait(timeout):
            raise TimeoutError("Timed out starting the Discord Bidi media adapter")
        if self.start_error:
            raise self.start_error

    def push_pcm(self, user_id: str, pcm: bytes, sample_rate: int, channels: int):
        if not self.stopping:
            self.input_track.push_pcm(user_id, pcm, sample_rate, channels)

    def stop(self):
        self.stopping = True
        loop = self.loop
        if loop and loop.is_running():
            try:
                future = asyncio.run_coroutine_threadsafe(self._close(), loop)
                future.result(timeout=3)
            except Exception:
                pass
            loop.call_soon_threadsafe(loop.stop)
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=3)
        self.thread = None

    def _thread_main(self):
        loop = asyncio.new_event_loop()
        self.loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._start())
            self.started.set()
            loop.run_forever()
        except Exception as error:
            self.start_error = error
            self.started.set()
            self.worker.running = False
        finally:
            try:
                loop.run_until_complete(self._close())
            except Exception:
                pass
            loop.close()
            self.loop = None

    async def _start(self):
        peer = RTCPeerConnection()
        self.peer = peer
        peer.addTrack(self.input_track)
        peer.createDataChannel("oai-events")

        @peer.on("track")
        def on_track(track):
            if track.kind == "audio":
                self.output_task = asyncio.create_task(self._consume_output(track))

        @peer.on("connectionstatechange")
        async def on_connection_state_change():
            state = peer.connectionState
            self.log(f"Discord Bidi WebRTC state: {state}")
            if state in {"failed", "closed"} and not self.stopping:
                self.worker.running = False

        offer = await peer.createOffer()
        await peer.setLocalDescription(offer)
        await self._wait_for_ice(peer)
        local = peer.localDescription
        if not local or not local.sdp:
            raise RuntimeError("Discord Bidi WebRTC did not produce an SDP offer")
        answer = await asyncio.to_thread(
            self.exocortex.attach_media,
            self.conv_id,
            self.call_id,
            local.sdp,
        )
        await peer.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))

    async def _wait_for_ice(self, peer, timeout=5):
        if peer.iceGatheringState == "complete":
            return
        complete = asyncio.Event()

        @peer.on("icegatheringstatechange")
        def changed():
            if peer.iceGatheringState == "complete":
                complete.set()

        try:
            await asyncio.wait_for(complete.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    async def _consume_output(self, track):
        resampler = av.AudioResampler(format="s16", layout="stereo", rate=SAMPLE_RATE)
        codec = av.CodecContext.create("libopus", "w")
        codec.sample_rate = SAMPLE_RATE
        codec.layout = "stereo"
        codec.format = "s16"
        codec.bit_rate = 96_000
        codec.time_base = fractions.Fraction(1, SAMPLE_RATE)
        codec.open()
        speaking = False
        last_audible_at = 0.0
        try:
            while not self.stopping and self.worker.running:
                frame = await track.recv()
                for output in resampler.resample(frame):
                    now = time.monotonic()
                    if _frame_rms_db(output) >= OUTPUT_SILENCE_THRESHOLD_DB:
                        last_audible_at = now
                    elif now - last_audible_at > OUTPUT_SPEECH_HANGOVER_SECONDS:
                        if speaking:
                            self.worker._send_speaking(False)
                            speaking = False
                        continue
                    if not speaking:
                        self.worker._send_speaking(True)
                        speaking = True
                    for packet in codec.encode(output):
                        self.worker.send_bidi_opus(bytes(packet))
        except Exception as error:
            if not self.stopping:
                self.log(f"Discord Bidi output stopped: {error}")
                self.worker.running = False
        finally:
            if speaking:
                self.worker._send_speaking(False)

    async def _close(self):
        task, self.output_task = self.output_task, None
        if task:
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        peer, self.peer = self.peer, None
        if peer:
            await peer.close()
