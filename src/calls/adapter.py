"""Discord media adapter for an Exocortex realtime call."""

from __future__ import annotations

from array import array
import asyncio
import fractions
import math
import queue
import threading
import time
import uuid

try:  # Optional until a call adapter is started.
    import av  # type: ignore
    from aiortc import AudioStreamTrack, RTCPeerConnection, RTCSessionDescription  # type: ignore
except Exception:  # pragma: no cover - deployment dependency validation
    av = None
    AudioStreamTrack = object
    RTCPeerConnection = None
    RTCSessionDescription = None

from src.calls.send import send_outgoing_opus_payload
from src.calls.utterances import DiscordUtteranceSegmenter


SAMPLE_RATE = 48_000
FRAME_SAMPLES = 960
FRAME_SECONDS = FRAME_SAMPLES / SAMPLE_RATE
OUTPUT_SILENCE_THRESHOLD_DB = -58.0
OUTPUT_SPEECH_HANGOVER_SECONDS = 0.3
INPUT_GAIN = 1.5


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
        raise RuntimeError("PyAV is required to resample Discord call audio")
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


def _pcm_rms_db(pcm: bytes) -> float:
    samples = array("h")
    samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
    if not samples:
        return -120.0
    mean_square = sum((sample / 32768.0) ** 2 for sample in samples) / len(samples)
    return 20 * math.log10(max(mean_square ** 0.5, 1e-6))


def _apply_s16_gain(pcm: bytes, gain: float) -> bytes:
    """Apply bounded gain to signed 16-bit PCM without wrapping on peaks."""
    if not pcm or gain == 1.0:
        return pcm
    samples = array("h")
    samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
    for index, sample in enumerate(samples):
        samples[index] = max(-32768, min(32767, round(sample * gain)))
    return samples.tobytes()


class DiscordInputAudioTrack(AudioStreamTrack):
    """Paced silent carrier; attributed Discord speech enters through IPC text."""

    kind = "audio"

    def __init__(self):
        super().__init__()
        self._start = None
        self._timestamp = 0

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
        frame.planes[0].update(bytes(FRAME_SAMPLES * 2))
        frame.pts = self._timestamp
        frame.sample_rate = SAMPLE_RATE
        frame.time_base = fractions.Fraction(1, SAMPLE_RATE)
        return frame


class DiscordCallAdapter:
    """Own one aiortc peer and route its audio through an active Discord worker."""

    def __init__(self, worker, exocortex, conv_id: str, call_id: str, *, log=print):
        if RTCPeerConnection is None or av is None:
            raise RuntimeError("aiortc and PyAV are required for Discord calls")
        self.worker = worker
        self.exocortex = exocortex
        self.conv_id = str(conv_id)
        self.call_id = str(call_id)
        self.log = log
        self.input_track = DiscordInputAudioTrack()
        self.utterance_queue = queue.Queue()
        self.utterance_thread = None
        self.segmenter = DiscordUtteranceSegmenter(
            on_utterance=self._queue_utterance,
            on_speakers=self._publish_speakers,
        )
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
        self.utterance_thread = threading.Thread(
            target=self._publish_utterances,
            name="discord-call-utterances",
            daemon=True,
        )
        self.utterance_thread.start()
        self.thread = threading.Thread(target=self._thread_main, name="discord-call-webrtc", daemon=True)
        self.thread.start()
        if not self.started.wait(timeout):
            raise TimeoutError("Timed out starting the Discord media adapter")
        if self.start_error:
            raise self.start_error

    def push_pcm(self, user_id: str, pcm: bytes, sample_rate: int, channels: int):
        if not self.stopping:
            normalized = _apply_s16_gain(_mono_48k(pcm, sample_rate, channels), INPUT_GAIN)
            self.segmenter.push_pcm(user_id, normalized)

    def _queue_utterance(self, participant_id, wav, started_at, ended_at):
        self.utterance_queue.put((
            str(uuid.uuid4()),
            str(participant_id),
            bytes(wav),
            int(started_at),
            int(ended_at),
        ))

    def _publish_utterances(self):
        while True:
            item = self.utterance_queue.get()
            try:
                if item is None:
                    return
                utterance_id, participant_id, wav, started_at, ended_at = item
                self.exocortex.submit_utterance(
                    self.conv_id,
                    self.call_id,
                    utterance_id=utterance_id,
                    participant_id=participant_id,
                    audio=wav,
                    started_at=started_at,
                    ended_at=ended_at,
                )
            except Exception as exc:
                self.log(f"Could not publish Discord call utterance: {exc}")
            finally:
                self.utterance_queue.task_done()

    def _publish_speakers(self, participant_ids, observed_at):
        if self.stopping or self.exocortex is None:
            return
        try:
            self.exocortex.update_speakers(
                self.conv_id,
                self.call_id,
                participant_ids,
                observed_at,
            )
        except Exception as exc:
            self.log(f"Could not publish Discord call speakers: {exc}")

    def stop(self):
        self.segmenter.flush()
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
        self.utterance_queue.put(None)
        if self.utterance_thread and self.utterance_thread is not threading.current_thread():
            self.utterance_thread.join(timeout=5)
        self.utterance_thread = None

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
            self.log(f"Discord call WebRTC state: {state}")
            if state in {"failed", "closed"} and not self.stopping:
                self.worker.running = False

        offer = await peer.createOffer()
        await peer.setLocalDescription(offer)
        await self._wait_for_ice(peer)
        local = peer.localDescription
        if not local or not local.sdp:
            raise RuntimeError("Discord call WebRTC did not produce an SDP offer")
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
                    if self.worker.self_mute:
                        if speaking:
                            self.worker._send_speaking(False)
                            speaking = False
                        continue
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
                        self.worker.send_call_opus(bytes(packet))
        except Exception as error:
            if not self.stopping:
                self.log(f"Discord call output stopped: {error}")
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
