"""Per-participant utterance segmentation for Discord realtime calls."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import io
import math
import threading
import time
import wave
from array import array


SAMPLE_RATE = 48_000
FRAME_SAMPLES = 960
FRAME_BYTES = FRAME_SAMPLES * 2
FRAME_MILLISECONDS = 20
SPEECH_THRESHOLD_DB = -52.0
PRE_ROLL_FRAMES = 10
END_SILENCE_FRAMES = 25
TRAILING_SILENCE_FRAMES = 10
MIN_SPEECH_FRAMES = 4
MAX_UTTERANCE_FRAMES = 2_250  # 45 seconds


def pcm_rms_db(pcm: bytes) -> float:
    samples = array("h")
    samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
    if not samples:
        return -120.0
    mean_square = sum((sample / 32768.0) ** 2 for sample in samples) / len(samples)
    return 20 * math.log10(max(mean_square ** 0.5, 1e-6))


def pcm_to_wav(pcm: bytes) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(SAMPLE_RATE)
        wav.writeframes(pcm)
    return output.getvalue()


@dataclass
class _SpeakerState:
    remainder: bytearray = field(default_factory=bytearray)
    preroll: deque = field(default_factory=lambda: deque(maxlen=PRE_ROLL_FRAMES))
    frames: list[bytes] = field(default_factory=list)
    started_at: int | None = None
    silent_frames: int = 0
    speech_frames: int = 0
    active: bool = False


class DiscordUtteranceSegmenter:
    """Split already-attributed 48 kHz mono PCM into independent WAV utterances."""

    def __init__(self, *, on_utterance, on_speakers=None, wall_clock=time.time):
        self._on_utterance = on_utterance
        self._on_speakers = on_speakers
        self._wall_clock = wall_clock
        self._states = defaultdict(_SpeakerState)
        self._active_speakers = set()
        self._lock = threading.Lock()

    def push_pcm(self, participant_id: str, pcm: bytes):
        participant_id = str(participant_id)
        if not pcm:
            return
        utterances = []
        speaker_events = []
        with self._lock:
            state = self._states[participant_id]
            state.remainder.extend(pcm[: len(pcm) - (len(pcm) % 2)])
            frame_count = len(state.remainder) // FRAME_BYTES
            batch_end = int(self._wall_clock() * 1000)
            batch_start = batch_end - frame_count * FRAME_MILLISECONDS
            for index in range(frame_count):
                frame = bytes(state.remainder[:FRAME_BYTES])
                del state.remainder[:FRAME_BYTES]
                frame_started_at = batch_start + index * FRAME_MILLISECONDS
                produced, changed = self._push_frame(participant_id, state, frame, frame_started_at)
                if produced:
                    utterances.append(produced)
                if changed:
                    speaker_events.append((tuple(sorted(self._active_speakers)), frame_started_at))
        for speaker_event in speaker_events:
            self._emit_speakers(*speaker_event)
        for utterance in utterances:
            self._emit_utterance(*utterance)

    def flush(self):
        utterances = []
        speaker_event = None
        now = int(self._wall_clock() * 1000)
        with self._lock:
            for participant_id, state in self._states.items():
                produced = self._finish(participant_id, state, now)
                if produced:
                    utterances.append(produced)
            if self._active_speakers:
                self._active_speakers.clear()
                speaker_event = ((), now)
        if speaker_event:
            self._emit_speakers(*speaker_event)
        for utterance in utterances:
            self._emit_utterance(*utterance)

    def _push_frame(self, participant_id: str, state: _SpeakerState, frame: bytes, started_at: int):
        audible = pcm_rms_db(frame) >= SPEECH_THRESHOLD_DB
        changed = False
        if not state.active:
            state.preroll.append((frame, started_at))
            if not audible:
                return None, False
            state.active = True
            state.frames = [value for value, _timestamp in state.preroll]
            state.started_at = state.preroll[0][1] if state.preroll else started_at
            state.preroll.clear()
            state.speech_frames = 1
            state.silent_frames = 0
            self._active_speakers.add(participant_id)
            return None, True

        state.frames.append(frame)
        if audible:
            state.speech_frames += 1
            state.silent_frames = 0
        else:
            state.silent_frames += 1

        if state.silent_frames >= END_SILENCE_FRAMES or len(state.frames) >= MAX_UTTERANCE_FRAMES:
            ended_at = started_at + FRAME_MILLISECONDS
            produced = self._finish(participant_id, state, ended_at)
            changed = True
            return produced, changed
        return None, changed

    def _finish(self, participant_id: str, state: _SpeakerState, ended_at: int):
        if not state.active:
            return None
        frames = state.frames
        if state.silent_frames > TRAILING_SILENCE_FRAMES:
            trimmed_frames = state.silent_frames - TRAILING_SILENCE_FRAMES
            frames = frames[:-trimmed_frames]
            ended_at -= trimmed_frames * FRAME_MILLISECONDS
        started_at = state.started_at if state.started_at is not None else ended_at
        valid = state.speech_frames >= MIN_SPEECH_FRAMES and bool(frames)
        state.frames = []
        state.started_at = None
        state.silent_frames = 0
        state.speech_frames = 0
        state.active = False
        self._active_speakers.discard(participant_id)
        if not valid:
            return None
        return participant_id, pcm_to_wav(b"".join(frames)), started_at, ended_at

    def _emit_speakers(self, participants, observed_at):
        if not self._on_speakers:
            return
        try:
            self._on_speakers(participants, observed_at)
        except Exception:
            pass

    def _emit_utterance(self, participant_id, wav, started_at, ended_at):
        try:
            self._on_utterance(participant_id, wav, started_at, ended_at)
        except Exception:
            pass
