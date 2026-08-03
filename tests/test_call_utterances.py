from array import array
import io
import unittest
import wave

from src.calls.utterances import (
    DiscordUtteranceSegmenter,
    END_SILENCE_FRAMES,
    FRAME_SAMPLES,
)


class DiscordCallUtteranceTests(unittest.TestCase):
    def test_overlapping_speakers_produce_separate_wav_utterances(self):
        clock = [100.0]
        speaker_events = []
        utterances = []
        segmenter = DiscordUtteranceSegmenter(
            on_utterance=lambda *args: utterances.append(args),
            on_speakers=lambda speakers, observed_at: speaker_events.append((speakers, observed_at)),
            wall_clock=lambda: clock[0],
        )
        speech_a = (array("h", [4000]) * FRAME_SAMPLES).tobytes()
        speech_b = (array("h", [8000]) * FRAME_SAMPLES).tobytes()
        silence = bytes(FRAME_SAMPLES * 2)

        def push(participant, frame):
            clock[0] += 0.02
            segmenter.push_pcm(participant, frame)

        for _ in range(5):
            push("speaker-a", speech_a)
        for _ in range(5):
            push("speaker-b", speech_b)
        for _ in range(END_SILENCE_FRAMES):
            push("speaker-a", silence)
            push("speaker-b", silence)

        self.assertEqual([event[0] for event in speaker_events], [
            ("speaker-a",),
            ("speaker-a", "speaker-b"),
            ("speaker-b",),
            (),
        ])
        self.assertEqual([item[0] for item in utterances], ["speaker-a", "speaker-b"])

        decoded_peaks = []
        for _participant, wav_bytes, started_at, ended_at in utterances:
            self.assertLess(started_at, ended_at)
            with wave.open(io.BytesIO(wav_bytes), "rb") as wav:
                self.assertEqual(wav.getnchannels(), 1)
                self.assertEqual(wav.getframerate(), 48_000)
                samples = array("h")
                samples.frombytes(wav.readframes(wav.getnframes()))
                decoded_peaks.append(max(samples))
        self.assertEqual(decoded_peaks, [4000, 8000])

    def test_flush_emits_an_active_utterance(self):
        utterances = []
        segmenter = DiscordUtteranceSegmenter(
            on_utterance=lambda *args: utterances.append(args),
            wall_clock=lambda: 200.0,
        )
        speech = (array("h", [4000]) * FRAME_SAMPLES).tobytes()
        for _ in range(5):
            segmenter.push_pcm("speaker", speech)
        segmenter.flush()
        self.assertEqual(len(utterances), 1)
        self.assertEqual(utterances[0][0], "speaker")


if __name__ == "__main__":
    unittest.main()
