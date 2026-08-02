from array import array
import unittest

import av

from src.calls.bidi import DiscordInputAudioTrack, FRAME_SAMPLES, SAMPLE_RATE, _frame_rms_db, _mono_48k


class DiscordBidiAudioTests(unittest.IsolatedAsyncioTestCase):
    async def test_track_preserves_one_speaker_pcm(self):
        track = DiscordInputAudioTrack()
        samples = array("h", [1200]) * FRAME_SAMPLES
        track.push_pcm("speaker-a", samples.tobytes(), SAMPLE_RATE, 1)

        frame = await track.recv()
        output = array("h")
        output.frombytes(bytes(frame.planes[0])[: FRAME_SAMPLES * 2])
        self.assertEqual(len(output), FRAME_SAMPLES)
        self.assertEqual(set(output), {1200})
        track.stop()

    async def test_track_mixes_simultaneous_speakers_into_one_realtime_frame(self):
        track = DiscordInputAudioTrack()
        first = array("h", [1000]) * FRAME_SAMPLES
        second = array("h", [2000]) * FRAME_SAMPLES
        track.push_pcm("speaker-a", first.tobytes(), SAMPLE_RATE, 1)
        track.push_pcm("speaker-b", second.tobytes(), SAMPLE_RATE, 1)

        frame = await track.recv()
        output = array("h")
        output.frombytes(bytes(frame.planes[0])[: FRAME_SAMPLES * 2])
        # sqrt(2) headroom prevents ordinary overlap from clipping.
        self.assertTrue(all(2100 <= value <= 2130 for value in output))
        track.stop()

    def test_stereo_input_is_downmixed_without_changing_duration(self):
        stereo = array("h")
        for _ in range(FRAME_SAMPLES):
            stereo.extend((1000, 3000))
        mono = array("h")
        mono.frombytes(_mono_48k(stereo.tobytes(), SAMPLE_RATE, 2))
        self.assertEqual(len(mono), FRAME_SAMPLES)
        self.assertEqual(set(mono), {2000})

    def test_output_level_gate_distinguishes_speech_from_silence(self):
        silence = av.AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        silence.planes[0].update(bytes(FRAME_SAMPLES * 4))
        speech = av.AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        speech.planes[0].update((array("h", [4000, 4000]) * FRAME_SAMPLES).tobytes())
        self.assertLess(_frame_rms_db(silence), -58)
        self.assertGreater(_frame_rms_db(speech), -58)


if __name__ == "__main__":
    unittest.main()
