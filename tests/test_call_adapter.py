from array import array
import unittest
from unittest.mock import Mock

import av

from src.calls.adapter import DiscordCallAdapter, DiscordInputAudioTrack, FRAME_SAMPLES, SAMPLE_RATE, _apply_s16_gain, _frame_rms_db, _mono_48k


class DiscordCallAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_webrtc_input_track_is_silent_to_avoid_duplicate_model_turns(self):
        track = DiscordInputAudioTrack()
        frame = await track.recv()
        output = array("h")
        output.frombytes(bytes(frame.planes[0])[: FRAME_SAMPLES * 2])
        self.assertEqual(len(output), FRAME_SAMPLES)
        self.assertEqual(set(output), {0})
        track.stop()

    def test_adapter_applies_fifty_percent_gain_before_per_speaker_segmentation(self):
        adapter = DiscordCallAdapter(Mock(), Mock(), "conv", "call", log=lambda _message: None)
        adapter.segmenter = Mock()
        samples = array("h", [1200]) * FRAME_SAMPLES
        adapter.push_pcm("speaker-a", samples.tobytes(), SAMPLE_RATE, 1)
        output = array("h")
        output.frombytes(adapter.segmenter.push_pcm.call_args.args[1])
        self.assertEqual(set(output), {1800})

    def test_stereo_input_is_downmixed_without_changing_duration(self):
        stereo = array("h")
        for _ in range(FRAME_SAMPLES):
            stereo.extend((1000, 3000))
        mono = array("h")
        mono.frombytes(_mono_48k(stereo.tobytes(), SAMPLE_RATE, 2))
        self.assertEqual(len(mono), FRAME_SAMPLES)
        self.assertEqual(set(mono), {2000})

    def test_input_gain_saturates_instead_of_wrapping(self):
        boosted = array("h")
        boosted.frombytes(_apply_s16_gain(array("h", [30000, -30000]).tobytes(), 1.5))
        self.assertEqual(list(boosted), [32767, -32768])

    def test_output_level_gate_distinguishes_speech_from_silence(self):
        silence = av.AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        silence.planes[0].update(bytes(FRAME_SAMPLES * 4))
        speech = av.AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        speech.planes[0].update((array("h", [4000, 4000]) * FRAME_SAMPLES).tobytes())
        self.assertLess(_frame_rms_db(silence), -58)
        self.assertGreater(_frame_rms_db(speech), -58)

    async def test_output_track_uses_the_worker_media_send_contract(self):
        worker = Mock()
        worker.running = True
        worker.self_mute = False
        adapter = DiscordCallAdapter(worker, None, "conv", "call", log=lambda _message: None)
        speech = av.AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        speech.planes[0].update((array("h", [4000, 4000]) * FRAME_SAMPLES).tobytes())
        speech.sample_rate = SAMPLE_RATE

        class OneFrameTrack:
            async def recv(self):
                worker.running = False
                return speech

        await adapter._consume_output(OneFrameTrack())
        worker.send_call_opus.assert_called()


if __name__ == "__main__":
    unittest.main()
