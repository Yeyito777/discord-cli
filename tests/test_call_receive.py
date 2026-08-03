import unittest
from unittest.mock import MagicMock

from src.calls.receive import VoiceReceiveMedia


class DiscordCallReceiveTests(unittest.TestCase):
    def test_authoritative_mapping_preserves_multiple_ssrcs_for_one_user(self):
        media = object.__new__(VoiceReceiveMedia)
        media.ssrc_to_user_id = {100: "owner"}
        media.fallback_user_ids = {"owner"}
        media.decoders = {}
        media.resamplers = {}
        media.pre_dave_jitter_buffers = {}
        media.jitter_buffers = {}
        media.unknown_ssrc_buffers = {}
        media.dave = MagicMock()
        media.name_for_user = lambda user_id: user_id
        media.log = lambda _message: None

        media.add_ssrc_mapping(200, "owner")

        self.assertEqual(media.ssrc_to_user_id, {100: "owner", 200: "owner"})
        media.dave.add_ssrc_mapping.assert_called_once_with(200, "owner")

    def test_second_ssrc_can_bind_the_only_participant_without_stealing(self):
        media = object.__new__(VoiceReceiveMedia)
        media.packet_count = 0
        media.ssrc_to_user_id = {100: "owner"}
        media.fallback_user_ids = {"owner"}
        media.unknown_ssrc_count = 0
        media.add_ssrc_mapping = MagicMock()
        media._buffer_unknown_ssrc_packet = MagicMock()
        media._handle_mapped_packet = MagicMock()
        media.name_for_user = lambda user_id: user_id
        media.log = lambda _message: None

        # Minimal RTP v2 packet with payload type 120 and enough encrypted body.
        packet = bytes([0x80, 120, 0, 1, 0, 0, 0, 1]) + (200).to_bytes(4, "big") + bytes(32)
        media._handle_packet(packet)

        media.add_ssrc_mapping.assert_called_once_with(200, "owner")
        media._handle_mapped_packet.assert_called_once()
        media._buffer_unknown_ssrc_packet.assert_not_called()

    def test_unknown_ssrc_is_never_guessed_in_a_multi_party_call(self):
        media = object.__new__(VoiceReceiveMedia)
        media.packet_count = 0
        media.ssrc_to_user_id = {100: "owner"}
        media.fallback_user_ids = {"owner", "friend"}
        media.unknown_ssrc_count = 0
        media.add_ssrc_mapping = MagicMock()
        media._buffer_unknown_ssrc_packet = MagicMock()
        media._handle_mapped_packet = MagicMock()
        media.name_for_user = lambda user_id: user_id
        media.log = lambda _message: None

        packet = bytes([0x80, 120, 0, 1, 0, 0, 0, 1]) + (200).to_bytes(4, "big") + bytes(32)
        media._handle_packet(packet)

        media.add_ssrc_mapping.assert_not_called()
        media._handle_mapped_packet.assert_not_called()
        media._buffer_unknown_ssrc_packet.assert_called_once()

    def test_first_unknown_ssrc_can_still_bind_the_only_unmapped_participant(self):
        media = object.__new__(VoiceReceiveMedia)
        media.packet_count = 0
        media.ssrc_to_user_id = {}
        media.fallback_user_ids = {"owner"}
        media.unknown_ssrc_count = 0
        media.add_ssrc_mapping = MagicMock()
        media._buffer_unknown_ssrc_packet = MagicMock()
        media._handle_mapped_packet = MagicMock()
        media.name_for_user = lambda user_id: user_id
        media.log = lambda _message: None

        packet = bytes([0x80, 120, 0, 1, 0, 0, 0, 1]) + (200).to_bytes(4, "big") + bytes(32)
        media._handle_packet(packet)

        media.add_ssrc_mapping.assert_called_once_with(200, "owner")
        media._handle_mapped_packet.assert_called_once()
        media._buffer_unknown_ssrc_packet.assert_not_called()


if __name__ == "__main__":
    unittest.main()
