import unittest
from unittest.mock import MagicMock

from src.calls.receive import VoiceReceiveMedia


class DiscordCallReceiveTests(unittest.TestCase):
    def test_unknown_ssrc_does_not_steal_an_already_mapped_participant(self):
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
