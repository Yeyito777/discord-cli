import contextlib
import io
import unittest
from unittest.mock import patch

from src import api, reading, resolve, writing

DMS = [
    {"id": "111", "type": 3, "name": "", "recipients": [
        {"username": "bobby_s", "global_name": "Bobby"}, {"username": "carol"}]},
    {"id": "222", "type": 1, "recipients": [{"username": "rob", "global_name": "Robert"}]},
    {"id": "333", "type": 1, "recipients": [{"username": "bob", "global_name": "Bob"}]},
    {"id": "444", "type": 3, "name": "weekend plans", "recipients": [{"username": "dave"}]},
]
CHANNELS = [
    {"id": "1", "name": "general", "type": 0, "guild_id": "9"},
    {"id": "2", "name": "gen-ai", "type": 0, "guild_id": "9"},
    {"id": "3", "name": "General", "type": 2, "guild_id": "9"},  # voice: never a text target
    {"id": "4", "name": "memes", "type": 0, "guild_id": "9"},
]


class DmResolutionTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(api, "get_dm_channels", return_value=DMS)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_exact_username_beats_group_dm_and_earlier_partial_matches(self):
        self.assertEqual(resolve.resolve_dm("bob")["id"], "333")
        self.assertEqual(resolve.resolve_dm("@bob")["id"], "333")

    def test_exact_display_name_resolves_the_1to1(self):
        self.assertEqual(resolve.resolve_dm("robert")["id"], "222")

    def test_exact_group_name_resolves_the_group(self):
        self.assertEqual(resolve.resolve_dm("weekend plans")["id"], "444")

    def test_unique_partial_match_is_accepted(self):
        self.assertEqual(resolve.resolve_dm("week")["id"], "444")

    def test_partial_match_in_several_conversations_is_refused_with_candidates(self):
        with self.assertRaises(resolve.AmbiguousTarget) as caught:
            resolve.resolve_dm("ob")
        message = str(caught.exception)
        for channel_id in ("111", "222", "333"):
            self.assertIn(channel_id, message)

    def test_dm_send_goes_to_the_exact_1to1_and_names_it(self):
        sent = []
        stdout = io.StringIO()

        class Stdin(io.StringIO):
            def isatty(self):
                return False

        with (
            patch.object(api, "send_message", side_effect=lambda ch, text, **k: sent.append(ch) or {"id": "9"}),
            patch.object(writing, "_resolve_mentions", side_effect=lambda text, guild_id=None: text),
            patch("sys.stdin", Stdin("hello")),
            contextlib.redirect_stdout(stdout),
        ):
            reading.dm(["bob", "--send"])
        self.assertEqual(sent, ["333"])
        self.assertIn("Sent to DM with Bob (@bob) (333). Message ID: 9", stdout.getvalue())


class ChannelResolutionTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(api, "get_guild_channels", return_value=CHANNELS)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_ambiguous_prefix_is_refused_instead_of_picking_the_shortest(self):
        with self.assertRaisesRegex(resolve.AmbiguousTarget, "2 channels") as caught:
            resolve.resolve_channel("gen", "9")
        self.assertIn("#general (1)", str(caught.exception))
        self.assertIn("#gen-ai (2)", str(caught.exception))

    def test_exact_and_unique_names_resolve(self):
        self.assertEqual(resolve.resolve_channel("general", "9")["id"], "1")
        self.assertEqual(resolve.resolve_channel("#mem", "9")["id"], "4")

    def test_non_text_channels_are_never_targets(self):
        with patch.object(api, "get_guild_channels", return_value=[CHANNELS[2]]):
            with self.assertRaisesRegex(RuntimeError, "Cannot resolve channel"):
                resolve.resolve_channel("general", "9")

    def test_ambiguous_server_names_are_refused(self):
        guilds = [{"id": "10", "name": "My Server"}, {"id": "11", "name": "My Other Server"}]
        with patch.object(api, "get_guilds", return_value=guilds):
            with self.assertRaises(resolve.AmbiguousTarget):
                resolve.resolve_guild("My")
            self.assertEqual(resolve.resolve_guild("my server")["id"], "10")

    def test_destination_names_channel_and_server(self):
        self.assertEqual(
            resolve.describe_destination(CHANNELS[0], {"id": "9", "name": "Home"}),
            "#general (1) in Home (9)",
        )
        self.assertEqual(resolve.describe_destination(CHANNELS[0]), "#general (1) in server 9")
        self.assertEqual(resolve.describe_destination(DMS[0]),
                         "group DM with Bobby, carol (111)")
        self.assertEqual(resolve.describe_destination(DMS[3]),
                         'group DM "weekend plans" with dave (444)')
