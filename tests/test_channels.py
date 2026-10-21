import contextlib
import io
import unittest
from unittest.mock import patch

from src import api, channels


def ch(cid, position, parent=None, type_=0, name=None):
    return {"id": cid, "position": position, "parent_id": parent, "type": type_, "name": name or f"c{cid}"}


GUILD = "709232473271304285"
EXISTING = [ch("11", 0), ch("12", 1), ch("13", 2), ch("20", 3, type_=4), ch("21", 0, "20")]


class ChannelApiTests(unittest.TestCase):
    def test_create_posts_once_with_optional_fields(self):
        with patch.object(api, "post_once", return_value={"id": "5"}) as post:
            api.create_guild_channel(GUILD, "polymarket", topic="t", parent_id="20")
        post.assert_called_once_with(
            f"/guilds/{GUILD}/channels",
            body={"name": "polymarket", "type": 0, "topic": "t", "parent_id": "20"},
        )

    def test_create_omits_unset_fields(self):
        with patch.object(api, "post_once", return_value={"id": "5"}) as post:
            api.create_guild_channel(GUILD, "x", channel_type=2)
        self.assertEqual(post.call_args.kwargs["body"], {"name": "x", "type": 2})

    def test_positions_are_patched(self):
        with patch.object(api, "patch", return_value=None) as patch_:
            api.set_channel_positions(GUILD, [{"id": "1", "position": 0}])
        patch_.assert_called_once_with(f"/guilds/{GUILD}/channels", body=[{"id": "1", "position": 0}])


class ReorderTests(unittest.TestCase):
    def reorder(self, listing, channel_id, position):
        with (
            patch.object(api, "get_guild_channels", return_value=listing),
            patch.object(api, "set_channel_positions") as setter,
        ):
            index = channels.reorder(GUILD, channel_id, position)
        return index, (setter.call_args.args[1] if setter.called else None)

    def test_top_renumbers_only_changed_siblings_of_the_same_parent(self):
        index, sent = self.reorder(EXISTING + [ch("14", 4)], "14", 0)
        self.assertEqual(index, 0)
        # 20 (category) is a top-level sibling too: 11,12,13,20 shift by one.
        self.assertEqual(sent, [
            {"id": "14", "position": 0}, {"id": "11", "position": 1}, {"id": "12", "position": 2},
            {"id": "13", "position": 3}, {"id": "20", "position": 4},
        ])

    def test_children_of_a_category_are_not_touched(self):
        _, sent = self.reorder(EXISTING + [ch("14", 4)], "14", 0)
        self.assertNotIn("21", [c["id"] for c in sent])

    def test_position_past_the_end_means_last(self):
        index, sent = self.reorder(EXISTING, "11", 99)
        self.assertEqual(index, 3)
        self.assertEqual(sent, [
            {"id": "12", "position": 0}, {"id": "13", "position": 1},
            {"id": "20", "position": 2}, {"id": "11", "position": 3},
        ])

    def test_nothing_sent_when_already_in_place(self):
        index, sent = self.reorder(EXISTING, "11", 0)
        self.assertEqual((index, sent), (0, None))

    def test_duplicate_positions_break_ties_by_id(self):
        _, sent = self.reorder([ch("30", 0), ch("31", 0), ch("32", 0)], "32", 0)
        # 32 is already numbered 0, so only the two it displaces are sent.
        self.assertEqual(sent, [{"id": "30", "position": 1}, {"id": "31", "position": 2}])

    def test_channel_missing_from_the_server(self):
        with self.assertRaisesRegex(RuntimeError, "not in server"):
            self.reorder(EXISTING, "999", 0)


class ChannelCliTests(unittest.TestCase):
    def run_cli(self, fn, argv, listing=EXISTING, created=None):
        out = io.StringIO()
        with (
            patch.object(channels, "resolve_guild", return_value={"id": GUILD}),
            patch.object(channels.api, "get_guild_channels", return_value=listing),
            patch.object(channels.api, "create_guild_channel", return_value=created) as create,
            patch.object(channels.api, "set_channel_positions") as setter,
            contextlib.redirect_stdout(out),
        ):
            fn(argv)
        return out.getvalue(), create, setter

    def test_create_at_top(self):
        listing_after = EXISTING + [ch("50", 4, name="polymarket")]
        out = io.StringIO()
        with (
            patch.object(channels, "resolve_guild", return_value={"id": GUILD}),
            patch.object(channels.api, "get_guild_channels", side_effect=[EXISTING, listing_after]),
            patch.object(channels.api, "create_guild_channel", return_value={"id": "50", "name": "polymarket"}) as create,
            patch.object(channels.api, "set_channel_positions") as setter,
            contextlib.redirect_stdout(out),
        ):
            channels.create([GUILD, "polymarket", "--topic", "updates", "--position", "top"])
        create.assert_called_once_with(GUILD, "polymarket", channel_type=0, topic="updates", parent_id=None)
        self.assertEqual(setter.call_args.args[1][0], {"id": "50", "position": 0})
        self.assertIn("Created text channel #polymarket. Channel ID: 50", out.getvalue())
        self.assertIn("Moved to position 0 (top).", out.getvalue())

    def test_create_without_position_does_not_reorder(self):
        out, create, setter = self.run_cli(channels.create, [GUILD, "new"], created={"id": "51", "name": "new"})
        create.assert_called_once()
        setter.assert_not_called()
        self.assertIn("https://discord.com/channels/709232473271304285/51", out)

    def test_parent_is_resolved_to_a_category(self):
        with patch.object(channels, "resolve_category", return_value={"id": "20"}) as resolve:
            self.run_cli(channels.create, [GUILD, "new", "--parent", "cat"], created={"id": "51"})
        resolve.assert_called_once_with("cat", GUILD)

    def test_duplicate_name_is_refused_before_posting(self):
        with patch.object(channels.api, "create_guild_channel") as create:
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                self.run_cli(channels.create, [GUILD, "C11"])
        create.assert_not_called()

    def test_failed_reorder_reports_the_new_channel(self):
        with patch.object(channels, "reorder", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, r"(?s)created \(ID 51\).*channel move 51 --position 0"):
                self.run_cli(channels.create, [GUILD, "new", "--position", "0"], created={"id": "51"})

    def test_invalid_arguments(self):
        for argv in (
            [GUILD, "n", "--position", "-1"],
            [GUILD, "n", "--position", "middle"],
            [GUILD, "n", "--type", "voice", "--topic", "t"],
            [GUILD, "n", "--type", "category", "--parent", "c"],
            [GUILD, "x" * 101],
        ):
            with self.subTest(argv=argv), patch("sys.stderr"), self.assertRaises(SystemExit):
                channels.create(argv)

    def test_move_by_id(self):
        out = io.StringIO()
        with (
            patch.object(channels, "resolve_channel", return_value={"id": "13", "guild_id": GUILD, "name": "c13"}),
            patch.object(channels.api, "get_guild_channels", return_value=EXISTING),
            patch.object(channels.api, "set_channel_positions") as setter,
            contextlib.redirect_stdout(out),
        ):
            channels.move(["13", "--position", "top"])
        self.assertEqual(setter.call_args.args[1][0], {"id": "13", "position": 0})
        self.assertIn("Moved #c13 to position 0 (top).", out.getvalue())

    def test_move_requires_a_position(self):
        with patch("sys.stderr"), self.assertRaises(SystemExit):
            channels.move(["13"])

    def test_dispatch_rejects_unknown_subcommand(self):
        with self.assertRaisesRegex(RuntimeError, "Unknown channel subcommand"):
            channels.dispatch("channel", ["delete"])


class ChannelPolicyTests(unittest.TestCase):
    def test_channel_command_is_a_mutation(self):
        from src import accounts
        self.assertTrue(accounts._is_mutating("channel", ["create", "x", "y"]))
        self.assertFalse(accounts._is_mutating("channels", ["x"]))


if __name__ == "__main__":
    unittest.main()
