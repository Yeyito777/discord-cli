import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import gifs


def _klipy(slug, title):
    return {
        "id": slug,
        "title": title,
        "url": f"https://klipy.com/gifs/{slug}",
        "src": f"https://static.klipy.com/ii/x/{slug}.mp4",
        "gif_src": f"https://static.klipy.com/ii/x/{slug}.webp",
        "width": 320,
        "height": 240,
        "preview": f"https://static.klipy.com/ii/x/{slug}.mp4",
    }


def _msg(msg_id, content="", embeds=None, attachments=None, name="alice"):
    return {
        "id": msg_id,
        "channel_id": "100000000000000001",
        "author": {"id": "1", "username": name, "global_name": name.title()},
        "content": content,
        "timestamp": "2026-10-12T20:00:00+00:00",
        "embeds": embeds or [],
        "attachments": attachments or [],
    }


def _gifv(url, title="Dancing Cat"):
    return {
        "type": "gifv",
        "url": url,
        "title": title,
        "provider": {"name": "Klipy", "url": "https://klipy.com"},
        "video": {"url": "https://static.klipy.com/ii/x/cat.mp4"},
    }


def _run(fn, argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        fn(argv)
    return out.getvalue(), err.getvalue()


class PickerSearchTests(unittest.TestCase):
    def test_search_mirrors_client_request_and_prints_sendable_urls(self):
        data = [_klipy(f"cat-{i}", f"Cat {i}") for i in range(25)]
        with patch.object(gifs.api, "get", return_value=data) as get:
            out, _ = _run(gifs.search, ["dancing", "cat", "-n", "3"])
        get.assert_called_once_with(
            "/gifs/search",
            params={"q": "dancing cat", "media_format": "mp4", "locale": "en-US", "limit": "20"},
        )
        self.assertIn("1. Cat 0", out)
        self.assertIn("https://klipy.com/gifs/cat-2", out)
        self.assertNotIn("cat-3", out)

    def test_search_json_has_page_url_and_media_url(self):
        with patch.object(gifs.api, "get", return_value=[_klipy("ok", "Ok")]):
            out, _ = _run(gifs.search, ["ok", "--json"])
        self.assertEqual(json.loads(out), [{
            "id": "ok",
            "title": "Ok",
            "url": "https://klipy.com/gifs/ok",
            "media_url": "https://static.klipy.com/ii/x/ok.mp4",
            "width": 320,
            "height": 240,
        }])

    def test_large_limits_are_passed_through_up_to_the_api_maximum(self):
        with patch.object(gifs.api, "get", return_value=[]) as get:
            _run(gifs.trending, ["-n", "100"])
        get.assert_called_once_with(
            "/gifs/trending-gifs",
            params={"media_format": "mp4", "locale": "en-US", "limit": "100"},
        )

    def test_limit_out_of_range_is_rejected_before_api_call(self):
        for value in ("0", "101"):
            with self.subTest(value=value), patch.object(gifs.api, "get") as get:
                with self.assertRaises(SystemExit):
                    _run(gifs.search, ["cat", "-n", value])
                get.assert_not_called()


class LibraryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = patch.object(gifs, "LIBRARY_FILE", Path(self.tmp.name) / "gifs.json")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_save_normalizes_tags_and_records_source(self):
        out, err = _run(gifs.save, [
            "https://klipy.com/gifs/this-fine-4",
            "--tags", " This is fine, #Coping ,,coping",
            "--title", "Dog in burning room",
            "--note", "pretending things are ok",
            "--source", "https://discord.com/channels/111111111111111111/222222222222222222/333333333333333333",
        ])
        self.assertEqual(err, "")
        self.assertIn("Saved https://klipy.com/gifs/this-fine-4  [this is fine, coping]", out)
        entry = gifs.load_library()["gifs"][0]
        self.assertEqual(entry["tags"], ["this is fine", "coping"])
        self.assertEqual(entry["title"], "Dog in burning room")
        self.assertEqual(entry["source"], {
            "guild_id": "111111111111111111",
            "channel_id": "222222222222222222",
            "message_id": "333333333333333333",
        })

    def test_saving_existing_url_merges_tags_and_keeps_fields(self):
        url = "https://klipy.com/gifs/awesome-ok"
        _run(gifs.save, [url, "-t", "approval", "--note", "nice work"])
        out, _ = _run(gifs.save, [url, "-t", "thumbs-up", "-t", "approval"])
        self.assertIn("Updated", out)
        library = gifs.load_library()
        self.assertEqual(len(library["gifs"]), 1)
        self.assertEqual(library["gifs"][0]["tags"], ["approval", "thumbs-up"])
        self.assertEqual(library["gifs"][0]["note"], "nice work")

    def test_save_rejects_non_http_urls_and_empty_tags(self):
        for argv in (["klipy.com/gifs/x", "-t", "a"], ["https://klipy.com/gifs/x", "-t", " , "]):
            with self.subTest(argv=argv), self.assertRaises(SystemExit):
                _run(gifs.save, argv)
        self.assertFalse(gifs.LIBRARY_FILE.exists())

    def test_save_warns_for_urls_discord_may_not_animate(self):
        _, err = _run(gifs.save, ["https://example.com/page", "-t", "a"])
        self.assertIn("may not animate", err)
        _, err = _run(gifs.save, ["https://media.example.com/x.gif", "-t", "a"])
        self.assertEqual(err, "")

    def test_find_ranks_by_terms_matched_then_strength(self):
        _run(gifs.save, ["https://klipy.com/gifs/a", "-t", "celebrate,win"])
        _run(gifs.save, ["https://klipy.com/gifs/b", "-t", "winning-streak"])
        _run(gifs.save, ["https://klipy.com/gifs/c", "-t", "sad", "--note", "lost the game, no win"])
        _run(gifs.save, ["https://klipy.com/gifs/d", "-t", "unrelated"])
        found = gifs.find_gifs(gifs.load_library(), ["win", "celebrate"])
        self.assertEqual([g["url"][-1] for g in found], ["a", "b", "c"])

        out, _ = _run(gifs.find, ["sad,", "game", "--json"])
        self.assertEqual([g["url"] for g in json.loads(out)], ["https://klipy.com/gifs/c"])

    def test_list_tags_and_remove(self):
        _run(gifs.save, ["https://klipy.com/gifs/a", "-t", "hi,wave"])
        _run(gifs.save, ["https://klipy.com/gifs/b", "-t", "wave"])
        out, _ = _run(gifs.tags, [])
        self.assertEqual(out.split("\n")[0].split(), ["2", "wave"])
        out, _ = _run(gifs.list_cmd, ["--json"])
        self.assertEqual(len(json.loads(out)), 2)

        _run(gifs.remove, ["https://klipy.com/gifs/a"])
        self.assertEqual([g["url"] for g in gifs.load_library()["gifs"]], ["https://klipy.com/gifs/b"])
        with self.assertRaisesRegex(RuntimeError, "not in the GIF library"):
            _run(gifs.remove, ["https://klipy.com/gifs/a"])

    def test_corrupt_library_is_reported_not_overwritten(self):
        gifs.LIBRARY_FILE.write_text("{not json")
        with self.assertRaisesRegex(RuntimeError, "Failed to read GIF library"):
            _run(gifs.save, ["https://klipy.com/gifs/a", "-t", "x"])
        self.assertEqual(gifs.LIBRARY_FILE.read_text(), "{not json")


class GifDetectionTests(unittest.TestCase):
    def test_gifv_embed_is_rendered_and_not_duplicated_from_content(self):
        url = "https://klipy.com/gifs/cat-paw-VTl"
        found = gifs.gifs_in_message(_msg("1", url, embeds=[_gifv(url, "Cute Cat")]))
        self.assertEqual(found, [{"url": url, "title": "Cute Cat", "provider": "Klipy", "rendered": True}])

    def test_gif_image_embed_counts_but_other_images_do_not(self):
        embeds = [
            {"type": "image", "url": "https://media.example.com/a.gif"},
            {"type": "image", "url": "https://media.example.com/b.png"},
            {"type": "rich", "url": "https://example.com/c"},
        ]
        found = gifs.gifs_in_message(_msg("1", embeds=embeds))
        self.assertEqual([g["url"] for g in found], ["https://media.example.com/a.gif"])

    def test_unembedded_gif_page_link_is_flagged_as_not_rendered(self):
        content = "lol (https://tenor.com/view/dead-gif-123)."
        found = gifs.gifs_in_message(_msg("1", content))
        self.assertEqual(found, [{
            "url": "https://tenor.com/view/dead-gif-123", "title": "", "provider": "", "rendered": False,
        }])

    def test_uploaded_gif_attachments_use_canonical_unsigned_url(self):
        attachments = [
            {"id": "9", "filename": "x.gif", "size": 10, "content_type": "image/gif",
             "url": "https://cdn.discordapp.com/attachments/1/2/x.gif?ex=1&is=2&hm=3&"},
            {"id": "8", "filename": "y.png", "size": 10, "content_type": "image/png",
             "url": "https://cdn.discordapp.com/attachments/1/3/y.png?ex=1&is=2&hm=3&"},
        ]
        self.assertEqual(gifs.gifs_in_message(_msg("1", attachments=attachments)), [{
            "url": "https://cdn.discordapp.com/attachments/1/2/x.gif",
            "title": "x.gif", "provider": "Discord upload", "rendered": True,
        }])

    def test_signed_cdn_link_embed_is_canonicalized_and_deduplicated(self):
        signed = "https://cdn.discordapp.com/attachments/1/2/x.gif?ex=1&is=2&hm=3&"
        msg = _msg("1", signed, embeds=[{"type": "image", "url": signed}])
        self.assertEqual([g["url"] for g in gifs.gifs_in_message(msg)],
                         ["https://cdn.discordapp.com/attachments/1/2/x.gif"])


class AttachmentUrlTests(unittest.TestCase):
    def test_canonical_attachment_url(self):
        for url, expected in (
            ("https://cdn.discordapp.com/attachments/1/2/a.gif?ex=1&is=2&hm=3&",
             "https://cdn.discordapp.com/attachments/1/2/a.gif"),
            ("https://media.discordapp.net/attachments/1/2/a.gif?width=200",
             "https://cdn.discordapp.com/attachments/1/2/a.gif"),
            ("https://cdn.discordapp.com/ephemeral-attachments/1/2/a.gif",
             "https://cdn.discordapp.com/ephemeral-attachments/1/2/a.gif"),
            ("https://cdn.discordapp.com/emojis/1.gif", None),
            ("https://klipy.com/gifs/a", None),
        ):
            with self.subTest(url=url):
                self.assertEqual(gifs.canonical_attachment_url(url), expected)

    def test_status_fetches_refreshed_url_and_treats_unsigned_as_dead(self):
        alive = "https://cdn.discordapp.com/attachments/1/2/a.gif"
        gone = "https://cdn.discordapp.com/attachments/1/3/b.gif"
        refreshed = {alive: alive + "?ex=1&is=2&hm=3&", gone: None}
        with (
            patch.object(gifs.api, "refresh_attachment_urls", return_value=refreshed) as refresh,
            patch.object(gifs, "_probe", return_value=True) as probe,
        ):
            status = gifs.attachment_status([alive, gone, alive, "https://klipy.com/gifs/a"])
        refresh.assert_called_once_with([alive, gone])
        probe.assert_called_once_with(alive + "?ex=1&is=2&hm=3&")
        self.assertEqual(status, {alive: True, gone: False})

    def test_status_is_unknown_when_refresh_fails(self):
        url = "https://cdn.discordapp.com/attachments/1/2/a.gif"
        with patch.object(gifs.api, "refresh_attachment_urls", side_effect=RuntimeError("HTTP 500")):
            self.assertEqual(gifs.attachment_status([url]), {url: None})

    def test_probe_distinguishes_gone_from_unknown(self):
        import urllib.error
        for code, expected in ((404, False), (410, False), (500, None), (403, None)):
            error = urllib.error.HTTPError("u", code, "x", {}, None)
            with self.subTest(code=code), patch.object(gifs.urllib.request, "urlopen", side_effect=error):
                self.assertIs(gifs._probe("https://cdn.discordapp.com/attachments/1/2/a.gif?ex=1"), expected)
        with patch.object(gifs.urllib.request, "urlopen", side_effect=gifs.urllib.error.URLError("dns")):
            self.assertIsNone(gifs._probe("https://cdn.discordapp.com/attachments/1/2/a.gif?ex=1"))

    def test_refresh_endpoint_is_batched_and_not_audited(self):
        urls = [f"https://cdn.discordapp.com/attachments/1/{i}/a.gif" for i in range(60)]

        def fake_post(path, body=None, **kwargs):
            return {"refreshed_urls": [{"original": u, "refreshed": u + "?ex=1"} for u in body["attachment_urls"]]}

        with patch.object(gifs.api, "post", side_effect=fake_post) as post:
            result = gifs.api.refresh_attachment_urls(urls)
        self.assertEqual([len(c.kwargs["body"]["attachment_urls"]) for c in post.call_args_list], [50, 10])
        for call in post.call_args_list:
            self.assertEqual(call.args, ("/attachments/refresh-urls",))
            self.assertIs(call.kwargs["audit"], False)
        self.assertEqual(result[urls[59]], urls[59] + "?ex=1")


class AttachmentLibraryTests(unittest.TestCase):
    URL = "https://cdn.discordapp.com/attachments/1/2/a.gif"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = patch.object(gifs, "LIBRARY_FILE", Path(self.tmp.name) / "gifs.json")
        patcher.start()
        self.addCleanup(patcher.stop)

    def _save(self, status, url=None):
        with patch.object(gifs, "attachment_status", side_effect=lambda urls: {u: status for u in urls}) as check:
            out, err = _run(gifs.save, [url or self.URL + "?ex=1&is=2&hm=3&", "-t", "bleed"])
        return out, err, check

    def test_save_stores_verified_unsigned_url(self):
        out, err, check = self._save(True)
        check.assert_called_once_with([self.URL])
        self.assertIn(f"Saved {self.URL}", out)
        self.assertIn("Discord upload", err)
        entry = gifs.load_library()["gifs"][0]
        self.assertEqual(entry["url"], self.URL)
        self.assertIn("verified_at", entry)

    def test_save_refuses_deleted_or_unverifiable_uploads(self):
        for status, message in ((False, "no longer available"), (None, "could not verify")):
            with self.subTest(status=status), self.assertRaisesRegex(RuntimeError, message):
                self._save(status)
        self.assertFalse(gifs.LIBRARY_FILE.exists())

    def test_provider_urls_are_not_checked(self):
        with patch.object(gifs.api, "refresh_attachment_urls") as refresh:
            _run(gifs.save, ["https://klipy.com/gifs/a", "-t", "x"])
            _run(gifs.find, ["x"])
        refresh.assert_not_called()

    def test_find_skips_dead_uploads_and_backfills(self):
        library = {"version": 1, "gifs": [
            {"url": "https://cdn.discordapp.com/attachments/1/1/dead.gif", "tags": ["bleed"], "updated_at": "3"},
            {"url": "https://cdn.discordapp.com/attachments/1/2/live.gif", "tags": ["bleed"], "updated_at": "2"},
            {"url": "https://klipy.com/gifs/bleed", "tags": ["bleed"], "updated_at": "1"},
        ]}
        gifs.LIBRARY_FILE.write_text(json.dumps(library))
        alive = {"https://cdn.discordapp.com/attachments/1/1/dead.gif": False,
                 "https://cdn.discordapp.com/attachments/1/2/live.gif": True}
        with patch.object(gifs, "attachment_status",
                          side_effect=lambda urls: {u: alive[u] for u in urls if u in alive}):
            out, err = _run(gifs.find, ["bleed", "-n", "2", "--json"])
        self.assertEqual([g["url"] for g in json.loads(out)],
                         ["https://cdn.discordapp.com/attachments/1/2/live.gif", "https://klipy.com/gifs/bleed"])
        self.assertIn("dead.gif", err)

        with patch.object(gifs, "attachment_status") as check:
            out, _ = _run(gifs.find, ["bleed", "--no-check", "--json"])
        check.assert_not_called()
        self.assertEqual(len(json.loads(out)), 3)

    def test_check_prunes_only_dead_uploads(self):
        library = {"version": 1, "gifs": [
            {"url": "https://cdn.discordapp.com/attachments/1/1/dead.gif", "tags": ["a"]},
            {"url": "https://cdn.discordapp.com/attachments/1/2/unknown.gif", "tags": ["a"]},
            {"url": "https://klipy.com/gifs/a", "tags": ["a"]},
        ]}
        gifs.LIBRARY_FILE.write_text(json.dumps(library))
        status = {"https://cdn.discordapp.com/attachments/1/1/dead.gif": False,
                  "https://cdn.discordapp.com/attachments/1/2/unknown.gif": None}
        with patch.object(gifs, "attachment_status", return_value=status):
            out, _ = _run(gifs.check, ["--prune"])
        self.assertIn("Removed 1 dead upload(s)", out)
        self.assertEqual([g["url"] for g in gifs.load_library()["gifs"]],
                         ["https://cdn.discordapp.com/attachments/1/2/unknown.gif", "https://klipy.com/gifs/a"])

    def test_remove_accepts_signed_form(self):
        self._save(True)
        _run(gifs.remove, [self.URL + "?ex=9&is=9&hm=9&"])
        self.assertEqual(gifs.load_library()["gifs"], [])


class LearnTests(unittest.TestCase):
    CHANNEL = "100000000000000001"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = patch.object(gifs, "LIBRARY_FILE", Path(self.tmp.name) / "gifs.json")
        patcher.start()
        self.addCleanup(patcher.stop)
        url = "https://klipy.com/gifs/power-chainsaw-man-69"
        # API order: newest first.
        self.history = [
            _msg("6", "after two", name="carol"),
            _msg("5", "after one", name="bob"),
            _msg("4", url, embeds=[_gifv(url, "Power Chainsaw Man")], name="yeyito"),
            _msg("3", "before one", name="bob"),
            _msg("2", "before two", name="alice"),
            _msg("1", "too early", name="alice"),
        ]

    def _learn(self, argv):
        with (
            patch.object(gifs, "resolve_channel", return_value={"id": self.CHANNEL}),
            patch.object(gifs.api, "get_messages", return_value=self.history) as get_messages,
        ):
            out, _ = _run(gifs.learn, [self.CHANNEL] + argv)
        return out, get_messages

    def test_learn_prints_gif_with_surrounding_context(self):
        out, get_messages = self._learn(["-C", "2"])
        get_messages.assert_called_once_with(self.CHANNEL, limit=100, before=None)
        self.assertIn("GIF 1/1  https://klipy.com/gifs/power-chainsaw-man-69 — Power Chainsaw Man (Klipy)", out)
        self.assertIn(f"source: {self.CHANNEL}/4", out)
        lines = out.splitlines()
        marked = next(i for i, line in enumerate(lines) if "▶ Yeyito" in line)
        self.assertIn("before one", lines[marked - 1])
        self.assertIn("before two", lines[marked - 2])
        self.assertIn("after one", lines[marked + 1])
        self.assertIn("after two", lines[marked + 2])
        self.assertNotIn("too early", out)

    def test_learn_hides_saved_gifs_unless_all(self):
        _run(gifs.save, ["https://klipy.com/gifs/power-chainsaw-man-69", "-t", "smug"])
        out, _ = self._learn([])
        self.assertIn("No new GIFs in the last 6 message(s)", out)
        out, _ = self._learn(["--all", "--json"])
        item = json.loads(out)[0]
        self.assertEqual(item["saved_tags"], ["smug"])
        self.assertEqual(item["message"]["id"], "4")
        self.assertEqual([m["content"] for m in item["before"]], ["too early", "before two", "before one"])
        self.assertEqual([m["content"] for m in item["after"]], ["after one", "after two"])

    def test_history_paginates_backwards(self):
        pages = [
            [_msg(str(i)) for i in range(300, 200, -1)],
            [_msg(str(i)) for i in range(200, 150, -1)],
        ]
        with patch.object(gifs.api, "get_messages", side_effect=pages) as get_messages:
            messages = gifs._fetch_history(self.CHANNEL, 150)
        self.assertEqual(get_messages.call_args_list[1].kwargs, {"limit": 50, "before": "201"})
        self.assertEqual([m["id"] for m in messages[:2]], ["151", "152"])
        self.assertEqual(len(messages), 150)


class DispatchTests(unittest.TestCase):
    def test_unknown_subcommand_exits_with_message(self):
        with self.assertRaisesRegex(SystemExit, "unknown subcommand 'nope'"):
            gifs.dispatch("gifs", ["nope"])

    def test_bare_command_prints_usage(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            gifs.dispatch("gifs", [])
        self.assertIn("search QUERY", out.getvalue())
        self.assertIn("learn CHANNEL", out.getvalue())


if __name__ == "__main__":
    unittest.main()
