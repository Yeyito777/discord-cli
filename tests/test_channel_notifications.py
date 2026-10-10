import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src import channel_notifications as lifecycle, notify
from src import exocortex
from src.gateway import GatewayListener

C = "709232473783140375"
G = "709232473271304285"
ACCOUNT = {"alias": "paramount", "owner": "assistant"}
CHANNEL = {"id": C, "guild_id": G, "type": 0, "name": "storage"}


class ChannelCliTests(unittest.TestCase):
    def test_source_is_account_and_channel_scoped(self):
        source = notify.channel_notification_source(C, ACCOUNT)
        self.assertEqual(source["id"], f"account:paramount:channel:{C}:messages")
        self.assertNotEqual(source["id"], notify.notification_source(ACCOUNT)["id"])
        self.assertNotEqual(source["id"], notify.channel_notification_source(C, {"alias": "other"})["id"])

    def test_exact_names_and_guild_validation(self):
        with (patch("src.api.get_guilds", return_value=[{"id": G, "name": "The Toolbar Cult"}]),
              patch("src.api.get_guild_channels", return_value=[CHANNEL]),
              patch("src.api.get_channel", return_value=CHANNEL)):
            self.assertEqual(notify._resolve_notification_channel("#storage", "The Toolbar Cult"), C)
            self.assertEqual(notify._resolve_notification_channel(C, G), C)
            for target, guild in [("stor", G), ("storage", None), ("storage", "Toolbar"),
                                  (C, "111111111111111111")]:
                with self.subTest(target=target, guild=guild), self.assertRaises(RuntimeError):
                    notify._resolve_notification_channel(target, guild)

    def test_ambiguous_and_private_channels_rejected(self):
        with patch("src.api.get_guild_channels", return_value=[CHANNEL, CHANNEL]):
            with self.assertRaises(RuntimeError):
                notify._resolve_notification_channel("storage", G)
        with patch("src.api.get_channel", return_value={"id": C, "type": 1}):
            with self.assertRaises(RuntimeError):
                notify._resolve_notification_channel(C, validate=True)

    def test_subscribe_channel_and_no_implicit_listener(self):
        with (patch.object(notify, "selected_account", return_value=ACCOUNT),
              patch("src.api.get_channel", return_value=CHANNEL),
              patch.object(notify, "subscribe_external_notification", return_value={"id": "sub"}) as subscribe,
              patch.object(lifecycle, "start_listener") as start,
              patch("sys.stdout", new_callable=io.StringIO)):
            notify.subscribe(["conv", "--channel", C, "--guild", G, "--delivery", "inbox"])
        source = notify.channel_notification_source(C, ACCOUNT)
        subscribe.assert_called_once_with("discord", source["id"], "conv", "inbox", source_label=source["label"])
        start.assert_not_called()

    def test_list_unsubscribe_by_id_need_no_discord_access(self):
        with (patch.object(notify, "selected_account", return_value=ACCOUNT),
              patch.object(notify, "list_external_notification_subscriptions", return_value=[]) as listing,
              patch.object(notify, "unsubscribe_external_notification") as unsubscribe,
              patch.object(notify, "get_labels", return_value={}),
              patch.object(lifecycle, "listener_status", return_value="ready (PID 5)"),
              patch("src.api.get_channel") as fetch,
              patch("sys.stdout", new_callable=io.StringIO) as output):
            notify.list_config(["--channel", C])
            notify.unsubscribe(["conv", "--channel", C])
        source_id = notify.channel_notification_source(C, ACCOUNT)["id"]
        listing.assert_called_once_with(tool_name="discord", source_id=source_id)
        unsubscribe.assert_called_once_with(tool_name="discord", source_id=source_id, conv_id="conv")
        fetch.assert_not_called()
        self.assertIn("ready (PID 5)", output.getvalue())

    def test_registry_id_cannot_remove_other_source(self):
        with (patch.object(notify, "selected_account", return_value=ACCOUNT),
              patch.object(notify, "list_external_notification_subscriptions", return_value=[]),
              patch.object(notify, "unsubscribe_external_notification") as remove):
            with self.assertRaises(RuntimeError):
                notify.unsubscribe(["--subscription-id", "other-account-route", "--channel", C])
        remove.assert_not_called()

    def test_channel_start_stop_do_not_manage_account_daemon(self):
        with (patch.object(notify, "selected_account", return_value=ACCOUNT),
              patch("src.api.get_channel", return_value=CHANNEL),
              patch.object(lifecycle, "start_listener") as start,
              patch.object(lifecycle, "stop_listener") as stop,
              patch.object(notify, "manage_external_tool_daemon") as daemon):
            notify.start(["--channel", C])
            notify.stop(["--channel", C])
        start.assert_called_once_with(C)
        stop.assert_called_once_with(C)
        daemon.assert_not_called()


class ChannelGatewayTests(unittest.TestCase):
    def listener(self):
        listener = object.__new__(GatewayListener)
        listener.channel_id = lifecycle.PREFIX + C
        listener.notification_channel = C
        listener.notification_source = notify.channel_notification_source(C, ACCOUNT)
        listener.account = ACCOUNT
        listener.my_id = "self"
        listener._guilds = {G: "The Toolbar Cult"}
        listener._channels = {C: {"name": "storage"}}
        listener._notification_seen = {}
        listener._write = Mock()
        listener._log = Mock()
        listener._queue_notification = Mock()
        return listener

    def message(self, **fields):
        return {"id": "post-1", "channel_id": C, "guild_id": G,
                "author": {"id": "friend", "username": "Friend"},
                "content": "ordinary post", "mentions": [], **fields}

    def test_normal_mention_attachment_posts_use_one_channel_path(self):
        for changes in ({}, {"mentions": [{"id": "self"}]},
                        {"content": "", "attachments": [{"id": "attachment", "filename": "notes.txt",
                                                       "url": "https://cdn/file?secret=signed"}]}):
            with self.subTest(changes=changes):
                listener = self.listener()
                listener._on_dispatch("MESSAGE_CREATE", self.message(**changes))
                listener._queue_notification.assert_called_once()
                event = listener._queue_notification.call_args.args[0]
                self.assertEqual(event["type"], "channel_message")
                self.assertNotIn("signed", json.dumps(event))
                with (patch("src.notify.get_labels", return_value={}),
                      patch.object(listener, "_fetch_channel_history") as history,
                      patch("src.exocortex.publish_external_notification", return_value={"deliveries": []}) as publish):
                    listener._publish_notification(event)
                    listener._publish_notification(event)  # retry/replay has the same core dedup key
                history.assert_not_called()
                self.assertEqual(publish.call_count, 2)
                for call in publish.call_args_list:
                    self.assertEqual(call.args[:3], ("discord", listener.notification_source["id"], "post-1"))
                    self.assertNotIn("convId", call.kwargs)
                    self.assertEqual(call.kwargs["data"]["guild"]["id"], G)
                    self.assertEqual(call.kwargs["data"]["kind"], "channel_message")
                if "attachments" in changes:
                    self.assertIn("[attachment:attachment] notes.txt", publish.call_args.args[3])
                    self.assertEqual(publish.call_args.kwargs["data"]["content"], "")

    def test_filter_other_channels_dm_self_edits_history_and_calls(self):
        for event, message in [
            ("MESSAGE_CREATE", self.message(channel_id="elsewhere")),
            ("MESSAGE_CREATE", self.message(guild_id=None)),
            ("MESSAGE_CREATE", self.message(author={"id": "self"})),
            ("MESSAGE_CREATE", self.message(type=6)),
            ("MESSAGE_UPDATE", self.message()),
            ("MESSAGE_DELETE", self.message()),
            ("CALL_CREATE", self.message()),
            ("GUILD_CREATE", self.message()),
        ]:
            with self.subTest(event=event, message=message):
                listener = self.listener()
                listener._on_dispatch(event, message)
                listener._queue_notification.assert_not_called()

    def test_legacy_account_filters_ordinary_server_posts(self):
        listener = self.listener()
        listener.notification_channel = None
        listener.channel_id = "__notify__"
        listener._on_notify("MESSAGE_CREATE", self.message())
        listener._queue_notification.assert_not_called()
        listener._on_notify("MESSAGE_CREATE", self.message(mentions=[{"id": "self"}]))
        self.assertEqual(listener._queue_notification.call_args.args[0]["type"], "mention")

    def test_guild_stream_activated_on_ready_and_resume(self):
        listener = self.listener()
        listener.ws = Mock()
        listener._channel_notify_state = Mock()
        with patch("src.api.get_channel", return_value=CHANNEL):
            listener._on_dispatch("READY", {
                "session_id": "session", "user": {"id": "self", "username": "Me"}, "guilds": [],
            })
            listener._on_dispatch("RESUMED", {})
        self.assertEqual(listener.ws.send.call_count, 2)
        payload = json.loads(listener.ws.send.call_args.args[0])
        self.assertEqual(payload["op"], 14)
        self.assertEqual(payload["d"]["guild_id"], G)
        self.assertEqual(list(payload["d"]["channels"]), [C])
        listener._channel_notify_state.assert_called_with("ready")

    def test_failed_delivery_retries_stable_event_id(self):
        listener = self.listener()
        listener._on_notify("MESSAGE_CREATE", self.message())
        event = listener._queue_notification.call_args.args[0]
        with (patch("src.notify.get_labels", return_value={}),
              patch("src.exocortex.publish_external_notification",
                    return_value={"deliveries": [{"status": "failed"}]}) as publish):
            with self.assertRaises(RuntimeError):
                listener._publish_notification(event)
        self.assertEqual(publish.call_args.args[2], "post-1")
        self.assertFalse(listener._notification_seen)

    def test_core_owns_live_routes_and_dedup_without_worker_restart(self):
        listener = self.listener()
        source = listener.notification_source["id"]
        routes, seen, deliveries = {}, set(), []

        def core(tool, source_id, event_id, text, **kwargs):
            receipts = []
            for subscription in routes.get(source_id, []):
                key = (subscription, event_id)
                status = "duplicate" if key in seen else "queued"
                if status == "queued":
                    deliveries.append(key)
                    seen.add(key)
                receipts.append({"status": status})
            return {"deliveries": receipts}

        listener._on_notify("MESSAGE_CREATE", self.message())
        event = listener._queue_notification.call_args.args[0]
        with (patch("src.notify.get_labels", return_value={}),
              patch("src.exocortex.publish_external_notification", side_effect=core)):
            listener._publish_notification(event)  # source exists, no routes
            routes[source] = ["sub-one"]
            routes["account:other:channel:" + C + ":messages"] = ["other-account"]
            routes["account:paramount:notifications"] = ["account-route"]
            listener._publish_notification(event)
            listener._publish_notification(event)  # reconnect replay
            routes[source] = ["sub-two"]  # unsubscribe + subscribe, no worker change
            listener._publish_notification({**event, "msg_id": "post-2"})
        self.assertEqual(deliveries, [("sub-one", "post-1"), ("sub-two", "post-2")])


class ChannelLifecycleTests(unittest.TestCase):
    def test_explicit_daemon_endpoint_is_inherited(self):
        with patch.dict(os.environ, {"EXOCORTEX_SOCKET": "/tmp/example-instance.sock"}):
            self.assertEqual(exocortex._socket_path(), Path("/tmp/example-instance.sock"))

    def test_process_identity_is_account_channel_and_path_specific(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(lifecycle, "listener_dir", return_value=Path(tmp)):
            files = lifecycle.paths(C)
            files["pid"].write_text("123")
            expected = f"python {lifecycle.PROJECT_DIR / 'src/gateway.py'} {lifecycle.PREFIX}{C} {files['log']}"
            with patch("subprocess.check_output", return_value=expected):
                self.assertEqual(lifecycle.owned_pid(C), 123)
                files["state"].write_text(json.dumps({"pid": 123, "status": "ready"}))
                self.assertEqual(lifecycle.listener_status(C), "ready (PID 123)")
            with patch("subprocess.check_output", return_value=expected.replace(str(files["log"]), "/other/account.log")):
                self.assertIsNone(lifecycle.owned_pid(C))

    def test_repeated_start_reuses_ready_listener_without_spawn(self):
        with (patch.object(lifecycle, "owned_pid", return_value=123),
              patch.object(lifecycle, "listener_status", return_value="ready (PID 123)"),
              patch("subprocess.Popen") as spawn,
              patch("sys.stdout", new_callable=io.StringIO)):
            lifecycle.start_listener(C)
        spawn.assert_not_called()

    def test_start_detaches_and_timeout_does_not_kill_reconnecting_worker(self):
        with (tempfile.TemporaryDirectory() as tmp,
              patch.object(lifecycle, "listener_dir", return_value=Path(tmp)),
              patch.object(lifecycle, "owned_pid", return_value=None),
              patch.object(lifecycle, "listener_status", return_value="connecting (PID 123)"),
              patch("subprocess.Popen") as spawn):
            with self.assertRaisesRegex(RuntimeError, "not ready"):
                lifecycle.start_listener(C, timeout=0)
        self.assertTrue(spawn.call_args.kwargs["start_new_session"])
        self.assertEqual(spawn.call_args.args[0][2], lifecycle.PREFIX + C)
        spawn.return_value.terminate.assert_not_called()

    def test_singleton_is_independent_of_account_listener(self):
        with tempfile.TemporaryDirectory() as tmp:
            files = {key: Path(tmp) / f"channel.{key}" for key in ("lock", "pid", "state")}
            first = object.__new__(GatewayListener)
            first._channel_notify_paths = files
            first._notify_lock_fd = None
            second = object.__new__(GatewayListener)
            second._channel_notify_paths = files
            second._notify_lock_fd = None
            with patch("atexit.register"):
                first._acquire_notify_singleton()
                try:
                    self.assertEqual(files["pid"].read_text().strip(), str(os.getpid()))
                    with self.assertRaises(SystemExit):
                        second._acquire_notify_singleton()
                finally:
                    first._release_notify_singleton()
            self.assertFalse(files["pid"].exists())

    def test_stop_only_signals_owned_channel_worker_and_retains_registry(self):
        with (patch.object(lifecycle, "owned_pid", return_value=123),
              patch.object(notify, "_stop_notify_pids", return_value=([123], [])) as stop,
              patch.object(notify, "unsubscribe_external_notification") as unsubscribe,
              patch("sys.stdout", new_callable=io.StringIO)):
            lifecycle.stop_listener(C)
        stop.assert_called_once_with([123])
        unsubscribe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
