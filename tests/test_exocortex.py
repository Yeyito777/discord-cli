import json
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from src import exocortex


class ExocortexRequestTests(unittest.TestCase):
    def _serve_once(self, socket_path, handler):
        ready = threading.Event()
        errors = []

        def run():
            try:
                server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                server.bind(str(socket_path))
                server.listen(1)
                ready.set()
                conn, _ = server.accept()
                with conn:
                    raw = b""
                    while b"\n" not in raw:
                        raw += conn.recv(65536)
                    payload = json.loads(raw.split(b"\n", 1)[0])
                    handler(conn, payload)
                server.close()
            except Exception as exc:  # make thread failures visible to the test
                errors.append(exc)
                ready.set()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        ready.wait(2)
        return thread, errors

    def test_request_matches_req_id_and_response_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "exocortexd.sock"
            received = []

            def handler(conn, payload):
                received.append(payload)
                conn.sendall(b'{"type":"external_notification_source","reqId":"other","source":{}}\n')
                response = {
                    "type": "external_notification_source",
                    "reqId": payload["reqId"],
                    "source": payload["source"],
                }
                conn.sendall((json.dumps(response) + "\n").encode())

            thread, errors = self._serve_once(path, handler)
            with patch.object(exocortex, "_socket_path", return_value=path):
                result = exocortex.request(
                    "register_external_notification_source",
                    "external_notification_source",
                    req_id="req-1",
                    toolName="discord",
                    source={"id": "account:bot:notifications", "label": "Bot"},
                    omitted=None,
                )
            thread.join(2)

            self.assertFalse(errors)
            self.assertEqual(result["source"]["id"], "account:bot:notifications")
            self.assertEqual(received, [{
                "type": "register_external_notification_source",
                "reqId": "req-1",
                "toolName": "discord",
                "source": {"id": "account:bot:notifications", "label": "Bot"},
            }])

    def test_request_surfaces_matching_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "exocortexd.sock"

            def handler(conn, payload):
                response = {"type": "error", "reqId": payload["reqId"], "message": "nope"}
                conn.sendall((json.dumps(response) + "\n").encode())

            thread, errors = self._serve_once(path, handler)
            with patch.object(exocortex, "_socket_path", return_value=path):
                with self.assertRaisesRegex(RuntimeError, "nope"):
                    exocortex.request("something", "ack", req_id="req-2")
            thread.join(2)
            self.assertFalse(errors)

    def test_notification_helpers_use_wire_field_names(self):
        subscription = {"id": "sub-1", "convId": "conv-1", "delivery": "inbox"}
        with patch.object(exocortex, "request") as request:
            request.side_effect = [
                {"source": {"id": "source-1"}},
                {"subscriptions": [subscription]},
                {"subscription": subscription},
                {"subscriptions": []},
                {"result": {"delivered": 1}},
            ]

            self.assertEqual(
                exocortex.register_external_notification_source(
                    "discord", {"id": "source-1", "label": "Source"}
                ),
                {"id": "source-1"},
            )
            self.assertEqual(
                exocortex.list_external_notification_subscriptions(
                    tool_name="discord", source_id="source-1", conv_id="conv-1"
                ),
                [subscription],
            )
            self.assertEqual(
                exocortex.subscribe_external_notification(
                    "discord", "source-1", "conv-1", "inbox", source_label="Source"
                ),
                subscription,
            )
            self.assertEqual(
                exocortex.unsubscribe_external_notification(
                    tool_name="discord", source_id="source-1", conv_id="conv-1"
                ),
                [],
            )
            self.assertEqual(
                exocortex.publish_external_notification(
                    "discord",
                    "source-1",
                    "event-1",
                    "hello",
                    occurred_at="2026-07-15T12:00:00Z",
                    data={"schemaVersion": 1, "kind": "dm"},
                ),
                {"delivered": 1},
            )

        self.assertEqual(request.call_args_list[2].kwargs, {
            "timeout_seconds": 10,
            "toolName": "discord",
            "sourceId": "source-1",
            "sourceLabel": "Source",
            "sourceDescription": None,
            "convId": "conv-1",
            "delivery": "inbox",
        })
        self.assertEqual(request.call_args_list[3].kwargs, {
            "timeout_seconds": 10,
            "toolName": "discord",
            "sourceId": "source-1",
            "convId": "conv-1",
        })
        self.assertEqual(request.call_args_list[4].kwargs["occurredAt"], "2026-07-15T12:00:00Z")
        self.assertEqual(
            request.call_args_list[4].kwargs["data"],
            {"schemaVersion": 1, "kind": "dm"},
        )

    def test_unsubscribe_by_id_does_not_mix_tuple_fields(self):
        with patch.object(exocortex, "request", return_value={"subscriptions": []}) as request:
            exocortex.unsubscribe_external_notification(
                subscription_id="sub-1", tool_name="ignored", source_id="ignored", conv_id="ignored"
            )
        self.assertEqual(request.call_args.kwargs, {
            "timeout_seconds": 10,
            "subscriptionId": "sub-1",
        })


if __name__ == "__main__":
    unittest.main()
