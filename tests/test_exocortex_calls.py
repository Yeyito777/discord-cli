import json
import socket
import threading
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

from src.calls.exocortex import ExocortexCallClient


def send_line(sock, value):
    sock.sendall(json.dumps(value).encode() + b"\n")


def recv_line(file):
    return json.loads(file.readline())


class ExocortexCallClientTests(unittest.TestCase):
    def test_runs_adapter_control_contract(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "exocortexd.sock"
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(str(path))
            server.listen(1)
            seen = []

            def serve():
                conn, _ = server.accept()
                file = conn.makefile("rb")
                try:
                    create = recv_line(file)
                    seen.append(create)
                    send_line(conn, {"type": "conversation_created", "reqId": create["reqId"], "convId": "conv-discord"})

                    subscribe = recv_line(file)
                    seen.append(subscribe)
                    send_line(conn, {"type": "ack", "reqId": subscribe["reqId"], "convId": "conv-discord"})

                    start = recv_line(file)
                    seen.append(start)
                    # Lifecycle may race ahead of the command acknowledgement.
                    send_line(conn, {
                        "type": "call_state",
                        "convId": "conv-discord",
                        "callId": "call-discord",
                        "adapter": start["adapter"],
                        "state": "waiting_for_media",
                    })
                    send_line(conn, {"type": "ack", "reqId": start["reqId"], "convId": "conv-discord"})

                    attach = recv_line(file)
                    seen.append(attach)
                    send_line(conn, {
                        "type": "call_sdp_answer",
                        "reqId": attach["reqId"],
                        "convId": "conv-discord",
                        "callId": "call-discord",
                        "adapter": start["adapter"],
                        "sdp": "v=0\r\no=answer",
                    })
                    send_line(conn, {"type": "ack", "reqId": attach["reqId"], "convId": "conv-discord"})

                    stop = recv_line(file)
                    seen.append(stop)
                    send_line(conn, {"type": "ack", "reqId": stop["reqId"], "convId": "conv-discord"})
                finally:
                    file.close()
                    conn.close()

            thread = threading.Thread(target=serve)
            thread.start()
            client = ExocortexCallClient(path, timeout=2)
            try:
                client.connect()
                conv_id = client.create_conversation("Discord call · #voice")
                adapter = {
                    "type": "discord",
                    "id": "paramount:123",
                    "accountAlias": "paramount",
                    "channelId": "123",
                    "label": "#voice",
                }
                call_id, state = client.start_call(conv_id, adapter, voice="cove")
                answer = client.attach_media(conv_id, call_id, "v=0\r\no=offer")
                client.stop_call(conv_id, call_id)
            finally:
                client.close()
                thread.join(timeout=2)
                server.close()

            self.assertEqual(
                (conv_id, call_id, state, answer),
                ("conv-discord", "call-discord", "waiting_for_media", "v=0\r\no=answer"),
            )
            self.assertEqual(
                [item["type"] for item in seen],
                ["new_conversation", "subscribe", "start_call", "attach_call_media", "stop_call"],
            )
            self.assertEqual(seen[2]["adapter"]["accountAlias"], "paramount")
            self.assertEqual(seen[4]["callId"], "call-discord")


if __name__ == "__main__":
    unittest.main()
