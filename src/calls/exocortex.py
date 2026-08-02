"""Small persistent Exocortex IPC client for Discord realtime-call adapters."""

from __future__ import annotations

from collections import deque
import json
import os
from pathlib import Path
import socket
import time
import uuid


DEFAULT_TIMEOUT = 30.0


def default_exocortex_socket() -> Path:
    configured = os.environ.get("EXOCORTEX_SOCKET", "").strip()
    if configured:
        return Path(configured).expanduser()
    # discord-cli is normally <exocortex>/external-tools/discord-cli. Resolve the
    # physical checkout so the main daemon remains the default; isolated tests
    # and worktrees select their socket explicitly through EXOCORTEX_SOCKET.
    root = Path(__file__).resolve().parents[4]
    return root / "config" / "runtime" / "exocortexd.sock"


class ExocortexCallClient:
    """JSONL control connection owned for the lifetime of one Discord call."""

    def __init__(self, socket_path: str | os.PathLike | None = None, *, timeout: float = DEFAULT_TIMEOUT):
        self.socket_path = Path(socket_path).expanduser() if socket_path else default_exocortex_socket()
        self.timeout = float(timeout)
        self.sock: socket.socket | None = None
        self.buffer = b""
        self.pending = deque()

    def connect(self):
        if self.sock is not None:
            return
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(str(self.socket_path))
        self.sock = sock

    def close(self):
        sock, self.sock = self.sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def send(self, message: dict):
        if self.sock is None:
            raise RuntimeError("Exocortex call client is not connected")
        self.sock.sendall(json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n")

    def receive(self, *, timeout: float | None = None) -> dict:
        if self.pending:
            return self.pending.popleft()
        if self.sock is None:
            raise RuntimeError("Exocortex call client is not connected")
        deadline = time.monotonic() + (self.timeout if timeout is None else float(timeout))
        while True:
            newline = self.buffer.find(b"\n")
            if newline >= 0:
                raw, self.buffer = self.buffer[:newline], self.buffer[newline + 1:]
                if not raw.strip():
                    continue
                value = json.loads(raw.decode("utf-8"))
                if not isinstance(value, dict):
                    raise RuntimeError("Exocortex returned a non-object IPC event")
                return value
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for Exocortex call event")
            self.sock.settimeout(remaining)
            chunk = self.sock.recv(64 * 1024)
            if not chunk:
                raise ConnectionError("Exocortex daemon disconnected")
            self.buffer += chunk

    def wait_for(self, predicate, *, timeout: float | None = None) -> dict:
        deadline = time.monotonic() + (self.timeout if timeout is None else float(timeout))
        deferred = []
        try:
            while True:
                event = self.receive(timeout=max(0.001, deadline - time.monotonic()))
                if predicate(event):
                    return event
                deferred.append(event)
        finally:
            self.pending.extendleft(reversed(deferred))

    def request(self, message: dict, *, timeout: float | None = None) -> dict:
        req_id = str(message.get("reqId") or f"discord-call-{uuid.uuid4()}")
        message = {**message, "reqId": req_id}
        self.send(message)
        event = self.wait_for(lambda item: item.get("reqId") == req_id, timeout=timeout)
        if event.get("type") == "error":
            raise RuntimeError(str(event.get("message") or "Exocortex call request failed"))
        if event.get("type") != "ack":
            raise RuntimeError(f"Unexpected Exocortex response: {event.get('type')}")
        return event

    def create_conversation(self, title: str) -> str:
        req_id = f"discord-call-create-{uuid.uuid4()}"
        self.send({
            "type": "new_conversation",
            "reqId": req_id,
            "provider": "openai",
            "title": title,
        })
        created = self.wait_for(
            lambda event: event.get("type") in {"conversation_created", "error"} and event.get("reqId") == req_id,
        )
        if created.get("type") == "error":
            raise RuntimeError(str(created.get("message") or "Could not create the Discord call conversation"))
        conv_id = str(created.get("convId") or "").strip()
        if not conv_id:
            raise RuntimeError("Exocortex did not return a conversation ID")
        return conv_id

    def start_call(self, conv_id: str, adapter: dict, *, voice: str | None = None) -> tuple[str, str]:
        self.request({"type": "subscribe", "convId": conv_id})
        req_id = f"discord-call-start-{uuid.uuid4()}"
        command = {
            "type": "start_call",
            "reqId": req_id,
            "convId": conv_id,
            "adapter": adapter,
        }
        if voice:
            command["voice"] = voice
        self.send(command)
        acked = False
        call_id = None
        state = None
        deadline = time.monotonic() + self.timeout
        deferred = []
        try:
            while not acked or not call_id or state != "waiting_for_media":
                event = self.receive(timeout=max(0.001, deadline - time.monotonic()))
                if event.get("reqId") == req_id:
                    if event.get("type") == "error":
                        raise RuntimeError(str(event.get("message") or "Could not start the Discord call"))
                    acked = event.get("type") == "ack"
                    continue
                if event.get("type") == "call_state" and event.get("convId") == conv_id:
                    event_adapter = event.get("adapter") or {}
                    if event_adapter.get("type") == adapter.get("type") and event_adapter.get("id") == adapter.get("id"):
                        call_id = str(event.get("callId") or "")
                        state = str(event.get("state") or "")
                        if state == "error":
                            raise RuntimeError(str(event.get("message") or "Discord call preparation failed"))
                        continue
                deferred.append(event)
        finally:
            self.pending.extendleft(reversed(deferred))
        return call_id, state or "starting"

    def attach_media(self, conv_id: str, call_id: str, offer_sdp: str) -> str:
        req_id = f"discord-call-media-{uuid.uuid4()}"
        self.send({
            "type": "attach_call_media",
            "reqId": req_id,
            "convId": conv_id,
            "callId": call_id,
            "offerSdp": offer_sdp,
        })
        answer = None
        acked = False
        deadline = time.monotonic() + self.timeout
        deferred = []
        try:
            while answer is None or not acked:
                event = self.receive(timeout=max(0.001, deadline - time.monotonic()))
                if event.get("reqId") != req_id:
                    deferred.append(event)
                    continue
                if event.get("type") == "error":
                    raise RuntimeError(str(event.get("message") or "Could not attach Discord call media"))
                if event.get("type") == "ack":
                    acked = True
                elif event.get("type") == "call_sdp_answer":
                    answer = str(event.get("sdp") or "")
        finally:
            self.pending.extendleft(reversed(deferred))
        if not answer.startswith("v=0"):
            raise RuntimeError("Exocortex returned an invalid call SDP answer")
        return answer

    def stop_call(self, conv_id: str, call_id: str):
        self.request({"type": "stop_call", "convId": conv_id, "callId": call_id})
