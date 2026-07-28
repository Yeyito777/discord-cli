import json
import os
import socket
import subprocess
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_ROOT = Path(os.environ.get("EXOCORTEX_CONFIG_DIR", "").strip() or (REPO_ROOT / "config"))


def _detect_worktree_name():
    try:
        git_dir = subprocess.check_output(
            ["git", "rev-parse", "--git-dir"],
            cwd=REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        git_common_dir = subprocess.check_output(
            ["git", "rev-parse", "--git-common-dir"],
            cwd=REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        git_dir_path = (REPO_ROOT / git_dir).resolve()
        git_common_dir_path = (REPO_ROOT / git_common_dir).resolve()
        if git_dir_path != git_common_dir_path:
            return Path(git_dir).name
    except Exception:
        pass
    return None


def _socket_path():
    worktree = _detect_worktree_name()
    runtime_dir = CONFIG_ROOT / "runtime"
    if worktree:
        runtime_dir = runtime_dir / worktree
    return runtime_dir / "exocortexd.sock"


def request(command_type, response_type, *, timeout_seconds=10, req_id=None, **fields):
    """Send one newline-JSON IPC request and return its matching response event.

    Exocortex may emit unrelated events on the socket before replying.  Match
    both the generated request ID and the expected event type, and surface a
    matching ``error`` event as ``RuntimeError``.
    """
    socket_path = _socket_path()
    if not socket_path.exists():
        raise RuntimeError("exocortexd is not running")

    req_id = req_id or f"{command_type}_{os.getpid()}_{uuid.uuid4().hex}"
    payload = {"type": command_type, "reqId": req_id}
    payload.update({key: value for key, value in fields.items() if value is not None})

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout_seconds)
    try:
        sock.connect(str(socket_path))
        sock.sendall((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))

        buffer = ""
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                raise RuntimeError("Connection closed before exocortexd replied")
            buffer += chunk.decode("utf-8")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                if event.get("reqId") != req_id:
                    continue
                if event.get("type") == "error":
                    raise RuntimeError(event.get("message") or "exocortexd returned an error")
                if event.get("type") == response_type:
                    return event
    except socket.timeout as exc:
        raise RuntimeError(f"Timed out waiting for {response_type} from exocortexd") from exc
    finally:
        try:
            sock.close()
        except Exception:
            pass


def manage_external_tool_daemon(tool_name, action, timeout_seconds=10):
    event = request(
        "manage_external_tool_daemon",
        "external_tool_daemon_result",
        timeout_seconds=timeout_seconds,
        toolName=tool_name,
        action=action,
    )
    return event.get("status") or {}


def register_external_notification_source(tool_name, source, timeout_seconds=10):
    event = request(
        "register_external_notification_source",
        "external_notification_source",
        timeout_seconds=timeout_seconds,
        toolName=tool_name,
        source=source,
    )
    return event.get("source") or {}


def list_external_notification_subscriptions(
    *, tool_name=None, source_id=None, conv_id=None, timeout_seconds=10
):
    event = request(
        "list_external_notification_subscriptions",
        "external_notification_subscriptions",
        timeout_seconds=timeout_seconds,
        toolName=tool_name,
        sourceId=source_id,
        convId=conv_id,
    )
    return event.get("subscriptions") or []


def subscribe_external_notification(
    tool_name,
    source_id,
    conv_id,
    delivery,
    *,
    source_label=None,
    source_description=None,
    timeout_seconds=10,
):
    event = request(
        "subscribe_external_notification",
        "external_notification_subscription",
        timeout_seconds=timeout_seconds,
        toolName=tool_name,
        sourceId=source_id,
        sourceLabel=source_label,
        sourceDescription=source_description,
        convId=conv_id,
        delivery=delivery,
    )
    return event.get("subscription") or {}


def unsubscribe_external_notification(
    *,
    subscription_id=None,
    tool_name=None,
    source_id=None,
    conv_id=None,
    timeout_seconds=10,
):
    if subscription_id:
        fields = {"subscriptionId": subscription_id}
    else:
        if not (tool_name and source_id and conv_id):
            raise ValueError(
                "unsubscribe requires subscription_id or tool_name/source_id/conv_id"
            )
        fields = {"toolName": tool_name, "sourceId": source_id, "convId": conv_id}
    event = request(
        "unsubscribe_external_notification",
        "external_notification_subscriptions",
        timeout_seconds=timeout_seconds,
        **fields,
    )
    return event.get("subscriptions") or []


def publish_external_notification(
    tool_name,
    source_id,
    event_id,
    text,
    *,
    occurred_at=None,
    data=None,
    timeout_seconds=10,
):
    event = request(
        "publish_external_notification",
        "external_notification_publish_result",
        timeout_seconds=timeout_seconds,
        toolName=tool_name,
        sourceId=source_id,
        eventId=event_id,
        text=text,
        occurredAt=occurred_at,
        data=data,
    )
    # Keep the helper useful if the wire event later grows top-level receipt
    # fields while supporting the conventional nested result shape now.
    return event.get("result") if "result" in event else event
