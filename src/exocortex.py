import json
import os
import socket
import subprocess
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


def manage_external_tool_daemon(tool_name, action, timeout_seconds=10):
    socket_path = _socket_path()
    if not socket_path.exists():
        raise RuntimeError(
            "exocortexd is not running. Start exocortexd to manage supervised tool daemons."
        )

    req_id = f"tool_daemon_{os.getpid()}_{tool_name}_{action}"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout_seconds)
    try:
        sock.connect(str(socket_path))
        payload = {
            "type": "manage_external_tool_daemon",
            "reqId": req_id,
            "toolName": tool_name,
            "action": action,
        }
        sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))

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
                if event.get("type") == "error" and event.get("reqId") == req_id:
                    raise RuntimeError(event.get("message") or "exocortexd returned an error")
                if event.get("type") == "external_tool_daemon_result" and event.get("reqId") == req_id:
                    return event.get("status") or {}
    finally:
        try:
            sock.close()
        except Exception:
            pass
