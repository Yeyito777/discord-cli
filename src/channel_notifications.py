"""Account+channel-scoped Gateway status; the supervised daemon owns the processes.

Channel listeners follow core subscriptions: ``src.supervisor`` runs one
Gateway per subscribed channel and stops it after the last unsubscribe.  The CLI
only nudges the daemon and reads the Gateway's PID/state files.
"""

import re
import time
from pathlib import Path

from src.listener_state import gateway_paths, gateway_status
from src.listener_state import owned_pid as owned_gateway_pid

PREFIX = "__channel_notify__"
PROJECT_DIR = Path(__file__).resolve().parent.parent


def paths(channel_id, alias=None):
    if not re.fullmatch(r"\d{17,20}", channel_id):
        raise ValueError("Expected a channel snowflake")
    return gateway_paths(f"{PREFIX}{channel_id}", alias)


def owned_pid(channel_id, alias=None):
    paths(channel_id, alias)  # validates the snowflake
    return owned_gateway_pid(f"{PREFIX}{channel_id}", alias)


def listener_status(channel_id, alias=None):
    paths(channel_id, alias)
    return gateway_status(f"{PREFIX}{channel_id}", alias)


def daemon_status():
    from src.supervisor import supervisor_pid
    pid = supervisor_pid()
    return f"supervised by the Discord daemon (PID {pid})" if pid else "Discord daemon not running"


def ensure_daemon():
    """Nudge the supervised daemon to reconcile now, starting it if needed."""
    from src.exocortex import manage_external_tool_daemon
    from src.supervisor import request_reconcile
    if not request_reconcile():
        # A starting daemon reconciles immediately.
        status = manage_external_tool_daemon("discord", "start")
        print(f"  {status.get('message', 'Requested start for supervised Discord daemon')}")


def start_listener(channel_id, *, timeout=30):
    """Wait for the daemon-owned Gateway of a subscribed channel to be ready."""
    ensure_daemon()
    files = paths(channel_id)
    deadline = time.monotonic() + timeout
    while True:
        status = listener_status(channel_id)
        if status.startswith("ready "):
            print(f"  Channel {channel_id}: {status}, {daemon_status()}")
            return
        if time.monotonic() >= deadline:
            break
        time.sleep(0.2)
    raise RuntimeError(
        f"Channel listener not ready ({status}); the Discord daemon keeps retrying. "
        f"Inspect {files['err']} or the daemon's config/service.log."
    )


def stop_listener(channel_id):
    """Stop a channel Gateway that no longer has subscriptions.

    The daemon stops it on its own within seconds of the last unsubscribe; this
    only makes that immediate and removes any Gateway started outside it.
    """
    from src.notify import _stop_notify_pids
    from src.supervisor import request_reconcile
    if request_reconcile():
        deadline = time.monotonic() + 6
        while owned_pid(channel_id) and time.monotonic() < deadline:
            time.sleep(0.2)
    pid = owned_pid(channel_id)
    if pid:
        _, alive = _stop_notify_pids([pid])
        if alive:
            raise RuntimeError(f"Failed to stop channel listener PID {pid}")
    print(f"  Stopped channel {channel_id} listener.")
