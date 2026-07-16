"""Discord notification source, subscriptions, and sender-label configuration.

Exocortex owns notification subscriptions and delivery.  The selected Discord
account keeps only external sender labels in its ``notify.json``.  Older
``relay_targets`` values are imported into Exocortex once when the supervised
notification daemon starts.
"""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

from src.accounts import (
    account_label,
    account_root,
    listener_dir,
    selected_account,
    selected_alias,
    supervised_notify_alias,
)
from src.exocortex import (
    list_external_notification_subscriptions,
    manage_external_tool_daemon,
    subscribe_external_notification,
    unsubscribe_external_notification,
)

TOOL_NAME = "discord"
CONFIG_DIR = account_root()
CONFIG_FILE = CONFIG_DIR / "notify.json"
LISTENER_DIR = listener_dir()
PROJECT_DIR = Path(__file__).resolve().parent.parent


# ─── Source and config management ─────────────────────────────────────────────


def notification_source(account=None):
    """Return the generic notification source metadata for one Discord account."""
    account = account or selected_account()
    alias = account.get("alias") or selected_alias(required=False) or "legacy"
    return {
        "id": f"account:{alias}:notifications",
        "label": f"{account_label(account)} · DMs and @mentions",
    }


def _load_config():
    if CONFIG_FILE.exists():
        try:
            value = json.loads(CONFIG_FILE.read_text())
            if isinstance(value, dict):
                value.setdefault("labels", {})
                return value
        except (json.JSONDecodeError, OSError):
            pass
    return {"labels": {}}


def _save_config(cfg):
    """Atomically preserve account-local sender-label configuration."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_FILE.with_suffix(CONFIG_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(cfg, indent=2) + "\n")
    tmp.replace(CONFIG_FILE)


def get_labels():
    """Return dict of user_id → {label, username, display_name}."""
    labels = _load_config().get("labels", {})
    return labels if isinstance(labels, dict) else {}


def get_subscription_targets(*, delivery=None):
    """Return conversation IDs subscribed to the selected account's source.

    This compatibility helper is used by detached Discord call sessions.  New
    gateway notification delivery is always performed by Exocortex itself.
    """
    source = notification_source()
    subscriptions = list_external_notification_subscriptions(
        tool_name=TOOL_NAME, source_id=source["id"]
    )
    targets = []
    seen = set()
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        if delivery and subscription.get("delivery") != delivery:
            continue
        conv_id = str(subscription.get("convId") or "").strip()
        if conv_id and conv_id not in seen:
            targets.append(conv_id)
            seen.add(conv_id)
    return targets


def _migrate_relay_targets():
    """Import legacy relay targets into core, then remove the legacy key.

    Listing first makes retries safe if an earlier startup imported only part
    of the file or imported everything but failed while rewriting notify.json.
    The old key is retained on every import failure.
    """
    cfg = _load_config()
    if "relay_targets" not in cfg:
        return 0

    raw_targets = cfg.get("relay_targets")
    if not isinstance(raw_targets, list):
        raise RuntimeError("notify.json relay_targets must be a list")

    targets = []
    seen_targets = set()
    for raw_target in raw_targets:
        target = str(raw_target).strip() if isinstance(raw_target, str) else ""
        if not target:
            raise RuntimeError("notify.json contains an invalid relay target")
        if target not in seen_targets:
            targets.append(target)
            seen_targets.add(target)

    source = notification_source()
    existing = list_external_notification_subscriptions(
        tool_name=TOOL_NAME, source_id=source["id"]
    )
    subscribed_conversations = {
        str(subscription.get("convId"))
        for subscription in existing
        if isinstance(subscription, dict) and subscription.get("convId")
    }

    imported = 0
    for conv_id in targets:
        if conv_id in subscribed_conversations:
            continue
        subscribe_external_notification(
            TOOL_NAME,
            source["id"],
            conv_id,
            "wake",
            source_label=source["label"],
        )
        subscribed_conversations.add(conv_id)
        imported += 1

    # Do not mutate the on-disk file until all subscriptions succeeded.  The
    # atomic replacement also makes a crash during cleanup retry-safe.
    cfg.pop("relay_targets", None)
    _save_config(cfg)
    return imported


# ─── Listener process management ──────────────────────────────────────────────


def _find_notify_gateway_pids():
    """Return PIDs of any running account-local __notify__ gateways."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", rf"gateway\.py\s+__notify__\s+{re.escape(str(LISTENER_DIR / '__notify__.log'))}"],
            capture_output=True,
            text=True,
        )
        pids = []
        for raw in result.stdout.strip().split():
            try:
                pid = int(raw)
                os.kill(pid, 0)
            except (ValueError, ProcessLookupError):
                continue
            pids.append(pid)
        return pids
    except Exception:
        return []


def _find_notify_gateway_pid():
    """Return PID of any running __notify__ gateway process, or None."""
    pids = _find_notify_gateway_pids()
    return pids[0] if pids else None


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _is_our_notify_pid(pid):
    try:
        cmdline = Path(f"/proc/{int(pid)}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    except Exception:
        return False
    return "gateway.py" in cmdline and "__notify__" in cmdline and str(LISTENER_DIR) in cmdline


def _listener_paths():
    return {
        "pid": LISTENER_DIR / "__notify__.pid",
        "log": LISTENER_DIR / "__notify__.log",
        "err": LISTENER_DIR / "__notify__.err",
        "meta": LISTENER_DIR / "__notify__.meta",
    }


def _write_pid_hint(pid):
    LISTENER_DIR.mkdir(parents=True, exist_ok=True)
    _listener_paths()["pid"].write_text(f"{pid}\n")


def _collect_notify_pids():
    """Return all live notify gateway PIDs, preferring the PID file first."""
    paths = _listener_paths()
    pids = []
    seen = set()

    if paths["pid"].exists():
        try:
            candidate = int(paths["pid"].read_text().strip())
            os.kill(candidate, 0)
            if not _is_our_notify_pid(candidate):
                raise ProcessLookupError
            pids.append(candidate)
            seen.add(candidate)
        except (ProcessLookupError, ValueError):
            paths["pid"].unlink(missing_ok=True)

    for pid in _find_notify_gateway_pids():
        if pid in seen:
            continue
        pids.append(pid)
        seen.add(pid)

    return pids


def _stop_notify_pids(pids):
    """Terminate notify gateway PIDs, returning (stopped, still_alive)."""
    pids = list(dict.fromkeys(pids))
    if not pids:
        return [], []

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    alive = list(pids)
    for _ in range(10):
        if not alive:
            break
        time.sleep(0.5)
        alive = [pid for pid in alive if _pid_alive(pid)]

    for pid in alive:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    if alive:
        time.sleep(0.2)
        alive = [pid for pid in alive if _pid_alive(pid)]

    stopped = [pid for pid in pids if pid not in set(alive)]
    return stopped, alive


# ─── Commands ─────────────────────────────────────────────────────────────────


def _subscribe(argv, *, prog="discord notify subscribe"):
    p = argparse.ArgumentParser(
        prog=prog, description="Subscribe an Exocortex conversation to Discord notifications."
    )
    p.add_argument("conv_id", help="Exocortex conversation ID")
    p.add_argument(
        "--delivery", choices=("wake", "inbox"), default="wake",
        help="Delivery mode (default: wake)",
    )
    args = p.parse_args(argv)

    source = notification_source()
    subscription = subscribe_external_notification(
        TOOL_NAME,
        source["id"],
        args.conv_id,
        args.delivery,
        source_label=source["label"],
    )
    subscription_id = subscription.get("id") or subscription.get("subscriptionId")
    suffix = f" [{subscription_id}]" if subscription_id else ""
    print(f"  Subscribed {args.conv_id} ({args.delivery}){suffix}")


def subscribe(argv):
    return _subscribe(argv)


def add(argv):
    """Backward-compatible alias for subscribe."""
    return _subscribe(argv, prog="discord notify add")


def _unsubscribe(argv, *, prog="discord notify unsubscribe"):
    p = argparse.ArgumentParser(
        prog=prog, description="Unsubscribe an Exocortex conversation from Discord notifications."
    )
    p.add_argument("conv_id", nargs="?", help="Exocortex conversation ID")
    p.add_argument("--subscription-id", help="Remove a subscription by registry ID")
    args = p.parse_args(argv)
    if bool(args.conv_id) == bool(args.subscription_id):
        p.error("specify exactly one conversation ID or --subscription-id")

    source = notification_source()
    if args.subscription_id:
        account_subscriptions = list_external_notification_subscriptions(
            tool_name=TOOL_NAME, source_id=source["id"]
        )
        belongs_to_account = any(
            isinstance(subscription, dict)
            and (subscription.get("id") or subscription.get("subscriptionId")) == args.subscription_id
            for subscription in account_subscriptions
        )
        if not belongs_to_account:
            raise RuntimeError(
                f"Subscription {args.subscription_id} does not belong to {source['id']}"
            )
        unsubscribe_external_notification(subscription_id=args.subscription_id)
        print(f"  Unsubscribed registry entry: {args.subscription_id}")
    else:
        unsubscribe_external_notification(
            tool_name=TOOL_NAME, source_id=source["id"], conv_id=args.conv_id
        )
        print(f"  Unsubscribed: {args.conv_id}")


def unsubscribe(argv):
    return _unsubscribe(argv)


def remove(argv):
    """Backward-compatible alias for unsubscribe."""
    return _unsubscribe(argv, prog="discord notify remove")


def label(argv):
    p = argparse.ArgumentParser(
        prog="discord notify label",
        description="Set a local label for a Discord sender (keyed by user ID).",
    )
    p.add_argument("user_id", help="Discord user ID (snowflake)")
    p.add_argument(
        "label_value", nargs="?", metavar="label",
        help="Label (e.g. owner, friend). Omit to remove.",
    )
    p.add_argument("--username", "-u", help="Username (for display, not matching)")
    p.add_argument("--name", "-n", help="Display name / nickname (for display)")
    args = p.parse_args(argv)

    cfg = _load_config()
    labels = cfg.setdefault("labels", {})
    if not isinstance(labels, dict):
        labels = cfg["labels"] = {}

    if args.label_value:
        entry = {"label": args.label_value}
        if args.username:
            entry["username"] = args.username
        if args.name:
            entry["name"] = args.name
        labels[args.user_id] = entry
        _save_config(cfg)
        display = f"@{args.username}" if args.username else args.user_id
        print(f"  {display} → {args.label_value}")
    elif args.user_id in labels:
        del labels[args.user_id]
        _save_config(cfg)
        print(f"  Removed label for {args.user_id}")
    else:
        print(f"  No label set for {args.user_id}")


def _label_sort_key(item):
    entry = item[1]
    return str(entry.get("label", "")) if isinstance(entry, dict) else str(entry)


def list_config(argv):
    p = argparse.ArgumentParser(
        prog="discord notify list",
        description="Show core subscriptions and local Discord sender labels.",
    )
    p.parse_args(argv)

    source = notification_source()
    subscriptions = list_external_notification_subscriptions(
        tool_name=TOOL_NAME, source_id=source["id"]
    )
    labels = get_labels()

    print(f"  Source: {source['label']} ({source['id']})")
    print("  Subscriptions:")
    if subscriptions:
        for subscription in sorted(
            (item for item in subscriptions if isinstance(item, dict)),
            key=lambda item: (str(item.get("convId") or ""), str(item.get("id") or "")),
        ):
            conv_id = subscription.get("convId") or "?"
            delivery = subscription.get("delivery") or "?"
            subscription_id = subscription.get("id") or subscription.get("subscriptionId")
            suffix = f" [{subscription_id}]" if subscription_id else ""
            print(f"    • {conv_id} ({delivery}){suffix}")
    else:
        print("    (none)")

    print("  Sender labels (local):")
    if labels:
        for user_id, entry in sorted(labels.items(), key=_label_sort_key):
            if isinstance(entry, dict):
                lbl = entry.get("label", "?")
                uname = entry.get("username", "")
                name = entry.get("name", "")
                display = f"@{uname}" if uname else user_id
                extra = f" ({name})" if name and name != uname else ""
                print(f"    {display}{extra} [{user_id}] → {lbl}")
            else:
                print(f"    {user_id} → {entry}")
    else:
        print("    (none)")


def start(argv):
    p = argparse.ArgumentParser(
        prog="discord notify start", description="Start the notification listener."
    )
    p.parse_args(argv)

    alias = selected_alias()
    if alias == supervised_notify_alias():
        status = manage_external_tool_daemon(TOOL_NAME, "start")
        print(f"  {status.get('message', 'Requested start for supervised Discord daemon')}")
    else:
        from src.listening import _start_notify_listener
        _start_notify_listener()


def stop(argv):
    p = argparse.ArgumentParser(
        prog="discord notify stop", description="Stop the notification listener."
    )
    p.parse_args(argv)

    alias = selected_alias()
    if alias == supervised_notify_alias():
        status = manage_external_tool_daemon(TOOL_NAME, "stop")
        print(f"  {status.get('message', 'Requested stop for supervised Discord daemon')}")
    else:
        pids = _collect_notify_pids()
        stopped, alive = _stop_notify_pids(pids)
        if alive:
            raise RuntimeError(
                f"Failed to stop notify listener PID(s): {', '.join(map(str, alive))}"
            )
        print(f"  Stopped {len(stopped)} notification listener(s)")


def _run_daemon_mode(argv):
    if len(argv) > 1:
        raise SystemExit("usage: python -m src.notify __daemon__ [log_file]")
    log_file = argv[0] if argv else str(LISTENER_DIR / "__notify__.log")
    gateway_script = PROJECT_DIR / "src" / "gateway.py"

    # Complete the registry migration before starting a publisher.  On any IPC
    # failure exocortexd's supervisor retries this process, while notify.json
    # remains an authoritative migration marker.
    _migrate_relay_targets()

    while True:
        existing = [pid for pid in _find_notify_gateway_pids() if pid != os.getpid()]
        if existing:
            _stopped, alive = _stop_notify_pids(existing)
            if alive:
                _write_pid_hint(alive[0])
                time.sleep(5)
                continue

        os.execv(
            sys.executable,
            [sys.executable, str(gateway_script), "__notify__", str(log_file)],
        )


# ─── Dispatch ─────────────────────────────────────────────────────────────────

_COMMANDS = {
    "subscribe": subscribe,
    "unsubscribe": unsubscribe,
    "add": add,
    "remove": remove,
    "label": label,
    "list": list_config,
    "start": start,
    "stop": stop,
}


def _print_help():
    print("usage: discord notify <subscribe|unsubscribe|list|label|start|stop> [args]")
    print()
    print("  subscribe CONV_ID [--delivery wake|inbox]  Subscribe via Exocortex core")
    print("  unsubscribe CONV_ID | --subscription-id ID  Remove the account subscription")
    print("  list                                      Show subscriptions and local labels")
    print("  label USER_ID [LABEL]                     Manage local sender labels")
    print("  start | stop                              Manage the notification gateway")
    print("  add | remove                              Aliases for subscribe/unsubscribe")


def dispatch(cmd, argv):
    if not argv:
        list_config([])
        return

    subcmd = argv[0]
    if subcmd in {"-h", "--help", "help"}:
        _print_help()
        return
    fn = _COMMANDS.get(subcmd)
    if fn is None:
        print(f"  Unknown notify subcommand: {subcmd}")
        print(f"  Available: {', '.join(_COMMANDS.keys())}")
        raise SystemExit(1)
    fn(argv[1:])


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "__daemon__":
        _run_daemon_mode(argv[1:])
    else:
        dispatch("notify", argv)


if __name__ == "__main__":
    main()
