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


# ─── Source and config management ─────────────────────────────────────────────


def notification_source(account=None):
    """Return the generic notification source metadata for one Discord account."""
    account = account or selected_account()
    alias = account.get("alias") or selected_alias(required=False) or "legacy"
    return {
        "id": f"account:{alias}:notifications",
        "label": f"{account_label(account)} · DMs and @mentions",
    }


def channel_notification_source(channel_id, account=None):
    """A channel source never includes the account-wide DM/mention route."""
    account = account or selected_account()
    alias = account.get("alias") or selected_alias(required=False) or "legacy"
    return {
        "id": f"account:{alias}:channel:{channel_id}:messages",
        "label": f"{account_label(account)} · channel {channel_id} · new posts",
        "description": "New incoming posts in this exact server channel; no edits, history, threads, or self-authored posts.",
    }


def _channel_options(parser):
    parser.add_argument("--channel", help="Exact server channel ID or name (names require --guild)")
    parser.add_argument("--guild", "-g", help="Exact server name or ID")


def _resolve_notification_channel(target, guild=None, *, validate=False):
    """Do not use the CLI's fuzzy resolver for durable subscriptions."""
    from src import api

    def exact(items, value, kind):
        matches = [item for item in items if item.get("name") == value]
        if len(matches) != 1:
            raise RuntimeError(f"Expected one exact {kind} named {value!r}; use its ID.")
        return matches[0]

    guild_id = None
    if guild:
        guild_id = guild if re.fullmatch(r"\d{17,20}", guild) else exact(api.get_guilds(), guild, "server")["id"]
    if re.fullmatch(r"\d{17,20}", target):
        if not validate and not guild:
            return target  # removal/listing must work after access is lost
        channel = api.get_channel(target)
    else:
        if not guild_id:
            raise RuntimeError("Channel names require --guild; alternatively use a channel ID.")
        channel = exact(api.get_guild_channels(guild_id), target.removeprefix("#"), "channel")
    if not channel.get("guild_id") or channel.get("type") not in (0, 5, 10, 11, 12):
        raise RuntimeError("Select a server text/announcement channel or an exact thread ID.")
    if guild_id and channel.get("guild_id") != guild_id:
        raise RuntimeError("Channel does not belong to the specified server.")
    return str(channel["id"])


def _source_for_args(args, parser, *, validate=False):
    if args.guild and not args.channel:
        parser.error("--guild requires --channel")
    if args.channel:
        return channel_notification_source(
            _resolve_notification_channel(args.channel, args.guild, validate=validate)
        )
    return notification_source()


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
    _channel_options(p)
    args = p.parse_args(argv)

    source = _source_for_args(args, p, validate=True)
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
    # The supervised daemon owns every subscribed Gateway; nudge it now rather
    # than waiting for its next poll.
    from src.channel_notifications import ensure_daemon, start_listener
    try:
        if args.channel:
            start_listener(source["id"].split(":")[3])
        else:
            ensure_daemon()
    except Exception as exc:
        print(f"  Listener: {exc}")


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
    _channel_options(p)
    args = p.parse_args(argv)
    if bool(args.conv_id) == bool(args.subscription_id):
        p.error("specify exactly one conversation ID or --subscription-id")

    source = _source_for_args(args, p)
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
    from src.supervisor import request_reconcile
    request_reconcile()  # the last unsubscribe stops a channel Gateway


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
    _channel_options(p)
    args = p.parse_args(argv)

    source = _source_for_args(args, p)
    subscriptions = list_external_notification_subscriptions(
        tool_name=TOOL_NAME, source_id=source["id"]
    )
    labels = get_labels()

    print(f"  Source: {source['label']} ({source['id']})")
    if args.channel:
        from src.channel_notifications import daemon_status, listener_status
        print(f"  Listener: {listener_status(source['id'].split(':')[3])}, {daemon_status()}")
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


def _enabled_subscriptions(source):
    return [
        subscription
        for subscription in list_external_notification_subscriptions(
            tool_name=TOOL_NAME, source_id=source["id"]
        )
        if isinstance(subscription, dict) and subscription.get("enabled") is not False
    ]


def start(argv):
    p = argparse.ArgumentParser(
        prog="discord notify start", description="Start the notification listener."
    )
    _channel_options(p)
    args = p.parse_args(argv)
    source = _source_for_args(args, p, validate=True)
    if args.channel:
        if not _enabled_subscriptions(source):
            raise RuntimeError(
                f"No enabled subscription for {source['id']}. Channel listeners follow "
                "subscriptions: run 'discord notify subscribe CONV --channel ...' instead."
            )
        from src.channel_notifications import start_listener
        start_listener(source["id"].split(":")[3])
        return

    alias = selected_alias()
    if alias == supervised_notify_alias():
        status = manage_external_tool_daemon(TOOL_NAME, "start")
        print(f"  {status.get('message', 'Requested start for supervised Discord daemon')}")
    elif _enabled_subscriptions(source):
        from src.channel_notifications import ensure_daemon
        ensure_daemon()
        print("  The Discord daemon runs this account's gateway while it has subscriptions")
    else:
        from src.listening import _start_notify_listener
        _start_notify_listener()


def stop(argv):
    p = argparse.ArgumentParser(
        prog="discord notify stop", description="Stop the notification listener."
    )
    _channel_options(p)
    args = p.parse_args(argv)
    source = _source_for_args(args, p)
    if args.channel:
        remaining = _enabled_subscriptions(source)
        if remaining:
            raise RuntimeError(
                f"{len(remaining)} subscription(s) keep this channel listener running; "
                "unsubscribe them to stop it."
            )
        from src.channel_notifications import stop_listener
        stop_listener(source["id"].split(":")[3])
        return

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
    if argv:
        raise SystemExit("usage: python -m src.notify __daemon__")

    # Complete the registry migration before starting a publisher.  On any IPC
    # failure exocortexd's supervisor retries this process, while notify.json
    # remains an authoritative migration marker.
    _migrate_relay_targets()

    from src.accounts import LEGACY_CREDENTIALS_FILE
    from src.supervisor import Supervisor

    alias = selected_alias(required=False)
    if alias is None and LEGACY_CREDENTIALS_FILE.exists():
        alias = "legacy"
    Supervisor(alias).run()


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
    print("  start | stop                              Manage the notification daemon")
    print("  add | remove                              Aliases for subscribe/unsubscribe")
    print("  subscribe/unsubscribe/list/start/stop: --channel ID|NAME [--guild ID|NAME]")
    print("  No --channel: account-wide DMs/@mentions. The Discord daemon runs a channel")
    print("  listener while it has an enabled subscription; start only waits for it.")


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
