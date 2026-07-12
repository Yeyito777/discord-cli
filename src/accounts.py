"""Named Discord account profiles and per-account state.

The CLI is process-scoped: ``DISCORD_ACCOUNT`` selects an account for one
invocation and is inherited by detached workers.  There is deliberately no
mutable global "currently active" account for writes.
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_ROOT = Path(os.environ.get("DISCORD_CONFIG_DIR", "").strip() or (PROJECT_ROOT / "config"))
ACCOUNTS_ROOT = CONFIG_ROOT / "accounts"
REGISTRY_FILE = CONFIG_ROOT / "accounts.json"
LEGACY_CREDENTIALS_FILE = CONFIG_ROOT / "credentials.json"
ACCOUNT_ENV = "DISCORD_ACCOUNT"
ACCOUNT_EXPLICIT_ENV = "DISCORD_ACCOUNT_EXPLICIT"
_ALIAS_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


class AccountError(RuntimeError):
    pass


def _empty_registry():
    return {"version": 1, "default_account": None, "notify_account": None, "accounts": {}}


def load_registry():
    if not REGISTRY_FILE.exists():
        return _empty_registry()
    try:
        data = json.loads(REGISTRY_FILE.read_text())
    except (OSError, json.JSONDecodeError) as e:
        raise AccountError(f"Failed to read account registry {REGISTRY_FILE}: {e}") from e
    if not isinstance(data, dict) or not isinstance(data.get("accounts"), dict):
        raise AccountError(f"Invalid account registry: {REGISTRY_FILE}")
    data.setdefault("version", 1)
    data.setdefault("default_account", None)
    data.setdefault("notify_account", data.get("default_account"))
    for alias, meta in data["accounts"].items():
        if normalize_alias(alias) != alias:
            raise AccountError(f"Invalid account alias in registry: {alias!r}")
        if not isinstance(meta, dict) or not str(meta.get("user_id") or "").isdigit():
            raise AccountError(f"Invalid metadata for Discord account '{alias}'.")
        if meta.get("owner") not in {"assistant", "user", "other"}:
            raise AccountError(f"Invalid owner for Discord account '{alias}'.")
        if meta.get("access") not in {"read-only", "full"}:
            raise AccountError(f"Invalid access for Discord account '{alias}'.")
    default = data.get("default_account")
    if default is not None and default not in data["accounts"]:
        raise AccountError(f"Default Discord account '{default}' is not configured.")
    notify = data.get("notify_account")
    if notify is not None and notify not in data["accounts"]:
        raise AccountError(f"Supervised notification account '{notify}' is not configured.")
    return data


def _write_secure_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.chmod(tmp, 0o600)
    tmp.replace(path)
    try:
        os.chmod(path.parent, 0o700)
        os.chmod(path, 0o600)
    except OSError:
        pass


def save_registry(registry) -> None:
    _write_secure_json(REGISTRY_FILE, registry)


def normalize_alias(alias: str) -> str:
    value = str(alias or "").strip().lower()
    if not _ALIAS_RE.fullmatch(value):
        raise AccountError(
            "Account aliases must start with a letter or number and contain only "
            "lowercase letters, numbers, '.', '_', or '-'."
        )
    return value


def account_count() -> int:
    return len(load_registry()["accounts"])


def selected_alias(*, required: bool = True) -> str | None:
    registry = load_registry()
    requested = os.environ.get(ACCOUNT_ENV, "").strip()
    if requested:
        alias = normalize_alias(requested)
        if alias not in registry["accounts"]:
            choices = ", ".join(sorted(registry["accounts"])) or "(none)"
            raise AccountError(f"Discord account '{alias}' is not configured. Available: {choices}")
        return alias

    default = registry.get("default_account")
    if default and default in registry["accounts"]:
        return default
    if len(registry["accounts"]) == 1:
        return next(iter(registry["accounts"]))
    if not registry["accounts"] and LEGACY_CREDENTIALS_FILE.exists():
        return None  # legacy single-account installation
    if required:
        raise AccountError("No Discord account is configured. Run 'discord account add <alias>'.")
    return None


def selected_account(*, required: bool = True):
    alias = selected_alias(required=required)
    if alias is None:
        if LEGACY_CREDENTIALS_FILE.exists():
            return {
                "alias": "legacy",
                "owner": "unknown",
                "access": "full",
                "legacy": True,
            }
        return None
    account = dict(load_registry()["accounts"][alias])
    account["alias"] = alias
    return account


def default_alias() -> str | None:
    registry = load_registry()
    value = registry.get("default_account")
    return value if value in registry["accounts"] else None


def supervised_notify_alias() -> str | None:
    registry = load_registry()
    value = registry.get("notify_account")
    return value if value in registry["accounts"] else None


def account_root(alias: str | None = None) -> Path:
    alias = normalize_alias(alias) if alias else selected_alias(required=False)
    if alias is None:
        return CONFIG_ROOT
    return ACCOUNTS_ROOT / alias


def account_config_path(name: str, alias: str | None = None) -> Path:
    return account_root(alias) / name


def listener_dir(alias: str | None = None) -> Path:
    if alias is None:
        alias = selected_alias(required=False)
    safe = "legacy" if alias is None else normalize_alias(alias)
    runtime_home = os.environ.get("XDG_RUNTIME_DIR", "").strip()
    if runtime_home:
        base = Path(runtime_home) / "discord-cli"
    else:
        base = Path("/tmp") / f"discord-cli-{os.getuid()}"
    config_id = hashlib.sha256(str(CONFIG_ROOT.resolve()).encode()).hexdigest()[:12]
    path = base / config_id / safe
    for directory in (base, base / config_id, path):
        if directory.is_symlink():
            raise AccountError(f"Refusing symlinked Discord runtime directory: {directory}")
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        stat = directory.stat()
        if stat.st_uid != os.getuid():
            raise AccountError(f"Discord runtime directory is owned by another user: {directory}")
        os.chmod(directory, 0o700)
    return path


def credentials_file(alias: str | None = None) -> Path:
    if alias is None:
        alias = selected_alias()
    return LEGACY_CREDENTIALS_FILE if alias is None else account_root(alias) / "credentials.json"


def read_token(alias: str | None = None) -> str:
    path = credentials_file(alias)
    if not path.exists():
        selected = alias or selected_alias(required=False) or "legacy"
        raise AccountError(
            f"Discord account '{selected}' has no credentials. "
            f"Run 'discord -a {selected} login <token>' to configure it."
        )
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        raise AccountError(f"Failed to read credentials file {path}: {e}") from e
    token = str(data.get("token") or "").strip()
    if not token:
        raise AccountError(f"Credentials file is missing a token: {path}")
    return token


def write_token(token: str, alias: str | None = None) -> Path:
    path = credentials_file(alias)
    _write_secure_json(path, {"token": token})
    return path


def verify_and_refresh_selected_identity(user) -> None:
    """Ensure a replacement token belongs to the selected account and refresh labels."""
    account = selected_account()
    if account.get("legacy"):
        return
    user_id = str(user.get("id") or "")
    if user_id != str(account.get("user_id") or ""):
        raise AccountError(
            f"That token belongs to Discord user {user_id}, not account "
            f"'{account['alias']}' ({account.get('user_id')}). Add it as a separate account instead."
        )
    registry = load_registry()
    meta = registry["accounts"][account["alias"]]
    meta["username"] = str(user.get("username") or user_id)
    meta["global_name"] = str(user.get("global_name") or "")
    save_registry(registry)


def audit_event(action: str, *, target: str | None = None, result_id: str | None = None) -> None:
    """Append a content-free mutation receipt to the selected account's audit log."""
    account = selected_account()
    entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "account": account["alias"],
        "user_id": account.get("user_id"),
        "action": str(action),
    }
    if target:
        entry["target"] = str(target)
    if result_id:
        entry["result_id"] = str(result_id)
    path = account_config_path("audit.jsonl")
    try:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (json.dumps(entry, sort_keys=True) + "\n").encode())
        finally:
            os.close(fd)
    except OSError:
        # The Discord action already succeeded; an audit-disk failure must not
        # make callers retry and accidentally duplicate the mutation.
        return


def account_label(account=None) -> str:
    account = account or selected_account()
    alias = account.get("alias", "?")
    username = account.get("username") or account.get("user_id") or "unknown"
    global_name = str(account.get("global_name") or "").strip()
    identity = f"{global_name} (@{username})" if global_name and global_name != username else f"@{username}"
    owner = account.get("owner", "unknown")
    return f"{alias} — {identity} [{owner}]"


def _is_mutating(cmd: str, argv: list[str]) -> bool:
    if cmd == "dm":
        # argparse accepts unambiguous long-option abbreviations by default;
        # --sen is therefore also --send and must not bypass account policy.
        return any(value == "--send" or value.startswith("--send=") or value.startswith("--sen") for value in argv)
    if cmd == "notify":
        return bool(argv and argv[0] != "list")
    return cmd in {
        "send", "reply", "edit", "delete", "del", "react", "unreact",
        "call", "voice", "join-call", "listen", "unlisten", "join", "leave",
        "typing", "read",
    }


def preflight(cmd: str, argv: list[str]) -> None:
    """Validate account selection/access and print an identity banner."""
    registry = load_registry()
    if any(value in {"-h", "--help"} for value in argv):
        return
    if cmd in {"account", "accounts", "help", "--help", "-h"}:
        return
    if not registry["accounts"] and not LEGACY_CREDENTIALS_FILE.exists():
        return

    auth_change = cmd in {"login", "setup", "auth", "logout"}
    mutating = _is_mutating(cmd, argv)
    explicit = os.environ.get(ACCOUNT_EXPLICIT_ENV) == "1"
    if (mutating or auth_change) and len(registry["accounts"]) > 1 and not explicit:
        choices = "\n".join(
            f"  {account_label({**meta, 'alias': alias})}"
            for alias, meta in sorted(registry["accounts"].items())
        )
        raise AccountError(
            "Multiple Discord accounts are configured; this command requires "
            "'-a/--account <alias>'.\n" + choices
        )

    account = selected_account()
    if mutating and account.get("access", "read-only") != "full":
        raise AccountError(
            f"Discord account '{account['alias']}' is read-only. "
            f"Run 'discord account access {account['alias']} full' to enable writes."
        )
    if len(registry["accounts"]) > 1 or explicit:
        print(f"Discord account: {account_label(account)}", file=sys.stderr)


def _metadata_from_user(user, *, owner: str, access: str):
    return {
        "user_id": str(user["id"]),
        "username": str(user.get("username") or user["id"]),
        "global_name": str(user.get("global_name") or ""),
        "owner": owner,
        "access": access,
    }


def _validate_owner_access(owner: str, access: str) -> None:
    if owner not in {"assistant", "user", "other"}:
        raise AccountError("Owner must be one of: assistant, user, other")
    if access not in {"read-only", "full"}:
        raise AccountError("Access must be one of: read-only, full")


def add_account(argv) -> None:
    p = argparse.ArgumentParser(prog="discord account add", description="Add a named Discord account.")
    p.add_argument("alias")
    p.add_argument("--owner", choices=("assistant", "user", "other"), required=True)
    p.add_argument("--access", choices=("read-only", "full"), default="read-only")
    source = p.add_mutually_exclusive_group()
    source.add_argument("--token-stdin", action="store_true", help="Read the token from standard input")
    source.add_argument("--token-prompt", action="store_true", help="Read the token from a hidden prompt")
    p.add_argument("--default", action="store_true", help="Make this the default account for reads")
    args = p.parse_args(argv)
    alias = normalize_alias(args.alias)
    registry = load_registry()
    if alias in registry["accounts"]:
        raise AccountError(f"Discord account alias '{alias}' already exists.")

    if args.token_stdin:
        token = sys.stdin.read().strip().strip('"')
    else:
        token = getpass.getpass("Discord token: ").strip().strip('"')
    if not token:
        raise AccountError("A Discord token is required.")

    from src.auth import validate_token
    user = validate_token(token)
    registry["accounts"][alias] = _metadata_from_user(user, owner=args.owner, access=args.access)
    if args.default or not registry.get("default_account"):
        registry["default_account"] = alias
    if not registry.get("notify_account"):
        registry["notify_account"] = alias
    write_token(token, alias)
    save_registry(registry)
    print(f"Added Discord account: {account_label({**registry['accounts'][alias], 'alias': alias})}")
    print(f"Access: {args.access}")


def migrate_legacy(argv) -> None:
    p = argparse.ArgumentParser(prog="discord account migrate", description="Migrate the legacy login into a named account.")
    p.add_argument("alias")
    p.add_argument("--owner", choices=("assistant", "user", "other"), required=True)
    p.add_argument("--access", choices=("read-only", "full"), default="full")
    p.add_argument("--default", action="store_true")
    args = p.parse_args(argv)
    alias = normalize_alias(args.alias)
    registry = load_registry()
    if alias in registry["accounts"]:
        raise AccountError(f"Discord account alias '{alias}' already exists.")
    if not LEGACY_CREDENTIALS_FILE.exists():
        raise AccountError(f"No legacy credentials found at {LEGACY_CREDENTIALS_FILE}")

    data = json.loads(LEGACY_CREDENTIALS_FILE.read_text())
    token = str(data.get("token") or "").strip()
    from src.auth import validate_token
    user = validate_token(token)
    registry["accounts"][alias] = _metadata_from_user(user, owner=args.owner, access=args.access)
    if args.default or not registry.get("default_account"):
        registry["default_account"] = alias
    if not registry.get("notify_account"):
        registry["notify_account"] = alias

    target = account_root(alias)
    target.mkdir(parents=True, exist_ok=True, mode=0o700)
    legacy_notify = CONFIG_ROOT / "notify.json"
    if legacy_notify.exists() and not (target / "notify.json").exists():
        shutil.move(str(legacy_notify), str(target / "notify.json"))
    shutil.move(str(LEGACY_CREDENTIALS_FILE), str(target / "credentials.json"))
    save_registry(registry)
    print(f"Migrated Discord account: {account_label({**registry['accounts'][alias], 'alias': alias})}")


def list_accounts(argv) -> None:
    p = argparse.ArgumentParser(prog="discord accounts", description="List configured Discord accounts.")
    p.parse_args(argv)
    registry = load_registry()
    if not registry["accounts"]:
        if LEGACY_CREDENTIALS_FILE.exists():
            print("  legacy       unlabelled legacy login (run 'discord account migrate <alias>')")
        else:
            print("  No Discord accounts configured.")
        return
    default = registry.get("default_account")
    notify = registry.get("notify_account")
    for alias, meta in sorted(registry["accounts"].items()):
        roles = []
        if alias == default:
            roles.append("default-read")
        if alias == notify:
            roles.append("notifications")
        suffix = f"; {', '.join(roles)}" if roles else ""
        print(f"  {account_label({**meta, 'alias': alias})}; access={meta.get('access', 'read-only')}{suffix}")


def set_default(argv) -> None:
    p = argparse.ArgumentParser(prog="discord account default")
    p.add_argument("alias")
    args = p.parse_args(argv)
    alias = normalize_alias(args.alias)
    registry = load_registry()
    if alias not in registry["accounts"]:
        raise AccountError(f"Discord account '{alias}' is not configured.")
    registry["default_account"] = alias
    save_registry(registry)
    print(f"Default read account: {alias}")


def set_access(argv) -> None:
    p = argparse.ArgumentParser(prog="discord account access")
    p.add_argument("alias")
    p.add_argument("access", choices=("read-only", "full"))
    p.add_argument("--yes", action="store_true", help="Confirm enabling write access")
    args = p.parse_args(argv)
    alias = normalize_alias(args.alias)
    registry = load_registry()
    if alias not in registry["accounts"]:
        raise AccountError(f"Discord account '{alias}' is not configured.")
    if args.access == "full" and not args.yes:
        raise AccountError("Enabling full Discord write access requires --yes.")
    registry["accounts"][alias]["access"] = args.access
    save_registry(registry)
    print(f"Updated {alias}: access={args.access}")


def set_notify_account(argv) -> None:
    p = argparse.ArgumentParser(prog="discord account notify")
    p.add_argument("alias")
    args = p.parse_args(argv)
    alias = normalize_alias(args.alias)
    registry = load_registry()
    if alias not in registry["accounts"]:
        raise AccountError(f"Discord account '{alias}' is not configured.")
    registry["notify_account"] = alias
    save_registry(registry)
    print(f"Supervised notification account: {alias}")
    print("Restart the Discord notification daemon for this change to take effect.")


def remove_account(argv) -> None:
    p = argparse.ArgumentParser(prog="discord account remove")
    p.add_argument("alias")
    p.add_argument("--yes", action="store_true", help="Confirm removal of credentials and account-local state")
    args = p.parse_args(argv)
    alias = normalize_alias(args.alias)
    if not args.yes:
        raise AccountError("Account removal requires --yes.")
    registry = load_registry()
    if alias not in registry["accounts"]:
        raise AccountError(f"Discord account '{alias}' is not configured.")
    if registry.get("notify_account") == alias and len(registry["accounts"]) > 1:
        raise AccountError(
            f"Account '{alias}' owns the supervised notification daemon. "
            "Select another with 'discord account notify <alias>' before removal."
        )

    runtime = listener_dir(alias)
    state_dir = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state")) / "discord-cli" / "accounts" / alias
    cache_dir = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "discord-cli" / "accounts" / alias
    pid_files = list(runtime.glob("*.pid")) + list(state_dir.rglob("*.json"))
    live = []
    for path in pid_files:
        pid = None
        try:
            payload = json.loads(path.read_text()) if path.suffix == ".json" else path.read_text().strip()
            pid = int(payload.get("pid")) if isinstance(payload, dict) else int(payload)
            os.kill(pid, 0)
            live.append(pid)
        except (FileNotFoundError, ProcessLookupError, ValueError, TypeError, json.JSONDecodeError):
            continue
        except PermissionError as e:
            if pid is None:
                raise AccountError(f"Cannot inspect account runtime state {path}: {e}") from e
            live.append(pid)
    if live:
        raise AccountError(
            f"Account '{alias}' still has live process(es): {', '.join(map(str, sorted(set(live))))}. "
            "Stop its listeners and calls before removal."
        )

    for path in (ACCOUNTS_ROOT / alias, runtime, state_dir, cache_dir):
        if path.exists():
            shutil.rmtree(path)

    del registry["accounts"][alias]
    if registry.get("default_account") == alias:
        registry["default_account"] = next(iter(sorted(registry["accounts"])), None)
    if registry.get("notify_account") == alias:
        registry["notify_account"] = next(iter(sorted(registry["accounts"])), None)
    save_registry(registry)
    print(f"Removed Discord account: {alias}")


def dispatch(argv) -> None:
    if not argv or argv[0] in {"list", "ls"}:
        list_accounts(argv[1:] if argv else [])
        return
    subcmd, rest = argv[0], argv[1:]
    commands = {
        "add": add_account,
        "migrate": migrate_legacy,
        "default": set_default,
        "access": set_access,
        "notify": set_notify_account,
        "remove": remove_account,
    }
    if subcmd in {"-h", "--help", "help"}:
        print("usage: discord account <list|add|migrate|default|access|notify|remove> [args]")
        return
    fn = commands.get(subcmd)
    if fn is None:
        raise AccountError(f"Unknown account subcommand '{subcmd}'.")
    fn(rest)


def cli_preflight(cmd: str, argv: list[str]) -> None:
    try:
        preflight(cmd, argv)
    except AccountError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)


def cli_dispatch(argv: list[str]) -> None:
    try:
        dispatch(argv)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
