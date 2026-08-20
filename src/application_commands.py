"""Discover and invoke Discord application (slash) commands."""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import tempfile
import threading
import time

from src import api
from src.accounts import audit_event
from src.gateway import GatewayListener
from src.resolve import resolve_channel, resolve_guild


CHAT_INPUT_COMMAND = 1
COMMAND_INTERACTION = 2
DISCORD_EPOCH_MS = 1_420_070_400_000
SNOWFLAKE_RE = re.compile(r"^\d{17,20}$")
MENTION_RE = re.compile(r"^<[@#](?:[!&])?(\d{17,20})>$")


def _normalized_name(value):
    return re.sub(r"[\s_-]+", "_", str(value or "").strip().lower())


def _command_index(guild_id):
    value = api.get(f"/guilds/{guild_id}/application-command-index")
    if not isinstance(value, dict):
        raise RuntimeError("Discord returned an invalid application-command index.")
    applications = {
        str(item.get("id")): item
        for item in value.get("applications", [])
        if isinstance(item, dict) and item.get("id")
    }
    commands = [
        item
        for item in value.get("application_commands", [])
        if isinstance(item, dict) and item.get("type") == CHAT_INPUT_COMMAND
    ]
    return applications, commands


def _application_name(command, applications):
    application_id = str(command.get("application_id") or "")
    application = applications.get(application_id, {})
    return str(application.get("name") or f"App {application_id}")


def _find_application(app_query, applications, commands):
    query = str(app_query).strip()
    ids = {str(command.get("application_id") or "") for command in commands}
    if query in ids:
        return query
    normalized = _normalized_name(query)
    matches = {
        app_id
        for app_id in ids
        if _normalized_name((applications.get(app_id) or {}).get("name")) == normalized
    }
    if not matches:
        raise RuntimeError(
            f'Application "{app_query}" is not available in this server. '
            "Run 'discord commands <channel>' to list applications."
        )
    if len(matches) > 1:
        raise RuntimeError(
            f'Application name "{app_query}" is ambiguous; use its application ID.'
        )
    return next(iter(matches))


def _find_command(application_id, command_name, commands):
    matches = [
        command
        for command in commands
        if str(command.get("application_id") or "") == application_id
        and str(command.get("name") or "").lower() == str(command_name).lower()
    ]
    if not matches:
        raise RuntimeError(
            f'Command "{command_name}" is not available for that application.'
        )
    if len(matches) > 1:
        raise RuntimeError(
            f'Command name "{command_name}" is ambiguous; command index is invalid.'
        )
    return matches[0]


def _option_name(option):
    return str(option.get("name_default") or option.get("name") or "")


def _resolve_route(command, tokens):
    remaining = list(tokens)
    options = list(command.get("options") or [])
    path = []
    while any(isinstance(option, dict) and option.get("type") in {1, 2} for option in options):
        if not remaining or remaining[0].startswith("--"):
            choices = ", ".join(
                _option_name(option)
                for option in options
                if isinstance(option, dict) and option.get("type") in {1, 2}
            )
            raise RuntimeError(f"Choose a subcommand: {choices}")
        requested = remaining.pop(0)
        branch = next(
            (
                option
                for option in options
                if isinstance(option, dict)
                and option.get("type") in {1, 2}
                and _option_name(option).lower() == requested.lower()
            ),
            None,
        )
        if branch is None:
            raise RuntimeError(f'Unknown subcommand "{requested}".')
        path.append(branch)
        options = list(branch.get("options") or [])
    return path, options, remaining


def _parse_raw_options(tokens):
    values = {}
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if not token.startswith("--") or token == "--":
            raise RuntimeError(f'Unexpected command argument "{token}".')
        body = token[2:]
        if not body:
            raise RuntimeError("Application-command option name is empty.")
        if "=" in body:
            name, value = body.split("=", 1)
        else:
            name = body
            index += 1
            if index >= len(tokens):
                raise RuntimeError(f"--{name} requires a value.")
            value = tokens[index]
        key = name.lower()
        if key in values:
            raise RuntimeError(f"--{name} was provided more than once.")
        values[key] = value
        index += 1
    return values


def _choice_value(option, raw):
    choices = [choice for choice in option.get("choices", []) if isinstance(choice, dict)]
    if not choices:
        return None, False
    for choice in choices:
        names = {str(choice.get("name") or "").lower()}
        if choice.get("name_default") is not None:
            names.add(str(choice.get("name_default")).lower())
        if raw.lower() in names or raw == str(choice.get("value")):
            return choice.get("value"), True
    allowed = ", ".join(str(choice.get("name_default") or choice.get("name")) for choice in choices)
    raise RuntimeError(f"--{_option_name(option)} must be one of: {allowed}")


def _convert_value(option, raw):
    option_type = option.get("type")
    choice, matched = _choice_value(option, raw)
    if matched:
        return choice
    if option_type == 3:
        value = raw
        minimum = option.get("min_length")
        maximum = option.get("max_length")
        if isinstance(minimum, int) and len(value) < minimum:
            raise RuntimeError(f"--{_option_name(option)} must contain at least {minimum} characters.")
        if isinstance(maximum, int) and len(value) > maximum:
            raise RuntimeError(f"--{_option_name(option)} may contain at most {maximum} characters.")
        return value
    if option_type == 4:
        if not re.fullmatch(r"-?\d+", raw):
            raise RuntimeError(f"--{_option_name(option)} must be an integer.")
        value = int(raw)
    elif option_type == 5:
        lowered = raw.lower()
        if lowered in {"true", "yes", "on", "1"}:
            return True
        if lowered in {"false", "no", "off", "0"}:
            return False
        raise RuntimeError(f"--{_option_name(option)} must be true or false.")
    elif option_type in {6, 7, 8, 9}:
        match = MENTION_RE.fullmatch(raw)
        value = match.group(1) if match else raw
        if not SNOWFLAKE_RE.fullmatch(value):
            raise RuntimeError(f"--{_option_name(option)} must be a Discord mention or ID.")
        return value
    elif option_type == 10:
        try:
            value = float(raw)
        except ValueError as error:
            raise RuntimeError(f"--{_option_name(option)} must be a number.") from error
    elif option_type == 11:
        raise RuntimeError("Attachment application-command options are not supported yet.")
    else:
        raise RuntimeError(f"--{_option_name(option)} uses an unsupported Discord option type.")
    minimum = option.get("min_value")
    maximum = option.get("max_value")
    if isinstance(minimum, (int, float)) and value < minimum:
        raise RuntimeError(f"--{_option_name(option)} must be at least {minimum}.")
    if isinstance(maximum, (int, float)) and value > maximum:
        raise RuntimeError(f"--{_option_name(option)} may be at most {maximum}.")
    return value


def _interaction_options(command, tokens):
    path, leaf_options, remaining = _resolve_route(command, tokens)
    raw_values = _parse_raw_options(remaining)
    by_name = {
        _option_name(option).lower(): option
        for option in leaf_options
        if isinstance(option, dict) and option.get("type") not in {1, 2}
    }
    unknown = sorted(set(raw_values) - set(by_name))
    if unknown:
        raise RuntimeError(f"Unknown option --{unknown[0]}.")
    result = []
    for name, option in by_name.items():
        if name not in raw_values:
            if option.get("required") is True:
                raise RuntimeError(f"Missing required option --{_option_name(option)}.")
            continue
        result.append({
            "type": option.get("type"),
            "name": _option_name(option),
            "value": _convert_value(option, raw_values[name]),
        })
    for branch in reversed(path):
        wrapped = {"type": branch.get("type"), "name": _option_name(branch)}
        if result:
            wrapped["options"] = result
        result = [wrapped]
    return result


def _application_command_payload(command):
    payload = {
        "id": str(command.get("id")),
        "application_id": str(command.get("application_id")),
        "version": str(command.get("version")),
        "type": CHAT_INPUT_COMMAND,
        "name": command.get("name"),
        "description": command.get("description", ""),
    }
    for key in (
        "dm_permission", "nsfw", "name_localized", "name_localizations",
        "description_localized", "description_localizations",
        "default_member_permissions", "contexts", "integration_types",
        "guild_id", "options",
    ):
        if key in command:
            payload[key] = command[key]
    return payload


def _command_usage(command):
    options = [option for option in command.get("options", []) if isinstance(option, dict)]
    branches = [option for option in options if option.get("type") in {1, 2}]
    if branches:
        return "{" + "|".join(_option_name(option) for option in branches) + "}"
    parts = []
    for option in options:
        marker = f"--{_option_name(option)} VALUE"
        parts.append(marker if option.get("required") else f"[{marker}]")
    return " ".join(parts)


def _nonce(now_ms=None):
    now_ms = int(time.time() * 1000 if now_ms is None else now_ms)
    return str(max(0, now_ms - DISCORD_EPOCH_MS) << 22)


@contextlib.contextmanager
def _gateway_session(timeout=20):
    with tempfile.NamedTemporaryFile(prefix="discord-command-gateway-") as output:
        listener = GatewayListener("__command__", output.name)
        thread = threading.Thread(target=listener.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + timeout
        while not listener.session_id and time.monotonic() < deadline:
            if not thread.is_alive():
                break
            time.sleep(0.05)
        if not listener.session_id:
            listener.running = False
            raise RuntimeError("Discord Gateway did not provide a session for the command.")
        try:
            yield listener.session_id
        finally:
            listener.running = False
            thread.join(timeout=2)


def _guild_channel(channel_target, guild_target=None):
    guild_id = None
    if guild_target:
        guild_id = str(resolve_guild(guild_target)["id"])
    channel = resolve_channel(channel_target, guild_id)
    channel_id = str(channel["id"])
    guild_id = str(channel.get("guild_id") or guild_id or "")
    if not guild_id:
        raise RuntimeError("Application commands require a server channel, not a DM.")
    return guild_id, channel_id


def commands(argv):
    parser = argparse.ArgumentParser(
        prog="discord commands",
        description="List Discord application commands available in a server channel.",
    )
    parser.add_argument("channel", help="Channel name or ID")
    parser.add_argument("app", nargs="?", help="Application name or ID")
    parser.add_argument("-g", "--guild", help="Server name or ID when CHANNEL is a name")
    args = parser.parse_args(argv)
    guild_id, _ = _guild_channel(args.channel, args.guild)
    applications, command_index = _command_index(guild_id)
    if not args.app:
        counts = {}
        for command in command_index:
            app_id = str(command.get("application_id") or "")
            counts[app_id] = counts.get(app_id, 0) + 1
        for app_id in sorted(counts, key=lambda value: _application_name({"application_id": value}, applications).lower()):
            print(f"{_application_name({'application_id': app_id}, applications)} [{app_id}] — {counts[app_id]} commands")
        return
    application_id = _find_application(args.app, applications, command_index)
    app_commands = [
        command for command in command_index
        if str(command.get("application_id") or "") == application_id
    ]
    print(f"{_application_name({'application_id': application_id}, applications)} [{application_id}]")
    for command in sorted(app_commands, key=lambda value: str(value.get("name") or "")):
        usage = _command_usage(command)
        suffix = f" {usage}" if usage else ""
        print(f"  {command.get('name')}{suffix} — {command.get('description', '')}")


def command(argv):
    parser = argparse.ArgumentParser(
        prog="discord command",
        description="Invoke a Discord application command using its live server schema.",
        epilog=(
            'Example: discord command -a paramount CHANNEL "Better Rhythm" play '
            '--url https://www.youtube.com/watch?v=VIDEO'
        ),
        allow_abbrev=False,
    )
    parser.add_argument("channel", help="Channel name or ID")
    parser.add_argument("app", help="Application name or ID")
    parser.add_argument("name", help="Command name")
    parser.add_argument("-g", "--guild", help="Server name or ID when CHANNEL is a name")
    args, command_args = parser.parse_known_args(argv)
    guild_id, channel_id = _guild_channel(args.channel, args.guild)
    applications, command_index = _command_index(guild_id)
    application_id = _find_application(args.app, applications, command_index)
    selected = _find_command(application_id, args.name, command_index)
    options = _interaction_options(selected, command_args)
    nonce = _nonce()
    payload = {
        "type": COMMAND_INTERACTION,
        "application_id": application_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "data": {
            "application_command": _application_command_payload(selected),
            "attachments": [],
            "id": str(selected.get("id")),
            "name": selected.get("name"),
            "options": options,
            "type": CHAT_INPUT_COMMAND,
            "version": str(selected.get("version")),
            "guild_id": guild_id,
        },
        "nonce": nonce,
        "analytics_location": "slash_ui",
    }
    with _gateway_session() as session_id:
        payload["session_id"] = session_id
        api.post_once("/interactions", body=payload)
    audit_event(
        "application-command",
        target=f"{guild_id}/{channel_id}/{application_id}/{selected.get('name')}",
        result_id=nonce,
    )
    print(
        f"Invoked /{selected.get('name')} from "
        f"{_application_name(selected, applications)}. Nonce: {nonce}"
    )


def dispatch(cmd, argv):
    try:
        if cmd == "commands":
            return commands(argv)
        if cmd == "command":
            return command(argv)
        raise RuntimeError(f"Unknown application-command action: {cmd}")
    except RuntimeError as error:
        raise SystemExit(f"discord {cmd}: {error}") from error
