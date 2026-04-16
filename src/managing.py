"""Managing subcommands — join, leave, typing, read."""

import argparse

from src import api
from src.captcha_output import print_captcha_challenge
from src.resolve import resolve_guild
from src.invite import join_server
from src.webbroker import join_invite as browser_join_invite


def join(argv):
    p = argparse.ArgumentParser(
        prog="discord join",
        description=(
            "Join a server via invite link. The browser-native path is used by "
            "default because invite joins commonly trigger hCaptcha. If Discord "
            "surfaces a visible text challenge, the command prints a prompt for "
            "`discord captcha solve`. Use --raw to force the legacy non-browser "
            "path."
        ),
        epilog=(
            "examples:\n"
            "  discord join anthropic\n"
            "  discord join anthropic --seed-accessibility\n"
            "  discord join discord.gg/example --raw"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("invite", help="Invite link or code (e.g. discord.gg/abc123 or abc123)")
    p.add_argument("--raw", action="store_true", help="Use the legacy non-browser join path")
    p.add_argument("--seed-accessibility", action="store_true", help="Seed hCaptcha accessibility cookies from the best available browser source into the dedicated browser profile first")
    args = p.parse_args(argv)

    if args.raw:
        result = join_server(args.invite)
        name = result.get("guild", {}).get("name", "Unknown")
        guild_id = result.get("guild", {}).get("id", "")
        print(f"Joined {name}. Guild ID: {guild_id}")
        return

    result = browser_join_invite(args.invite, seed_accessibility=args.seed_accessibility)
    if result.get("status") == "captcha_required":
        print_captcha_challenge(result)
        raise SystemExit(10)
    status = result.get("status", "joined")
    invite = result.get("invite", "")
    captcha = result.get("captcha", False)
    url = result.get("url", "")
    guild_id = result.get("guild_id", "")
    guild_name = result.get("guild_name", "")
    print(
        f"{status.capitalize()} invite {invite}. captcha={str(captcha).lower()} "
        f"url={url} guild_id={guild_id} guild_name={guild_name}"
    )


def leave(argv):
    p = argparse.ArgumentParser(prog="discord leave", description="Leave a server.")
    p.add_argument("server", help="Server name or ID")
    args = p.parse_args(argv)

    g = resolve_guild(args.server)
    api.leave_guild(g["id"])
    print(f"Left {g.get('name', g['id'])}.")


def typing(argv):
    p = argparse.ArgumentParser(prog="discord typing", description="Trigger typing indicator.")
    p.add_argument("channel", help="Channel ID")
    args = p.parse_args(argv)

    api.trigger_typing(args.channel)
    print("Typing indicator sent.")


def read(argv):
    p = argparse.ArgumentParser(prog="discord read", description="Mark channel as read.")
    p.add_argument("channel", help="Channel ID")
    p.add_argument("message", nargs="?", help="Message ID to mark read up to (default: latest)")
    args = p.parse_args(argv)

    channel_id = args.channel
    message_id = args.message
    if not message_id:
        msgs = api.get_messages(channel_id, limit=1)
        if msgs:
            message_id = msgs[0]["id"]
        else:
            print("  No messages to mark as read.")
            return

    api.ack_message(channel_id, message_id)
    print("Marked as read.")


_COMMANDS = {
    "join": join,
    "leave": leave,
    "typing": typing,
    "read": read,
}


def dispatch(cmd, argv):
    """Dispatch managing subcommands."""
    fn = _COMMANDS.get(cmd)
    if fn is None:
        raise RuntimeError(f"Unknown managing command: {cmd}")
    fn(argv)
