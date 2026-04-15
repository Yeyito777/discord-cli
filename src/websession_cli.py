"""CLI entrypoints for dedicated Discord web-session setup/status."""

from __future__ import annotations

import argparse
import json

from src.auth import get_token
from src.captcha_output import print_captcha_challenge
from src.webbroker import (
    ensure_started as broker_ensure_started,
    join_invite as broker_join_invite,
    send_dm as broker_send_dm,
    status as broker_status,
    stop as broker_stop,
)
from src.webprofile import (
    WEB_PROFILE_DIR,
    DiscordWebError,
    ensure_logged_in,
    launch_context,
    open_app,
    seed_hcaptcha_cookies_from_captcha_profile,
)
from src.websession import (
    join_invite_with_captcha,
    send_dm_with_captcha,
)


def _profile_status() -> dict:
    default_dir = WEB_PROFILE_DIR / "Default"
    local_storage = default_dir / "Local Storage" / "leveldb"
    cookies = default_dir / "Cookies"
    network_cookies = default_dir / "Network" / "Cookies"
    return {
        "profile_dir": str(WEB_PROFILE_DIR),
        "exists": WEB_PROFILE_DIR.exists(),
        "has_default_profile": default_dir.exists(),
        "has_local_storage_leveldb": local_storage.exists(),
        "has_cookies_db": cookies.exists() or network_cookies.exists(),
        "broker": broker_status(),
    }


def setup(argv):
    p = argparse.ArgumentParser(
        prog="discord web setup",
        description="Open the dedicated Chromium Discord-web profile for manual login/bootstrap.",
    )
    p.parse_args(argv)

    pw = None
    context = None
    try:
        pw, context = launch_context(headed=True)
        page = open_app(context)
        print()
        print("discord-cli web setup")
        print("─────────────────────")
        print()
        print("A dedicated Chromium profile for Discord web automation has been opened.")
        print("Log into Discord in that browser/profile if needed.")
        print("When you're done, come back here and press Enter.")
        print()
        input("Press Enter after the Discord web profile is ready... ")
        print()
        print(json.dumps(_profile_status(), indent=2))
    finally:
        try:
            if context is not None:
                context.close()
        finally:
            if pw is not None:
                pw.stop()


def status(argv):
    p = argparse.ArgumentParser(
        prog="discord web status",
        description="Show local filesystem status for the dedicated Discord web profile.",
    )
    p.parse_args(argv)
    print(json.dumps(_profile_status(), indent=2))


def _maybe_seed_accessibility(enabled: bool) -> None:
    if not enabled:
        return
    try:
        seed_hcaptcha_cookies_from_captcha_profile()
    except DiscordWebError as e:
        raise SystemExit(f"Error: {e}")


def _run_one_shot(headed: bool, action):
    pw = None
    context = None
    try:
        pw, context = launch_context(headed=headed)
        page = open_app(context)
        ensure_logged_in(page, get_token())
        return action(page)
    except DiscordWebError as e:
        raise SystemExit(f"Error: {e}")
    finally:
        try:
            if context is not None:
                context.close()
        finally:
            if pw is not None:
                pw.stop()


def seed_accessibility(argv):
    p = argparse.ArgumentParser(
        prog="discord web seed-accessibility",
        description="Copy hCaptcha cookie state from the captcha profile into the dedicated web profile.",
    )
    p.parse_args(argv)
    try:
        copied = seed_hcaptcha_cookies_from_captcha_profile()
    except DiscordWebError as e:
        raise SystemExit(f"Error: {e}")
    print(json.dumps({"copied_rows": copied, **_profile_status()}, indent=2))


def broker_start(argv):
    p = argparse.ArgumentParser(
        prog="discord web broker-start",
        description="Start/reuse the persistent Discord web browser broker.",
    )
    p.add_argument("--seed-accessibility", action="store_true", help="Copy hCaptcha cookies from captcha profile before starting the broker")
    p.add_argument("--headed", action="store_true", help="Start the broker with a visible browser window")
    args = p.parse_args(argv)
    try:
        print(json.dumps(broker_ensure_started(seed_accessibility=args.seed_accessibility, headed=args.headed), indent=2))
    except Exception as e:
        raise SystemExit(f"Error: {e}")


def broker_status_cmd(argv):
    p = argparse.ArgumentParser(
        prog="discord web broker-status",
        description="Show persistent Discord web broker status.",
    )
    p.parse_args(argv)
    print(json.dumps(broker_status(), indent=2))


def broker_stop_cmd(argv):
    p = argparse.ArgumentParser(
        prog="discord web broker-stop",
        description="Stop the persistent Discord web broker.",
    )
    p.parse_args(argv)
    print(json.dumps(broker_stop(), indent=2))


def send_dm(argv):
    p = argparse.ArgumentParser(
        prog="discord web send-dm",
        description="Send a DM through the dedicated Discord web profile, auto-solving visible text hCaptcha if needed.",
    )
    p.add_argument("channel_id", help="DM channel ID")
    p.add_argument("text", help="Message text")
    p.add_argument("--headed", action="store_true", help="Run with a visible browser window")
    p.add_argument("--seed-accessibility", action="store_true", help="Copy hCaptcha cookies from captcha profile before launching the web profile")
    p.add_argument("--one-shot", action="store_true", help="Do not use the persistent broker; launch a one-shot browser instead")
    args = p.parse_args(argv)

    if not args.one_shot:
        try:
            result = broker_send_dm(args.channel_id, args.text, seed_accessibility=args.seed_accessibility)
        except Exception as e:
            raise SystemExit(f"Error: {e}")
        if result.get("status") == "captcha_required":
            print_captcha_challenge(result)
            raise SystemExit(10)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    _maybe_seed_accessibility(args.seed_accessibility)
    result = _run_one_shot(args.headed, lambda page: send_dm_with_captcha(page, args.channel_id, args.text))
    if result.get("status") == "captcha_required":
        print_captcha_challenge(result)
        raise SystemExit(10)
    print(json.dumps(result, indent=2, ensure_ascii=False))


def join_invite(argv):
    p = argparse.ArgumentParser(
        prog="discord web join-invite",
        description="Join a server through the dedicated Discord web profile, auto-solving visible text hCaptcha if needed.",
    )
    p.add_argument("invite", help="Invite URL or code")
    p.add_argument("--headed", action="store_true", help="Run with a visible browser window")
    p.add_argument("--seed-accessibility", action="store_true", help="Copy hCaptcha cookies from captcha profile before launching the web profile")
    p.add_argument("--one-shot", action="store_true", help="Do not use the persistent broker; launch a one-shot browser instead")
    args = p.parse_args(argv)

    if not args.one_shot:
        try:
            result = broker_join_invite(args.invite, seed_accessibility=args.seed_accessibility)
        except Exception as e:
            raise SystemExit(f"Error: {e}")
        if result.get("status") == "captcha_required":
            print_captcha_challenge(result)
            raise SystemExit(10)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    _maybe_seed_accessibility(args.seed_accessibility)
    result = _run_one_shot(args.headed, lambda page: join_invite_with_captcha(page, args.invite))
    if result.get("status") == "captcha_required":
        print_captcha_challenge(result)
        raise SystemExit(10)
    print(json.dumps(result, indent=2, ensure_ascii=False))


_COMMANDS = {
    "setup": setup,
    "status": status,
    "seed-accessibility": seed_accessibility,
    "broker-start": broker_start,
    "broker-status": broker_status_cmd,
    "broker-stop": broker_stop_cmd,
    "send-dm": send_dm,
    "join-invite": join_invite,
}


def dispatch(cmd, argv):
    fn = _COMMANDS.get(cmd)
    if fn is None:
        raise RuntimeError(f"Unknown web-session command: {cmd}")
    fn(argv)
