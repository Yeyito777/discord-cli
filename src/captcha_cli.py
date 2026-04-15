"""CLI entrypoints for discord captcha browser setup/status/solve."""

import argparse
import json

from src.captcha import (
    browser_status,
    setup_accessibility_browser,
)
from src.captcha_output import print_captcha_challenge
from src.webbroker import solve_captcha as broker_solve_captcha


def setup(argv):
    p = argparse.ArgumentParser(
        prog="discord captcha setup",
        description="Open the persistent Chromium captcha profile for hCaptcha accessibility login.",
    )
    p.parse_args(argv)
    setup_accessibility_browser()
    print("Captcha browser setup complete.")


def status(argv):
    p = argparse.ArgumentParser(
        prog="discord captcha status",
        description="Show stored hCaptcha browser-profile cookie state.",
    )
    p.parse_args(argv)
    print(json.dumps(browser_status(), indent=2))


def solve(argv):
    p = argparse.ArgumentParser(
        prog="discord captcha solve",
        description="Submit an AI-provided answer for the current pending browser-native captcha challenge.",
    )
    p.add_argument("answer", help="Answer text to submit")
    p.add_argument("--id", dest="challenge_id", help="Specific challenge ID to solve")
    p.add_argument("--action-id", help="Specific action ID to solve for")
    args = p.parse_args(argv)
    result = broker_solve_captcha(
        args.answer,
        challenge_id=args.challenge_id,
        action_id=args.action_id,
    )
    if result.get("status") == "captcha_required":
        print_captcha_challenge(result)
        raise SystemExit(10)
    print(json.dumps(result, indent=2, ensure_ascii=False))


_COMMANDS = {
    "setup": setup,
    "status": status,
    "solve": solve,
}


def dispatch(cmd, argv):
    fn = _COMMANDS.get(cmd)
    if fn is None:
        raise SystemExit(
            f"Unknown captcha command: {cmd}. Available: {', '.join(sorted(_COMMANDS))}"
        )
    fn(argv)
