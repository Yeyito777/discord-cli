"""Shared stdout formatting for browser-native captcha prompts."""


def print_captcha_challenge(result: dict) -> None:
    print(f"solve captcha challenge: {result['prompt']}")
    if result.get("challenge_id"):
        print(f"challenge_id: {result['challenge_id']}")
    if result.get("action_id"):
        print(f"action_id: {result['action_id']}")
