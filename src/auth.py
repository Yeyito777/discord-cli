"""Discord auth token validation and named-account credential management."""

import json
import sys
from urllib import error, request

from src.accounts import (
    credentials_file,
    read_token,
    selected_account,
    verify_and_refresh_selected_identity,
    write_token,
)

API_ME_URL = "https://discord.com/api/v9/users/@me"
VALIDATION_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) discord/0.0.115 "
    "Chrome/138.0.7204.251 Electron/37.6.0 Safari/537.36"
)


class AuthError(RuntimeError):
    """Raised for local auth setup/validation failures."""


def get_token():
    """Load the token for the invocation's selected account."""
    return read_token()


def save_token(token):
    """Save a token for the invocation's selected account."""
    return write_token(token)


def delete_token():
    """Delete credentials for the invocation's selected account."""
    path = credentials_file()
    if not path.exists():
        return False
    path.unlink()
    return True


def _token_usage(cmd="login"):
    return f"usage: discord {cmd} <token>"


def _decode_json_bytes(raw):
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        return None


def validate_token(token):
    """Validate a Discord token by calling /users/@me.

    Returns the user object on success.
    Raises AuthError on failure.
    """
    token = token.strip().strip('"')
    if not token:
        raise AuthError("Token is required.")

    req = request.Request(
        API_ME_URL,
        headers={
            "Accept": "application/json",
            "Authorization": token,
            "User-Agent": VALIDATION_USER_AGENT,
        },
        method="GET",
    )

    try:
        with request.urlopen(req, timeout=15) as resp:
            data = _decode_json_bytes(resp.read())
    except error.HTTPError as e:
        payload = _decode_json_bytes(e.read())
        if e.code in (401, 403):
            raise AuthError("Invalid Discord token.") from e
        detail = f"Discord returned HTTP {e.code} while validating the token"
        if isinstance(payload, dict) and payload.get("message"):
            detail += f": {payload['message']}"
        raise AuthError(detail) from e
    except error.URLError as e:
        raise AuthError(f"Failed to reach Discord while validating the token: {e.reason}") from e
    except Exception as e:
        raise AuthError(f"Failed to validate the token: {e}") from e

    if not isinstance(data, dict) or not data.get("id"):
        raise AuthError("Discord did not return a valid user object for this token.")

    return data


def _user_label(user):
    username = str(user.get("username") or user.get("id") or "unknown")
    discriminator = str(user.get("discriminator") or "")
    if discriminator and discriminator != "0":
        base = f"{username}#{discriminator}"
    else:
        base = username

    global_name = str(user.get("global_name") or "").strip()
    if global_name and global_name != username:
        return f"{global_name} ({base})"
    return base


def _login_with_token(argv):
    if len(argv) == 1 and argv[0] in ("-h", "--help"):
        print(_token_usage())
        return

    if len(argv) != 1:
        print(_token_usage(), file=sys.stderr)
        raise SystemExit(2)

    token = argv[0].strip().strip('"')
    if not token:
        print("Error: token must not be empty.", file=sys.stderr)
        raise SystemExit(2)

    try:
        user = validate_token(token)
        verify_and_refresh_selected_identity(user)
    except AuthError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)

    save_token(token)
    account = selected_account()
    print(f"Logged in as {_user_label(user)} using account '{account['alias']}'.")
    print(f"Token saved to {credentials_file()}")


def _logout(argv):
    if len(argv) == 1 and argv[0] in ("-h", "--help"):
        print("usage: discord logout")
        return
    if argv:
        print("usage: discord logout", file=sys.stderr)
        raise SystemExit(2)
    try:
        removed = delete_token()
    except RuntimeError:
        removed = False
    print("Logged out." if removed else "Not logged in.")


def dispatch(cmd, argv):
    """Dispatch auth subcommands."""
    if cmd in ("login", "setup", "auth"):
        _login_with_token(argv)
    elif cmd == "logout":
        _logout(argv)
    else:
        raise RuntimeError(f"Unknown auth command: {cmd}")
