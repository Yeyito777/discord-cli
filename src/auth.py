"""Discord auth token management.

Tokens are stored in PROJECT_ROOT/config/credentials.json.

Use `discord login <token>` to save a token after validating it against
Discord's `/users/@me` endpoint.
"""

import json
import os
import sys
from pathlib import Path
from urllib import error, request

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"
API_ME_URL = "https://discord.com/api/v9/users/@me"
VALIDATION_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) discord/0.0.115 "
    "Chrome/138.0.7204.251 Electron/37.6.0 Safari/537.36"
)


class AuthError(RuntimeError):
    """Raised for local auth setup/validation failures."""


def get_token():
    """Load the Discord auth token from credentials.json.

    Returns the token string.
    Raises RuntimeError if credentials are missing or the file doesn't exist.
    """
    if not CREDENTIALS_FILE.exists():
        raise RuntimeError(
            "Not authenticated. Run 'discord login <token>' to configure your token."
        )

    try:
        data = json.loads(CREDENTIALS_FILE.read_text())
    except (json.JSONDecodeError, OSError) as e:
        raise RuntimeError(f"Failed to read credentials file {CREDENTIALS_FILE}: {e}")

    token = data.get("token", "")
    if not token:
        raise RuntimeError(
            "Credentials file is missing the token. "
            "Run 'discord login <token>' to reconfigure."
        )

    return token


def save_token(token):
    """Save the Discord token to credentials.json.

    Creates the config directory if it doesn't exist.
    Sets restrictive file permissions (600) since these are credentials.
    """
    CREDENTIALS_FILE.parent.mkdir(parents=True, exist_ok=True)

    data = {"token": token}
    CREDENTIALS_FILE.write_text(json.dumps(data, indent=2) + "\n")

    try:
        os.chmod(CREDENTIALS_FILE, 0o600)
    except OSError:
        pass


def delete_token():
    """Delete credentials.json.

    Returns True if the file was deleted, False if it didn't exist.
    """
    if not CREDENTIALS_FILE.exists():
        return False
    CREDENTIALS_FILE.unlink()
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
    except AuthError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)

    save_token(token)
    print(f"Logged in as {_user_label(user)}.")
    print(f"Token saved to {CREDENTIALS_FILE}")


def _logout(argv):
    if len(argv) == 1 and argv[0] in ("-h", "--help"):
        print("usage: discord logout")
        return
    if argv:
        print("usage: discord logout", file=sys.stderr)
        raise SystemExit(2)
    if delete_token():
        print("Logged out.")
    else:
        print("Not logged in.")


def dispatch(cmd, argv):
    """Dispatch auth subcommands."""
    if cmd in ("login", "setup", "auth"):
        _login_with_token(argv)
    elif cmd == "logout":
        _logout(argv)
    else:
        raise RuntimeError(f"Unknown auth command: {cmd}")
