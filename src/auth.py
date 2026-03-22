"""Discord auth token management.

The token is a user auth token extracted from a logged-in Discord session.
Stored in PROJECT_ROOT/config/credentials.json.

Run 'discord login' to configure.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"


def get_token():
    """Load the Discord auth token from credentials.json.

    Returns the token string.
    Raises RuntimeError if credentials are missing or the file doesn't exist.
    """
    if not CREDENTIALS_FILE.exists():
        raise RuntimeError(
            "Not authenticated. Run 'discord login' to configure your token."
        )

    try:
        data = json.loads(CREDENTIALS_FILE.read_text())
    except (json.JSONDecodeError, OSError) as e:
        raise RuntimeError(f"Failed to read credentials file {CREDENTIALS_FILE}: {e}")

    token = data.get("token", "")
    if not token:
        raise RuntimeError(
            "Credentials file is missing the token. "
            "Run 'discord login' to reconfigure."
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


def _try_extract_from_qb():
    """Try to extract the Discord token from a running qutebrowser instance.

    Tries exocortex profile first, then yeyito.
    Returns the token string or None if extraction fails.
    """
    for profile in ("exocortex", "yeyito"):
        token = _try_extract_from_profile(profile)
        if token:
            return token
    return None


def _try_extract_from_profile(profile):
    """Try to extract the Discord token from a specific qb profile."""
    try:
        result = subprocess.run(
            ["qb", "tabs", "-b", profile],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            return None

        discord_tab = None
        for line in result.stdout.strip().split("\n"):
            if "discord.com" in line:
                parts = line.split()
                discord_tab = parts[0]
                break

        if not discord_tab:
            return None

        # Extract token via iframe localStorage trick
        js = (
            'var f=document.createElement("iframe");'
            'f.style.display="none";'
            'document.body.appendChild(f);'
            'var t=f.contentWindow.localStorage.getItem("token");'
            'f.remove();t'
        )
        result = subprocess.run(
            ["qb", "console", "-b", profile, discord_tab, js],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            return None

        token = result.stdout.strip().strip('"')
        if token and token != "null" and len(token) > 20:
            return token

        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def _setup_interactive():
    """Interactive setup flow — extract from browser or paste manually."""
    print()
    print("  discord-cli setup")
    print("  ─────────────────")
    print()

    # Try auto-extraction first
    print("  Attempting to extract token from qutebrowser...", end=" ", flush=True)
    token = _try_extract_from_qb()

    if token:
        print("found!")
        print()
        save_token(token)
        print(f"  ✓ Token saved to {CREDENTIALS_FILE}")
        print("  You're all set! Try 'discord guilds' to see your servers.")
        print()
        return True

    print("not found.")
    print()
    print("  To get your token manually:")
    print()
    print("  1. Open https://discord.com in your browser and log in")
    print("  2. Open DevTools (F12) → Console")
    print('  3. Paste: (webpackChunkdiscord_app.push([[Symbol()],{},r=>{')
    print('       m=Object.values(r.c);for(let x of m){try{')
    print('       let t=x.exports?.default?.getToken?.();')
    print('       if(t){console.log(t);break}}catch{}}}]),void 0)')
    print()
    print("  Or use the iframe trick:")
    print('  4. Paste: var f=document.createElement("iframe");')
    print('     f.style.display="none";document.body.appendChild(f);')
    print('     console.log(f.contentWindow.localStorage.getItem("token"));')
    print('     f.remove()')
    print()

    token = input("  Paste token: ").strip().strip('"')
    if not token:
        print("\n  ✗ Token is required.", flush=True)
        return False

    save_token(token)
    print()
    print(f"  ✓ Token saved to {CREDENTIALS_FILE}")
    print("  You're all set! Try 'discord guilds' to see your servers.")
    print()
    return True


def dispatch(cmd, argv):
    """Dispatch auth subcommands."""
    if cmd in ("login", "setup", "auth"):
        _setup_interactive()
    elif cmd == "logout":
        if delete_token():
            print("Logged out.")
        else:
            print("Not logged in.")
