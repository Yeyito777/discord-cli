"""The one Discord desktop-client identity every REST and Gateway request presents.

Before this module the same constants lived in api.py, gateway.py and auth.py,
the timezone header was hardcoded to America/Chicago and the build number was
scraped from discord.com twice per process.  Header shapes follow the Linux
desktop client (Electron), as endcord does.
"""

import base64
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path

# Linux stable 1.0.167 (2026-10-28): updates.discord.com manifest, versions read
# from the package's build_info.json and Discord binary. Discord refuses phone
# verification ("update your app") from the old 0.0.x line.
CLIENT_VERSION = "1.0.167"
ELECTRON_VERSION = "44.9.3"
CHROME_VERSION = "152.0.7977.167"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    f"(KHTML, like Gecko) discord/{CLIENT_VERSION} "
    f"Chrome/{CHROME_VERSION} Electron/{ELECTRON_VERSION} Safari/537.36"
)
FALLBACK_BUILD_NUMBER = 653945  # observed 2026-10-28
BUILD_NUMBER_TTL = 12 * 60 * 60

# One launch per process, shared by REST and Gateway like a single client run.
LAUNCH_ID = str(uuid.uuid4())
HEARTBEAT_SESSION_ID = str(uuid.uuid4())

_build_number = None


def _cache_file():
    root = os.environ.get("XDG_CACHE_HOME", "").strip() or str(Path.home() / ".cache")
    return Path(root) / "discord-cli" / "build-number.json"


def _fetch_build_number():
    import http.client

    conn = http.client.HTTPSConnection("discord.com", 443, timeout=10)
    try:
        conn.request("GET", "/app", headers={"User-Agent": USER_AGENT})
        html = conn.getresponse().read().decode("utf-8", errors="replace")
        match = re.search(r'src="/assets/(web\.[a-f0-9]+\.js)"', html)
        if not match:
            return None
        conn.request("GET", f"/assets/{match.group(1)}", headers={"User-Agent": USER_AGENT})
        js = conn.getresponse().read().decode("utf-8", errors="replace")
    finally:
        conn.close()
    match = re.search(r'buildNumber["\s:]*(\d+)', js)
    return int(match.group(1)) if match else None


def build_number():
    """Discord's current client build number, cached on disk for BUILD_NUMBER_TTL."""
    global _build_number
    if _build_number is not None:
        return _build_number
    path = _cache_file()
    cached = None
    try:
        cached = json.loads(path.read_text())
        if time.time() - cached["fetched_at"] < BUILD_NUMBER_TTL:
            _build_number = int(cached["build_number"])
            return _build_number
    except (OSError, ValueError, KeyError, TypeError):
        cached = None
    try:
        fetched = _fetch_build_number()
    except Exception:
        fetched = None
    if fetched:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"build_number": fetched, "fetched_at": time.time()}))
            tmp.replace(path)
        except OSError:
            pass
        _build_number = fetched
    else:
        _build_number = int((cached or {}).get("build_number") or FALLBACK_BUILD_NUMBER)
    return _build_number


def os_version():
    try:
        return subprocess.check_output(["uname", "-r"], text=True).strip()
    except Exception:
        return ""


def system_locale():
    """BCP 47 locale as the client reports it (en-US, not en_US)."""
    raw = os.environ.get("LC_ALL") or os.environ.get("LANG") or "en_US"
    raw = raw.split(".")[0].split("@")[0]
    if raw in {"C", "POSIX", ""}:
        return "en-US"
    return raw.replace("_", "-")


def timezone():
    """The host's IANA timezone (TZ, then /etc/localtime), as X-Discord-Timezone."""
    tz = os.environ.get("TZ", "").strip().lstrip(":")
    if tz and "/" in tz and not tz.startswith("/"):
        return tz
    try:
        target = os.path.realpath("/etc/localtime")
        match = re.search(r"zoneinfo/(?:posix/|right/)?(.+)$", target)
        if match:
            return match.group(1)
    except OSError:
        pass
    try:
        name = Path("/etc/timezone").read_text().strip()
        if name:
            return name
    except OSError:
        pass
    return "UTC"


def super_properties():
    """X-Super-Properties / Gateway identify properties of the desktop client."""
    return {
        "os": "Linux",
        "browser": "Discord Client",
        "release_channel": "stable",
        "os_version": os_version(),
        "os_arch": "x64",
        "app_arch": "x64",
        "system_locale": system_locale(),
        "has_client_mods": False,
        "browser_user_agent": USER_AGENT,
        "browser_version": "",
        "runtime_environment": "native",
        "client_build_number": build_number(),
        "native_build_number": None,
        "client_event_source": None,
        "client_launch_id": LAUNCH_ID,
        "client_heartbeat_session_id": HEARTBEAT_SESSION_ID,
        "client_version": CLIENT_VERSION,
        "window_manager": (
            f"{os.environ.get('XDG_CURRENT_DESKTOP', 'unknown')},"
            f"{os.environ.get('GDMSESSION', 'unknown')}"
        ),
    }


def encoded_super_properties():
    return base64.b64encode(json.dumps(super_properties(), separators=(",", ":")).encode()).decode()


def rest_headers(token):
    return {
        "Accept": "*/*",
        "Authorization": token,
        "Content-Type": "application/json",
        "Priority": "u=1",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": USER_AGENT,
        "X-Super-Properties": encoded_super_properties(),
        "X-Discord-Locale": "en-US",
        "X-Discord-Timezone": timezone(),
    }
