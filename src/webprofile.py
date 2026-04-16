"""Dedicated Discord web profile/session helpers.

This module owns:
- the dedicated persistent Chromium profile
- profile-local state like cookies/fingerprint/session bootstrap
- generic Discord web navigation/session checks

Higher-level actions (DM send, invite join, captcha handling) live in
`src.websession`.
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
WEB_DIR = CONFIG_DIR / "web"
WEB_PROFILE_DIR = WEB_DIR / "chromium-profile"
CAPTCHA_PROFILE_DIR = CONFIG_DIR / "captcha" / "chromium-profile"

DISCORD_WEBAPP_URL = "https://discord.com/channels/@me"
DISCORD_LOGIN_URL = "https://discord.com/login"
API_BASE = "/api/v9"
WEB_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.7632.6 Safari/537.36"
)

# Selectors anchored from a real Discord web DOM snapshot gathered from the
# exocortex qutebrowser profile on 2026-04-13.
DM_SEARCH_SELECTOR = 'input[placeholder="Find or start a conversation"]'
COMPOSER_SELECTOR = (
    'div[role="textbox"][data-slate-editor="true"]'
    '[contenteditable="true"][aria-label^="Message @"]'
)


class DiscordWebError(RuntimeError):
    """Raised for Discord web browser-session failures."""


def logged_in_js() -> str:
    return """() => {
        const bodyText = document.body ? document.body.innerText : '';
        const hasSearch = !!document.querySelector('input[placeholder="Find or start a conversation"]');
        const looksLikeDmShell = bodyText.includes('Direct Messages') && bodyText.includes('Find or start a conversation');
        return hasSearch || looksLikeDmShell;
    }"""


def authenticated_session_js() -> str:
    return """async () => {
        let rawToken = null;
        try {
            const frame = document.createElement('iframe');
            frame.style.display = 'none';
            document.body.appendChild(frame);
            rawToken = frame.contentWindow.localStorage.getItem('token');
            frame.remove();
        } catch (_) {}
        if (!rawToken || rawToken === 'null' || rawToken === 'undefined') {
            return false;
        }
        let token = rawToken;
        try {
            token = JSON.parse(rawToken);
        } catch (_) {}
        if (!token) {
            return false;
        }
        try {
            const resp = await fetch('/api/v9/users/@me', {
                credentials: 'include',
                cache: 'no-store',
                headers: { Authorization: token },
            });
            return resp.status === 200;
        } catch (_) {
            return false;
        }
    }"""


def ensure_dirs() -> None:
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    WEB_PROFILE_DIR.mkdir(parents=True, exist_ok=True)


def cookie_db_candidates(profile_dir: Path) -> list[Path]:
    return [
        profile_dir / 'Default' / 'Cookies',
        profile_dir / 'Default' / 'Network' / 'Cookies',
    ]


def existing_cookie_db(profile_dir: Path) -> Path | None:
    for path in cookie_db_candidates(profile_dir):
        if path.exists():
            return path
    return None


def _chrome_expires_utc_is_valid(expires_utc: int) -> bool:
    if not expires_utc:
        return True
    unix_secs = (expires_utc / 1_000_000) - 11644473600
    return unix_secs > time.time()


def _cookie_db_has_valid_accessibility_cookie(path: Path) -> bool:
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            rows = con.execute(
                "select name, coalesce(expires_utc, 0) from cookies where host_key like '%hcaptcha%'"
            ).fetchall()
        finally:
            con.close()
    except Exception:
        return False
    for name, expires_utc in rows:
        if name == 'hc_accessibility' and _chrome_expires_utc_is_valid(int(expires_utc or 0)):
            return True
    return False


def _hcaptcha_cookie_source_candidates() -> list[Path]:
    candidates: list[Path] = []
    primary = existing_cookie_db(CAPTCHA_PROFILE_DIR)
    if primary is not None:
        candidates.append(primary)
    for extra in (
        Path.home() / '.runtime' / 'qutebrowser-exocortex' / 'data' / 'webengine' / 'Cookies',
        Path.home() / '.runtime' / 'qutebrowser-yeyito' / 'data' / 'webengine' / 'Cookies',
    ):
        if extra.exists() and extra not in candidates:
            candidates.append(extra)
    return candidates


def _pick_hcaptcha_cookie_source() -> Path | None:
    candidates = _hcaptcha_cookie_source_candidates()
    for path in candidates:
        if _cookie_db_has_valid_accessibility_cookie(path):
            return path
    return candidates[0] if candidates else None


def seed_hcaptcha_cookies_from_captcha_profile() -> int:
    """Copy hCaptcha-related cookies from captcha profile into the web profile.

    Returns the number of copied rows.
    This does not guarantee the target profile is authenticated to Discord; it
    only helps migrate the accessibility/browser state.
    """
    src = _pick_hcaptcha_cookie_source()
    if src is None:
        raise DiscordWebError(
            f"No hCaptcha cookie source DB found under {CAPTCHA_PROFILE_DIR} or qutebrowser runtime profiles"
        )

    dst = existing_cookie_db(WEB_PROFILE_DIR)
    if dst is None:
        raise DiscordWebError(
            "No web-profile cookie DB found yet. Run `discord web setup` once first."
        )

    src_con = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    try:
        rows = src_con.execute(
            "select creation_utc,host_key,top_frame_site_key,name,value,encrypted_value,"
            "path,expires_utc,is_secure,is_httponly,last_access_utc,has_expires,"
            "is_persistent,priority,samesite,source_scheme,source_port,last_update_utc,"
            "source_type,has_cross_site_ancestor "
            "from cookies where host_key like '%hcaptcha%'"
        ).fetchall()
    finally:
        src_con.close()

    if not rows:
        return 0

    dst_con = sqlite3.connect(dst)
    try:
        dst_con.execute("delete from cookies where host_key like '%hcaptcha%'")
        dst_con.executemany(
            "insert into cookies(creation_utc,host_key,top_frame_site_key,name,value,encrypted_value,"
            "path,expires_utc,is_secure,is_httponly,last_access_utc,has_expires,is_persistent,"
            "priority,samesite,source_scheme,source_port,last_update_utc,source_type,"
            "has_cross_site_ancestor) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        dst_con.commit()
    finally:
        dst_con.close()
    return len(rows)


_cached_fingerprint = None


def fetch_fingerprint() -> str | None:
    """Fetch a Discord web fingerprint from the public experiments endpoint."""
    global _cached_fingerprint
    if _cached_fingerprint:
        return _cached_fingerprint
    req = urllib.request.Request(
        'https://discord.com/api/v9/experiments',
        headers={'User-Agent': WEB_USER_AGENT},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode('utf-8', errors='replace'))
        fp = data.get('fingerprint')
        if fp:
            _cached_fingerprint = fp
            return fp
    except Exception:
        return None
    return None


def playwright_start():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise DiscordWebError(
            "Playwright is not installed in discord-cli's virtualenv. "
            "Install it in .venv before using browser-native Discord automation."
        ) from e
    return sync_playwright().start()


def launch_context(*, headed: bool = False):
    """Launch the dedicated persistent Chromium context for Discord web."""
    ensure_dirs()
    pw = playwright_start()
    args = [
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-dev-shm-usage",
        "--no-sandbox",
    ]
    if not headed:
        args.append("--headless=new")

    try:
        context = pw.chromium.launch_persistent_context(
            str(WEB_PROFILE_DIR),
            headless=not headed,
            args=args,
            ignore_default_args=["--enable-automation"],
            viewport={"width": 1440, "height": 900},
            user_agent=WEB_USER_AGENT,
            locale="en-US",
            timezone_id="America/Chicago",
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        return pw, context
    except Exception:
        pw.stop()
        raise


def open_app(context, *, timeout_ms: int = 30_000):
    """Open/return a clean Discord web app page in the persistent context.

    Persistent Chromium sessions can restore stale Discord tabs (login pages,
    failed invite pages, old captcha tabs). For new actions we want a stable
    app-shell page, not an arbitrary restored discord.com tab.
    """
    preferred = None
    for page in context.pages:
        try:
            url = page.url or ""
            if "discord.com" not in url:
                continue
            if "/channels/" in url:
                return page
            if preferred is None and "/invite/" not in url and "/login" not in url:
                preferred = page
        except Exception:
            pass

    if preferred is not None:
        try:
            preferred.goto(DISCORD_WEBAPP_URL, wait_until="domcontentloaded", timeout=timeout_ms)
            return preferred
        except Exception:
            pass

    page = context.new_page()
    page.goto(DISCORD_WEBAPP_URL, wait_until="domcontentloaded", timeout=timeout_ms)
    return page


def inject_token(page, token: str, *, reload_delay_ms: int = 2500) -> None:
    """Best-effort token injection into a dedicated Discord browser profile."""
    page.goto(DISCORD_LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
    page.evaluate(
        """({token, reloadDelayMs}) => {
            const apply = () => {
                try {
                    const frame = document.createElement('iframe');
                    frame.style.display = 'none';
                    document.body.appendChild(frame);
                    frame.contentWindow.localStorage.token = JSON.stringify(token);
                    frame.remove();
                } catch (_) {}
            };
            const interval = setInterval(apply, 50);
            setTimeout(() => {
                clearInterval(interval);
                location.href = 'https://discord.com/channels/@me';
            }, reloadDelayMs);
        }""",
        {"token": token, "reloadDelayMs": reload_delay_ms},
    )


def is_logged_in(page, *, timeout_ms: int = 5_000) -> bool:
    deadline = time.time() + (timeout_ms / 1000.0)
    while time.time() < deadline:
        try:
            if bool(page.evaluate(authenticated_session_js())):
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def ensure_logged_in(page, token: str, *, timeout_ms: int = 20_000) -> bool:
    """Ensure the isolated Discord web profile is authenticated.

    Returns True if a token injection/bootstrap was needed, False if already
    authenticated. This intentionally recognizes logged-in invite pages too,
    so resume flows do not get bounced back to /channels/@me mid-captcha.
    """
    if is_logged_in(page, timeout_ms=2_000):
        return False
    inject_token(page, token)
    wait_for_logged_in(page, timeout_ms=timeout_ms)
    return True


def wait_for_logged_in(page, *, timeout_ms: int = 20_000) -> None:
    if is_logged_in(page, timeout_ms=timeout_ms):
        return
    raise DiscordWebError(
        "Discord web session did not reach an authenticated state in time."
    )


def extract_invite_code(invite: str) -> str:
    m = re.match(
        r'(?:https?://)?(?:(?:ptb|canary)\.)?discord(?:\.gg|\.com/invite)/([A-Za-z0-9\-_]+)',
        invite,
    )
    if m:
        return m.group(1)
    if re.match(r'^[A-Za-z0-9\-_]+$', invite):
        return invite
    raise DiscordWebError(f'Invalid invite: {invite}')


def invite_url(invite: str) -> str:
    return f"https://discord.com/invite/{extract_invite_code(invite)}"


def dm_url(channel_id: str) -> str:
    return f"https://discord.com/channels/@me/{channel_id}"


def open_dm(page, channel_id: str, *, timeout_ms: int = 20_000) -> None:
    """Navigate directly to a DM channel and wait for the message composer."""
    page.goto(dm_url(channel_id), wait_until="domcontentloaded", timeout=timeout_ms)
    wait_for_composer(page, timeout_ms=timeout_ms)


def wait_for_composer(page, *, timeout_ms: int = 15_000) -> None:
    try:
        page.wait_for_selector(COMPOSER_SELECTOR, timeout=timeout_ms)
    except Exception as e:
        raise DiscordWebError("Timed out waiting for the Discord message composer.") from e


def open_invite(page, invite: str, *, timeout_ms: int = 20_000) -> str:
    code = extract_invite_code(invite)
    page.goto(invite_url(code), wait_until="domcontentloaded", timeout=timeout_ms)
    return code
