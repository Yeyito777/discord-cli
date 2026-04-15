"""Browser-backed Discord captcha solving.

This module handles Discord's hCaptcha challenge flow by rendering the actual
hCaptcha widget inside a persistent Playwright Chromium profile. The intent is
for discord-cli to remain a CLI tool while transparently falling back to a real
browser context when Discord returns `captcha-required`.

High-level flow:
1. Discord REST request returns captcha metadata (sitekey, rqdata, rqtoken...)
2. We render the hCaptcha widget in a persistent Chromium profile
3. If the browser profile has the user's accessibility cookie / trusted state,
   hCaptcha can auto-pass or present the lighter accessibility flow
4. We capture the solved token and retry the original Discord request with the
   required X-Captcha-* headers

The browser profile is kept under config/ so the captcha/browser state persists
between invocations.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import base64
import json
import os
import re
import socket
import sqlite3
import subprocess
import time
import urllib.parse
import uuid

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
CAPTCHA_DIR = CONFIG_DIR / "captcha"
BROWSER_PROFILE_DIR = CAPTCHA_DIR / "chromium-profile"
PENDING_DIR = CAPTCHA_DIR / "pending"

ACCESSIBILITY_URL = "https://dashboard.hcaptcha.com/signup?type=accessibility"
HCAPTCHA_ORIGIN = "https://hcaptcha.com"
DEFAULT_TIMEOUT_SECS = 90


class CaptchaError(RuntimeError):
    """Base class for captcha-related failures."""


@dataclass(frozen=True)
class AccessibilityPrompt:
    instruction: str
    kind: str = "text"  # "text" or "interactive"


@dataclass(frozen=True)
class DeferredTextCaptcha(CaptchaError):
    challenge_id: str
    prompt: str
    kind: str = "text"
    broker_port: int | None = None
    broker_pid: int | None = None

    def format_stdout(self) -> str:
        payload = {
            "id": self.challenge_id,
            "kind": f"hcaptcha_{self.kind}",
            "prompt": self.prompt,
            "note": "Legacy manual captcha-resume flow has been removed; use automated browser-native captcha handling instead.",
        }
        return (
            "DISCORD_CAPTCHA_CHALLENGE\n"
            f"id: {self.challenge_id}\n"
            f"prompt: {self.prompt}\n"
            f"kind: {self.kind}\n"
            "resume: removed (automated path only)\n"
            f"DISCORD_CAPTCHA_JSON: {json.dumps(payload, ensure_ascii=False)}"
        )


@dataclass(frozen=True)
class CaptchaChallenge:
    """Discord captcha challenge metadata."""

    service: str
    sitekey: str
    session_id: str | None = None
    rqdata: str | None = None
    rqtoken: str | None = None
    should_serve_invisible: bool = False

    @classmethod
    def from_discord_error(cls, payload: dict) -> "CaptchaChallenge":
        if "captcha_key" not in payload:
            raise CaptchaError("Not a Discord captcha payload")
        service = payload.get("captcha_service") or "hcaptcha"
        if service != "hcaptcha":
            raise CaptchaError(
                f"Unsupported captcha service from Discord: {service}"
            )
        sitekey = payload.get("captcha_sitekey")
        if not sitekey:
            raise CaptchaError("Discord captcha payload is missing captcha_sitekey")
        return cls(
            service=service,
            sitekey=sitekey,
            session_id=payload.get("captcha_session_id"),
            rqdata=payload.get("captcha_rqdata"),
            rqtoken=payload.get("captcha_rqtoken"),
            should_serve_invisible=bool(payload.get("should_serve_invisible")),
        )


@dataclass(frozen=True)
class CaptchaSolution:
    token: str
    broker_port: int | None = None
    broker_pid: int | None = None
    replay_result: object | None = None
    replay_status: int | None = None
    replay_text: str | None = None


@dataclass(frozen=True)
class PendingCaptchaRequest:
    challenge_id: str
    prompt: str
    kind: str
    method: str
    path: str
    body: dict | None
    body_bytes_b64: str | None
    token: str | None
    params: dict | None
    extra_headers: dict | None
    broker_port: int | None
    broker_pid: int | None
    challenge: CaptchaChallenge

    @classmethod
    def from_dict(cls, payload: dict) -> "PendingCaptchaRequest":
        return cls(
            challenge_id=payload["challenge_id"],
            prompt=payload["prompt"],
            kind=payload.get("kind", "text"),
            method=payload["method"],
            path=payload["path"],
            body=payload.get("body"),
            body_bytes_b64=payload.get("body_bytes_b64"),
            token=payload.get("token"),
            params=payload.get("params"),
            extra_headers=payload.get("extra_headers"),
            broker_port=payload.get("broker_port"),
            broker_pid=payload.get("broker_pid"),
            challenge=CaptchaChallenge(**payload["challenge"]),
        )

    def to_dict(self) -> dict:
        return {
            "challenge_id": self.challenge_id,
            "prompt": self.prompt,
            "kind": self.kind,
            "method": self.method,
            "path": self.path,
            "body": self.body,
            "body_bytes_b64": self.body_bytes_b64,
            "token": self.token,
            "params": self.params,
            "extra_headers": self.extra_headers,
            "broker_port": self.broker_port,
            "broker_pid": self.broker_pid,
            "challenge": {
                "service": self.challenge.service,
                "sitekey": self.challenge.sitekey,
                "session_id": self.challenge.session_id,
                "rqdata": self.challenge.rqdata,
                "rqtoken": self.challenge.rqtoken,
                "should_serve_invisible": self.challenge.should_serve_invisible,
            },
        }

    def body_bytes(self) -> bytes | None:
        if self.body_bytes_b64 is None:
            return None
        return base64.b64decode(self.body_bytes_b64)


def _ensure_dirs() -> None:
    CAPTCHA_DIR.mkdir(parents=True, exist_ok=True)
    BROWSER_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    PENDING_DIR.mkdir(parents=True, exist_ok=True)


def _should_launch_headless() -> bool:
    """Whether the hidden Chromium worker should be headless.

    Default strategy:
    - If DISCORD_CAPTCHA_HEADLESS is explicitly set, obey it.
    - Otherwise default to headless for seamless CLI behavior.
    """
    forced = os.environ.get("DISCORD_CAPTCHA_HEADLESS")
    if forced is not None:
        return forced.strip().lower() in {"1", "true", "yes", "on"}
    return True


def _bootstrap_widget_on_page(page) -> None:
    """Prepare a discord.com page to host the hCaptcha widget.

    We intentionally bootstrap the widget from Playwright's JS execution context
    after navigating to discord.com. That keeps the origin closer to Discord's
    actual captcha flow while avoiding inline-script CSP issues.
    """
    page.goto("https://discord.com/login", wait_until="domcontentloaded")
    page.evaluate(
        """
        () => new Promise((resolve, reject) => {
          document.head.innerHTML = '';
          document.body.innerHTML = '';
          document.body.style.margin = '0';
          document.body.style.padding = '24px';
          document.body.style.background = '#111827';
          document.body.style.color = '#e5e7eb';
          document.body.style.fontFamily = 'system-ui, sans-serif';

          const status = document.createElement('div');
          status.id = 'status';
          status.style.marginBottom = '16px';
          status.textContent = 'Loading hCaptcha…';
          document.body.appendChild(status);

          const container = document.createElement('div');
          container.id = 'captcha';
          container.style.minHeight = '80px';
          document.body.appendChild(container);

          window.__captchaReady = false;
          window.__captchaResult = { status: 'pending' };
          window.__captchaWidgetId = null;

          window.__setCaptchaStatus = (text) => {
            const el = document.getElementById('status');
            if (el) el.textContent = text;
          };

          window.startCaptcha = (challenge) => {
            window.__captchaResult = { status: 'pending' };
            window.__setCaptchaStatus('Rendering hCaptcha...');
            try {
              const opts = {
                sitekey: challenge.sitekey,
                size: challenge.should_serve_invisible ? 'invisible' : 'normal',
                callback: (token) => {
                  window.__captchaResult = { status: 'solved', token };
                  window.__setCaptchaStatus('Captcha solved');
                },
                'error-callback': (err) => {
                  window.__captchaResult = { status: 'error', error: String(err) };
                  window.__setCaptchaStatus('Captcha error: ' + String(err));
                },
                'expired-callback': () => {
                  window.__captchaResult = { status: 'expired' };
                  window.__setCaptchaStatus('Captcha expired');
                },
                'open-callback': () => window.__setCaptchaStatus('Captcha opened'),
                'close-callback': () => window.__setCaptchaStatus('Captcha closed'),
              };
              if (challenge.rqdata) {
                opts.rqdata = challenge.rqdata;
              }
              window.__captchaWidgetId = hcaptcha.render('captcha', opts);
              window.__setCaptchaStatus('Executing hCaptcha...');
              hcaptcha.execute(window.__captchaWidgetId);
            } catch (err) {
              window.__captchaResult = { status: 'error', error: String(err) };
              window.__setCaptchaStatus('Captcha exception: ' + String(err));
            }
          };

          const script = document.createElement('script');
          script.src = 'https://js.hcaptcha.com/1/api.js?render=explicit';
          script.async = true;
          script.defer = true;
          script.onload = () => {
            window.__captchaReady = true;
            window.__setCaptchaStatus('hCaptcha script loaded');
            resolve(true);
          };
          script.onerror = () => reject(new Error('Failed to load hCaptcha script'));
          document.head.appendChild(script);
        })
        """
    )


def _playwright_start():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise CaptchaError(
            "Playwright is not installed in discord-cli's virtualenv. "
            "Install it in .venv before using captcha automation."
        ) from e
    return sync_playwright().start()


def _launch_context(*, headed: bool):
    """Launch a persistent Chromium context for setup/status work."""
    _ensure_dirs()

    pw = _playwright_start()
    args = [
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-dev-shm-usage",
    ]
    if not headed:
        args.append("--headless=new")
    else:
        # Best effort: keep the window away from the main workspace.
        args.extend([
            "--window-position=-24000,-24000",
            "--window-size=1280,800",
        ])

    try:
        context = pw.chromium.launch_persistent_context(
            str(BROWSER_PROFILE_DIR),
            headless=not headed,
            args=args,
            ignore_default_args=["--enable-automation"],
            viewport={"width": 1280, "height": 800},
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        return pw, context
    except Exception:
        pw.stop()
        raise


def _reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        return s.getsockname()[1]


def _launch_broker_browser(*, headed: bool):
    """Launch a reconnectable Chromium process for a deferred captcha challenge."""
    _ensure_dirs()
    for stale in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        try:
            (BROWSER_PROFILE_DIR / stale).unlink()
        except FileNotFoundError:
            pass
    pw = _playwright_start()
    port = _reserve_port()
    args = [
        pw.chromium.executable_path,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={BROWSER_PROFILE_DIR}",
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "about:blank",
    ]
    if not headed:
        args.append("--headless=new")
    proc = subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

    browser = None
    last_error = None
    for _ in range(60):
        try:
            browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
            break
        except Exception as e:
            last_error = e
            time.sleep(0.25)
    if browser is None:
        try:
            proc.kill()
        except Exception:
            pass
        pw.stop()
        raise CaptchaError(f"Failed to connect to captcha browser broker: {last_error}")

    context = browser.contexts[0]
    return pw, browser, context, port, proc.pid


def _pick_captcha_page(context):
    pages = list(context.pages)
    for page in reversed(pages):
        try:
            marker = page.evaluate("() => ({ready: window.__captchaReady, hasResult: window.__captchaResult !== undefined})")
            if marker and (marker.get('ready') is True or marker.get('hasResult')):
                return page
        except Exception:
            pass
    for page in reversed(pages):
        try:
            if any("frame=challenge" in (f.url or "") for f in page.frames):
                return page
        except Exception:
            pass
    for page in reversed(pages):
        try:
            if "discord.com/login" in (page.url or ""):
                return page
        except Exception:
            pass
    return pages[-1] if pages else context.new_page()


def _nudge_checkbox_frame(page) -> bool:
    for frame in page.frames:
        try:
            txt = frame.locator('body').inner_text(timeout=700)
        except Exception:
            continue
        lowered = txt.lower()
        if 'soy humano' in lowered or 'i am human' in lowered:
            acted = False
            try:
                frame.evaluate("""() => {
                    const el = document.getElementById('checkbox') || document.querySelector('[role="checkbox"]') || document.body;
                    const rect = el.getBoundingClientRect();
                    const opts = { bubbles: true, cancelable: true, view: window, clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2 };
                    el.dispatchEvent(new MouseEvent('mousemove', opts));
                    el.dispatchEvent(new MouseEvent('mousedown', opts));
                    el.dispatchEvent(new MouseEvent('mouseup', opts));
                    el.dispatchEvent(new MouseEvent('click', opts));
                    if (el.focus) el.focus();
                }""")
                acted = True
            except Exception:
                pass
            try:
                frame.locator('body').click(timeout=700)
                acted = True
            except Exception:
                pass
            try:
                frame.locator('body').press('Space', timeout=700)
                acted = True
            except Exception:
                pass
            try:
                frame.locator('body').press('Enter', timeout=700)
                acted = True
            except Exception:
                pass
            if acted:
                return True
    return False


def _connect_broker_browser(port: int):
    pw = _playwright_start()
    try:
        browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
        context = browser.contexts[0]
        return pw, browser, context
    except Exception:
        pw.stop()
        raise


def _profile_cookie_rows() -> list[tuple[str, str, int]]:
    rows: list[tuple[str, str, int]] = []
    candidates = [
        BROWSER_PROFILE_DIR / 'Default' / 'Cookies',
        BROWSER_PROFILE_DIR / 'Default' / 'Network' / 'Cookies',
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                rows.extend(con.execute(
                    "select host_key, name, coalesce(expires_utc, 0) from cookies where host_key like '%hcaptcha%'"
                ).fetchall())
            finally:
                con.close()
        except Exception:
            continue
    return rows


def _chrome_expires_utc_is_valid(expires_utc: int) -> bool:
    if not expires_utc:
        return True
    # Chrome stores microseconds since 1601-01-01.
    unix_secs = (expires_utc / 1_000_000) - 11644473600
    return unix_secs > time.time()


def _profile_has_accessibility_cookie() -> bool:
    for _host, name, expires_utc in _profile_cookie_rows():
        if name == 'hc_accessibility' and _chrome_expires_utc_is_valid(expires_utc):
            return True
    return False


def _cookie_hint(context) -> str:
    try:
        cookies = context.cookies() if context is not None else []
        has_accessibility = any(c.get("name") == "hc_accessibility" for c in cookies)
        if not has_accessibility and not _profile_has_accessibility_cookie():
            return (
                " No hCaptcha accessibility cookie is stored yet; run `discord captcha setup` "
                "to seed the persistent Chromium profile with your accessibility login."
            )
    except Exception:
        if not _profile_has_accessibility_cookie():
            return (
                " No hCaptcha accessibility cookie is stored yet; run `discord captcha setup` "
                "to seed the persistent Chromium profile with your accessibility login."
            )
    return ""


def _pending_path(challenge_id: str) -> Path:
    _ensure_dirs()
    return PENDING_DIR / f"{challenge_id}.json"


def store_pending_request(*, prompt: str, kind: str, method: str, path: str,
                          body: dict | None, body_bytes: bytes | None,
                          token: str | None, params: dict | None,
                          extra_headers: dict | None,
                          broker_port: int | None,
                          broker_pid: int | None,
                          challenge: CaptchaChallenge) -> DeferredTextCaptcha:
    challenge_id = uuid.uuid4().hex[:12]
    pending = PendingCaptchaRequest(
        challenge_id=challenge_id,
        prompt=prompt,
        kind=kind,
        method=method,
        path=path,
        body=body,
        body_bytes_b64=(base64.b64encode(body_bytes).decode("ascii") if body_bytes is not None else None),
        token=token,
        params=params,
        extra_headers=extra_headers,
        broker_port=broker_port,
        broker_pid=broker_pid,
        challenge=challenge,
    )
    _pending_path(challenge_id).write_text(
        json.dumps(pending.to_dict(), indent=2, ensure_ascii=False) + "\n"
    )
    return DeferredTextCaptcha(challenge_id=challenge_id, prompt=prompt, kind=kind)


def load_pending_request(challenge_id: str) -> PendingCaptchaRequest:
    path = _pending_path(challenge_id)
    if not path.exists():
        raise CaptchaError(f"No pending captcha challenge found for id: {challenge_id}")
    return PendingCaptchaRequest.from_dict(json.loads(path.read_text()))


def save_pending_request(pending: PendingCaptchaRequest) -> None:
    _pending_path(pending.challenge_id).write_text(
        json.dumps(pending.to_dict(), indent=2, ensure_ascii=False) + "\n"
    )


def delete_pending_request(challenge_id: str) -> None:
    path = _pending_path(challenge_id)
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def list_pending_requests() -> list[dict]:
    _ensure_dirs()
    items = []
    for path in sorted(PENDING_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text())
            items.append({
                "id": payload.get("challenge_id"),
                "prompt": payload.get("prompt"),
                "path": payload.get("path"),
                "method": payload.get("method"),
            })
        except Exception:
            items.append({"id": path.stem, "prompt": "<unreadable>", "path": None, "method": None})
    return items


def _extract_accessibility_prompt(challenge_frame) -> AccessibilityPrompt | None:
    try:
        body_text = challenge_frame.locator("body").inner_text(timeout=1500)
    except Exception:
        return None

    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    if not lines:
        return None

    try:
        has_text_input = (
            challenge_frame.locator("input").count() > 0
            or challenge_frame.locator("textarea").count() > 0
        )
    except Exception:
        has_text_input = False

    instruction = None
    for line in lines:
        lowered = line.lower()
        if any(skip in lowered for skip in [
            "try again",
            "inténtalo de nuevo",
            "please answer",
            "por favor, responda",
            "por favor, use",
            "por favor, utilice",
            "responda usando",
            "responda la siguiente pregunta",
            "answer the following question",
            "please use only numbers",
            "please use only numbers in your answer",
            "please use only letters",
            "please use only letters in your answer",
            "please use only letters in your answer to the following question",
            "please respond to the following question using only letters",
            "please use one word, number or phrase",
            "por favor, use solo numeros",
            "por favor, use solo números",
            "por favor, use solo letras",
            "numbers only",
            "letters only",
            "solo números",
            "solo letras",
            "una sola palabra",
            "one word, number or phrase",
            "omit",
            "omitir",
            "soy humano",
            "i am human",
            "privacidad",
            "privacy",
            "condiciones",
            "terms",
        ]):
            continue
        if (
            ("digits only" in lowered)
            or ("question below" in lowered)
            or ("question above" in lowered)
            or ("use only numbers" in lowered)
            or ("answer using" in lowered and "question" in lowered)
            or ("following question" in lowered and any(tok in lowered for tok in ["number", "numbers", "digits", "word", "phrase"]))
            or ("question below" in lowered and any(tok in lowered for tok in ["number", "numbers", "digits", "word", "phrase"]))
        ):
            continue
        if lowered in {"es", "en", "verify", "verificar", "next", "siguiente"}:
            continue
        if len(line) < 6:
            continue
        instruction = line
        break
    if instruction is None:
        return None
    return AccessibilityPrompt(
        instruction=instruction,
        kind="text" if has_text_input else "interactive",
    )


def _submit_accessibility_prompt(challenge_frame, answer: str) -> None:
    input_box = challenge_frame.locator("input").first
    input_box.click()
    try:
        input_box.press("Control+A")
        input_box.press("Backspace")
    except Exception:
        pass
    # Avoid `fill()` here; type more like a human so the captcha widget sees
    # real key events instead of an abrupt value replacement.
    input_box.type(answer, delay=110)
    # In live probing, Enter on the answer box advanced the challenge more
    # reliably than clicking the visible Verificar/Verify control.
    input_box.press("Enter")


def _find_accessibility_prompt(page) -> tuple[object, AccessibilityPrompt] | tuple[None, None]:
    frames = [f for f in page.frames if f is not page.main_frame]
    # Prefer frames that look like the challenge iframe, but fall back to all
    # child frames because the URL can become unreliable after reconnecting.
    ordered = sorted(
        frames,
        key=lambda f: 0 if "frame=challenge" in (f.url or "") else 1,
    )
    for frame in ordered:
        prompt = _extract_accessibility_prompt(frame)
        if prompt is not None:
            return frame, prompt
    return None, None


def _browser_replay_request(page, method: str, path: str, *, body: dict | None,
                            body_bytes: bytes | None, token: str | None,
                            params: dict | None, extra_headers: dict | None):
    from src import api

    if body is not None and body_bytes is not None:
        raise ValueError("Provide either body or body_bytes, not both")

    url = f"{api.API_BASE}{path}"
    if params:
        qs = urllib.parse.urlencode(params, doseq=True)
        url = f"{url}?{qs}"

    headers = api._build_headers(token, extra_headers)
    blocked = {
        'host', 'content-length', 'user-agent', 'origin', 'referer',
        'accept-encoding', 'connection',
    }
    safe_headers = {
        k: v for k, v in headers.items()
        if v is not None and k.lower() not in blocked and not k.lower().startswith('sec-')
    }

    payload = {
        'method': method,
        'url': url,
        'headers': safe_headers,
        'jsonBody': body,
        'bodyBase64': base64.b64encode(body_bytes).decode('ascii') if body_bytes is not None else None,
    }
    result = page.evaluate(
        """async req => {
            const opts = {
                method: req.method,
                headers: req.headers,
                credentials: 'include',
            };
            if (req.bodyBase64 !== null) {
                const raw = atob(req.bodyBase64);
                const bytes = Uint8Array.from(raw, c => c.charCodeAt(0));
                opts.body = bytes;
            } else if (req.jsonBody !== null) {
                opts.body = JSON.stringify(req.jsonBody);
            }
            const resp = await fetch(req.url, opts);
            const text = await resp.text();
            return {
                status: resp.status,
                text,
                headers: Object.fromEntries(resp.headers.entries()),
            };
        }""",
        payload,
    )

    status = result.get('status')
    text = result.get('text') or ''
    if status == 204:
        return None, status, text
    if 200 <= status < 300:
        if not text:
            return None, status, text
        return json.loads(text), status, text
    return None, status, text


def solve_hcaptcha(challenge: CaptchaChallenge, timeout_secs: int = DEFAULT_TIMEOUT_SECS,
                   provided_answer: str | None = None,
                   broker_port: int | None = None,
                   previous_prompt: str | None = None,
                   broker_pid: int | None = None,
                   replay_request: dict | None = None) -> CaptchaSolution:
    """Solve a Discord hCaptcha challenge in the persistent Chromium profile."""
    timeout_secs = int(os.environ.get("DISCORD_CAPTCHA_TIMEOUT", timeout_secs))
    headed = (not _should_launch_headless()) or (not _profile_has_accessibility_cookie())
    pw = None
    browser = None
    context = None
    submitted_answer = False
    preserve_broker = False
    last_prompt_after_submit = previous_prompt
    submit_started_at = None
    last_checkbox_nudge_at = None
    try:
        if broker_port is None:
            pw, browser, context, broker_port, broker_pid = _launch_broker_browser(headed=headed)
            page = context.new_page()
            _bootstrap_widget_on_page(page)
            page.wait_for_function("() => window.__captchaReady === true", timeout=30_000)
            page.evaluate(
                "challenge => window.startCaptcha(challenge)",
                {
                    "sitekey": challenge.sitekey,
                    "rqdata": challenge.rqdata,
                    "should_serve_invisible": challenge.should_serve_invisible,
                },
            )
        else:
            pw, browser, context = _connect_broker_browser(broker_port)
            page = _pick_captcha_page(context)

        deadline = time.time() + timeout_secs
        while time.time() < deadline:
            result = page.evaluate("() => window.__captchaResult") or {}
            status = result.get("status")
            if status == "solved" and result.get("token"):
                replay_result = None
                replay_status = None
                replay_text = None
                if replay_request is not None:
                    replay_headers = dict(replay_request.get('extra_headers') or {})
                    replay_headers['X-Captcha-Key'] = result['token']
                    if challenge.session_id:
                        replay_headers['X-Captcha-Session-Id'] = challenge.session_id
                    if challenge.rqtoken:
                        replay_headers['X-Captcha-Rqtoken'] = challenge.rqtoken
                    replay_result, replay_status, replay_text = _browser_replay_request(
                        page,
                        replay_request['method'],
                        replay_request['path'],
                        body=replay_request.get('body'),
                        body_bytes=replay_request.get('body_bytes'),
                        token=replay_request.get('token'),
                        params=replay_request.get('params'),
                        extra_headers=replay_headers,
                    )
                if broker_port is not None:
                    preserve_broker = True
                return CaptchaSolution(
                    token=result["token"],
                    broker_port=broker_port,
                    broker_pid=broker_pid,
                    replay_result=replay_result,
                    replay_status=replay_status,
                    replay_text=replay_text,
                )
            if status == "expired":
                raise CaptchaError("hCaptcha challenge expired before a token was produced")
            if status == "error":
                err = result.get("error") or "unknown error"
                raise CaptchaError(f"hCaptcha solve failed: {err}.{_cookie_hint(context)}")

            frame, prompt = _find_accessibility_prompt(page)
            if prompt is not None:
                if prompt.kind != "text":
                    if headed:
                        # Visible browser fallback: let the operator solve the
                        # interactive challenge directly in the window.
                        time.sleep(0.5)
                        continue
                    raise CaptchaError(
                        "Interactive hCaptcha challenge requires a visible browser session. "
                        "Use the browser-native path or rerun with a headed captcha/browser profile."
                    )

                should_submit = (
                    (not submitted_answer)
                    or (prompt.instruction != last_prompt_after_submit)
                    or (
                        submit_started_at is not None
                        and (time.time() - submit_started_at) > 8
                    )
                )
                if should_submit:
                    if provided_answer is not None and not submitted_answer:
                        answer = provided_answer
                    else:
                        from src.hcaptcha_text import solve_accessibility_prompt
                        answer = solve_accessibility_prompt(prompt.instruction)
                    _submit_accessibility_prompt(frame, answer)
                    submitted_answer = True
                    submit_started_at = time.time()
                    last_prompt_after_submit = prompt.instruction
                    time.sleep(1.5)
                    continue

            if submitted_answer:
                now = time.time()
                if last_checkbox_nudge_at is None or (now - last_checkbox_nudge_at) >= 3.0:
                    if _nudge_checkbox_frame(page):
                        last_checkbox_nudge_at = now
                        # In live probing the next prompt often reappears ~2s later.
                        time.sleep(2.5)
                        continue
                else:
                    time.sleep(0.5)
                    continue

            time.sleep(0.5)

        if provided_answer is not None:
            preserve_broker = True
        raise CaptchaError(
            f"Timed out waiting for hCaptcha solve.{_cookie_hint(context)}"
        )
    except CaptchaError:
        raise
    except Exception as e:
        raise CaptchaError(f"Browser captcha flow failed: {e}.{_cookie_hint(context)}") from e
    finally:
        try:
            if not preserve_broker and context is not None:
                try:
                    context.close()
                except Exception:
                    pass
        finally:
            if pw is not None:
                try:
                    pw.stop()
                except Exception:
                    pass


def solve_pending_request(challenge_id: str, answer: str, timeout_secs: int = DEFAULT_TIMEOUT_SECS):
    """Resume a stored text captcha challenge and replay the original Discord request."""
    pending = load_pending_request(challenge_id)
    if pending.kind == "interactive":
        raise CaptchaError(
            "This pending captcha requires manual browser interaction, not a text answer. "
            "Restore the accessibility cookie with `discord captcha setup` or solve it in a visible browser session."
        )
    try:
        try:
            solution = solve_hcaptcha(
                pending.challenge,
                timeout_secs=timeout_secs,
                provided_answer=answer,
                broker_port=pending.broker_port,
                previous_prompt=pending.prompt,
                broker_pid=pending.broker_pid,
                replay_request={
                    'method': pending.method,
                    'path': pending.path,
                    'body': pending.body,
                    'body_bytes': pending.body_bytes(),
                    'token': pending.token,
                    'params': pending.params,
                    'extra_headers': None,  # filled after captcha token below
                },
            )
        except DeferredTextCaptcha as deferred:
            updated = PendingCaptchaRequest(
                challenge_id=pending.challenge_id,
                prompt=deferred.prompt,
                kind=deferred.kind,
                method=pending.method,
                path=pending.path,
                body=pending.body,
                body_bytes_b64=pending.body_bytes_b64,
                token=pending.token,
                params=pending.params,
                extra_headers=pending.extra_headers,
                broker_port=deferred.broker_port or pending.broker_port,
                broker_pid=deferred.broker_pid or pending.broker_pid,
                challenge=pending.challenge,
            )
            save_pending_request(updated)
            raise DeferredTextCaptcha(
                challenge_id=pending.challenge_id,
                prompt=deferred.prompt,
                kind=deferred.kind,
                broker_port=updated.broker_port,
                broker_pid=updated.broker_pid,
            )

        from src import api

        retry_headers = dict(pending.extra_headers or {})
        retry_headers["X-Captcha-Key"] = solution.token
        if pending.challenge.session_id:
            retry_headers["X-Captcha-Session-Id"] = pending.challenge.session_id
        if pending.challenge.rqtoken:
            retry_headers["X-Captcha-Rqtoken"] = pending.challenge.rqtoken

        # Prefer replaying from inside the same Discord browser context that
        # produced the captcha token. If Discord still responds with a fresh
        # captcha payload, surface that as a new pending challenge.
        if solution.replay_status is not None:
            if 200 <= solution.replay_status < 300:
                delete_pending_request(challenge_id)
                return solution.replay_result

            err = None
            if solution.replay_text:
                try:
                    err = json.loads(solution.replay_text)
                except Exception:
                    err = None
            if isinstance(err, dict) and "captcha_key" in err:
                delete_pending_request(challenge_id)
                new_challenge = CaptchaChallenge.from_discord_error(err)
                try:
                    solve_hcaptcha(new_challenge)
                except DeferredTextCaptcha as deferred:
                    pending2 = store_pending_request(
                        prompt=deferred.prompt,
                        kind=deferred.kind,
                        method=pending.method,
                        path=pending.path,
                        body=pending.body,
                        body_bytes=pending.body_bytes(),
                        token=pending.token,
                        params=pending.params,
                        extra_headers=pending.extra_headers,
                        broker_port=deferred.broker_port,
                        broker_pid=deferred.broker_pid,
                        challenge=new_challenge,
                    )
                    raise pending2
                # If the new challenge somehow auto-solved without text
                # deferral, fall back to the raw API retry path below.
            else:
                msg = solution.replay_text or f"HTTP {solution.replay_status}"
                raise RuntimeError(f"HTTP {solution.replay_status}: {msg}")

        result = api._request(
            pending.method,
            pending.path,
            body=pending.body,
            body_bytes=pending.body_bytes(),
            token=pending.token,
            params=pending.params,
            extra_headers=retry_headers,
            allow_captcha_retry=True,
        )
        delete_pending_request(challenge_id)
        return result
    finally:
        still_pending = _pending_path(challenge_id).exists()
        if pending.broker_pid and not still_pending:
            try:
                os.kill(pending.broker_pid, 15)
            except Exception:
                pass


def setup_accessibility_browser() -> None:
    """Open the persistent browser profile so the user can log into hCaptcha.

    This is a one-time/manual setup path for obtaining the accessibility cookie
    inside the Chromium profile used by solve_hcaptcha().
    """
    pw = None
    context = None
    try:
        pw, context = _launch_context(headed=True)
        page = context.new_page()
        page.goto(ACCESSIBILITY_URL, wait_until="domcontentloaded")
        print()
        print("discord-cli captcha setup")
        print("────────────────────────")
        print()
        print("A persistent Chromium profile has been opened for hCaptcha.")
        print("Complete the accessibility signup/login flow in that browser.")
        print("When you're done, come back here and press Enter.")
        print()
        input("Press Enter after the hCaptcha accessibility cookie is set... ")
    except EOFError:
        raise CaptchaError(
            "Setup requires an interactive terminal so you can confirm when login is complete."
        )
    finally:
        try:
            if context is not None:
                context.close()
        finally:
            if pw is not None:
                pw.stop()


def browser_status() -> dict:
    """Return a lightweight status summary for the persistent captcha profile."""
    rows = [
        (host, name, expires)
        for host, name, expires in _profile_cookie_rows()
        if 'hcaptcha' in host
    ]
    names = sorted({name for _host, name, _expires in rows})
    return {
        "profile_dir": str(BROWSER_PROFILE_DIR),
        "hcaptcha_cookie_count": len(rows),
        "hcaptcha_cookie_names": names,
        "has_accessibility_cookie": _profile_has_accessibility_cookie(),
    }
