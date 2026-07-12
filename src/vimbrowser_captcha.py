"""Minimal manual Discord hCaptcha bridge using a running vimbrowser's IPC.

No browser runtime or automation package is bundled here.  A captcha is rendered
in an account-specific persistent vimbrowser request context, keeping its cookies
and storage separate from the user's normal browser tabs and Discord login.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import sys
import time
import urllib.parse

from src.accounts import selected_alias


DEFAULT_TIMEOUT_SECS = 300
_REQUIRED_IPC_COMMAND = "open-context-tab"


class VimbrowserCaptchaError(RuntimeError):
    pass


class VimbrowserUnavailable(VimbrowserCaptchaError):
    pass


@dataclass(frozen=True)
class CaptchaChallenge:
    sitekey: str
    rqdata: str | None
    invisible: bool

    @classmethod
    def from_payload(cls, payload: dict) -> "CaptchaChallenge":
        service = str(payload.get("captcha_service") or "hcaptcha")
        if service != "hcaptcha":
            raise VimbrowserCaptchaError(
                f"unsupported Discord captcha service {service!r}"
            )
        sitekey = str(payload.get("captcha_sitekey") or "").strip()
        if not sitekey:
            raise VimbrowserCaptchaError(
                "Discord's captcha response did not include a site key"
            )
        return cls(
            sitekey=sitekey,
            rqdata=payload.get("captcha_rqdata") or None,
            invisible=bool(payload.get("should_serve_invisible")),
        )


@dataclass(frozen=True)
class BrowserReplayResult:
    status: int
    text: str
    body: object | None


def captcha_reason(payload: dict) -> str:
    value = payload.get("captcha_key")
    if isinstance(value, list):
        text = "; ".join(str(item).strip() for item in value if str(item).strip())
    else:
        text = str(value or "").strip()
    return text or "captcha verification is required"


def _candidate_socket_paths() -> list[Path]:
    if value := os.environ.get("DISCORD_VIMBROWSER_IPC", "").strip():
        return [Path(value).expanduser()]
    if value := os.environ.get("VIMBROWSER_IPC", "").strip():
        return [Path(value).expanduser()]
    if value := os.environ.get("DISCORD_VIMBROWSER_PROFILE_DIR", "").strip():
        return [Path(value).expanduser() / "ipc.sock"]
    if value := os.environ.get("VIMBROWSER_PROFILE_DIR", "").strip():
        return [Path(value).expanduser() / "ipc.sock"]

    paths = [Path.home() / ".runtime" / "vimbrowser-yeyito" / "ipc.sock"]
    if value := os.environ.get("XDG_STATE_HOME", "").strip():
        paths.append(Path(value).expanduser() / "vimbrowser" / "ipc.sock")
    paths.extend([
        Path.home() / ".local" / "state" / "vimbrowser" / "ipc.sock",
        Path("/tmp/vimbrowser/ipc.sock"),
    ])
    result = []
    seen = set()
    for path in paths:
        key = str(path)
        if key not in seen:
            result.append(path)
            seen.add(key)
    return result


class VimbrowserIpc:
    def __init__(self, socket_path: Path):
        self.socket_path = socket_path

    @classmethod
    def detect(cls) -> "VimbrowserIpc":
        errors = []
        for path in _candidate_socket_paths():
            try:
                mode = path.stat().st_mode
            except OSError:
                continue
            if not stat.S_ISSOCK(mode):
                continue
            client = cls(path)
            try:
                protocol = client.send_json("protocol", timeout=2)
                if protocol.get("protocol") != "vimbrowser-ipc":
                    errors.append(f"{path}: incompatible IPC protocol")
                    continue
                return client
            except VimbrowserCaptchaError as e:
                errors.append(str(e))

        detail = f" ({errors[-1]})" if errors else ""
        raise VimbrowserUnavailable(
            "vimbrowser could not be found or reached; it may not be installed "
            f"or may not be open. Start vimbrowser and retry{detail}"
        )

    def send(self, command: str, *, timeout: float = 15) -> str:
        if "\n" in command or "\r" in command:
            raise VimbrowserCaptchaError("refusing a multiline IPC command")
        path = str(self.socket_path)
        if len(path.encode()) >= 108:
            raise VimbrowserCaptchaError(f"vimbrowser IPC socket path is too long: {path}")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect(path)
                sock.sendall((command + "\n").encode())
                chunks = []
                while True:
                    chunk = sock.recv(262144)
                    if not chunk:
                        break
                    chunks.append(chunk)
        except (FileNotFoundError, ConnectionRefusedError, socket.timeout, OSError) as e:
            raise VimbrowserUnavailable(
                f"vimbrowser IPC at {path} is not reachable; is vimbrowser open? ({e})"
            ) from e
        response = b"".join(chunks).decode("utf-8", errors="replace").strip()
        if response.startswith("ERR "):
            raise VimbrowserCaptchaError(response[4:])
        if not response:
            raise VimbrowserCaptchaError("vimbrowser returned an empty IPC response")
        return response

    def send_json(self, command: str, *, timeout: float = 15) -> dict:
        response = self.send(command, timeout=timeout)
        try:
            value = json.loads(response)
        except json.JSONDecodeError as e:
            raise VimbrowserCaptchaError(
                f"vimbrowser returned invalid JSON for {command.split()[0]!r}"
            ) from e
        if not isinstance(value, dict):
            raise VimbrowserCaptchaError("vimbrowser returned a non-object JSON response")
        return value

    def require_context_tabs(self) -> None:
        payload = self.send_json("commands")
        names = {
            item.get("name")
            for item in payload.get("commands", [])
            if isinstance(item, dict)
        }
        if _REQUIRED_IPC_COMMAND not in names:
            raise VimbrowserUnavailable(
                "the open vimbrowser does not support isolated context tabs; "
                "update and restart vimbrowser, then retry"
            )

    def open_context_tab(self, context: str, url: str) -> int:
        payload = self.send_json(f"open-context-tab {context} {url}")
        tab_id = payload.get("active_tabid")
        if not isinstance(tab_id, int) or tab_id <= 0:
            raise VimbrowserCaptchaError("vimbrowser did not return the new tab ID")
        return tab_id

    def network_clear(self, tab_id: int) -> None:
        self.send_json(f"network {tab_id} clear")

    def network_list(self, tab_id: int) -> list[dict]:
        payload = self.send_json(f"network {tab_id} list")
        requests = payload.get("requests", [])
        return [item for item in requests if isinstance(item, dict)]

    def network_body(self, tab_id: int, request_id: int) -> str:
        return self.send(f"network {tab_id} body {request_id}")

    def eval(self, tab_id: int, script: str):
        compact = " ".join(line.strip() for line in script.splitlines())
        payload = self.send_json(f"js {tab_id} {compact}")
        if not payload.get("ok"):
            raise VimbrowserCaptchaError(
                f"JavaScript failed in vimbrowser tab {tab_id}: "
                f"{payload.get('error') or 'unknown error'}"
            )
        return payload.get("result")

    def close_tab(self, tab_id: int) -> None:
        self.send_json(f"tab-delete {tab_id}")


def _context_name() -> str:
    alias = selected_alias(required=False) or "legacy"
    normalized = re.sub(r"[^a-z0-9_-]", "-", alias.lower()).strip("-_") or "account"
    candidate = f"discord-{normalized}"
    if len(candidate) <= 48:
        return candidate
    digest = hashlib.sha256(alias.encode()).hexdigest()[:8]
    return f"discord-{normalized[:31]}-{digest}"


def _timeout_seconds(value: int | None) -> int:
    if value is not None:
        return max(1, int(value))
    raw = os.environ.get("DISCORD_VIMBROWSER_CAPTCHA_TIMEOUT", "").strip()
    if not raw:
        return DEFAULT_TIMEOUT_SECS
    try:
        return max(1, int(raw))
    except ValueError as e:
        raise VimbrowserCaptchaError(
            "DISCORD_VIMBROWSER_CAPTCHA_TIMEOUT must be an integer"
        ) from e


def _wait_for_discord_page(ipc: VimbrowserIpc, tab_id: int, deadline: float) -> None:
    last_error = None
    while time.monotonic() < deadline:
        try:
            raw = ipc.eval(
                tab_id,
                "JSON.stringify({origin:location.origin,ready:document.readyState})",
            )
            state = json.loads(raw) if isinstance(raw, str) else {}
            if state.get("origin") == "https://discord.com" and state.get("ready") in {
                "interactive", "complete",
            }:
                return
        except (VimbrowserCaptchaError, json.JSONDecodeError) as e:
            last_error = e
        time.sleep(0.25)
    suffix = f": {last_error}" if last_error else ""
    raise VimbrowserCaptchaError(f"timed out loading the Discord captcha page{suffix}")


def _invite_bootstrap_script(token: str, code: str) -> str:
    token_json = json.dumps(token)
    invite_url_json = json.dumps(f"https://discord.com/invite/{code}")
    return f"""
    (() => {{
      const authToken = {token_json};
      const frame = document.createElement('iframe');
      frame.style.display = 'none';
      document.body.appendChild(frame);
      frame.contentWindow.localStorage.setItem('token', JSON.stringify(authToken));
      frame.remove();
      location.href = {invite_url_json};
      return 'opening invite';
    }})()
    """


def _invite_page_state_script(code: str) -> str:
    code_json = json.dumps(code.lower())
    return f"""
    (() => {{
      const code = {code_json};
      const buttons = [...document.querySelectorAll('button')];
      const action = buttons.find(button => {{
        const text = (button.innerText || button.textContent || '').trim().toLowerCase();
        return text === 'accept invite' || text === 'join server' || text === 'join';
      }});
      return JSON.stringify({{
        url: location.href,
        ready: document.readyState,
        has_code: location.href.toLowerCase().includes(code),
        has_action: !!action,
        body: (document.body?.innerText || '').slice(0, 500)
      }});
    }})()
    """


def _click_invite_action_script() -> str:
    return """
    (() => {
      const buttons = [...document.querySelectorAll('button')];
      const action = buttons.find(button => {
        const text = (button.innerText || button.textContent || '').trim().toLowerCase();
        return text === 'accept invite' || text === 'join server' || text === 'join';
      });
      if (!action) return false;
      action.click();
      return true;
    })()
    """


def _wait_for_invite_action(ipc: VimbrowserIpc, tab_id: int, code: str,
                            deadline: float) -> None:
    last_state = {}
    while time.monotonic() < deadline:
        try:
            raw = ipc.eval(tab_id, _invite_page_state_script(code))
            last_state = json.loads(raw) if isinstance(raw, str) else {}
            if last_state.get("has_code") and last_state.get("has_action"):
                return
        except (VimbrowserCaptchaError, json.JSONDecodeError):
            pass
        time.sleep(0.25)
    body = str(last_state.get("body") or "").replace("\n", " ")[:240]
    raise VimbrowserCaptchaError(
        f"Discord's invite page did not show an Accept Invite action. Page text: {body}"
    )


def _invite_network_result(ipc: VimbrowserIpc, tab_id: int, code: str,
                           deadline: float, *, expected_guild_id: str | None = None,
                           expected_guild_name: str | None = None,
                           membership_check=None) -> BrowserReplayResult:
    expected = f"/api/v9/invites/{code}"
    seen_failures = set()
    next_membership_check = 0.0
    while time.monotonic() < deadline:
        for request in reversed(ipc.network_list(tab_id)):
            if request.get("method") != "POST" or expected not in str(request.get("url") or ""):
                continue
            if not request.get("complete"):
                continue
            request_id = int(request.get("id") or 0)
            status = int(request.get("status") or 0)
            if request_id <= 0:
                continue
            text = ipc.network_body(tab_id, request_id)
            try:
                body = json.loads(text) if text else None
            except json.JSONDecodeError:
                body = text
            if 200 <= status < 300:
                return BrowserReplayResult(status=status, text=text, body=body)
            if status == 400 and isinstance(body, dict) and "captcha_key" in body:
                seen_failures.add(request_id)
                continue
            if request_id not in seen_failures:
                return BrowserReplayResult(status=status, text=text, body=body)

        now = time.monotonic()
        if membership_check is not None and now >= next_membership_check:
            next_membership_check = now + 1.0
            try:
                guild = membership_check()
            except RuntimeError:
                guild = None
            if guild:
                guild_data = {
                    "id": str(guild.get("id") or expected_guild_id or ""),
                    "name": str(guild.get("name") or expected_guild_name or "Unknown"),
                }
                body = {"guild": guild_data}
                return BrowserReplayResult(
                    status=200,
                    text=json.dumps(body, separators=(",", ":")),
                    body=body,
                )
        time.sleep(0.5)
    raise VimbrowserCaptchaError(
        "timed out waiting for Discord web to finish the invite join"
    )


def complete_invite(code: str, token: str, *, timeout_secs: int | None = None,
                    ipc: VimbrowserIpc | None = None,
                    expected_guild_id: str | None = None,
                    expected_guild_name: str | None = None,
                    membership_check=None) -> BrowserReplayResult:
    """Join an invite through Discord's real web UI in an isolated context."""
    ipc = ipc or VimbrowserIpc.detect()
    ipc.require_context_tabs()
    timeout = _timeout_seconds(timeout_secs)
    context = _context_name()
    tab_id = None
    try:
        tab_id = ipc.open_context_tab(context, "https://discord.com/login")
        print(
            f"Discord invite opened in vimbrowser tab {tab_id} "
            f"(isolated context: {context}). Complete any captcha to continue.",
            file=sys.stderr,
            flush=True,
        )
        deadline = time.monotonic() + timeout
        _wait_for_discord_page(ipc, tab_id, min(deadline, time.monotonic() + 30))
        ipc.eval(tab_id, _invite_bootstrap_script(token, code))
        _wait_for_invite_action(ipc, tab_id, code, min(deadline, time.monotonic() + 45))
        ipc.network_clear(tab_id)
        clicked = ipc.eval(tab_id, _click_invite_action_script())
        if clicked is not True:
            raise VimbrowserCaptchaError("Discord's Accept Invite button disappeared")
        return _invite_network_result(
            ipc,
            tab_id,
            code,
            deadline,
            expected_guild_id=expected_guild_id,
            expected_guild_name=expected_guild_name,
            membership_check=membership_check,
        )
    finally:
        if tab_id is not None:
            try:
                ipc.close_tab(tab_id)
            except VimbrowserCaptchaError:
                pass


def _bootstrap_script(challenge: CaptchaChallenge) -> str:
    challenge_json = json.dumps({
        "sitekey": challenge.sitekey,
        "rqdata": challenge.rqdata,
        "invisible": challenge.invisible,
    }, separators=(",", ":"))
    return f"""
    (() => {{
      const challenge = {challenge_json};
      document.title = 'Discord captcha';
      document.head.innerHTML = '';
      document.body.innerHTML = '';
      Object.assign(document.body.style, {{margin:'0',padding:'32px',background:'#111827',color:'#e5e7eb',fontFamily:'system-ui,sans-serif'}});
      const heading = document.createElement('h1');
      heading.textContent = 'Discord verification required';
      heading.style.fontSize = '24px';
      document.body.appendChild(heading);
      const note = document.createElement('p');
      note.textContent = 'Complete the captcha below. This tab uses an isolated Discord CLI browser context.';
      document.body.appendChild(note);
      const status = document.createElement('p');
      status.id = 'discord-cli-captcha-status';
      status.textContent = 'Loading hCaptcha...';
      document.body.appendChild(status);
      const container = document.createElement('div');
      container.id = 'discord-cli-captcha';
      document.body.appendChild(container);
      window.__discordCliCaptcha = {{status:'loading'}};
      const setStatus = (value, text) => {{
        window.__discordCliCaptcha = value;
        status.textContent = text;
      }};
      const script = document.createElement('script');
      script.src = 'https://js.hcaptcha.com/1/api.js?render=explicit';
      script.async = true;
      script.defer = true;
      script.onload = () => {{
        try {{
          const options = {{
            sitekey: challenge.sitekey,
            size: challenge.invisible ? 'invisible' : 'normal',
            callback: token => setStatus({{status:'solved',token}}, 'Captcha complete. Retrying Discord...'),
            'error-callback': error => setStatus({{status:'error',error:String(error)}}, 'Captcha error: ' + String(error)),
            'expired-callback': () => setStatus({{status:'expired'}}, 'Captcha expired. Retry the Discord command.'),
            'chalexpired-callback': () => setStatus({{status:'expired'}}, 'Captcha expired. Retry the Discord command.')
          }};
          if (challenge.rqdata) options.rqdata = challenge.rqdata;
          const widget = hcaptcha.render(container, options);
          setStatus({{status:'pending'}}, 'Complete the hCaptcha challenge.');
          hcaptcha.execute(widget);
        }} catch (error) {{
          setStatus({{status:'error',error:String(error)}}, 'Captcha error: ' + String(error));
        }}
      }};
      script.onerror = () => setStatus({{status:'error',error:'script load failed'}}, 'Failed to load hCaptcha.');
      document.head.appendChild(script);
      return 'started';
    }})()
    """


def _browser_replay_script(token: str, request: dict) -> str:
    params = request.get("params")
    path = str(request["path"])
    if params:
        path += "?" + urllib.parse.urlencode(params, doseq=True)
    url = f"https://discord.com/api/v9{path}"

    headers = {}
    forbidden = {
        "connection", "content-length", "cookie", "host", "origin",
        "priority", "referer", "user-agent",
    }
    for name, value in (request.get("headers") or {}).items():
        lowered = str(name).lower()
        if lowered in forbidden or lowered.startswith("sec-"):
            continue
        headers[str(name)] = str(value)
    headers["X-Captcha-Key"] = token

    body_bytes = request.get("body_bytes")
    if body_bytes is not None and len(body_bytes) > 700_000:
        raise VimbrowserCaptchaError(
            "captcha replay with an attachment larger than 700 KB is not supported"
        )
    payload = {
        "url": url,
        "method": request["method"],
        "headers": headers,
        "json_body": request.get("body"),
        "body_b64": base64.b64encode(body_bytes).decode() if body_bytes is not None else None,
    }
    request_json = json.dumps(payload, separators=(",", ":"))
    return f"""
    (() => {{
      const request = {request_json};
      window.__discordCliReplay = {{status:'pending'}};
      (async () => {{
        try {{
          const options = {{method:request.method,headers:request.headers,credentials:'include'}};
          if (request.body_b64 !== null) {{
            const raw = atob(request.body_b64);
            options.body = Uint8Array.from(raw, character => character.charCodeAt(0));
          }} else if (request.json_body !== null) {{
            options.body = JSON.stringify(request.json_body);
          }}
          const response = await fetch(request.url, options);
          const text = await response.text();
          window.__discordCliReplay = {{status:'complete',http_status:response.status,text}};
        }} catch (error) {{
          window.__discordCliReplay = {{status:'error',error:String(error)}};
        }}
      }})();
      return 'started';
    }})()
    """


def _replay_in_browser(ipc: VimbrowserIpc, tab_id: int, token: str,
                       request: dict, deadline: float) -> BrowserReplayResult:
    ipc.eval(tab_id, _browser_replay_script(token, request))
    while time.monotonic() < deadline:
        raw = ipc.eval(tab_id, "JSON.stringify(window.__discordCliReplay || null)")
        try:
            state = json.loads(raw) if isinstance(raw, str) else {}
        except json.JSONDecodeError as e:
            raise VimbrowserCaptchaError(
                "vimbrowser returned invalid Discord replay state"
            ) from e
        if state.get("status") == "error":
            raise VimbrowserCaptchaError(
                f"Discord request replay failed in vimbrowser: "
                f"{state.get('error') or 'unknown error'}"
            )
        if state.get("status") == "complete":
            text = str(state.get("text") or "")
            body = None
            if text:
                try:
                    body = json.loads(text)
                except json.JSONDecodeError:
                    body = text
            return BrowserReplayResult(
                status=int(state.get("http_status") or 0),
                text=text,
                body=body,
            )
        time.sleep(0.25)
    raise VimbrowserCaptchaError("timed out waiting for vimbrowser to replay the request")


def solve_captcha(payload: dict, *, timeout_secs: int | None = None,
                  ipc: VimbrowserIpc | None = None,
                  replay_request: dict | None = None) -> str | BrowserReplayResult:
    """Render a challenge and optionally replay the request in the same tab."""
    challenge = CaptchaChallenge.from_payload(payload)
    ipc = ipc or VimbrowserIpc.detect()
    ipc.require_context_tabs()
    timeout = _timeout_seconds(timeout_secs)
    context = _context_name()
    tab_id = None
    try:
        tab_id = ipc.open_context_tab(context, "https://discord.com/login")
        print(
            f"Discord captcha opened in vimbrowser tab {tab_id} "
            f"(isolated context: {context}). Complete it to continue.",
            file=sys.stderr,
            flush=True,
        )
        deadline = time.monotonic() + timeout
        _wait_for_discord_page(ipc, tab_id, min(deadline, time.monotonic() + 30))
        ipc.eval(tab_id, _bootstrap_script(challenge))

        while time.monotonic() < deadline:
            raw = ipc.eval(
                tab_id,
                "JSON.stringify(window.__discordCliCaptcha || null)",
            )
            try:
                state = json.loads(raw) if isinstance(raw, str) else {}
            except json.JSONDecodeError as e:
                raise VimbrowserCaptchaError(
                    "vimbrowser returned invalid captcha state"
                ) from e
            status_value = state.get("status")
            if status_value == "solved" and state.get("token"):
                token = str(state["token"])
                if replay_request is None:
                    return token
                return _replay_in_browser(
                    ipc,
                    tab_id,
                    token,
                    replay_request,
                    deadline,
                )
            if status_value == "error":
                raise VimbrowserCaptchaError(
                    f"hCaptcha failed: {state.get('error') or 'unknown error'}"
                )
            if status_value == "expired":
                raise VimbrowserCaptchaError("hCaptcha expired before it was submitted")
            time.sleep(0.5)
        raise VimbrowserCaptchaError(
            f"timed out after {timeout} seconds waiting for the captcha"
        )
    finally:
        if tab_id is not None:
            try:
                ipc.close_tab(tab_id)
            except VimbrowserCaptchaError:
                pass
