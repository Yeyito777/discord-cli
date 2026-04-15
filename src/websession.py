"""Browser-native Discord web actions.

This module builds on `src.webprofile` and focuses on:
- interacting with the live Discord web UI
- detecting/solving visible in-session hCaptcha challenges
- performing high-risk actions like DM send and invite join
"""

from __future__ import annotations

import base64
import json
import re
import time
import urllib.request
from dataclasses import dataclass
from urllib.parse import urlencode

from src import api
from src.webprofile import (
    API_BASE,
    COMPOSER_SELECTOR,
    DiscordWebError,
    extract_invite_code,
    fetch_fingerprint,
    logged_in_js,
    open_dm,
    open_invite,
)


@dataclass(frozen=True)
class BrowserFetchResult:
    status: int
    text: str
    headers: dict[str, str]

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def json_or_none(self):
        if not self.text:
            return None
        try:
            return json.loads(self.text)
        except Exception:
            return None


def _body_text(page, *, timeout_ms: int = 1500) -> str:
    try:
        return page.locator('body').inner_text(timeout=timeout_ms)
    except Exception:
        return ''


def _shorten(value: str | None, limit: int = 500) -> str:
    if value is None:
        return ''
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + '...'


def _trace_event(trace, event: str, *, page=None, screenshot: bool = False, **fields) -> None:
    if trace is None:
        return
    try:
        trace(event, page=page, screenshot=screenshot, **fields)
    except Exception:
        pass


def _wait_for_body_contains(page, text: str, *, timeout_ms: int = 15_000) -> None:
    deadline = time.time() + (timeout_ms / 1000.0)
    while time.time() < deadline:
        if text in _body_text(page):
            return
        time.sleep(0.25)
    raise DiscordWebError(
        "Timed out waiting for the message text to appear in the Discord UI."
    )


def _composer_text(page) -> str:
    try:
        return page.evaluate(
            f"""() => {{
                const el = document.querySelector({json.dumps(COMPOSER_SELECTOR)});
                return el ? (el.innerText || '').trim() : '';
            }}"""
        ) or ""
    except Exception:
        return ""


def composer_locator(page):
    return page.locator(COMPOSER_SELECTOR).first


def set_composer_text(page, text: str, *, char_delay_ms: int = 35) -> None:
    """Type text into Discord's slate composer."""
    box = composer_locator(page)
    box.click()
    try:
        box.press("Control+A")
        box.press("Backspace")
    except Exception:
        pass
    page.keyboard.type(text, delay=char_delay_ms)


def submit_current_message(page) -> None:
    page.keyboard.press("Enter")


def send_message_ui(page, channel_id: str, text: str) -> None:
    open_dm(page, channel_id)
    set_composer_text(page, text)
    submit_current_message(page)


def has_hcaptcha(page) -> bool:
    """Heuristic for whether a *visible* hCaptcha challenge is active."""
    try:
        from src.captcha import _find_accessibility_prompt

        _frame, prompt = _find_accessibility_prompt(page)
        if prompt is not None:
            return True
    except Exception:
        pass

    markers = (
        'soy humano',
        'i am human',
        'inténtalo de nuevo',
        'intentalo de nuevo',
        'try again',
        'verify',
        'verificar',
        'omitir',
        'omit',
    )
    for frame in page.frames:
        try:
            url = frame.url or ''
        except Exception:
            url = ''
        if 'hcaptcha' not in url and 'newassets.hcaptcha.com' not in url:
            continue
        try:
            txt = frame.locator('body').inner_text(timeout=700).lower()
        except Exception:
            continue
        if any(marker in txt for marker in markers):
            return True
    return False


def _advance_visible_hcaptcha(page, *, answer: str | None = None,
                              expected_prompt: str | None = None,
                              timeout_secs: int = 60, trace=None) -> dict:
    """Advance the visible hCaptcha flow until it clears or needs another answer."""
    from src.captcha import (
        _find_accessibility_prompt,
        _nudge_checkbox_frame,
        _submit_accessibility_prompt,
    )

    if not has_hcaptcha(page):
        _trace_event(trace, 'captcha_absent', page=page)
        return {"status": "no_captcha"}

    deadline = time.time() + timeout_secs
    submitted_answer = False
    last_prompt_after_submit = None
    submit_started_at = None
    clear_since = None
    last_checkbox_nudge_at = None

    while time.time() < deadline:
        frame, prompt = _find_accessibility_prompt(page)
        if prompt is not None:
            if prompt.kind != 'text':
                _trace_event(
                    trace,
                    'captcha_interactive',
                    page=page,
                    screenshot=True,
                    prompt=prompt.instruction,
                )
                raise DiscordWebError(
                    f"Interactive hCaptcha challenge surfaced in browser session: {prompt.instruction}"
                )
            if answer is None and not submitted_answer:
                _trace_event(
                    trace,
                    'captcha_prompt',
                    page=page,
                    prompt=prompt.instruction,
                    kind=prompt.kind,
                )
                return {
                    "status": "captcha_required",
                    "kind": "text",
                    "prompt": prompt.instruction,
                }
            if answer is not None and not submitted_answer:
                if expected_prompt is not None and prompt.instruction != expected_prompt:
                    _trace_event(
                        trace,
                        'captcha_prompt_mismatch',
                        page=page,
                        expected_prompt=expected_prompt,
                        actual_prompt=prompt.instruction,
                    )
                    return {
                        "status": "captcha_required",
                        "kind": "text",
                        "prompt": prompt.instruction,
                    }
                _trace_event(
                    trace,
                    'captcha_submit',
                    page=page,
                    prompt=prompt.instruction,
                    answer=answer,
                )
                _submit_accessibility_prompt(frame, answer)
                submitted_answer = True
                last_prompt_after_submit = prompt.instruction
                submit_started_at = time.time()
                answer = None
                clear_since = None
                time.sleep(1.5)
                continue
            if submitted_answer and prompt.instruction != last_prompt_after_submit:
                _trace_event(
                    trace,
                    'captcha_next_prompt',
                    page=page,
                    prompt=prompt.instruction,
                    kind=prompt.kind,
                )
                return {
                    "status": "captcha_required",
                    "kind": "text",
                    "prompt": prompt.instruction,
                }
            if submitted_answer and submit_started_at is not None and time.time() - submit_started_at > 8:
                _trace_event(
                    trace,
                    'captcha_prompt_persisted',
                    page=page,
                    prompt=prompt.instruction,
                )
                return {
                    "status": "captcha_required",
                    "kind": "text",
                    "prompt": prompt.instruction,
                }

        if submitted_answer:
            if has_hcaptcha(page):
                clear_since = None
                now = time.time()
                if last_checkbox_nudge_at is None or (now - last_checkbox_nudge_at) >= 3.0:
                    if _nudge_checkbox_frame(page):
                        last_checkbox_nudge_at = now
                        _trace_event(trace, 'captcha_checkbox_nudge', page=page)
                        time.sleep(2.5)
                        continue
                time.sleep(0.5)
                continue
            if clear_since is None:
                clear_since = time.time()
            elif time.time() - clear_since >= 2.0:
                _trace_event(trace, 'captcha_cleared', page=page)
                return {"status": "captcha_cleared"}
        else:
            if not has_hcaptcha(page):
                _trace_event(trace, 'captcha_disappeared_before_answer', page=page)
                return {"status": "no_captcha"}
            now = time.time()
            if last_checkbox_nudge_at is None or (now - last_checkbox_nudge_at) >= 3.0:
                if _nudge_checkbox_frame(page):
                    last_checkbox_nudge_at = now
                    _trace_event(trace, 'captcha_checkbox_nudge', page=page)
                    time.sleep(2.5)
                    continue
        time.sleep(0.3)

    _trace_event(trace, 'captcha_timeout', page=page, screenshot=True)
    raise DiscordWebError("Timed out while advancing in-session hCaptcha challenge.")


def send_dm_with_captcha(page, channel_id: str, text: str, *, timeout_secs: int = 180,
                         trace=None) -> dict:
    """Send a DM through the live Discord web UI and auto-handle text captcha."""
    _trace_event(trace, 'dm_send_start', page=page, channel_id=channel_id, text=text)
    send_message_ui(page, channel_id, text)
    return continue_send_dm_with_captcha(
        page,
        channel_id,
        text,
        timeout_secs=timeout_secs,
        trace=trace,
    )


def continue_send_dm_with_captcha(page, channel_id: str, text: str, *, answer: str | None = None,
                                  timeout_secs: int = 180, saw_captcha: bool = False,
                                  trace=None) -> dict:
    deadline = time.time() + timeout_secs
    pending_answer = answer
    last_state_sig = None
    while time.time() < deadline:
        if has_hcaptcha(page):
            saw_captcha = True
            step = _advance_visible_hcaptcha(
                page,
                answer=pending_answer,
                expected_prompt=None,
                timeout_secs=max(10, int(deadline - time.time())),
                trace=trace,
            )
            pending_answer = None
            if step.get("status") == "captcha_required":
                _trace_event(trace, 'dm_captcha_required', page=page, prompt=step.get('prompt'))
                return {
                    **step,
                    "captcha": True,
                    "channel_id": channel_id,
                    "text": text,
                }
            if step.get("status") == "captcha_cleared":
                open_dm(page, channel_id)

        body_text = _body_text(page)
        state_sig = ((page.url or ''), body_text[:180])
        if state_sig != last_state_sig:
            _trace_event(trace, 'dm_state', page=page, body_text=_shorten(body_text, 320))
            last_state_sig = state_sig

        try:
            _wait_for_body_contains(page, text, timeout_ms=1_500)
            _trace_event(trace, 'dm_sent', page=page, text=text)
            return {
                'status': 'sent',
                'captcha': saw_captcha,
                'channel_id': channel_id,
                'text': text,
            }
        except DiscordWebError:
            pass
        time.sleep(0.5)

    _trace_event(trace, 'dm_timeout', page=page, screenshot=True)
    raise DiscordWebError("Timed out waiting for browser-native DM send outcome.")


def _wait_accept_invite_button(page, code: str, *, timeout_ms: int = 12_000) -> None:
    try:
        page.get_by_role('button', name='Accept Invite').wait_for(timeout=timeout_ms)
    except Exception as e:
        raise DiscordWebError(
            f'Invite page did not show an Accept Invite action for {code}. Body: {_body_text(page, timeout_ms=2_000)[:240]}'
        ) from e


def _invite_failure(body_text: str) -> bool:
    lowered = body_text.lower()
    return any(msg in lowered for msg in ['unable to accept invite', 'invite invalid', 'expired invite'])


def _invite_has_accept_action(body_text: str) -> bool:
    return 'Accept Invite' in body_text


def _retryable_invite_api_error(status: int, payload: object) -> bool:
    if status != 403 or not isinstance(payload, dict):
        return False
    return payload.get('code') == 10008 and payload.get('message') == 'Unknown Message'


def _click_accept_invite(page) -> None:
    page.get_by_role('button', name='Accept Invite').click()


def _invite_preview(code: str) -> dict:
    url = (
        f"https://discord.com/api/v9/invites/{code}"
        f"?with_counts=true&with_expiration=true"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _guild_membership_lookup(guild_id: str, guild_name: str) -> dict | None:
    guilds = api.get_guilds()
    for guild in guilds:
        if guild_id and str(guild.get('id') or '') == guild_id:
            return {'id': guild_id, 'name': guild.get('name') or guild_name}
        if guild_name and (guild.get('name') or '') == guild_name:
            return {'id': str(guild.get('id') or guild_id), 'name': guild_name}
    return None


def _current_channels_guild_id(current_url: str) -> str | None:
    m = re.search(r'/channels/([^/]+)', current_url or '')
    if not m:
        return None
    guild_id = m.group(1)
    if guild_id == '@me':
        return None
    return guild_id


def _payload_has_captcha(payload: object) -> bool:
    return isinstance(payload, dict) and 'captcha_key' in payload


def _summarize_payload(payload: object) -> object:
    if not isinstance(payload, dict):
        return payload
    summary = {}
    for key in ('message', 'code', 'captcha_service', 'captcha_sitekey', 'captcha_session_id', 'captcha_rqtoken'):
        if key in payload:
            summary[key] = payload.get(key)
    if 'captcha_key' in payload:
        summary['captcha_key'] = payload.get('captcha_key')
    if 'guild' in payload and isinstance(payload.get('guild'), dict):
        guild = payload['guild']
        summary['guild'] = {'id': guild.get('id'), 'name': guild.get('name')}
    if not summary:
        summary['keys'] = sorted(payload.keys())[:20]
    return summary


def _invite_error_message(status: int, payload: object, response_text: str) -> str:
    if isinstance(payload, dict):
        message = payload.get('message')
        code = payload.get('code')
        if message and code is not None:
            return f'{message} (code {code})'
        if message:
            return str(message)
    return f'HTTP {status}: {_shorten(response_text, 220)}'


def join_invite_with_captcha(page, invite: str, *, timeout_secs: int = 180, trace=None) -> dict:
    """Join a server from a real Discord invite page, auto-handling visible text hCaptcha."""
    return continue_join_invite_with_captcha(
        page,
        invite,
        timeout_secs=timeout_secs,
        trace=trace,
        open_invite_page=True,
        click_accept=True,
    )


def continue_join_invite_with_captcha(page, invite: str, *, answer: str | None = None,
                                      expected_prompt: str | None = None,
                                      timeout_secs: int = 180, saw_captcha: bool = False,
                                      reclick_after_captcha: bool = False,
                                      open_invite_page: bool = False,
                                      click_accept: bool = False,
                                      trace=None) -> dict:
    code = extract_invite_code(invite)
    fingerprint = fetch_fingerprint()
    route_registered = False
    preview = {}
    preview_guild_id = ''
    preview_guild_name = ''
    try:
        preview = _invite_preview(code)
        preview_guild = preview.get('guild') or {}
        preview_guild_id = str(preview_guild.get('id') or '')
        preview_guild_name = preview_guild.get('name') or ''
    except Exception as e:
        _trace_event(trace, 'invite_preview_error', invite=code, error=str(e))

    _trace_event(
        trace,
        'invite_join_start',
        page=page,
        invite=code,
        preview_guild_id=preview_guild_id,
        preview_guild_name=preview_guild_name,
        reclick_after_captcha=reclick_after_captcha,
    )

    invite_events: list[dict] = []
    invite_seq = 0
    last_handled_post_seq = 0
    last_state_sig = None
    next_membership_check_at = 0.0
    failure_page_first_seen_at = None
    failure_page_retry_count = 0

    def _invite_route(route):
        route.continue_()

    def _record_invite_response(response):
        nonlocal invite_seq
        try:
            url = response.url or ''
            if '/api/v9/invites/' not in url:
                return
            req = response.request
            headers = req.headers or {}
            invite_seq += 1
            try:
                response_text = response.text()
            except Exception as e:
                response_text = f'<response text unavailable: {e}>'
            try:
                payload = json.loads(response_text)
            except Exception:
                payload = None
            event = {
                'seq': invite_seq,
                'url': url,
                'method': req.method,
                'status': response.status,
                'has_authorization': any(k.lower() == 'authorization' for k in headers),
                'has_cookie': any(k.lower() == 'cookie' for k in headers),
                'request_headers': {
                    k: v for k, v in headers.items()
                    if k.lower() in {
                        'x-captcha-key',
                        'x-captcha-rqtoken',
                        'x-captcha-session-id',
                        'x-fingerprint',
                        'x-super-properties',
                        'x-context-properties',
                    }
                },
                'request_post_data': _shorten(req.post_data or '', 800),
                'response_text': _shorten(response_text, 1200),
                'payload': payload,
            }
            invite_events.append(event)
            _trace_event(
                trace,
                'invite_api_response',
                page=page,
                seq=event['seq'],
                method=event['method'],
                status=event['status'],
                url=url,
                has_authorization=event['has_authorization'],
                has_cookie=event['has_cookie'],
                request_headers=event['request_headers'],
                request_post_data=event['request_post_data'],
                payload=_summarize_payload(payload),
                response_text=event['response_text'],
            )
        except Exception as e:
            _trace_event(trace, 'invite_api_trace_error', error=str(e))

    if fingerprint:
        page.route('**/api/v9/invites/*', _invite_route)
        route_registered = True
    page.on('response', _record_invite_response)

    try:
        if open_invite_page:
            open_invite(page, code)
            _trace_event(trace, 'invite_opened', page=page, invite=code)
            _wait_accept_invite_button(page, code)

        if click_accept:
            # Let the invite page hydrate a bit before clicking. In live testing,
            # clicking the moment the button appears could fall into an immediate
            # failure/unauthorized branch, while a short settle delay produced the
            # normal captcha flow.
            time.sleep(1.5)
            _trace_event(trace, 'invite_accept_click_settle', page=page, invite=code, delay_ms=1500)
            _click_accept_invite(page)
            _trace_event(trace, 'invite_accept_clicked', page=page, invite=code)

        deadline = time.time() + timeout_secs
        pending_answer = answer
        resume_reopen_started_at = None
        last_resume_wait_bucket = None
        while time.time() < deadline:
            if pending_answer is not None and not has_hcaptcha(page):
                current_url = page.url or ''
                now = time.time()
                if '/invite/' not in current_url and (
                    resume_reopen_started_at is None or (now - resume_reopen_started_at) >= 6.0
                ):
                    _trace_event(
                        trace,
                        'invite_resume_reopen',
                        page=page,
                        expected_prompt=expected_prompt,
                        current_url=current_url,
                    )
                    try:
                        open_invite(page, code)
                        _wait_accept_invite_button(page, code, timeout_ms=8_000)
                        _click_accept_invite(page)
                        resume_reopen_started_at = time.time()
                        time.sleep(1.25)
                    except Exception as e:
                        _trace_event(trace, 'invite_resume_reopen_error', page=page, error=str(e), screenshot=True)
                if pending_answer is not None and not has_hcaptcha(page):
                    body_text = _body_text(page)
                    wait_bucket = int((time.time() - (resume_reopen_started_at or now)) // 2)
                    if wait_bucket != last_resume_wait_bucket:
                        _trace_event(
                            trace,
                            'invite_resume_waiting',
                            page=page,
                            expected_prompt=expected_prompt,
                            body_text=_shorten(body_text, 240),
                        )
                        last_resume_wait_bucket = wait_bucket

            if has_hcaptcha(page):
                saw_captcha = True
                step = _advance_visible_hcaptcha(
                    page,
                    answer=pending_answer,
                    expected_prompt=expected_prompt,
                    timeout_secs=max(10, int(deadline - time.time())),
                    trace=trace,
                )
                pending_answer = None
                expected_prompt = None
                if step.get("status") == "captcha_required":
                    _trace_event(trace, 'invite_captcha_required', page=page, prompt=step.get('prompt'))
                    return {
                        **step,
                        "captcha": True,
                        "invite": code,
                        "reclick_after_captcha": reclick_after_captcha,
                    }
                if step.get("status") == "captcha_cleared":
                    reclick_after_captcha = True
                    _trace_event(trace, 'invite_captcha_cleared', page=page)

            body_text = _body_text(page)
            current_url = page.url or ''
            state_sig = (
                current_url,
                body_text[:220],
                reclick_after_captcha,
                len(invite_events),
            )
            if state_sig != last_state_sig:
                _trace_event(
                    trace,
                    'invite_state',
                    page=page,
                    url=current_url,
                    body_text=_shorten(body_text, 500),
                    reclick_after_captcha=reclick_after_captcha,
                    saw_captcha=saw_captcha,
                )
                last_state_sig = state_sig

            latest_post = None
            latest_post_status = None
            latest_post_payload = None
            latest_post_message = None
            for event in reversed(invite_events):
                if event.get('method') == 'POST':
                    latest_post = event
                    break
            if latest_post is not None and latest_post['seq'] > last_handled_post_seq:
                last_handled_post_seq = latest_post['seq']
                latest_post_payload = latest_post.get('payload')
                latest_post_status = int(latest_post.get('status') or 0)
                latest_post_message = _invite_error_message(
                    latest_post_status,
                    latest_post_payload,
                    latest_post.get('response_text') or '',
                )
                if 200 <= latest_post_status < 300:
                    guild_id = preview_guild_id or _current_channels_guild_id(current_url) or ''
                    guild_name = preview_guild_name
                    if isinstance(latest_post_payload, dict):
                        guild = latest_post_payload.get('guild') or {}
                        guild_id = str(guild.get('id') or guild_id)
                        guild_name = guild.get('name') or guild_name
                    _trace_event(trace, 'invite_join_success_api', page=page, status=latest_post_status, guild_id=guild_id, guild_name=guild_name)
                    return {
                        'status': 'joined',
                        'captcha': saw_captcha,
                        'invite': code,
                        'url': current_url,
                        'guild_id': guild_id,
                        'guild_name': guild_name,
                    }

            current_guild_id = _current_channels_guild_id(current_url)
            if current_guild_id and preview_guild_id and current_guild_id == preview_guild_id:
                _trace_event(trace, 'invite_join_success_url', page=page, guild_id=preview_guild_id, guild_name=preview_guild_name)
                return {
                    'status': 'joined',
                    'captcha': saw_captcha,
                    'invite': code,
                    'url': current_url,
                    'guild_id': preview_guild_id,
                    'guild_name': preview_guild_name,
                }

            if reclick_after_captcha:
                if _invite_has_accept_action(body_text):
                    try:
                        _click_accept_invite(page)
                        reclick_after_captcha = False
                        _trace_event(trace, 'invite_accept_reclicked', page=page)
                        time.sleep(1.0)
                        continue
                    except Exception as e:
                        _trace_event(trace, 'invite_accept_reclick_error', page=page, error=str(e))
                elif current_url.endswith('/channels/@me') or current_url.endswith('/channels/@me/'):
                    try:
                        open_invite(page, code)
                        _wait_accept_invite_button(page, code)
                        _click_accept_invite(page)
                        reclick_after_captcha = False
                        _trace_event(trace, 'invite_reopened_and_reclicked', page=page)
                        time.sleep(1.0)
                        continue
                    except Exception as e:
                        _trace_event(trace, 'invite_reopen_error', page=page, error=str(e))

            if (
                latest_post is not None
                and latest_post_status is not None
                and latest_post_status >= 400
                and not _payload_has_captcha(latest_post_payload)
                and not has_hcaptcha(page)
            ):
                if reclick_after_captcha and _invite_has_accept_action(body_text) and _retryable_invite_api_error(latest_post_status, latest_post_payload):
                    _trace_event(
                        trace,
                        'invite_join_retryable_api_error',
                        page=page,
                        status=latest_post_status,
                        message=latest_post_message,
                    )
                else:
                    _trace_event(trace, 'invite_join_error_api', page=page, screenshot=True, status=latest_post_status, message=latest_post_message)
                    raise DiscordWebError(f'Invite API {latest_post_status} for {code}: {latest_post_message}')

            if _invite_failure(body_text):
                has_accept_action = _invite_has_accept_action(body_text)
                if reclick_after_captcha and has_accept_action:
                    failure_page_first_seen_at = None
                elif has_accept_action and not saw_captcha:
                    now = time.time()
                    if failure_page_first_seen_at is None:
                        failure_page_first_seen_at = now
                        _trace_event(
                            trace,
                            'invite_failure_page_pre_captcha_grace',
                            page=page,
                            body_text=_shorten(body_text, 240),
                            retry_count=failure_page_retry_count,
                        )
                    elif failure_page_retry_count < 2 and (now - failure_page_first_seen_at) >= 1.25:
                        try:
                            _click_accept_invite(page)
                            failure_page_retry_count += 1
                            failure_page_first_seen_at = time.time()
                            _trace_event(
                                trace,
                                'invite_failure_page_pre_captcha_retry',
                                page=page,
                                retry_count=failure_page_retry_count,
                            )
                            time.sleep(1.0)
                            continue
                        except Exception as e:
                            _trace_event(trace, 'invite_failure_page_pre_captcha_retry_error', page=page, error=str(e))
                else:
                    failure_page_first_seen_at = None

                if not (reclick_after_captcha and has_accept_action) and not (
                    has_accept_action and not saw_captcha and failure_page_retry_count < 2
                ):
                    _trace_event(trace, 'invite_failure_page', page=page, screenshot=True, body_text=_shorten(body_text, 240))
                    raise DiscordWebError(f'Invite join failed for {code}: {body_text[:240]}')
            else:
                failure_page_first_seen_at = None

            if preview_guild_id and time.time() >= next_membership_check_at and not has_hcaptcha(page):
                next_membership_check_at = time.time() + 5.0
                try:
                    joined_guild = _guild_membership_lookup(preview_guild_id, preview_guild_name)
                except Exception as e:
                    _trace_event(trace, 'invite_membership_check_error', error=str(e))
                    joined_guild = None
                if joined_guild is not None:
                    _trace_event(trace, 'invite_join_success_membership', page=page, guild_id=joined_guild.get('id'), guild_name=joined_guild.get('name'))
                    return {
                        'status': 'joined',
                        'captcha': saw_captcha,
                        'invite': code,
                        'url': current_url,
                        'guild_id': joined_guild.get('id'),
                        'guild_name': joined_guild.get('name'),
                    }

            time.sleep(0.5)

        _trace_event(trace, 'invite_join_timeout', page=page, screenshot=True, invite=code, current_url=page.url or '')
        raise DiscordWebError(f'Timed out waiting for browser-native invite join outcome for {code}.')
    finally:
        try:
            page.remove_listener('response', _record_invite_response)
        except Exception:
            try:
                page.off('response', _record_invite_response)
            except Exception:
                pass
        if route_registered:
            try:
                page.unroute('**/api/v9/invites/*', _invite_route)
            except Exception:
                pass


def browser_fetch(page, method: str, path: str, *, headers: dict | None = None,
                  body: dict | None = None, body_bytes: bytes | None = None,
                  params: dict | None = None) -> BrowserFetchResult:
    """Perform an authenticated fetch from inside the real Discord web page."""
    if body is not None and body_bytes is not None:
        raise ValueError("Provide either body or body_bytes, not both")

    url = path if path.startswith("http://") or path.startswith("https://") else f"{API_BASE}{path}"
    if params:
        qs = urlencode(params, doseq=True)
        url = f"{url}?{qs}"

    payload = {
        "method": method,
        "url": url,
        "headers": headers or {},
        "jsonBody": body,
        "bodyBase64": base64.b64encode(body_bytes).decode("ascii") if body_bytes is not None else None,
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
    return BrowserFetchResult(
        status=int(result.get("status") or 0),
        text=result.get("text") or "",
        headers=result.get("headers") or {},
    )


def debug_snapshot(page) -> dict:
    return page.evaluate(
        f"""() => ({{
            href: location.href,
            title: document.title,
            hasDmShell: ({logged_in_js()})(),
            hasComposer: !!document.querySelector({json.dumps(COMPOSER_SELECTOR)}),
        }})"""
    )
