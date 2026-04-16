"""Persistent local broker for Discord web automation.

Keeps a single dedicated Chromium persistent profile alive and accepts simple
JSON requests over a local UNIX socket.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from src.auth import get_token
from src.webprofile import (
    WEB_DIR,
    ensure_logged_in,
    launch_context,
    open_app,
    seed_hcaptcha_cookies_from_captcha_profile,
)
from src.websession import (
    debug_snapshot,
    join_invite_with_captcha,
    continue_join_invite_with_captcha,
    send_dm_with_captcha,
    continue_send_dm_with_captcha,
)
from src.webtrace import TRACE_DIR, ensure_trace_dir, make_tracer, trace_path

BROKER_SOCKET = WEB_DIR / "broker.sock"
BROKER_PID = WEB_DIR / "broker.pid"
BROKER_LOG = WEB_DIR / "broker.log"
MAX_INVITE_CAPTCHA_PROMPTS = 20


class WebBrokerError(RuntimeError):
    pass


def _ensure_parent() -> None:
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    ensure_trace_dir()


def _remove_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _new_action_id() -> str:
    return uuid.uuid4().hex[:12]


def _new_challenge_id() -> str:
    return uuid.uuid4().hex[:12]


@dataclass
class PendingAction:
    action_id: str
    challenge_id: str
    op: str
    prompt: str
    kind: str = 'text'
    captcha: bool = True
    channel_id: str | None = None
    text: str | None = None
    invite: str | None = None
    reclick_after_captcha: bool = False
    invite_request_session_id: str | None = None
    invite_request_instance_id: str | None = None
    prompt_count: int = 1

    def summary(self) -> dict:
        return {
            'action_id': self.action_id,
            'challenge_id': self.challenge_id,
            'op': self.op,
            'prompt': self.prompt,
            'kind': self.kind,
            'prompt_count': self.prompt_count,
            'trace_path': str(trace_path(self.action_id)),
        }

    def conflict_error(self) -> str:
        return (
            'A browser-native Discord action is already awaiting captcha resolution: '
            f'op={self.op} action_id={self.action_id} '
            f'challenge_id={self.challenge_id} prompt={self.prompt!r}. '
            'Solve or clear that action before starting another browser-native action.'
        )

    def validate_request(self, *, challenge_id: str | None, action_id: str | None) -> str | None:
        if challenge_id and challenge_id != self.challenge_id:
            return (
                f'Pending challenge mismatch: requested challenge_id={challenge_id} '
                f'but active challenge_id={self.challenge_id}'
            )
        if action_id and action_id != self.action_id:
            return (
                f'Pending action mismatch: requested action_id={action_id} '
                f'but active action_id={self.action_id}'
            )
        return None


def _ensure_ready_page(context, page):
    token = get_token()
    if page is None or page.is_closed():
        page = open_app(context)
    try:
        ensure_logged_in(page, token)
        return page
    except Exception:
        try:
            if page is not None and not page.is_closed():
                page.close()
        except Exception:
            pass
        fresh_page = context.new_page()
        ensure_logged_in(fresh_page, token)
        return fresh_page


def _ensure_resume_page(context, page):
    """Preserve the current page during pending captcha resume.

    Re-running the normal readiness/bootstrap logic here can navigate away from
    the live invite/DM captcha surface and restart the flow from `/app` or
    `/login`, which loses Discord's in-page captcha state.
    """
    if page is None or page.is_closed():
        return open_app(context)
    return page


def _annotate_result(result: dict, *, action_id: str) -> dict:
    result['action_id'] = action_id
    result['trace_path'] = str(trace_path(action_id))
    return result


def _new_pending_action(*, op: str, action_id: str, req: dict, result: dict) -> PendingAction:
    kwargs = {
        'action_id': action_id,
        'challenge_id': _new_challenge_id(),
        'op': op,
        'prompt': result.get('prompt') or '',
        'kind': result.get('kind', 'text'),
        'captcha': True,
    }
    if op == 'send_dm':
        kwargs.update(channel_id=req['channel_id'], text=req['text'])
    elif op == 'join_invite':
        kwargs.update(
            invite=req['invite'],
            reclick_after_captcha=bool(result.get('reclick_after_captcha', False)),
            invite_request_session_id=result.get('invite_request_session_id'),
            invite_request_instance_id=result.get('invite_request_instance_id'),
        )
    else:
        raise WebBrokerError(f'Unsupported pending action op: {op}')
    return PendingAction(**kwargs)


def _refresh_pending_action(pending: PendingAction, result: dict) -> None:
    pending.challenge_id = _new_challenge_id()
    pending.prompt = result.get('prompt') or pending.prompt
    pending.kind = result.get('kind', pending.kind)
    pending.prompt_count += 1
    if pending.op == 'join_invite':
        pending.reclick_after_captcha = bool(result.get('reclick_after_captcha', False))
        pending.invite_request_session_id = result.get('invite_request_session_id') or pending.invite_request_session_id
        pending.invite_request_instance_id = result.get('invite_request_instance_id') or pending.invite_request_instance_id


def status() -> dict:
    pid = None
    if BROKER_PID.exists():
        try:
            pid = int(BROKER_PID.read_text().strip())
        except Exception:
            pid = None
    running = bool(pid and _pid_is_running(pid))
    return {
        "socket": str(BROKER_SOCKET),
        "socket_exists": BROKER_SOCKET.exists(),
        "pid_file": str(BROKER_PID),
        "pid": pid,
        "running": running,
        "log_file": str(BROKER_LOG),
        "trace_dir": str(TRACE_DIR),
    }


def _request(payload: dict, *, timeout: int = 300) -> dict:
    if not BROKER_SOCKET.exists():
        raise WebBrokerError("Discord web broker socket does not exist.")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(str(BROKER_SOCKET))
        data = json.dumps(payload).encode("utf-8")
        sock.sendall(data)
        sock.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            buf = sock.recv(65536)
            if not buf:
                break
            chunks.append(buf)
    except socket.timeout as e:
        raise WebBrokerError(f"Timed out waiting for web broker response to {payload.get('op')}") from e
    finally:
        try:
            sock.close()
        except Exception:
            pass
    raw = b"".join(chunks).decode("utf-8", errors="replace").strip()
    if not raw:
        raise WebBrokerError("Discord web broker returned no response.")
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        raise WebBrokerError(f"Invalid broker response: {raw[:200]}") from e
    if not msg.get("ok"):
        raise WebBrokerError(msg.get("error") or "Discord web broker request failed")
    return msg.get("result")


def ping(*, timeout: int = 10) -> dict:
    return _request({"op": "ping"}, timeout=timeout)


def ensure_started(*, seed_accessibility: bool = False, headed: bool = False, timeout: int = 30) -> dict:
    _ensure_parent()
    info = status()
    if info["running"] and info["socket_exists"]:
        try:
            ping(timeout=3)
            return status()
        except Exception:
            pid = info.get("pid")
            if pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
                time.sleep(0.5)

    if seed_accessibility:
        seed_hcaptcha_cookies_from_captcha_profile()

    _remove_if_exists(BROKER_SOCKET)
    _remove_if_exists(BROKER_PID)

    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    project_root = str(Path(__file__).resolve().parent.parent)
    env["PYTHONPATH"] = f"{project_root}:{existing}" if existing else project_root

    cmd = [
        sys.executable,
        "-c",
        (
            "from src.webbroker import run_server; "
            f"run_server(headed={bool(headed)!r})"
        ),
    ]
    with BROKER_LOG.open("ab") as log:
        proc = subprocess.Popen(
            cmd,
            cwd=project_root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        if proc.poll() is not None:
            raise WebBrokerError(f"Discord web broker exited early with code {proc.returncode}")
        try:
            ping(timeout=2)
            return status()
        except Exception as e:
            last_err = e
            time.sleep(0.4)
    raise WebBrokerError(f"Timed out waiting for Discord web broker startup: {last_err}")


def stop(*, timeout: int = 15) -> dict:
    info = status()
    if not info["running"]:
        _remove_if_exists(BROKER_SOCKET)
        _remove_if_exists(BROKER_PID)
        return status()
    try:
        _request({"op": "shutdown"}, timeout=timeout)
    except Exception:
        pass
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = status()
        if not info["running"] and not info["socket_exists"]:
            return info
        time.sleep(0.3)
    pid = info.get("pid")
    if pid:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.kill(pid, sig)
            except Exception:
                pass
            time.sleep(0.4)
            if not _pid_is_running(pid):
                break
    _remove_if_exists(BROKER_SOCKET)
    _remove_if_exists(BROKER_PID)
    return status()


def _request_with_broker_restart(payload: dict, *, timeout: int, seed_accessibility: bool = False) -> dict:
    ensure_started(seed_accessibility=seed_accessibility)
    try:
        return _request(payload, timeout=timeout)
    except WebBrokerError as e:
        msg = str(e).lower()
        if 'returned no response' not in msg and 'socket' not in msg:
            raise
        stop(timeout=5)
        ensure_started(seed_accessibility=seed_accessibility)
        return _request(payload, timeout=timeout)


def send_dm(channel_id: str, text: str, *, seed_accessibility: bool = False,
            action_id: str | None = None) -> dict:
    payload = {"op": "send_dm", "channel_id": channel_id, "text": text}
    if action_id:
        payload['action_id'] = action_id
    return _request_with_broker_restart(
        payload,
        timeout=600,
        seed_accessibility=seed_accessibility,
    )


def join_invite(invite: str, *, seed_accessibility: bool = False,
                action_id: str | None = None) -> dict:
    payload = {"op": "join_invite", "invite": invite}
    if action_id:
        payload['action_id'] = action_id
    return _request_with_broker_restart(
        payload,
        timeout=900,
        seed_accessibility=seed_accessibility,
    )


def solve_captcha(answer: str, *, timeout: int = 900,
                  challenge_id: str | None = None,
                  action_id: str | None = None) -> dict:
    payload = {"op": "solve_captcha", "answer": answer}
    if challenge_id:
        payload['challenge_id'] = challenge_id
    if action_id:
        payload['action_id'] = action_id
    return _request(payload, timeout=timeout)


def run_server(*, headed: bool = False) -> None:
    _ensure_parent()
    _remove_if_exists(BROKER_SOCKET)

    pw = None
    context = None
    page = None
    server = None
    try:
        pw, context = launch_context(headed=headed)
        page = open_app(context)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(BROKER_SOCKET))
        server.listen(4)
        BROKER_PID.write_text(str(os.getpid()))

        pending = None
        stopping = False
        while not stopping:
            conn, _addr = server.accept()
            with conn:
                raw = []
                while True:
                    buf = conn.recv(65536)
                    if not buf:
                        break
                    raw.append(buf)
                payload = b"".join(raw).decode("utf-8", errors="replace").strip()
                if not payload:
                    resp = {"ok": False, "error": "Empty broker request"}
                else:
                    try:
                        req = json.loads(payload)
                        op = req.get("op")
                        if op == "ping":
                            result = {"pong": True, **status()}
                            if pending is not None:
                                result["pending_captcha"] = pending.summary()
                            resp = {"ok": True, "result": result}
                        elif op == "shutdown":
                            stopping = True
                            resp = {"ok": True, "result": {"stopping": True}}
                        elif op == "status":
                            if page is None or page.is_closed():
                                page = open_app(context)
                            result = {**status(), **debug_snapshot(page)}
                            if pending is not None:
                                result["pending_captcha"] = pending.summary()
                            resp = {"ok": True, "result": result}
                        elif op == "send_dm":
                            if pending is not None:
                                resp = {"ok": False, "error": pending.conflict_error()}
                            else:
                                page = _ensure_ready_page(context, page)
                                action_id = req.get('action_id') or _new_action_id()
                                trace = make_tracer(action_id, snapshot_fn=debug_snapshot)
                                trace('action_start', op='send_dm', channel_id=req['channel_id'], text=req['text'], headed=headed)
                                result = send_dm_with_captcha(
                                    page,
                                    req["channel_id"],
                                    req["text"],
                                    trace=trace,
                                )
                                result = _annotate_result(result, action_id=action_id)
                                if result.get("status") == "captcha_required":
                                    pending = _new_pending_action(op='send_dm', action_id=action_id, req=req, result=result)
                                    result['challenge_id'] = pending.challenge_id
                                    trace('action_pending', challenge_id=pending.challenge_id, prompt=pending.prompt, kind=pending.kind, prompt_count=pending.prompt_count)
                                else:
                                    trace('action_result', result=result, page=page)
                                resp = {"ok": True, "result": result}
                        elif op == "join_invite":
                            if pending is not None:
                                resp = {"ok": False, "error": pending.conflict_error()}
                            else:
                                page = _ensure_ready_page(context, page)
                                action_id = req.get('action_id') or _new_action_id()
                                trace = make_tracer(action_id, snapshot_fn=debug_snapshot)
                                trace('action_start', op='join_invite', invite=req['invite'], headed=headed)
                                result = join_invite_with_captcha(page, req["invite"], trace=trace)
                                result = _annotate_result(result, action_id=action_id)
                                if result.get("status") == "captcha_required":
                                    pending = _new_pending_action(op='join_invite', action_id=action_id, req=req, result=result)
                                    result['challenge_id'] = pending.challenge_id
                                    trace(
                                        'action_pending',
                                        challenge_id=pending.challenge_id,
                                        prompt=pending.prompt,
                                        kind=pending.kind,
                                        prompt_count=pending.prompt_count,
                                        reclick_after_captcha=pending.reclick_after_captcha,
                                    )
                                else:
                                    trace('action_result', result=result, page=page)
                                resp = {"ok": True, "result": result}
                        elif op == "solve_captcha":
                            if pending is None:
                                resp = {"ok": False, "error": "No pending captcha challenge to solve."}
                            else:
                                error = pending.validate_request(
                                    challenge_id=req.get('challenge_id'),
                                    action_id=req.get('action_id'),
                                )
                                if error is not None:
                                    resp = {"ok": False, "error": error}
                                else:
                                    page = _ensure_resume_page(context, page)
                                    trace = make_tracer(pending.action_id, snapshot_fn=debug_snapshot)
                                    trace(
                                        'solve_requested',
                                        challenge_id=pending.challenge_id,
                                        answer=req['answer'],
                                        op=pending.op,
                                    )
                                    if pending.op == "send_dm":
                                        result = continue_send_dm_with_captcha(
                                            page,
                                            pending.channel_id,
                                            pending.text,
                                            answer=req["answer"],
                                            saw_captcha=bool(pending.captcha),
                                            trace=trace,
                                        )
                                    elif pending.op == "join_invite":
                                        result = continue_join_invite_with_captcha(
                                            page,
                                            pending.invite,
                                            answer=req["answer"],
                                            expected_prompt=pending.prompt,
                                            saw_captcha=bool(pending.captcha),
                                            reclick_after_captcha=bool(pending.reclick_after_captcha),
                                            invite_request_session_id=pending.invite_request_session_id,
                                            invite_request_instance_id=pending.invite_request_instance_id,
                                            trace=trace,
                                        )
                                    else:
                                        raise WebBrokerError(f"Unknown pending captcha op: {pending.op}")

                                    result = _annotate_result(result, action_id=pending.action_id)
                                    if result.get("status") == "captcha_required":
                                        old_challenge = pending.challenge_id
                                        _refresh_pending_action(pending, result)
                                        if pending.op == 'join_invite' and pending.prompt_count > MAX_INVITE_CAPTCHA_PROMPTS:
                                            trace(
                                                'invite_captcha_loop_suspected',
                                                page=page,
                                                prompt_count=pending.prompt_count,
                                                prompt=pending.prompt,
                                                invite=pending.invite,
                                                screenshot=True,
                                            )
                                            raise WebBrokerError(
                                                f"Invite {pending.invite} exceeded {MAX_INVITE_CAPTCHA_PROMPTS} captcha prompts; "
                                                "suspected Discord invite captcha loop."
                                            )
                                        result['challenge_id'] = pending.challenge_id
                                        trace(
                                            'action_pending',
                                            challenge_id=pending.challenge_id,
                                            previous_challenge_id=old_challenge,
                                            prompt=pending.prompt,
                                            kind=pending.kind,
                                            prompt_count=pending.prompt_count,
                                            reclick_after_captcha=pending.reclick_after_captcha,
                                        )
                                    else:
                                        trace('action_result', result=result, page=page, screenshot=result.get('status') != 'sent')
                                        pending = None
                                    resp = {"ok": True, "result": result}
                        else:
                            resp = {"ok": False, "error": f"Unknown broker op: {op}"}
                    except Exception as e:
                        if pending is not None:
                            trace = make_tracer(pending.action_id or 'unknown', snapshot_fn=debug_snapshot)
                            trace('action_error', page=page, screenshot=True, error=str(e))
                        resp = {"ok": False, "error": str(e)}
                try:
                    conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                except BrokenPipeError:
                    pass
                except OSError:
                    pass
    finally:
        try:
            if server is not None:
                server.close()
        except Exception:
            pass
        _remove_if_exists(BROKER_SOCKET)
        _remove_if_exists(BROKER_PID)
        try:
            if page is not None and not page.is_closed():
                page.close()
        except Exception:
            pass
        try:
            if context is not None:
                context.close()
        except Exception:
            pass
        try:
            if pw is not None:
                pw.stop()
        except Exception:
            pass
