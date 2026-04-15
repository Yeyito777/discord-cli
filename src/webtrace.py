"""Shared tracing helpers for browser-native Discord automation."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from src.webprofile import WEB_DIR

TRACE_DIR = WEB_DIR / "traces"


def ensure_trace_dir() -> None:
    TRACE_DIR.mkdir(parents=True, exist_ok=True)


def trace_path(action_id: str) -> Path:
    ensure_trace_dir()
    return TRACE_DIR / f"{action_id}.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return str(value)


def _page_body_snippet(page, limit: int = 600) -> str:
    try:
        text = page.locator('body').inner_text(timeout=1200)
    except Exception:
        return ''
    if len(text) <= limit:
        return text
    return text[: limit - 3] + '...'


def make_tracer(action_id: str, *, snapshot_fn):
    file_path = trace_path(action_id)

    def trace(event: str, *, page=None, screenshot: bool = False, **fields) -> None:
        entry = {
            'ts': _now_iso(),
            'action_id': action_id,
            'event': event,
        }
        entry.update({k: _json_safe(v) for k, v in fields.items()})
        if page is not None:
            try:
                entry['page'] = _json_safe(snapshot_fn(page))
            except Exception as e:
                entry['page_error'] = str(e)
            try:
                entry['body_text'] = _page_body_snippet(page)
            except Exception:
                pass
            if screenshot:
                try:
                    ensure_trace_dir()
                    name = f"{action_id}-{int(time.time() * 1000)}-{event}.png"
                    screenshot_path = TRACE_DIR / name
                    page.screenshot(path=str(screenshot_path), full_page=True)
                    entry['screenshot'] = str(screenshot_path)
                except Exception as e:
                    entry['screenshot_error'] = str(e)
        with file_path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    return trace
