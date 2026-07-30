"""Exact stdin payload handling for mutating commands."""

import sys


def reject_inline_text(parser, values, *, label="content"):
    """Reject legacy inline payload positionals with an actionable error."""
    if values:
        parser.error(f"{label} must be provided via stdin; inline text is not accepted")


def read_stdin_text(parser, *, label="content", required=True):
    """Read an exact text payload from stdin without trimming or decoding escapes."""
    if sys.stdin.isatty():
        if required:
            parser.error(f"{label} is required on stdin")
        return None

    stream = getattr(sys.stdin, "buffer", sys.stdin)
    raw = stream.read()
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            parser.error(f"{label} on stdin must be valid UTF-8")
    else:
        text = raw
    if text == "":
        if required:
            parser.error(f"{label} is required on stdin")
        return None
    return text
