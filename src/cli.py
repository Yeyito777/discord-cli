"""Entry point used by bin/discord: run one command, report errors in one line."""

import importlib
import sys


def run(module: str, cmd: str, argv: list[str]) -> None:
    try:
        importlib.import_module(module).dispatch(cmd, argv)
    except RuntimeError as e:
        # Covers AccountError and API failures; a traceback adds nothing for
        # the caller and can bury which account or target was refused.
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
