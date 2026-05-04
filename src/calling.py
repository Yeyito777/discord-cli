"""Compatibility entrypoint for `discord call` commands."""

from src.calls.cli import dispatch, main  # noqa: F401

if __name__ == "__main__":
    main()
