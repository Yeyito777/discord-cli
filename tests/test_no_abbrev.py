import re
import unittest
from pathlib import Path
from unittest.mock import patch

from src import reading

SRC = Path(__file__).resolve().parents[1] / "src"


class NoAbbreviationTests(unittest.TestCase):
    def test_every_parser_disables_long_option_abbreviations(self):
        # '--se' must never silently mean '--send'.
        for path in SRC.rglob("*.py"):
            text = path.read_text()
            calls = re.findall(r"(?:ArgumentParser|add_parser)\((?:[^()]|\([^()]*\))*\)", text, re.S)
            for call in calls:
                with self.subTest(file=path.name, call=call[:60]):
                    self.assertIn("allow_abbrev=False", call)

    def test_dm_rejects_abbreviated_send(self):
        # Never reach stdin or the network, even where the bug is present.
        with (
            patch("sys.stderr"),
            patch.object(reading, "read_stdin_text", side_effect=AssertionError("treated as --send")),
            self.assertRaises(SystemExit),
        ):
            reading.dm(["bob", "--se"])
