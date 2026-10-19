"""Discord CLI tests.

Point the tool at an empty config before any test imports ``src``: no test
may read the real accounts or tokens, or write to their audit logs.
"""

import os
import tempfile

_CONFIG = tempfile.TemporaryDirectory(prefix="discord-cli-tests-")
os.environ["DISCORD_CONFIG_DIR"] = _CONFIG.name
for _name in ("DISCORD_ACCOUNT", "DISCORD_ACCOUNT_EXPLICIT"):
    os.environ.pop(_name, None)
