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
os.environ["XDG_CACHE_HOME"] = os.path.join(_CONFIG.name, "cache")

# Never scrape discord.com for the client build number during tests.
from src import fingerprint as _fingerprint  # noqa: E402

_fingerprint._build_number = _fingerprint.FALLBACK_BUILD_NUMBER
