"""The supervised Discord daemon: one process that owns every notification Gateway.

exocortexd runs this (manifest ``daemon``, restart always) in its own process
group and kills that whole group on stop, so the Gateways started here never
depend on the cgroup or process tree of whoever ran the CLI.  The core
subscription registry is authoritative for what runs:

* ``__notify__`` (DMs and @mentions) for the supervised account, always, and
  for any other account with an enabled ``account:<alias>:notifications`` route;
* ``__channel_notify__<id>`` for every enabled
  ``account:<alias>:channel:<id>:messages`` route.

Each Gateway is its own process with ``DISCORD_ACCOUNT`` set, so accounts stay
isolated, and it still takes its per-account/channel singleton lock, so there
is at most one collector per source.  Gateways reconnect through network drops
on their own; this loop restarts any that exit, with backoff, and re-reads the
registry every ``RECONCILE_SECONDS`` or at once on SIGHUP (sent by the CLI
after subscribe/unsubscribe/start).
"""

import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

from src.accounts import listener_dir, load_registry, runtime_root
from src.channel_notifications import PREFIX

PROJECT_DIR = Path(__file__).resolve().parent.parent
GATEWAY_SCRIPT = PROJECT_DIR / "src" / "gateway.py"
ACCOUNT_GATEWAY = "__notify__"
SOURCE_RE = re.compile(
    r"^account:(?P<alias>[a-z0-9][a-z0-9._-]{0,63}):"
    r"(?:notifications|channel:(?P<channel>\d{17,20}):messages)$"
)
TICK_SECONDS = 1.0
RECONCILE_SECONDS = 15.0
BACKOFF_SECONDS = (1, 2, 5, 10, 30, 60)
STABLE_SECONDS = 300.0
STOP_GRACE_SECONDS = 4.0


def _log(message):
    print(f"[{time.strftime('%H:%M:%S')} supervisor] {message}", file=sys.stderr, flush=True)


def pid_file():
    return runtime_root() / "supervisor.pid"


def supervisor_pid():
    """PID of the running supervised daemon for this config root, or None."""
    try:
        pid = int(pid_file().read_text().strip())
        # macOS has no /proc; ps works on both supported Unix platforms.
        command = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "command="], text=True, stderr=subprocess.DEVNULL,
        )
    except (OSError, ValueError, subprocess.CalledProcessError):
        return None
    return pid if "src.notify __daemon__" in command else None


def request_reconcile():
    """Ask the daemon to re-read subscriptions now; return its PID or None."""
    pid = supervisor_pid()
    if pid:
        try:
            os.kill(pid, signal.SIGHUP)
        except ProcessLookupError:
            return None
    return pid


def desired_gateways(subscriptions, aliases, account_alias=None):
    """Return the {(alias, gateway key)} set that these core routes need."""
    desired = {(account_alias, ACCOUNT_GATEWAY)} if account_alias else set()
    for subscription in subscriptions:
        if not isinstance(subscription, dict) or subscription.get("enabled") is False:
            continue
        match = SOURCE_RE.match(str(subscription.get("sourceId") or ""))
        if match and match["alias"] in aliases:
            channel = match["channel"]
            desired.add((match["alias"], f"{PREFIX}{channel}" if channel else ACCOUNT_GATEWAY))
    return desired


def _core_subscriptions():
    from src.exocortex import list_external_notification_subscriptions
    return list_external_notification_subscriptions(tool_name="discord")


class Gateway:
    """One supervised Gateway process for (account alias, gateway key)."""

    def __init__(self, alias, key, *, registered=True):
        self.alias = alias
        self.key = key
        self.registered = registered  # False only for a legacy credentials.json install
        self.proc = None
        self.started = 0.0
        self.failures = 0
        self.next_start = 0.0

    def __str__(self):
        return f"{self.alias}/{self.key}"

    def files(self):
        root = listener_dir(self.alias)
        return root / f"{self.key}.log", root / f"{self.key}.err"

    def command(self):
        return [sys.executable, str(GATEWAY_SCRIPT), self.key, str(self.files()[0])]

    def env(self):
        env = dict(os.environ)
        env.pop("DISCORD_ACCOUNT_EXPLICIT", None)
        if self.registered:
            env["DISCORD_ACCOUNT"] = self.alias
        else:
            env.pop("DISCORD_ACCOUNT", None)
        return env

    def unsupervised_pids(self):
        """The same Gateway started elsewhere: an older CLI, a manual run, a stopgap."""
        pattern = rf"gateway\.py\s+{re.escape(self.key)}\s+{re.escape(str(self.files()[0]))}$"
        try:
            found = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True).stdout
        except OSError:
            return []
        own = self.proc.pid if self.proc else None
        return [int(pid) for pid in found.split() if pid.isdigit() and int(pid) != own]

    def start(self, now):
        stale = self.unsupervised_pids()
        if stale:
            from src.notify import _stop_notify_pids
            _, alive = _stop_notify_pids(stale)
            _log(f"{self}: stopped unsupervised PID(s) {', '.join(map(str, stale))}")
            if alive:
                raise RuntimeError(f"unsupervised PID(s) {alive} survived SIGKILL")
        # The account Gateway keeps logging to service.log as before; channel
        # Gateways keep their per-channel .err file next to the state file.
        stderr = None if self.key == ACCOUNT_GATEWAY else open(self.files()[1], "a")
        try:
            # Same process group as this daemon: exocortexd's group kill stops it.
            self.proc = subprocess.Popen(
                self.command(), cwd=PROJECT_DIR, env=self.env(),
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=stderr,
            )
        finally:
            if stderr:
                stderr.close()
        self.started = now
        _log(f"{self}: started PID {self.proc.pid}")

    def _backoff(self, now, reason):
        if self.started and now - self.started >= STABLE_SECONDS:
            self.failures = 0
        delay = BACKOFF_SECONDS[min(self.failures, len(BACKOFF_SECONDS) - 1)]
        self.failures += 1
        self.next_start = now + delay
        _log(f"{self}: {reason}; restarting in {delay}s")

    def tick(self, now):
        """Keep the process running: restart it with backoff after it exits."""
        if self.proc is not None:
            code = self.proc.poll()
            if code is None:
                return
            self.proc = None
            self._backoff(now, f"exited with code {code}")
        if now < self.next_start:
            return
        try:
            self.start(now)
        except Exception as exc:
            self.proc = None
            self.started = now
            self._backoff(now, f"start failed: {exc}")

    def terminate(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()

    def wait(self, deadline):
        if self.proc is None:
            return
        try:
            self.proc.wait(max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc = None


class Supervisor:
    def __init__(self, account_alias, *, list_subscriptions=None, registry=load_registry):
        self.account_alias = account_alias
        self.list_subscriptions = list_subscriptions or _core_subscriptions
        self.registry = registry
        self.registered = set(registry()["accounts"])
        self.desired = {(account_alias, ACCOUNT_GATEWAY)} if account_alias else set()
        self.gateways = {}
        self.running = True
        self.reconcile_requested = True
        self.next_reconcile = 0.0

    def refresh(self):
        """Re-read the authoritative registries; keep the last good view on failure."""
        try:
            self.registered = set(self.registry()["accounts"])
            aliases = self.registered | ({self.account_alias} if self.account_alias else set())
            self.desired = desired_gateways(self.list_subscriptions(), aliases, self.account_alias)
        except Exception as exc:
            _log(f"subscription refresh failed, keeping current gateways: {exc}")

    def step(self, now):
        if self.reconcile_requested or now >= self.next_reconcile:
            self.reconcile_requested = False
            self.next_reconcile = now + RECONCILE_SECONDS
            self.refresh()
        retired = [self.gateways.pop(key) for key in list(self.gateways) if key not in self.desired]
        for gateway in retired:
            gateway.terminate()
        for gateway in retired:
            gateway.wait(time.monotonic() + STOP_GRACE_SECONDS)
            _log(f"{gateway}: stopped (no enabled subscription)")
        for key in sorted(self.desired):
            gateway = self.gateways.get(key)
            if gateway is None:
                registered = key[0] in self.registered
                gateway = self.gateways[key] = Gateway(*key, registered=registered)
            gateway.tick(now)

    def shutdown(self):
        gateways = list(self.gateways.values())
        for gateway in gateways:
            gateway.terminate()
        deadline = time.monotonic() + STOP_GRACE_SECONDS
        for gateway in gateways:
            gateway.wait(deadline)
        self.gateways.clear()

    def _stop(self, signum=None, frame=None):
        self.running = False

    def _reconcile_now(self, signum=None, frame=None):
        self.reconcile_requested = True

    def run(self):
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGHUP, self._reconcile_now)
        path = pid_file()
        path.write_text(f"{os.getpid()}\n")
        _log(f"supervising Discord gateways (account {self.account_alias or 'none'})")
        try:
            while self.running:
                self.step(time.monotonic())
                time.sleep(TICK_SECONDS)
        finally:
            self.shutdown()
            try:
                if path.read_text().strip() == str(os.getpid()):
                    path.unlink()
            except OSError:
                pass
