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

Health: a Gateway's main thread rewrites its state file at least every 30 s.
One that stops doing so for ``STALL_SECONDS`` is wedged and gets killed and
restarted.  One that stays out of ``ready`` for ``ALERT_SECONDS`` (at once for
a rejected token) is reported to the conversations subscribed to its source,
once per outage, followed by a recovery notice.  Both clocks are monotonic, so
time asleep (a laptop lid) never counts as an outage.
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
from src.listener_state import ago, read_state

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
# Discord invalidates sessions (op 9) that identify at the same moment on one
# account, so Gateway starts are spaced out.
START_SPACING_SECONDS = 5.0
HEALTH_CHECK_SECONDS = 5.0
STALL_SECONDS = 180.0
ALERT_SECONDS = 600.0
ALERT_RETRY_SECONDS = 60.0


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


def _core_publish(source_id, event_id, text, data):
    from src.exocortex import publish_external_notification
    return publish_external_notification("discord", source_id, event_id, text, data=data, timeout_seconds=5)


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
        # Health (supervisor monotonic clock)
        self.next_health = 0.0
        self.last_updated = None
        self.progress_at = 0.0
        self.unready_since = None
        self.outage_id = None
        self.alerted = False
        self.next_alert = 0.0

    def __str__(self):
        return f"{self.alias}/{self.key}"

    def source_id(self):
        if self.key == ACCOUNT_GATEWAY:
            return f"account:{self.alias}:notifications"
        return f"account:{self.alias}:channel:{self.key[len(PREFIX):]}:messages"

    def describe_source(self):
        what = "DMs and @mentions" if self.key == ACCOUNT_GATEWAY else f"channel [ch:{self.key[len(PREFIX):]}]"
        return f"Discord ({self.alias}) {what}"

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
        # Each Gateway logs to <key>.err next to its state file, which the
        # Gateway itself rotates; config/service.log keeps only this loop.
        with open(self.files()[1], "a") as stderr:
            # Same process group as this daemon: exocortexd's group kill stops it.
            self.proc = subprocess.Popen(
                self.command(), cwd=PROJECT_DIR, env=self.env(),
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=stderr,
            )
        self.started = now
        self.progress_at = now
        self.last_updated = None
        _log(f"{self}: started PID {self.proc.pid}")

    def _backoff(self, now, reason):
        if self.started and now - self.started >= STABLE_SECONDS:
            self.failures = 0
        delay = BACKOFF_SECONDS[min(self.failures, len(BACKOFF_SECONDS) - 1)]
        self.failures += 1
        self.next_start = now + delay
        _log(f"{self}: {reason}; restarting in {delay}s")

    def tick(self, now, *, may_start=True):
        """Keep the process running: restart it with backoff after it exits.

        Returns True when it attempted a start.
        """
        if self.proc is not None:
            code = self.proc.poll()
            if code is None:
                return False
            self.proc = None
            self._backoff(now, f"exited with code {code}")
        if now < self.next_start or not may_start:
            return False
        try:
            self.start(now)
        except Exception as exc:
            self.proc = None
            self.started = now
            self._backoff(now, f"start failed: {exc}")
        return True

    def check_health(self, now, publish):
        """Restart a wedged Gateway; tell subscribers about a long outage, once."""
        if now < self.next_health:
            return
        self.next_health = now + HEALTH_CHECK_SECONDS
        running = self.proc is not None and self.proc.poll() is None
        state = read_state(self.key, self.alias) if running else None
        if state and state.get("pid") != self.proc.pid:
            state = None
        if running:
            updated = state.get("updated") if state else None
            if updated != self.last_updated:
                self.last_updated, self.progress_at = updated, now
            elif now - self.progress_at >= STALL_SECONDS:
                _log(f"{self}: no progress for {int(now - self.progress_at)}s "
                     f"(status {state.get('status') if state else 'unknown'}); killing it for a restart")
                self.progress_at = now
                try:
                    self.proc.kill()
                except OSError:
                    pass
        status = (state or {}).get("status") or ("starting" if running else "stopped")
        if status == "ready":
            if self.alerted and now >= self.next_alert:
                down = ago(time.time() - self.outage_id) if self.outage_id else "a while"
                if self._publish(publish, f"listener-recovered:{self.key}:{self.outage_id}",
                                 f"Recovered: {self.describe_source()} is connected again after {down}. "
                                 "Messages sent while it was down are delivered late, marked as such.",
                                 "recovered"):
                    self.alerted = False
                else:
                    self.next_alert = now + ALERT_RETRY_SECONDS
            if not self.alerted:
                self.unready_since = self.outage_id = None
            return
        if self.unready_since is None:
            self.unready_since, self.outage_id = now, int(time.time())
        threshold = 0.0 if status == "token-rejected" else ALERT_SECONDS
        if self.alerted or now - self.unready_since < threshold or now < self.next_alert:
            return
        detail = (state or {}).get("detail")
        text = (f"{self.describe_source()} has not been connected for "
                f"{ago(now - self.unready_since)} (status: {status}"
                + (f"; last error: {detail}" if detail else "") + "). "
                "New messages are not arriving. It keeps retrying and delivers what was missed "
                "once it reconnects.")
        if status == "token-rejected":
            text = (f"{self.describe_source()} stopped: Discord rejected the account token "
                    f"({detail or 'run discord login'}). Nothing arrives until the token is replaced.")
        if self._publish(publish, f"listener-down:{self.key}:{self.outage_id}", text, status):
            self.alerted = True
        else:
            self.next_alert = now + ALERT_RETRY_SECONDS

    def _publish(self, publish, event_id, text, status):
        _log(f"{self}: {text}")
        try:
            publish(self.source_id(), event_id, f"[Discord listener notice] {text}", {
                "schemaVersion": 1, "accountAlias": self.alias, "kind": "listener_health",
                "status": status, "content": text,
            })
            return True
        except Exception as exc:
            _log(f"{self}: could not publish the listener notice: {exc}")
            return False

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
    def __init__(self, account_alias, *, list_subscriptions=None, registry=load_registry, publish=None):
        self.account_alias = account_alias
        self.list_subscriptions = list_subscriptions or _core_subscriptions
        self.publish = publish or _core_publish
        self.registry = registry
        self.registered = set(registry()["accounts"])
        self.desired = {(account_alias, ACCOUNT_GATEWAY)} if account_alias else set()
        self.gateways = {}
        self.running = True
        self.reconcile_requested = True
        self.next_reconcile = 0.0
        self.next_spawn = 0.0

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
        # DMs/@mentions first, then channels.
        for key in sorted(self.desired, key=lambda key: (key[1] != ACCOUNT_GATEWAY, key)):
            gateway = self.gateways.get(key)
            if gateway is None:
                registered = key[0] in self.registered
                gateway = self.gateways[key] = Gateway(*key, registered=registered)
            if gateway.tick(now, may_start=now >= self.next_spawn):
                self.next_spawn = now + START_SPACING_SECONDS
            gateway.check_health(now, self.publish)

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
