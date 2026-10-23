import os
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from src import notify, supervisor
from src.channel_notifications import PREFIX

A = "709232473783140375"
B = "1558342249048186951"
REGISTRY = {"accounts": {"paramount": {}, "personal": {}}}


def channel(alias, channel_id, **fields):
    return {"sourceId": f"account:{alias}:channel:{channel_id}:messages", "enabled": True, **fields}


class FakeProc:
    next_pid = 1000

    def __init__(self, argv, **kwargs):
        FakeProc.next_pid += 1
        self.pid = FakeProc.next_pid
        self.argv = argv
        self.kwargs = kwargs
        self.returncode = None
        self.terminated = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -signal.SIGTERM

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        self.returncode = -signal.SIGKILL


class SupervisorTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)

        def listener_dir(alias=None):
            path = root / (alias or "legacy")
            path.mkdir(exist_ok=True)
            return path

        for target, value in [
            ("listener_dir", listener_dir),
            ("runtime_root", lambda: root),
            ("_log", lambda message: None),
            ("START_SPACING_SECONDS", 0.0),
            ("read_state", lambda key, alias=None: None),
        ]:
            patcher = patch.object(supervisor, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.root = root
        self.subscriptions = []

    def make(self, alias: "str | None" = "paramount"):
        return supervisor.Supervisor(
            alias, list_subscriptions=lambda: list(self.subscriptions), registry=lambda: REGISTRY,
            publish=lambda *args: None,
        )


class DesiredGatewayTests(unittest.TestCase):
    def test_reconciles_core_routes_per_account(self):
        subscriptions = [
            channel("paramount", A),
            channel("paramount", A, convId="second-subscriber"),
            channel("personal", A),
            channel("personal", B, enabled=False),
            channel("ghost", B),
            {"sourceId": "account:personal:notifications", "enabled": True},
            {"sourceId": "account:paramount:channel:not-a-snowflake:messages"},
            {"sourceId": "twitter:whatever"},
            "garbage",
        ]
        self.assertEqual(
            supervisor.desired_gateways(subscriptions, {"paramount", "personal"}, "paramount"),
            {("paramount", "__notify__"), ("paramount", PREFIX + A),
             ("personal", PREFIX + A), ("personal", "__notify__")},
        )

    def test_no_account_gateway_without_supervised_account(self):
        self.assertEqual(supervisor.desired_gateways([], {"paramount"}, None), set())


class SupervisorLifecycleTests(SupervisorTestCase):
    def setUp(self):
        super().setUp()
        for patcher in (patch.object(supervisor.subprocess, "Popen", FakeProc),
                        patch.object(supervisor.Gateway, "unsupervised_pids", lambda gateway: [])):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_subscribed_channel_starts_in_daemon_process_group(self):
        self.subscriptions = [channel("paramount", A)]
        sup = self.make()
        sup.step(100.0)
        self.assertEqual(set(sup.gateways), {("paramount", "__notify__"), ("paramount", PREFIX + A)})
        proc = sup.gateways[("paramount", PREFIX + A)].proc
        self.assertEqual(proc.argv[2:], [PREFIX + A, str(self.root / "paramount" / f"{PREFIX}{A}.log")])
        self.assertEqual(proc.kwargs["env"]["DISCORD_ACCOUNT"], "paramount")
        self.assertNotIn("start_new_session", proc.kwargs)
        self.assertEqual(sup.gateways[("paramount", "__notify__")].proc.kwargs["stderr"].name,
                         str(self.root / "paramount" / "__notify__.err"))

    def test_restarts_dead_listener_with_backoff(self):
        self.subscriptions = [channel("paramount", A)]
        sup = self.make()
        sup.step(100.0)
        gateway = sup.gateways[("paramount", PREFIX + A)]
        first = gateway.proc
        first.returncode = -signal.SIGKILL
        sup.step(100.5)
        self.assertIsNone(gateway.proc)  # 1s backoff
        sup.step(101.5)
        second = gateway.proc
        self.assertNotEqual(second.pid, first.pid)
        second.returncode = 1
        sup.step(102.0)
        sup.step(103.5)
        self.assertIsNone(gateway.proc)  # 2s backoff after a quick second failure
        sup.step(104.0)
        self.assertIsNotNone(gateway.proc)
        # Stable uptime resets the backoff.
        gateway.proc.returncode = 1
        sup.step(104.0 + supervisor.STABLE_SECONDS)
        self.assertEqual(gateway.next_start, 105.0 + supervisor.STABLE_SECONDS)

    def test_last_unsubscribe_stops_only_that_listener(self):
        self.subscriptions = [channel("paramount", A), channel("paramount", B)]
        sup = self.make()
        sup.step(100.0)
        stopped = sup.gateways[("paramount", PREFIX + A)].proc
        kept = sup.gateways[("paramount", PREFIX + B)].proc
        self.subscriptions = [channel("paramount", B)]
        sup._reconcile_now()
        sup.step(101.0)
        self.assertTrue(stopped.terminated)
        self.assertFalse(kept.terminated)
        self.assertEqual(set(sup.gateways), {("paramount", "__notify__"), ("paramount", PREFIX + B)})

    def test_polls_registry_without_a_nudge(self):
        sup = self.make()
        sup.step(100.0)
        self.subscriptions = [channel("paramount", A)]
        sup.step(101.0)
        self.assertNotIn(("paramount", PREFIX + A), sup.gateways)
        sup.step(100.0 + supervisor.RECONCILE_SECONDS)
        self.assertIn(("paramount", PREFIX + A), sup.gateways)

    def test_multiple_accounts_are_isolated(self):
        self.subscriptions = [channel("paramount", A), channel("personal", A),
                              {"sourceId": "account:personal:notifications", "enabled": True}]
        sup = self.make()
        sup.step(100.0)
        for alias in ("paramount", "personal"):
            for key in ("__notify__", PREFIX + A):
                proc = sup.gateways[(alias, key)].proc
                self.assertEqual(proc.kwargs["env"]["DISCORD_ACCOUNT"], alias)
                self.assertEqual(proc.argv[3], str(self.root / alias / f"{key}.log"))
        self.subscriptions = [channel("paramount", A)]
        sup._reconcile_now()
        sup.step(101.0)
        self.assertEqual(set(sup.gateways), {("paramount", "__notify__"), ("paramount", PREFIX + A)})

    def test_starts_are_spaced_out_account_gateway_first(self):
        self.subscriptions = [channel("paramount", A), channel("paramount", B)]
        sup = self.make()
        with patch.object(supervisor, "START_SPACING_SECONDS", 5.0):
            sup.step(100.0)
            running = lambda: {key for key, gateway in sup.gateways.items() if gateway.proc}
            self.assertEqual(running(), {("paramount", "__notify__")})
            sup.step(104.0)
            self.assertEqual(len(running()), 1)
            sup.step(105.0)
            sup.step(109.0)
            self.assertEqual(len(running()), 2)
            sup.step(110.0)
            self.assertEqual(len(running()), 3)

    def test_registry_failure_keeps_running_listeners(self):
        self.subscriptions = [channel("paramount", A)]
        sup = self.make()
        sup.step(100.0)
        proc = sup.gateways[("paramount", PREFIX + A)].proc
        sup.list_subscriptions = lambda: (_ for _ in ()).throw(RuntimeError("exocortexd is not running"))
        sup._reconcile_now()
        sup.step(101.0)
        self.assertIs(sup.gateways[("paramount", PREFIX + A)].proc, proc)
        self.assertFalse(proc.terminated)

    def test_legacy_install_runs_without_account_env(self):
        sup = supervisor.Supervisor("legacy", list_subscriptions=lambda: [],
                                    registry=lambda: {"accounts": {}})
        with patch.dict(os.environ, {"DISCORD_ACCOUNT": "stale"}):
            sup.step(100.0)
        proc = sup.gateways[("legacy", "__notify__")].proc
        self.assertNotIn("DISCORD_ACCOUNT", proc.kwargs["env"])

    def test_shutdown_terminates_every_gateway(self):
        self.subscriptions = [channel("paramount", A)]
        sup = self.make()
        sup.step(100.0)
        procs = [gateway.proc for gateway in sup.gateways.values()]
        sup.shutdown()
        self.assertTrue(all(proc.terminated for proc in procs))
        self.assertEqual(sup.gateways, {})

    def test_unsupervised_copy_is_stopped_before_start(self):
        self.subscriptions = [channel("paramount", A)]
        sup = self.make()
        with (patch.object(supervisor.Gateway, "unsupervised_pids", lambda gateway: [4242]),
              patch.object(notify, "_stop_notify_pids", return_value=([4242], [])) as stop):
            sup.step(100.0)
        self.assertEqual(stop.call_count, 2)
        stop.assert_called_with([4242])
        self.assertIsNotNone(sup.gateways[("paramount", PREFIX + A)].proc)


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


class SupervisorProcessTests(SupervisorTestCase):
    """Real child processes: a stand-in gateway.py that just sleeps."""

    def setUp(self):
        super().setUp()
        script = self.root / "bin" / "gateway.py"
        script.parent.mkdir()
        script.write_text("import time\ntime.sleep(60)\n")
        patcher = patch.object(supervisor, "GATEWAY_SCRIPT", script)
        patcher.start()
        self.addCleanup(patcher.stop)

    def wait_for(self, sup, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            sup.step(time.monotonic())
            if predicate():
                return
            time.sleep(0.05)
        self.fail("condition not reached")

    def test_killed_listener_is_restarted_and_stray_copy_replaced(self):
        self.subscriptions = [channel("paramount", A)]
        sup = self.make(alias=None)
        self.addCleanup(sup.shutdown)
        key = ("paramount", PREFIX + A)
        self.wait_for(sup, lambda: key in sup.gateways and sup.gateways[key].proc is not None)
        gateway = sup.gateways[key]
        first = gateway.proc.pid
        self.assertEqual(gateway.unsupervised_pids(), [])
        os.kill(first, signal.SIGKILL)
        self.wait_for(sup, lambda: gateway.proc is not None and gateway.proc.pid != first)
        second = gateway.proc.pid

        # A copy started outside the daemon (e.g. orphaned by a turn) is found
        # by its exact command line and replaced on the next start.
        launcher = " ".join(f"'{part}'" for part in gateway.command())
        stray = int(subprocess.check_output(
            ["sh", "-c", f"{launcher} >/dev/null 2>&1 & echo $!"], text=True))
        self.addCleanup(lambda: alive(stray) and os.kill(stray, signal.SIGKILL))
        self.wait_for(sup, lambda: stray in gateway.unsupervised_pids())
        os.kill(second, signal.SIGKILL)
        self.wait_for(sup, lambda: not alive(stray) and gateway.proc is not None
                      and gateway.proc.pid != second)

        self.subscriptions = []
        sup._reconcile_now()
        last = gateway.proc
        sup.step(time.monotonic())
        self.assertIsNotNone(last.poll())
        self.assertEqual(sup.gateways, {})


class SupervisorCliTests(SupervisorTestCase):
    def test_request_reconcile_signals_only_the_daemon(self):
        (self.root / "supervisor.pid").write_text("4321\n")
        with (patch("subprocess.check_output", return_value="python3 -m src.notify __daemon__\n"),
              patch("os.kill") as kill):
            self.assertEqual(supervisor.request_reconcile(), 4321)
        kill.assert_called_once_with(4321, signal.SIGHUP)
        with (patch("subprocess.check_output", return_value="vim notes.txt\n"),
              patch("os.kill") as kill):
            self.assertIsNone(supervisor.request_reconcile())
        kill.assert_not_called()

    def test_sighup_requests_reconcile(self):
        sup = self.make()
        sup.reconcile_requested = False
        sup._reconcile_now(signal.SIGHUP, None)
        self.assertTrue(sup.reconcile_requested)

    def test_daemon_mode_migrates_then_supervises_account(self):
        with (patch.object(notify, "_migrate_relay_targets") as migrate,
              patch.object(notify, "supervised_notify_alias", return_value="paramount"),
              patch.object(supervisor.Supervisor, "__init__", return_value=None) as init,
              patch.object(supervisor.Supervisor, "run") as run):
            notify.main(["__daemon__"])
        migrate.assert_called_once_with()
        init.assert_called_once_with("paramount")
        run.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
