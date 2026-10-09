from __future__ import annotations

import signal

import pytest

from storm_tracker_worker import cpu_limited_launcher as launcher


def test_arguments_require_bounded_limit_and_command() -> None:
    assert launcher._arguments([
        "--limit",
        "50",
        "--",
        "python",
        "-m",
        "storm_tracker_worker.service",
    ]) == (50, ["python", "-m", "storm_tracker_worker.service"])

    assert launcher._arguments([
        "--limit",
        "200",
        "--",
        "python",
    ]) == (200, ["python"])

    with pytest.raises(SystemExit, match="tussen 25 en 200"):
        launcher._arguments(["--limit", "24", "--", "python"])
    with pytest.raises(SystemExit, match="tussen 25 en 200"):
        launcher._arguments(["--limit", "201", "--", "python"])
    with pytest.raises(SystemExit, match="Workercommando ontbreekt"):
        launcher._arguments(["--limit", "50"])


def test_signal_return_code_is_shell_compatible() -> None:
    assert launcher._exit_code(0) == 0
    assert launcher._exit_code(7) == 7
    assert launcher._exit_code(-15) == 143


def test_affinity_capacity_is_a_hard_percent_ceiling(monkeypatch) -> None:
    monkeypatch.setattr(
        launcher.os,
        "sched_getaffinity",
        lambda _pid: {1, 3},
        raising=False,
    )
    assert launcher._affinity_capacity_percent() == 200


def test_run_skips_racy_userspace_limiter_when_affinity_is_tighter(
    monkeypatch,
) -> None:
    class FakeWorker:
        pid = 321
        returncode = None

        def poll(self):
            if self.returncode is None:
                self.returncode = 0
                return None
            return self.returncode

    worker = FakeWorker()
    monkeypatch.setattr(launcher, "_affinity_capacity_percent", lambda: 200)
    monkeypatch.setattr(launcher.shutil, "which", lambda _name: pytest.fail(
        "cpulimit mag niet worden gezocht wanneer taskset de grens afdwingt"
    ))
    monkeypatch.setattr(launcher.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(
        launcher.subprocess,
        "Popen",
        lambda *_args, **_kwargs: worker,
    )
    monkeypatch.setattr(launcher.time, "sleep", lambda _seconds: None)

    assert launcher.run(["--limit", "200", "--", "python"]) == 0


def test_process_state_exposes_a_paused_limiter(tmp_path) -> None:
    process = tmp_path / "321"
    process.mkdir()
    (process / "status").write_text(
        "Name:\tcpulimit\nState:\tT (stopped)\n",
        encoding="utf-8",
    )

    assert launcher._process_state(321, proc_root=tmp_path) == "T"
    assert "T" in launcher.STOPPED_PROCESS_STATES
    assert launcher._process_state(999, proc_root=tmp_path) is None


def test_run_stops_worker_fail_closed_when_limiter_is_paused(monkeypatch) -> None:
    class FakeProcess:
        def __init__(self, *, worker: bool) -> None:
            self.pid = 321 if worker else 654
            self.returncode = None
            self.signals: list[int] = []

        def poll(self):
            return self.returncode

        def send_signal(self, signum: int) -> None:
            self.signals.append(signum)
            if signum == signal.SIGTERM:
                self.returncode = 0

        def terminate(self) -> None:
            self.returncode = 0

        def kill(self) -> None:
            self.returncode = -signal.SIGKILL

        def wait(self, timeout=None):
            return self.returncode

    worker = FakeProcess(worker=True)
    limiter = FakeProcess(worker=False)
    processes = iter((worker, limiter))
    monkeypatch.setattr(launcher.shutil, "which", lambda _name: "/usr/bin/cpulimit")
    monkeypatch.setattr(launcher.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(launcher.signal, "SIGCONT", 18, raising=False)
    monkeypatch.setattr(launcher.subprocess, "Popen", lambda *_args, **_kwargs: next(processes))
    monkeypatch.setattr(launcher, "_process_state", lambda _pid: "T")

    assert launcher.run(["--limit", "25", "--", "python", "worker.py"]) == 70
    assert worker.signals[:2] == [18, signal.SIGTERM]
