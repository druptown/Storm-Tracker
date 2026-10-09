"""Run the worker tree behind a fail-closed Linux CPU limiter."""
from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path


LIMITER_FAILURE_EXIT = 70
LIMITER_START_EXIT = 71
STOP_GRACE_SECONDS = 25.0
POLL_SECONDS = 0.1
STOPPED_PROCESS_STATES = frozenset({"T", "t"})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def _arguments(argv: Sequence[str] | None) -> tuple[int, list[str]]:
    args = _parser().parse_args(argv)
    command = list(args.command)
    if command[:1] == ["--"]:
        command = command[1:]
    if not 25 <= args.limit <= 100:
        raise SystemExit("CPU-limiet moet tussen 25 en 100 procent liggen")
    if not command:
        raise SystemExit("Workercommando ontbreekt")
    return args.limit, command


def _exit_code(return_code: int) -> int:
    return return_code if return_code >= 0 else 128 + abs(return_code)


def _process_state(pid: int, *, proc_root: Path = Path("/proc")) -> str | None:
    """Return the Linux process state, or ``None`` after process exit."""
    try:
        status = (proc_root / str(pid) / "status").read_text(encoding="utf-8")
    except (FileNotFoundError, ProcessLookupError):
        return None
    for line in status.splitlines():
        if line.startswith("State:"):
            fields = line.split()
            return fields[1] if len(fields) >= 2 else None
    return None


def _terminate_limiter(limiter: subprocess.Popen[bytes]) -> None:
    if limiter.poll() is None:
        limiter.terminate()
        try:
            limiter.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            limiter.kill()
            limiter.wait(timeout=2.0)


def _request_worker_stop(
    worker: subprocess.Popen[bytes],
    limiter: subprocess.Popen[bytes] | None,
    signum: int,
) -> None:
    if limiter is not None:
        _terminate_limiter(limiter)
    if worker.poll() is None:
        # The limiter may have paused the worker exactly when shutdown began.
        # Resume first so Python can receive SIGTERM and release its fences.
        worker.send_signal(signal.SIGCONT)
        worker.send_signal(signum)


def run(argv: Sequence[str] | None = None) -> int:
    limit, command = _arguments(argv)
    cpulimit = shutil.which("cpulimit")
    if cpulimit is None:
        print("CPU-limiter ontbreekt; worker start fail-closed", file=sys.stderr)
        return LIMITER_START_EXIT

    pending_signal: int | None = None

    def remember_signal(signum: int, _frame: object) -> None:
        nonlocal pending_signal
        if pending_signal is None:
            pending_signal = signum

    signal.signal(signal.SIGINT, remember_signal)
    signal.signal(signal.SIGTERM, remember_signal)

    worker = subprocess.Popen(command, start_new_session=True)
    try:
        limiter = subprocess.Popen([
            cpulimit,
            "--pid",
            str(worker.pid),
            "--limit",
            str(limit),
            "--include-children",
            "--lazy",
        ])
    except BaseException:
        worker.send_signal(signal.SIGTERM)
        try:
            worker.wait(timeout=STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait()
        print("CPU-limiter kon niet starten; worker gestopt", file=sys.stderr)
        return LIMITER_START_EXIT

    limiter_failed = False
    stop_deadline: float | None = None
    while worker.poll() is None:
        if pending_signal is not None and stop_deadline is None:
            _request_worker_stop(worker, limiter, pending_signal)
            stop_deadline = time.monotonic() + STOP_GRACE_SECONDS
        if limiter.poll() is not None and stop_deadline is None:
            limiter_failed = True
            _request_worker_stop(worker, None, signal.SIGTERM)
            stop_deadline = time.monotonic() + STOP_GRACE_SECONDS
        if (
            stop_deadline is None
            and _process_state(limiter.pid) in STOPPED_PROCESS_STATES
        ):
            # A paused limiter is equivalent to no limiter.  Fail closed so a
            # SIGSTOP-based "temporary boost" can never expose HA/Z2M to an
            # unrestricted worker.
            limiter_failed = True
            # Stop the now-unrestricted worker first.  A SIGSTOPped limiter
            # may otherwise take the full cleanup timeout before it accepts a
            # normal termination signal.
            _request_worker_stop(worker, None, signal.SIGTERM)
            _terminate_limiter(limiter)
            stop_deadline = time.monotonic() + STOP_GRACE_SECONDS
        if stop_deadline is not None and time.monotonic() >= stop_deadline:
            os.killpg(worker.pid, signal.SIGKILL)
        time.sleep(POLL_SECONDS)

    _terminate_limiter(limiter)
    if limiter_failed:
        print("CPU-limiter stopte of werd gepauzeerd; worker fail-closed", file=sys.stderr)
        return LIMITER_FAILURE_EXIT
    assert worker.returncode is not None
    return _exit_code(worker.returncode)


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
