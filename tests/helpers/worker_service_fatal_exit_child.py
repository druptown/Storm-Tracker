from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import sys
import threading

from storm_tracker_worker import service


_NEVER = threading.Event()


class _RaisedFatalService:
    def run(self) -> None:
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(_NEVER.wait)
        raise service._WorkerFirstRuntimeFatalError(
            "private-detail-must-not-be-printed"
        )


class _HungFatalService:
    _runtime_role = "worker_first"
    _worker_first_fatal_watchdog_seconds = 0.05

    def __init__(self) -> None:
        self._worker_first_runtime_fatal_event = threading.Event()

    def run(self) -> None:
        self._worker_first_runtime_fatal_event.set()
        _NEVER.wait()


if len(sys.argv) != 2 or sys.argv[1] not in {"raised", "watchdog"}:
    raise SystemExit(64)

selected = (
    _RaisedFatalService()
    if sys.argv[1] == "raised"
    else _HungFatalService()
)
service._run_service_main(selected)
