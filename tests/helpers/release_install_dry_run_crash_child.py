"""Abrupt child process for local release-switch recovery boundaries."""
from __future__ import annotations

import os
from pathlib import Path
import sys

import release_install_dry_run as dry_run_module
from release_install_dry_run import (
    LegacyRollbackPolicy,
    execute_release_install_rollback_dry_run,
    recover_release_install_dry_run,
)


def main() -> None:
    if len(sys.argv) == 4 and sys.argv[1] == "hold-lease":
        lease = dry_run_module._acquire_process_lease(Path(sys.argv[2]))
        print("lease-held", flush=True)
        if sys.stdin.buffer.readline().rstrip(b"\r\n") != b"release":
            raise SystemExit("invalid lease release signal")
        if sys.argv[3] == "abrupt":
            os._exit(73)
        if sys.argv[3] != "clean":
            raise SystemExit("invalid lease exit mode")
        lease.release()
        return
    if len(sys.argv) == 4 and sys.argv[1] == "recover":
        expected = sys.argv[3]

        def recovery_crash(point: str) -> None:
            if point == expected:
                os._exit(73)

        recover_release_install_dry_run(
            Path(sys.argv[2]), fault_injector=recovery_crash
        )
        raise SystemExit("requested recovery crash boundary was not reached")
    if len(sys.argv) != 11:
        raise SystemExit("invalid local dry-run crash-helper arguments")
    policy = LegacyRollbackPolicy(
        integration_version=sys.argv[7],
        integration_sha256=sys.argv[8],
        worker_version=sys.argv[9],
        worker_sha256=sys.argv[10],
    )
    expected = sys.argv[6]

    def crash(point: str) -> None:
        if point == expected:
            os._exit(73)

    execute_release_install_rollback_dry_run(
        candidate_evidence=Path(sys.argv[1]),
        rollback_integration_archive=Path(sys.argv[2]),
        rollback_worker_archive=Path(sys.argv[3]),
        options_snapshot=Path(sys.argv[4]),
        run_root=Path(sys.argv[5]),
        rollback_policy=policy,
        fault_injector=crash,
    )
    raise SystemExit("requested crash boundary was not reached")


if __name__ == "__main__":
    main()
