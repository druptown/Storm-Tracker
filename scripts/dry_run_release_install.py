"""Run a Storm Tracker install/upgrade/rollback simulation locally only."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from release_install_dry_run import (  # noqa: E402
    DryRunError,
    execute_release_install_rollback_dry_run,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Simuleer lokaal een Storm Tracker-upgrade en rollback in een "
            "nieuw temp/.analysis-runroot. Er wordt niets gedeployed."
        )
    )
    parser.add_argument("--candidate-evidence", type=Path, required=True)
    parser.add_argument("--rollback-integration-archive", type=Path, required=True)
    parser.add_argument("--rollback-worker-archive", type=Path, required=True)
    parser.add_argument("--sanitized-options-snapshot", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _arguments()
    result = execute_release_install_rollback_dry_run(
        candidate_evidence=args.candidate_evidence,
        rollback_integration_archive=args.rollback_integration_archive,
        rollback_worker_archive=args.rollback_worker_archive,
        options_snapshot=args.sanitized_options_snapshot,
        run_root=args.run_root,
    )
    report = result.report
    print(json.dumps({
        "deployment_performed": False,
        "atomic_noreplace_publication_proven": report[
            "atomic_noreplace_publication_proven"
        ],
        "blocker_namespaces": report["blocker_namespaces"],
        "destination_absence_prechecked": True,
        "installation_preflight_blockers": report[
            "installation_preflight_blockers"
        ],
        "live_paths_touched": False,
        "local_acceptance_blockers": report["local_acceptance_blockers"],
        "local_acceptance_blockers_closed": report[
            "local_acceptance_blockers_closed"
        ],
        "local_simulation_passed": report["local_simulation_passed"],
        "network_performed": False,
        "real_installation_ready": False,
        "report": str(result.report_path),
        "report_sha256": result.report_sha256,
        "simulation_completed": report["simulation_completed"],
        "supervisor_installation_blockers": report[
            "supervisor_installation_blockers"
        ],
        "supervisor_options_roundtrip_proven": report[
            "supervisor_options_roundtrip_proven"
        ],
        "unresolved_external_gates": report["unresolved_external_gates"],
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (DryRunError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
