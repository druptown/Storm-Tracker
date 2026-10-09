"""Verify and simulate the ordinary-HA integration-only release route."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ordinary_ha_release_dry_run import (  # noqa: E402
    OrdinaryHaDryRunError,
    execute_ordinary_ha_integration_only_dry_run,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verifieer lokaal de bestaande v1 pair-release-evidence, de "
            "sealed workerbaseline en "
            "simuleer uitsluitend installatie en exact same-version herstel "
            "van de Home Assistant-integratie, inclusief canonieke "
            "niet-overlappende doelpaden, exact één actief Storm "
            "Tracker-manifest en de expliciete normale-HA legacyroute. "
            "Er wordt niets gedeployed."
        )
    )
    parser.add_argument("--candidate-evidence", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    result = execute_ordinary_ha_integration_only_dry_run(
        candidate_evidence=arguments.candidate_evidence,
        run_root=arguments.run_root,
    )
    report = result.report
    target_filesystem = report["target_filesystem_contract"]
    print(json.dumps({
        "canonical_component_layout_contract_verified": target_filesystem[
            "canonical_paths_verified"
        ],
        "deployment_mode": report["deployment_mode"],
        "deployment_performed": False,
        "exact_one_storm_tracker_manifest_contract_verified": target_filesystem[
            "exact_one_storm_tracker_manifest_below_custom_components_verified"
        ],
        "integration_installation_only": True,
        "legacy_runtime_static_contract_verified": report[
            "static_runtime_contract"
        ]["legacy_local_processing"],
        "live_installation_ready": False,
        "live_paths_touched": False,
        "local_simulation_passed": report["local_simulation"][
            "local_simulation_passed"
        ],
        "network_performed": False,
        "report": str(result.report_path),
        "report_file_sha256": result.report_file_sha256,
        "report_sha256": report["report_sha256"],
        "unresolved_live_gates": report["blocker_namespaces"]["live"],
        "worker_archive_installed": False,
        "worker_baseline": report["pair_release_evidence"][
            "sealed_worker_baseline"
        ],
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, OrdinaryHaDryRunError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
