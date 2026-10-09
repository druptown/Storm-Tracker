"""Build one read-only RAW target preflight and recovery-plan evidence file."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from raw_capture_target_preflight import (  # noqa: E402
    TargetCanaryPreflightError,
    build_raw_capture_target_canary_preflight,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verifieer lokaal het RAW-releasekwartet, materializerpins, "
            "de cadence-only materializerparameters, capaciteit, "
            "Supervisor-snapshot en rollbackidentiteiten. "
            "Publiceer alleen een herstelplan/evidence; deploy niets."
        )
    )
    parser.add_argument("--raw-requirements", type=Path, required=True)
    parser.add_argument(
        "--sanitized-options-snapshot",
        type=Path,
        required=True,
    )
    parser.add_argument("--materializer-module", type=Path, required=True)
    parser.add_argument(
        "--rollback-integration-archive",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--rollback-worker-archive",
        type=Path,
        required=True,
    )
    parser.add_argument("--persistent-base-root", type=Path, required=True)
    parser.add_argument("--planned-output-directory", type=Path, required=True)
    parser.add_argument(
        "--operator-declared-target-bytes",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--evidence-output-directory",
        type=Path,
        required=True,
        help="new absent directory outside every runtime/rollback root",
    )
    return parser.parse_args()


def main() -> int:
    args = _arguments()
    result = build_raw_capture_target_canary_preflight(
        requirements_path=args.raw_requirements,
        sanitized_options_snapshot=args.sanitized_options_snapshot,
        materializer_path=args.materializer_module,
        rollback_integration_archive=args.rollback_integration_archive,
        rollback_worker_archive=args.rollback_worker_archive,
        persistent_base_root=args.persistent_base_root,
        planned_output_directory=args.planned_output_directory,
        operator_declared_target_bytes=args.operator_declared_target_bytes,
        evidence_output_directory=args.evidence_output_directory,
    )
    print(json.dumps({
        "canary_execution_authorized": False,
        "canary_started": False,
        "dashboard_output": False,
        "deployment_bundle_complete": False,
        "deployment_performed": False,
        "evidence": str(result.evidence_path),
        "evidence_sha256": result.evidence_sha256,
        "live_ha_mutation_performed": False,
        "live_target_verified": False,
        "local_preflight_blockers": list(result.local_preflight_blockers),
        "local_preflight_passed": result.local_preflight_passed,
        "network_performed": False,
        "meteorological_admission": False,
        "meteorological_output": False,
        "production_enable_ready": False,
        "real_installation_ready": False,
        "supervisor_mutation_performed": False,
        "target_materialization_performed": False,
        "target_activation_ready": False,
    }, ensure_ascii=False, sort_keys=True))
    return 0 if result.local_preflight_passed else 3


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, TargetCanaryPreflightError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
