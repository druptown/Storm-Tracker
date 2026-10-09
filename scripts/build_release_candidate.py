"""Build a deterministic local integration/worker candidate; never deploy it."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.release_artifacts import (  # noqa: E402
    ReleaseArtifactError,
    build_release_candidate,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bouw reproduceerbare, volledig herlezen Storm Tracker-archieven "
            "en een content-addressed evidencebestand. Er wordt niets gedeployed."
        )
    )
    parser.add_argument("--integration-root", type=Path, required=True)
    parser.add_argument("--worker-root", type=Path, required=True)
    parser.add_argument("--integration-version", required=True)
    parser.add_argument("--worker-version", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument(
        "--raw-capture-requirements",
        action="store_true",
        help=(
            "publish the separate portable RAW requirements-v2 commit marker "
            "after the unchanged v1 release artifacts"
        ),
    )
    parser.add_argument(
        "--sanitized-options-snapshot",
        type=Path,
        help=(
            "exact secret-free pre-target Supervisor snapshot; required only "
            "with --raw-capture-requirements"
        ),
    )
    arguments = parser.parse_args()
    if arguments.raw_capture_requirements != bool(
        arguments.sanitized_options_snapshot
    ):
        parser.error(
            "--raw-capture-requirements and --sanitized-options-snapshot "
            "must be supplied together"
        )
    return arguments


def main() -> int:
    args = _arguments()
    raw_pinset = None
    if args.raw_capture_requirements:
        from storm_tracker_worker.raw_capture_release_requirements import (
            MATERIALIZER_MEMBER_PATH,
            RawCaptureReleaseRequirementsError,
            derive_frozen_raw_capture_release_pinset,
            validate_frozen_sanitized_options_snapshot,
        )

        try:
            validate_frozen_sanitized_options_snapshot(
                args.sanitized_options_snapshot
            )
            raw_pinset = derive_frozen_raw_capture_release_pinset(
                materializer_path=(
                    args.worker_root
                    / MATERIALIZER_MEMBER_PATH.removeprefix(
                        "storm_tracker_worker/"
                    )
                ),
            )
        except RawCaptureReleaseRequirementsError as error:
            raise ReleaseArtifactError(
                f"RAW release preflight failed: {error}"
            ) from error
    raw = None
    if raw_pinset is None:
        result = build_release_candidate(
            integration_root=args.integration_root,
            worker_root=args.worker_root,
            integration_version=args.integration_version,
            worker_version=args.worker_version,
            output_directory=args.output_directory,
        )
    else:
        from storm_tracker_worker.raw_capture_release_requirements import (
            RAW_RELEASE_REQUIREMENTS_CONTRACT,
            RawCaptureReleaseRequirementsError,
            build_raw_capture_release_quartet,
        )

        try:
            quartet = build_raw_capture_release_quartet(
                integration_root=args.integration_root,
                worker_root=args.worker_root,
                integration_version=args.integration_version,
                worker_version=args.worker_version,
                output_directory=args.output_directory,
                pinset=raw_pinset,
                sanitized_options_snapshot=args.sanitized_options_snapshot,
            )
        except RawCaptureReleaseRequirementsError as error:
            raise ReleaseArtifactError(
                f"RAW quartet staging/publication failed: {error}"
            ) from error
        result = quartet.core_release
        raw = quartet.requirements
    report = {
        "deployment_performed": False,
        "evidence": str(result.evidence_path),
        "evidence_sha256": result.evidence_sha256,
        "integration_archive": str(result.integration_archive),
        "integration_sha256": result.integration_sha256,
        "worker_archive": str(result.worker_archive),
        "worker_sha256": result.worker_sha256,
    }
    if raw is not None:
        report.update({
            "dashboard_output": False,
            "deployment_bundle_complete": False,
            "live_target_verified": False,
            "meteorological_admission": False,
            "meteorological_output": False,
            "production_enable_ready": False,
            "raw_capture_requirements": str(raw.requirements_path),
            "raw_capture_requirements_contract": (
                RAW_RELEASE_REQUIREMENTS_CONTRACT
            ),
            "raw_capture_requirements_file_sha256": (
                raw.requirements_file_sha256
            ),
            "raw_capture_requirements_sha256": raw.requirements_sha256,
            "selected_target_profile": raw_pinset.selected_profile,
            "target_activation_ready": False,
            "target_activation_manifest_sha256": None,
            "target_materialization_required": True,
        })
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ReleaseArtifactError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
