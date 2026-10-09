"""Thin CLI for target-side raw-capture activation materialization."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.raw_capture_activation_artifacts import (  # noqa: E402,F401
    RawCaptureArtifactBuild,
    RawCaptureArtifactBuildError,
    RawCaptureCapacityPlan,
    build_raw_capture_shadow_artifacts,
    derive_raw_capture_shadow_capacity,
    verify_raw_capture_shadow_artifacts,
)


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize a new host/path-pinned raw_capture_shadow config, "
            "authority, capacity profile and SHA-256 manifest on its final "
            "target filesystem. No runtime is started or deployed."
        )
    )
    parser.add_argument("--persistent-base-root", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--capture-window-hours", required=True, type=int)
    parser.add_argument("--target-raw-capacity-bytes", required=True, type=int)
    parser.add_argument(
        "--blitzortung-profile",
        required=True,
        choices=("disabled", "enabled"),
        help=(
            "Explicitly claim or omit Blitzortung event-ingress capacity."
        ),
    )
    parser.add_argument(
        "--operator-host-reserve-bytes",
        required=True,
        type=int,
        help=(
            "Operator-declared host operational free-space reserve; the "
            "materializer requires at least 2 GiB."
        ),
    )
    parser.add_argument("--expected-catalog-sha256", required=True)
    parser.add_argument("--expected-activation-matrix-sha256", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _arguments(argv)
    result = build_raw_capture_shadow_artifacts(
        persistent_base_root=args.persistent_base_root,
        output_directory=args.output_directory,
        capture_window_hours=args.capture_window_hours,
        target_raw_capacity_bytes=args.target_raw_capacity_bytes,
        blitzortung_enabled=args.blitzortung_profile == "enabled",
        operator_host_operational_free_reserve_bytes=(
            args.operator_host_reserve_bytes
        ),
        expected_catalog_sha256=args.expected_catalog_sha256,
        expected_activation_matrix_sha256=(
            args.expected_activation_matrix_sha256
        ),
    )
    print(json.dumps({
        "activation_environment": dict(result.activation_environment),
        "artifact_role": "target_side_materialization",
        "authority_sha256": result.authority_sha256,
        "capacity_path": str(result.capacity_path),
        "capacity_sha256": result.capacity_sha256,
        "config_path": str(result.config_path),
        "config_sha256": result.config_sha256,
        "deployment_performed": False,
        "host_path_pinned": True,
        "manifest_path": str(result.manifest_path),
        "manifest_sha256": result.manifest_sha256,
        "output_directory": str(result.output_directory),
        "portable_release_artifact": False,
        "reproducibility_scope": (
            "same_canonical_target_root_and_path_semantics_only"
        ),
        "target_materialization_required": True,
        "target_path_flavour": "windows" if os.name == "nt" else "posix",
    }, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RawCaptureArtifactBuildError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
