"""Build a bounded release-evidence bundle from two immutable soak artifacts."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

from storm_tracker_worker.operational_slo_evidence import (
    MAX_RUN_ARTIFACT_BYTES,
    OperationalSloEvidenceError,
    build_operational_slo_evidence,
    parse_operational_soak_run_artifact,
)


def _read_bounded(path: Path) -> bytes:
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_file():
        raise OperationalSloEvidenceError(f"not a regular file: {resolved}")
    size = resolved.stat().st_size
    if not 0 < size <= MAX_RUN_ARTIFACT_BYTES:
        raise OperationalSloEvidenceError(
            f"run artifact size is outside its bound: {resolved}"
        )
    payload = resolved.read_bytes()
    if len(payload) != size:
        raise OperationalSloEvidenceError(
            f"run artifact changed while it was read: {resolved}"
        )
    return payload


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate an integration-off baseline and worker-only candidate "
            "artifact and emit content-addressed operations.* release evidence."
        )
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = _arguments()
    try:
        baseline = parse_operational_soak_run_artifact(_read_bounded(args.baseline))
        candidate = parse_operational_soak_run_artifact(_read_bounded(args.candidate))
        bundle = build_operational_slo_evidence(baseline, candidate)
        destination = args.output.expanduser().resolve()
        if destination.exists():
            raise OperationalSloEvidenceError(
                f"refusing to overwrite evidence bundle: {destination}"
            )
        if not destination.parent.is_dir():
            raise OperationalSloEvidenceError(
                f"output directory does not exist: {destination.parent}"
            )
        encoded = (
            json.dumps(
                bundle.to_mapping(),
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("ascii")
        staging_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w+b",
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".staging",
                delete=False,
            ) as handle:
                staging_path = Path(handle.name)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            # Same-directory hardlink publication is atomic and exclusive: a
            # racing process cannot overwrite an already published bundle.
            os.link(staging_path, destination)
        finally:
            if staging_path is not None:
                try:
                    staging_path.unlink()
                except FileNotFoundError:
                    pass
        print(json.dumps({
            "baseline_run_id": bundle.baseline_run_id,
            "bundle_sha256": bundle.bundle_sha256,
            "candidate_run_id": bundle.candidate_run_id,
            "evidence_count": len(bundle.evidence),
            "output": str(destination),
        }, sort_keys=True))
        return 0
    except (OSError, OperationalSloEvidenceError) as exc:
        print(f"operational SLO evidence rejected: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
