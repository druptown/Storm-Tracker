"""Emit the local fail-closed remaining-baseline executability receipt."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from storm_tracker_worker.offline_replay_remaining_baseline_audit import (  # noqa: E402
    audit_remaining_baseline_executability_file,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Content-address and fail closed the executability decision for "
            "the three remaining offline-replay baselines."
        ),
    )
    parser.add_argument("--replay-pack", type=Path, required=True)
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=REPOSITORY_ROOT,
    )
    parser.add_argument(
        "--worker-package",
        type=Path,
        default=REPOSITORY_ROOT / "storm_tracker_worker",
    )
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    receipt = audit_remaining_baseline_executability_file(
        arguments.replay_pack,
        repository_directory=arguments.repository_root,
        package_directory=arguments.worker_package,
    )
    body = (
        json.dumps(
            receipt.to_mapping(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    sys.stdout.write(body)
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
