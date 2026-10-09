"""Run the local nowcast-baseline primary/repeat evidence producer."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from storm_tracker_worker.offline_replay_nowcast_runner import (  # noqa: E402
    run_nowcast_baseline_primary_repeat_file,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute offline Eulerian and two-frame translation baselines "
            "twice from the exact same immutable replay bytes."
        ),
    )
    parser.add_argument("--replay-pack", type=Path, required=True)
    parser.add_argument(
        "--worker-package",
        type=Path,
        default=REPOSITORY_ROOT / "storm_tracker_worker",
    )
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    receipt = run_nowcast_baseline_primary_repeat_file(
        arguments.replay_pack,
        package_directory=arguments.worker_package,
    )
    document = receipt.to_mapping()
    body = (
        json.dumps(
            document,
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
