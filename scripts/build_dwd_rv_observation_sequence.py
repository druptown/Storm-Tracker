"""Build the inert official-DWD observation replay and print hash-only output."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from storm_tracker_worker.dwd_rv_completion_process_isolation import (
    DwdRvCompletionIsolationLimits,
)
from storm_tracker_worker.dwd_rv_observation_replay import (
    build_dwd_rv_observation_replay,
    receipt_summary,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--export-root", type=Path, required=True)
    parser.add_argument("--frames", type=int, default=36)
    parser.add_argument("--rss-mib", type=int, default=1536)
    parser.add_argument("--cpu-seconds", type=float, default=60.0)
    parser.add_argument("--wall-seconds", type=float, default=90.0)
    return parser


def main() -> int:
    args = _parser().parse_args()
    receipt = build_dwd_rv_observation_replay(
        source_directory=args.source.resolve(strict=True),
        work_root=args.work_root.resolve(strict=False),
        export_root=args.export_root.resolve(strict=False),
        expected_frame_count=args.frames,
        isolation_limits=DwdRvCompletionIsolationLimits(
            wall_seconds=args.wall_seconds,
            cpu_seconds=args.cpu_seconds,
            rss_bytes=args.rss_mib * 1024 * 1024,
        ),
    )
    print(json.dumps(receipt_summary(receipt), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
