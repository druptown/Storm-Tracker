"""Closed subprocess driver for durable provider bridge crash proofs.

The parent tests create all durable inputs before starting this process.  This
helper only reopens those local authorities, arms one allowlisted checkpoint,
and terminates through ``os._exit(73)`` when production reaches it.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import Callable, Sequence


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.acquisition_capture_recovery import (  # noqa: E402
    CaptureCapacityReconciler,
)
from storm_tracker_worker.dwd_rv_capture_handoff import (  # noqa: E402
    adapt_dwd_rv_raw_capture,
)
from storm_tracker_worker.raw_artifacts import (  # noqa: E402
    RawArtifactLimits,
    RawArtifactStore,
)
from storm_tracker_worker.raw_capacity_ledger import (  # noqa: E402
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)


CRASH_EXIT_CODE = 73
RECOVERY_NOW = 1_800_000_000.0
RECOVERY_TOTAL_BYTES = 64 * 1024 * 1024
DWD_HANDOFF_CHECKPOINTS = frozenset(
    {
        "handoff.after_source_verified",
        "handoff.after_target_receipt",
        "handoff.before_target_capture",
        "handoff.after_target_capture",
    }
)
ACQUISITION_RECOVERY_CHECKPOINTS = frozenset(
    {
        "reconcile.after_capture_verified",
        "reconcile.before_capacity_commit",
        "reconcile.after_capacity_commit",
    }
)


def _crash_at(checkpoint: str) -> Callable[[str], None]:
    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(CRASH_EXIT_CODE)

    return crash


def _recovery_limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=40 * 1024 * 1024,
        max_total_bytes=RECOVERY_TOTAL_BYTES,
        max_object_entries=32,
        max_staging_entries=4,
        max_receipt_entries=64,
        max_capture_entries=64,
        max_capture_members=4,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _recovery_profile() -> RawCapacityProfile:
    return RawCapacityProfile(
        global_max_bytes=RECOVERY_TOTAL_BYTES,
        global_max_entries=32,
        providers=(
            ProviderCapacityLimit("aviationweather", RECOVERY_TOTAL_BYTES, 32),
            ProviderCapacityLimit("dwd", RECOVERY_TOTAL_BYTES, 32),
            ProviderCapacityLimit("knmi", RECOVERY_TOTAL_BYTES, 32),
            ProviderCapacityLimit("opera", RECOVERY_TOTAL_BYTES, 32),
        ),
        max_reservations=32,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )


def _run_dwd_handoff(
    scenario_root: Path,
    checkpoint: str,
    source_capture_id: str,
) -> None:
    if checkpoint not in DWD_HANDOFF_CHECKPOINTS:
        raise SystemExit("DWD handoff checkpoint is not allowlisted")
    adapt_dwd_rv_raw_capture(
        source_capture_id=source_capture_id,
        source_raw_store=RawArtifactStore(
            (scenario_root / "source-raw").resolve()
        ),
        handoff_raw_store=RawArtifactStore(
            (scenario_root / "handoff-raw").resolve()
        ),
        fault_injector=_crash_at(checkpoint),
    )
    raise RuntimeError("allowlisted DWD handoff checkpoint was not reached")


def _run_acquisition_recovery(scenario_root: Path, checkpoint: str) -> None:
    if checkpoint not in ACQUISITION_RECOVERY_CHECKPOINTS:
        raise SystemExit("acquisition recovery checkpoint is not allowlisted")
    raw_store = RawArtifactStore(
        (scenario_root / "raw").resolve(),
        limits=_recovery_limits(),
    )
    raw_store.initialize()
    capacity_ledger = RawCapacityLedger(
        (scenario_root / "capacity.sqlite3").resolve(),
        profile=_recovery_profile(),
        clock=lambda: RECOVERY_NOW + 1.0,
    )
    CaptureCapacityReconciler(
        raw_store=raw_store,
        capacity_ledger=capacity_ledger,
        fault_injector=_crash_at(checkpoint),
    ).audit_existing()
    raise RuntimeError("allowlisted acquisition checkpoint was not reached")


def main(arguments: Sequence[str]) -> None:
    if not arguments:
        raise SystemExit(
            "usage: provider_crash_durable_bridge_child.py "
            "SCENARIO ROOT CHECKPOINT [SOURCE_CAPTURE_ID]"
        )
    scenario = arguments[0]
    if scenario == "dwd-handoff" and len(arguments) == 4:
        _run_dwd_handoff(
            Path(arguments[1]).resolve(),
            arguments[2],
            arguments[3],
        )
        return
    if scenario == "acquisition-recovery" and len(arguments) == 3:
        _run_acquisition_recovery(
            Path(arguments[1]).resolve(),
            arguments[2],
        )
        return
    raise SystemExit("provider crash scenario or argument count is not allowlisted")


if __name__ == "__main__":
    main(sys.argv[1:])
