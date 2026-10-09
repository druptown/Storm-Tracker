"""Subprocess helper for an uncatchable pipeline-receipt store crash."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from storm_tracker_worker.certified_shadow_fact_pipeline import (
    CertifiedShadowFactPipelineReceipt,
    CertifiedShadowFactPipelineStore,
)


def main() -> int:
    if len(sys.argv) != 4:
        return 64
    database_path = Path(sys.argv[1]).resolve()
    receipt_path = Path(sys.argv[2]).resolve()
    crash_point = sys.argv[3]
    receipt = CertifiedShadowFactPipelineReceipt.from_mapping(
        json.loads(receipt_path.read_text(encoding="ascii"))
    )

    def fault(point: str) -> None:
        if point == crash_point:
            os._exit(73)

    store = CertifiedShadowFactPipelineStore(
        database_path,
        fault_injector=fault,
    )
    store.record(receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
