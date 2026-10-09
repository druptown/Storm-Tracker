"""Canonical ground decoding used by the worker-only runtime.

The provider-specific decoders already own exact raw-byte verification and
append-only canonical-ground persistence.  This module is the deliberately
small live routing boundary: it selects only the three workloads whose
canonical decoders exist and returns their immutable receipts.  It performs no
wet/dry inference, consensus, forecast, admission or publication.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable
import time

from .canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
    GroundObservationReceipt,
)
from .metar_ground_evidence import (
    MetarGroundTargetScope,
    decode_metar_ground_capture,
)
from .multi_source_ground_facts import GroundTargetScope
from .netatmo_ground_decoder import decode_netatmo_ground_capture
from .raw_artifacts import RawArtifactStore
from .synoptic_ground_decoder import decode_synoptic_ground_capture


GROUND_WORKLOAD_IDS = frozenset({
    "ground.metar.global",
    "ground.netatmo.region",
    "ground.synoptic.region",
})


class WorkerOnlyGroundProcessingError(RuntimeError):
    """One supported canonical-ground capture could not be re-proved."""


def decode_worker_only_ground_receipts(
    *,
    workload_id: str,
    capture_id: str,
    raw_store: RawArtifactStore,
    evidence_path: str | Path,
    target_scopes: tuple[GroundTargetScope, ...] = (),
    clock: Callable[[], float] = time.time,
) -> tuple[GroundObservationReceipt, ...]:
    """Decode one supported capture into durable canonical receipts.

    Exact retries are intentionally safe: every provider decoder and the
    canonical evidence store are content-addressed and idempotent.
    """

    if workload_id not in GROUND_WORKLOAD_IDS:
        raise WorkerOnlyGroundProcessingError(
            "ground_workload_not_certified"
        )
    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store heeft het verkeerde type")
    if not callable(clock):
        raise TypeError("clock moet callable zijn")
    if not isinstance(target_scopes, tuple) or any(
        not isinstance(item, GroundTargetScope) for item in target_scopes
    ):
        raise TypeError("target_scopes moet een tuple GroundTargetScope zijn")

    ground_store = CanonicalGroundEvidenceStore(
        Path(evidence_path),
        raw_artifact_reader=raw_store,
        clock=clock,
    )
    if workload_id == "ground.metar.global":
        metar_scopes = tuple(
            MetarGroundTargetScope(
                target_id=scope.target.target_id,
                latitude=scope.target.latitude,
                longitude=scope.target.longitude,
                radius_km=scope.radius_km,
                available=scope.target.available,
            )
            for scope in target_scopes
        )
        result = decode_metar_ground_capture(
            capture_id,
            raw_store=raw_store,
            evidence_store=ground_store,
            target_scopes=metar_scopes,
        )
        receipts = result.receipts
    elif workload_id == "ground.netatmo.region":
        result = decode_netatmo_ground_capture(
            capture_id,
            raw_store=raw_store,
            ground_store=ground_store,
        )
        receipts = result.fact_receipts
    else:
        result = decode_synoptic_ground_capture(
            capture_id,
            raw_store=raw_store,
            ground_store=ground_store,
        )
        receipts = result.fact_receipts

    if not isinstance(receipts, tuple) or any(
        not isinstance(item, GroundObservationReceipt) for item in receipts
    ):
        raise WorkerOnlyGroundProcessingError(
            "ground_decoder_receipts_invalid"
        )
    return receipts


__all__ = [
    "GROUND_WORKLOAD_IDS",
    "WorkerOnlyGroundProcessingError",
    "decode_worker_only_ground_receipts",
]
