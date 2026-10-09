from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import threading

import pytest

from storm_tracker_worker.acquisition_consumer_registry import (
    AcquisitionConsumerClosureReceipt,
)
from storm_tracker_worker.acquisition_source_retention import (
    AcquisitionSourceRetentionBusy,
    AcquisitionSourceRetentionConflict,
    AcquisitionSourceRetentionIntegrityError,
    AcquisitionSourceRetentionLimitsV2,
)

from test_worker_acquisition_consumer_registry import _archive_authorities
from test_worker_acquisition_control_reclamation import (
    _committed,
    _prepare_bindings,
)
from test_worker_acquisition_lifecycle_retention_v2 import (
    _participant,
    _v2_context,
)
from test_worker_acquisition_source_reclamation_registry_closure import (
    WORKLOAD_ID,
    _bound_registry,
)
from test_worker_acquisition_source_retention import NOW, _archive


def _closure_for_bound(context, bound, *, member=None):
    candidate = context.sources.candidates[0]
    downstream = context.sources.evidence.load_terminal_consumption(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    )
    identity = context.sources.evidence.terminal_event_identity(
        candidate.evidence_stream_id,
        candidate.evidence_stream_sequence,
    )
    lookup = context.source_archive.lookup(candidate.candidate_sha256)
    assert downstream is not None and downstream.status == "archived"
    assert identity is not None and identity.status == "archived"
    assert lookup.record is not None and lookup.segment_sha256 is not None
    selected_member = member or bound.registry_root.by_workload[WORKLOAD_ID]
    return AcquisitionConsumerClosureReceipt.create(
        candidate_sha256=candidate.candidate_sha256,
        acquisition_job_id=candidate.acquisition_job_id,
        member=selected_member,
        verification=bound.verification,
        producer_receipt=downstream.producer_receipt,
        consumption_receipt=downstream.consumption_receipt,
        terminal_identity=identity,
        source_archive_anchor_sha256=context.source_archive.audit()[
            "archive_anchor_sha256"
        ],
        source_archive_segment_sha256=lookup.segment_sha256,
        source_archive_record_sha256=lookup.record["record_sha256"],
    )


def _advance_registry(context, salt: str):
    catalog, reproof = _archive_authorities(salt)
    epoch = context.registry.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    bound = context.registry.verifier(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    ).bind_current_epoch(
        claimed_epoch_root_sha256=epoch.epoch_root_sha256,
    )
    return bound, _closure_for_bound(context, bound)


def test_registry_hold_current_epoch_blocks_advance_and_reproves(tmp_path):
    registry, first = _bound_registry(tmp_path)
    catalog, reproof = _archive_authorities("retention-v2-epoch-two")
    entered = threading.Event()

    def advance():
        entered.set()
        return registry.ensure_epoch(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with registry.hold_current_epoch() as held:
                assert held.verification == first.verification
                future = executor.submit(advance)
                assert entered.wait(timeout=1.0)
                assert future.done() is False
                assert held.verify_held() == first.verification
            second = future.result(timeout=5.0)
        assert second.epoch == first.verification.epoch + 1
        assert registry.load_current()[1] == second
    finally:
        registry.close()


def test_retention_v2_nested_current_hold_is_reentrant_and_thread_fenced(
    tmp_path,
):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        contender = _archive(
            tmp_path / "closure-source",
            context.sources,
            limits=replace(
                context.source_archive._limits,
                lock_timeout_seconds=0.05,
            ),
        )
        entered = threading.Event()

        def contend_for_v1_archive():
            entered.set()
            try:
                contender.audit()
            except AcquisitionSourceRetentionBusy as err:
                return str(err)
            return "unexpectedly_acquired"

        with ThreadPoolExecutor(max_workers=1) as executor:
            with context.archive.hold_current_request_authority_v1(
                context.request
            ) as current:
                with context.archive.hold_candidate_page_v1(
                    context.request,
                    after_cursor=0,
                    max_candidates=64,
                ) as nested:
                    assert nested == current
                future = executor.submit(contend_for_v1_archive)
                assert entered.wait(timeout=1.0)
                assert future.result(timeout=1.0) == "archive_lock_busy"

        assert contender.audit()["archive_anchor_sha256"] == (
            context.source_archive.audit()["archive_anchor_sha256"]
        )


def test_retention_v2_rejects_forged_closure_and_stale_epoch_request(
    tmp_path,
):
    with _v2_context(tmp_path, seal=False) as context:
        member = context.registry_verifier.registry_root.by_workload[WORKLOAD_ID]
        forged_member = replace(
            member,
            archive_resolver_sha256="f" * 64,
        )
        forged = _closure_for_bound(
            context,
            context.registry_verifier,
            member=forged_member,
        )
        with pytest.raises(
            AcquisitionSourceRetentionIntegrityError,
            match="current registry",
        ):
            context.archive.seal_candidates_v2(
                context.sources.candidates,
                consumer_closures=(forged,),
                now=NOW + 500.0,
            )

        sealed = context.archive.seal_candidates_v2(
            context.sources.candidates,
            consumer_closures=(context.closure,),
            now=NOW + 500.0,
        )
        request = context.archive.create_reclamation_request_v2(
            sealed.candidate_bindings_sha256,
            source_binding_sha256="1" * 64,
            source_view_sha256="2" * 64,
            created_at=NOW + 600.0,
        )
        _advance_registry(context, "retention-v2-stale-epoch")

        with context.archive.hold_candidate_page_v1(
            request,
            after_cursor=0,
            max_candidates=64,
        ) as historical:
            assert historical.exhausted is True
        with pytest.raises(AcquisitionSourceRetentionConflict, match="stale"):
            context.archive.create_reclamation_request_v2(
                sealed.candidate_bindings_sha256,
                source_binding_sha256="1" * 64,
                source_view_sha256="2" * 64,
                created_at=NOW + 601.0,
            )
        with pytest.raises(AcquisitionSourceRetentionConflict, match="stale"):
            with context.archive.hold_current_request_authority_v1(request):
                pass


def test_retention_v2_successor_reseal_same_inventory_and_exact_retry_at_cap(
    tmp_path,
):
    limits = AcquisitionSourceRetentionLimitsV2(
        min_terminal_age_seconds=10.0,
        max_archive_segments=2,
    )
    with _v2_context(tmp_path, limits=limits) as context:
        assert context.seal is not None and context.request is not None
        second_bound, second_closure = _advance_registry(
            context,
            "retention-v2-successor",
        )
        successor = context.archive.seal_candidates_v2(
            context.sources.candidates,
            consumer_closures=(second_closure,),
            now=NOW + 700.0,
        )
        assert successor.candidate_bindings_sha256 != (
            context.seal.candidate_bindings_sha256
        )
        assert successor.archive_anchor_sha256 != context.seal.archive_anchor_sha256
        candidate_set = context.archive._read_candidate_set_v2(
            successor.candidate_bindings_sha256
        )
        assert candidate_set["superseded_candidate_bindings_sha256"] == (
            context.seal.candidate_bindings_sha256
        )
        assert candidate_set["superseded_consumer_registry_epoch"] == (
            context.registry_verifier.verification.epoch
        )
        assert candidate_set["consumer_registry_epoch"] == (
            second_bound.verification.epoch
        )
        assert candidate_set["closure_source_archive_anchor_sha256"] == (
            context.closure.source_archive_anchor_sha256
        )

        request = context.archive.create_reclamation_request_v2(
            successor.candidate_bindings_sha256,
            source_binding_sha256="3" * 64,
            source_view_sha256="4" * 64,
            created_at=NOW + 701.0,
        )
        with context.archive.hold_current_request_authority_v1(request) as page:
            assert page.total_candidate_count == 1
        with pytest.raises(AcquisitionSourceRetentionConflict, match="stale"):
            with context.archive.hold_current_request_authority_v1(
                context.request
            ):
                pass

        replay = context.archive.seal_candidates_v2(
            context.sources.candidates,
            consumer_closures=(second_closure,),
            now=NOW + 702.0,
        )
        assert replay == successor
        assert context.archive.audit_v2()["archive_generation"] == 2


def test_retention_v2_page_rejects_same_size_object_tamper(tmp_path):
    with _v2_context(tmp_path) as context:
        assert context.request is not None
        object_path = next(context.archive.object_directory.iterdir())
        payload = bytearray(object_path.read_bytes())
        payload[len(payload) // 2] ^= 1
        object_path.write_bytes(payload)
        with pytest.raises(AcquisitionSourceRetentionIntegrityError):
            with context.archive.hold_candidate_page_v1(
                context.request,
                after_cursor=0,
                max_candidates=64,
            ):
                pass


def test_retention_v2_historical_page_survives_lifecycle_prune_and_v1_append(
    tmp_path,
):
    with _v2_context(tmp_path, source_candidate_count=2) as context:
        assert context.request is not None
        participant = _participant(context, context.decisions)
        prepared = participant.prepare_source_local_v2(context.request)
        bindings = _prepare_bindings((prepared,))
        context.decisions.prepares = {item.role: item for item in bindings}
        context.decisions.committed = _committed(context.request, bindings)
        participant.apply_source_local_v1(
            context.request.request_sha256,
            predecessor_apply_receipt_sha256=None,
        )

        with context.archive.hold_candidate_page_v1(
            context.request,
            after_cursor=0,
            max_candidates=64,
        ) as after_prune:
            assert after_prune.exhausted is True

        pinned_anchor = context.source_archive.audit()["archive_anchor_sha256"]
        append_candidate = context.sources.candidates[1]
        append_plan = context.source_archive.plan(
            (append_candidate,),
            now=NOW + 900.0,
        )
        appended = context.source_archive.export(append_plan)
        assert appended.archived_jobs == 1
        assert appended.archive_anchor_sha256 != pinned_anchor

        with context.archive.hold_candidate_page_v1(
            context.request,
            after_cursor=0,
            max_candidates=64,
        ) as after_append:
            assert after_append == after_prune
