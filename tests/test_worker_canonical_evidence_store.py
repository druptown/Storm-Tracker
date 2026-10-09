from __future__ import annotations

import ast
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from storm_tracker_worker import canonical_evidence_store as module
from storm_tracker_worker.canonical_evidence_store import (
    CanonicalEvidenceCapacityError,
    CanonicalEvidenceConflict,
    CanonicalEvidenceIntegrityError,
    CanonicalEvidenceStore,
    CaptureVerificationError,
    EVIDENCE_CONTRACT,
    decoder_commit_id,
    decoder_result_sha256,
    decoder_run_id,
    parse_decoder_run,
)
from storm_tracker_worker.native_capture import (
    NativeCaptureMemberInput,
    capture_native_set,
)
from storm_tracker_worker.raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactStore,
)


PRODUCT_AT = 1_800_000_000.25
RECEIVED_AT = 1_800_000_011.75
EVALUATED_AT = 1_800_000_040.0
COMMITTED_AT = 1_800_000_050.0
DECODER_JOB_ID = "decoder-job-" + "a" * 64
ATTEMPT_ID = "decoder-attempt-" + "b" * 64


def _raw_store(tmp_path: Path) -> RawArtifactStore:
    store = RawArtifactStore((tmp_path / "raw-authority").resolve())
    store.initialize()
    return store


def _capture(store: RawArtifactStore, suffix: str = "a"):
    return capture_native_set(
        store,
        provider="synthetic-test-provider",
        capture_product="synthetic_test_capture",
        fetch_id=f"canonical:{suffix}",
        upstream_capture_id=f"synthetic/capture-{suffix}",
        product_timestamp=PRODUCT_AT,
        collector="canonical-evidence-test",
        collector_version="test-v1",
        members=(
            NativeCaptureMemberInput(
                role="capture_manifest",
                product="synthetic_capture_manifest",
                content_type="application/json",
                received_at=RECEIVED_AT - 2.0,
                upstream_id=f"synthetic/{suffix}/capture.json",
                payload=(f'{{"capture":"{suffix}"}}').encode("ascii"),
                provenance={"semantic_class": "capture_manifest"},
            ),
            NativeCaptureMemberInput(
                role="forecast_json",
                product="synthetic_forecast_json",
                content_type="application/json",
                received_at=RECEIVED_AT - 1.0,
                upstream_id=f"synthetic/{suffix}/forecast.json",
                payload=(f'{{"forecast":"{suffix}"}}').encode("ascii"),
                provenance={"semantic_class": "forecast_manifest"},
            ),
            NativeCaptureMemberInput(
                role="radar_png",
                product="synthetic_raster",
                content_type="image/png",
                received_at=RECEIVED_AT,
                upstream_id=f"synthetic/{suffix}/raster.bin",
                payload=b"\x89PNG\r\n\x1a\nprivate-raw-payload-" + suffix.encode(),
                provenance={"semantic_class": "rendered_radar_image"},
            ),
        ),
    ).capture


def _members(capture) -> list[dict]:
    return [
        {
            "member_id": member.member_id,
            "role": member.role,
            "receipt_id": member.receipt_id,
            "content_sha256": member.content_sha256,
            "receipt_sha256": member.receipt_sha256,
            "received_at": member.received_at,
        }
        for member in capture.members
    ]


def _run(capture, **overrides) -> dict:
    value = {
        "contract": EVIDENCE_CONTRACT,
        "decoder_job_id": DECODER_JOB_ID,
        "attempt_id": ATTEMPT_ID,
        "capture": {
            "capture_id": capture.capture_id,
            "capture_receipt_sha256": capture.capture_receipt_sha256,
            "members": _members(capture),
        },
        "decoder": {
            "identity": "synthetic-contract-decoder",
            "version": "decoder-v1",
            "config_sha256": "c" * 64,
        },
        "provider": capture.provider,
        "product": capture.product,
        "product_time": capture.product_timestamp,
        "received_at": max(member.received_at for member in capture.members),
        "evaluated_at": EVALUATED_AT,
        "quantity": "synthetic_quantity",
        "unit": "synthetic_unit",
        "native_crs": "EPSG:4326",
        "native_grid": {
            "kind": "regular",
            "width": 512,
            "height": 512,
            "axis_order": "xy",
            "definition_sha256": "d" * 64,
        },
        "coverage": {
            "kind": "native_bbox",
            "minimum_x": 2.0,
            "minimum_y": 49.0,
            "maximum_x": 7.0,
            "maximum_y": 52.0,
            "definition_sha256": "e" * 64,
        },
        "non_operational_candidate_artifact": {
            "handle": "artifact:decoded:synthetic:frame-0001",
            "sha256": "f" * 64,
        },
        "outcome": "decoded_complete",
        "reason_code": "decoded_complete",
    }
    value.update(overrides)
    return value


def _non_meteorological(capture, outcome: str, evaluated_at=EVALUATED_AT) -> dict:
    return _run(
        capture,
        evaluated_at=evaluated_at,
        quantity=None,
        unit=None,
        native_crs=None,
        native_grid=None,
        coverage=None,
        non_operational_candidate_artifact=None,
        outcome=outcome,
        reason_code=f"{outcome}:source_contract",
    )


def _store(path: Path, raw_reader, **kwargs) -> CanonicalEvidenceStore:
    clock = kwargs.pop("clock", lambda: COMMITTED_AT)
    return CanonicalEvidenceStore(
        path.resolve(),
        raw_artifact_reader=raw_reader,
        clock=clock,
        **kwargs,
    )


def _counts(path: Path) -> tuple[int, int]:
    with closing(sqlite3.connect(path)) as database, database:
        return (
            database.execute("SELECT count(*) FROM decoder_runs").fetchone()[0],
            database.execute(
                "SELECT count(*) FROM decoder_run_members"
            ).fetchone()[0],
        )


def test_complete_capture_commits_one_payload_free_decoder_receipt(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    request = _run(capture)
    store = _store(tmp_path / "canonical.sqlite3", raw_store)

    receipt = store.commit(request)

    assert receipt.run_id == decoder_run_id(request)
    assert receipt.commit_id == decoder_commit_id(request)
    assert receipt.result_sha256 == decoder_result_sha256(request)
    assert receipt.revision == 1
    assert receipt.previous_receipt_sha256 is None
    assert receipt.non_operational_candidate_artifact_handle == (
        "artifact:decoded:synthetic:frame-0001"
    )
    assert receipt.is_structurally_complete_candidate is True
    metadata = receipt.to_metadata()
    assert "is_meteorological" not in metadata
    assert "downstream_artifact_handle" not in metadata
    assert "admission" not in json.dumps(metadata).lower()
    assert store.load(receipt.run_id) == receipt
    assert store.load_commit(receipt.commit_id) == receipt
    assert store.audit() == {
        "healthy": True,
        "checked_runs": 1,
        "issues": [],
    }

    with closing(sqlite3.connect(store.path)) as database, database:
        row = database.execute(
            "SELECT capture_receipt_sha256,capture_manifest_sha256,"
            "member_count,typeof(canonical_run) "
            "FROM decoder_runs"
        ).fetchone()
        members = database.execute(
            "SELECT member_id,role,receipt_id,content_sha256,receipt_sha256 "
            "FROM decoder_run_members ORDER BY member_id"
        ).fetchall()
    assert row == (
        capture.capture_receipt_sha256,
        capture.manifest_sha256,
        len(capture.members),
        "text",
    )
    assert members == [
        (
            member.member_id,
            member.role,
            member.receipt_id,
            member.content_sha256,
            member.receipt_sha256,
        )
        for member in capture.members
    ]


@pytest.mark.parametrize("outcome", ["decode_error", "unsupported", "incomplete"])
def test_non_complete_outcomes_are_never_candidates_or_dry(tmp_path, outcome):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    request = _non_meteorological(capture, outcome)

    receipt = _store(tmp_path / f"{outcome}.sqlite3", raw_store).commit(request)

    assert receipt.is_structurally_complete_candidate is False
    assert receipt.non_operational_candidate_artifact_handle is None
    assert receipt.run.non_operational_candidate_artifact is None
    assert "dry" not in json.dumps(receipt.to_metadata()).lower()

    unsafe = deepcopy(request)
    unsafe["non_operational_candidate_artifact"] = {
        "handle": "artifact:forbidden",
        "sha256": "1" * 64,
    }
    with pytest.raises(ValueError, match="cannot expose derived candidate"):
        parse_decoder_run(unsafe)


def test_decoded_complete_requires_every_explicit_candidate_field(tmp_path):
    capture = _capture(_raw_store(tmp_path))
    for field in (
        "quantity",
        "unit",
        "native_crs",
        "native_grid",
        "coverage",
        "non_operational_candidate_artifact",
    ):
        request = _run(capture)
        request[field] = None
        with pytest.raises(ValueError, match="requires all structural candidate"):
            parse_decoder_run(request)


def test_contract_rejects_paths_payloads_secrets_and_unknown_fields(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    request = _run(capture)
    for field in ("payload", "artifact_path", "api_key", "access_token"):
        unsafe = deepcopy(request)
        unsafe[field] = "super-secret-provider-token"
        with pytest.raises(ValueError, match="keys mismatch"):
            parse_decoder_run(unsafe)
    unsafe_handle = deepcopy(request)
    unsafe_handle["non_operational_candidate_artifact"]["handle"] = (
        "C:\\private\\decoded.bin"
    )
    with pytest.raises(ValueError, match="not a path"):
        parse_decoder_run(unsafe_handle)

    store = _store(tmp_path / "no-raw.sqlite3", raw_store)
    store.commit(request)
    database_bytes = store.path.read_bytes()
    assert b"private-raw-payload" not in database_bytes
    assert str(raw_store.root).encode() not in database_bytes
    assert b"super-secret-provider-token" not in database_bytes


@pytest.mark.parametrize(
    "field,value,match",
    [
        ("decoder_job_id", "job-created-after-decode", "preassigned decoder-job"),
        ("attempt_id", "attempt-created-after-result", "preassigned decoder-attempt"),
        ("decoder_job_id", "decoder-job-" + "a" * 65, "decoder_job_id"),
        ("attempt_id", "decoder-attempt-" + "b" * 65, "attempt_id"),
    ],
)
def test_preassigned_job_and_attempt_identities_are_required_and_bounded(
    tmp_path,
    field,
    value,
    match,
):
    capture = _capture(_raw_store(tmp_path))
    request = _run(capture)
    request[field] = value
    with pytest.raises(ValueError, match=match):
        parse_decoder_run(request)

    missing = _run(capture)
    del missing[field]
    with pytest.raises(ValueError, match="keys mismatch"):
        parse_decoder_run(missing)


def test_direct_decoder_run_dataclass_cannot_bypass_closed_contract(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    valid = parse_decoder_run(_run(capture))
    store = _store(tmp_path / "dataclass-bypass.sqlite3", raw_store)

    with pytest.raises(ValueError, match="decoder_job_id"):
        store.commit(replace(valid, decoder_job_id="unbounded-human-job"))
    with pytest.raises(ValueError, match="requires all structural candidate"):
        store.commit(replace(valid, non_operational_candidate_artifact=None))
    assert store.stats() == {"decoder_runs": 0, "member_links": 0}


class _MissingReader:
    def load_capture_receipt(self, capture_id):
        return None


class _StaticReader:
    def __init__(self, capture):
        self.capture = capture
        self.calls = 0

    def load_capture_receipt(self, capture_id):
        self.calls += 1
        if self.capture is None or capture_id != self.capture.capture_id:
            return None
        return self.capture


def test_missing_capture_and_missing_or_swapped_member_fail_before_write(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    missing_store = _store(tmp_path / "missing.sqlite3", _MissingReader())
    with pytest.raises(CaptureVerificationError, match="missing"):
        missing_store.commit(_run(capture))
    assert _counts(missing_store.path) == (0, 0)

    store = _store(tmp_path / "member.sqlite3", raw_store)
    missing_member = _run(capture)
    missing_member["capture"]["members"].pop()
    with pytest.raises(CaptureVerificationError, match="exactly match"):
        store.commit(missing_member)
    swapped = _run(capture)
    first, second = swapped["capture"]["members"][:2]
    for key in ("receipt_id", "content_sha256", "receipt_sha256", "received_at"):
        first[key], second[key] = second[key], first[key]
    with pytest.raises(CaptureVerificationError, match="exactly match"):
        store.commit(swapped)
    assert _counts(store.path) == (0, 0)


@pytest.mark.parametrize("mutation", ["missing", "swapped", "tampered"])
def test_structural_reader_cannot_forge_capture_member_or_receipt_hash(
    tmp_path,
    mutation,
):
    capture = _capture(_raw_store(tmp_path))
    members = list(capture.members)
    if mutation == "missing":
        members.pop()
    elif mutation == "swapped":
        members[0], members[1] = (
            replace(
                members[0],
                receipt_id=members[1].receipt_id,
                content_sha256=members[1].content_sha256,
                receipt_sha256=members[1].receipt_sha256,
                received_at=members[1].received_at,
            ),
            replace(
                members[1],
                receipt_id=members[0].receipt_id,
                content_sha256=members[0].content_sha256,
                receipt_sha256=members[0].receipt_sha256,
                received_at=members[0].received_at,
            ),
        )
    else:
        members[0] = replace(members[0], receipt_sha256="0" * 64)
    forged = replace(capture, members=tuple(members))
    reader = _StaticReader(forged)
    store = _store(tmp_path / f"forged-{mutation}.sqlite3", reader)

    with pytest.raises(CaptureVerificationError, match="exactly match"):
        store.commit(_run(capture))
    assert _counts(store.path) == (0, 0)


def test_actual_raw_member_tampering_fails_closed_before_local_write(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    with closing(sqlite3.connect(raw_store.catalog_path)) as database, database:
        database.execute(
            "UPDATE raw_artifact_receipts SET receipt_sha256=? "
            "WHERE receipt_id=?",
            ("0" * 64, capture.members[0].receipt_id),
        )
        database.commit()
    store = _store(tmp_path / "tampered.sqlite3", raw_store)

    with pytest.raises(CaptureVerificationError, match="verification failed"):
        store.commit(_run(capture))
    assert _counts(store.path) == (0, 0)


def test_commit_load_and_audit_each_reverify_injected_raw_authority(tmp_path):
    capture = _capture(_raw_store(tmp_path))
    reader = _StaticReader(capture)
    store = _store(tmp_path / "reverify.sqlite3", reader)
    receipt = store.commit(_run(capture))
    assert reader.calls == 1

    assert store.load(receipt.run_id) == receipt
    assert reader.calls == 2
    assert store.audit()["healthy"] is True
    assert reader.calls == 3

    reader.capture = None
    with pytest.raises(CaptureVerificationError, match="missing"):
        store.load(receipt.run_id)
    report = store.audit()
    assert report["healthy"] is False
    assert report["checked_runs"] == 0
    assert "missing" in report["issues"][0]


def test_exact_retry_conflict_and_revision_receipt_chain(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    store = _store(tmp_path / "history.sqlite3", raw_store)
    request = _run(capture)
    first = store.commit(request)
    before = store.stats()

    assert store.commit(request) == first
    assert store.stats() == before

    changed = _non_meteorological(
        capture,
        "decode_error",
        evaluated_at=EVALUATED_AT + 1.0,
    )
    assert decoder_commit_id(changed) == first.commit_id
    assert decoder_run_id(changed) != first.run_id
    with pytest.raises(CanonicalEvidenceConflict, match="reused"):
        store.commit(changed)

    changed["attempt_id"] = "decoder-attempt-" + "c" * 64
    second = store.commit(changed)
    assert second.revision_scope_id == first.revision_scope_id
    assert second.revision == 2
    assert second.previous_receipt_sha256 == first.receipt_sha256
    assert second.revision_sha256 != first.revision_sha256
    assert second.receipt_sha256 != first.receipt_sha256


def test_lost_result_retry_with_changed_output_or_time_conflicts(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    store = _store(tmp_path / "lost-result.sqlite3", raw_store)
    original = _run(capture)
    first = store.commit(original)

    for mutation in ("evaluated_at", "output"):
        retry = deepcopy(original)
        if mutation == "evaluated_at":
            retry["evaluated_at"] += 1.0
        else:
            retry["non_operational_candidate_artifact"]["sha256"] = "0" * 64
        assert decoder_commit_id(retry) == first.commit_id
        assert decoder_run_id(retry) != first.run_id
        assert decoder_result_sha256(retry) != first.result_sha256
        with pytest.raises(CanonicalEvidenceConflict, match="different facts"):
            store.commit(retry)

    assert store.stats()["decoder_runs"] == 1


def test_result_hash_is_independent_from_preassigned_attempt_identity(tmp_path):
    capture = _capture(_raw_store(tmp_path))
    first = _run(capture)
    second = deepcopy(first)
    second["attempt_id"] = "decoder-attempt-" + "d" * 64

    assert decoder_result_sha256(second) == decoder_result_sha256(first)
    assert decoder_run_id(second) != decoder_run_id(first)
    assert decoder_commit_id(second) != decoder_commit_id(first)


class _InjectedFailure(RuntimeError):
    pass


@pytest.mark.parametrize(
    "point",
    [
        "commit.after_begin",
        "commit.after_run",
        "commit.after_members",
        "commit.before_commit",
    ],
)
def test_exception_before_commit_rolls_back_every_fact(tmp_path, point):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)

    def inject(actual):
        if actual == point:
            raise _InjectedFailure(point)

    store = _store(
        tmp_path / f"rollback-{point}.sqlite3",
        raw_store,
        fault_injector=inject,
    )
    with pytest.raises(_InjectedFailure, match=point):
        store.commit(_run(capture))
    assert _counts(store.path) == (0, 0)


def _helper_context() -> tuple[Path, Path, dict[str, str]]:
    helper = Path(__file__).parent / "helpers/canonical_evidence_crash_child.py"
    environment = os.environ.copy()
    repository_path = Path(__file__).resolve().parents[1]
    repository = str(repository_path)
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        repository
        if not existing_pythonpath
        else repository + os.pathsep + existing_pythonpath
    )
    return helper, repository_path, environment


def _power_cut(
    path: Path,
    raw_store: RawArtifactStore,
    request: dict,
    checkpoint: str,
) -> subprocess.CompletedProcess:
    helper, repository, environment = _helper_context()
    return subprocess.run(
        [
            sys.executable,
            str(helper),
            str(path.resolve()),
            str(raw_store.root),
            "commit",
            checkpoint,
            json.dumps(request, separators=(",", ":"), sort_keys=True),
        ],
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _initialize_child(
    path: Path,
    raw_store: RawArtifactStore,
    checkpoint: str,
) -> subprocess.CompletedProcess:
    helper, repository, environment = _helper_context()
    return subprocess.run(
        [
            sys.executable,
            str(helper),
            str(path.resolve()),
            str(raw_store.root),
            "initialize",
            checkpoint,
        ],
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize(
    "point",
    [
        "initialize.after_begin",
        "initialize.after_ddl.table.manifest",
        "initialize.after_ddl.table.decoder_runs",
        "initialize.after_ddl.index.decoder_runs_scope",
        "initialize.after_manifest.contract",
        "initialize.after_ddl.trigger.decoder_runs_no_update",
        "initialize.before_commit",
    ],
)
def test_process_crash_during_initialize_leaves_no_partial_schema(
    tmp_path,
    point,
):
    raw_store = _raw_store(tmp_path)
    path = tmp_path / f"initialize-crash-{point}.sqlite3"
    completed = _initialize_child(path, raw_store, point)
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    with closing(sqlite3.connect(path)) as database, database:
        persisted = database.execute(
            "SELECT count(*) FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%'"
        ).fetchone()[0]
    assert persisted == 0
    reopened = _store(path, raw_store)
    assert reopened.stats() == {"decoder_runs": 0, "member_links": 0}
    assert reopened.audit()["healthy"] is True


def test_process_crash_after_initialize_commit_recovers_complete_schema(tmp_path):
    raw_store = _raw_store(tmp_path)
    path = tmp_path / "initialize-after-commit.sqlite3"
    completed = _initialize_child(path, raw_store, "initialize.after_commit")
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    reopened = _store(path, raw_store)
    assert reopened.stats() == {"decoder_runs": 0, "member_links": 0}
    assert reopened.audit()["healthy"] is True


def test_concurrent_process_first_initialize_is_idempotent(tmp_path):
    raw_store = _raw_store(tmp_path)
    path = tmp_path / "concurrent-first-initialize.sqlite3"
    helper, repository, environment = _helper_context()
    command = [
        sys.executable,
        str(helper),
        str(path.resolve()),
        str(raw_store.root),
        "initialize",
        "none",
    ]
    children = [
        subprocess.Popen(
            command,
            cwd=repository,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    results = [child.communicate(timeout=30) for child in children]
    assert [child.returncode for child in children] == [0, 0, 0, 0], results

    reopened = _store(path, raw_store)
    assert reopened.stats() == {"decoder_runs": 0, "member_links": 0}
    assert reopened.audit()["healthy"] is True


@pytest.mark.parametrize(
    "point",
    ["commit.after_run", "commit.after_members", "commit.before_commit"],
)
def test_process_crash_before_commit_leaves_no_partial_receipt(tmp_path, point):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    path = tmp_path / f"crash-before-{point}.sqlite3"
    completed = _power_cut(path, raw_store, _run(capture), point)
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    reopened = _store(path, raw_store)
    assert reopened.stats() == {"decoder_runs": 0, "member_links": 0}
    assert reopened.audit()["healthy"] is True


def test_process_crash_after_commit_recovers_by_exact_retry(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    request = _run(capture)
    path = tmp_path / "crash-after.sqlite3"
    completed = _power_cut(path, raw_store, request, "commit.after_commit")
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    reopened = _store(path, raw_store, clock=lambda: COMMITTED_AT + 999.0)
    receipt = reopened.commit(request)
    assert receipt.committed_at == COMMITTED_AT
    assert receipt.revision == 1
    assert reopened.stats() == {
        "decoder_runs": 1,
        "member_links": len(capture.members),
    }


def test_append_only_wal_full_schema_capacity_and_corruption_fail_closed(
    tmp_path,
    monkeypatch,
):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    path = tmp_path / "durable.sqlite3"
    store = _store(path, raw_store)
    receipt = store.commit(_run(capture))
    with closing(store._connect()) as database, database:
        assert database.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert database.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert database.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert database.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with closing(sqlite3.connect(path)) as database, database:
        for statement in (
            "UPDATE decoder_runs SET outcome='incomplete'",
            "DELETE FROM decoder_runs",
            "UPDATE decoder_run_members SET role='changed'",
            "DELETE FROM decoder_run_members",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="append-only table"):
                database.execute(statement)
            database.rollback()

    capacity_path = tmp_path / "capacity.sqlite3"
    capacity_store = _store(capacity_path, raw_store)
    monkeypatch.setattr(module, "MAX_TOTAL_RUNS", 0)
    with pytest.raises(CanonicalEvidenceCapacityError, match="run bound"):
        capacity_store.commit(_run(capture))
    assert _counts(capacity_path) == (0, 0)

    with closing(sqlite3.connect(path)) as database, database:
        database.execute("DROP TRIGGER decoder_runs_no_update")
        database.execute(
            "UPDATE decoder_runs SET result_sha256=? WHERE run_id=?",
            ("0" * 64, receipt.run_id),
        )
        database.commit()
    with pytest.raises(CanonicalEvidenceIntegrityError):
        store.load(receipt.run_id)
    assert store.audit()["healthy"] is False


@pytest.mark.parametrize(
    "table,trigger,statement",
    [
        (
            "decoder_runs",
            "decoder_runs_no_update",
            "UPDATE decoder_runs SET receipt_sha256='" + "0" * 64 + "'",
        ),
        (
            "decoder_runs",
            "decoder_runs_no_update",
            "UPDATE decoder_runs SET revision_sha256='" + "0" * 64 + "'",
        ),
        (
            "decoder_run_members",
            "decoder_run_members_no_update",
            "UPDATE decoder_run_members SET receipt_sha256='" + "0" * 64 + "' "
            "WHERE member_id=(SELECT min(member_id) FROM decoder_run_members)",
        ),
    ],
)
def test_local_receipt_revision_or_member_corruption_is_detected(
    tmp_path,
    table,
    trigger,
    statement,
):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    path = tmp_path / f"local-corrupt-{table}-{trigger}.sqlite3"
    store = _store(path, raw_store)
    receipt = store.commit(_run(capture))
    with closing(sqlite3.connect(path)) as database, database:
        database.execute(f"DROP TRIGGER {trigger}")
        database.execute(statement)
        database.executescript(f"""
            CREATE TRIGGER {trigger}
            BEFORE UPDATE ON {table} BEGIN
                SELECT RAISE(ABORT,'append-only table');
            END;
        """)
        database.commit()

    with pytest.raises(CanonicalEvidenceIntegrityError):
        store.load(receipt.run_id)
    assert store.audit()["healthy"] is False


def test_catalog_schema_has_no_raw_payload_blob_path_or_secret_columns(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    store = _store(tmp_path / "metadata-only.sqlite3", raw_store)
    store.commit(_run(capture))

    with closing(sqlite3.connect(store.path)) as database, database:
        columns = {
            str(row[1]).lower(): str(row[2]).lower()
            for table in ("decoder_runs", "decoder_run_members")
            for row in database.execute(f"PRAGMA table_info({table})")
        }
    forbidden = ("payload", "blob", "path", "secret", "token", "api_key")
    assert not any(part in name for name in columns for part in forbidden)
    assert all(column_type != "blob" for column_type in columns.values())


def test_no_old_evidence_or_schema2_raw_store_dependency_exists():
    path = Path(module.__file__)
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    imported_names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }

    assert not any("meteorological_evidence_store" in name for name in imported_modules)
    assert not any("tracking_core.engine.raw_artifact_store" in name for name in imported_modules)
    assert "MeteorologicalEvidenceStore" not in imported_names
    assert "RawArtifactSetReceipt" not in imported_names
    assert "RawArtifactStore" not in imported_names
    assert ".load_receipt(" not in source
    assert ".read_bytes(" not in source
    assert ".executescript(" not in source
    assert RAW_ARTIFACT_SCHEMA_VERSION == 3
    assert RAW_ARTIFACT_CAPTURE_SCHEMA == "stv3-worker-raw-native-capture-v2"
    assert RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA.endswith("receipt-v1")


def test_database_manifest_or_unknown_schema_fails_closed(tmp_path):
    raw_store = _raw_store(tmp_path)
    path = tmp_path / "schema.sqlite3"
    store = _store(path, raw_store)
    with closing(sqlite3.connect(path)) as database, database:
        database.execute("DROP TRIGGER manifest_no_update")
        database.execute(
            "UPDATE manifest SET value='999' WHERE key='schema_version'"
        )
        database.commit()
    with pytest.raises(
        CanonicalEvidenceIntegrityError,
        match="fingerprint|manifest",
    ):
        store.initialize()


@pytest.mark.parametrize(
    "mutation",
    [
        "weakened_trigger",
        "changed_trigger_literal",
        "missing_index",
        "extra_view",
    ],
)
def test_every_commit_rejects_post_initialize_schema_tampering(
    tmp_path,
    mutation,
):
    raw_store = _raw_store(tmp_path)
    capture = _capture(raw_store)
    store = _store(tmp_path / f"schema-tamper-{mutation}.sqlite3", raw_store)
    with closing(sqlite3.connect(store.path)) as database, database:
        if mutation == "weakened_trigger":
            database.execute("DROP TRIGGER decoder_runs_no_update")
            database.execute(
                "CREATE TRIGGER decoder_runs_no_update "
                "BEFORE UPDATE ON decoder_runs BEGIN SELECT 1; END"
            )
        elif mutation == "changed_trigger_literal":
            database.execute("DROP TRIGGER decoder_runs_no_update")
            database.execute(
                "CREATE TRIGGER decoder_runs_no_update "
                "BEFORE UPDATE ON decoder_runs BEGIN "
                "SELECT RAISE(ABORT,'append-only  table'); END"
            )
        elif mutation == "missing_index":
            database.execute("DROP INDEX decoder_runs_capture")
        else:
            database.execute(
                "CREATE VIEW unexpected_decoder_view AS "
                "SELECT run_id FROM decoder_runs"
            )
        database.commit()

    with pytest.raises(CanonicalEvidenceIntegrityError, match="fingerprint"):
        store.commit(_run(capture))
    assert _counts(store.path) == (0, 0)


def test_experimental_schema_v1_is_rejected_without_implicit_migration(tmp_path):
    raw_store = _raw_store(tmp_path)
    path = (tmp_path / "old-experimental-schema.sqlite3").resolve()
    with closing(sqlite3.connect(path)) as database, database:
        database.execute(
            "CREATE TABLE manifest(key TEXT PRIMARY KEY,value TEXT NOT NULL) "
            "WITHOUT ROWID"
        )
        database.execute(
            "INSERT INTO manifest(key,value) VALUES('schema_version','1')"
        )
        database.commit()

    with pytest.raises(CanonicalEvidenceIntegrityError, match="fingerprint"):
        _store(path, raw_store)


def test_bounded_ids_times_grid_coverage_and_handles(tmp_path):
    capture = _capture(_raw_store(tmp_path))
    request = _run(capture)
    request["evaluated_at"] = float("nan")
    with pytest.raises(ValueError, match="evaluated_at"):
        parse_decoder_run(request)

    request = _run(capture)
    request["native_grid"]["width"] = 1_000_001
    with pytest.raises(ValueError, match="dimensions"):
        parse_decoder_run(request)

    request = _run(capture)
    request["coverage"]["minimum_x"] = request["coverage"]["maximum_x"]
    with pytest.raises(ValueError, match="inverted or empty"):
        parse_decoder_run(request)

    store = _store(tmp_path / "commit-id.sqlite3", _raw_store(tmp_path / "other"))
    with pytest.raises(TypeError, match="commit_id"):
        store.commit(_run(capture), commit_id="commit:human-label")
