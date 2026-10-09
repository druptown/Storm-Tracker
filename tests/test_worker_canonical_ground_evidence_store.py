from __future__ import annotations

import ast
from contextlib import closing
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from storm_tracker_worker import canonical_ground_evidence_store as module
from storm_tracker_worker.canonical_ground_evidence_store import (
    ADMISSION_STATUS,
    CanonicalGroundEvidenceCapacityError,
    CanonicalGroundEvidenceConflict,
    CanonicalGroundEvidenceDeadlineError,
    CanonicalGroundEvidenceIntegrityError,
    CanonicalGroundEvidenceStore,
    GROUND_EVIDENCE_CONTRACT,
    GroundCaptureVerificationError,
    GroundEvidenceLimits,
    ground_fact_id,
    ground_fact_sha256,
    ground_observation_key,
    parse_ground_observation,
)
from storm_tracker_worker.native_capture import (
    NativeCaptureMemberInput,
    capture_native_set,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore


OBSERVED_AT = 1_800_000_000.25
RECEIVED_AT = 1_800_000_061.5
INGESTED_AT = 1_800_000_070.0
COMMITTED_AT = 1_800_000_080.0


def _raw_store(tmp_path: Path) -> RawArtifactStore:
    store = RawArtifactStore((tmp_path / "ground-raw-authority").resolve())
    store.initialize()
    return store


def _capture(store: RawArtifactStore, suffix: str = "a"):
    return capture_native_set(
        store,
        provider="synthetic-ground-provider",
        capture_product="synthetic_station_observations",
        fetch_id=f"ground:{suffix}",
        upstream_capture_id=f"ground/capture/{suffix}",
        product_timestamp=OBSERVED_AT,
        collector="canonical-ground-test",
        collector_version="test-v1",
        members=(
            NativeCaptureMemberInput(
                role="station_metadata_json",
                product="synthetic_station_metadata",
                content_type="application/json",
                received_at=RECEIVED_AT - 1.0,
                upstream_id=f"ground/{suffix}/stations.json",
                payload=(f'{{"station":"{suffix}"}}').encode(),
                provenance={"semantic_class": "station_metadata"},
            ),
            NativeCaptureMemberInput(
                role="observations_json",
                product="synthetic_station_observation_records",
                content_type="application/json",
                received_at=RECEIVED_AT,
                upstream_id=f"ground/{suffix}/observations.json",
                payload=(f'{{"rain":"0.10","capture":"{suffix}"}}').encode(),
                provenance={"semantic_class": "station_observations"},
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


def _request(capture, **overrides) -> dict:
    record = b'{"station":"SYN001","rain":"0.10","qc":"V"}'
    observation_member = next(
        member for member in capture.members if member.role == "observations_json"
    )
    value = {
        "contract": GROUND_EVIDENCE_CONTRACT,
        "operational_admission_eligible": False,
        "admission_status": ADMISSION_STATUS,
        "capture": {
            "capture_id": capture.capture_id,
            "capture_receipt_sha256": capture.capture_receipt_sha256,
            "members": _members(capture),
        },
        "source": {
            "member_role": "observations_json",
            "member_content_sha256": observation_member.content_sha256,
            "member_receipt_sha256": observation_member.receipt_sha256,
            "record_locator": "json-pointer:/STATION[0]/OBSERVATIONS/rain_rate",
            "record_sha256": hashlib.sha256(record).hexdigest(),
        },
        "decoder": {
            "identity": "synthetic-ground-decoder",
            "version": "decoder-v1",
            "config_sha256": "c" * 64,
        },
        "provider": capture.provider,
        "product": capture.product,
        "station": {
            "provider_station_id": "SYN001",
            "site_id": "site:synthetic:SYN001",
            "network": "SYNTHETIC",
            "station_name": "Heffen teststation",
            "position_state": "known",
            "latitude_decimal": "51.047500",
            "longitude_decimal": "4.419200",
            "elevation_state": "known",
            "elevation_m_decimal": "6.0",
        },
        "time": {
            "observed_at": OBSERVED_AT,
            "representative_interval_start": OBSERVED_AT - 60.0,
            "representative_interval_end": OBSERVED_AT,
            "received_at": RECEIVED_AT,
            "ingested_at": INGESTED_AT,
        },
        "measurement": {
            "variable": "precipitation_accumulation",
            "state": "observed",
            "representation": "decimal",
            "exact_value": "0.10",
            "unit": "mm",
            "censor_limit": None,
        },
        "provider_qc": [
            {"flag": "provider_qc", "value": "V", "text": "valide"},
            {
                "flag": "provider_text",
                "value": "",
                "text": "originele tekst met spaties en ü",
            },
        ],
    }
    value.update(overrides)
    return value


def _batch_request(capture, suffix: str) -> dict:
    value = _request(capture)
    value["source"]["record_locator"] += f":{suffix}"
    value["source"]["record_sha256"] = hashlib.sha256(
        suffix.encode("utf-8")
    ).hexdigest()
    return value


def _store(path: Path, reader, **kwargs) -> CanonicalGroundEvidenceStore:
    return CanonicalGroundEvidenceStore(
        path.resolve(),
        raw_artifact_reader=reader,
        clock=kwargs.pop("clock", lambda: COMMITTED_AT),
        **kwargs,
    )


def _counts(path: Path) -> tuple[int, int, int]:
    with closing(sqlite3.connect(path)) as database, database:
        return tuple(
            database.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("ground_facts", "ground_fact_members", "ground_fact_qc")
        )


def test_observed_decimal_fact_is_exact_payload_free_and_raw_reverified(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)
    store = _store(tmp_path / "ground.sqlite3", raw)

    receipt = store.commit(request)

    assert receipt.fact_id == ground_fact_id(request)
    assert receipt.fact_sha256 == ground_fact_sha256(request)
    assert receipt.observation_key == ground_observation_key(request)
    assert receipt.fact.measurement.exact_value == "0.10"
    assert receipt.fact.observed_at == OBSERVED_AT
    assert receipt.fact.received_at == RECEIVED_AT
    assert receipt.fact.ingested_at == INGESTED_AT
    assert receipt.operational_admission_eligible is False
    assert receipt.admission_status == ADMISSION_STATUS
    assert store.load(receipt.fact_id) == receipt
    assert store.load_observation(receipt.observation_key) == receipt
    assert store.audit() == {"healthy": True, "checked_facts": 1, "issues": []}
    assert _counts(store.path) == (1, len(capture.members), 2)

    metadata = receipt.to_metadata()
    encoded = json.dumps(metadata, ensure_ascii=False).lower()
    for forbidden in (
        "ground_confirmation",
        "ground_confirmed",
        "rain_decision",
        "wet_station",
        "dry_station",
        '"raining"',
        '"dry"',
        "confidence",
        "forecast",
    ):
        assert forbidden not in encoded
    assert metadata["operational_admission_eligible"] is False
    assert b'"rain":"0.10"' not in store.path.read_bytes()


@pytest.mark.parametrize(
    ("state", "representation", "exact_value", "censor_limit"),
    [
        ("observed", "decimal", "-0.000", None),
        ("observed", "text", "RA", None),
        ("missing", "none", None, None),
        ("trace", "none", None, None),
        ("unknown", "none", None, None),
        ("censored_below", "none", None, "0.10"),
        ("censored_above", "none", None, "999.9"),
    ],
)
def test_all_explicit_value_states_round_trip(
    tmp_path, state, representation, exact_value, censor_limit
):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)
    request["measurement"].update(
        state=state,
        representation=representation,
        exact_value=exact_value,
        censor_limit=censor_limit,
    )
    store = _store(tmp_path / "states.sqlite3", raw)

    receipt = store.commit(request)

    assert receipt.fact.measurement.state == state
    assert receipt.fact.measurement.representation == representation
    assert receipt.fact.measurement.exact_value == exact_value
    assert receipt.fact.measurement.censor_limit == censor_limit


@pytest.mark.parametrize(
    ("state", "representation", "exact_value", "censor_limit"),
    [
        ("observed", "none", None, None),
        ("observed", "decimal", "NaN", None),
        ("observed", "decimal", "0.1", "0.2"),
        ("missing", "decimal", "0", None),
        ("trace", "none", "T", None),
        ("unknown", "none", None, "0.1"),
        ("censored_below", "decimal", "0.1", "0.1"),
        ("censored_above", "none", None, None),
    ],
)
def test_value_state_contradictions_fail_closed(
    tmp_path, state, representation, exact_value, censor_limit
):
    capture = _capture(_raw_store(tmp_path))
    request = _request(capture)
    request["measurement"].update(
        state=state,
        representation=representation,
        exact_value=exact_value,
        censor_limit=censor_limit,
    )
    with pytest.raises(ValueError):
        parse_ground_observation(request)


def test_unknown_site_position_is_explicit_not_fake_zero(tmp_path):
    capture = _capture(_raw_store(tmp_path))
    request = _request(capture)
    request["station"].update(
        position_state="unknown",
        latitude_decimal=None,
        longitude_decimal=None,
        elevation_state="unknown",
        elevation_m_decimal=None,
    )
    parsed = parse_ground_observation(request)
    assert parsed.station.position_state == "unknown"
    assert parsed.station.latitude_decimal is None

    unsafe = deepcopy(request)
    unsafe["station"]["latitude_decimal"] = "0"
    with pytest.raises(ValueError, match="unknown position"):
        parse_ground_observation(unsafe)


def test_time_axes_are_separate_and_interval_is_strict(tmp_path):
    capture = _capture(_raw_store(tmp_path))
    base = _request(capture)
    for mutation in (
        {"representative_interval_start": OBSERVED_AT + 1},
        {"representative_interval_end": OBSERVED_AT - 1},
        {"ingested_at": RECEIVED_AT - 1},
    ):
        request = deepcopy(base)
        request["time"].update(mutation)
        with pytest.raises(ValueError):
            parse_ground_observation(request)

    request = deepcopy(base)
    request["time"]["received_at"] = RECEIVED_AT - 1
    # Parsing accepts the separately labelled fact; raw authority decides whether
    # the declared receipt time is exact.
    assert parse_ground_observation(request).received_at == RECEIVED_AT - 1


def test_raw_authority_enforces_selected_member_received_time(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)
    request["time"]["received_at"] = RECEIVED_AT - 1
    store = _store(tmp_path / "received.sqlite3", raw)
    with pytest.raises(GroundCaptureVerificationError, match="source binding"):
        store.commit(request)
    assert _counts(store.path) == (0, 0, 0)


def test_exact_retry_is_idempotent_but_same_observation_changed_value_conflicts(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(tmp_path / "retry.sqlite3", raw)
    request = _request(capture)
    first = store.commit(request)

    assert store.commit(deepcopy(request)) == first
    assert _counts(store.path) == (1, 2, 2)

    changed = deepcopy(request)
    changed["measurement"]["exact_value"] = "0.20"
    assert ground_observation_key(changed) == first.observation_key
    assert ground_fact_id(changed) != first.fact_id
    with pytest.raises(CanonicalGroundEvidenceConflict, match="different facts"):
        store.commit(changed)
    assert _counts(store.path) == (1, 2, 2)


def test_new_decoder_identity_creates_a_separate_candidate_fact(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(tmp_path / "decoder-revision.sqlite3", raw)
    first = store.commit(_request(capture))
    changed = _request(capture)
    changed["decoder"]["version"] = "decoder-v2"
    changed["measurement"]["exact_value"] = "0.20"

    second = store.commit(changed)

    assert second.observation_key != first.observation_key
    assert store.stats()["ground_facts"] == 2


def test_capture_provider_product_member_and_digest_bindings_fail_closed(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(tmp_path / "binding.sqlite3", raw)
    mutations = []
    provider = _request(capture)
    provider["provider"] = "other-provider"
    mutations.append(provider)
    product = _request(capture)
    product["product"] = "other_product"
    mutations.append(product)
    digest = _request(capture)
    digest["capture"]["capture_receipt_sha256"] = "0" * 64
    mutations.append(digest)
    member = _request(capture)
    selected = next(
        item
        for item in member["capture"]["members"]
        if item["role"] == "observations_json"
    )
    selected["content_sha256"] = "1" * 64
    member["source"]["member_content_sha256"] = "1" * 64
    mutations.append(member)

    for request in mutations:
        with pytest.raises(GroundCaptureVerificationError):
            store.commit(request)
    assert _counts(store.path) == (0, 0, 0)


def test_missing_incomplete_or_forged_raw_receipt_fails_closed(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)

    class Missing:
        def load_capture_receipt(self, capture_id):
            return None

    with pytest.raises(GroundCaptureVerificationError, match="missing"):
        _store(tmp_path / "missing.sqlite3", Missing()).commit(request)

    good = raw.load_capture_receipt(capture.capture_id)
    forged = SimpleNamespace(
        **{
            name: getattr(good, name)
            for name in (
                "capture_id",
                "provider",
                "product",
                "fetch_id",
                "upstream_capture_id",
                "product_timestamp",
                "manifest_sha256",
                "capture_receipt_sha256",
                "members",
            )
        },
        raw_store_schema_version=3,
        capture_schema="wrong-schema",
        capture_receipt_schema=good.capture_receipt_schema,
        complete=True,
    )

    class Forged:
        def load_capture_receipt(self, capture_id):
            return forged

    with pytest.raises(GroundCaptureVerificationError, match="invalid"):
        _store(tmp_path / "forged.sqlite3", Forged()).commit(request)


@pytest.mark.parametrize(
    "point",
    [
        "commit.after_begin",
        "commit.after_fact",
        "commit.after_members",
        "commit.after_qc",
        "commit.before_commit",
    ],
)
def test_every_precommit_fault_rolls_back_all_rows(tmp_path, point):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    path = tmp_path / f"fault-{point.replace('.', '-')}.sqlite3"
    store = _store(path, raw)

    def fault(current):
        if current == point:
            raise RuntimeError("injected power loss boundary")

    store._fault_injector = fault
    with pytest.raises(RuntimeError, match="power loss"):
        store.commit(_request(capture))
    assert _counts(path) == (0, 0, 0)
    store._fault_injector = None
    assert store.audit()["healthy"] is True


def _crash_helper_context() -> tuple[Path, Path, dict[str, str]]:
    helper = (
        Path(__file__).parent
        / "helpers/canonical_ground_evidence_crash_child.py"
    )
    repository = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    current = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        str(repository)
        if not current
        else str(repository) + os.pathsep + current
    )
    return helper, repository, environment


def _power_cut(path, raw, mode, point, request=None):
    helper, repository, environment = _crash_helper_context()
    command = [
        sys.executable,
        str(helper),
        str(path.resolve()),
        str(raw.root),
        mode,
        point,
    ]
    if request is not None:
        command.append(json.dumps(request, separators=(",", ":"), sort_keys=True))
    return subprocess.run(
        command,
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize(
    "point",
    ["commit.after_fact", "commit.after_members", "commit.after_qc", "commit.before_commit"],
)
def test_process_power_cut_before_commit_recovers_without_partial_fact(
    tmp_path, point
):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    path = tmp_path / f"power-cut-{point}.sqlite3"
    completed = _power_cut(path, raw, "commit", point, _request(capture))
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    reopened = _store(path, raw)
    assert _counts(path) == (0, 0, 0)
    assert reopened.audit() == {"healthy": True, "checked_facts": 0, "issues": []}


def test_process_power_cut_after_commit_recovers_by_exact_retry(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)
    path = tmp_path / "power-cut-after-commit.sqlite3"
    completed = _power_cut(
        path, raw, "commit", "commit.after_commit", request
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    reopened = _store(path, raw, clock=lambda: COMMITTED_AT + 999.0)
    barriers: list[str] = []
    original_barrier = reopened._durability_barrier

    def barrier():
        barriers.append("retry")
        original_barrier()

    reopened._durability_barrier = barrier
    receipt = reopened.commit(request)
    assert receipt.committed_at == COMMITTED_AT
    assert barriers == ["retry"]
    assert _counts(path) == (1, 2, 2)


def test_process_power_cut_between_sqlite_commit_and_directory_fsync_recovers(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)
    path = tmp_path / "power-cut-before-directory-fsync.sqlite3"
    completed = _power_cut(
        path,
        raw,
        "commit",
        "commit.after_sqlite_commit",
        request,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    reopened = _store(path, raw, clock=lambda: COMMITTED_AT + 999.0)
    barriers: list[str] = []
    original_barrier = reopened._durability_barrier

    def barrier():
        barriers.append("recovery-retry")
        original_barrier()

    reopened._durability_barrier = barrier
    receipt = reopened.commit(request)
    assert receipt.committed_at == COMMITTED_AT
    assert barriers == ["recovery-retry"]
    assert _counts(path) == (1, 2, 2)


def test_exact_retry_cannot_return_when_its_durability_barrier_fails(
    tmp_path, monkeypatch
):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)
    store = _store(tmp_path / "retry-barrier.sqlite3", raw)
    first = store.commit(request)

    def fail():
        raise CanonicalGroundEvidenceIntegrityError(
            "forced retry durability barrier failure"
        )

    monkeypatch.setattr(store, "_durability_barrier", fail)
    with pytest.raises(
        CanonicalGroundEvidenceIntegrityError, match="retry durability"
    ):
        store.commit(request)
    assert _counts(store.path) == (1, 2, 2)
    assert first.fact_id == ground_fact_id(request)


def test_concurrent_process_exact_retries_commit_one_fact(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)
    path = tmp_path / "concurrent-retry.sqlite3"
    helper, repository, environment = _crash_helper_context()
    command = [
        sys.executable,
        str(helper),
        str(path.resolve()),
        str(raw.root),
        "commit",
        "none",
        json.dumps(request, separators=(",", ":"), sort_keys=True),
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
    outputs = [child.communicate(timeout=30) for child in children]
    assert [child.returncode for child in children] == [0, 0, 0, 0], outputs
    reopened = _store(path, raw)
    assert _counts(path) == (1, 2, 2)
    assert reopened.audit()["healthy"] is True


def test_process_power_cut_during_first_schema_creation_leaves_no_partial_schema(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    path = tmp_path / "power-cut-schema.sqlite3"
    completed = _power_cut(
        path,
        raw,
        "initialize",
        "initialize.after_ddl.table.ground_facts",
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)
    with closing(sqlite3.connect(path)) as database, database:
        persisted = database.execute(
            "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
        ).fetchone()[0]
    assert persisted == 0
    reopened = _store(path, raw)
    assert reopened.audit()["healthy"] is True


def test_load_fails_closed_after_raw_object_tamper(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(tmp_path / "raw-tamper.sqlite3", raw)
    receipt = store.commit(_request(capture))
    selected = next(
        member for member in capture.members if member.role == "observations_json"
    )
    raw_receipt = raw.load_receipt(selected.receipt_id)
    assert raw_receipt is not None
    Path(raw_receipt.artifact_path).write_bytes(b"tampered upstream bytes")

    with pytest.raises(GroundCaptureVerificationError):
        store.load(receipt.fact_id)


def test_append_only_triggers_and_schema_fingerprint_reject_mutation(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(tmp_path / "append.sqlite3", raw)
    receipt = store.commit(_request(capture))

    with closing(sqlite3.connect(store.path)) as database, database:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            database.execute(
                "UPDATE ground_facts SET exact_value='9' WHERE fact_id=?",
                (receipt.fact_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            database.execute(
                "DELETE FROM ground_fact_members WHERE fact_id=?",
                (receipt.fact_id,),
            )

    with closing(sqlite3.connect(store.path)) as database, database:
        database.execute("DROP INDEX ground_facts_capture")
    with pytest.raises(CanonicalGroundEvidenceIntegrityError, match="fingerprint"):
        store.load(receipt.fact_id)


def test_materialized_or_receipt_tamper_is_detected_even_with_schema_restored(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(tmp_path / "tamper.sqlite3", raw)
    receipt = store.commit(_request(capture))
    trigger_sql = next(
        sql for name, _table, sql in module._TRIGGER_DDL
        if name == "ground_facts_no_update"
    )
    with closing(sqlite3.connect(store.path)) as database, database:
        database.execute("DROP TRIGGER ground_facts_no_update")
        database.execute(
            "UPDATE ground_facts SET exact_value='9.9' WHERE fact_id=?",
            (receipt.fact_id,),
        )
        database.execute(trigger_sql)
    with pytest.raises(CanonicalGroundEvidenceIntegrityError):
        store.load(receipt.fact_id)


def test_row_member_qc_and_operation_bounds_are_atomic(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(
        tmp_path / "row-limit.sqlite3",
        raw,
        limits=GroundEvidenceLimits(max_observations=1),
    )
    store.commit(_request(capture))
    second = _request(capture)
    second["source"]["record_locator"] += ":second"
    second["source"]["record_sha256"] = "d" * 64
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="row bound"):
        store.commit(second)
    assert _counts(store.path) == (1, 2, 2)

    member_limited = _store(
        tmp_path / "member-limit.sqlite3",
        raw,
        limits=GroundEvidenceLimits(max_member_links=1),
    )
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="member-link"):
        member_limited.commit(_request(capture))
    assert _counts(member_limited.path) == (0, 0, 0)

    qc_limited = _store(
        tmp_path / "qc-limit.sqlite3",
        raw,
        limits=GroundEvidenceLimits(max_qc_rows=1),
    )
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="QC"):
        qc_limited.commit(_request(capture))
    assert _counts(qc_limited.path) == (0, 0, 0)

    operation_limited = _store(
        tmp_path / "operation-limit.sqlite3",
        raw,
        limits=GroundEvidenceLimits(max_operation_bytes=512),
    )
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="byte bound"):
        operation_limited.commit(_request(capture))
    assert _counts(operation_limited.path) == (0, 0, 0)


def test_database_page_limit_and_stats_are_explicit(tmp_path):
    raw = _raw_store(tmp_path)
    store = _store(
        tmp_path / "bounded.sqlite3",
        raw,
        limits=GroundEvidenceLimits(max_catalog_bytes=1024 * 1024),
    )
    stats = store.stats()
    assert stats["database_page_bytes"] <= stats["max_main_database_bytes"]
    assert stats["catalog_file_bytes"] <= stats["max_catalog_bytes"]
    assert stats["max_batch_facts"] == module.HARD_MAX_BATCH_FACTS
    assert (
        stats["max_batch_canonical_bytes"]
        == module.HARD_MAX_BATCH_CANONICAL_BYTES
    )
    with closing(store._connect(None)) as database:
        page_size = database.execute("PRAGMA page_size").fetchone()[0]
        max_pages = database.execute("PRAGMA max_page_count").fetchone()[0]
    assert page_size * max_pages <= 1024 * 1024 // 4


def test_limits_profile_is_frozen_hashed_and_store_bound(tmp_path):
    raw = _raw_store(tmp_path)
    path = tmp_path / "limits-profile.sqlite3"
    limits = GroundEvidenceLimits(
        max_catalog_bytes=4 * 1024 * 1024,
        max_observations=123,
        max_member_links=456,
        max_qc_rows=789,
        max_query_rows=7,
        max_operation_bytes=60_000,
        max_batch_facts=11,
        max_batch_canonical_bytes=500_000,
        sqlite_busy_timeout_seconds=0.75,
    )
    store = _store(path, raw, limits=limits)
    with pytest.raises(FrozenInstanceError):
        limits.max_query_rows = 99

    with closing(sqlite3.connect(path)) as database, database:
        manifest = dict(database.execute("SELECT key,value FROM manifest"))
        mode = database.execute("PRAGMA journal_mode").fetchone()[0]
    profile = json.loads(manifest["limits_profile_json"])
    assert profile == limits.to_mapping()
    assert hashlib.sha256(
        manifest["limits_profile_json"].encode()
    ).hexdigest() == manifest["limits_profile_sha256"]
    assert profile["journal_mode"] == "delete"
    assert profile["main_database_envelope_divisor"] == 4
    assert mode.lower() == "delete"
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()
    assert _store(path, raw, limits=replace(limits))
    assert store.audit()["healthy"] is True


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("max_catalog_bytes", 5 * 1024 * 1024),
        ("max_observations", 124),
        ("max_member_links", 457),
        ("max_qc_rows", 790),
        ("max_query_rows", 8),
        ("max_operation_bytes", 59_999),
        ("max_batch_facts", 12),
        ("max_batch_canonical_bytes", 500_001),
        ("sqlite_busy_timeout_seconds", 0.76),
    ],
)
def test_every_limits_drift_fails_closed_before_use(tmp_path, field, changed):
    raw = _raw_store(tmp_path)
    path = tmp_path / f"limits-drift-{field}.sqlite3"
    limits = GroundEvidenceLimits(
        max_catalog_bytes=4 * 1024 * 1024,
        max_observations=123,
        max_member_links=456,
        max_qc_rows=789,
        max_query_rows=7,
        max_operation_bytes=60_000,
        max_batch_facts=11,
        max_batch_canonical_bytes=500_000,
        sqlite_busy_timeout_seconds=0.75,
    )
    _store(path, raw, limits=limits)
    with pytest.raises(
        CanonicalGroundEvidenceIntegrityError, match="limits profile"
    ):
        _store(path, raw, limits=replace(limits, **{field: changed}))


def _large_request(capture) -> dict:
    request = _request(capture)
    request["provider_qc"] = [
        {
            "flag": f"provider_flag_{index:02d}",
            "value": f"{index:02d}",
            "text": (f"originele-provider-qc-{index:02d}-" + "x" * 760),
        }
        for index in range(64)
    ]
    assert len(json.dumps(request, ensure_ascii=False).encode()) < 64 * 1024
    return request


def test_actual_sqlite_full_is_typed_atomic_and_leaves_no_sidecars(
    tmp_path, monkeypatch
):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    path = tmp_path / "catalog-full.sqlite3"
    limits = GroundEvidenceLimits(max_catalog_bytes=1024 * 1024)
    store = _store(path, raw, limits=limits)
    first_request = _request(capture)
    first = store.commit(first_request)
    with closing(sqlite3.connect(path)) as database, database:
        current_pages = database.execute("PRAGMA page_count").fetchone()[0]
    original = store._main_database_page_limit
    monkeypatch.setattr(
        store,
        "_main_database_page_limit",
        lambda page_size: current_pages,
    )

    large = _large_request(capture)
    large["source"]["record_locator"] += ":catalog-full"
    large["source"]["record_sha256"] = "8" * 64
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="capacity"):
        store.commit(large)

    assert store.commit(first_request) == first

    monkeypatch.setattr(store, "_main_database_page_limit", original)
    assert _counts(path) == (1, 2, 2)
    assert store.audit()["healthy"] is True
    assert store._catalog_physical_bytes() <= limits.max_catalog_bytes


def test_actual_sqlite_full_rolls_back_complete_batch(tmp_path, monkeypatch):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    path = tmp_path / "batch-catalog-full.sqlite3"
    limits = GroundEvidenceLimits(max_catalog_bytes=1024 * 1024)
    store = _store(path, raw, limits=limits)
    existing_request = _request(capture)
    existing = store.commit(existing_request)
    with closing(sqlite3.connect(path)) as database, database:
        current_pages = database.execute("PRAGMA page_count").fetchone()[0]
    original = store._main_database_page_limit
    monkeypatch.setattr(
        store,
        "_main_database_page_limit",
        lambda page_size: current_pages,
    )
    first = _large_request(capture)
    first["source"]["record_locator"] += ":batch-full-first"
    first["source"]["record_sha256"] = "7" * 64
    second = _large_request(capture)
    second["source"]["record_locator"] += ":batch-full-second"
    second["source"]["record_sha256"] = "6" * 64

    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="capacity"):
        store.commit_batch((first, second))

    assert store.commit(existing_request) == existing
    monkeypatch.setattr(store, "_main_database_page_limit", original)
    assert _counts(path) == (1, 2, 2)
    assert store.audit()["healthy"] is True
    assert store._catalog_physical_bytes() <= limits.max_catalog_bytes
    assert not Path(str(path) + "-journal").exists()
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()


def test_pinned_reader_cannot_create_wal_or_escape_catalog_envelope(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    path = tmp_path / "pinned-reader.sqlite3"
    limits = GroundEvidenceLimits(
        max_catalog_bytes=4 * 1024 * 1024,
        sqlite_busy_timeout_seconds=0.25,
    )
    store = _store(path, raw, limits=limits)
    store.commit(_request(capture))
    second = _large_request(capture)
    second["source"]["record_locator"] += ":pinned-reader"
    second["source"]["record_sha256"] = "9" * 64

    reader = sqlite3.connect(path, timeout=1.0)
    reader.execute("BEGIN")
    reader.execute("SELECT canonical_fact FROM ground_facts").fetchone()
    outcome: list[BaseException | None] = []

    def write() -> None:
        try:
            store.commit(second, deadline_at=time.monotonic() + 0.35)
        except BaseException as exc:
            outcome.append(exc)
        else:
            outcome.append(None)

    writer = threading.Thread(target=write)
    writer.start()
    journal = Path(str(path) + "-journal")
    deadline = time.monotonic() + 0.25
    observed_journal = False
    while time.monotonic() < deadline and writer.is_alive():
        if journal.exists():
            observed_journal = True
            assert store._catalog_physical_bytes() <= limits.max_catalog_bytes
            break
        time.sleep(0.005)
    writer.join(timeout=2.0)
    reader.rollback()
    reader.close()
    writer.join(timeout=2.0)

    assert not writer.is_alive()
    assert observed_journal is True
    assert len(outcome) == 1
    assert isinstance(outcome[0], CanonicalGroundEvidenceDeadlineError)
    assert _counts(path) == (1, 2, 2)
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()
    assert not journal.exists()
    assert store._catalog_physical_bytes() <= limits.max_catalog_bytes


def test_forbidden_wal_sidecar_fails_closed(tmp_path):
    raw = _raw_store(tmp_path)
    path = tmp_path / "forbidden-wal.sqlite3"
    _store(path, raw)
    Path(str(path) + "-wal").write_bytes(b"")
    with pytest.raises(CanonicalGroundEvidenceIntegrityError, match="WAL"):
        _store(path, raw)


def test_posix_directory_fsync_is_used_for_initialization_and_commit(
    tmp_path, monkeypatch
):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    calls: list[Path] = []
    monkeypatch.setattr(
        module.CanonicalGroundEvidenceStore,
        "_fsync_directory",
        staticmethod(lambda path: calls.append(Path(path))),
    )
    store = _store(tmp_path / "fsync-success" / "catalog.sqlite3", raw)
    before_commit = len(calls)
    store.commit(_request(capture))

    assert before_commit > 0
    assert len(calls) > before_commit
    assert store.path.parent in calls


def test_posix_directory_fsync_calls_open_fsync_and_close(tmp_path, monkeypatch):
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(module, "DIRECTORY_FSYNC_SUPPORTED", True)
    monkeypatch.setattr(
        module.os,
        "open",
        lambda path, flags: calls.append(("open", Path(path))) or 4242,
    )
    monkeypatch.setattr(
        module.os,
        "fsync",
        lambda descriptor: calls.append(("fsync", descriptor)),
    )
    monkeypatch.setattr(
        module.os,
        "close",
        lambda descriptor: calls.append(("close", descriptor)),
    )
    module.CanonicalGroundEvidenceStore._fsync_directory(tmp_path)
    assert calls[0] == ("open", tmp_path)
    assert calls[1:] == [("fsync", 4242), ("close", 4242)]


def test_posix_directory_fsync_failure_is_never_silently_swallowed(
    tmp_path, monkeypatch
):
    raw = _raw_store(tmp_path)
    monkeypatch.setattr(module, "DIRECTORY_FSYNC_SUPPORTED", True)

    def fail_open(path, flags):
        raise OSError("simulated directory fsync failure")

    monkeypatch.setattr(module.os, "open", fail_open)
    with pytest.raises(CanonicalGroundEvidenceIntegrityError, match="fsync failed"):
        _store(tmp_path / "fsync-failure" / "catalog.sqlite3", raw)


def test_fsync_failure_after_sqlite_commit_is_recoverable_by_exact_retry(
    tmp_path, monkeypatch
):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    request = _request(capture)
    store = _store(tmp_path / "postcommit-fsync.sqlite3", raw)

    def fail(_path):
        raise CanonicalGroundEvidenceIntegrityError("directory fsync failed")

    monkeypatch.setattr(store, "_fsync_directory", fail)
    with pytest.raises(CanonicalGroundEvidenceIntegrityError, match="fsync"):
        store.commit(request)
    monkeypatch.undo()

    reopened = _store(store.path, raw, clock=lambda: COMMITTED_AT + 999.0)
    receipt = reopened.commit(request)
    assert receipt.committed_at == COMMITTED_AT
    assert _counts(store.path) == (1, 2, 2)


def test_windows_directory_fsync_limitation_is_explicit(tmp_path, monkeypatch):
    raw = _raw_store(tmp_path)
    monkeypatch.setattr(module, "DIRECTORY_FSYNC_SUPPORTED", False)

    def must_not_open(path, flags):
        raise AssertionError("Windows limitation must not fake directory fsync")

    monkeypatch.setattr(module.os, "open", must_not_open)
    store = _store(tmp_path / "windows-fsync.sqlite3", raw)
    assert store.stats()["directory_fsync_supported"] == 0


def test_bounded_query_filters_and_reverifies_every_result(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)

    class CountingReader:
        def __init__(self, delegate):
            self.delegate = delegate
            self.reads = 0

        def load_capture_receipt(self, capture_id):
            self.reads += 1
            return self.delegate.load_capture_receipt(capture_id)

    reader = CountingReader(raw)
    store = _store(
        tmp_path / "query.sqlite3",
        reader,
        limits=GroundEvidenceLimits(max_query_rows=2),
    )
    rain = _request(capture)
    temperature = _request(capture)
    temperature["source"]["record_locator"] += ":temperature"
    temperature["source"]["record_sha256"] = "e" * 64
    temperature["measurement"].update(
        variable="air_temperature",
        exact_value="12.30",
        unit="Cel",
    )
    store.commit(rain)
    store.commit(temperature)
    before = reader.reads

    results = store.query(
        site_id="site:synthetic:SYN001",
        variable="precipitation_accumulation",
        limit=2,
    )

    assert len(results) == 1
    assert results[0].fact.measurement.variable == "precipitation_accumulation"
    assert reader.reads == before + 1
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="query"):
        store.query(limit=3)


def test_expired_deadline_fails_before_raw_authority_and_writes_nothing(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)

    class CountingReader:
        reads = 0

        def load_capture_receipt(self, capture_id):
            self.reads += 1
            return raw.load_capture_receipt(capture_id)

    reader = CountingReader()
    store = _store(tmp_path / "deadline.sqlite3", reader, monotonic=lambda: 10.0)
    with pytest.raises(CanonicalGroundEvidenceDeadlineError):
        store.commit(_request(capture), deadline_at=10.0)
    assert reader.reads == 0
    assert _counts(store.path) == (0, 0, 0)


def test_deadline_elapsed_during_raw_verification_fails_before_sqlite_write(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    current = [0.0]

    class AdvancingReader:
        def load_capture_receipt(self, capture_id):
            result = raw.load_capture_receipt(capture_id)
            current[0] = 2.0
            return result

    store = _store(
        tmp_path / "deadline-after-raw.sqlite3",
        AdvancingReader(),
        monotonic=lambda: current[0],
    )
    with pytest.raises(CanonicalGroundEvidenceDeadlineError):
        store.commit(_request(capture), deadline_at=1.0)
    assert _counts(store.path) == (0, 0, 0)


def test_batch_reads_raw_authority_once_and_returns_ordered_receipts(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)

    class CountingReader:
        reads = 0

        def load_capture_receipt(self, capture_id):
            self.reads += 1
            return raw.load_capture_receipt(capture_id)

    reader = CountingReader()
    store = _store(tmp_path / "batch-one-read.sqlite3", reader)
    first = _batch_request(capture, "first")
    second = _batch_request(capture, "second")

    receipts = store.commit_batch((first, second))

    assert reader.reads == 1
    assert [item.fact.source_record_locator for item in receipts] == [
        first["source"]["record_locator"],
        second["source"]["record_locator"],
    ]
    assert _counts(store.path) == (2, 4, 4)


def test_batch_exact_duplicates_mixed_retry_and_all_existing_use_one_barrier(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(tmp_path / "batch-mixed.sqlite3", raw)
    first = _batch_request(capture, "first")
    second = _batch_request(capture, "second")
    first_receipt = store.commit(first)
    barriers: list[str] = []
    original_barrier = store._durability_barrier

    def barrier():
        barriers.append("barrier")
        original_barrier()

    store._durability_barrier = barrier
    mixed = store.commit_batch((first, second, first))
    assert barriers == ["barrier"]
    assert mixed[0] == first_receipt
    assert mixed[0] == mixed[2]
    assert mixed[0].fact_id != mixed[1].fact_id
    assert _counts(store.path) == (2, 4, 4)

    barriers.clear()
    retried = store.commit_batch((second, first, second))
    assert barriers == ["barrier"]
    assert [item.fact_id for item in retried] == [
        mixed[1].fact_id,
        mixed[0].fact_id,
        mixed[1].fact_id,
    ]
    assert _counts(store.path) == (2, 4, 4)


def test_batch_rejects_generator_empty_is_noop_and_bounds_precede_raw(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)

    class CountingReader:
        reads = 0

        def load_capture_receipt(self, capture_id):
            self.reads += 1
            return raw.load_capture_receipt(capture_id)

    reader = CountingReader()
    store = _store(tmp_path / "batch-shape.sqlite3", reader)
    barriers: list[str] = []
    store._durability_barrier = lambda: barriers.append("unexpected")
    assert store.commit_batch(()) == ()
    with pytest.raises(TypeError, match="concrete bounded sequence"):
        store.commit_batch(item for item in (_request(capture),))
    assert reader.reads == 0
    assert barriers == []
    assert _counts(store.path) == (0, 0, 0)

    count_limited = _store(
        tmp_path / "batch-count.sqlite3",
        reader,
        limits=GroundEvidenceLimits(max_batch_facts=1),
    )
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="fact-count"):
        count_limited.commit_batch(
            (_batch_request(capture, "a"), _batch_request(capture, "b"))
        )
    byte_limited = _store(
        tmp_path / "batch-bytes.sqlite3",
        reader,
        limits=GroundEvidenceLimits(max_batch_canonical_bytes=1),
    )
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match="canonical-byte"):
        byte_limited.commit_batch((_request(capture),))
    assert reader.reads == 0
    assert _counts(count_limited.path) == (0, 0, 0)
    assert _counts(byte_limited.path) == (0, 0, 0)


def test_batch_conflict_and_authority_mismatch_fail_before_raw_or_db(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw, "one")

    class CountingReader:
        reads = 0

        def load_capture_receipt(self, capture_id):
            self.reads += 1
            return raw.load_capture_receipt(capture_id)

    reader = CountingReader()
    store = _store(tmp_path / "batch-preflight.sqlite3", reader)
    base = _request(capture)
    changed = deepcopy(base)
    changed["measurement"]["exact_value"] = "0.20"
    with pytest.raises(CanonicalGroundEvidenceConflict, match="observation identity"):
        store.commit_batch((base, changed))
    assert reader.reads == 0

    other_capture = _capture(raw, "two")
    with pytest.raises(GroundCaptureVerificationError, match="one exact raw authority"):
        store.commit_batch((base, _request(other_capture)))
    assert reader.reads == 0
    assert _counts(store.path) == (0, 0, 0)


def test_batch_later_fact_is_checked_against_single_verified_authority(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)

    class CountingReader:
        reads = 0

        def load_capture_receipt(self, capture_id):
            self.reads += 1
            return raw.load_capture_receipt(capture_id)

    reader = CountingReader()
    store = _store(tmp_path / "batch-source-binding.sqlite3", reader)
    first = _batch_request(capture, "first")
    second = _batch_request(capture, "second")
    second["time"]["received_at"] = RECEIVED_AT - 0.5
    with pytest.raises(
        GroundCaptureVerificationError, match="exact raw member receipt"
    ):
        store.commit_batch((first, second))
    assert reader.reads == 1
    assert _counts(store.path) == (0, 0, 0)


@pytest.mark.parametrize(
    "point",
    [
        "commit_batch.after_begin",
        "commit_batch.after_fact",
        "commit_batch.after_members",
        "commit_batch.after_qc",
        "commit_batch.before_commit",
    ],
)
def test_every_batch_precommit_fault_rolls_back_every_fact(tmp_path, point):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    path = tmp_path / f"batch-fault-{point.replace('.', '-')}.sqlite3"
    store = _store(path, raw)

    def fault(current):
        if current == point:
            raise RuntimeError("injected batch power loss boundary")

    store._fault_injector = fault
    with pytest.raises(RuntimeError, match="batch power loss"):
        store.commit_batch(
            (_batch_request(capture, "first"), _batch_request(capture, "second"))
        )
    assert _counts(path) == (0, 0, 0)


def test_mid_batch_fault_and_deadline_roll_back_whole_batch_and_retry(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    requests = (_batch_request(capture, "first"), _batch_request(capture, "second"))
    path = tmp_path / "batch-mid-fault.sqlite3"
    store = _store(path, raw)
    visits = 0

    def fault(point):
        nonlocal visits
        if point == "commit_batch.after_batch_fact":
            visits += 1
            if visits == 1:
                raise RuntimeError("mid-batch")

    store._fault_injector = fault
    with pytest.raises(RuntimeError, match="mid-batch"):
        store.commit_batch(requests)
    assert _counts(path) == (0, 0, 0)
    store._fault_injector = None
    assert len(store.commit_batch(requests)) == 2
    assert _counts(path) == (2, 4, 4)

    current = [0.0]
    deadline_store = _store(
        tmp_path / "batch-mid-deadline.sqlite3",
        raw,
        monotonic=lambda: current[0],
    )

    def advance(point):
        if point == "commit_batch.after_batch_fact":
            current[0] = 2.0

    deadline_store._fault_injector = advance
    with pytest.raises(CanonicalGroundEvidenceDeadlineError):
        deadline_store.commit_batch(requests, deadline_at=1.0)
    assert _counts(deadline_store.path) == (0, 0, 0)


def test_batch_post_sqlite_commit_fault_is_durable_and_exactly_retryable(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    requests = (_batch_request(capture, "first"), _batch_request(capture, "second"))
    store = _store(tmp_path / "batch-postcommit.sqlite3", raw)

    def fault(point):
        if point == "commit_batch.after_sqlite_commit":
            raise RuntimeError("post-sqlite-commit")

    store._fault_injector = fault
    with pytest.raises(RuntimeError, match="post-sqlite-commit"):
        store.commit_batch(requests)
    assert _counts(store.path) == (2, 4, 4)

    store._fault_injector = None
    barriers: list[str] = []
    original_barrier = store._durability_barrier

    def barrier():
        barriers.append("retry")
        original_barrier()

    store._durability_barrier = barrier
    assert len(store.commit_batch(requests)) == 2
    assert barriers == ["retry"]
    assert _counts(store.path) == (2, 4, 4)


@pytest.mark.parametrize(
    ("limits", "match"),
    [
        (GroundEvidenceLimits(max_observations=1), "row bound"),
        (GroundEvidenceLimits(max_member_links=3), "member-link"),
        (GroundEvidenceLimits(max_qc_rows=3), "QC"),
    ],
)
def test_batch_aggregate_capacity_rejects_all_new_facts(tmp_path, limits, match):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(
        tmp_path / "batch-capacity.sqlite3",
        raw,
        limits=limits,
    )
    with pytest.raises(CanonicalGroundEvidenceCapacityError, match=match):
        store.commit_batch(
            (_batch_request(capture, "first"), _batch_request(capture, "second"))
        )
    assert _counts(store.path) == (0, 0, 0)


def test_existing_conflict_rejects_new_prefix_before_first_insert(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    store = _store(tmp_path / "batch-existing-conflict.sqlite3", raw)
    existing = _batch_request(capture, "existing")
    new = _batch_request(capture, "new")
    conflict = deepcopy(existing)
    conflict["measurement"]["exact_value"] = "9.99"
    persisted = store.commit(existing)

    with pytest.raises(CanonicalGroundEvidenceConflict, match="observation identity"):
        store.commit_batch((new, conflict))

    assert _counts(store.path) == (1, 2, 2)
    assert store.commit(existing) == persisted


@pytest.mark.parametrize(
    "point",
    [
        "commit_batch.after_fact",
        "commit_batch.after_batch_fact",
        "commit_batch.before_commit",
    ],
)
def test_process_power_cut_mid_batch_recovers_without_partial_facts(tmp_path, point):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    path = tmp_path / f"batch-power-cut-{point}.sqlite3"
    requests = [_batch_request(capture, "first"), _batch_request(capture, "second")]
    completed = _power_cut(path, raw, "commit_batch", point, requests)
    assert completed.returncode == 73, (completed.stdout, completed.stderr)
    reopened = _store(path, raw)
    assert _counts(path) == (0, 0, 0)
    assert len(reopened.commit_batch(requests)) == 2


def test_process_power_cut_after_batch_commit_recovers_exact_batch(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    path = tmp_path / "batch-power-cut-after-commit.sqlite3"
    requests = [_batch_request(capture, "first"), _batch_request(capture, "second")]
    completed = _power_cut(
        path,
        raw,
        "commit_batch",
        "commit_batch.after_sqlite_commit",
        requests,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)
    reopened = _store(path, raw, clock=lambda: COMMITTED_AT + 999.0)
    retried = reopened.commit_batch(requests)
    assert len(retried) == 2
    assert {item.committed_at for item in retried} == {COMMITTED_AT}
    assert _counts(path) == (2, 4, 4)


def test_concurrent_process_exact_batch_retries_commit_one_batch(tmp_path):
    raw = _raw_store(tmp_path)
    capture = _capture(raw)
    requests = [_batch_request(capture, "first"), _batch_request(capture, "second")]
    path = tmp_path / "concurrent-batch-retry.sqlite3"
    helper, repository, environment = _crash_helper_context()
    command = [
        sys.executable,
        str(helper),
        str(path.resolve()),
        str(raw.root),
        "commit_batch",
        "none",
        json.dumps(requests, separators=(",", ":"), sort_keys=True),
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
    outputs = [child.communicate(timeout=30) for child in children]
    assert [child.returncode for child in children] == [0, 0, 0, 0], outputs
    reopened = _store(path, raw)
    assert _counts(path) == (2, 4, 4)
    assert reopened.audit()["healthy"] is True


def test_candidate_module_is_isolated_from_service_and_home_assistant():
    root = Path(__file__).resolve().parents[1]
    module_name = "canonical_ground_evidence_store"
    importers: list[str] = []
    for path in [
        *(root / "storm_tracker_worker").rglob("*.py"),
        *(root / "custom_components").rglob("*.py"),
    ]:
        if path.name == f"{module_name}.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if module_name in node.module:
                    importers.append(str(path.relative_to(root)))
            elif isinstance(node, ast.Import):
                if any(module_name in alias.name for alias in node.names):
                    importers.append(str(path.relative_to(root)))

    # Provider-specific shadow decoders are now the intended consumers of
    # this lowest-level fact store.  Additional consumers are restricted to the
    # exact raw-reverified fusion bridge, its durable still-unwired shadow
    # orchestrator, the one closed-world METAR factory which constructs that
    # orchestrator, and the inert terminal-reference METAR completion consumer.
    # Keep the boundary closed
    # everywhere else: no HA module, service entry point or generic provider
    # runtime may import it, and every allowed consumer must expose an explicit
    # admission denial in its source.
    assert importers
    for importer in importers:
        normalized = importer.replace("\\", "/")
        assert normalized.startswith("storm_tracker_worker/")
        assert (
            normalized.endswith(("_ground_decoder.py", "_ground_evidence.py"))
            or normalized == (
                "storm_tracker_worker/tracking_core/engine/"
                "canonical_ground_fusion_bridge.py"
            )
            or normalized == (
                "storm_tracker_worker/ground_fusion_shadow_runtime.py"
            )
            or normalized == (
                "storm_tracker_worker/metar_ground_shadow_factory.py"
            )
            or normalized == (
                "storm_tracker_worker/metar_ground_completion_consumer.py"
            )
        )
        source = (root / importer).read_text(encoding="utf-8")
        if normalized.endswith("metar_ground_shadow_factory.py"):
            assert 'config["operational_admission_granted"] is not False' in source
            assert "cannot grant operational admission" in source
        else:
            assert "operational_admission_eligible" in source

    service = (root / "storm_tracker_worker" / "service.py").read_text(
        encoding="utf-8"
    )
    assert module_name not in service
    assert "canonical_ground" not in service


def test_contract_rejects_unknown_keys_and_any_admission_claim(tmp_path):
    capture = _capture(_raw_store(tmp_path))
    base = _request(capture)
    unsafe = deepcopy(base)
    unsafe["operational_admission_eligible"] = True
    with pytest.raises(ValueError, match="candidate-only"):
        parse_ground_observation(unsafe)
    unsafe = deepcopy(base)
    unsafe["admission_status"] = "admitted"
    with pytest.raises(ValueError, match="cannot claim admission"):
        parse_ground_observation(unsafe)
    unsafe = deepcopy(base)
    unsafe["ground_confirmation"] = True
    with pytest.raises(ValueError, match="keys mismatch"):
        parse_ground_observation(unsafe)


def test_limits_reject_unbounded_or_invalid_configuration():
    with pytest.raises(ValueError):
        GroundEvidenceLimits(max_query_rows=0)
    with pytest.raises(ValueError):
        GroundEvidenceLimits(max_operation_bytes=65_537)
    with pytest.raises(ValueError):
        GroundEvidenceLimits(max_batch_facts=0)
    with pytest.raises(ValueError):
        GroundEvidenceLimits(max_batch_facts=module.HARD_MAX_BATCH_FACTS + 1)
    with pytest.raises(ValueError):
        GroundEvidenceLimits(max_batch_canonical_bytes=0)
    with pytest.raises(ValueError):
        GroundEvidenceLimits(
            max_batch_canonical_bytes=module.HARD_MAX_BATCH_CANONICAL_BYTES + 1
        )
    with pytest.raises(ValueError):
        GroundEvidenceLimits(max_catalog_bytes=1024)
    with pytest.raises(ValueError):
        GroundEvidenceLimits(sqlite_busy_timeout_seconds=60)
