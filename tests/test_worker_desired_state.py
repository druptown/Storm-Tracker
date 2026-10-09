from __future__ import annotations

import copy
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from storm_tracker_worker import desired_state as ds

RECEIVED = 1_800_000_010.0


def _claim(
    store: ds.DesiredStateStore,
    *,
    producer: str = "ha-boot-a",
    claim_id: str = "claim-a",
    claimed_at: float = 1_799_999_999.0,
) -> dict:
    return store.claim_producer(
        producer, claim_id, claimed_at=claimed_at
    )


def _state(
    *,
    producer: str = "ha-boot-a",
    authority_epoch: int = 1,
    generation: int = 1,
    previous: str | None = None,
) -> dict:
    return {
        "contract": ds.CONTRACT,
        "producer_id": producer,
        "authority_epoch": authority_epoch,
        "generation": generation,
        "issued_at": 1_800_000_000.0 + generation,
        "previous_state_sha256": previous,
        "targets": [
            {
                "target_id": "device_tracker.wim",
                "display_name": "Wim",
                "kind": "person",
                "available": True,
                "latitude": 40.7128,
                "longitude": -74.006,
                "position_observed_at": 1_800_000_000.0,
                "location_accuracy_m": 15.0,
                "observation_radius_km": 250.0,
                "country_code": "US",
            },
            {
                "target_id": "zone.home",
                "display_name": "Thuis",
                "kind": "home",
                "available": True,
                "latitude": 51.04,
                "longitude": 4.48,
                "position_observed_at": 1_799_999_990.0,
                "location_accuracy_m": None,
                "observation_radius_km": 300.0,
                "country_code": "BE",
            },
        ],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 150.0,
        },
    }


def test_contract_is_order_independent_and_separates_event_time() -> None:
    first = _state()
    second = copy.deepcopy(first)
    second["targets"].reverse()
    parsed = ds.parse_desired_state(first)
    reordered = ds.parse_desired_state(second)
    assert parsed.canonical_bytes == reordered.canonical_bytes
    assert parsed.state_sha256 == reordered.state_sha256
    assert parsed.issued_at == 1_800_000_001.0
    assert [target.target_id for target in parsed.targets] == [
        "device_tracker.wim",
        "zone.home",
    ]


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: value.update(extra=True),
        lambda value: value.update(contract="v0"),
        lambda value: value.update(generation=True),
        lambda value: value.update(authority_epoch=0),
        lambda value: value.update(generation=-1),
        lambda value: value.update(issued_at=float("nan")),
        lambda value: value.update(previous_state_sha256="x" * 64),
        lambda value: value.update(producer_id="/invalid"),
        lambda value: value["targets"].append(copy.deepcopy(value["targets"][0])),
        lambda value: value["targets"][0].update(target_id="@invalid"),
        lambda value: value["targets"][0].update(display_name=" Cafe\u0301"),
        lambda value: value["targets"][0].update(latitude=91),
        lambda value: value["targets"][0].update(available=1),
        lambda value: value["targets"][0].update(position_observed_at=None),
        lambda value: value["targets"][0].update(location_accuracy_m=float("inf")),
        lambda value: value["targets"][0].update(country_code="usa"),
        lambda value: value["targets"][0].update(secret="forbidden"),
        lambda value: value["settings"].update(model_guidance_enabled=1),
        lambda value: value["settings"].update(lightning_source_mode="magic"),
    ],
)
def test_contract_fails_closed(mutator) -> None:
    value = _state()
    mutator(value)
    with pytest.raises(ds.DesiredStateError):
        ds.parse_desired_state(value)


def test_contract_rejects_duplicate_json_keys_and_oversize() -> None:
    raw = json.dumps(_state(), separators=(",", ":"))
    duplicate = raw.replace(
        '"generation":1,', '"generation":1,"generation":2,', 1
    )
    with pytest.raises(ds.DesiredStateError, match="dubbele"):
        ds.parse_desired_state(duplicate)
    with pytest.raises(ds.DesiredStateError, match="bytebudget"):
        ds.parse_desired_state(b"{" + b" " * ds.MAX_CONTRACT_BYTES + b"}")


def test_store_bootstrap_retry_cas_and_restart(tmp_path) -> None:
    path = tmp_path / "desired.sqlite3"
    store = ds.DesiredStateStore(path)
    authority = _claim(store)
    assert authority["authority_epoch"] == 1
    first_value = _state()
    first = store.apply(first_value, received_at=RECEIVED)
    assert first["status"] == "activated"

    retry = store.apply(first_value, received_at=RECEIVED + 20.0)
    assert retry["status"] == "idempotent"
    assert retry["state_sha256"] == first["state_sha256"]
    assert retry["first_received_at"] == RECEIVED
    assert retry["last_received_at"] == RECEIVED + 20.0
    assert retry["receipt_count"] == 2

    current = ds.DesiredStateStore(path).current()
    assert current["status"] == "configured"
    assert current["state_sha256"] == first["state_sha256"]
    assert current["target_count"] == 2
    assert current["first_received_at"] == RECEIVED
    assert current["last_received_at"] == RECEIVED + 20.0
    assert "desired_state" not in current
    assert ds.DesiredStateStore(path).load_active().targets[0].target_id == (
        "device_tracker.wim"
    )

    second_value = _state(generation=2, previous=first["state_sha256"])
    second_value["targets"][0]["latitude"] = 40.8
    second = ds.DesiredStateStore(path).apply(
        second_value, received_at=RECEIVED + 30.0
    )
    assert second["status"] == "activated"
    assert second["state_sha256"] != first["state_sha256"]
    assert ds.DesiredStateStore(path).current()["generation"] == 2


def test_store_rejects_stale_fork_and_equal_generation_conflict(tmp_path) -> None:
    store = ds.DesiredStateStore(tmp_path / "desired.sqlite3")
    _claim(store)
    first = store.apply(_state(generation=4), received_at=RECEIVED)

    fork = _state(generation=5, previous="0" * 64)
    with pytest.raises(ds.DesiredStateConflict, match="actieve SHA"):
        store.apply(fork, received_at=RECEIVED + 1.0)

    equal = _state(generation=4, previous=first["state_sha256"])
    equal["targets"][0]["longitude"] = -73.5
    with pytest.raises(ds.DesiredStateConflict, match="strikt stijgen"):
        store.apply(equal, received_at=RECEIVED + 2.0)

    assert store.current()["state_sha256"] == first["state_sha256"]


def test_new_producer_can_take_over_only_from_active_head(tmp_path) -> None:
    store = ds.DesiredStateStore(tmp_path / "desired.sqlite3")
    _claim(store, producer="ha-old", claim_id="claim-old")
    first = store.apply(
        _state(producer="ha-old", generation=99), received_at=RECEIVED + 100.0
    )
    authority = _claim(
        store,
        producer="ha-new",
        claim_id="claim-new",
        claimed_at=RECEIVED + 101.0,
    )
    takeover = _state(
        producer="ha-new",
        authority_epoch=authority["authority_epoch"],
        generation=0,
        previous=first["state_sha256"],
    )
    result = store.apply(takeover, received_at=RECEIVED + 102.0)
    assert result["status"] == "activated"
    assert result["producer_id"] == "ha-new"
    assert result["generation"] == 0
    old_after_takeover = _state(
        producer="ha-old",
        authority_epoch=1,
        generation=100,
        previous=result["state_sha256"],
    )
    with pytest.raises(ds.DesiredStateConflict, match="fencing"):
        store.apply(old_after_takeover, received_at=RECEIVED + 103.0)


def test_first_state_cannot_claim_unknown_previous_head(tmp_path) -> None:
    store = ds.DesiredStateStore(tmp_path / "desired.sqlite3")
    _claim(store)
    with pytest.raises(ds.DesiredStateConflict, match="eerste"):
        store.apply(_state(previous="0" * 64), received_at=RECEIVED)
    assert store.current()["status"] == "unconfigured"


def test_handlers_are_strict_and_snapshot_is_bounded(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(ds, "DEFAULT_STORE_PATH", tmp_path / "state.sqlite3")
    # Default arguments are bound at function definition, so replace the
    # constructor with a scoped factory as the production handlers see it.
    real_store = ds.DesiredStateStore
    monkeypatch.setattr(
        ds,
        "DesiredStateStore",
        lambda: real_store(tmp_path / "state.sqlite3"),
    )
    monkeypatch.setattr(ds.time, "time", lambda: RECEIVED)
    claimed = ds.desired_state_claim(SimpleNamespace(payload={
        "producer_id": "ha-boot-a",
        "claim_id": "claim-handler",
    }))
    assert claimed["authority_epoch"] == 1
    applied = ds.desired_state_apply(
        SimpleNamespace(payload={"desired_state": _state()})
    )
    assert applied["target_count"] == 2
    snapshot = ds.desired_state_snapshot(SimpleNamespace(payload={}))
    assert snapshot["state_sha256"] == applied["state_sha256"]
    assert len(json.dumps(snapshot).encode("utf-8")) < ds.MAX_CONTRACT_BYTES
    assert "desired_state" not in snapshot
    with pytest.raises(ds.DesiredStateError):
        ds.desired_state_apply(SimpleNamespace(payload={"desired_state": _state(), "x": 1}))
    with pytest.raises(ds.DesiredStateError):
        ds.desired_state_snapshot(SimpleNamespace(payload={"x": 1}))
    with pytest.raises(ds.DesiredStateError):
        ds.desired_state_claim(SimpleNamespace(payload={"producer_id": "ha-boot-a"}))


def test_authority_claim_is_idempotent_and_fences_old_boot(tmp_path) -> None:
    store = ds.DesiredStateStore(tmp_path / "desired.sqlite3")
    first = _claim(store, claim_id="claim-one")
    retry = store.claim_producer(
        "ha-boot-a", "claim-one", claimed_at=RECEIVED + 50.0
    )
    assert retry["status"] == "claim_idempotent"
    assert retry["authority_epoch"] == first["authority_epoch"] == 1
    assert retry["claimed_at"] == first["claimed_at"]
    with pytest.raises(ds.DesiredStateConflict, match="andere producer"):
        store.claim_producer(
            "ha-boot-b", "claim-one", claimed_at=RECEIVED + 51.0
        )
    second = store.claim_producer(
        "ha-boot-b", "claim-two", claimed_at=RECEIVED + 52.0
    )
    assert second["authority_epoch"] == 2
    snapshot = store.current()
    assert snapshot == {
        "method": ds.METHOD,
        "status": "unconfigured",
        "producer_id": "ha-boot-b",
        "authority_epoch": 2,
    }


def test_moving_target_and_issued_times_cannot_regress_or_be_stale(tmp_path) -> None:
    store = ds.DesiredStateStore(tmp_path / "desired.sqlite3")
    _claim(store)
    first = store.apply(_state(), received_at=RECEIVED)

    regressive = _state(generation=2, previous=first["state_sha256"])
    regressive["targets"][0]["position_observed_at"] -= 1.0
    with pytest.raises(ds.DesiredStateConflict, match="position_observed_at"):
        store.apply(regressive, received_at=RECEIVED + 1.0)

    issued_backwards = _state(generation=2, previous=first["state_sha256"])
    issued_backwards["issued_at"] = _state()["issued_at"] - 1.0
    issued_backwards["targets"][0]["position_observed_at"] += 1.0
    with pytest.raises(ds.DesiredStateConflict, match="issued_at"):
        store.apply(issued_backwards, received_at=RECEIVED + 2.0)

    stale = _state(generation=2, previous=first["state_sha256"])
    stale["targets"][0]["position_observed_at"] += 1.0
    with pytest.raises(ds.DesiredStateConflict, match="te oud"):
        store.apply(
            stale,
            received_at=(
                stale["targets"][0]["position_observed_at"]
                + ds.MAX_DYNAMIC_POSITION_AGE_SECONDS
                + 1.0
            ),
        )

    future = _state(generation=2, previous=first["state_sha256"])
    future["targets"][0]["position_observed_at"] = (
        RECEIVED + ds.MAX_CLOCK_FUTURE_SKEW_SECONDS + 2.0
    )
    with pytest.raises(ds.DesiredStateConflict, match="toekomst"):
        store.apply(future, received_at=RECEIVED + 1.0)
    assert store.current()["state_sha256"] == first["state_sha256"]


@pytest.mark.parametrize("corruption", ("canonical", "metadata", "orphan"))
def test_corrupt_active_state_fails_closed(tmp_path, corruption) -> None:
    path = tmp_path / f"desired-{corruption}.sqlite3"
    store = ds.DesiredStateStore(path)
    _claim(store)
    first = store.apply(_state(), received_at=RECEIVED)
    with closing(sqlite3.connect(path)) as database, database:
        if corruption == "canonical":
            database.execute(
                "UPDATE desired_states SET canonical_json=? WHERE state_sha256=?",
                (b'{"broken":true}', first["state_sha256"]),
            )
        elif corruption == "metadata":
            database.execute(
                "UPDATE desired_states SET producer_id='forged', generation=0 "
                "WHERE state_sha256=?",
                (first["state_sha256"],),
            )
        else:
            database.execute(
                "DELETE FROM desired_states WHERE state_sha256=?",
                (first["state_sha256"],),
            )
    with pytest.raises(ds.DesiredStateError):
        ds.DesiredStateStore(path).current()
    with pytest.raises(ds.DesiredStateError):
        ds.DesiredStateStore(path).apply(_state(), received_at=RECEIVED + 1.0)


def test_history_and_authority_bounds_preserve_last_consistent_heads(
    monkeypatch, tmp_path
) -> None:
    path = tmp_path / "desired.sqlite3"
    store = ds.DesiredStateStore(path)
    _claim(store)
    first = store.apply(_state(), received_at=RECEIVED)
    monkeypatch.setattr(ds, "MAX_STATE_ROWS", 1)
    second = _state(generation=2, previous=first["state_sha256"])
    second["targets"][0]["position_observed_at"] += 1.0
    with pytest.raises(ds.DesiredStateError, match="historiekgrens"):
        store.apply(second, received_at=RECEIVED + 1.0)
    assert store.current()["state_sha256"] == first["state_sha256"]

    monkeypatch.setattr(ds, "MAX_AUTHORITY_CLAIMS", 1)
    with pytest.raises(ds.DesiredStateError, match="claimgrens"):
        store.claim_producer(
            "ha-boot-b", "claim-b", claimed_at=RECEIVED + 2.0
        )
    assert store.current()["producer_id"] == "ha-boot-a"


def test_full_integrity_and_capacity_audit_is_explicit(tmp_path) -> None:
    store = ds.DesiredStateStore(tmp_path / "desired.sqlite3")
    _claim(store)
    store.apply(_state(), received_at=RECEIVED)
    audit = store.audit()
    assert audit["status"] == "ok"
    assert audit["state_rows"] == 1
    assert audit["authority_claim_rows"] == 1
    assert 0 < audit["database_bytes"] <= audit["database_limit_bytes"]


def test_store_instance_identity_is_stable_and_same_inode_replacement_blocks(
    tmp_path,
) -> None:
    path = tmp_path / "desired.sqlite3"
    store = ds.DesiredStateStore(path)
    assert store.load_active() is None
    instance = store.store_instance_sha256
    identity = store.identity_sha256
    reopened = ds.DesiredStateStore(path)
    assert reopened.store_instance_sha256 == instance
    assert reopened.identity_sha256 == identity

    replacement_path = tmp_path / "replacement.sqlite3"
    replacement = ds.DesiredStateStore(replacement_path)
    assert replacement.load_active() is None
    replacement_instance = replacement.store_instance_sha256
    replacement_identity = replacement.identity_sha256
    assert replacement_instance != instance
    assert replacement_identity != identity

    original_inode = path.stat().st_ino
    replacement_bytes = replacement_path.read_bytes()
    with path.open("r+b") as target:
        target.seek(0)
        target.write(replacement_bytes)
        target.truncate()
        target.flush()
        os.fsync(target.fileno())
    assert path.stat().st_ino == original_inode

    with pytest.raises(ds.DesiredStateError, match="store-instance"):
        store.current()
    adopted = ds.DesiredStateStore(path)
    assert adopted.store_instance_sha256 == replacement_instance
    assert adopted.identity_sha256 != identity


def test_store_instance_manifest_is_immutable_and_missing_trigger_fails_closed(
    tmp_path,
) -> None:
    path = tmp_path / "desired.sqlite3"
    store = ds.DesiredStateStore(path)
    assert store.load_active() is None
    for statement in (
        "UPDATE schema_meta SET store_instance_sha256='" + "0" * 64
        + "' WHERE singleton=1",
        "DELETE FROM schema_meta WHERE singleton=1",
    ):
        with closing(sqlite3.connect(path)) as database:
            with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                database.execute(statement)

    with closing(sqlite3.connect(path)) as database, database:
        database.execute("DROP TRIGGER schema_meta_no_update")
    with pytest.raises(ds.DesiredStateError, match="trigger ontbreekt"):
        ds.DesiredStateStore(path).current()


def test_schema_v2_migrates_once_and_preserves_active_state(tmp_path) -> None:
    path = tmp_path / "desired.sqlite3"
    store = ds.DesiredStateStore(path)
    _claim(store)
    activated = store.apply(_state(), received_at=RECEIVED)
    with closing(sqlite3.connect(path)) as database, database:
        database.execute("DROP TRIGGER schema_meta_no_update")
        database.execute("DROP TRIGGER schema_meta_no_delete")
        database.execute(
            "UPDATE schema_meta SET schema_version=? WHERE singleton=1",
            (ds.LEGACY_SCHEMA_VERSION,),
        )
        database.execute(
            "ALTER TABLE schema_meta DROP COLUMN store_instance_sha256"
        )

    migrated = ds.DesiredStateStore(path)
    assert migrated.current()["state_sha256"] == activated["state_sha256"]
    assert len(migrated.store_instance_sha256) == 64
    with closing(sqlite3.connect(path)) as database:
        row = database.execute(
            "SELECT schema_version,store_instance_sha256 FROM schema_meta "
            "WHERE singleton=1"
        ).fetchone()
        trigger_names = {
            item[0] for item in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert row[0] == ds.SCHEMA_VERSION
    assert row[1] == migrated.store_instance_sha256
    assert trigger_names == {"schema_meta_no_update", "schema_meta_no_delete"}


def test_store_closes_every_owned_connection_deterministically(tmp_path, monkeypatch):
    real_connect = sqlite3.connect
    opened = []

    class TrackingConnection(sqlite3.Connection):
        was_closed = False

        def close(self):
            self.was_closed = True
            super().close()

    def tracked_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs, factory=TrackingConnection)
        opened.append(connection)
        return connection

    monkeypatch.setattr(ds.sqlite3, "connect", tracked_connect)
    store = ds.DesiredStateStore(tmp_path / "owned-connections.sqlite3")
    _claim(store)
    store.apply(_state(), received_at=RECEIVED)
    assert store.load_active() is not None
    assert store.current()["status"] == "configured"
    assert store.audit()["status"] == "ok"

    assert len(opened) == 5
    assert all(connection.was_closed for connection in opened)


def test_schema_migration_write_lock_is_not_repeated_on_read_hot_path(
    tmp_path, monkeypatch,
) -> None:
    original = ds.DesiredStateStore._ensure_schema.__func__
    calls = 0

    def counted(cls, database):
        nonlocal calls
        calls += 1
        return original(cls, database)

    monkeypatch.setattr(
        ds.DesiredStateStore,
        "_ensure_schema",
        classmethod(counted),
    )
    store = ds.DesiredStateStore(tmp_path / "bounded-read-hot-path.sqlite3")
    assert store.current()["status"] == "unconfigured"
    assert store.current()["status"] == "unconfigured"
    assert store.load_active() is None
    assert calls == 1


def test_connection_setup_failure_closes_partial_connection(tmp_path, monkeypatch):
    class FailingConnection:
        row_factory = None
        closed = False

        def execute(self, _statement):
            raise RuntimeError("synthetic PRAGMA failure")

        def close(self):
            self.closed = True

    connection = FailingConnection()
    monkeypatch.setattr(ds.sqlite3, "connect", lambda *_args, **_kwargs: connection)

    with pytest.raises(RuntimeError, match="synthetic PRAGMA failure"):
        ds.DesiredStateStore(tmp_path / "partial.sqlite3")._connect()

    assert connection.closed is True


@pytest.mark.parametrize(
    "fault_point",
    ("apply.after_state", "apply.after_head", "apply.before_commit"),
)
def test_apply_faults_rollback_state_and_head(tmp_path, fault_point) -> None:
    path = tmp_path / f"desired-{fault_point}.sqlite3"
    normal = ds.DesiredStateStore(path)
    _claim(normal)

    def inject(point: str) -> None:
        if point == fault_point:
            raise RuntimeError("injected")

    with pytest.raises(RuntimeError, match="injected"):
        ds.DesiredStateStore(path, fault_injector=inject).apply(
            _state(), received_at=RECEIVED
        )
    assert ds.DesiredStateStore(path).current()["status"] == "unconfigured"


@pytest.mark.parametrize(
    "fault_point",
    ("claim.after_receipt", "claim.after_head", "claim.before_commit"),
)
def test_claim_faults_rollback_receipt_and_fencing_head(tmp_path, fault_point) -> None:
    path = tmp_path / f"desired-{fault_point}.sqlite3"

    def inject(point: str) -> None:
        if point == fault_point:
            raise RuntimeError("injected")

    with pytest.raises(RuntimeError, match="injected"):
        ds.DesiredStateStore(path, fault_injector=inject).claim_producer(
            "ha-boot-a", "claim-a", claimed_at=RECEIVED
        )
    assert ds.DesiredStateStore(path).current()["status"] == "unconfigured"


def test_desired_updates_share_meteorological_fifo_tier_not_control_flood() -> None:
    from storm_tracker_worker import service

    assert "desired_state_claim" in service.JOB_PRIORITY_CONTROL
    assert "desired_state_snapshot" in service.JOB_PRIORITY_CONTROL
    assert "desired_state_apply" not in service.JOB_PRIORITY_CONTROL
    assert "desired_state_apply" in service.JOB_PRIORITY_METEOROLOGICAL


@pytest.mark.parametrize(
    ("checkpoint", "committed"),
    (
        ("apply.after_state", False),
        ("apply.after_head", False),
        ("apply.before_commit", False),
        ("apply.after_commit", True),
    ),
)
def test_real_process_exit_during_apply_is_atomic_and_retryable(
    tmp_path, checkpoint, committed
) -> None:
    path = tmp_path / f"crash-{checkpoint}.sqlite3"
    normal = ds.DesiredStateStore(path)
    _claim(normal)
    payload_path = tmp_path / f"payload-{checkpoint}.json"
    payload_path.write_text(
        json.dumps({
            "desired_state": _state(),
            "received_at": RECEIVED,
        }),
        encoding="utf-8",
    )
    helper = Path(__file__).resolve().parent / (
        "helpers/worker_desired_state_crash_child.py"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [
            sys.executable,
            str(helper),
            str(path),
            "apply",
            checkpoint,
            str(payload_path),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 73, result.stderr.decode(errors="replace")
    reopened = ds.DesiredStateStore(path)
    assert (reopened.current()["status"] == "configured") is committed
    retry = reopened.apply(_state(), received_at=RECEIVED + 1.0)
    assert retry["status"] == ("idempotent" if committed else "activated")
    assert reopened.current()["state_sha256"] == retry["state_sha256"]


@pytest.mark.parametrize(
    ("checkpoint", "committed"),
    (
        ("claim.after_receipt", False),
        ("claim.after_head", False),
        ("claim.before_commit", False),
        ("claim.after_commit", True),
    ),
)
def test_real_process_exit_during_claim_is_atomic_and_retryable(
    tmp_path, checkpoint, committed
) -> None:
    path = tmp_path / f"claim-crash-{checkpoint}.sqlite3"
    payload_path = tmp_path / f"claim-payload-{checkpoint}.json"
    payload_path.write_text(
        json.dumps({
            "producer_id": "ha-boot-a",
            "claim_id": "claim-crash",
            "claimed_at": RECEIVED,
        }),
        encoding="utf-8",
    )
    helper = Path(__file__).resolve().parent / (
        "helpers/worker_desired_state_crash_child.py"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [
            sys.executable,
            str(helper),
            str(path),
            "claim",
            checkpoint,
            str(payload_path),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 73, result.stderr.decode(errors="replace")
    reopened = ds.DesiredStateStore(path)
    snapshot = reopened.current()
    assert (snapshot.get("authority_epoch") == 1) is committed
    retry = reopened.claim_producer(
        "ha-boot-a", "claim-crash", claimed_at=RECEIVED + 1.0
    )
    assert retry["status"] == ("claim_idempotent" if committed else "claimed")
    assert retry["authority_epoch"] == 1
