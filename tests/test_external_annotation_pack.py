"""Fail-closed tests for the external annotation-pack boundary."""
from __future__ import annotations

from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from storm_tracker_worker import external_annotation_pack as annotation_pack
from storm_tracker_worker.external_annotation_pack import (
    PACK_FORMAT_VERSION,
    SUBMISSION_FORMAT_VERSION,
    TIME_STANDARD,
    UNIT_POLICY,
    AnnotationEventSpec,
    AnnotationFrameSpec,
    AnnotationLabelRequest,
    AnnotationPackRequest,
    AnnotationProtocol,
    AnnotationTargetSpec,
    EvidenceKind,
    TargetKind,
    annotation_pack_from_dict,
    annotation_pack_request_from_dict,
    empty_label_template_document,
    export_annotation_pack,
    ingest_annotation_submission,
)
from storm_tracker_worker.tracking_core.engine.meteorological_evidence_store import (
    MeteorologicalEvidenceStore,
)
from storm_tracker_worker.validation_corpus import (
    EventSplitPolicy,
    ExternalLabelKind,
    ExternalSourceKind,
)


INSTRUCTIONS_SHA256 = "1" * 64
SOURCE_ARTIFACT_SHA256 = "2" * 64
FIELD_ARTIFACT_SHA256 = "3" * 64


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rehash_pack_document(document: dict) -> None:
    content = deepcopy(document)
    content.pop("annotation_pack_sha256")
    canonical = json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    document["annotation_pack_sha256"] = hashlib.sha256(
        canonical.encode("ascii")
    ).hexdigest()


def _database(tmp_path: Path) -> Path:
    path = tmp_path / "calibration-snapshot.sqlite3"
    with closing(sqlite3.connect(path)) as database, database:
        database.executescript(
            """
            CREATE TABLE original_radar_frames (
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_timestamp REAL NOT NULL,
                saved_at REAL NOT NULL,
                encoding TEXT NOT NULL,
                payload BLOB NOT NULL
            );
            CREATE TABLE frames (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                product_timestamp REAL NOT NULL,
                region_key TEXT,
                collected_at REAL,
                grid_deg REAL,
                observation_count INTEGER,
                grid_point_count INTEGER
            );
            CREATE TABLE ground_observations (
                id INTEGER PRIMARY KEY,
                provider TEXT NOT NULL,
                observed_at REAL NOT NULL,
                received_at REAL,
                site_id TEXT,
                precipitation_present INTEGER NOT NULL,
                quantity TEXT,
                unit TEXT,
                value_numeric REAL,
                capable_of_dry_detection INTEGER,
                quality_json TEXT
            );
            CREATE TABLE forecast_verification_samples (
                id INTEGER PRIMARY KEY,
                observed_at REAL NOT NULL,
                snapshot_json TEXT NOT NULL,
                target_id TEXT
            );
            """
        )
        database.execute(
            "INSERT INTO original_radar_frames VALUES (?, ?, ?, ?, ?, ?)",
            ("region-belgium", "kmi", 150.0, 160.0, "synthetic", b"radar"),
        )
        database.execute(
            "INSERT INTO frames VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "kmi", 150.0, "region-belgium", 160.0, 0.01, 8, 10),
        )
        database.execute(
            "INSERT INTO ground_observations VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                "kmi",
                151.0,
                161.0,
                "station-1",
                1,
                "precipitation_accumulation",
                "mm",
                0.4,
                1,
                '{"accepted":true}',
            ),
        )
        database.execute(
            "INSERT INTO forecast_verification_samples VALUES (?, ?, ?, ?)",
            (1, 152.0, '{"candidate":"hidden"}', "target-tracker"),
        )
    return path


def _request(*, provider_ids: tuple[str, ...] = ("kmi",)) -> AnnotationPackRequest:
    frames = (
        AnnotationFrameSpec(
            frame_id="frame-150",
            valid_time_utc=150.0,
            grid_id="grid-belgium-1km",
            variable_id="precipitation_rate",
            unit_id="mm_h",
        ),
    )
    targets = (
        AnnotationTargetSpec(
            "target-cell",
            TargetKind.CELL,
            "candidate-cell-1",
            None,
            None,
        ),
        AnnotationTargetSpec(
            "target-grid",
            TargetKind.GRID,
            "grid-belgium-1km",
            None,
            None,
        ),
        AnnotationTargetSpec(
            "target-site",
            TargetKind.SITE,
            "station-1",
            50.85,
            4.35,
        ),
        AnnotationTargetSpec(
            "target-tracker",
            TargetKind.TRACKER,
            "tracker-home",
            50.85,
            4.35,
        ),
    )
    requests = tuple(
        sorted(
            (
                AnnotationLabelRequest(
                    "request-cell-identity",
                    ExternalLabelKind.CELL_IDENTITY,
                    "frame-150",
                    "target-cell",
                ),
                AnnotationLabelRequest(
                    "request-cell-position",
                    ExternalLabelKind.CELL_POSITION,
                    "frame-150",
                    "target-cell",
                ),
                AnnotationLabelRequest(
                    "request-cell-speed",
                    ExternalLabelKind.CELL_SPEED,
                    "frame-150",
                    "target-cell",
                ),
                AnnotationLabelRequest(
                    "request-passage",
                    ExternalLabelKind.PASSAGE,
                    "frame-150",
                    "target-tracker",
                ),
                AnnotationLabelRequest(
                    "request-precipitation-field",
                    ExternalLabelKind.PRECIPITATION_FIELD,
                    "frame-150",
                    "target-grid",
                ),
                AnnotationLabelRequest(
                    "request-wet-dry",
                    ExternalLabelKind.WET_DRY,
                    "frame-150",
                    "target-site",
                ),
            ),
            key=lambda item: item.request_id,
        )
    )
    event = AnnotationEventSpec(
        event_id="event-belgium-001",
        window_start_utc=100.0,
        window_end_utc=200.0,
        leakage_group_ids=("physical-storm-family-001",),
        provider_ids=provider_ids,
        evidence_kinds=tuple(sorted(EvidenceKind, key=lambda item: item.value)),
        frames=frames,
        targets=targets,
        label_requests=requests,
    )
    return AnnotationPackRequest(
        request_id="annotation-request-001",
        created_at_utc=300.0,
        source_database_id="calibration-snapshot-001",
        time_standard=TIME_STANDARD,
        protocol=AnnotationProtocol(
            protocol_id="storm-cell-annotation",
            protocol_version="1.0.0",
            instructions_sha256=INSTRUCTIONS_SHA256,
            required_blinding=True,
            annotator_role_ids=("meteorologist",),
            allowed_source_kinds=(
                ExternalSourceKind.BLINDED_HUMAN_CONSENSUS,
            ),
        ),
        split_policy=EventSplitPolicy(
            policy_id="heldout-events-v1",
            hash_salt="preregistered-test-salt",
            train_basis_points=6000,
            validation_basis_points=2000,
            test_basis_points=2000,
        ),
        events=(event,),
    )


def _vnext_database_and_request(
    tmp_path: Path,
) -> tuple[Path, AnnotationPackRequest]:
    path = tmp_path / "vnext-evidence.sqlite3"
    store = MeteorologicalEvidenceStore(path)
    store.initialize()
    receipt = store.append_raw_receipt(
        provider_id="kmi",
        upstream_record_id="kmi/1500",
        payload=b"exact-upstream-radar-bytes",
        observed_start=1500.0,
        observed_end=1500.0,
        received_at=1505.0,
        media_type="application/octet-stream",
        parser_version="kmi-test-parser-v1",
        time_quality={"state": "current"},
    )
    native = store.append_native_frame(
        receipt_id=receipt,
        source_frame_id="kmi-native-1500",
        observed_start=1500.0,
        observed_end=1500.0,
        received_at=1505.0,
        quantity="precipitation_rate",
        unit="mm/h",
        native_projection="EPSG:31370",
        width=2,
        height=2,
        native_resolution_x=1000.0,
        native_resolution_y=1000.0,
        encoding="float32-le",
        frame_payload=b"\x00\x00\x80?" * 4,
        parser_version="kmi-test-parser-v1",
        calibration_id="kmi-test-calibration-v1",
        time_quality={"state": "current"},
    )
    store.append_analysis_frame(
        native_frame_id=native,
        analysis_grid_id="grid-belgium-1km",
        observed_start=1500.0,
        observed_end=1500.0,
        quantity="precipitation_rate",
        unit="mm/h",
        analysis_projection="EPSG:31370",
        width=2,
        height=2,
        resolution_x=1000.0,
        resolution_y=1000.0,
        encoding="float32-le",
        frame_payload=b"\x00\x00\x80?" * 4,
        transform_version="identity-test-v1",
        calibration_id="kmi-test-calibration-v1",
    )
    site = store.append_physical_site(
        site_key="station-1",
        latitude=50.85,
        longitude=4.35,
        elevation_m=20.0,
        independence_group="station-1",
        site_metadata={"network": "synthetic-kmi"},
    )
    alias = store.append_site_alias(
        physical_site_id=site,
        source_id="kmi",
        external_station_id="station-1",
        valid_start=1000.0,
    )
    store.append_ground_observation(
        receipt_id=receipt,
        site_alias_id=alias,
        source_observation_id="station-1/1500",
        observed_start=1499.0,
        observed_end=1501.0,
        received_at=1505.0,
        quantity="precipitation_accumulation",
        unit="mm",
        value_numeric=0.4,
        categorical={"raining": True},
        parser_version="kmi-ground-test-v1",
        calibration_id="kmi-ground-test-v1",
        quality={"usable": True},
        time_quality={"state": "current"},
    )
    base = _request()
    event = base.events[0]
    frame = replace(event.frames[0], valid_time_utc=1500.0)
    vnext_event = replace(
        event,
        window_start_utc=1400.0,
        window_end_utc=1600.0,
        frames=(frame,),
    )
    return path, replace(
        base,
        created_at_utc=1700.0,
        source_database_id="vnext-evidence-snapshot-001",
        events=(vnext_event,),
    )


def _completed_submission(pack) -> dict:
    document = empty_label_template_document(pack)
    document["submission_id"] = "submission-001"
    document["submitted_at_utc"] = 500.0
    document["sources"] = [
        {
            "source_id": "human-consensus-001",
            "source_kind": "blinded_human_consensus",
            "producer": "independent-meteorologists",
            "dataset_version": "2026-07-30",
            "methodology_id": "storm-cell-annotation-v1",
            "source_artifact_sha256": SOURCE_ARTIFACT_SHA256,
            "candidate_independent": True,
            "blinded_to_candidate": True,
            "annotator_ids": ["annotator-001"],
            "protocol_id": "storm-cell-annotation",
            "protocol_version": "1.0.0",
            "instructions_sha256": INSTRUCTIONS_SHA256,
            "time_standard": TIME_STANDARD,
            "unit_policy": UNIT_POLICY,
        }
    ]
    payloads = {
        "cell_identity": {
            "reference_cell_id": "reference-cell-1",
            "parent_reference_cell_ids": [],
        },
        "cell_position": {
            "reference_cell_id": "reference-cell-1",
            "latitude": 50.9,
            "longitude": 4.4,
            "horizontal_uncertainty_m": 750.0,
        },
        "cell_speed": {
            "reference_cell_id": "reference-cell-1",
            "eastward_mps": 4.0,
            "northward_mps": 1.0,
            "interval_start_utc": 100.0,
            "interval_end_utc": 200.0,
            "vector_uncertainty_mps": 0.5,
        },
        "passage": {
            "tracker_id": "tracker-home",
            "window_start_utc": 100.0,
            "window_end_utc": 200.0,
            "occurred": True,
            "passage_time_utc": 155.0,
            "time_uncertainty_s": 30.0,
        },
        "wet_dry": {
            "site_id": "station-1",
            "interval_start_utc": 100.0,
            "interval_end_utc": 200.0,
            "state": "wet",
            "capable_of_dry_detection": True,
            "accumulation_mm": 0.4,
        },
        "precipitation_field": {
            "grid_id": "grid-belgium-1km",
            "valid_time_utc": 150.0,
            "variable_id": "precipitation_rate",
            "unit_id": "mm_h",
            "field_artifact_sha256": FIELD_ARTIFACT_SHA256,
        },
    }
    for item in document["annotations"]:
        item["source_id"] = "human-consensus-001"
        item["annotator_id"] = "annotator-001"
        item["annotator_role_id"] = "meteorologist"
        item["annotation_started_at_utc"] = 400.0
        item["annotation_completed_at_utc"] = 450.0
        item["blinded_to_candidate"] = True
        item["label_state"] = "confirmed"
        item["payload"] = payloads[item["label_kind"]]
        item["evidence_reference_ids_used"] = list(
            item["allowed_evidence_reference_ids"][:1]
        )
    return document


def test_export_is_read_only_deterministic_and_records_all_scopes(tmp_path):
    database = _database(tmp_path)
    before = _sha256_file(database)
    request = _request()

    first = export_annotation_pack(database, request)
    second = export_annotation_pack(database, request)

    assert first == second
    assert first.sha256 == second.sha256
    assert _sha256_file(database) == before
    assert first.format_version == PACK_FORMAT_VERSION
    assert first.source_database.database_sha256 == before
    assert first.source_database.connection_mode == (
        "sqlite-uri-mode-ro-immutable-query-only"
    )
    assert len(first.evidence_references) == 4
    assert first.missing_evidence == ()
    assert len(first.templates) == 6
    assert {item.evidence_kind for item in first.evidence_references} == set(
        EvidenceKind
    )
    engine = next(
        item
        for item in first.evidence_references
        if item.evidence_kind is EvidenceKind.ENGINE_CONTEXT
    )
    assert engine.truth_eligibility == "engine_context_not_truth"
    assert engine.annotator_visibility == "prohibited"
    ground = next(
        item
        for item in first.evidence_references
        if item.evidence_kind is EvidenceKind.GROUND_FACT
    )
    assert ground.provider_id == "kmi"
    assert ground.target_id == "target-site"
    assert ground.quantity == "precipitation_accumulation"
    assert ground.unit_id == "mm"
    assert ground.observed_start_utc == 151.0
    visible_ids = {
        item.reference_id
        for item in first.evidence_references
        if item.annotator_visibility == "allowed"
    }
    assert all(
        set(template.allowed_evidence_reference_ids) == visible_ids
        for template in first.templates
    )
    assert engine.reference_id not in visible_ids
    frame = first.request.events[0].frames[0]
    assert (frame.grid_id, frame.variable_id, frame.unit_id) == (
        "grid-belgium-1km",
        "precipitation_rate",
        "mm_h",
    )


def test_export_reads_vnext_receipt_frame_ground_and_analysis_chains(tmp_path):
    database, request = _vnext_database_and_request(tmp_path)

    pack = export_annotation_pack(database, request)

    assert len(pack.evidence_references) == 4
    assert pack.missing_evidence == ()
    by_kind = {
        item.evidence_kind: item for item in pack.evidence_references
    }
    raw = by_kind[EvidenceKind.UPSTREAM_ARTIFACT]
    assert raw.source_table == "meteorological_raw_receipts"
    assert raw.provider_id == "kmi"
    assert raw.artifact_sha256 is not None
    assert raw.metadata["capture_fidelity"] == (
        "exact_content_addressed_upstream_blob"
    )
    native = by_kind[EvidenceKind.RADAR_FRAME_METADATA]
    assert native.source_table == "meteorological_native_frames"
    assert native.quantity == "precipitation_rate"
    assert native.unit_id == "mm/h"
    assert native.metadata["native_projection"] == "EPSG:31370"
    ground = by_kind[EvidenceKind.GROUND_FACT]
    assert ground.source_table == "meteorological_ground_observations"
    assert ground.target_id == "target-site"
    assert ground.metadata["physical_site_id"]
    assert ground.metadata["automatic_truth_promotion"] is False
    analysis = by_kind[EvidenceKind.ENGINE_CONTEXT]
    assert analysis.source_table == "meteorological_analysis_frames"
    assert analysis.grid_id == "grid-belgium-1km"
    assert analysis.annotator_visibility == "prohibited"


def test_export_emits_only_blank_truth_fields_and_no_margins(tmp_path):
    pack = export_annotation_pack(_database(tmp_path), _request())
    document = empty_label_template_document(pack)

    assert document["sources"] == []
    assert document["submission_id"] is None
    assert document["submitted_at_utc"] is None
    assert all(item["source_id"] is None for item in document["annotations"])
    assert all(
        item["evidence_reference_ids_used"] == []
        for item in document["annotations"]
    )
    rendered = json.dumps(pack.to_dict(), sort_keys=True)
    assert '"labels_created_by_export": false' in rendered
    assert '"margins_selected_by_export": false' in rendered
    assert "practical_margin" not in rendered
    assert "release_threshold" not in rendered


def test_request_parser_is_strict_and_has_no_policy_defaults():
    request_document = _request().to_dict()
    assert annotation_pack_request_from_dict(request_document) == _request()

    request_document["split_policy"].pop("test_basis_points")
    with pytest.raises(ValueError, match="velden komen niet exact overeen"):
        annotation_pack_request_from_dict(request_document)


def test_missing_evidence_is_explicit_per_provider_and_target(tmp_path):
    pack = export_annotation_pack(
        _database(tmp_path),
        _request(provider_ids=("kmi", "metar")),
    )

    metar_missing = [
        item for item in pack.missing_evidence if item.provider_id == "metar"
    ]
    assert {item.evidence_kind for item in metar_missing} == {
        EvidenceKind.UPSTREAM_ARTIFACT,
        EvidenceKind.RADAR_FRAME_METADATA,
        EvidenceKind.GROUND_FACT,
    }
    ground = next(
        item
        for item in metar_missing
        if item.evidence_kind is EvidenceKind.GROUND_FACT
    )
    assert ground.target_id == "target-site"
    assert ground.reason_code == "no_exact_target_ground_reference"
    assert ground.window_start_utc == 100.0
    assert ground.window_end_utc == 200.0


@pytest.mark.parametrize("suffix", ("-wal", "-journal"))
def test_export_refuses_non_snapshot_sqlite_sidecars(tmp_path, suffix):
    database = _database(tmp_path)
    database.with_name(database.name + suffix).write_bytes(b"active")

    with pytest.raises(RuntimeError, match="consistente read-only snapshot"):
        export_annotation_pack(database, _request())


def test_export_detects_source_hash_change_before_return(tmp_path, monkeypatch):
    database = _database(tmp_path)
    real_hash = annotation_pack._sha256_file
    calls = 0

    def changing_hash(path):
        nonlocal calls
        calls += 1
        digest = real_hash(path)
        return digest if calls == 1 else "f" * 64

    monkeypatch.setattr(annotation_pack, "_sha256_file", changing_hash)
    with pytest.raises(RuntimeError, match="wijzigde tijdens"):
        export_annotation_pack(database, _request())


def test_pack_roundtrip_verifies_canonical_hash_and_derived_contract(tmp_path):
    pack = export_annotation_pack(_database(tmp_path), _request())
    document = pack.to_dict()

    assert annotation_pack_from_dict(document) == pack

    changed = deepcopy(document)
    changed["request"]["events"][0]["frames"][0]["valid_time_utc"] = 151.0
    with pytest.raises(ValueError, match="SHA-256|niet exact.*afgeleid"):
        annotation_pack_from_dict(changed)

    changed = deepcopy(document)
    changed["split"]["event_partitions"][0][1] = (
        "test"
        if changed["split"]["event_partitions"][0][1] != "test"
        else "train"
    )
    changed["split"]["frame_partitions"][0][1] = (
        changed["split"]["event_partitions"][0][1]
    )
    _rehash_pack_document(changed)
    with pytest.raises(ValueError, match="niet exact.*afgeleid"):
        annotation_pack_from_dict(changed)

    changed = deepcopy(document)
    changed["templates"][0]["allowed_evidence_reference_ids"] = []
    _rehash_pack_document(changed)
    with pytest.raises(ValueError, match="templates zijn niet exact"):
        annotation_pack_from_dict(changed)


def test_successful_ingest_types_every_label_and_preserves_provenance(tmp_path):
    pack = export_annotation_pack(_database(tmp_path), _request())
    submission = _completed_submission(pack)

    result = ingest_annotation_submission(pack, submission)

    assert result.annotation_pack_sha256 == pack.sha256
    assert len(result.corpus.labels) == 6
    assert result.unresolved_template_ids == ()
    assert {label.label_kind for label in result.corpus.labels} == set(
        ExternalLabelKind
    )
    assert all(
        item.annotator_role_id == "meteorologist"
        for item in result.provenance
    )
    assert all(
        item.evidence_reference_ids_used
        for item in result.provenance
    )
    assert result.truth_context.split == pack.split
    assert result.to_dict()["scientific_contract"] == {
        "metrics_computed": False,
        "margins_selected": False,
        "engine_derived_labels_accepted": False,
    }
    assert result.to_dict()["annotation_ingest_sha256"] == result.sha256
    changed_provenance = replace(
        result.provenance[0],
        annotator_role_id="independent-reviewer",
    )
    changed_result = replace(
        result,
        provenance=(changed_provenance, *result.provenance[1:]),
    )
    assert changed_result.sha256 != result.sha256


def test_unresolved_template_never_creates_truth_label(tmp_path):
    pack = export_annotation_pack(_database(tmp_path), _request())
    submission = _completed_submission(pack)
    unresolved = submission["annotations"][0]
    unresolved["label_state"] = "unknown"
    unresolved["payload"] = next(
        template.empty_payload
        for template in pack.templates
        if template.template_id == unresolved["template_id"]
    )
    unresolved["evidence_reference_ids_used"] = []
    unresolved["missing_annotation_reason"] = "insufficient_external_evidence"

    result = ingest_annotation_submission(pack, submission)

    assert len(result.corpus.labels) == 5
    assert result.unresolved_template_ids == (unresolved["template_id"],)
    provenance = next(
        item
        for item in result.provenance
        if item.template_id == unresolved["template_id"]
    )
    assert provenance.label_id is None
    assert provenance.missing_annotation_reason == (
        "insufficient_external_evidence"
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda submission, pack: submission["sources"][0].update(
                {"candidate_independent": False}
            ),
            "niet eligible",
        ),
        (
            lambda submission, pack: submission["sources"][0].update(
                {"blinded_to_candidate": False}
            ),
            "niet eligible|geblindeerde source",
        ),
        (
            lambda submission, pack: submission["sources"][0].update(
                {"protocol_version": "wrong"}
            ),
            "protocol_version",
        ),
        (
            lambda submission, pack: submission["sources"][0].update(
                {"instructions_sha256": "4" * 64}
            ),
            "instructions_sha256",
        ),
        (
            lambda submission, pack: submission["sources"][0].update(
                {
                    "source_artifact_sha256": (
                        pack.source_database.database_sha256
                    )
                }
            ),
            "engine/DB-artifact",
        ),
        (
            lambda submission, pack: submission["annotations"][0].update(
                {"annotation_started_at_utc": 299.0}
            ),
            "annotatie-/submissiontijd",
        ),
        (
            lambda submission, pack: submission["annotations"][0].update(
                {"evidence_reference_ids_used": ["unknown-reference"]}
            ),
            "buiten de toegelaten",
        ),
        (
            lambda submission, pack: submission["annotations"][0].update(
                {"allowed_evidence_reference_ids": []}
            ),
            "vaste template-identiteit",
        ),
    ),
)
def test_ingest_rejects_provenance_blinding_time_and_reference_tampering(
    tmp_path,
    mutation,
    message,
):
    pack = export_annotation_pack(_database(tmp_path), _request())
    submission = _completed_submission(pack)
    mutation(submission, pack)

    with pytest.raises((TypeError, ValueError), match=message):
        ingest_annotation_submission(pack, submission)


def test_ingest_rejects_fixed_units_duplicates_and_partial_submissions(tmp_path):
    pack = export_annotation_pack(_database(tmp_path), _request())
    submission = _completed_submission(pack)
    field = next(
        item
        for item in submission["annotations"]
        if item["label_kind"] == "precipitation_field"
    )
    field["payload"]["unit_id"] = "dBZ"
    with pytest.raises(ValueError, match="vaste tijd/unit"):
        ingest_annotation_submission(pack, submission)

    submission = _completed_submission(pack)
    submission["annotations"][1] = deepcopy(submission["annotations"][0])
    with pytest.raises(ValueError, match="dubbel|exact eenmaal"):
        ingest_annotation_submission(pack, submission)

    submission = _completed_submission(pack)
    submission["annotations"].pop()
    with pytest.raises(ValueError, match="exact eenmaal"):
        ingest_annotation_submission(pack, submission)


def test_cli_exports_and_ingests_without_overwriting(tmp_path):
    database = _database(tmp_path)
    request_path = tmp_path / "request.json"
    pack_path = tmp_path / "pack.json"
    template_path = tmp_path / "template.json"
    submission_path = tmp_path / "submission.json"
    ingest_path = tmp_path / "ingest.json"
    request_path.write_text(
        json.dumps(_request().to_dict()),
        encoding="utf-8",
    )
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "external_annotation_pack.py"
    )

    exported = subprocess.run(
        [
            sys.executable,
            str(script),
            "export",
            "--database",
            str(database),
            "--request",
            str(request_path),
            "--pack-out",
            str(pack_path),
            "--template-out",
            str(template_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    summary = json.loads(exported.stdout)
    loaded_pack = annotation_pack_from_dict(
        json.loads(pack_path.read_text(encoding="utf-8"))
    )
    assert summary["annotation_pack_sha256"] == loaded_pack.sha256
    assert summary["label_count"] == 0

    original_pack_bytes = pack_path.read_bytes()
    rerun = subprocess.run(
        [
            sys.executable,
            str(script),
            "export",
            "--database",
            str(database),
            "--request",
            str(request_path),
            "--pack-out",
            str(pack_path),
            "--template-out",
            str(template_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rerun.returncode != 0
    assert pack_path.read_bytes() == original_pack_bytes

    submission_path.write_text(
        json.dumps(_completed_submission(loaded_pack)),
        encoding="utf-8",
    )
    ingested = subprocess.run(
        [
            sys.executable,
            str(script),
            "ingest",
            "--pack",
            str(pack_path),
            "--submission",
            str(submission_path),
            "--output",
            str(ingest_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    ingest_summary = json.loads(ingested.stdout)
    assert ingest_summary["labels"] == 6
    assert ingest_summary["unresolved"] == 0
    result_document = json.loads(ingest_path.read_text(encoding="utf-8"))
    assert result_document["annotation_pack_sha256"] == loaded_pack.sha256
    assert result_document["corpus_sha256"] == ingest_summary["corpus_sha256"]
    assert (
        result_document["annotation_ingest_sha256"]
        == ingest_summary["annotation_ingest_sha256"]
    )
