from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from storm_tracker_worker import opera_native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


PRODUCT_TIMESTAMP = 1_800_000_000.0
REPOSITORY = Path(__file__).resolve().parents[1]
HARD_EXIT_HELPER = (
    REPOSITORY / "tests" / "helpers" / "opera_transport_spool_os_exit73_child.py"
)
RECEIPT_HARD_EXIT_HELPER = (
    REPOSITORY / "tests" / "helpers" / "opera_raw_receipt_os_exit73_child.py"
)
ACQUISITION_RAW_IDENTITY = "acquisition:" + "a" * 64


def _child_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (str(REPOSITORY), environment.get("PYTHONPATH"))
        if value
    )
    return environment


def test_raw_only_drop_in_hook_and_rights_authority_are_explicit():
    assert opera_native.OPERA_RAW_ONLY_HANDLER_PATH == (
        "storm_tracker_worker.opera_native:capture_opera_dbzh_hdf5_raw_only"
    )
    assert opera_native.OPERA_RAW_ONLY_REQUIRED_ROLES == ("hdf5",)
    assert opera_native.OPERA_RAW_RETENTION_AUTHORITY_ID == (
        "worker_authority.opera_raw_retention_v1"
    )


@dataclass(frozen=True)
class _Job:
    job_id: str
    payload: dict[str, object]


def _job() -> _Job:
    return _Job(
        job_id=opera_native._canonical_job_id(PRODUCT_TIMESTAMP),
        payload={"product_timestamp": PRODUCT_TIMESTAMP},
    )


def _revision(payload: bytes) -> opera_native.OperaRevisionIdentity:
    return opera_native.OperaRevisionIdentity(
        etag='"0123456789abcdef0123456789abcdef"',
        version_id=None,
        content_length=len(payload),
        content_type="application/x-hdf5",
    )


def _install_transport(monkeypatch, tmp_path: Path, payload: bytes):
    revision = _revision(payload)
    calls = {"head": 0, "download": 0}

    def head(_object_key: str, *, deadline: float):
        assert deadline > 0
        calls["head"] += 1
        return revision

    def download(
        _object_key: str,
        observed: opera_native.OperaRevisionIdentity,
        *,
        deadline: float,
        destination_path: str | Path,
    ) -> opera_native.OperaDownloadedFile:
        assert observed == revision
        assert deadline > 0
        calls["download"] += 1
        path = Path(destination_path)
        assert path.parent.name.startswith(
            opera_native.OPERA_TRANSPORT_SPOOL_PREFIX
        )
        path.write_bytes(payload)
        return opera_native.OperaDownloadedFile(
            path=str(path),
            size_bytes=len(payload),
            content_sha256=hashlib.sha256(payload).hexdigest(),
            content_type=revision.content_type,
            received_at=PRODUCT_TIMESTAMP + 1.0,
            etag=revision.etag,
            version_id=revision.version_id,
        )

    monkeypatch.setattr(opera_native, "_head_hdf5", head)
    monkeypatch.setattr(opera_native, "_download_hdf5", download)
    return revision, calls


def test_malformed_hdf5_is_a_complete_exact_raw_capture_without_inspection(
    monkeypatch,
    tmp_path,
):
    payload = b"not-an-hdf5-container\x00but-exact-upstream-bytes"
    revision, calls = _install_transport(monkeypatch, tmp_path, payload)
    monkeypatch.setattr(
        opera_native,
        "_inspect_opera_hdf5",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("raw-only capture mag HDF5 niet inspecteren")
        ),
    )
    monkeypatch.setattr(
        opera_native,
        "_read_once_for_schema3",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("raw-only capture mag geen full-objectbuffer maken")
        ),
    )
    store = RawArtifactStore((tmp_path / "raw").resolve())

    result = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(),
        raw_store=store,
    )

    assert calls == {"head": 1, "download": 1}
    assert result["capture_state"] == "exact_raw_committed"
    assert result["interpretation"] == "unverified"
    assert result["validation_status"] == "not_interpreted"
    assert result["decode_status"] == "not_attempted"
    assert result["weather_state"] == "unknown"
    assert result["weather_claim_generated"] is False
    assert result["operational_admission_eligible"] is False
    assert result["rights_activation_prerequisite"] == (
        opera_native.OPERA_RAW_RETENTION_AUTHORITY_ID
    )
    raw = result["raw_provenance"]
    capture = store.load_capture(raw["raw_capture_id"])
    assert capture is not None
    assert capture.complete is True
    assert capture.product_timestamp is None
    source = raw["raw_sources"]["hdf5"]
    assert store.read_bytes(source["raw_receipt_id"]) == payload
    assert source["raw_content_sha256"] == hashlib.sha256(payload).hexdigest()
    assert source["upstream_revision_token"] == opera_native._revision_token(revision)
    assert source["interpretation"] == "unverified"
    assert source["hdf5_inspection_attempted"] is False


def test_raw_only_retry_is_fully_local_and_reuses_fixed_exact_capture(
    monkeypatch,
    tmp_path,
):
    payload = b"arbitrary-upstream-object"
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store_root = (tmp_path / "raw").resolve()
    first = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(),
        raw_store=RawArtifactStore(store_root),
    )
    second = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(),
        raw_store=RawArtifactStore(store_root),
    )

    assert calls == {"head": 1, "download": 1}
    assert first["raw_provenance"]["raw_capture_id"] == second["raw_provenance"][
        "raw_capture_id"
    ]
    source = second["raw_provenance"]["raw_sources"]["hdf5"]
    assert source["fetch_id"].endswith(":hdf5")
    assert ":revision-" not in source["fetch_id"]
    assert second["raw_provenance"]["raw_capture_fetch_id"].endswith(":capture")


def test_raw_only_result_is_payload_path_url_and_weather_claim_free(
    monkeypatch,
    tmp_path,
):
    payload = b"uninterpreted"
    _revision_value, _calls = _install_transport(monkeypatch, tmp_path, payload)
    result = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(),
        raw_store=RawArtifactStore((tmp_path / "raw").resolve()),
    )

    encoded = json.dumps(result, sort_keys=True).lower()
    assert str(tmp_path).lower().replace("\\", "\\\\") not in encoded
    assert "https://" not in encoded
    assert "payload" not in encoded
    assert "declared_quantity" not in result
    assert "declared_unit" not in result
    assert "rain" not in result
    assert "dry" not in result
    assert result["weather_claim_generated"] is False


def test_raw_only_path_calls_store_file_not_legacy_bytes_store(monkeypatch, tmp_path):
    payload = b"file-api-only"
    _revision_value, _calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore((tmp_path / "raw").resolve())
    real_store_file = store.store_file
    observed: list[dict[str, object]] = []

    def store_file(**kwargs):
        observed.append(dict(kwargs))
        return real_store_file(**kwargs)

    monkeypatch.setattr(store, "store_file", store_file)
    monkeypatch.setattr(
        store,
        "store",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("raw-only OPERA mag bytes-store niet gebruiken")
        ),
    )

    opera_native.capture_opera_dbzh_hdf5_raw_only(_job(), raw_store=store)

    assert len(observed) == 1
    assert observed[0]["expected_size"] == len(payload)
    assert observed[0]["expected_sha256"] == hashlib.sha256(payload).hexdigest()
    assert isinstance(observed[0]["source_path"], Path)


def test_receipt_only_retry_recovers_before_head_and_pins_original_revision(
    monkeypatch,
    tmp_path,
):
    payload = b"receipt-survives-lost-capture-return"
    revision, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore((tmp_path / "raw").resolve())
    real_commit = store.commit_capture

    monkeypatch.setattr(
        store,
        "commit_capture",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("lost return")),
    )
    with pytest.raises(RuntimeError, match="lost return"):
        opera_native.capture_opera_dbzh_hdf5_raw_only(_job(), raw_store=store)
    assert calls == {"head": 1, "download": 1}

    monkeypatch.setattr(store, "commit_capture", real_commit)
    monkeypatch.setattr(
        opera_native,
        "_head_hdf5",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("durable receipt recovery mag geen HEAD doen")
        ),
    )
    result = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(), raw_store=store
    )

    source = result["raw_provenance"]["raw_sources"]["hdf5"]
    assert source["upstream_revision_token"] == opera_native._revision_token(
        revision
    )
    assert calls == {"head": 1, "download": 1}


def test_capture_commit_lost_return_recovers_without_network(
    monkeypatch,
    tmp_path,
):
    payload = b"capture-survives-lost-return"
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore((tmp_path / "raw").resolve())
    real_commit = store.commit_capture

    def commit_then_lose(**kwargs):
        real_commit(**kwargs)
        raise RuntimeError("capture return lost")

    monkeypatch.setattr(store, "commit_capture", commit_then_lose)
    with pytest.raises(RuntimeError, match="capture return lost"):
        opera_native.capture_opera_dbzh_hdf5_raw_only(_job(), raw_store=store)
    monkeypatch.setattr(store, "commit_capture", real_commit)
    monkeypatch.setattr(
        opera_native,
        "_head_hdf5",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("durable capture recovery mag geen HEAD doen")
        ),
    )

    result = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(), raw_store=store
    )
    assert result["capture_state"] == "exact_raw_committed"
    assert calls == {"head": 1, "download": 1}


def test_stale_slot_spool_is_reconciled_before_fetch(monkeypatch, tmp_path):
    payload = b"fresh-after-hard-kill-residue"
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore((tmp_path / "raw").resolve())
    store.initialize()
    stale = opera_native._slot_spool_path(store, _job().job_id)
    stale.write_bytes(b"partial-crash-residue")

    result = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(), raw_store=store
    )

    assert result["capture_state"] == "exact_raw_committed"
    assert calls == {"head": 1, "download": 1}
    assert not stale.exists()


def test_native_os_exit73_spool_residue_is_reconciled_before_refetch(
    monkeypatch,
    tmp_path,
):
    store = RawArtifactStore((tmp_path / "raw").resolve())
    store.initialize()
    completed = subprocess.run(
        [
            sys.executable,
            str(HARD_EXIT_HELPER),
            str(store.root),
            _job().job_id,
        ],
        cwd=REPOSITORY,
        env=_child_environment(),
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)
    stale = opera_native._slot_spool_path(store, _job().job_id)
    assert stale.read_bytes() == b"partial-opera-hard-exit-residue"

    payload = b"fresh-after-native-hard-exit"
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    result = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(),
        raw_store=store,
    )

    assert result["capture_state"] == "exact_raw_committed"
    assert calls == {"head": 1, "download": 1}
    assert not stale.exists()


def test_native_os_exit73_after_receipt_commit_cleans_spool_without_refetch(
    monkeypatch,
    tmp_path,
):
    store = RawArtifactStore((tmp_path / "raw").resolve())
    store.initialize()
    completed = subprocess.run(
        [
            sys.executable,
            str(RECEIPT_HARD_EXIT_HELPER),
            str(store.root),
            ACQUISITION_RAW_IDENTITY,
        ],
        cwd=REPOSITORY,
        env=_child_environment(),
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)
    stale = opera_native._slot_spool_path(store, ACQUISITION_RAW_IDENTITY)
    assert stale.read_bytes() == b"opera-durable-receipt-before-hard-exit"
    receipt_id = RawArtifactStore._receipt_id(
        provider="opera",
        product=opera_native.OPERA_HDF5_PRODUCT,
        fetch_id=f"{ACQUISITION_RAW_IDENTITY}:hdf5",
    )
    assert store.load_receipt(receipt_id) is not None
    monkeypatch.setattr(
        opera_native,
        "_head_hdf5",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("durable OPERA receipt mag geen HEAD herhalen")
        ),
    )
    monkeypatch.setattr(
        opera_native,
        "_download_hdf5",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("durable OPERA receipt mag geen download herhalen")
        ),
    )

    result = opera_native.capture_opera_dbzh_hdf5_raw_only(
        _job(),
        raw_store=store,
        raw_identity=ACQUISITION_RAW_IDENTITY,
    )

    assert result["capture_state"] == "exact_raw_committed"
    assert result["raw_provenance"]["raw_capture_fetch_id"] == (
        f"{ACQUISITION_RAW_IDENTITY}:capture"
    )
    assert not stale.exists()


def test_spool_and_cas_free_space_preflight_fails_before_download(
    monkeypatch,
    tmp_path,
):
    payload = b"space-bound"
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    monkeypatch.setattr(
        opera_native.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=0),
    )

    with pytest.raises(OSError, match="onvoldoende vrije ruimte"):
        opera_native.capture_opera_dbzh_hdf5_raw_only(
            _job(),
            raw_store=RawArtifactStore((tmp_path / "raw").resolve()),
        )

    assert calls == {"head": 1, "download": 0}
