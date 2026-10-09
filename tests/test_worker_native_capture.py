from __future__ import annotations

from dataclasses import replace

import pytest

from storm_tracker_worker.native_capture import (
    MAX_CAPTURE_MEMBERS,
    NativeCaptureError,
    NativeCaptureMemberInput,
    capture_native_set,
)
from storm_tracker_worker.raw_artifacts import (
    RawArtifactConflictError,
    RawArtifactStore,
)


NOW = 1_800_000_000.0


def _members():
    return [
        NativeCaptureMemberInput(
            role="manifest_json",
            product="public_manifest",
            content_type="application/json",
            received_at=NOW + 1,
            upstream_id="manifest:slot:20260801T1200Z",
            payload=b'{"frames":["frame-a"]}',
            provenance={
                "semantic_class": "upstream_manifest",
                "request_path_sha256": "a" * 64,
            },
        ),
        NativeCaptureMemberInput(
            role="radar_frame",
            product="native_radar_frame",
            content_type="image/png",
            received_at=NOW + 2,
            upstream_id="frame:20260801T1200Z:sha256=" + "b" * 64,
            payload=b"\x89PNG\r\nexact-native-bytes",
            provenance={
                "semantic_class": "rendered_radar_image",
                "product_valid_at": NOW,
            },
        ),
    ]


def _capture(store, members=None, fetch_id="job:one"):
    return capture_native_set(
        store,
        provider="test_radar",
        capture_product="test_radar_capture",
        fetch_id=fetch_id,
        upstream_capture_id="capture:20260801T1200Z:frame-a",
        product_timestamp=NOW,
        collector="test_radar_native",
        collector_version="test-radar-native-v1",
        members=_members() if members is None else members,
    )


def test_complete_set_persists_exact_bytes_before_capture(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()

    result = _capture(store)
    references = result.references()

    assert len(result.members) == 2
    assert [member.role for member in result.capture.members] == [
        "manifest_json", "radar_frame"
    ]
    assert references["capture_id"] == result.capture.capture_id
    assert references["manifest_sha256"] == result.capture.manifest_sha256
    assert "artifact_path" not in str(references)
    assert store.read_bytes(result.members[0].receipt_id) == _members()[0].payload
    assert store.read_bytes(result.members[1].receipt_id) == _members()[1].payload
    for receipt in result.members:
        assert receipt.provenance["representation"] == "upstream_native_bytes"
        assert receipt.provenance["capture_stage"] == "before_decode"


def test_exact_retry_reuses_members_and_capture(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    first = _capture(store)
    retry = _capture(store)

    assert retry.capture == first.capture
    assert retry.members == first.members
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1


def test_changed_member_under_same_fetch_fails_without_false_capture(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    _capture(store)
    changed = _members()
    changed[1] = replace(changed[1], payload=b"different")

    with pytest.raises(RawArtifactConflictError):
        _capture(store, changed)
    assert store.stats()["capture_count"] == 1


def test_partial_member_write_is_not_a_complete_capture_and_retry_recovers(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    bad = _members()
    bad[1] = replace(bad[1], payload="not-bytes")
    with pytest.raises(NativeCaptureError, match="exact bytes"):
        _capture(store, bad)
    # Validation happens before any writes, so even caller-shape errors cannot
    # create a misleading partial attempt.
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0
    assert _capture(store).capture.capture_id


@pytest.mark.parametrize(
    "mutation,match",
    (
        (lambda values: values + [values[0]], "uniek"),
        (
            lambda values: [replace(
                values[0], provenance={"access_token": "secret-value"}
            ), values[1]],
            "credential",
        ),
        (
            lambda values: [replace(
                values[0], provenance={"capture_stage": "after_decode"}
            ), values[1]],
            "overschrijven",
        ),
        (
            lambda values: [replace(values[0], role="bad role"), values[1]],
            "veilig",
        ),
    ),
)
def test_unsafe_or_ambiguous_descriptions_fail_before_writes(
    tmp_path, mutation, match
):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    with pytest.raises(NativeCaptureError, match=match):
        _capture(store, mutation(_members()))
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0


def test_member_count_is_bounded_before_store_work(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    base = _members()[0]
    members = [replace(base, role=f"role-{index:02d}") for index in range(
        MAX_CAPTURE_MEMBERS + 1
    )]
    with pytest.raises(NativeCaptureError, match="begrensd"):
        _capture(store, members)
    assert store.stats()["receipt_count"] == 0
