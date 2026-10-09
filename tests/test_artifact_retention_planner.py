"""Fail-closed tests for the pure raw-artifact retention planner."""
from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine import artifact_retention_planner as planner
from storm_tracker_worker.tracking_core.engine.provider_artifact_contracts import (
    CONTRACTS,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
HA_MODULE = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "engine"
    / "artifact_retention_planner.py"
)
WORKER_MODULE = (
    REPO_ROOT
    / "storm_tracker_worker"
    / "tracking_core"
    / "engine"
    / "artifact_retention_planner.py"
)
DOC = REPO_ROOT / "docs" / "ARTIFACT_RETENTION_PLANNER.md"


def _attribution(provider: str) -> str:
    contract = CONTRACTS.get(provider)
    if contract is None or contract.attribution.value is None:
        return "unverified source"
    return contract.attribution.value


def _artifact(
    provider: str,
    *,
    receipt: str = "receipt-1",
    revision: str = "revision-1",
    size: int = 100,
    received: float = 1_800_000_000.0,
    digest: str = "a" * 64,
    archived: bool = False,
    verified: bool = True,
    archive_receipt: bool = True,
    replay_refs: int = 0,
    legal_hold: bool = False,
    locally_present: bool = True,
    attribution: str | None = None,
) -> planner.ArtifactEvidence:
    return planner.ArtifactEvidence(
        key=planner.ArtifactKey(provider, receipt, revision),
        size_bytes=size,
        received_at=received,
        nominal_at=received - 60.0,
        content_sha256=digest,
        attribution=attribution or _attribution(provider),
        provenance=(
            ("media_type", "application/octet-stream"),
            ("source_record", f"{provider}-frame-001"),
        ),
        replay_dependency_refcount=replay_refs,
        archive_state=(
            planner.ArchiveState.ARCHIVE_VERIFIED
            if archived
            else planner.ArchiveState.LOCAL_ONLY
        ),
        archive_checksum_sha256=digest if archived else None,
        archive_checksum_verified=verified if archived else False,
        archive_receipt_id="archive-receipt-1" if archived and archive_receipt else None,
        legal_hold=legal_hold,
        locally_present=locally_present,
    )


def _policy(
    *providers: str,
    global_quota: int = 10_000,
    provider_quota: int = 5_000,
    reserve: int = 1_000,
    attribution_overrides: dict[str, str] | None = None,
) -> planner.RetentionPolicy:
    attribution = {
        provider: _attribution(provider)
        for provider in providers
    }
    if attribution_overrides:
        attribution.update(attribution_overrides)
    return planner.RetentionPolicy.from_mappings(
        global_quota_bytes=global_quota,
        reserve_free_bytes=reserve,
        per_provider_quota_bytes={
            provider: provider_quota for provider in providers
        },
        attribution_by_provider=attribution,
    )


def _capacity(
    *,
    free: int = 20_000,
    growth: int = 500,
) -> planner.CapacitySnapshot:
    return planner.CapacitySnapshot(
        free_bytes=free,
        projected_daily_growth_bytes=growth,
        measured_at=1_800_000_100.0,
    )


@pytest.mark.parametrize("provider", ("kmi", "rainviewer"))
def test_contract_review_sources_are_explicit_hold(provider):
    result = planner.plan_artifact_retention(
        _artifact(provider),
        inventory=(),
        policy=_policy(provider),
        capacity=_capacity(),
    )
    assert result.decision is planner.RetentionDecision.HOLD
    assert result.review_required is True
    assert "CONTRACT_REVIEW_HOLD" in result.reasons
    assert "REVIEW_REQUIRED" in result.reasons
    assert result.selected_archive_candidates == ()


def test_unknown_provider_and_unknown_licence_fail_closed():
    result = planner.plan_artifact_retention(
        _artifact("future_provider"),
        inventory=(),
        policy=_policy("future_provider"),
        capacity=_capacity(),
    )
    assert result.decision is planner.RetentionDecision.HOLD
    assert result.reasons == ("UNKNOWN_PROVIDER", "REVIEW_REQUIRED")

    kmi = planner.plan_artifact_retention(
        _artifact("kmi"),
        inventory=(),
        policy=_policy("kmi"),
        capacity=_capacity(),
    )
    assert "LICENSE_UNKNOWN" in kmi.reasons


@pytest.mark.parametrize(
    "provider",
    ("noaa_enterprise_rainrate", "dwd_radolan", "knmi"),
)
def test_allowed_retention_classes_store_only_with_caller_policy(provider):
    result = planner.plan_artifact_retention(
        _artifact(provider),
        inventory=(),
        policy=_policy(provider),
        capacity=_capacity(),
    )
    assert result.decision is planner.RetentionDecision.STORE
    assert result.reasons == ("WITHIN_CONFIGURED_POLICY",)
    assert result.review_required is False
    assert result.days_to_reserve_before == pytest.approx(38.0)
    assert result.days_to_reserve_after_store == pytest.approx(37.8)


def test_attribution_and_provider_quota_are_explicit_admission_requirements():
    provider = "noaa_enterprise_rainrate"
    no_attribution = planner.RetentionPolicy.from_mappings(
        global_quota_bytes=10_000,
        reserve_free_bytes=1_000,
        per_provider_quota_bytes={provider: 5_000},
        attribution_by_provider={},
    )
    result = planner.plan_artifact_retention(
        _artifact(provider),
        inventory=(),
        policy=no_attribution,
        capacity=_capacity(),
    )
    assert result.decision is planner.RetentionDecision.HOLD
    assert "ATTRIBUTION_NOT_CONFIGURED" in result.reasons

    wrong_attribution = _policy(
        provider,
        attribution_overrides={provider: "different attribution"},
    )
    result = planner.plan_artifact_retention(
        _artifact(provider),
        inventory=(),
        policy=wrong_attribution,
        capacity=_capacity(),
    )
    assert result.decision is planner.RetentionDecision.HOLD
    assert "ATTRIBUTION_MISMATCH" in result.reasons

    no_quota = planner.RetentionPolicy.from_mappings(
        global_quota_bytes=10_000,
        reserve_free_bytes=1_000,
        per_provider_quota_bytes={},
        attribution_by_provider={provider: _attribution(provider)},
    )
    result = planner.plan_artifact_retention(
        _artifact(provider),
        inventory=(),
        policy=no_quota,
        capacity=_capacity(),
    )
    assert result.decision is planner.RetentionDecision.HOLD
    assert "PROVIDER_QUOTA_NOT_CONFIGURED" in result.reasons


def test_verified_archive_candidates_are_selected_deterministically():
    provider = "noaa_enterprise_rainrate"
    old_provider = _artifact(
        provider,
        receipt="provider-old",
        revision="rev-a",
        size=150,
        received=100.0,
        archived=True,
    )
    old_other = _artifact(
        "dwd_radolan",
        receipt="other-old",
        revision="rev-a",
        size=100,
        received=90.0,
        archived=True,
        digest="b" * 64,
    )
    policy = planner.RetentionPolicy.from_mappings(
        global_quota_bytes=300,
        reserve_free_bytes=100,
        per_provider_quota_bytes={
            provider: 250,
            "dwd_radolan": 250,
        },
        attribution_by_provider={
            provider: _attribution(provider),
            "dwd_radolan": _attribution("dwd_radolan"),
        },
    )
    candidate = _artifact(
        provider,
        receipt="incoming",
        revision="rev-b",
        size=150,
    )
    result = planner.plan_artifact_retention(
        candidate,
        inventory=(old_other, old_provider),
        policy=policy,
        capacity=_capacity(free=2_000),
    )
    assert result.decision is planner.RetentionDecision.ARCHIVE_REQUIRED
    assert result.provider_reclaim_required_bytes == 50
    assert result.global_reclaim_required_bytes == 100
    assert [item.key for item in result.selected_archive_candidates] == [
        old_provider.key
    ]
    assert result.selected_reclaim_bytes == 150
    assert old_provider.locally_present is True

    reversed_result = planner.plan_artifact_retention(
        candidate,
        inventory=(old_provider, old_other),
        policy=policy,
        capacity=_capacity(free=2_000),
    )
    assert reversed_result == result


def test_archive_candidate_requires_every_safety_fact_and_reports_blocks():
    provider = "noaa_enterprise_rainrate"
    dependent = _artifact(
        provider,
        receipt="dependent",
        revision="r1",
        archived=True,
        replay_refs=2,
    )
    held = _artifact(
        "dwd_radolan",
        receipt="held",
        revision="r1",
        archived=True,
        legal_hold=True,
        digest="b" * 64,
    )
    unverified = _artifact(
        "knmi",
        receipt="unverified",
        revision="r1",
        archived=True,
        verified=False,
        archive_receipt=False,
        digest="c" * 64,
    )
    review_hold = _artifact(
        "kmi",
        receipt="review",
        revision="r1",
        archived=True,
        digest="d" * 64,
    )
    result = planner.plan_artifact_retention(
        _artifact(provider, receipt="incoming", size=200),
        inventory=(dependent, held, unverified, review_hold),
        policy=_policy(
            provider,
            "dwd_radolan",
            "knmi",
            "kmi",
            global_quota=300,
            provider_quota=300,
            reserve=100,
        ),
        capacity=_capacity(free=1_000),
    )
    assert result.decision is planner.RetentionDecision.REJECT_CAPACITY
    assert result.eligible_archive_candidates == ()
    blocked = {block.key: set(block.reasons) for block in result.archive_blocks}
    assert "ACTIVE_REPLAY_DEPENDENCY" in blocked[dependent.key]
    assert "LEGAL_HOLD" in blocked[held.key]
    assert {
        "ARCHIVE_CHECKSUM_NOT_VERIFIED",
        "ARCHIVE_RECEIPT_MISSING",
    } <= blocked[unverified.key]
    assert {
        "CONTRACT_REVIEW_HOLD",
        "LICENSE_UNKNOWN",
        "REVIEW_REQUIRED",
    } <= blocked[review_hold.key]


def test_capacity_reserve_can_require_archive_and_never_auto_deletes():
    provider = "noaa_enterprise_rainrate"
    archived = _artifact(
        "dwd_radolan",
        receipt="archive",
        revision="r1",
        size=250,
        archived=True,
    )
    result = planner.plan_artifact_retention(
        _artifact(provider, receipt="incoming", size=200),
        inventory=(archived,),
        policy=_policy(
            provider,
            "dwd_radolan",
            global_quota=5_000,
            provider_quota=5_000,
            reserve=500,
        ),
        capacity=_capacity(free=600),
    )
    assert result.decision is planner.RetentionDecision.ARCHIVE_REQUIRED
    assert result.reserve_reclaim_required_bytes == 100
    assert result.selected_archive_candidates[0].key == archived.key
    assert result.projected_free_bytes_after_store == 400
    assert archived.locally_present is True


def test_legal_hold_capacity_pressure_is_review_not_silent_rejection():
    provider = "noaa_enterprise_rainrate"
    result = planner.plan_artifact_retention(
        _artifact(provider, size=200, legal_hold=True),
        inventory=(),
        policy=_policy(
            provider,
            global_quota=100,
            provider_quota=100,
            reserve=50,
        ),
        capacity=_capacity(free=100),
    )
    assert result.decision is planner.RetentionDecision.HOLD
    assert result.reasons == ("LEGAL_HOLD_CAPACITY_REVIEW", "REVIEW_REQUIRED")


def test_source_revisions_are_independently_counted_even_for_one_receipt():
    provider = "noaa_enterprise_rainrate"
    first = _artifact(provider, receipt="same", revision="r1", size=40)
    second = _artifact(
        provider,
        receipt="same",
        revision="r2",
        size=40,
        digest="b" * 64,
    )
    result = planner.plan_artifact_retention(
        _artifact(provider, receipt="same", revision="r3", size=10),
        inventory=(first, second),
        policy=_policy(
            provider,
            global_quota=1_000,
            provider_quota=85,
            reserve=100,
        ),
        capacity=_capacity(free=1_000),
    )
    assert result.current_provider_usage_bytes == 80
    assert result.projected_provider_usage_bytes == 90
    assert result.provider_reclaim_required_bytes == 5
    assert result.decision is planner.RetentionDecision.REJECT_CAPACITY


@pytest.mark.parametrize("size", (0, -1))
def test_zero_and_negative_artifact_size_are_invalid(size):
    with pytest.raises(ValueError):
        _artifact("noaa_enterprise_rainrate", size=size)


@pytest.mark.parametrize("received", (float("nan"), float("inf"), -1.0))
def test_nonfinite_or_negative_time_is_invalid(received):
    with pytest.raises(ValueError):
        _artifact("noaa_enterprise_rainrate", received=received)


def test_zero_budgets_and_integer_overflow_fail_before_planning():
    with pytest.raises(ValueError):
        _capacity(free=0)
    with pytest.raises(ValueError):
        _capacity(growth=0)
    with pytest.raises(ValueError):
        _policy("noaa_enterprise_rainrate", global_quota=0)
    with pytest.raises(OverflowError):
        _policy(
            "noaa_enterprise_rainrate",
            global_quota=planner.MAX_STORAGE_BYTES + 1,
        )
    with pytest.raises(OverflowError):
        _artifact(
            "noaa_enterprise_rainrate",
            size=planner.MAX_STORAGE_BYTES + 1,
        )

    enormous = _artifact(
        "noaa_enterprise_rainrate",
        receipt="huge",
        size=planner.MAX_STORAGE_BYTES,
    )
    with pytest.raises(OverflowError, match="current global usage"):
        planner.plan_artifact_retention(
            _artifact(
                "dwd_radolan",
                receipt="one-more",
                size=1,
                digest="b" * 64,
            ),
            inventory=(
                enormous,
                _artifact(
                    "knmi",
                    receipt="overflow",
                    size=1,
                    digest="c" * 64,
                ),
            ),
            policy=_policy(
                "noaa_enterprise_rainrate",
                "dwd_radolan",
                "knmi",
                global_quota=planner.MAX_STORAGE_BYTES,
                provider_quota=planner.MAX_STORAGE_BYTES,
            ),
            capacity=_capacity(free=planner.MAX_STORAGE_BYTES),
        )


@pytest.mark.parametrize(
    "mutation",
    (
        {"receipt": "https://example.invalid/raw/frame"},
        {"revision": "frame?access_token=opaque"},
    ),
)
def test_record_identities_reject_full_urls_and_secret_material(mutation):
    with pytest.raises(Exception):
        _artifact("noaa_enterprise_rainrate", **mutation)

    base = _artifact("noaa_enterprise_rainrate")
    with pytest.raises(Exception):
        replace(
            base,
            provenance=(
                ("media_type", "application/octet-stream"),
                ("source_record", "https://example.invalid/frame"),
            ),
        )
    with pytest.raises(Exception):
        planner.RetentionPolicy.from_mappings(
            global_quota_bytes=1_000,
            reserve_free_bytes=100,
            per_provider_quota_bytes={"noaa_enterprise_rainrate": 500},
            attribution_by_provider={
                "noaa_enterprise_rainrate": (
                    "https://example.invalid/source?token=opaque"
                )
            },
        )


def test_inventory_duplicate_revision_is_rejected():
    artifact = _artifact("noaa_enterprise_rainrate")
    with pytest.raises(ValueError, match="duplicate source revision"):
        planner.plan_artifact_retention(
            _artifact(
                "noaa_enterprise_rainrate",
                receipt="incoming",
                revision="r2",
            ),
            inventory=(artifact, artifact),
            policy=_policy("noaa_enterprise_rainrate"),
            capacity=_capacity(),
        )


def test_ha_and_worker_modules_are_exact_byte_mirrors():
    ha_bytes = HA_MODULE.read_bytes()
    worker_bytes = WORKER_MODULE.read_bytes()
    assert ha_bytes == worker_bytes
    assert hashlib.sha256(ha_bytes).hexdigest() == hashlib.sha256(
        worker_bytes
    ).hexdigest()


def test_module_is_pure_and_live_runtime_does_not_import_it():
    tree = ast.parse(WORKER_MODULE.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "").split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert imported_roots <= {
        "__future__",
        "dataclasses",
        "enum",
        "math",
        "provider_artifact_contracts",
        "re",
        "typing",
    }
    forbidden_calls = {
        "open",
        "unlink",
        "remove",
        "rmdir",
        "rename",
        "replace",
    }
    assert not {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        if node.func.id in forbidden_calls
    }

    runtime_roots = (
        REPO_ROOT / "custom_components" / "storm_tracker_v3",
        REPO_ROOT / "storm_tracker_worker",
    )
    imports: list[Path] = []
    for root in runtime_roots:
        for path in root.rglob("*.py"):
            if path.name in {
                "artifact_retention_planner.py",
                "artifact_capture_admission.py",
            }:
                continue
            parsed = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(parsed):
                if isinstance(node, ast.ImportFrom) and (
                    "artifact_retention_planner" in (node.module or "")
                ):
                    imports.append(path)
                if isinstance(node, ast.Import) and any(
                    "artifact_retention_planner" in alias.name
                    for alias in node.names
                ):
                    imports.append(path)
    assert imports == []
    assert planner.SHADOW_ONLY is True


def test_documentation_warns_that_projection_is_not_yet_proven():
    text = DOC.read_text(encoding="utf-8")
    assert "geen bewezen opslagprojectie" in text
    assert "echte capture-size-inventaris" in text
    assert "nooit" in text and "verwijdert" in text
