"""Shared-matrix integration for exact-RAW satellite lightning capture."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import io
from pathlib import Path
from types import SimpleNamespace

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import acquisition_handler_child as child
from storm_tracker_worker import lightning_exact_raw as lightning
from storm_tracker_worker import raw_manifest_audit_completion_consumer as raw_audit
from storm_tracker_worker import terminal_raw_completion_router as terminal
from storm_tracker_worker import worker_acquisition_plan as plan
from storm_tracker_worker.acquisition_query_authority import safe_native_job_id
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore


ORIGIN_JOB_ID = "acquisition:" + "a" * 64
NATIVE_JOB_ID = safe_native_job_id(ORIGIN_JOB_ID)
NOW = datetime(2026, 8, 14, 12, 30, tzinfo=timezone.utc)


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self._stream = io.BytesIO(payload)
        self.status = 200
        self.headers = {
            "Content-Length": str(len(payload)),
            "Content-Type": "application/octet-stream",
        }
        self._url = url

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def geturl(self) -> str:
        return self._url

    def close(self) -> None:
        return None


class _Router:
    def __init__(self) -> None:
        self._routes: dict[str, bytes] = {}

    def add(self, url: str, payload: bytes) -> None:
        self._routes[url] = payload

    def __call__(self, request, *, timeout: float):
        assert 0.0 < timeout <= lightning.HTTP_IO_TIMEOUT_SECONDS
        url = request.full_url
        try:
            payload = self._routes.pop(url)
        except KeyError as error:  # pragma: no cover - diagnostic guard
            raise AssertionError(f"unexpected URL: {url}") from error
        return _Response(payload, url)


def _store(tmp_path: Path) -> RawArtifactStore:
    result = RawArtifactStore(
        tmp_path / "raw",
        limits=RawArtifactLimits(
            max_object_bytes=16 * 1024 * 1024,
            max_total_bytes=256 * 1024 * 1024,
            max_capture_members=32,
        ),
    )
    result.initialize()
    return result


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y%j%H%M%S") + "0"


def _key(satellite: int, moment: datetime) -> str:
    return (
        f"{lightning._goes_prefix(moment)}OR_GLM-L2-LCFA_G{satellite}_"
        f"s{_stamp(moment)}_e{_stamp(moment + timedelta(seconds=20))}_"
        f"c{_stamp(moment + timedelta(seconds=40))}.nc"
    )


def _listing(satellite: int, prefix: str, keys: tuple[str, ...]) -> bytes:
    contents = "".join(
        f"<Contents><Key>{key}</Key><Size>0</Size></Contents>"
        for key in keys
    )
    return (
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Name>noaa-goes{satellite}</Name><Prefix>{prefix}</Prefix>"
        f"<KeyCount>{len(keys)}</KeyCount><MaxKeys>1000</MaxKeys>"
        f"<IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
    ).encode("ascii")


def _capture_goes18(store: RawArtifactStore) -> dict:
    previous = NOW.replace(minute=0) - timedelta(hours=1)
    current = NOW.replace(minute=0)
    keys = (_key(18, previous + timedelta(minutes=59)), _key(18, current))
    router = _Router()
    for prefix, members in (
        (lightning._goes_prefix(previous), keys[:1]),
        (lightning._goes_prefix(current), keys[1:]),
    ):
        router.add(
            lightning._goes_listing_binding(18, prefix).url,
            _listing(18, prefix, members),
        )
    for key in keys:
        # A successful zero-byte entity is malformed meteorologically, but it
        # remains an exact durable transport response by contract.
        router.add(lightning._goes_object_binding(18, key).url, b"")
    return lightning.capture_noaa_goes18_glm_raw(
        SimpleNamespace(job_id=NATIVE_JOB_ID, payload={}),
        raw_store=store,
        now=NOW,
        opener=router,
    )


def test_lightning_matrix_is_active_raw_only_with_exact_budgets() -> None:
    rows = {
        row["workload_id"]: row
        for row in plan.acquisition_catalog_snapshot()["workloads"]
    }
    expected = {
        "lightning.eumetsat.li": (14 * 1024 * 1024, 2, 50),
        "lightning.goes18.glm": (68 * 1024 * 1024, 14, 105),
        "lightning.goes19.glm": (68 * 1024 * 1024, 14, 105),
    }
    for workload_id, budget in expected.items():
        row = rows[workload_id]
        rule = activation.ACTIVATION_RULES[workload_id]
        assert row["status"] == plan.CapabilityStatus.CONTRACT_READY_UNWIRED.value
        assert (
            row["budget"]["max_transport_bytes"],
            row["budget"]["max_artifacts"],
            row["budget"]["max_wall_seconds"],
        ) == budget
        assert rule.status is activation.ActivationStatus.ACTIVE_SHADOW_RAW
        assert rule.raw_capture_service_only is True
        assert rule.process_target == activation.SHADOW_CAPTURE_TARGET
        assert rule.handler_payload_contract == "exact_empty_object_v1"
    assert len(activation.DEFAULT_SHADOW_WORKLOAD_IDS) == 11
    assert not set(expected) & activation.DEFAULT_SHADOW_WORKLOAD_IDS
    assert set(expected) <= activation.RAW_CAPTURE_WORKLOAD_IDS


def test_shared_specs_are_exact_drop_ins_for_standalone_contracts() -> None:
    native_contracts = lightning.lightning_child_handler_contracts()
    native_recovery = lightning.lightning_recovery_binding_metadata()
    for workload_id in (
        "lightning.eumetsat.li",
        "lightning.goes18.glm",
        "lightning.goes19.glm",
    ):
        contract = native_contracts[workload_id]
        rule = activation.ACTIVATION_RULES[workload_id]
        spec = child._HANDLERS[rule.handler_id]
        binding = recovery.RECOVERY_BINDINGS[rule.handler_id]
        metadata = native_recovery[rule.handler_id]
        assert contract["handler_id"] == rule.handler_id == spec.handler_id
        assert contract["required_static_roles"] == list(rule.required_roles)
        assert contract["maximum_transport_bytes"] == rule.maximum_transport_bytes
        assert contract["maximum_artifacts"] == rule.maximum_artifacts
        assert contract["dynamic_role_pattern"] == rule.dynamic_role_pattern
        assert metadata["minimum_artifacts"] == binding.minimum_artifacts
        assert metadata["maximum_artifacts"] == binding.maximum_artifacts
        assert child._recovery_binding(spec) == binding


@pytest.mark.parametrize(
    ("workload_id", "satellite"),
    (("lightning.goes18.glm", 18), ("lightning.goes19.glm", 19)),
)
def test_dynamic_recovery_roles_are_contiguous_and_satellite_bound(
    workload_id: str,
    satellite: int,
) -> None:
    rule = activation.ACTIVATION_RULES[workload_id]
    binding = recovery.RECOVERY_BINDINGS[str(rule.handler_id)]
    roles = (
        "listing_previous", "listing_current", "netcdf_00", "netcdf_01",
    )
    members = binding.members_for_roles(roles)
    assert tuple(member.role for member in members) == roles
    assert binding.satellite == satellite
    assert binding.minimum_artifacts == 3
    assert binding.maximum_artifacts == 14
    assert binding.member("netcdf_11").receipt_suffix == ":netcdf_11"
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        binding.members_for_roles((
            "listing_previous", "listing_current", "netcdf_01",
        ))
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        binding.members_for_roles(("listing_previous", "listing_current"))
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        binding.member("netcdf_12")


def test_goes_exact_bundle_revalidates_dynamic_set_and_zero_byte_2xx(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    result = _capture_goes18(store)
    capture_id = result["raw_provenance"]["raw_capture_id"]
    loaded = store.load_capture_bundle_bytes(capture_id)
    assert loaded is not None
    capture, receipts, payloads = loaded
    binding = recovery.RECOVERY_BINDINGS[lightning.GOES18_HANDLER_ID]
    members = binding.members_for_roles(receipts)
    metadata = {}
    job_ids = set()
    for member in members:
        job_id, item = recovery._validate_receipt(
            receipts[member.role], binding, member
        )
        job_ids.add(job_id)
        metadata[member.role] = item
    assert any(receipt.size_bytes == 0 for receipt in receipts.values())
    assert recovery._validate_capture(
        capture,
        binding=binding,
        receipts_by_role=receipts,
        metadata_by_role=metadata,
        payloads_by_role=payloads,
    ) == NATIVE_JOB_ID
    digest = recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role=receipts,
        capture=capture,
    )
    assert job_ids == {NATIVE_JOB_ID}
    assert digest.actual_entries == 4
    wrong = recovery.RECOVERY_BINDINGS[lightning.GOES19_HANDLER_ID]
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        recovery._validate_receipt(
            receipts["listing_previous"],
            wrong,
            wrong.member("listing_previous"),
        )


def test_manifest_audit_accepts_only_lightning_zero_byte_dynamic_members(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    result = _capture_goes18(store)
    loaded = store.load_capture_bundle_bytes(
        result["raw_provenance"]["raw_capture_id"]
    )
    assert loaded is not None
    _capture, receipts, _payloads = loaded
    binding = recovery.RECOVERY_BINDINGS[lightning.GOES18_HANDLER_ID]
    references = [
        {
            "content_sha256": receipt.content_sha256,
            "content_type": receipt.content_type,
            "product": receipt.product,
            "provider": receipt.provider,
            "receipt_id": receipt.receipt_id,
            "receipt_sha256": receipt.receipt_sha256,
            "role": role,
            "size_bytes": receipt.size_bytes,
        }
        for role, receipt in sorted(receipts.items())
    ]
    roles = raw_audit._audit_roles_from_reference(binding, references)
    assert tuple(item.role for item in roles) == tuple(sorted(receipts))
    assert any(item.size_bytes == 0 for item in roles)

    gap = [item for item in references if item["role"] != "netcdf_00"]
    with pytest.raises(raw_audit.RawManifestAuditIntegrityError):
        raw_audit._audit_roles_from_reference(binding, gap)

    fixed = recovery.RECOVERY_BINDINGS[child.METAR_HANDLER_ID]
    fixed_member = fixed.members[0]
    with pytest.raises(raw_audit.RawManifestAuditIntegrityError):
        raw_audit._audit_roles_from_reference(fixed, [{
            "content_sha256": "0" * 64,
            "content_type": next(iter(fixed_member.allowed_content_types)),
            "product": fixed_member.raw_product,
            "provider": fixed.raw_provider,
            "receipt_id": "receipt-zero",
            "receipt_sha256": "1" * 64,
            "role": fixed_member.role,
            "size_bytes": 0,
        }])


def test_eumetsat_parent_credentials_and_child_token_remain_out_of_contracts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    rule = activation.ACTIVATION_RULES["lightning.eumetsat.li"]
    assert rule.secret_requirement_ids == (
        "worker_secret.eumetsat_consumer_key",
        "worker_secret.eumetsat_consumer_secret",
    )
    assert rule.secret_delivery == (
        "parent_oauth_client_credentials_to_ephemeral_child_bearer_v1"
    )
    resolver = activation.EnvironmentSecretResolver({
        "STV3_EUMETSAT_CONSUMER_KEY": "consumer-key",
        "STV3_EUMETSAT_CONSUMER_SECRET": "consumer-secret",
    })
    assert all(resolver.resolve(item) for item in rule.secret_requirement_ids)

    token_text = "ephemeral-child-token-0123456789"
    monkeypatch.setenv(lightning.EUMETSAT_EPHEMERAL_TOKEN_ENV, token_text)
    captured: dict[str, object] = {}

    def fake_capture(job, *, raw_store, access_token_supplier, deadline_at):
        del raw_store
        token = access_token_supplier()
        captured["job_id"] = job.job_id
        captured["authorization"] = token.authorization_value()
        captured["deadline_at"] = deadline_at
        captured["token"] = token
        return {"status": "technical_only"}

    monkeypatch.setattr(
        lightning, "capture_eumetsat_mtg_li_raw", fake_capture
    )
    spec = child._HANDLERS[lightning.EUMETSAT_HANDLER_ID]
    result = child._run_native(
        spec,
        job_id=ORIGIN_JOB_ID,
        store=_store(tmp_path),
        execution_deadline_at=1234.5,
    )
    assert result == {"status": "technical_only"}
    assert captured["job_id"] == NATIVE_JOB_ID
    assert captured["authorization"] == f"Bearer {token_text}"
    assert captured["deadline_at"] == 1234.5
    assert captured["token"].closed is True
    assert lightning.EUMETSAT_EPHEMERAL_TOKEN_ENV not in child.os.environ
    assert token_text not in repr(result)


def test_eumetsat_child_request_requires_same_bounded_absolute_deadline(
    tmp_path: Path,
) -> None:
    request = {
        "acquisition_job_id": ORIGIN_JOB_ID,
        "contract": child.CONTRACT,
        "handler_id": lightning.EUMETSAT_HANDLER_ID,
        "handler_payload": {},
        "raw_store_limits": {
            name: getattr(RawArtifactLimits(), name)
            for name in child._LIMIT_FIELDS
        },
        "raw_store_root": str(tmp_path / "raw"),
        "workload_id": lightning.EUMETSAT_WORKLOAD_ID,
    }
    with pytest.raises(ValueError, match="contract mismatch"):
        child.execute_shadow_capture(request, _clock=lambda: 1000.0)
    request["execution_deadline_at"] = 1050.0001
    with pytest.raises(ValueError, match="outside its bound"):
        child.execute_shadow_capture(request, _clock=lambda: 1000.0)


def test_terminal_lightning_routes_are_inert_manifest_audits() -> None:
    for workload_id in (
        "lightning.eumetsat.li",
        "lightning.goes18.glm",
        "lightning.goes19.glm",
    ):
        consumer = terminal.TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        assert consumer.consumer_kind == "raw_manifest_audit"
        assert consumer.transfer_mode == terminal.REFERENCE_TRANSFER_MODE
        assert consumer.uses_reference_payload is True
        assert consumer.as_dict()["meteorological_admission"] is False
