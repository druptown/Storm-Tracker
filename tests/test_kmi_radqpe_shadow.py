"""Tests for the non-operational KMI RADQPE/public-composite boundary."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import types

import numpy as np
from PIL import Image, TiffImagePlugin
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
HA_CONTRACT = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "engine"
    / "kmi_radqpe_contract.py"
)
WORKER_CONTRACT = (
    REPO_ROOT
    / "storm_tracker_worker"
    / "tracking_core"
    / "engine"
    / "kmi_radqpe_contract.py"
)
PROVIDER = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "providers"
    / "kmi_public_radar_composite.py"
)
REGISTRY = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "providers"
    / "registry.py"
)
POLICY = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "provider_policy.json"
)
HA_INIT = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "__init__.py"
)


def _package(name: str) -> None:
    if name not in sys.modules:
        module = types.ModuleType(name)
        module.__path__ = []
        sys.modules[name] = module


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def contract():
    _package("storm_tracker_v3")
    _package("storm_tracker_v3.engine")
    return _load(
        "storm_tracker_v3.engine.kmi_radqpe_contract",
        HA_CONTRACT,
    )


@pytest.fixture(scope="module")
def provider(contract, bounded_payload_module):
    _package("storm_tracker_v3.providers")
    return _load(
        "storm_tracker_v3.providers.kmi_public_radar_composite",
        PROVIDER,
    )


def _utc(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _wms(
    *,
    start: str = "2026-07-29T23:35:00Z",
    end: str = "2026-07-30T12:30:00Z",
    default: str = "2026-07-30T12:30:00Z",
    cadence: str = "PT5M",
    layer: str = "radar:belgian_rainfall_composite",
) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<WMS_Capabilities xmlns="http://www.opengis.net/wms" version="1.3.0">
  <Service>
    <Title>KMI-IRM - Web Map Service</Title>
    <Fees>none</Fees>
    <AccessConstraints>none</AccessConstraints>
  </Service>
  <Capability><Layer><Title>root</Title>
    <Layer>
      <Name>{layer}</Name>
      <Title>belgian_rainfall_composite</Title>
      <CRS>EPSG:4326</CRS><CRS>CRS:84</CRS>
      <EX_GeographicBoundingBox>
        <westBoundLongitude>-0.9</westBoundLongitude>
        <eastBoundLongitude>-0.88</eastBoundLongitude>
        <southBoundLatitude>53.78</southBoundLatitude>
        <northBoundLatitude>53.8</northBoundLatitude>
      </EX_GeographicBoundingBox>
      <Dimension name="time" default="{default}" units="ISO8601">
        {start}/{end}/{cadence}
      </Dimension>
    </Layer>
  </Layer></Capability>
</WMS_Capabilities>
""".encode()


def _wcs(
    *,
    start: str = "2026-07-29T23:35:00Z",
    end: str = "2026-07-30T12:30:00Z",
    default: str = "2026-07-30T12:30:00Z",
    width: int = 2,
    height: int = 2,
    range_field: str = "GRAY_INDEX",
    uom: str = "W.m-2.Sr-1",
    nil_value: str = "NaN",
) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<wcs:CoverageDescriptions
 xmlns:wcs="http://www.opengis.net/wcs/2.0"
 xmlns:gml="http://www.opengis.net/gml/3.2"
 xmlns:gmlcov="http://www.opengis.net/gmlcov/1.0"
 xmlns:swe="http://www.opengis.net/swe/2.0"
 xmlns:wcsgs="http://www.geoserver.org/wcsgs/2.0">
 <wcs:CoverageDescription gml:id="radar__belgian_rainfall_composite">
  <gml:name>belgian_rainfall_composite</gml:name>
  <gml:boundedBy>
   <gml:EnvelopeWithTimePeriod
    srsName="http://www.opengis.net/def/crs/EPSG/0/4326"
    axisLabels="Lat Long time">
    <gml:lowerCorner>47.4 -0.9</gml:lowerCorner>
    <gml:upperCorner>53.8 9.6</gml:upperCorner>
    <gml:beginPosition>{start}</gml:beginPosition>
    <gml:endPosition>{end}</gml:endPosition>
   </gml:EnvelopeWithTimePeriod>
  </gml:boundedBy>
  <wcs:CoverageId>radar__belgian_rainfall_composite</wcs:CoverageId>
  <gmlcov:metadata><gmlcov:Extension>
   <wcsgs:TimeDomain default="{default}">
    <gml:TimePeriod gml:id="tp">
     <gml:beginPosition>{start}</gml:beginPosition>
     <gml:endPosition>{end}</gml:endPosition>
     <gml:timeInterval unit="minute">5</gml:timeInterval>
    </gml:TimePeriod>
   </wcsgs:TimeDomain>
  </gmlcov:Extension></gmlcov:metadata>
  <gml:domainSet><gml:RectifiedGrid gml:id="grid" dimension="2">
   <gml:limits><gml:GridEnvelope>
    <gml:low>0 0</gml:low>
    <gml:high>{width - 1} {height - 1}</gml:high>
   </gml:GridEnvelope></gml:limits>
   <gml:origin><gml:Point gml:id="origin">
    <gml:pos>53.8 -0.9</gml:pos>
   </gml:Point></gml:origin>
   <gml:offsetVector>0.0 0.01</gml:offsetVector>
   <gml:offsetVector>-0.01 0.0</gml:offsetVector>
  </gml:RectifiedGrid></gml:domainSet>
  <gmlcov:rangeType><swe:DataRecord>
   <swe:field name="{range_field}"><swe:Quantity>
    <swe:nilValues><swe:NilValues>
     <swe:nilValue>{nil_value}</swe:nilValue>
    </swe:NilValues></swe:nilValues>
    <swe:uom code="{uom}"/>
   </swe:Quantity></swe:field>
  </swe:DataRecord></gmlcov:rangeType>
  <wcs:ServiceParameters>
   <wcs:CoverageSubtype>RectifiedGridCoverage</wcs:CoverageSubtype>
   <wcs:nativeFormat>image/tiff</wcs:nativeFormat>
  </wcs:ServiceParameters>
 </wcs:CoverageDescription>
</wcs:CoverageDescriptions>
""".encode()


def _tiff(
    *,
    rgb: bool = False,
    epsg: int = 4326,
    nodata: str | None = None,
) -> bytes:
    if rgb:
        values = np.zeros((2, 2, 3), dtype=np.uint8)
    else:
        values = np.asarray(
            [[-999.0, 0.0], [0.6, 56.85]],
            dtype=np.float32,
        )
    image = Image.fromarray(values)
    tags = TiffImagePlugin.ImageFileDirectory_v2()
    tags[34264] = (
        0.01, 0.0, 0.0, -0.9,
        0.0, -0.01, 0.0, 53.8,
        0.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 1.0,
    )
    if epsg == 4326:
        tags[34735] = (
            1, 1, 2, 3,
            1024, 0, 1, 2,
            1025, 0, 1, 1,
            2048, 0, 1, 4326,
        )
    else:
        tags[34735] = (
            1, 1, 2, 3,
            1024, 0, 1, 1,
            1025, 0, 1, 1,
            3072, 0, 1, epsg,
        )
    if nodata is not None:
        tags[42113] = nodata
    output = io.BytesIO()
    image.save(output, format="TIFF", tiffinfo=tags)
    return output.getvalue()


def test_contract_is_explicitly_shadow_only_and_fail_closed(contract):
    value = contract.KMI_RADQPE_SHADOW_CONTRACT
    assert value.shadow_only is True
    assert value.public_source_id == "kmi_public_radar_composite"
    assert value.native_source_id == "kmi_radqpe_native"
    assert value.native_crs_epsg == 3812
    assert value.native_resolution_metres == 1000
    assert value.native_transport_access is contract.ContractGate.BLOCKED
    assert (
        value.public_product_identity_as_radqpe
        is contract.ContractGate.UNVERIFIED
    )
    assert value.retention_and_value_added_use is contract.ContractGate.BLOCKED
    assert value.detection_eligible is False
    assert value.retention_eligible is False
    assert value.to_dict()["operational_cutover"] == "BLOCKED"


def test_public_catalog_preserves_transport_facts_not_product_identity(contract):
    snapshot = contract.parse_public_wms_capabilities(_wms())
    assert snapshot.default_time == "2026-07-30T12:30:00Z"
    assert snapshot.cadence_seconds == 300
    assert snapshot.crs == ("CRS:84", "EPSG:4326")
    assert snapshot.fees == "none"
    assert snapshot.access_constraints == "none"
    assert snapshot.payload_sha256 == hashlib.sha256(_wms()).hexdigest()
    assert "radqpe" not in snapshot.to_dict()


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (_wms(cadence="PT10M"), "vijf minuten"),
        (_wms(default="2026-07-30T13:30:00Z"), "buiten"),
        (_wms(layer="different_layer"), "ontbreekt"),
        (_wms(start="2026-07-29T23:35:00+00:00"), "UTC"),
    ),
)
def test_public_catalog_fails_closed_on_changed_contract(
    contract, payload, message
):
    with pytest.raises(contract.KmiRadqpeContractError, match=message):
        contract.parse_public_wms_capabilities(payload)


def test_wcs_description_exposes_wrong_quantity_and_unit(contract):
    description = contract.parse_public_wcs_description(_wcs())
    assert description.coverage_id == "radar__belgian_rainfall_composite"
    assert (description.width, description.height) == (2, 2)
    assert description.cadence_seconds == 300
    assert description.range_field == "GRAY_INDEX"
    assert description.declared_uom == "W.m-2.Sr-1"
    assert description.declared_nil_values == ("NaN",)
    assert description.quantitative_semantics_eligible is False
    assert description.semantic_blockers == (
        "range_field_does_not_declare_precipitation_quantity",
        "declared_uom_is_not_a_precipitation_unit",
    )


def test_geotiff_audit_keeps_values_unknown(contract):
    description = contract.parse_public_wcs_description(_wcs())
    payload = _tiff()
    audit = contract.audit_public_wcs_geotiff(payload, description)
    assert audit.payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert (audit.width, audit.height) == (2, 2)
    assert audit.mode == "float32"
    assert audit.epsg == 4326
    assert audit.bounds == pytest.approx((-0.9, 53.78, -0.88, 53.8))
    assert audit.embedded_nodata is None
    assert audit.finite_samples == 4
    assert audit.nonfinite_samples == 0
    assert audit.negative_samples == 1
    assert audit.minimum == pytest.approx(-999.0)
    assert audit.maximum == pytest.approx(56.85)
    assert audit.quantitative_semantics_eligible is False
    assert "public_tiff_has_no_embedded_nodata" in audit.semantic_blockers
    assert "public_tiff_is_not_native_lambert_2008_grid" in audit.semantic_blockers


def test_geotiff_audit_rejects_rendered_rgb_and_wrong_dimensions(contract):
    description = contract.parse_public_wcs_description(_wcs())
    with pytest.raises(contract.KmiRadqpeContractError, match="enkelbands"):
        contract.audit_public_wcs_geotiff(_tiff(rgb=True), description)
    wrong_shape = contract.parse_public_wcs_description(
        _wcs(width=3, height=2)
    )
    with pytest.raises(contract.KmiRadqpeContractError, match="afmetingen"):
        contract.audit_public_wcs_geotiff(_tiff(), wrong_shape)
    with pytest.raises(contract.KmiRadqpeContractError, match="CRS"):
        contract.audit_public_wcs_geotiff(_tiff(epsg=3812), description)


def test_geotiff_embedded_nodata_is_a_fact_but_does_not_unlock_semantics(
    contract,
):
    description = contract.parse_public_wcs_description(
        _wcs(range_field="RATE", uom="mm/h", nil_value="-999")
    )
    audit = contract.audit_public_wcs_geotiff(
        _tiff(nodata="-999"),
        description,
    )
    assert description.quantitative_semantics_eligible is True
    assert audit.embedded_nodata == -999.0
    assert audit.quantitative_semantics_eligible is False
    assert "public_tiff_has_no_embedded_precipitation_unit" in (
        audit.semantic_blockers
    )


def test_request_builders_are_canonical_and_secret_free(contract):
    timestamp = _utc("2026-07-30T12:30:00Z")
    requests = (
        contract.public_capabilities_request(),
        contract.public_description_request(),
        contract.public_coverage_request(timestamp),
    )
    assert requests[0] == {
        "SERVICE": "WMS",
        "VERSION": "1.3.0",
        "REQUEST": "GetCapabilities",
    }
    assert requests[2]["SUBSET"] == 'time("2026-07-30T12:30:00Z")'
    serialized = json.dumps(requests).lower()
    assert all(
        sensitive not in serialized
        for sensitive in ("password", "secret", "token", "api_key", "auth")
    )


def test_native_filename_contract_is_utc_specific_and_still_blocked(contract):
    rate = contract.native_filename_facts(
        "20260730123000.rad.best.comp.rate.qpe.hdf"
    )
    accumulation = contract.native_filename_facts(
        "20260730123000.rad.best.comp.acrr.qpe.10m.tif"
    )
    assert rate["nominal_time"] == "2026-07-30T12:30:00Z"
    assert rate["quantity_token"] == "rate.qpe"
    assert accumulation["quantity_token"] == "acrr.qpe.10m"
    assert rate["native_crs"] == "Belgian Lambert 2008"
    assert rate["access_configured"] is False
    assert rate["detection_eligible"] is False
    with pytest.raises(contract.KmiRadqpeContractError):
        contract.native_filename_facts("latest.radqpe.tif")


class _Chunks:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    async def iter_chunked(self, size: int):
        for index in range(0, len(self.payload), max(1, size)):
            yield self.payload[index:index + size]


class _Response:
    def __init__(
        self,
        payload: bytes,
        media_type: str,
        *,
        status: int = 200,
    ) -> None:
        self.status = status
        self.headers = {
            "Content-Type": media_type,
            "Content-Length": str(len(payload)),
            "Content-Encoding": "identity",
        }
        self.content = _Chunks(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = list(responses)
        self.requests: list[dict] = []

    def get(self, endpoint, *, params, headers, timeout):
        self.requests.append({
            "endpoint": endpoint,
            "params": dict(params),
            "headers": dict(headers),
            "timeout": timeout,
        })
        if not self.responses:
            raise AssertionError("onverwachte extra request")
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_shadow_adapter_fetches_complete_set_but_no_weather(
    contract, provider
):
    wms, wcs, tiff = _wms(), _wcs(), _tiff()
    session = _Session([
        _Response(wms, "text/xml"),
        _Response(wcs, "application/xml"),
        _Response(tiff, "image/tiff"),
    ])
    now = _utc("2026-07-30T12:32:00Z")
    adapter = provider.KmiPublicRadarCompositeShadowAdapter(
        session,
        clock=lambda: now,
    )
    result = await adapter.async_shadow_fetch()
    assert result.exact_raw_artifacts_complete_in_memory is True
    assert result.exact_raw_artifacts_durably_committed is False
    summary = result.summary_dict()
    assert summary["exact_raw_artifacts_complete_in_memory"] is True
    assert summary["exact_raw_artifacts_durably_committed"] is False
    assert "exact_raw_artifacts_complete" not in summary
    assert result.detection_eligible is False
    assert result.retention_eligible is False
    assert {artifact.role for artifact in result.artifacts} == {
        "wms_capabilities",
        "wcs_description",
        "wcs_coverage",
    }
    assert all(isinstance(artifact.payload, bytes) for artifact in result.artifacts)
    assert len(session.requests) == 3
    assert all(
        request["endpoint"] == contract.PUBLIC_OWS_ENDPOINT
        for request in session.requests
    )
    assert session.requests[2]["params"]["SUBSET"] == (
        'time("2026-07-30T12:30:00Z")'
    )
    assert adapter.diagnostics["status"] == "captured_semantics_blocked"
    assert adapter.diagnostics["detection_eligible"] is False
    assert adapter.diagnostics["retention_eligible"] is False
    assert adapter.diagnostics[
        "exact_raw_artifacts_complete_in_memory"
    ] is True
    assert adapter.diagnostics[
        "exact_raw_artifacts_durably_committed"
    ] is False
    assert "exact_raw_artifacts_complete" not in adapter.diagnostics
    assert not hasattr(adapter, "fetch_observations")
    assert not hasattr(adapter, "async_fetch")


@pytest.mark.asyncio
async def test_shadow_adapter_rejects_stale_catalog_before_coverage(
    contract, provider
):
    session = _Session([_Response(_wms(), "text/xml")])
    adapter = provider.KmiPublicRadarCompositeShadowAdapter(
        session,
        clock=lambda: _utc("2026-07-30T13:30:00Z"),
        maximum_age_seconds=20 * 60,
    )
    with pytest.raises(contract.KmiRadqpeContractError, match="ouder"):
        await adapter.async_shadow_fetch()
    assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_shadow_adapter_rejects_wrong_media_type(contract, provider):
    session = _Session([_Response(_wms(), "image/png")])
    adapter = provider.KmiPublicRadarCompositeShadowAdapter(
        session,
        clock=lambda: _utc("2026-07-30T12:32:00Z"),
    )
    with pytest.raises(contract.KmiRadqpeContractError, match="media type"):
        await adapter.async_shadow_fetch()


def test_contract_is_exact_ha_worker_mirror():
    ha = HA_CONTRACT.read_bytes()
    worker = WORKER_CONTRACT.read_bytes()
    assert ha == worker
    assert hashlib.sha256(ha).hexdigest() == hashlib.sha256(worker).hexdigest()


def test_shadow_adapter_is_not_registered_or_live_wired(provider):
    registry = _load("stv3_kmi_shadow_registry", REGISTRY)
    assert "kmi_radqpe" not in registry.IMPLEMENTED_RADAR_PROVIDERS
    assert "kmi_public_radar_composite" not in (
        registry.IMPLEMENTED_RADAR_PROVIDERS
    )
    assert "kmi_radqpe" not in POLICY.read_text(encoding="utf-8")
    assert "kmi_public_radar_composite" not in POLICY.read_text(
        encoding="utf-8"
    )
    init_source = HA_INIT.read_text(encoding="utf-8")
    assert "providers.kmi_radqpe" not in init_source
    assert "providers.kmi_public_radar_composite" not in init_source
    assert not hasattr(provider.KmiPublicRadarCompositeShadowAdapter, "plugin_id")
    assert not hasattr(
        provider.KmiPublicRadarCompositeShadowAdapter,
        "capabilities",
    )
    assert not hasattr(provider.KmiPublicRadarCompositeShadowAdapter, "priority")
    assert not any(
        name.endswith("Factory")
        for name in vars(provider)
    )
