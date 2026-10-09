"""Read-only KMI public-composite shadow transport.

This is intentionally not an operational provider:

* it has no ``plugin_id``, capabilities, priority or factory;
* it is absent from ``providers.registry`` and source-selection policy;
* it exposes only ``async_shadow_fetch``;
* returned public WCS values are never converted to observations.

The native KMI RADQPE transport remains externally blocked.  This adapter
captures the separate public ``belgian_rainfall_composite`` evidence set so
its product identity and semantics can be verified without losing live rain
events.
"""
from __future__ import annotations

import time
from typing import Callable

from ..engine.bounded_payload import iter_limited_async
from ..engine.kmi_radqpe_contract import (
    KmiPublicCompositeArtifactSet,
    KmiRadqpeContractError,
    MAX_CAPABILITIES_BYTES,
    MAX_COVERAGE_BYTES,
    MAX_DESCRIPTION_BYTES,
    PUBLIC_OWS_ENDPOINT,
    PUBLIC_SOURCE_ID,
    ShadowArtifact,
    audit_public_wcs_geotiff,
    format_utc,
    parse_public_wcs_description,
    parse_public_wms_capabilities,
    public_capabilities_request,
    public_coverage_request,
    public_description_request,
)


DEFAULT_TIMEOUT_SECONDS = 60
MAX_LIVE_AGE_SECONDS = 20 * 60
MAX_FUTURE_SKEW_SECONDS = 60
_HTTP_CHUNK_BYTES = 64 * 1024

_CAPABILITIES_MEDIA_TYPES = frozenset({
    "application/vnd.ogc.wms_xml",
    "application/xml",
    "text/xml",
})
_DESCRIPTION_MEDIA_TYPES = frozenset({"application/xml", "text/xml"})
_COVERAGE_MEDIA_TYPES = frozenset({"image/tiff"})


def _media_type(response) -> str:
    return (
        str(response.headers.get("Content-Type") or "")
        .split(";", 1)[0]
        .strip()
        .lower()
    )


async def _read_bounded_response(
    response,
    *,
    maximum_bytes: int,
    allowed_media_types: frozenset[str],
) -> tuple[bytes, str]:
    """Read one exact identity-encoded HTTP body under a byte/media contract."""

    if int(response.status) != 200:
        raise KmiRadqpeContractError(
            f"KMI public OWS antwoordde HTTP {response.status}"
        )
    media_type = _media_type(response)
    if media_type not in allowed_media_types:
        raise KmiRadqpeContractError(
            f"KMI public OWS media type {media_type!r} is niet toegestaan"
        )
    encoding = str(response.headers.get("Content-Encoding") or "identity")
    if encoding.lower() != "identity":
        raise KmiRadqpeContractError(
            "KMI public OWS gebruikte onverwachte transportcompressie"
        )
    payload = await iter_limited_async(
        response.content.iter_chunked(_HTTP_CHUNK_BYTES),
        maximum_bytes=maximum_bytes,
        declared_bytes=response.headers.get("Content-Length"),
        require_declared_match=True,
    )
    if not payload:
        raise KmiRadqpeContractError("KMI public OWS gaf een lege payload")
    return payload, media_type


class KmiPublicRadarCompositeShadowAdapter:
    """Fetch a complete public WMS/WCS artifact set; never emit weather."""

    def __init__(
        self,
        session,
        *,
        clock: Callable[[], float] = time.time,
        maximum_age_seconds: int = MAX_LIVE_AGE_SECONDS,
    ) -> None:
        if session is None:
            raise TypeError("een aiohttp-compatibele sessie is vereist")
        if (
            isinstance(maximum_age_seconds, bool)
            or not isinstance(maximum_age_seconds, int)
            or maximum_age_seconds <= 0
        ):
            raise ValueError("maximum_age_seconds moet een positieve integer zijn")
        self._session = session
        self._clock = clock
        self._maximum_age_seconds = maximum_age_seconds
        self.diagnostics: dict = {
            "source": PUBLIC_SOURCE_ID,
            "status": "not_fetched",
            "shadow_only": True,
            "detection_eligible": False,
            "retention_eligible": False,
        }

    async def _get(
        self,
        params: dict[str, str],
        *,
        maximum_bytes: int,
        allowed_media_types: frozenset[str],
    ) -> tuple[bytes, str, float]:
        headers = {
            "Accept": ", ".join(sorted(allowed_media_types)),
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerV3-KMI-public-composite-shadow/1.0",
        }
        async with self._session.get(
            PUBLIC_OWS_ENDPOINT,
            params=params,
            headers=headers,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        ) as response:
            payload, media_type = await _read_bounded_response(
                response,
                maximum_bytes=maximum_bytes,
                allowed_media_types=allowed_media_types,
            )
            return payload, media_type, float(self._clock())

    async def async_shadow_fetch(self) -> KmiPublicCompositeArtifactSet:
        """Fetch and audit exact current artifacts without operational output."""

        capabilities, capabilities_type, capabilities_at = await self._get(
            public_capabilities_request(),
            maximum_bytes=MAX_CAPABILITIES_BYTES,
            allowed_media_types=_CAPABILITIES_MEDIA_TYPES,
        )
        catalog = parse_public_wms_capabilities(capabilities)
        reference_now = float(self._clock())
        age_seconds = reference_now - catalog.default_at
        if age_seconds < -MAX_FUTURE_SKEW_SECONDS:
            raise KmiRadqpeContractError(
                "KMI public composiettijd ligt te ver in de toekomst"
            )
        if age_seconds > self._maximum_age_seconds:
            raise KmiRadqpeContractError(
                "KMI public composiet is ouder dan het shadow-livecontract"
            )

        description_payload, description_type, description_at = await self._get(
            public_description_request(),
            maximum_bytes=MAX_DESCRIPTION_BYTES,
            allowed_media_types=_DESCRIPTION_MEDIA_TYPES,
        )
        description = parse_public_wcs_description(description_payload)
        nominal_at = catalog.default_at
        if not (
            description.start_at <= nominal_at <= description.end_at
        ):
            raise KmiRadqpeContractError(
                "WMS-standaardtijd ontbreekt uit WCS-beschrijving"
            )
        if (
            description.width <= 0
            or description.height <= 0
            or description.cadence_seconds != catalog.cadence_seconds
        ):
            raise KmiRadqpeContractError("WMS en WCS spreken elkaar tegen")

        coverage_payload, coverage_type, coverage_at = await self._get(
            public_coverage_request(nominal_at),
            maximum_bytes=MAX_COVERAGE_BYTES,
            allowed_media_types=_COVERAGE_MEDIA_TYPES,
        )
        geotiff = audit_public_wcs_geotiff(
            coverage_payload,
            description,
        )

        artifacts = (
            ShadowArtifact(
                role="wms_capabilities",
                source_record_id=(
                    "wms-capabilities-" + catalog.payload_sha256
                ),
                payload=capabilities,
                media_type=capabilities_type,
                fetched_at=capabilities_at,
                nominal_at=None,
            ),
            ShadowArtifact(
                role="wcs_description",
                source_record_id=(
                    "wcs-description-" + description.payload_sha256
                ),
                payload=description_payload,
                media_type=description_type,
                fetched_at=description_at,
                nominal_at=None,
            ),
            ShadowArtifact(
                role="wcs_coverage",
                source_record_id=(
                    "wcs-coverage-"
                    + format_utc(nominal_at)
                    .replace("-", "")
                    .replace(":", "")
                    .lower()
                ),
                payload=coverage_payload,
                media_type=coverage_type,
                fetched_at=coverage_at,
                nominal_at=nominal_at,
            ),
        )
        result = KmiPublicCompositeArtifactSet(
            nominal_at=nominal_at,
            catalog=catalog,
            description=description,
            geotiff=geotiff,
            artifacts=artifacts,
        )
        self.diagnostics = {
            "source": PUBLIC_SOURCE_ID,
            "status": "captured_semantics_blocked",
            "shadow_only": True,
            "nominal_at": format_utc(nominal_at),
            "age_seconds": round(max(0.0, age_seconds), 3),
            "artifact_sha256": {
                artifact.role: artifact.content_sha256
                for artifact in result.artifacts
            },
            "geotiff_shape": [geotiff.height, geotiff.width],
            "geotiff_epsg": geotiff.epsg,
            "semantic_blockers": list(geotiff.semantic_blockers),
            "exact_raw_artifacts_complete_in_memory":
                result.exact_raw_artifacts_complete_in_memory,
            "exact_raw_artifacts_durably_committed":
                result.exact_raw_artifacts_durably_committed,
            "detection_eligible": result.detection_eligible,
            "retention_eligible": result.retention_eligible,
        }
        return result


__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "KmiPublicRadarCompositeShadowAdapter",
    "MAX_FUTURE_SKEW_SECONDS",
    "MAX_LIVE_AGE_SECONDS",
]
