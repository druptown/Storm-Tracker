"""Capture exact public KMI radar-composite evidence without live integration.

The public GeoServer layer is deliberately named ``kmi_public_radar_composite``
here.  KMI's public metadata does not currently prove that it is the native
RADQPE rate product or declare trustworthy precipitation units.

This script performs read-only HTTPS GETs, enforces byte limits and writes one
immutable local evidence bundle.  It never contacts Home Assistant and never
changes provider selection.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import urllib.parse
import urllib.request
from xml.etree import ElementTree

from PIL import Image


OWS_ENDPOINT = "https://opendata.meteo.be/geoserver/ows"
LAYER_NAME = "radar:belgian_rainfall_composite"
COVERAGE_ID = "radar__belgian_rainfall_composite"
MAX_CAPABILITIES_BYTES = 1 * 1024 * 1024
MAX_DESCRIPTION_BYTES = 1 * 1024 * 1024
MAX_COVERAGE_BYTES = 16 * 1024 * 1024
USER_AGENT = "StormTrackerV3-KMI-public-composite-audit/1.0"
_DURATION_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")


def _utc_iso(timestamp: float) -> str:
    return (
        datetime.fromtimestamp(timestamp, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _parse_utc(value: str) -> float:
    if not value.endswith("Z"):
        raise ValueError(f"timestamp is niet expliciet UTC: {value!r}")
    return datetime.fromisoformat(value[:-1] + "+00:00").timestamp()


def _duration_seconds(value: str) -> int:
    match = _DURATION_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"niet-ondersteunde ISO-8601-duur: {value!r}")
    hours, minutes, seconds = (int(item or 0) for item in match.groups())
    total = hours * 3600 + minutes * 60 + seconds
    if total <= 0:
        raise ValueError("tijdinterval moet positief zijn")
    return total


def _fetch(
    params: dict[str, str],
    *,
    maximum_bytes: int,
    allowed_media_types: frozenset[str],
) -> tuple[bytes, dict[str, object]]:
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{OWS_ENDPOINT}?{query}",
        headers={
            "Accept": ", ".join(sorted(allowed_media_types)),
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        },
    )
    fetched_at = datetime.now(timezone.utc)
    with urllib.request.urlopen(request, timeout=60) as response:
        media_type = (
            str(response.headers.get("Content-Type") or "")
            .split(";", 1)[0]
            .strip()
            .lower()
        )
        if media_type not in allowed_media_types:
            raise ValueError(
                f"onverwacht media type {media_type!r}; "
                f"verwacht {sorted(allowed_media_types)}"
            )
        encoding = str(response.headers.get("Content-Encoding") or "identity")
        if encoding.lower() != "identity":
            raise ValueError(f"onverwachte transportcompressie: {encoding}")
        declared_text = response.headers.get("Content-Length")
        declared = int(declared_text) if declared_text is not None else None
        if declared is not None and declared > maximum_bytes:
            raise ValueError("opgegeven payloadlengte overschrijdt contractlimiet")
        payload = response.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise ValueError("payload overschrijdt contractlimiet")
        if declared is not None and len(payload) != declared:
            raise ValueError("payloadlengte wijkt af van Content-Length")
        receipt = {
            "endpoint": OWS_ENDPOINT,
            "request_fields": dict(sorted(params.items())),
            "status": int(response.status),
            "media_type": media_type,
            "content_encoding": encoding.lower(),
            "content_length": declared,
            "received_bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "fetched_at": fetched_at.isoformat().replace("+00:00", "Z"),
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
        }
        return payload, receipt


def _layer_time_domain(payload: bytes) -> dict[str, object]:
    root = ElementTree.fromstring(payload)

    def local(element) -> str:
        return element.tag.rsplit("}", 1)[-1]

    target = None
    for layer in (item for item in root.iter() if local(item) == "Layer"):
        name = next(
            (
                (child.text or "").strip()
                for child in layer
                if local(child) == "Name"
            ),
            "",
        )
        if name in {LAYER_NAME, LAYER_NAME.split(":", 1)[1]}:
            target = layer
            break
    if target is None:
        raise ValueError(f"WMS-laag {LAYER_NAME!r} ontbreekt")
    dimension = next(
        (
            child
            for child in target
            if local(child) in {"Dimension", "Extent"}
            and child.attrib.get("name") == "time"
        ),
        None,
    )
    if dimension is None or not (dimension.text or "").strip():
        raise ValueError("WMS-tijddimensie ontbreekt")
    parts = (dimension.text or "").strip().split("/")
    if len(parts) != 3:
        raise ValueError("WMS-tijddimensie is geen expliciet interval")
    start, end = (_parse_utc(parts[index]) for index in (0, 1))
    interval = _duration_seconds(parts[2])
    default_text = dimension.attrib.get("default") or parts[1]
    default = _parse_utc(default_text)
    if interval != 300:
        raise ValueError(f"onverwacht WMS-interval: {interval} seconden")
    if not (start <= default <= end):
        raise ValueError("standaardtijd valt buiten WMS-tijddomein")
    return {
        "start": _utc_iso(start),
        "end": _utc_iso(end),
        "default": _utc_iso(default),
        "interval_seconds": interval,
        "_start_timestamp": start,
        "_end_timestamp": end,
    }


def _available_times(domain: dict[str, object]) -> tuple[float, ...]:
    start = float(domain["_start_timestamp"])
    end = float(domain["_end_timestamp"])
    interval = int(domain["interval_seconds"])
    count = int(round((end - start) / interval))
    return tuple(start + index * interval for index in range(count + 1))


def _tiff_audit(payload: bytes) -> dict[str, object]:
    with Image.open(io.BytesIO(payload)) as image:
        image.load()
        if image.format != "TIFF":
            raise ValueError(f"coverage is geen TIFF maar {image.format!r}")
        if len(image.getbands()) != 1:
            raise ValueError("coverage is geen enkelbandsraster")
        extrema = image.getextrema()
        tags = image.tag_v2
        geokeys = tuple(int(value) for value in tags.get(34735, ()))
        return {
            "format": image.format,
            "mode": image.mode,
            "width": int(image.width),
            "height": int(image.height),
            "bands": list(image.getbands()),
            "extrema": [float(extrema[0]), float(extrema[1])],
            "has_model_transformation": bool(tags.get(34264)),
            "has_model_tiepoint": bool(tags.get(33922)),
            "has_model_pixel_scale": bool(tags.get(33550)),
            "has_gdal_nodata": bool(tags.get(42113)),
            "geokey_directory": list(geokeys),
        }


def _write_new(path: Path, payload: bytes) -> None:
    if path.exists():
        raise FileExistsError(f"evidencebestand bestaat al: {path}")
    path.write_bytes(payload)


def capture(
    output_directory: Path,
    *,
    required_time: str,
    later_frames: int,
    latest_sequence_count: int | None = None,
) -> Path:
    if output_directory.exists() and any(output_directory.iterdir()):
        raise FileExistsError(
            f"outputdirectory is niet leeg: {output_directory.resolve()}"
        )
    output_directory.mkdir(parents=True, exist_ok=True)

    capabilities_params = {
        "SERVICE": "WMS",
        "VERSION": "1.3.0",
        "REQUEST": "GetCapabilities",
    }
    capabilities, capabilities_receipt = _fetch(
        capabilities_params,
        maximum_bytes=MAX_CAPABILITIES_BYTES,
        allowed_media_types=frozenset({
            "application/vnd.ogc.wms_xml",
            "application/xml",
            "text/xml",
        }),
    )
    domain = _layer_time_domain(capabilities)

    description_params = {
        "SERVICE": "WCS",
        "VERSION": "2.0.1",
        "REQUEST": "DescribeCoverage",
        "COVERAGEID": COVERAGE_ID,
    }
    description, description_receipt = _fetch(
        description_params,
        maximum_bytes=MAX_DESCRIPTION_BYTES,
        allowed_media_types=frozenset({"application/xml", "text/xml"}),
    )

    available = _available_times(domain)
    if latest_sequence_count is not None:
        if latest_sequence_count < 3:
            raise ValueError("laatste reeks moet minstens 3 frames bevatten")
        if len(available) < latest_sequence_count:
            raise ValueError(
                f"slechts {len(available)} frames beschikbaar; "
                f"{latest_sequence_count} vereist"
            )
        chosen = available[-latest_sequence_count:]
        capture_mode = {
            "mode": "latest_contiguous_sequence",
            "frame_count": latest_sequence_count,
        }
    else:
        required_timestamp = _parse_utc(required_time)
        if required_timestamp not in available:
            raise ValueError(
                f"vereist frame {required_time} ontbreekt uit het actuele "
                "WMS-domein"
            )
        later = tuple(
            value for value in available if value > required_timestamp
        )
        if len(later) < later_frames:
            raise ValueError(
                f"slechts {len(later)} latere frames beschikbaar; "
                f"{later_frames} vereist"
            )
        chosen = (required_timestamp, *later[-later_frames:])
        capture_mode = {
            "mode": "required_frame_plus_latest_later_frames",
            "required_time": required_time,
            "later_frame_count": later_frames,
        }

    _write_new(output_directory / "wms_capabilities.xml", capabilities)
    _write_new(output_directory / "wcs_description.xml", description)

    frame_receipts: list[dict[str, object]] = []
    for nominal_timestamp in chosen:
        nominal = _utc_iso(nominal_timestamp)
        coverage_params = {
            "SERVICE": "WCS",
            "VERSION": "2.0.1",
            "REQUEST": "GetCoverage",
            "COVERAGEID": COVERAGE_ID,
            "FORMAT": "image/tiff",
            "SUBSET": f'time("{nominal}")',
        }
        payload, receipt = _fetch(
            coverage_params,
            maximum_bytes=MAX_COVERAGE_BYTES,
            allowed_media_types=frozenset({"image/tiff"}),
        )
        filename = (
            "kmi_public_radar_composite_"
            + nominal.replace("-", "").replace(":", "").replace("T", "_")
            + ".tif"
        )
        _write_new(output_directory / filename, payload)
        frame_receipts.append({
            "role": "public_wcs_coverage",
            "local_file": filename,
            "nominal_at": nominal,
            "transport": receipt,
            "tiff_audit": _tiff_audit(payload),
            "meteorological_semantics": {
                "source_identity": "UNVERIFIED_AS_RADQPE",
                "quantity": "UNKNOWN",
                "unit": "UNKNOWN",
                "nodata": "UNKNOWN",
                "detection_eligible": False,
            },
        })

    public_domain = {
        key: value
        for key, value in domain.items()
        if not key.startswith("_")
    }
    bundle = {
        "schema": "kmi-public-radar-composite-capture-v1",
        "created_at": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "source_label": "kmi_public_radar_composite",
        "explicitly_not_claimed_as": "RADQPE",
        "live_integration_changed": False,
        "home_assistant_contacted": False,
        "capture_mode": capture_mode,
        "public_time_domain": public_domain,
        "artifacts": [
            {
                "role": "wms_capabilities",
                "local_file": "wms_capabilities.xml",
                "transport": capabilities_receipt,
            },
            {
                "role": "wcs_description",
                "local_file": "wcs_description.xml",
                "transport": description_receipt,
            },
            *frame_receipts,
        ],
        "gates": {
            "product_identity_verified_as_radqpe": False,
            "precipitation_quantity_verified": False,
            "precipitation_unit_verified": False,
            "nodata_semantics_verified": False,
            "retention_and_value_added_use_reviewed": False,
            "operational_cutover_allowed": False,
        },
    }
    receipt_path = output_directory / "fetch_receipt.json"
    _write_new(
        receipt_path,
        (
            json.dumps(bundle, indent=2, ensure_ascii=False, sort_keys=True)
            + "\n"
        ).encode("utf-8"),
    )
    return receipt_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Nieuwe of lege evidence-directory",
    )
    parser.add_argument(
        "--required-time",
        default="2026-07-30T12:20:00Z",
    )
    parser.add_argument(
        "--later-frames",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--latest-sequence-count",
        type=int,
        help=(
            "Leg de laatste N aaneengesloten vijfminutenframes vast; "
            "negeert --required-time en --later-frames"
        ),
    )
    args = parser.parse_args()
    if args.latest_sequence_count is None and args.later_frames < 2:
        parser.error("--later-frames moet minstens 2 zijn")
    if (
        args.latest_sequence_count is not None
        and args.latest_sequence_count < 3
    ):
        parser.error("--latest-sequence-count moet minstens 3 zijn")
    receipt = capture(
        args.output,
        required_time=args.required_time,
        later_frames=args.later_frames,
        latest_sequence_count=args.latest_sequence_count,
    )
    print(receipt.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
