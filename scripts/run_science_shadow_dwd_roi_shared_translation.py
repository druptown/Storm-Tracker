"""Seal the read-only 35-interval DWD shared-translation evidence."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.science_shadow_dwd_roi_replay import (  # noqa: E402
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    load_dwd_roi_science_replay_export,
)
from storm_tracker_worker.science_shadow_dwd_roi_shared_translation import (  # noqa: E402
    EXPECTED_FROZEN_BASELINE_FILENAME,
    DwdRoiSharedTranslationError,
    load_frozen_dwd_roi_phase_baseline,
    render_dwd_roi_shared_translation_markdown,
    run_dwd_roi_shared_translation_replay,
    verify_dwd_roi_shared_translation_document,
)


EXPORT_ROOT = ROOT / ".analysis" / "dwd-rv-germany-roi-2km-36-export-v1"
SEQUENCE_MANIFEST = (
    EXPORT_ROOT
    / "manifests"
    / f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json"
)
FROZEN_BASELINE = ROOT / ".analysis" / EXPECTED_FROZEN_BASELINE_FILENAME
OUTPUT_JSON = (
    ROOT
    / ".analysis"
    / "dwd_rv_germany_roi_2km_shared_system_translation_20260808_evidence.json"
)
OUTPUT_MARKDOWN = OUTPUT_JSON.with_suffix(".md")


def _atomic_write_new(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    if path.exists() or temporary.exists():
        raise DwdRoiSharedTranslationError("artifact target is not fresh")
    with temporary.open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _load_existing_document(path: Path) -> tuple[bytes, dict[str, object]]:
    body = path.read_bytes()
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DwdRoiSharedTranslationError("existing artifact is invalid JSON") from exc
    if not isinstance(document, dict):
        raise DwdRoiSharedTranslationError("existing artifact is not a mapping")
    verify_dwd_roi_shared_translation_document(document)
    return body, document


def main() -> int:
    if OUTPUT_MARKDOWN.exists() and not OUTPUT_JSON.exists():
        raise DwdRoiSharedTranslationError("markdown exists without JSON authority")
    if OUTPUT_JSON.exists():
        body, document = _load_existing_document(OUTPUT_JSON)
    else:
        export = load_dwd_roi_science_replay_export(
            EXPORT_ROOT.resolve(), SEQUENCE_MANIFEST.resolve()
        )
        baseline = load_frozen_dwd_roi_phase_baseline(FROZEN_BASELINE.resolve())
        receipt = run_dwd_roi_shared_translation_replay(export, baseline)
        body = receipt.canonical_body()
        document = dict(receipt.document)
        _atomic_write_new(OUTPUT_JSON, body)
    json_sha = hashlib.sha256(body).hexdigest()
    markdown = render_dwd_roi_shared_translation_markdown(
        document, json_sha256=json_sha
    )
    if OUTPUT_MARKDOWN.exists():
        if OUTPUT_MARKDOWN.read_bytes() != markdown:
            raise DwdRoiSharedTranslationError("existing markdown bytes differ")
    else:
        _atomic_write_new(OUTPUT_MARKDOWN, markdown)
    print(json.dumps({
        "admission_granted": document["admission_granted"],
        "interval_count": document["interval_count"],
        "json": str(OUTPUT_JSON),
        "json_sha256": json_sha,
        "markdown": str(OUTPUT_MARKDOWN),
        "markdown_sha256": hashlib.sha256(markdown).hexdigest(),
        "scientific_payload_sha256": document["scientific_payload_sha256"],
        "summary": document["summary"],
    }, allow_nan=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
