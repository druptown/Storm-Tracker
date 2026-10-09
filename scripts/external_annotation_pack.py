"""CLI for read-only external annotation-pack export and strict ingest."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from storm_tracker_worker.external_annotation_pack import (  # noqa: E402
    annotation_pack_from_dict,
    annotation_pack_request_from_dict,
    empty_label_template_document,
    export_annotation_pack,
    ingest_annotation_submission,
)


def _read_json(path_value: str) -> dict[str, Any]:
    path = Path(path_value).expanduser().resolve(strict=True)
    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise TypeError(f"JSON-root moet object zijn: {path}")
    return value


def _write_new_json(path_value: str, value: Any) -> Path:
    path = Path(path_value).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    )
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(rendered)
        stream.write("\n")
    return path


def _export(args: argparse.Namespace) -> None:
    request = annotation_pack_request_from_dict(_read_json(args.request))
    pack = export_annotation_pack(args.database, request)
    pack_path = _write_new_json(args.pack_out, pack.to_dict())
    try:
        template_path = _write_new_json(
            args.template_out,
            empty_label_template_document(pack),
        )
    except BaseException:
        # Keep the already written immutable pack; never overwrite/delete
        # caller files during a failed second output.
        raise
    print(
        json.dumps(
            {
                "annotation_pack_sha256": pack.sha256,
                "pack": str(pack_path),
                "template": str(template_path),
                "evidence_reference_count": len(pack.evidence_references),
                "missing_evidence_count": len(pack.missing_evidence),
                "label_count": 0,
            },
            sort_keys=True,
        )
    )


def _ingest(args: argparse.Namespace) -> None:
    pack = annotation_pack_from_dict(_read_json(args.pack))
    result = ingest_annotation_submission(
        pack,
        _read_json(args.submission),
    )
    output = _write_new_json(args.output, result.to_dict())
    print(
        json.dumps(
            {
                "annotation_pack_sha256": pack.sha256,
                "annotation_ingest_sha256": result.sha256,
                "corpus_sha256": result.corpus.sha256,
                "labels": len(result.corpus.labels),
                "unresolved": len(result.unresolved_template_ids),
                "output": str(output),
            },
            sort_keys=True,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export empty, leakage-safe external annotation packs from an "
            "immutable read-only SQLite snapshot, or strictly ingest one."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    export_parser = subparsers.add_parser("export")
    export_parser.add_argument("--database", required=True)
    export_parser.add_argument("--request", required=True)
    export_parser.add_argument("--pack-out", required=True)
    export_parser.add_argument("--template-out", required=True)
    export_parser.set_defaults(handler=_export)

    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("--pack", required=True)
    ingest_parser.add_argument("--submission", required=True)
    ingest_parser.add_argument("--output", required=True)
    ingest_parser.set_defaults(handler=_ingest)

    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
