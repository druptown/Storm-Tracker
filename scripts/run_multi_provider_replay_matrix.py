"""Run the inert real-fixture multi-provider replay matrix.

The script has no network path and never imports worker service, HA, MQTT or
snapshot publication.  It requires a new output directory so an earlier
durable receipt set can never be mistaken for a fresh resource run.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from storm_tracker_worker.multi_provider_replay_matrix import (
    build_report,
    load_houston_rainviewer_sequences,
    load_kmi_public_composite_sequence,
    probe_knmi_authority_golden,
    probe_metadata_golden,
    probe_noaa_err_golden,
    probe_opera_golden,
    probe_rainviewer_raw_golden,
    run_categorical_sequence,
    write_report,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--maximum-analysis-pixels", type=int, default=1_500)
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    root = arguments.repository_root.resolve(strict=True)
    output = arguments.output_directory.resolve(strict=False)
    if output.exists() and any(output.iterdir()):
        raise RuntimeError("output directory must be new or empty")
    output.mkdir(parents=True, exist_ok=True)

    kmi_source = root / ".analysis" / "kmi_public_radar_composite_20260730_sequence_20"
    houston_source = root / ".analysis" / "houston_motion_replay.json"
    kmi = load_kmi_public_composite_sequence(kmi_source)
    houston = load_houston_rainviewer_sequences(houston_source)
    sequence_results = [
        run_categorical_sequence(
            kmi,
            source_path=kmi_source,
            receipt_path=output / "receipts" / "kmi.sqlite3",
            maximum_analysis_pixels=arguments.maximum_analysis_pixels,
        )
    ]
    sequence_results.extend(
        run_categorical_sequence(
            sequence,
            source_path=houston_source,
            receipt_path=output / "receipts" / f"houston-{index}.sqlite3",
            maximum_analysis_pixels=arguments.maximum_analysis_pixels,
        )
        for index, sequence in enumerate(houston, start=1)
    )

    fixtures = root / "tests" / "fixtures"
    single_results = [
        probe_noaa_err_golden(
            root / ".analysis" / "noaa-err-live-golden",
            output / "single-frame" / "noaa-err",
        ),
        probe_opera_golden(
            root / ".analysis" / "opera-native-live-golden-20260802T0045",
            output / "single-frame" / "opera",
        ),
        probe_knmi_authority_golden(
            fixtures / "knmi_rtcor_analysis_grid_authority.json"
        ),
        probe_rainviewer_raw_golden(
            root / ".analysis" / "rainviewer-live-golden-v2"
        ),
    ]
    for provider, filename in (
        ("dpc_sri", "dpc_sri_v2_live_identity_profile.json"),
        ("dwd_rv", "dwd_rv_live_evidence.json"),
        ("hsaf_h40b", "hsaf_h40b_official_contract.json"),
        ("met_office_uk", "met_office_uk_odim_golden.json"),
        ("meteofrance_dpradar", "meteofrance_dpradar_official_catalog_example.json"),
    ):
        single_results.append(probe_metadata_golden(provider, fixtures / filename))

    report = build_report(sequence_results, single_results)
    report_path = write_report(report, output / "report.json")
    print(json.dumps({
        "admission": False,
        "houston_frames": sum(
            item.frame_count for item in sequence_results
            if item.provider == "rainviewer_houston"
        ),
        "kmi_frames": sequence_results[0].frame_count,
        "report_path": str(report_path),
        "report_sha256": report.report_sha256,
        "sequence_count": len(sequence_results),
        "single_frame_probe_count": len(single_results),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
