"""Real ``os._exit(73)`` harness for two exact-native capture candidates."""
from __future__ import annotations

import os
from pathlib import Path
import runpy
import sys

from storm_tracker_worker import hsaf_h40b_native
from storm_tracker_worker import meteofrance_native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


EXIT_CODE = 73
REPO_ROOT = Path(__file__).resolve().parents[2]


def _crash_at(expected: str):
    def crash(checkpoint: str) -> None:
        if checkpoint == expected:
            os._exit(EXIT_CODE)

    return crash


def _capture_hsaf_h40b(raw_root: Path, checkpoint: str) -> None:
    allowed = {
        "hsaf_h40b.after_listing_store",
        "hsaf_h40b.after_product_store",
        "hsaf_h40b.before_capture_commit",
        "hsaf_h40b.after_capture_commit",
    }
    if checkpoint not in allowed:
        raise SystemExit("unsupported H SAF H40B checkpoint")
    fixtures = runpy.run_path(
        str(REPO_ROOT / "tests" / "test_worker_hsaf_h40b_native_capture.py")
    )
    product = fixtures["_netcdf_gzip"]()
    response = fixtures["_response"]
    credentials = hsaf_h40b_native.HsafCredentials(
        fixtures["USERNAME"], fixtures["PASSWORD"]
    )

    hsaf_h40b_native._credentials_from_environment = lambda: credentials
    hsaf_h40b_native._fetch_listing = lambda _credentials, *, deadline: response(
        fixtures["LISTING"], fixtures["LATEST_TIME"] + 60
    )
    hsaf_h40b_native._fetch_product = (
        lambda _credentials, _remote_path, *, deadline: response(
            product,
            fixtures["LATEST_TIME"] + 61,
            declared_size=len(product),
        )
    )
    hsaf_h40b_native.capture_latest_hsaf_h40b(
        fixtures["_job"](),
        raw_store=RawArtifactStore(raw_root),
        _fault_injector=_crash_at(checkpoint),
    )


def _capture_meteofrance(raw_root: Path, checkpoint: str) -> None:
    allowed = {
        "meteofrance.after_catalog_store",
        "meteofrance.after_hdf5_store",
        "meteofrance.before_capture_commit",
        "meteofrance.after_capture_commit",
    }
    if checkpoint not in allowed:
        raise SystemExit("unsupported Meteo-France checkpoint")
    fixtures = runpy.run_path(
        str(REPO_ROOT / "tests" / "test_worker_meteofrance_native_capture.py")
    )
    hdf5 = fixtures["_hdf5"]()
    response = fixtures["_response"]

    meteofrance_native._token_from_environment = (
        lambda *, deadline: fixtures["TOKEN"]
    )
    meteofrance_native._fetch_catalog = lambda _token, *, deadline: response(
        fixtures["OFFICIAL_EXAMPLE"],
        "application/json",
        fixtures["VALIDITY"] + 60,
    )
    meteofrance_native._fetch_product = lambda _token, *, deadline: response(
        hdf5,
        "application/x-hdf",
        fixtures["VALIDITY"] + 61,
        disposition=f'attachment; filename="{fixtures["FILENAME"]}"',
    )
    meteofrance_native.capture_latest_meteofrance_hdf5(
        fixtures["_job"](),
        raw_store=RawArtifactStore(raw_root),
        _fault_injector=_crash_at(checkpoint),
    )


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: child PROVIDER RAW_ROOT CHECKPOINT")
    provider = sys.argv[1]
    raw_root = Path(sys.argv[2])
    checkpoint = sys.argv[3]
    if provider == "hsaf_h40b":
        _capture_hsaf_h40b(raw_root, checkpoint)
    elif provider == "meteofrance":
        _capture_meteofrance(raw_root, checkpoint)
    else:
        raise SystemExit("unsupported provider")
    raise SystemExit("crash checkpoint was not reached")


if __name__ == "__main__":
    main()
