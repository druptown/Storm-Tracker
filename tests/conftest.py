"""Storm Tracker V3 — tests/conftest.py

Gedeelde test-infrastructuur.

Laadt de echte modules (`engine/observation.py`, `providers/base.py`,
`providers/opera.py`) rechtstreeks via `importlib`, met paden relatief
aan dit bestand (`__file__`) — dus portable op elk systeem/CI, in
tegenstelling tot de vorige versie die `/home/claude/stv3/...` hardcodete.

We voeren bewust NIET de echte `storm_tracker_v3/__init__.py` uit (die
vereist een volledige Home Assistant-installatie). In plaats daarvan
registreren we lichte stub-packages in `sys.modules` zodat de relatieve
imports in de geteste modules (`from ..engine.observation import ...`)
gewoon oplossen naar de echte, hier geladen module.

`engine/observation.py` en `providers/base.py`/`opera.py` hebben zelf
geen Home Assistant-afhankelijkheden, dus dit is voldoende.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

# Repository layout:
#   tests/conftest.py
#   custom_components/storm_tracker_v3/
REPO_ROOT = Path(__file__).resolve().parent.parent
PKG_ROOT = REPO_ROOT / "custom_components" / "storm_tracker_v3"
PKG_NAME = "storm_tracker_v3"


def _ensure_stub_package(name: str) -> types.ModuleType:
    """Registreer een leeg stub-package in sys.modules als het nog niet bestaat."""
    mod = sys.modules.get(name)
    if mod is None:
        mod = types.ModuleType(name)
        mod.__path__ = []  # markeert het als package voor relatieve imports
        sys.modules[name] = mod
    return mod


def _load_module(module_name: str, file_path: Path) -> types.ModuleType:
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def observation_module():
    """Laadt het echte engine/observation.py (geen HA-afhankelijkheden)."""
    _ensure_stub_package(PKG_NAME)
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.observation",
        PKG_ROOT / "engine" / "observation.py",
    )


@pytest.fixture(scope="session")
def base_module(observation_module):
    """Laadt het echte providers/base.py."""
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.base",
        PKG_ROOT / "providers" / "base.py",
    )


@pytest.fixture(scope="session")
def opera_module(observation_module):
    """Laadt het echte providers/opera.py."""
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.opera",
        PKG_ROOT / "providers" / "opera.py",
    )


@pytest.fixture()
def opera_fixture_file(tmp_path):
    """Bouwt het synthetische ODIM-bestand in een tijdelijke testdirectory."""
    sys.path.insert(0, str(REPO_ROOT / "tests" / "fixtures"))
    from make_opera_fixture import build_fixture

    path = tmp_path / "opera_fixture.h5"
    meta = build_fixture(str(path))
    meta["path"] = str(path)
    return meta


# ── Geometrie-modules (pure functies, geen HA-afhankelijkheden) ───────────

@pytest.fixture(scope="session")
def distance_module():
    _ensure_stub_package(PKG_NAME)
    _ensure_stub_package(f"{PKG_NAME}.geometry")
    return _load_module(
        f"{PKG_NAME}.geometry.distance",
        PKG_ROOT / "geometry" / "distance.py",
    )


@pytest.fixture(scope="session")
def bounding_box_module():
    _ensure_stub_package(f"{PKG_NAME}.geometry")
    return _load_module(
        f"{PKG_NAME}.geometry.bounding_box",
        PKG_ROOT / "geometry" / "bounding_box.py",
    )


@pytest.fixture(scope="session")
def hull_module(distance_module):
    """hull.py doet een lokale `from .distance import haversine` — dus distance_module eerst laden."""
    _ensure_stub_package(f"{PKG_NAME}.geometry")
    return _load_module(
        f"{PKG_NAME}.geometry.hull",
        PKG_ROOT / "geometry" / "hull.py",
    )


@pytest.fixture(scope="session")
def geocode_module():
    _ensure_stub_package(f"{PKG_NAME}.geometry")
    return _load_module(
        f"{PKG_NAME}.geometry.geocode",
        PKG_ROOT / "geometry" / "geocode.py",
    )


@pytest.fixture
def location_resolver_module():
    return _load_module(
        f"{PKG_NAME}.geometry.location_resolver",
        PKG_ROOT / "geometry" / "location_resolver.py",
    )


# ── Engine-modules ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def storm_module():
    """engine/storm.py heeft geen relatieve imports (alleen stdlib)."""
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.storm",
        PKG_ROOT / "engine" / "storm.py",
    )


@pytest.fixture(scope="session")
def trajectory_module():
    """Laadt de lichte adaptieve trajectschatter."""
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.trajectory",
        PKG_ROOT / "engine" / "trajectory.py",
    )


@pytest.fixture(scope="session")
def pressure_trend_module():
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.pressure_trend",
        PKG_ROOT / "engine" / "pressure_trend.py",
    )


@pytest.fixture(scope="session")
def nowcast_module():
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.nowcast",
        PKG_ROOT / "engine" / "nowcast.py",
    )


@pytest.fixture(scope="session")
def targets_module():
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.targets",
        PKG_ROOT / "engine" / "targets.py",
    )


@pytest.fixture(scope="session")
def geojson_module(hull_module):
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.geojson",
        PKG_ROOT / "engine" / "geojson.py",
    )


@pytest.fixture(scope="session")
def ofe_module(observation_module):
    """engine/observation_fusion_engine.py: `from .observation import ...`."""
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.observation_fusion_engine",
        PKG_ROOT / "engine" / "observation_fusion_engine.py",
    )


@pytest.fixture(scope="session")
def storm_engine_module(
    storm_module,
    trajectory_module,
    bounding_box_module,
    hull_module,
    geocode_module,
    observation_module,
):
    """
    engine/storm_engine.py importeert relatief: .storm, ..geometry.bounding_box,
    ..geometry.hull, ..geometry.geocode — alle vier moeten al in sys.modules
    staan (via de fixture-afhankelijkheden hierboven) vóór deze geladen wordt.
    """
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.storm_engine",
        PKG_ROOT / "engine" / "storm_engine.py",
    )


@pytest.fixture(scope="session")
def region_manager_module(storm_engine_module, ofe_module):
    return _load_module(
        f"{PKG_NAME}.engine.region_manager",
        PKG_ROOT / "engine" / "region_manager.py",
    )


# ── Overige providers ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def raster_components_module():
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.raster_components",
        PKG_ROOT / "providers" / "raster_components.py",
    )


@pytest.fixture(scope="session")
def kmi_module(observation_module, raster_components_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(f"{PKG_NAME}.providers.kmi", PKG_ROOT / "providers" / "kmi.py")


@pytest.fixture(scope="session")
def knmi_module(observation_module, raster_components_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(f"{PKG_NAME}.providers.knmi", PKG_ROOT / "providers" / "knmi.py")


@pytest.fixture(scope="session")
def rainviewer_module(observation_module, raster_components_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(f"{PKG_NAME}.providers.rainviewer", PKG_ROOT / "providers" / "rainviewer.py")


@pytest.fixture(scope="session")
def netatmo_module(observation_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(f"{PKG_NAME}.providers.netatmo", PKG_ROOT / "providers" / "netatmo.py")


@pytest.fixture(scope="session")
def blitzortung_module(observation_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(f"{PKG_NAME}.providers.blitzortung", PKG_ROOT / "providers" / "blitzortung.py")


@pytest.fixture(scope="session")
def eumetsat_li_module(observation_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.eumetsat_li",
        PKG_ROOT / "providers" / "eumetsat_li.py",
    )


@pytest.fixture(scope="session")
def noaa_goes_glm_module(observation_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.noaa_goes_glm",
        PKG_ROOT / "providers" / "noaa_goes_glm.py",
    )


@pytest.fixture(scope="session")
def dwd_radolan_module(observation_module, base_module, raster_components_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.dwd_radolan",
        PKG_ROOT / "providers" / "dwd_radolan.py",
    )


@pytest.fixture(scope="session")
def odim_hdf5_module(observation_module, raster_components_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.odim_hdf5",
        PKG_ROOT / "providers" / "odim_hdf5.py",
    )


@pytest.fixture(scope="session")
def met_office_radar_module(observation_module, base_module, odim_hdf5_module):
    return _load_module(
        f"{PKG_NAME}.providers.met_office_radar",
        PKG_ROOT / "providers" / "met_office_radar.py",
    )


@pytest.fixture(scope="session")
def meteofrance_radar_module(observation_module, base_module, odim_hdf5_module):
    return _load_module(
        f"{PKG_NAME}.providers.meteofrance_radar",
        PKG_ROOT / "providers" / "meteofrance_radar.py",
    )


@pytest.fixture(scope="session")
def meteolux_module(observation_module, base_module):
    return _load_module(
        f"{PKG_NAME}.providers.meteolux",
        PKG_ROOT / "providers" / "meteolux.py",
    )


@pytest.fixture(scope="session")
def geosphere_at_module(observation_module, base_module):
    return _load_module(
        f"{PKG_NAME}.providers.geosphere_at",
        PKG_ROOT / "providers" / "geosphere_at.py",
    )


@pytest.fixture(scope="session")
def italiameteo_module(observation_module, base_module):
    return _load_module(
        f"{PKG_NAME}.providers.italiameteo",
        PKG_ROOT / "providers" / "italiameteo.py",
    )


@pytest.fixture(scope="session")
def dpc_radar_module(observation_module, base_module, odim_hdf5_module, raster_components_module):
    return _load_module(
        f"{PKG_NAME}.providers.dpc_radar",
        PKG_ROOT / "providers" / "dpc_radar.py",
    )


@pytest.fixture(scope="session")
def aemet_radar_module(observation_module, base_module, raster_components_module):
    return _load_module(
        f"{PKG_NAME}.providers.aemet_radar",
        PKG_ROOT / "providers" / "aemet_radar.py",
    )


@pytest.fixture(scope="session")
def hsaf_h40b_module(
    observation_module, base_module, odim_hdf5_module, raster_components_module
):
    return _load_module(
        f"{PKG_NAME}.providers.hsaf_h40b",
        PKG_ROOT / "providers" / "hsaf_h40b.py",
    )


@pytest.fixture(scope="session")
def noaa_goes_rrqpe_module(
    observation_module, base_module, odim_hdf5_module, raster_components_module
):
    return _load_module(
        f"{PKG_NAME}.providers.noaa_goes_rrqpe",
        PKG_ROOT / "providers" / "noaa_goes_rrqpe.py",
    )


@pytest.fixture(scope="session")
def engine_radar_policy_module():
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.engine_radar_policy",
        PKG_ROOT / "providers" / "engine_radar_policy.py",
    )


@pytest.fixture(scope="session")
def open_meteo_module():
    """open_meteo.py heeft geen relatieve imports naar engine/observation."""
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(f"{PKG_NAME}.providers.open_meteo", PKG_ROOT / "providers" / "open_meteo.py")


@pytest.fixture(scope="session")
def radar_policy_module():
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(
        f"{PKG_NAME}.providers.radar_policy",
        PKG_ROOT / "providers" / "radar_policy.py",
    )


@pytest.fixture
def radar_calibration_module():
    """Laadt de passieve radar-autokalibratie zonder Home Assistant."""
    return _load_module(
        f"{PKG_NAME}.engine.radar_calibration",
        PKG_ROOT / "engine" / "radar_calibration.py",
    )


@pytest.fixture
def provider_bias_module():
    return _load_module(
        f"{PKG_NAME}.engine.provider_bias",
        PKG_ROOT / "engine" / "provider_bias.py",
    )


@pytest.fixture
def calibration_store_module(provider_bias_module):
    return _load_module(
        f"{PKG_NAME}.engine.calibration_store",
        PKG_ROOT / "engine" / "calibration_store.py",
    )
