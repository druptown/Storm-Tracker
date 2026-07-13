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
def ofe_module(observation_module):
    """engine/observation_fusion_engine.py: `from .observation import ...`."""
    _ensure_stub_package(f"{PKG_NAME}.engine")
    return _load_module(
        f"{PKG_NAME}.engine.observation_fusion_engine",
        PKG_ROOT / "engine" / "observation_fusion_engine.py",
    )


@pytest.fixture(scope="session")
def storm_engine_module(storm_module, bounding_box_module, hull_module, geocode_module, observation_module):
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


# ── Overige providers ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def kmi_module(observation_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(f"{PKG_NAME}.providers.kmi", PKG_ROOT / "providers" / "kmi.py")


@pytest.fixture(scope="session")
def knmi_module(observation_module):
    _ensure_stub_package(f"{PKG_NAME}.providers")
    return _load_module(f"{PKG_NAME}.providers.knmi", PKG_ROOT / "providers" / "knmi.py")


@pytest.fixture(scope="session")
def rainviewer_module(observation_module):
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
