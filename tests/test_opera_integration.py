"""Storm Tracker V3 — tests/test_opera_integration.py v0.1.0

Optionele smoke-test tegen de ECHTE OPERA S3-bucket.

Dit vult het gat dat de synthetische fixture niet kan dichten: de
unit-tests in test_opera.py/test_opera_provider.py bewijzen dat de
parsing-logica correct is GEGEVEN de structuur die wij aannemen
(groepnamen, attributen, sentinelwaarden). Ze bewijzen niet dat die
aanname klopt met een echt OPERA-product.

Draait NIET standaard mee (geen CI-afhankelijkheid van een externe
S3-bucket): wordt automatisch overgeslagen als de host niet bereikbaar
is. Forceer met: STV3_RUN_INTEGRATION_TESTS=1 pytest tests/test_opera_integration.py

Op de NUC (met normale internettoegang) kan dit gewoon periodiek
handmatig gedraaid worden om te verifiëren dat EUMETNET niets aan de
bestandsstructuur heeft veranderd.
"""
from __future__ import annotations

import asyncio
import os

import pytest


def _s3_reachable() -> bool:
    """
    Een kale TCP-connect is niet genoeg: sommige omgevingen (zoals de
    sandbox waarin dit geschreven is) laten de TCP-handshake toe maar
    blokkeren het HTTP-verzoek zelf op host-niveau (egress-allowlist).
    Daarom hier een echte HTTP-request, niet alleen een socket-connect.
    """
    try:
        import urllib.request
        req = urllib.request.Request(
            "https://s3.waw3-1.cloudferro.com/openradar-24h/?list-type=2&max-keys=1",
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception:
        return False


_FORCED = os.environ.get("STV3_RUN_INTEGRATION_TESTS") == "1"
_SKIP_REASON = (
    "Echte OPERA S3-bucket niet bereikbaar vanuit deze omgeving "
    "(verwacht in sandboxes/CI zonder internettoegang naar externe hosts). "
    "Draai dit bestand op een machine met internettoegang (bv. de NUC), "
    "of forceer met STV3_RUN_INTEGRATION_TESTS=1."
)


@pytest.mark.skipif(not (_FORCED or _s3_reachable()), reason=_SKIP_REASON)
def test_real_opera_file_has_expected_structure(opera_module):
    """
    Downloadt het nieuwste echte OPERA-bestand en controleert alleen de
    STRUCTUUR (keys/attributen/dtypes) — niet de inhoud. Dit is de test
    die zou falen als EUMETNET ooit iets aan het formaat verandert dat
    onze synthetische fixture niet zou opmerken.
    """
    import aiohttp
    import h5py
    import io

    async def _fetch_latest_real_file():
        async with aiohttp.ClientSession() as session:
            key = await opera_module._find_latest_valid_key(session)
            assert key is not None, "geen geldig (< 15 min oud) OPERA-product gevonden"
            data = await session.get(opera_module._s3_path_from_key(key))
            async with data as resp:
                assert resp.status == 200, f"download mislukt: HTTP {resp.status}"
                return await resp.read()

    raw = asyncio.run(_fetch_latest_real_file())
    assert len(raw) > 1_000_000, "een echt OPERA-composiet is normaliter >1MB"

    with h5py.File(io.BytesIO(raw), "r") as h5:
        assert "where" in h5, "verwachte group 'where' ontbreekt"
        for attr in ("projdef", "xsize", "ysize", "xscale", "yscale"):
            assert attr in h5["where"].attrs, f"verwacht attribuut 'where/{attr}' ontbreekt"

        assert "dataset1/data1/data" in h5, "verwacht dataset 'dataset1/data1/data' ontbreekt"
        assert "dataset1/data1/quality1/data" in h5, "verwacht dataset quality1/data ontbreekt"
        assert "dataset1/data1/what" in h5, "verwacht 'dataset1/data1/what' ontbreekt"
        for attr in ("nodata", "undetect", "gain", "offset"):
            assert attr in h5["dataset1/data1/what"].attrs, f"verwacht attribuut '{attr}' ontbreekt"

        radar_shape = h5["dataset1/data1/data"].shape
        quality_shape = h5["dataset1/data1/quality1/data"].shape
        assert radar_shape == quality_shape, "radar- en quality-array moeten dezelfde vorm hebben"
        # Real OPERA composiet is publiek gedocumenteerd als 4400x3800 —
        # als dit ooit afwijkt, is dat op zich geen falen van de code,
        # maar wel relevante info (EUMETNET kan het grid wijzigen).
        assert radar_shape[0] > 1000 and radar_shape[1] > 1000, (
            f"onverwacht klein grid: {radar_shape} — controleer of dit nog het volledige "
            f"Europese composiet is"
        )
