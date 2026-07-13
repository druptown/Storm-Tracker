# Storm Tracker V3 — Volgende stappen

**Datum:** 12 juli 2026  
**Huidige versie:** v0.4.2 (gedeployed en werkend)  
**Vorige chat transcript:** zie journal.txt in /mnt/transcripts/

---

## Huidige status

| Sensor | Status |
|---|---|
| `sensor.stv3_opera_observaties` | ✅ 343 cellen (heel Europa) |
| `sensor.stv3_actieve_storms` | ✅ 15 storms |
| `sensor.stv3_dichtstbijzijnde_storm` | ✅ 848km (Noord-Schotland) |
| `sensor.stv3_rainviewer_observaties` | ✅ 333 |
| `sensor.stv3_blitzortung_inslagen` | ✅ actief |
| `sensor.stv3_netatmo_stations` | ✅ 454 stations |
| `sensor.stv3_knmi_intensiteit_nu` | ✅ 0 (droog) |
| `sensor.stv3_kmi_observaties` | ✅ 0 (droog) |

---

## Drie blokkers die eerst opgelost moeten worden

### Blokker 1 — OPERA verwerkt heel Europa (KRITISCH)

**Probleem:**
- `OperaProvider._bbox()` retourneert het volledige OPERA dekkingsgebied
- Dit verwerkt ~16.7 miljoen radarwaarden (~268 MB) per poll
- Alle Europese storms gaan naar één StormEngine → MAX_STORMS vol met verre storms
- Lokale Belgische cel kan genegeerd worden

**Fix:**
- OPERA bbox beperken tot observation horizon van de CoverageArea (200km rond tracker)
- Eén gedeelde HDF5 download cachen, meerdere slices mogelijk per RegionEngine

### Blokker 2 — S3 discovery niet betrouwbaar (KRITISCH)

**Probleem:**
- `max-keys=300` mist het nieuwste bestand later op de dag
- `_download()` gebruikt huidige UTC datum i.p.v. datum uit bestandsnaam
- Rond middernacht: verkeerd dagpad → 404 errors
- Geen validatie of product recent genoeg is

**Fix:**
- S3 paginering via `IsTruncated` en `NextContinuationToken`
- Volledige S3-key bewaren, datum uit bestandsnaam afleiden
- Productleeftijd valideren: verwerp data ouder dan 15 minuten
- Middernacht: check ook gisteren

### Blokker 3 — Alle radarproviders tegelijk actief (KRITISCH)

**Probleem:**
- OPERA, KMI, KNMI en RainViewer pollen tegelijk
- Geen cross-provider deduplicatie
- Zelfde regenbui meerdere keren aangeleverd → dubbele WeatherSystems

**Fix:**
- OPERA = primary radar provider voor Europa
- RainViewer = fallback (alleen als OPERA faalt/stale)
- KMI/KNMI = vergelijkingsmodus (niet actief in OFE)
- ProviderRegistry activeren in runtime

---

## Fase 1 — OPERA veilig maken (EERST DOEN)

1. S3 discovery corrigeren (paginering + volledige key)
2. Productleeftijd valideren (max 15 min oud)
3. Lokale bbox gebruiken (observation horizon CoverageArea)
4. Gedeelde downloadcache (één download, meerdere slices)
5. Poll-overlap voorkomen (asyncio.Lock)
6. Manifest versie synchroniseren (nu v0.1.0, moet v0.4.2 zijn)

## Fase 2 — Providerselectie

1. OPERA migreren naar ProviderPlugin contract (`providers/base.py`)
2. Capabilities invullen (`Capability.RADAR`)
3. ProviderRegistry activeren in runtime
4. OPERA = primary, RainViewer = fallback
5. KMI/KNMI tijdelijk uit OFE halen

## Fase 3 — RadarCellObservation

Zie review document voor details. Later.

## Fase 4 — RegionEngines activeren

Zie ONTWIKKELPLAN.md voor details. Later.

---

## Wat NIET te wijzigen

- HDF5 verwerking buiten eventloop (async_add_executor_job) ✅
- Quality/nodata/undetect/gain/offset verwerking ✅
- Providers kennen alleen hun eigen protocol ✅
- OFE creëert geen WeatherSystems ✅
- Lazy geocoding ✅

---

## Bekende issues

- Manifest versie = v0.1.0 (moet v0.4.2 zijn)
- iot_class = "local_push" (moet "cloud_polling" zijn)
- Tests hardcoded pad naar /home/claude/stv3 (niet portable)
- __pycache__ zit in distributie ZIP

---

## Architectuurbeslissingen (vastgelegd)

- Sharing radius: 50km (configureerbaar)
- Observation horizon: forecast_time × max_storm_speed (standaard 200km)
- OPERA dekkingsgebied (uit API metadata): lon -22.6 tot 29.8, lat 28.0 tot 70.6
- ProviderRegistry strategie: per capability (niet altijd "beste provider")
- RegionEngines: dynamisch, auto-aanmaken en auto-verwijderen

---

## Bestanden op NUC

```
/config/custom_components/storm_tracker_v3/   ← integratie
/config/storm_tracker_v3_logs/                ← CSV logbestanden
/config/secrets.yaml                          ← API keys
/config/configuration.yaml                   ← storm_tracker_v3 config
```

## Secrets in gebruik

```yaml
knmi_api_key: ...
knmi_wms_api_key: ...
netatmo_client_id: ...
netatmo_client_secret: ...
netatmo_refresh_token: ...
meteogate_api_key: ...  # voor MeteoGate API (niet nodig voor S3)
```

## Dependencies op NUC

```bash
# Al geïnstalleerd via HA requirements:
h5py>=3.11,<4
numpy>=2.0,<3
pyproj>=3.6,<4
```
