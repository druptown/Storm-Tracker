# Science-shadow calibration replay v3

> Opgevolgd door de begrensde multi-hypothesis v4.1-proef in
> `docs/SCIENCE_SHADOW_MULTI_HYPOTHESIS_REPLAY.md` en de multi-scan v5-proef in
> `docs/SCIENCE_SHADOW_MULTI_SCAN_REPLAY.md`. Dit document blijft het
> onveranderde one-best/v3-baselinebewijs voor de exacte-inputvergelijking.

## Status

Dit is een uitsluitend offline, worker-side evidencepad. Het is niet verbonden
met `service.py`, MQTT, Home Assistant, acquisition, forecast, admission of een
operationele snapshot. Het verleent nooit meteorologische admission.

De geaudite factory staat in
`storm_tracker_worker/science_shadow_calibration_replay.py`. De uitvoerbare
evidencerunner staat in
`scripts/run_science_shadow_calibration_replay.py`.

## Exacte inputauthority

De bron is uitsluitend de hash-identieke calibrationdatabase:

- bestand: `.calibration-data/storm_tracker_calibration_20260730_092754.sqlite3`;
- SHA-256: `177d4704d665000722dbd131f3db03a238b26de072ce32bde6ea4650815cb263`;
- tabel: `pysteps_frame_cache`;
- groep: `region-1` / `kmi`;
- 36 contigue frames, cadence 600 s;
- sequence-ID:
  `normalized:region-1:kmi:1785375000:1785396000:fca5657fc4c1`.

De database wordt eerst als regular file gestreamd en gehasht, daarna met
SQLite URI `mode=ro&immutable=1` plus `PRAGMA query_only=ON` gelezen, en na de
export opnieuw op file identity, bytes en SHA gecontroleerd. De canonical JSON
export is 761.427 bytes en heeft SHA-256
`9aa7874f63d290639621fcf64978e31d5412f14dc20d4513d8e0686a7aa896d7`.
De twee reeds op 30 juli bestaande `-shm`/lege `-wal` sidecars dateren van vóór
de replay en bleven tijdens de finale audit content- en stat-identiek.

Het databasepad gaat nooit naar de child. De processauthority bevat uitsluitend
het absolute, checksummed exportpad (`read_file`) en een aparte receiptstore
(`transactional_file`). Iedere frame-run-ID bindt source-database-SHA,
sequence-ID en framecontent-SHA.

## API

1. `export_normalized_cache_sequence(...) -> CalibrationReplayExportReceipt`
2. `create_calibration_replay_authority(...) -> CalibrationReplayAuthority`
3. `ScienceShadowIsolatedRunner(...).execute(attempt_id=...,
   canonical_run_id=...) -> ScienceShadowProcessResult`
4. `recover_calibration_replay_state(authority) -> CellTrackEngineState`

De factory-importtarget is
`storm_tracker_worker.science_shadow_calibration_replay:build_calibration_replay_orchestrator`.
Een caller moet de returned authority-SHA, factorycontract en isolatielimieten
exact gebruiken. Een stale authority, exporttamper, andere content-SHA,
ongeautoriseerd pad of incompatible receiptprofiel faalt vóór tracking.

## Trackingpolicy en computationele grenzen

De replay gebruikt dezelfde 112×177 bounded max-pool-view en expliciete
hiërarchische detectorconfig als de eerdere rapporten. Dat is geen native
KMI-raster. Een 3×3 Lucas–Kanade-window vereist bewust 10 geldige samples en
is dus expliciet unavailable: normalized intensiteitsklassen hebben geen
toegelaten fysische flowsemantiek. Tracks met twee posities gebruiken hun
last-two-hypothese; cold starts behouden de brede zero-centred uncertainty
prior.

Tot acht actuele detecties wordt exacte top-k dynamic programming gebruikt.
Daarboven gebruikt deze policy expliciet exact one-best Hungarian. De volledige
previous×current candidategraph, births en deaths blijven behouden. Alleen
alternatieve top-k-oplossingen ontbreken dan expliciet. Deze keuze ontstond uit
een echte fail-closed observatie: frame 26 met 17 detecties raakte tweemaal de
45 s CPU-bound; de duurzame head bleef beide keren revision 25.

Per child gelden wall 60 s, CPU 45 s, RSS 1 GiB, request 32 KiB, result 64 KiB
en authority 1 MiB. Eén globale process-slot voorkomt parallelle belasting.
De receiptstore bewaart maximaal 128 receipts, checkpoint iedere 20 advanced
frames en een contigue replaytail van maximaal 64 frames.

## Echte 36-frame-uitkomst

Twee volledig onafhankelijke receiptstores bereikten exact revision 36:

- 36/36 `advanced`, geen invalid/withheld/indeterminate/capacity/deadline;
- receiptketen, transition-ID's, per-frame accounting en finale state-SHA exact
  gelijk;
- exacte duplicate-retry voor alle 36 canonical runs;
- verse parent-runners op 20â†’21 en 32â†’33;
- finale state-SHA:
  `86e9ada8957a48b54fd0a14b5a95c7884ad0535cf72a7db6d26bafba3a3e018a`;
- 38 tracks: 23 actief, 15 ended;
- 38 birth-events, 15 death-events, 75 gaps en 34 reacquisitions;
- geen supplied split/mergehypothesen, dus nul expliciete lineage-events;
- finale stores elk 2.183.168 bytes; checkpoint revision 20 is 29.031 bytes
  compressed en de contigue tail bevat 16 frames;
- twee-run-commandowandklok 393,1 s;
- representatieve verse-child RSS-sample 70.819.840 bytes; dit is een
  ondergrens, niet het bewezen maximum van alle 72 children; de harde grens
  bleef 1 GiB.

WeatherSystem bleef uitsluitend afgeleid. De snapshots op frames 20/32/36
bevatten respectievelijk 3/5/11 systemen. Zij classificeren geen MCS en hun
descriptieve motion bevestigt geen trackidentiteit.

## Meteorologische conclusie: niet toegelaten

De technische determinisme- en restartbewijzen zijn groen, maar de
meteorologische identiteit is niet betrouwbaar:

- 249 gekozen lineaire intervalhypothesen;
- mediaan 354,89 km/u;
- p95 1.940,42 km/u;
- maximum 2.980,65 km/u;
- 161 hypothesen boven 200 km/u.

Die snelheden worden niet afgekeurd of verborgen, maar zij zijn ook geen
bewegingswaarheid. Samen met ontbrekende externe identity/speed truth tonen ze
dat one-best assignment nog niet voor waarschuwingen of forecasts gebruikt mag
worden. De eerstvolgende science-stap is dus onafhankelijke identiteittruth of
een lossless multi-hypothesis/track-before-detect laag; niet een hard speedcap
die het bewijs weggooit.

## Onverminderde blockers

- normalized cache is geen ruwe KMI-data;
- intensiteitsklassen zijn geen fysische neerslageenheden;
- provider-nodatamasker ontbreekt;
- externe cell identity en speed truth ontbreken;
- bounded max-pool-view is niet native resolution;
- optical-flow evidence is bewust unavailable;
- top-k-alternatieven ontbreken boven acht detecties;
- factory-certificaat en alle meteorologische admissioncertificaten ontbreken;
- geen live/admission/snapshotwiring is toegestaan.

Downloadbare evidence:

- `.analysis/kmi_region1_science_shadow_isolated_replay_v3_20260808_evidence.json`
- `.analysis/kmi_region1_science_shadow_isolated_replay_v3_20260808_evidence.md`
