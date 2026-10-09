# Offline replay acceptance gate v1

## Status en scope

Dit is een lokale, content-addressed toelatingsgrens voor roadmapstap 3
(`offline replay`). De grens staat in
`storm_tracker_worker/offline_replay_acceptance.py` en is bewust nergens aan
de worker-service, acquisition, MQTT, Home Assistant, operationele snapshots of
het dashboard gekoppeld.

Een geslaagd receipt betekent uitsluitend:

> het aangeleverde, vooraf vergrendelde replaydossier voldoet aan het
> offline-sciencecontract en mag voor menselijke beoordeling naar de volgende
> shadowfase.

Ook bij een volledig geslaagd dossier blijven deze velden hard `false`:

- `meteorological_admission`;
- `live_wiring_allowed`;
- `dashboard_publication_allowed`;
- `home_assistant_state_mutated`.

De huidige repository bezit nog geen compleet echt dossier met onafhankelijke
held-out/golden labels, vooraf vastgelegde praktische marges en alle vijf
baselineruns. Deze nieuwe gate maakt dat ontbreken controleerbaar; hij verandert
het niet in een vrijgaveclaim.

## Wat de gate werkelijk uitvoert

De gate:

1. leest een begrensd, duplicate-key-vrij inputcontract;
2. bindt raw-inputmanifest, eventsplit, golden-labelcommitment en
   annotatieprotocol aan SHA-256;
3. controleert dat alle inputframes uit dezelfde `test`-partition komen;
4. hasht de exacte lokale bronbestanden en leest de algorithmversies van:
   - `analysis_grid_transform.py`;
   - `hierarchical_detection.py`;
   - `lucas_kanade_flow.py`;
   - `pyramidal_sparse_flow.py`;
   - `cell_track_engine.py`;
   - `nowcast_baselines.py`;
   - `forecast_verification.py`;
   - `ground_verification.py`;
   - `event_probability_calibration.py`;
5. valideert door de caller aangeleverde stage-receipts tegen die exacte
   bronhashes, dezelfde inputhash en een byte-identieke herhalingsoutput;
6. vereist afzonderlijke, reproduceerbare receipts voor alle baselines uit
   roadmapsectie 12.4:
   - Eulerian persistence;
   - twee-frame lineaire translation;
   - legacy engine;
   - pySTEPS deterministic extrapolation;
   - laatst vrijgegeven vNext;
7. bindt candidate-, baseline-, truth- en metricmanifesten tot één
   content-addressed keten;
8. roept uitsluitend `scientific_validation.evaluate_manifest()` aan om de
   aangeleverde `MetricEvidence` tegen het vergrendelde preregistratiemanifest
   en `ExternalTruthContext` te controleren;
9. produceert een deterministisch acceptance receipt of vaste fail-closed
   blockercodes.

## Wat de gate niet uitvoert

De gate voert de analysegridtransformatie, detector, optical flow,
`CellTrackEngine`, nowcast, ground fusion/verificatie of calibratiefit **niet
zelf** uit. Hij simuleert die uitkomsten evenmin. Hij controleert uitsluitend
hun aangeleverde execution receipts, outputhashes, herhalingshashes,
eventdekking en code-identiteit.

Daarom is `completed=true` in een los JSON-document nooit voldoende. Een
stagebewijs moet tegelijk:

- exact dezelfde inputcontracthash hebben;
- de actuele lokale sourcehash en algorithmversie dragen;
- alle gedeclareerde held-out events en frames dekken;
- een byte-identieke primaire en onafhankelijke herhalingsoutput hebben;
- `shadow_only=true` en `operational_state_mutated=false` verklaren;
- nul issuecodes bevatten.

De onafhankelijke runner die deze stage-receipts uit echte replaybytes bouwt,
blijft afzonderlijk werk. Deze gate verhindert dat ontbrekend runnerbewijs als
een geslaagde replay wordt voorgesteld.

## Verplichte minimale metricdekking

Een vergrendeld manifest mag meer metrics of strata eisen, maar kan minimaal
deze cross-layer gates niet weglaten:

- `detection.csi`;
- `flow.one_step_advection_skill`;
- `tracking.id_switches`;
- `tracking.lineage_integrity`;
- `nowcast.passage_brier_skill`;
- `ground.cross_storm_false_confirmations`;
- `calibration.heldout_brier_skill`.

Iedere gate moet onafhankelijke held-out truth vereisen. De exacte
releasegrenzen en praktische marges worden vóór het openen van testlabels
vastgelegd; de gate verzint geen grens uit de gemeten resultaten.

## Golden inputcontract

`tests/fixtures/offline_replay_acceptance_golden.json` is uitsluitend een
synthetische schemafixture. Hij bewijst canonicalisatie, SHA-binding en
tamperdetectie, maar bevat drie frames en is dus bewust niet toelatingswaardig.
Een echt dossier vereist minimaal twintig frames en bindt ieder frame aan:

- stabiele `frame_id`;
- meteorologische `event_id`;
- werkelijke geldigheidstijd;
- SHA-256 van het raw-framebewijs.

Events, niet losse sterk gecorreleerde frames, vormen de held-out eenheid. Alle
metricreceipts moeten exact dezelfde eventset gebruiken; cherry-picking van een
subset faalt gesloten.

## Lokale preflight

De CLI maakt alleen een niet-toelatende preflight en voert geen science uit:

```text
python -I -B scripts/build_offline_replay_acceptance.py \
  --input-contract tests/fixtures/offline_replay_acceptance_golden.json
```

Met `--output <nieuw-bestand>` wordt uitsluitend een nieuw bestand aangemaakt.
Een bestaand doel wordt nooit overschreven. Als schrijven of `fsync` faalt,
blijft een mogelijk gedeeltelijk nieuw bestand zichtbaar voor operatorinspectie;
de cleanup verwijdert nooit via een opnieuw resolveerbaar tekstpad een mogelijk
verwisseld vreemd bestand.

De preflight vermeldt altijd onder andere:

- `candidate_stage_evidence_not_supplied`;
- `baseline_evidence_not_supplied`;
- `metric_bundle_not_supplied`;
- `external_truth_not_evaluated`.

Een compleet receipt kan alleen via
`evaluate_offline_replay_acceptance(...)` worden gebouwd nadat de gevalideerde
Pythonobjecten voor input, truth, manifest, stages, baselines en metrics in
dezelfde lokale evidence-run aanwezig zijn.

## Fail-closed eigenschappen

- Ontbrekende stage, baseline, coremetric, label of event wordt een blocker.
- Train/validationframes zijn geen toegestane replayinput.
- Een source-, input-, truth-, candidate-, baseline- of metrictamper verandert
  de ketenhash en blokkeert.
- Een mislukte herhaling wordt nooit als deterministisch bewijs behandeld.
- Caller-controlled exceptiontekst komt niet in het receipt; alleen een vaste
  exceptionklasse wordt opgeslagen.
- Geen blocker wordt gemiddeld of door een goede globale metric verborgen.
- Een offline pass verleent geen productie-, publicatie- of dashboardauthority.

## Open echte gates

Voor een werkelijk stap-3-receipt ontbreken nog minimaal:

1. immutable echte multi-event input voor primaire regio/providerstrata;
2. onafhankelijk geblindeerde en bevestigde golden labels;
3. vooraf vergrendelde praktische marges en hun rationale-artifacts;
4. de vijf volledige baselineruns op exact dezelfde eventset;
5. een runner die de negen bestaande sciencestages uitvoert en de primaire en
   herhalingsoutputs content-addressed vastlegt;
6. metriccomputatie uit die outputs zonder testsettuning;
7. pas daarna menselijke beoordeling voor live shadow.

Live soak, HA/Z2M-SLO's, storage/power-loss, deploy/rollback en production
admission blijven afzonderlijke latere releasegates.
