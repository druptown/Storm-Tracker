# Offline nowcast baseline runner v1

## Resultaat en grens

`storm_tracker_worker/offline_replay_nowcast_runner.py` is de eerste kleine,
echte execution runner achter de offline replay-roadmap. Hij leest uitsluitend
een lokaal JSON-replaypack en voert twee bestaande pure sciencefuncties uit:

- Eulerian persistence via `eulerian_persistence()`;
- twee-frame lineaire translatie via
  `derive_two_frame_constant_motion()` en
  `lagrangian_constant_motion()`.

De runner simuleert deze outputs niet. Voor ieder event voert hij ieder
opeenvolgend framepaar uit. Daardoor worden in de meegeleverde fixture met
twintig frames negentien forecastpunten per baseline geproduceerd. Het exacte
pack wordt daarna opnieuw geparsed en de volledige forecastsequentie wordt
opnieuw uitgevoerd. Alleen byte-identieke canonical JSON-output geldt als een
geslaagde primary/repeat-run.

Dit is nog geen offline-sciencepass. Het receipt houdt altijd hard `false`:

- `real_meteorological_case`;
- `offline_science_gate_passed`;
- `meteorological_admission`;
- `live_wiring_allowed`;
- `dashboard_publication_allowed`;
- `home_assistant_state_mutated`.

De runner is nergens geïmporteerd door de worker-service, Home Assistant,
acquisition, MQTT of het dashboard.

## Inputcontract

Het pack bindt de payloadbytes van ieder frame tweemaal: aan de SHA-256 in de
frame-entry en aan `raw_frame_sha256` in het bestaande held-out replaycontract.
De SHA van alle geordende framebindings moet bovendien overeenkomen met het
`raw_input_manifest`-artifact. JSON met dubbele sleutels, een niet-canonical
framepayload, een verkeerd grid, een bool als getal of een ontbrekende binding
faalt gesloten.

De harde grenzen zijn:

- minimaal 20 en maximaal 64 frames;
- minimaal twee tijdgeordende frames per event;
- maximaal 65.536 gridcellen;
- maximaal 4 MiB voor het volledige replaypack;
- maximaal 2 MiB per gedecodeerde framepayload;
- maximaal 16 MiB voor alle gedecodeerde payloads;
- maximaal 16 MiB voor de canonical execution-output;
- maximaal 7.200 seconden lead time.

De bron-ID voor iedere uitgevoerde baseline wordt vooraf uit het pack gelezen,
moet uniek zijn en wordt in zowel output als `BaselineReplayEvidence` gebonden.
Deze runner bewijst niet dat die ID al in een locked wetenschappelijk manifest
staat. De afzonderlijke offline acceptance gate controleert later de source-ID,
rol en vooraf vastgelegde outputhash tegen dat manifest.

## Bewijs dat wel wordt geproduceerd

Het receipt bevat:

- de SHA-256 van de exacte replaypackbytes;
- de bestaande held-out inputcontracthash;
- de hash en algorithmversie van de actuele lokale
  `tracking_core/engine/nowcast_baselines.py`;
- de volledige, geordende primary output en zijn SHA-256;
- de SHA-256 van de volledige repeat output;
- één `StageReplayEvidence` voor `nowcast_baselines`;
- afzonderlijke `BaselineReplayEvidence` voor beide uitgevoerde baselines;
- alle event-ID's, de volledige pack-framecount en ieder support-framepaar;
- expliciete blockers voor iedere nog niet uitgevoerde baseline.

De framecount claimt alleen volledige packdekking omdat elk adjacent paar van
elk event werkelijk door de functies gaat. Een event met één frame wordt
geweigerd.

## Open blockers

Deze drie roadmapbaselines worden niet uitgevoerd en blijven altijd expliciet
in `blocking_reasons` staan:

- `last_released_vnext`;
- `legacy_engine`;
- `pysteps_deterministic_extrapolation`.

De meegeleverde fixture is een synthetisch 3x1-grid met bekende verplaatsing.
Hij test determinisme en provenance, maar is geen meteorologisch event, geen
held-out golden truth en geen admissionbewijs.

Primary en repeat draaien momenteel als twee afzonderlijke parses en volledige
executiesequenties in hetzelfde proces. Deze versie claimt geen procesisolatie,
CPU/RSS-budget, deadlinebewijs of recovery na een procescrash. Zulke
operationele eigenschappen blijven latere hardening en kunnen niet uit dit
receipt worden afgeleid.

## Lokale CLI

De dunne CLI schrijft uitsluitend één canonical JSON-receipt naar stdout:

```text
python -I -B scripts/run_offline_replay_nowcast_baselines.py \
  --replay-pack tests/fixtures/offline_replay_nowcast_baseline_input.json
```

Er wordt geen bestand gepubliceerd of overschreven. Om het resultaat te
bewaren moet een operator de stdout buiten deze runner bewust afvangen; dat
maakt de inhoud nog steeds niet admissible of live.
