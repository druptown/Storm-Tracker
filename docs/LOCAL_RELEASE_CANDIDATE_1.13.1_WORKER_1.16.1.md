# Lokale RAW-releasekandidaat 1.13.1 / worker 1.16.1

## Status

Deze kandidaat is op 2026-08-21 lokaal gebouwd en gereproduceerd. Hij is niet
gedeployed, niet op een target gematerialiseerd en niet meteorologisch of
operationeel vrijgegeven.

De kandidaat is bedoeld voor de beperkte huidige doelstelling:

- zware provider- en opslagarbeid uit Home Assistant Core houden;
- exact ontvangen upstreambytes duurzaam in de worker bewaren;
- nog geen dashboard-, waarschuwing-, nowcast- of meteorologische output
  produceren.

## Identiteit

| Onderdeel | Waarde |
|---|---|
| Integratie | `1.13.1` |
| Worker | `1.16.1` |
| Build-ID | `integration-1.13.1-worker-1.16.1` |
| Runtime | `raw_capture_shadow` |
| Deployment | `false` |
| Targetmaterialisatie | vereist, niet uitgevoerd |
| Deploymentbundle compleet | `false` |
| Meteorologische toelating | `false` |
| Dashboardoutput | `false` |
| Productie-enable | `false` |

## Reproduceerbare artefacten

Primary:
`.analysis/release-candidates/20260821-i1.13.1-w1.16.1-raw-rc1`

Repro:
`.analysis/release-candidates/20260821-i1.13.1-w1.16.1-raw-rc1-repro`

| Bestand | Bytes | SHA-256 |
|---|---:|---|
| `storm_tracker_v3_v1.13.1_deploy.zip` | 643.072 | `a5575d95ddceb7982d426615e559a3ae4a49ac3745dc8d0a43eefea5836e1b0d` |
| `storm_tracker_worker_v1.16.1_deploy.tar.gz` | 2.054.797 | `226acc54364b35f5f9cf91c2e90ab65bd3522e34f60371ef52a83b36445e8a21` |
| `storm_tracker_release_v1.13.1_worker_v1.16.1_evidence.json` | 45.970 | `856832d942676983743b2707d3c860503f0c396dc7780d64e4d3a2f83af0a29f` |
| `storm_tracker_raw_capture_release_v1.13.1_worker_v1.16.1_requirements.json` | 37.701 | `d3e1d1691d15469fa6dfd8eb984c7fff6d383e2dc448f5634cf786fc284b3303` |

Alle vier bestanden zijn in primary en repro byte-identiek. De interne
contenthashes zijn afzonderlijk:

- v1 evidence: `2efe1d4e9afc78de42e71932f5146e6398e32a259d38e27e2b1b9ab14d15d677`;
- RAW requirements: `a9839ea6b274a082642c26dc566a22644b9e187d924679dc313a26e79963fece`.

Beide gepubliceerde kwartetten zijn na publicatie volledig herlezen met de
native verifier. Die controleerde de twee archieven, het v1-bewijs, de exacte
workerpakket-inventaris van 210 leden, het gesanitiseerde Supervisor-snapshot
en de vierde requirementsmarker.

## RAW-bereik

De worker heeft 27 cadenceworkloads met gesloten handler-, recovery- en
RAW-storebindingen. Blitzortung is een afzonderlijke opt-in event-ingang met
een eigen store en authority. De gesanitiseerde pre-targetopties zetten alle
zes retentie-/rechtenopt-ins bewust op `false`.

`radar.kmi.radqpe_native` blijft fail-closed omdat een echt native
leveringscontract en credentialtoegang ontbreken. Dat wordt als geblokkeerde
bron gerapporteerd en nooit als droog weer geïnterpreteerd.

Een duurzame RAW-commit is in deze modus terminaal. De terminalrouter en
downstreamconsumers zijn niet live gekoppeld. Er worden geen meteorologische
feiten, sensoren, kaarten, dashboardwaarden of waarschuwingen gegenereerd.

## Capaciteit en retentie

Automatische reclamation staat uit. Een volle store stopt fail-closed. Het
bevroren targetprofiel vereist minimaal `37.024.432.128` bytes aantoonbare
vrije ruimte op dezelfde filesystemgrens.

De laatste read-only NAS-inventaris zag ongeveer 24,6 GB vrij. Die target kan
dit profiel daarom niet veilig activeren zonder extra opslag of een later,
afzonderlijk ontworpen kleiner profiel. Een ingevuld getal in een attestatie
geldt niet als bewijs; targetdiagnostiek blijft ontrusted en kan deze release
niet `deployment_bundle_complete=true` maken.

## Testbewijs

De finale seriële, cross-cutting matrix op de definitieve versies gaf:

| Batch | Passed | Skipped | Failed |
|---|---:|---:|---:|
| A — integratie en HA-offload | 193 | 0 | 0 |
| B — RAW-activatie, opslag en materializer | 429 | 2 | 0 |
| C — crash, recovery, quarantaine en retentie | 238 | 0 | 0 |
| D — release, security, requirements en dry-run | 517 | 9 | 0 |
| **Totaal** | **1.377** | **11** | **0** |

Alle batches draaiden serieel met `-W error`, zonder pytest-cache en met
unieke basetemps. De finale quartetrollback kreeg daarna nog een aparte
regressiecyclus:

- 35 passed, 1 Windows-symlinkskip voor RAW requirements en publicatie;
- 38 passed, 1 skip voor het ongewijzigde v1-releasebewijs;
- onafhankelijke read-only red-team: geen resterende P0/P1.

De history-resettool gaf afzonderlijk 36 passed en 1 platformskip. Er is geen
live reset of delete uitgevoerd.

## Installatie- en rollbackdry-run

Runroot:
`.analysis/release-install-dry-run-20260821-i1.13.1-w1.16.1-raw-rc1`

Rapport:
`dry_run_report.json`, SHA-256
`454bbeb48ccbbc33f6e620ab69085e241a289e89355c424d9b23473f78106531`.

Bewezen:

- `simulation_completed=true`;
- `local_simulation_passed=true`;
- nul lokale simulatieblockers;
- Supervisoropties roundtripten exact;
- atomaire no-replace-publicatie werd bewezen;
- geen live paden, netwerk of deployment gebruikt.

Bewust open:

- `raw_capture_target_activation_pins_absent`;
- echte targetinstallatie en Supervisor build/cache;
- targetmaterialisatie en een extern vertrouwde targetattestatie;
- runtime stop/start op het doel;
- data- en schemarollback op het doel;
- hostile same-user races op het doel;
- fysieke power-loss/filesystemproef;
- menselijke deploygoedkeuring.

Daarom blijven `real_installation_ready=false` en `deployment_performed=false`
de enige geldige releaseconclusies.

## Geen meteorologische claim

Deze kandidaat verbetert de infrastructuur om bruikbare native bronbytes te
verzamelen. Hij bewijst niet dat de toepassing meteorologisch gekalibreerd of
betrouwbaar is. Onafhankelijke echte labels, held-out skill, live replays en
langdurige operationele soak blijven afzonderlijke roadmapgates.
