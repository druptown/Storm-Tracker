# Storm Tracker V3 — uitvoerstatus 2026-08-11

Status: lokale releasekandidaat; offline code- en artefactfreeze voltooid,
niet gedeployed en niet operationeel geactiveerd.

Dit document vervangt eerdere statusdocumenten niet. In het bijzonder blijft
`ROADMAP_EXECUTION_STATUS_2026-08-08.md` sealed historische bewijscontext. Dit
bestand beschrijft alleen de finale lokale freeze voor integration `1.13.0` en
worker `1.16.0`.

Latere lokale opt-in canarycode en de bijbehorende tests staan uitsluitend in
het [post-RC1-supplement](ROADMAP_EXECUTION_STATUS_2026-08-11_POST_RC1.md). Zij
wijzigen deze bevroren telling, bytes en vrijgavegrens niet.

## Einduitkomst

| Grens | Uitkomst |
|---|---|
| Lokale testinventory | groen binnen de hieronder gesloten scope |
| Reproduceerbare artefacten | primary en repro byte-identiek |
| Meteorologische productie-admission | niet verleend |
| Worker-service reclamationwiring | `false` |
| Production enable-ready | `false` |
| Home Assistant/MQTT-mutatie | niet uitgevoerd |
| Deployment | `false`; niet uitgevoerd |

De kandidaat is lokaal releaseklaar in de betekenis van reproduceerbare code,
gesloten offline contracten en een volledig verklaarde testmatrix. Dat is
nadrukkelijk niet hetzelfde als meteorologische of operationele vrijgave. De
live HA/Zigbee2MQTT-, provider-, filesystem-, power-loss-, soak- en science-
gates hieronder blijven open.

## Finale offline testmatrix

De finale telling is opgebouwd uit 48 seriële batches. De getelde batchduur is
exact `33m59s`; dit is de som van de tellende batchruns.

| Maatstaf | Waarde |
|---|---:|
| Testbestanden in inventory | 251 |
| Collected nodes | 4,949 |
| Passed | 4,934 |
| Skipped | 15 |
| Failed | 0 |
| Errors | 0 |
| Warnings | 0 |
| Warningbeleid | `-W error` |

De som sluit exact: `4,934 + 15 = 4,949`. Geen testnode uit de inventory bleef
onverklaard. De geregistreerde inventarisidentiteiten zijn:

- testinventory SHA-256, verkorte registratie:
  `ce7d1eca...bc2818c`;
- nodeinventory SHA-256, verkorte registratie:
  `da3d1d96...64948`.

De verkorte vorm is bewust letterlijk overgenomen uit de finale runner-
registratie; dit document vult de ontbrekende middelste hextekens niet in en
verzint dus geen volledige digest.

### Exacte skipverklaring

| Reden | Aantal |
|---|---:|
| open-SQLite platform | 4 |
| Windows symlink | 6 |
| HSAF local | 1 |
| AEMET live | 1 |
| GOES live | 2 |
| Met Office live | 1 |
| **Totaal** | **15** |

Dit zijn platform- of expliciet live-data-afhankelijke skips. Zij bewijzen de
betreffende POSIX-/livegrens niet en mogen niet als pass worden geteld.

`tests/test_opera_integration.py` bevat daarnaast twee CloudFerro-livecases.
Dat volledige bestand is bewust uitgesloten: beide cases zitten niet in de 251
testbestanden, niet in de 4,949 nodes en niet in de 15 skips. Er is dus geen
impliciete OPERA-liveclaim.

## Worker-first en performancegrens

De bevroren architectuur houdt providerfetch, decode, tracking, groundfusion,
forecast, replay en zware opslag buiten de Home Assistant-eventloop. HA
consumeert alleen desired-state-/snapshotcontracten; de worker gebruikt één
seriële completionlane, begrensde queues/processen en maximaal twee
rekenthreads, met een gereserveerde fysieke core voor HA en Zigbee2MQTT.

De lokale tests dekken ownership, restart, backpressure, deadlines, process-
isolatie, bounded payloads, storecaps, clock rollback, durable receipts,
snapshot-CAS en shutdown. Een echte langdurige NUC-test met gelijktijdige HA-
en Zigbee2MQTT-belasting, RSS/CPU/temperatuur/latency en thermalthrottling is
nog steeds vereist vóór activatie.

## Acquisition- en bronreclamationgrens

De lokale v2-keten heeft nu:

- een append-only consumerregistry met fysiek gehouden actuele epoch/root;
- een retention-v2 candidate-authority met gepinde sealed v1-records;
- een lifecycle-v4 participant met immutable effect/filejournal/floor;
- native execution-, scheduler- en capacityparticipants in hun eigen bron-DB;
- een raw participant met reference-counted capture/receipt/objectjournal;
- één duurzame coordinator met current holds, PREPARED, COMMITTED, APPLIED,
  ABORTING en ABORTED;
- de vaste applyvolgorde lifecycle -> execution -> scheduler -> raw ->
  capacity.

De echte vijfrollenintegratie bewijst lokale prepare, commit, apply, historische
archive-resolve, idempotent replay en volledig reopen zonder refetch of dubbel
effect. Registry-epochdrift tussen PREPARED en COMMITTED wordt geweigerd; abort
laat de sourcefeiten intact en een exact hogere-epoch-successor kan daarna
toepassen. V1 withheld-receipts blijven historisch en worden nooit tot v2-
permission opgewaardeerd.

Raw en lifecycle houden desondanks exact deze live blocker open:

```text
exclusive_filesystem_owner_dirfd_policy_required_for_crash_atomic_hardlink_exclusion
```

De participants detecteren reeds bestaande hardlinkaliases en falen gesloten,
maar portable Python kan een externe bevoegde writer niet crash-atomair
uitsluiten tussen iedere laatste witness en unlink. Live gebruik vereist daarom
exclusief workerownership van roots en parentdirectories, een geaudite
directorybarrier op het doel-OS en echte power-loss-/disk-/mediafaultproeven.
Zie `ACQUISITION_SOURCE_RECLAMATION_APPLY_AUTHORITY.md`,
`ACQUISITION_SOURCE_RETENTION.md`, `ACQUISITION_EVIDENCE_LIFECYCLE.md` en
`ACQUISITION_CONTROL_RECLAMATION.md`.

## Meteorologische bewijsgrens

De freeze bewaart de fundamentele regel dat ontbrekend, stale, ambigu, corrupt
of niet-gecertificeerd bewijs `unknown`/`indeterminate` oplevert en nooit een
verzonnen droog, veilig of operationeel feit. De lokale suite dekt onder meer
immutable lineage, hiërarchische detectie, association, actuele versus
historische geometrie, typed neerslag-/dreigingssemantiek, ground-evidence,
forecastcontracten, probabilistische kalibratiegrenzen en provider-specifieke
raw/decodeproofs.

Niet bewezen en dus niet toegelaten zijn onder meer:

- held-out meteorologische skill boven persistence en de legacy comparator;
- volledige echte providerreplays voor alle regio's en omstandigheden;
- live OPERA/CloudFerro en alle andere expliciet overgeslagen live datasets;
- operationele probabilitycalibratie en regionale canaryresultaten;
- langdurige providerhandoff-, outage- en clockdriftvalidatie;
- live sensoren/alerts als meteorologisch betrouwbare publicatie.

## Reproduceerbare lokale artefacten

Primary directory:
`.analysis/release-candidates/20260811-i1.13.0-w1.16.0-rc1`

Reproductiedirectory:
`.analysis/release-candidates/20260811-i1.13.0-w1.16.0-rc1-repro`

| Artefact | Bytes | SHA-256 |
|---|---:|---|
| `storm_tracker_v3_v1.13.0_deploy.zip` | 642.824 | `aeb419b772b348605069a383891ed0b0938115f83fd7c44f6cf583d4d58b5e72` |
| `storm_tracker_worker_v1.16.0_deploy.tar.gz` | 1.874.023 | `457cf9ba0f1513c39eab193b91016efa06a8c88ca265fe44f3fc2406d4f46a7e` |
| `storm_tracker_release_v1.13.0_worker_v1.16.0_evidence.json` | 44.299 | `59dd0bfa24a18d79ebb61ad783bc53668ca1e03a89ae64a55ab1207815e1ccac` |

Het evidencebestand gebruikt contract
`stv3-local-release-artifact-evidence-v1` en bevat intern
`evidence_sha256=7044f2f98220ea7a24cea9655cd4e0bf4d583b237a887b616376e4b6354401b7`.
Die interne contracthash en de SHA-256 van het volledige bestand zijn
verschillende, beide verwachte waarden.

| Sourcetree | Bestanden | Bytes | Contentroot SHA-256 |
|---|---:|---:|---|
| integration | 114 | 2.914.661 | `3e1ce740b384422a6191d9a2a15ba16723e89d0a30249dde4cea77ca77251126` |
| worker | 198 | 10.091.466 | `db3743eb92f50b5eb1a1467d6969ccfd25bdf6fc62eccb8d5904cde62f6e2673` |

Primary en repro zijn voor alle drie artefacten byte-identiek. Beide builds
bevatten nul cachebestanden, banned bestanden, private keys, databases,
nested archives, reparse points en symlinks. De evidence legt
`deployment_performed=false` vast.

## Open vrijgave- en live gates

De volgende voorwaarden blijven open en worden door de groene offline matrix
of reproduceerbare build niet afgevinkt:

- aparte eindvalidatie en expliciete menselijke deploybeslissing;
- geaudite POSIX/addon directorydurability en exclusief filesystemownership;
- echte host-power-loss-, disk-full-, WAL/SHM-, media- en corruptiematrix;
- rollbackdry-run op een afzonderlijke, herstelbare testinstallatie;
- langdurige NUC resource-/thermal-/HA-/Zigbee2MQTT-soak;
- live provider-, broker-, MQTT- en Home Assistant-faultmatrix;
- immutable New York- en aanvullende held-out meteorologische replays;
- science/admissioncertificaten, calibratie en gefaseerde canary;
- security/privacyreview van langdurige raw- en sealed archieven.

## Deploybesluit

Er is niet gedeployed. De kandidaatbestanden zijn uitsluitend lokale,
checksummed overdrachtsartefacten. `SERVICE_WIRING_ENABLED=false`,
`production_enable_ready=false` en `deployment=false` blijven staan. Geen
Home Assistant-, MQTT-, provider-, Zigbee2MQTT- of live storestate is door deze
freeze gewijzigd. Deployment vereist een afzonderlijke eindvalidatie; deze
status geeft daarvoor geen impliciete toestemming.
