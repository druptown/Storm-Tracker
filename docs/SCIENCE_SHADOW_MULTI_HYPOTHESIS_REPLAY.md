# Science-shadow multi-hypothesis identity v4.1

> Opgevolgd door de begrensde multi-scan/N-scan v5-proef in
> `docs/SCIENCE_SHADOW_MULTI_SCAN_REPLAY.md`. V4.1 blijft de onveranderde
> per-transition top-k-baseline voor de exacte-inputvergelijking.

## Besluit

De worker-side shadowketen heeft nu een begrensde top-k-identiteitslaag die
globale systeemtranslatie en lokale celdeformatie afzonderlijk modelleert. De
keten is process-geisoleerd, duurzaam herstartbaar en op twee onafhankelijke
36-frame-runs exact deterministisch. Er is geen vaste snelheids- of
afstandsafkeur.

Dit is nadrukkelijk **geen meteorologische admission**. De gebruikte
`pysteps_frame_cache` is genormaliseerde toepassingscache, geen ruwe KMI-data;
de intensiteitsklassen zijn geen fysische neerslageenheden en een
provider-nodatamasker, onafhankelijke celidentiteit en onafhankelijke
snelheidswaarheid ontbreken. Niets is verbonden met service, MQTT, Home
Assistant, forecast of operationele snapshots.

## Uitvoerbare interfaces

1. `export_normalized_cache_sequence(...) -> CalibrationReplayExportReceipt`
2. `create_calibration_replay_authority(...) -> CalibrationReplayAuthority`
3. `ScienceShadowIsolatedRunner(...).execute(attempt_id=...,
   canonical_run_id=...) -> ScienceShadowProcessResult`
4. `recover_calibration_replay_state(authority) -> CellTrackEngineState`

De factorytarget is
`storm_tracker_worker.science_shadow_calibration_replay:build_calibration_replay_orchestrator`.
De child krijgt alleen een exact geautoriseerd, checksummed exportpad en een
aparte transactionele receipt/checkpointstore. De calibrationdatabase zelf
wordt uitsluitend `mode=ro&immutable=1` en `query_only=ON` gelezen en haar pad
wordt niet aan de child doorgegeven.

## Identiteitsmodel

Per overgang voert `CellTrackEngine` de volgende stappen uit:

1. Detecteer alle rain shields en embedded cores op de begrensde analysis-view.
2. Bouw de volledige vorige-detectie x huidige-detectie cross-product; geen
   kandidaat wordt wegens snelheid, afstand of innovatie weggegooid.
3. Schat voor de hele scene, **voor** enige identiteit is geselecteerd, de
   area-gewogen verschuiving van de rain-shieldcentroid. De 2x2-onzekerheid
   wordt expliciet begrensd.
4. Trek die systeemtranslatie van iedere kandidaatverplaatsing af. Wat
   overblijft is lokale celdeformatie. Een halve vorige geselecteerde lokale
   deformatie is alleen een zachte geheugenprior; bij gaps wordt de gemeten
   systeemtranslatie niet als snelheid geextrapoleerd, maar wordt de
   onzekerheid verbreed.
5. Rangschik de volledige assignment met births/deaths via een begrensde
   Murty/Lawler-partitionering en een exacte Hungarian subsolver. De eerste
   vier unieke assignments worden bewaard zolang maximaal 4096 subproblemen
   volstaan.
6. Normaliseer de vier zachte kosten uitsluitend binnen de bewaarde set. Deze
   gewichten zijn conditioneel, niet gekalibreerd. Als de volledige
   combinatorische ruimte niet is uitgeput blijft `omitted_hypothesis_mass_unknown`
   expliciet waar.
7. Alleen rank 1 avanceert de lifecycle. De overige ranks blijven
   identiteitsalternatieven en voeden ook de afgeleide WeatherSystem-view.

De calibrationpolicy gebruikt birth/death-cost 15, top-k 4, maximaal 500.000
lokale kandidaten, systeemtranslatie-sigma 5-50 km en lokale
deformatiesigma 10-20 km. De optionele translation uit een bestaand
WeatherSystem vereist hier 512 volwassen steuntracks en wordt op deze kleine
replay dus effectief niet gebruikt; de assignment-onafhankelijke hele-scene
translation blijft de authority. Deze parameters zijn op dezelfde cache
verkend en zijn daardoor exploratieve overfit, geen onafhankelijke kalibratie.

## Split/merge-lineage

Naast de top-k assignment leidt de engine begrensde, niet-geselecteerde
split/merge-alternatieven af uit maximale same-kind
translated-footprint-overlap:

- maximaal een splitgroep per vorig object;
- maximaal een mergegroep per huidig object;
- geen powersetzoektocht;
- maximaal 128 hypothesen en 64 leden per hypothese;
- gaps zijn niet lineage-eligible;
- de alternatieven worden nooit automatisch toegepast zonder externe truth.

De 36-frame-run vond drie split- en twee merge-alternatieven, maar paste nul
lineage-events toe. De receipt bewaart de canonieke hypothesen zodat recovery
dezelfde transition maakt.

## Fail-closed capaciteit en duurzaamheid

Overschrijding van candidate-, lineage- of subprobleemgrenzen geeft expliciet
`indeterminate`; de oude state/head blijft exact staan. Er is geen stille
candidate-drop, merge, greedy fallback of one-best schaalfallback. De
checkpointstore bindt state, advanced receipt, state-head en provenanceketen
met checksums. Checkpointinterval 20 bewaart na frame 36 checkpoint 20 plus de
contigue tail 21-36. Verse runners op 20->21 en 32->33 bewezen deterministisch
herstel.

De nieuwste state/checkpoint bevat de volledige actuele identity snapshot.
Receipts bewaren exact candidate-accounting en lineage. Historische volledige
candidateobjecten worden na checkpoint niet als aparte onbeperkte ledger
gehouden; zij zijn deterministisch reconstrueerbaar uit de content-addressed
canonical export, policy en contigue replaydata. Een langdurig querybare
historische hypotheseledger zou een apart begrensd opslagcontract vereisen.

## Harde processgrens

Elke frame-run gebruikt een vers spawn-safe childproces. De parent handhaaft
wall-, CPU-, RSS-, grace-, authority-, request- en resultgrenzen, en
reconcilieert de duurzame receipt na ieder child-einde. Voor deze replay gold:

- wall 60 s en CPU 45 s per child;
- RSS 1 GiB;
- request 32 KiB, result 64 KiB, authority 1 MiB;
- een globale process-slot;
- geen paden of secrets in childdiagnostiek;
- geen nieuw hoofd bij kill voor commit; exact receipt/replay na commit.

De representatieve verse-child sample was 72.368.128 bytes RSS. Dat is een
sample/ondergrens en niet het bewezen maximum over alle children.

## Exacte KMI-cacheproef

Input:

- database SHA-256
  `177d4704d665000722dbd131f3db03a238b26de072ce32bde6ea4650815cb263`;
- sequence
  `normalized:region-1:kmi:1785375000:1785396000:fca5657fc4c1`;
- 36 contigue frames, cadence 600 s;
- canonical export 761.505 bytes, file-SHA-256
  `a95e318e8d8beee6691ab2c5606201d5a147b4010f2f33e7dc74f5c51b52ee3f`.

Uitkomst van twee onafhankelijke receiptstores:

- 36/36 advanced, revision 36;
- receiptketen, transition-ID's, per-frame accounting en finale state exact;
- exacte duplicate-retry voor alle canonical runs;
- finale state-SHA-256
  `1cf304eb7f01e6f4fb5db7076ff790d27645250a7259b0bf665d3ae888af8797`;
- 66 tracks: 31 actief en 35 ended;
- 66 births, 35 deaths, 221 geselecteerde intervals;
- 35 frames met top-k identity, candidate-accounting overal exact;
- maximaal 80 van 4096 assignment-subproblemen gebruikt;
- omitted assignmentmass onbekend op 30 frames;
- primaire/repeatruntime 194,08/192,22 s;
- finale primaire store 2.183.168 bytes;
- calibrationdatabase en bestaande sidecars bleven byte/stat-identiek.

## Objectieve v3-vergelijking op exact dezelfde input

| Metriek | v3 one-best | v4.1 top-k | Verschil |
|---|---:|---:|---:|
| Tracks | 38 | 66 | +28 |
| Intervals | 249 | 221 | -28 |
| Intervals >200 km/u | 161 | 120 | -41 |
| Mediaan km/u | 354,89 | 239,43 | -115,47 |
| p95 km/u | 1.940,42 | 1.134,19 | -806,23 |
| Maximum km/u | 2.980,65 | 1.730,97 | -1.249,68 |

De sprongmetingen dalen, maar de trackfragmentatie stijgt. Zonder onafhankelijke
identity/speed-truth kan niet worden vastgesteld welke identiteit correct is.
Ook 1.730,97 km/u blijft duidelijk een onbevestigde associatiehypothese, niet
meteorologische beweging. Dit is dus een objectieve gedragsvergelijking en
geen nauwkeurigheids- of betrouwbaarheidsclaim.

## Resterende admissionblockers

- normalized cache is geen ruwe providerdata;
- klassen zijn geen fysische intensiteiten en nodata ontbreekt;
- onafhankelijke identity-, split/merge- en speed-truth ontbreken;
- de kosten zijn op dezelfde cache gekozen; onafhankelijke holdout ontbreekt;
- whole-scene centroidtranslation is geen externe wind/motion truth;
- top-k is per overgang, geen volledige multi-scan branchposterior;
- conditionele weights zijn geen gekalibreerde kansen;
- dense optical flow is bewust unavailable voor normalized classes;
- historische volledige candidates vereisen reconstructie, geen aparte ledger;
- alle operationele admissioncertificaten en live wiring blijven dicht.

## Evidence

- `.analysis/kmi_region1_science_shadow_isolated_replay_v4_1_20260808_evidence.json`
  - 339.635 bytes
  - SHA-256 `dac04a04e6ded049e97544b7599dde3681b423b9a4d55f42c1c249f2b922a5f4`
- `.analysis/kmi_region1_science_shadow_isolated_replay_v4_1_20260808_evidence.md`
  - 5.336 bytes
  - SHA-256 `246d18ac8560a9d544e0b80126ccb7fad276106efdbc1ce1922830ce79539f80`
