# Science-shadow multi-scan identity v5

## Besluit

De ongewirede worker-side scienceketen bewaart nu een begrensde, branch-
consistente identiteitsposterior over meerdere frames. De kandidaat scheidt
hele-scene systeemtranslatie van lokale celdeformatie, houdt maximaal acht
branches aan en commit met een vier-frame N-scan horizon. Er is geen vaste
snelheids- of afstandsafkeur.

De implementatie, duurzame checkpoint/restart en twee onafhankelijke 36-frame
runs zijn technisch deterministisch. Het objectieve resultaat op deze ene
normalized-cache-sequence is echter **niet beter dan v4.1**: de geselecteerde
multi-scan-MAP-route heeft elf extra intervallen boven 200 km/u en een hogere
mediaan/p95. Daarom blijven meteorologische admission, service, MQTT, Home
Assistant, forecast en operationele snapshots volledig gesloten.

De input is `pysteps_frame_cache`: genormaliseerde Storm Tracker-cache, geen
raw KMI-data. De klassen zijn niet fysisch, provider-nodata ontbreekt en er is
geen onafhankelijke cell identity-, split/merge- of speed truth.

## Model en harde bounds

Per vorige posteriorbranch en huidig detectieframe gebeurt het volgende:

1. vorm de volledige actieve branch-path x huidige-detectie cross-product;
2. bereken iedere edge zonder speed/distance gate;
3. trek de vooraf bepaalde hele-scene translation af en rangschik de lokale
   deformatie plus vorm/intensiteit/footprint-termen;
4. los maximaal vier complete assignments per bronbranch op met de begrensde
   Murty/Hungarian-solver;
5. combineer de branchhistorische kost en huidige assignmentkost;
6. behoud maximaal acht laagste branches en publiceer conditionele weights,
   entropy, effective hypothesis count en expliciete unknown omitted mass;
7. commit, zodra de afstand tot de durable commit groter is dan vier frames,
   exact de oudste beslissing van de huidige MAP-ancestry en prune branches
   met een andere ancestor;
8. behoud alle nog gerefereerde split/merge-alternatieven tot hun decision uit
   de herzienbare horizon gecommit of gepruned is.

Calibrationbounds:

- horizon 4 frames, beam 8;
- maximaal 64 gegenereerde branches per transitie;
- maximaal 16.384 assignment-subproblemen totaal per transitie;
- maximaal 500.000 candidate edges totaal;
- maximaal 512 actieve identity paths per branch;
- maximaal 10.000 gebufferde detecties;
- maximaal 4.096 lineage-alternatieven;
- conditionele kosttemperatuur 2,0.

Een boundoverschrijding levert expliciet `indeterminate` en behoudt exact de
oude head. Er is geen silent candidate-drop, branchmerge, kleinere beam,
greedy fallback of one-best fallback. De policyfingerprint bindt alle bounds.

## Onzekerheid en lineage

De branchweights zijn alleen een softmax binnen de begrensd behouden set. Zij
zijn geen gekalibreerde kansen. Omitted mass was op 32 van 36 frames expliciet
onbekend. De geselecteerde route refereerde drie split- en twee
merge-alternatieven; alle bleven `selected=false`. Zonder onafhankelijke truth
wordt morfologische overlap nooit automatisch als identiteit uitgevoerd.

Na frame 36 was revision 32 duurzaam N-scan-gecommit; beslissingen 33-36
bleven als vier expliciet herzienbare MAP-stappen in de posterior. De commit-
chain is checksummed en de gesloten JSON-statecodec bewaart branches, paths,
steps, weights, lineage en rolling detection support zonder pickle.

De huidige primaire `CellTrackLedger` en de WeatherSystem-view blijven voor
veiligheid per-transition rank 1 gebruiken. De multi-scanposterior herschrijft
die lifecycle niet retroactief. WeatherSystem is dus in v5 **niet** opnieuw uit
het multi-scan-MAP-pad berekend. Dat is een expliciete integratie/admission-
blocker, geen stil gemengde identiteit.

## Synthetisch bewijs

De gerichte engineproeven dekken:

- crossing met meerdere branches en N-scan-commit;
- gedeelde translation versus afzonderlijke lokale deformatie;
- missing detection, gap en reacquisition;
- gebonden, nooit auto-geselecteerde split/merge-alternatieven;
- generated-branch-, candidate-edge- en assignment-subproblemcapaciteit;
- een 18.000.000 km/u kandidaat die niet wordt weggefilterd;
- exact deterministische replay;
- 1.024 frames met begrensde branchsteps, detectionbuffer, pathhistory en
  plateau van de gecodeerde multi-scanposterior;
- duurzame checkpoint/restart over de horizon en exacte duplicate-retry.

De oudere primaire CellTrack-detectiehistorie heeft een afzonderlijke,
caller-configureerbare fail-closed cap. De 1.024-frameproef meet bewust het
nieuwe posteriorplateau; zij beweert niet dat een onbeperkte primaire ledger
bestaat.

## Exacte KMI normalized-cache replay

Input:

- database SHA-256
  `177d4704d665000722dbd131f3db03a238b26de072ce32bde6ea4650815cb263`;
- sequence
  `normalized:region-1:kmi:1785375000:1785396000:fca5657fc4c1`;
- 36 contigue frames op 600 s cadence;
- v5-export 761.629 bytes, SHA-256
  `70e97af661afef0d83133065e67f3e87b7808513a343630ededc60926255d0a`.

Technisch bewijs:

- twee onafhankelijke stores: 36/36 `advanced`;
- 72 exacte duplicatechecks;
- receiptchain, transition-ID, per-frame accounting en finale state exact;
- directe reconstructie byte-exact gelijk aan de geisoleerde durable head;
- verse runnergrenzen op 20->21 en 32->33;
- checkpoint revision 20 plus contigue tail 21-36;
- maximaal acht branches, 5.796 candidate edges en 654 assignment-
  subproblemen in een transitie;
- candidate-, generated- en lineage-accounting op alle frames exact;
- calibrationdatabase en bestaande sidecars byte/stat-identiek gebleven.

Geselecteerde branch-consistente route:

| Metriek | v4.1 per-transition rank 1 | v5 multi-scan MAP | Verschil |
|---|---:|---:|---:|
| Tracks | 66 | 64 | -2 |
| Actief na frame 36 | 31 | 31 | 0 |
| Ended | 35 | 33 | -2 |
| Intervallen | 221 | 223 | +2 |
| Intervallen >200 km/u | 120 | 131 | +11 |
| Mediaan km/u | 239,43 | 262,20 | +22,77 |
| p95 km/u | 1.134,19 | 1.149,66 | +15,48 |
| Maximum km/u | 1.730,97 | 1.730,97 | 0 |

De hogere sprongmetrics betekenen niet dat multi-scan conceptueel onjuist is;
ze tonen dat horizon, rankingkosten en inputsemantiek zonder onafhankelijke
truth geen betrouwbare identity posterior opleveren. V5 wordt daarom niet
gepromoveerd en vervangt v4.1 niet als operationele kandidaat.

## Volledige child-resource-evidence

Alle 36 primaire vooruitframes hebben een eigen gemeten waarde; dit is geen
representatieve single-frame ondergrens:

| Metriek per child | p50 | p95 | maximum |
|---|---:|---:|---:|
| Parent walltijd (s) | 5,939 | 21,067 | 26,141 |
| Child user+kernel CPU (s) | 5,719 | 20,828 | 25,938 |
| OS lifetime peak RSS (bytes) | 93.413.376 | 101.736.448 | 102.326.272 |
| CPU/wall core-equivalent | 0,959 | 0,989 | 0,992 |

RSS gebruikt `PeakWorkingSetSize` op Windows en `VmHWM` op Linux, bemonsterd
terwijl ieder spawn-child leefde. CPU is de lifetime user+kernel process CPU;
wall is de parent-`perf_counter` rond `execute`.

Tegen een hypothetische 300 s cadence bleef 278,933 s p95- en 273,859 s
maximum-wallheadroom over. De totale primaire CPU gebruikte 2,629% en wall
2,713% van een strikt seriele 300 s cadencebudget. De echte cachecadence is
600 s; daar zijn de fracties 1,315% en 1,357%.

Dit bewijst alleen geisoleerde childbelasting op deze desktop en deze 36
frames. Het is **geen NUC/HA/Z2M-admissionbewijs**: er ontbreekt een
representatieve NUC co-location-soak met Home Assistant en Zigbee2MQTT,
thermiek, schedulerlatency, IO-wait en worst-case providerframes. De bijna
exacte ene core-equivalent toont bovendien dat deze kandidaat niet als
onbegrensde parallelle workload naast HA mag worden gestart.

## Evidence

- `.analysis/kmi_region1_science_shadow_isolated_replay_v5_20260808_evidence.json`
  - 496.393 bytes
  - SHA-256 `322cc7fa37efdd2e73267c1c7d84b69caa91868f2adc066c50726b21b0299815`
- `.analysis/kmi_region1_science_shadow_isolated_replay_v5_20260808_evidence.md`
  - 4.028 bytes
  - SHA-256 `428bb7b72c7ffc99e4dfe4dddc7565a1703693954e558f63a197f1742de61e17`

## Open blockers

- geen raw KMI-input, fysieke units of provider-nodata;
- geen onafhankelijke identity/split/merge/speed truth of holdoutkalibratie;
- zachte kosten zijn op dezelfde cache verkend;
- multi-scan MAP herschrijft primary lifecycle/WeatherSystem nog niet;
- branchweights zijn niet gekalibreerd en omitted mass is vaak onbekend;
- geen NUC/HA/Z2M co-location- en thermische admissiontest;
- DWD +000-only onafhankelijke providerreplay nog uit te voeren;
- geen service/MQTT/HA/snapshot/live-wiring toegestaan.
