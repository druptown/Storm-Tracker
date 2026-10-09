# CellTrackEngine — volledig ongewirede shadowkandidaat v3

## Status

`storm_tracker_worker/tracking_core/engine/cell_track_engine.py` is de eerste
immutable multi-frame compositor van de bestaande trackingprimitieven. De
module is niet geïmporteerd door de workerservice, Home Assistant, de legacy
`StormEngine`, providers, configuratie of deployment. Er werd niets live
geactiveerd. De vaste status is `candidate_only_unwired`.

Deze kandidaat maakt geen claim dat de gekozen identiteit meteorologische
waarheid is. Hij bewaart het volledige bewijs, de exacte top-k alternatieven en
de caller-supplied kosten waarmee een hypothese gekozen werd. De scalar costs
zijn rankingtermen, geen gekalibreerde identiteitkansen.

V2 voegt een optionele begrensde multi-hypothesispolicy toe. Die schat
hele-scene systeemtranslatie vóór de identiteit, bewaart lokale celdeformatie
per volledige candidate-edge en houdt maximaal top-k unieke assignments vast.
De volledige calibrationproef en haar beperkingen staan in
`docs/SCIENCE_SHADOW_MULTI_HYPOTHESIS_REPLAY.md`.

V3 voegt daar een optionele branch-consistente multi-scanposterior aan toe.
Elke branch bewaart zijn eigen identity paths, lokale-deformatiegeheugen en
contigue beslissingen. Een begrensde N-scan-horizon commit alleen de oudste
MAP-ancestor; recentere beslissingen blijven herzienbaar. De posterior bindt
conditionele onzekerheid, volledige candidate/subproblem/branch-accounting en
niet-geselecteerde split/merge-lineage. Hij gebruikt geen harde speedgate. De
exacte 36-frame-uitkomst, inclusief de objectief slechtere sprongmetrics en de
volledige child-resourceverdeling, staat in
`docs/SCIENCE_SHADOW_MULTI_SCAN_REPLAY.md`.

## Samengestelde bestaande primitives

De compositor dupliceert geen detector, flow-estimator, associatiesolver,
trackfilter of WeatherSystem-algoritme. Hij gebruikt rechtstreeks:

1. `detect_hierarchical_cells()` voor rain shields en iedere embedded core;
2. `estimate_lucas_kanade_flow()` plus
   `aggregate_detection_flow_sample()` als expliciete cold-start challenger;
3. `build_two_position_track_prior()` zodra twee geassocieerde posities
   bestaan; die exacte intervalsnelheid wordt nooit begrensd of afgekeurd;
4. `build_detection_association_graph()` én
   `build_association_feature_table()` voor de volledige vorige×huidige
   cross-product;
5. `solve_bounded_k_best_global_assignment()` voor de v2
   multi-hypothesispolicy: een begrensde Murty/Lawler-zoektocht met exacte
   Hungarian subsolver, volledige candidate-accounting en expliciete
   subprobleemcapaciteit; de oudere one-best/exact-DP-paden blijven alleen
   beschikbaar wanneer die policy niet is ingeschakeld;
6. de immutable `cell_track_lifecycle`-primitives voor continuation, birth,
   gap, reacquisition, death, split en merge;
7. `assimilate_recursive_detection()` voor een ungated recursive
   constant-velocity estimate met covariantie en bewaarde innovatie;
8. `group_weather_systems()` als uitsluitend afgeleide systeemview.

Iedere rain shield en iedere embedded core krijgt dus een eigen CellTrack. Een
WeatherSystem is nooit de primaire identiteit. V2 kan zijn descriptieve
translation alleen als zachte prior gebruiken wanneer een expliciet minimum
aan volwassen steuntracks aanwezig is; de assignment-onafhankelijke
hele-scene translation blijft de fallback. Dit is de formele uitvoering van
de “hondenlijn met puppies”-analogie: lokale cores mogen onderling anders
bewegen dan de gedeelde systeemtranslation.

## Immutable state

`CellTrackEngineState` bindt één exact policyfingerprint en bewaart:

- de volledige append-only `CellTrackLedger`, inclusief ended parents;
- precies één `RecursiveTrackEstimate` per bestaande track;
- de laatste hiërarchische detectie en footprint per track;
- het laatste genormaliseerde raster en detectieframe;
- de laatste uitsluitend afgeleide WeatherSystem-snapshot;
- een strikt oplopende frame-revision.

De state valideert dat ledger, estimator en hiërarchische latest detection
exact dezelfde track- en detection-ID's afdekken. Ended tracks, ouderdetecties,
lineage en estimatorbewijs worden niet stil verwijderd. Een frame moet een
unieke ID en een strikt latere meteorologische `valid_at` hebben.

Iedere transition-ID bindt de volledige genormaliseerde rasterwaarden en
gridmetadata, de kernstate met ledger-/estimatorrevisies, het policyfingerprint
en alle lineagehypothesekosten/provenance. Eenzelfde frame-ID en footprint met
gewijzigde intensiteitsbytes levert daardoor niet stil dezelfde
transitionidentiteit op. Dit is in-memory replaybinding, nog geen extern
duurzaam receipt of persistencecontract.

## Eén frameovergang

`advance_cell_track_engine()` voert deterministisch uit:

1. immutable policy-, tijd-, raster- en stateboundcontrole;
2. hiërarchische detectie van alle objecten;
3. expliciete provider/grid/quantity/unit-discontinuïteitscontrole;
4. Lucas–Kanade-evidence voor tracks uit het direct vorige raster;
5. per actieve track een prior:
   - last-two exact interval bij minstens twee posities;
   - beschikbare Lucas–Kanade-vector bij één direct vorige positie;
   - anders een caller-supplied brede zero-motion cold start;
6. per latest-detectiontijdcohort een complete graph en feature table, zodat
   ook coasting tracks met een oudere laatste detectie kunnen heropduiken;
7. één complete kostenrij per kandidaat, zonder snelheids-, afstands-,
   innovatie- of plausibility reject;
8. begrensde top-k global assignment met births en deaths, plus conditionele
   gewichten waarvan omitted mass expliciet onbekend kan blijven;
9. atomische immutable lifecycle- en estimatorrevisies;
10. een nieuwe afgeleide WeatherSystem-view van uitsluitend op het actuele
    frame geobserveerde actieve tracks, inclusief de bewaarde
    identiteitsalternatieven.

De associationfeatures bewaren onder meer observed interval speed,
last-two/flowprovenance, residual, innovationcovariantie, Mahalanobis-term,
area ratio, intensiteitsverschil, footprint-IoU, objectsoort en providerbron.
Extreme waarden blijven kandidaten en kunnen door de exacte assignment gekozen
worden. De recursive estimator assimileert een gekozen extreme innovatie
zonder gate en bewaart tegelijk de exacte latest interval velocity.

## Gaps en reacquisition

Een unmatched track krijgt een expliciet `gap`-event en blijft volgens de
caller-owned lifecyclepolicy `coasting`. Op een later frame doet zijn laatste
detectie opnieuw mee aan de volledige cross-product. Een gekozen match wordt
een `reacquired`-event op dezelfde track-ID. Overschrijding van de expliciete
gaplimiet eindigt de track; ze wordt niet uit de ledger gewist.

## Split en merge

De engine accepteert nog steeds caller-supplied immutable
`CellTrackLineageHypothesis`-records, maar v2 leidt daarnaast begrensde,
**niet-geselecteerde** alternatieven af uit maximale same-kind
translated-footprint-overlap. Er is maximaal één splitgroep per vorig object
en één mergegroep per huidig object; gaps zijn niet lineage-eligible en er is
geen powersetzoektocht.

Iedere hypothese bewaart:

- hypothesis-ID en type;
- exacte parent track-ID's;
- exacte current detection-ID → nieuwe child track-ID mapping;
- scalar cost, volledig uitgesplitste costcomponenten en costmodel-ID;
- niet-lege provenance-ID's;
- een expliciete selectie; automatisch afgeleide hypothesen staan altijd uit.

Alle hypothesen en hun kosten blijven in het transitionresultaat en de durable
receipt. Alleen geselecteerde, onderling niet-overlappende callerhypothesen
worden uitgevoerd. Split
eindigt exact één parent en creëert minstens twee nieuwe children. Merge eindigt
minstens twee parents en creëert exact één nieuwe child. Parenttracks en hun
estimates blijven historisch aanwezig. Child-estimators starten bewust met de
brede caller-supplied zero-motion prior: snelheid over een split/merge wordt
niet zonder apart gekalibreerd bewijs geërfd of gemiddeld.

## Capaciteit en `indeterminate`

Het policycontract begrenst rasterpixels, gedetecteerde objecten, actieve en
totale tracks, candidate edges, retained local candidates, top-k solutions,
assignment-subproblemen, lineagehypothesen/-leden, lifecycleevents en
detectiehistorie per track. De volledige graph/feature-cross-product wordt
nooit eerst geometrisch uitgedund.

Bij overschrijding van een geldige computationele bound:

- is de batchstatus expliciet `indeterminate`;
- blijft `next_state` exact hetzelfde immutable stateobject;
- blijft het volledige bounded detectieframe in het resultaat;
- blijven reeds berekende graph, features, priors en assignmentproblem waar
  mogelijk behouden;
- worden geen births, deaths, gaps, tracks of partial assignments geschreven;
- wordt standaard nooit stil naar greedy/Hungarian one-best of een kleinere
  kandidatenset teruggevallen.

`scalable_one_best_fallback=True` blijft uitsluitend een oude, afzonderlijke
fingerprint-gebonden keuze wanneer de multi-hypothesispolicy uit staat. De
v4.1 calibrationpolicy zet die fallback uit en gebruikt top-k 4 met maximaal
4096 Murty/Lawler-subproblemen. Als vier exacte oplossingen binnen die grens
niet bewezen kunnen worden, volgt `indeterminate`; er wordt niet stil naar
one-best teruggevallen.

De rasterpixelbound is de harde input/output-envelope: een hiërarchische
detector kan per rasterpixel maximaal één shield- en één coreobject leveren.
WeatherSystem-pairevidence gebruikt dezelfde expliciete pairbound.

## Provider- en gridhandoff: bewuste admission-gate

Bron-, analysis-grid-, quantity- of unitwissel is een
meetdiscontinuïteit. Zonder `ProviderHandoffEvent` wordt dit expliciet als
ontbrekend bewijs gemeld. Ook met zo'n event blijft de eerste kandidaat
`indeterminate`: de bestaande lifecycle en estimator vereisen één vast metric
grid en het handoffcontract verbiedt cross-handoff velocity updates.

Voor veilige toelating ontbreekt nog een geregistreerde **position-only state
transform** die:

1. confirmed/provisional/indeterminate handoffcontinuïteit per track bewaart;
2. sourcepositie en volledige covariantie naar het targetgrid transformeert;
3. geen velocity uit coördinaten aan weerszijden van de handoff berekent;
4. alle targetkandidaten en alternatieven behoudt;
5. een nieuwe estimatorrevision met expliciete discontinuïteitsprovenance
   maakt.

Tot die primitive wetenschappelijk bewezen is, worden zowel oude tracks als
nieuwe detecties behouden maar niet gekoppeld. Een productwissel die niet in
`NormalizedRasterFrame.source`, quantity, unit of grid zichtbaar is, kan deze
compositor niet herkennen; de toekomstige capture-adapter moet daarom een
provider/product-identiteit in `source` canoniek binden.

## Bewijs

`tests/test_cell_track_engine_shadow.py` bewijst:

- exact lineair interval na twee frames en last-two prior op het volgende;
- één track over 25 frames;
- afzonderlijke puppytracks plus een secundaire rain-shield systeembeweging;
- twee kruisende cellen die hun last-two identiteit behouden;
- expliciete split en merge met nieuwe children en ended parents;
- gap plus reacquisition op dezelfde track-ID;
- een extreem hoge snelheid die kandidaat blijft en gekozen kan worden;
- systeemtranslatie apart van lokale celdeformatie;
- bounded top-k gelijk aan een exhaustieve kleine exact-DP-oracle;
- volledige candidate-accounting en conditionele/omitted-mass accounting;
- automatisch afgeleide maar nooit geselecteerde split/merge-alternatieven;
- expliciete subprobleem- en local-candidate-capaciteitsfouten;
- capacity overflow als lossless `indeterminate` batch;
- expliciete schaalbare one-best-opt-in boven de top-k-bound, met alle
  candidates en zonder verzonnen alternatieven;
- afzonderlijke harde bound op de secundaire WeatherSystem-pairoutput;
- werkelijk beschikbare Lucas–Kanade cold-startprovenance;
- deterministische replay en canonieke hypothesevolgorde;
- strikt oplopende frametimestamps zonder statewijziging bij invalid input;
- immutable inputs/states;
- expliciete fail-closed providerwissel;
- afwezigheid van imports of wiring naar service, HA en legacy engine.

De v3-aanvulling bewijst daarnaast crossings met meerdere scanbranches,
N-scan-commit, lokale deformatie naast gedeelde translation, missing detection,
begrensde en exact gebonden split/merge-alternatieven, expliciete
branch/candidate/subproblem-capaciteitsfouten, deterministische replay en een
1.024-frame plateau van de gesloten multi-scanposterior.

Deze tests zijn synthetisch shadowbewijs, geen operationele meteorologische
validatie. Kalibratie van association-, birth/death- en lineagekosten tegen een
onafhankelijke gelabelde corpus, providerhandoffvalidatie, filesystemdurability
en langdurige resource-soak blijven admission-gates.

## Duurzame shadow-orchestratie

De afzonderlijke worker-only v2-orchestrator bewaart inmiddels een exact
checksummed `CellTrackEngineState`-checkpoint en alleen de contiguous
replaytail van genormaliseerde inputs. Daardoor is de vroegere 512-frame
restart-/levensduurgrens verwijderd. Dit verandert de kandidaatstatus van de
engine niet en activeert niets live. Het volledige atomaire opslag-,
retentie-, privacy- en restartcontract staat in
`docs/SCIENCE_SHADOW_CHECKPOINT_RETENTION.md`.

## Ongewirede harde procesgrens

Een afzonderlijke candidate runner kan de volledige duurzame science-run nu
in een vers `spawn`-proces uitvoeren met parent-owned wall/CPU/RSS/outputbounds
en harde process(group)-kill. Na iedere exit wordt de receipt uitsluitend uit
de durable store herladen; kill vóór commit bewaart de oude head en kill ná
commit levert de exacte replaybare receipt. Dit wijzigt de meteorologische
kandidaatstatus en admission van CellTrackEngine niet. Het authority-, IPC-,
privacy-, terminalstatus- en kill-racecontract staat in
`docs/SCIENCE_SHADOW_PROCESS_ISOLATION.md`.
