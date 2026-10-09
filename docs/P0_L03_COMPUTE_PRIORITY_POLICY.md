# P0-L03 — niet-evictend compute-prioriteitsbeleid

## Status en reikwijdte

De lokale HA- en workertrackingkern gebruiken
`target_distance_non_evicting_v1`. Dit contract lost het lokale
`MAX_STORMS`-gat op zonder een meteorologische selectiedrempel te introduceren.
Het bepaalt alleen de volgorde waarin een caller dure afgeleide berekeningen
kan plannen. Het verwijdert, weigert of verbergt geen observation, detection,
track, history of forecastinput.

Dit document maakt geen claim over live NUC-doorlooptijd, process-RAM,
temperatuur of thermalthrottling. Die eigenschappen vereisen een live soak met
de uiteindelijke dure nowcastconsument en representatieve providerdata.

## Niet-onderhandelbare invarianten

1. `max_storms` is een legacy capaciteitsrichtwaarde en nooit een ingestfilter.
2. Iedere door de trackingkern aangemaakte track staat in het volledige plan.
3. Iedere track blijft in `export_tracking_state()` aanwezig.
4. Iedere track behoudt zijn eigen historysleutel.
5. Iedere track blijft forecastinput; onvoldoende bewegingsevidence mag nog
   steeds een inhoudelijk `indeterminate` forecastresultaat geven.
6. Een tier mag de computevolgorde veranderen, maar nooit trackingstate
   evicten.
7. Een targetwijziging berekent relevantie opnieuw vanaf de actuele
   trackingstate. `background` is dus geen persistente status.
8. Reguliere lifecycle-expiry en een expliciete region-teardown blijven
   afzonderlijke, zichtbare verwijderpaden; zij zijn geen tiering.

## Publiek lokaal contract

`StormEngine.build_compute_schedule(...)` verwacht:

- een mapping `target_id -> (latitude, longitude)`;
- een expliciete, niet-negatieve `relevance_radius_km`;
- optioneel een positieve `foreground_slots`.

Wanneer `foreground_slots` ontbreekt, wordt de bestaande `max_storms`-waarde
alleen als voorkeursgrootte voor pass 0 gebruikt. Alle overige tracks staan in
pass 1, 2, enzovoort en hebben `execution_required=true`. `deferred_advisory`
betekent dus nooit verworpen of geëvict.

De caller levert de relevantieradius. Het contract interpreteert die straal
uitsluitend als operationele computeafstand, niet als onweers-, regen-,
impact- of waarschijnlijkheidsgrens.

## Afstand en rangschikking

Per track en target wordt de afstand als volgt bepaald:

1. de dichtste rand van de meest recente radarfootprint;
2. alleen als die ontbreekt: grootcirkelafstand tot het actuele centroid.

Oude radarfootprints bepalen de actuele computeprioriteit niet. Target-ID's
worden eerst lexicografisch genormaliseerd, zodat invoervolgorde geen invloed
heeft.

De vaste tierorde is:

1. `relevant`: actuele afstand kleiner dan of gelijk aan de expliciete
   relevantieradius, ongeacht confidence;
2. `background`: buiten die radius en niet low-confidence;
3. `low_confidence`: buiten die radius en dormant of inhoudelijk
   `Onvoldoende data`/`Laag`.

Targetrelevantie staat bewust vóór confidence. Een pas ontdekte bui boven een
tracker mag dus niet achter vijftien oudere, verre systemen terechtkomen.
Binnen een tier wordt gesorteerd op ongeronde targetafstand en daarna op
`storm_id`. Daarmee is ook iedere tie reproduceerbaar.

## Zichtbare diagnostics

Het resultaat bevat onder meer:

- policy- en schedulefingerprint;
- genormaliseerde targets en expliciete radius;
- geconfigureerde `max_storms` en de rol
  `scheduling_guideline_only`;
- tracked/history/export/forecastinput/scheduled counts;
- tier- en voorkeurs-passcounts;
- het aantal geëvalueerde track-targetparen;
- de canonieke JSON-grootte van het plan;
- de volledige schedulelijst met rang, tier, afstandsbasis en pass;
- positieve invarianten voor volledige scheduling, export, historypariteit,
  unieke IDs en niet-evictende capaciteit;
- expliciete `false`-claims voor wall-clockbenchmark, process-RAM,
  NUC-thermiek en meteorologische probability.

De policyfingerprint bindt de vaste semantiek. De schedulefingerprint bindt
daarnaast radius, slots, genormaliseerde targets en de volledige geordende
schedule.

## Runtimewiring

`RegionEngine` bouwt het plan nu uitsluitend in muterende runtimepaden:

- onmiddellijk na targettoewijzing of -verplaatsing;
- na iedere lokaal of door de worker gecommitteerde observationbatch;
- na herstel van de persistente MCS-/trackingstate.

De opgeslagen snapshot bindt de trackingrevision, targetset, region-ID,
regionale observatieradius en een monotone schedulesnapshotrevision.
`RegionEngineSensor` leest alleen een begrensde, vooraf berekende kopie:
maximaal 25 entries plus total/overflow. Een dashboardread herberekent niets
en muteert geen state.

Wanneer een interne schedulerfout optreedt, wordt het gecommitteerde
observationbatch niet teruggedraaid of opnieuw aangeboden. De runtime
publiceert dan `fallback_all_tracks`: iedere bekende track blijft exact één
keer `execution_required` en `forecast_input_retained`, maar zonder een
meteorologische prioriteitsclaim. De foutcategorie en verplichte fallback zijn
zichtbaar. Dit is fail-open voor databehoud en fail-closed voor
prioriteitssemantiek.

## Gerichte verificatie

De lokale tests bewijzen:

- vijf lightning- en vier radarobjects blijven behouden boven een kunstmatig
  lage capaciteit;
- vijftien verre ruisobjects gevolgd door een zestiende bui exact op het
  target leveren zestien tracks, zestien histories, zestien exports en zestien
  forecastinputs; de targetbui heeft rang 1;
- een `background`-track wordt na targetverplaatsing opnieuw `relevant`, zonder
  trackingstate te muteren;
- een deterministische microreplay met 96 onafhankelijke radarobjects, waarvan
  24 targetrelevant, twee targets en een vaste klok bewaart alle 96 tracks;
- omgekeerde targetinvoervolgorde levert bytegelijk diagnostics en dezelfde
  fingerprint;
- de microreplay telt exact 192 track-targetparen, zeven voorkeurspasses bij
  vijftien slots en een canonieke schedule kleiner dan 96 KiB;
- HA- en worker-`storm_engine.py` zijn byte-identiek;
- een echte `RegionEngine` commit dertig ruimtelijk onafhankelijke
  radarobjects en publiceert daarna 30/30 tracking-, schedule- en
  forecastinputrecords, verdeeld over twee voorkeurspasses;
- twee opeenvolgende sensorreads leveren dezelfde begrensde snapshot zonder
  revisionwijziging;
- een geïnjecteerde planningsfout levert een deterministische execute-all
  fallback zonder één track weg te laten.

Reproduceer de gerichte replay met:

```text
python -c "import sys; sys.path.insert(0, r'.testdeps'); import pytest; raise SystemExit(pytest.main(['tests/test_storm_engine.py::test_p0_l03_deterministic_peak_resource_microreplay','-q','-W','error']))"
```

## Wat de lokale microreplay niet bewijst

De replay gebruikt synthetische maar geldige onafhankelijke radarcellen. Hij
meet volledigheid, determinisme, rangschikking en de grootte van het
diagnosticdocument. Hij meet geen echte providerdecode, workerqueue,
MQTT-overdracht, wall-clockdeadline, RSS, CPU-per-core, Home
Assistant-responsiviteit of NUC-temperatuur.

Voor release blijven daarom nodig:

1. consumptie van alle voorkeurspasses door de uiteindelijke dure
   STEPS/LINDA-forecastjobs, met aantoonbare voortgang onder druk;
2. live replay met echte immutable providerartefacts;
3. langdurige queue-, CPU-, RSS- en thermische soak op de NUC;
4. bewijs dat iedere voorkeurspass onder druk voortgang maakt;
5. zichtbare runtime-diagnostics in Home Assistant;
6. rollbacktest.
