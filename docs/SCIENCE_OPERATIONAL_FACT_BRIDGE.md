# Science → OperationalFact bridge (gesloten kandidaat)

Status: uitsluitend lokaal en niet-live. Deze module heeft geen import of
koppeling naar `service.py`, Home Assistant, MQTT, de snapshotpublisher of een
legacy-pad. `admission=false` voor alle echte replaydata.

## Resultaat en grens

`storm_tracker_worker.science_operational_fact_bridge` vormt een smalle grens
rond de bestaande `OperationalFactRevisionStore` en
`FactPublicationBinding`-contracten. De bridge kan één exact verzoek verwerken:

1. herlees de science-receiptrij via de bestaande duurzame
   `ScienceShadowReceiptStore`;
2. valideer opnieuw de canonieke receipthash, science request, attempt, run,
   engine, runtimeprofiel en volledige science-provenancehash;
3. herlees de actieve `DesiredState` en vergelijk state-SHA, fencing epoch,
   target-ID en exact punt/radius-footprint;
4. controleer `product_valid_at ≤ received_at ≤ evaluated_at < valid_until`,
   maximaal 15 minuten bronleeftijd, maximaal 5 minuten TTL en actuele klok;
5. stage alleen wanneer een expliciete authority dit exacte verzoek toelaat;
6. geef dan de bestaande `FactPublicationBinding` terug, zonder zelf te
   publiceren.

Het verzoek heeft een vast schema en bevat precies één target, één huidige
positieve regenwaarde, één provider en één regio. Het kan geen droogte,
`next_threat=none`, voorspelling, ETA, kans, groundclaim, UI-storm of kaartclaim
maken. Willekeurige attributes bestaan niet. Hashlijsten zijn uniek, gesorteerd
en begrensd tot 16 items; request- en authoritybytes zijn respectievelijk 32 en
64 KiB begrensd. Paden en coördinaten komen niet in resultaatdiagnostiek.

## Waarom DWD en KMI nul ready facts opleveren

De huidige `ScienceShadowReceipt` v2 legt als klasse-invariant vast:

- `admission_status=shadow_candidate_unadmitted`;
- `meteorological_admission_granted=false`;
- `process_isolation_certified=false`;
- `operational_snapshot_claim_generated=false`;
- vaste operationele blockers.

De productieachtige DWD- en KMI-replays gebruiken exact dit contract. De
`DurableScienceShadowFactAuthority` herverifieert hun duurzame rij, maar kan
nooit een admission bit omzetten. Het resultaat is dus `withheld`, zonder call
naar `OperationalFactRevisionStore.stage`; tests bewijzen voor beide labels
`ready=0` en `retained_rows=0`. Een toekomstige science-receiptversie wordt
niet impliciet vertrouwd: ook zonder blockers voegt deze v1-bridge de vaste
blocker `science_receipt_contract_not_admitted_by_bridge` toe.

## Positief bewijs is uitsluitend synthetisch

Een afzonderlijk canoniek JSON-contract
`stv3-science-operational-fact-synthetic-test-receipt-v1` bewijst alleen dat de
opslag- en bindingmechaniek werkt. De positieve branch vereist tegelijk:

- exact het concrete `SyntheticScienceTestFileAuthority`-type;
- een absoluut, regulier, niet-symlink, content-addressed authoritybestand;
- `admitted_for_test_only=true` en `meteorological_truth_claimed=false`;
- authority-ID, desired producer, enig target, provider en regio met zichtbare
  prefix `synthetic-test:`;
- exacte binding van claimhash, science-receipt, processreceipt,
  processauthority, runtimeprofiel, desired SHA/epoch, footprint,
  provenancehash, certificaten en vervaltijd.

Alleen dan bouwt de bridge één synthetische `current_raining`-snapshot met een
onbekende groundstatus en onbekende volgende dreiging. De drie materiële claims
(regio current, provider current en target raining) krijgen elk een bestaande
`FactAdmissionReceipt`. Dit is een softwaretest, geen meteorologische
waarheidsclaim en geen toegelaten generator voor echte data.

De `pipeline_item_id` is `science-fact:<bridge_request_sha256>`. Die
contenthash bindt het science-receipt en de volledige claim. Het synthetische
receipt bindt dezelfde claimhash en alle afzonderlijke authorities. De
`FactPublicationBinding` bindt daarnaast exact processreceipt, desired SHA,
epoch, snapshotrevision, revisionhash en snapshothash.

## Herstart, duplicate en fouten

- Een exacte duplicate vindt dezelfde content-addressed pipeline-ID, maakt geen
  tweede rij en retourneert `duplicate` met dezelfde binding.
- Een crash vóór SQLite-commit laat geen rij/head achter; retry kan exact één
  rij stagen.
- Een crash na commit laat de volledige oude-of-nieuwe transactie achter;
  restart classificeert dezelfde aanvraag als duplicate.
- Random/missing receipt, authority-hashdrift, canonical-JSON-drift, gewijzigde
  process/profile/provenance, stale desired authority, footprintdrift,
  expiry en byte/itemoverschrijding leveren geen ready fact.
- Secretachtige labels/waarden en onbegrensde JSON-structuren worden vóór de
  storeovergang geweigerd.

De bridge herleest DesiredState onmiddellijk vóór staging. Dat is voldoende
voor deze geïsoleerde proef, maar geen atomaire transactie over twee SQLite
stores. Een echte runtime moet de reeds ontworpen centrale ownership/fencing-
lifecycle gebruiken; deze module claimt die live lineariseerbaarheid niet.

## Ontbrekende certificaten voor echte admission

Voordat een echte DWD-, KMI- of andere providerreceipt operationeel mag worden,
ontbreken minimaal:

1. een nieuwe, expliciet toegelaten science/admission-contractversie; de bridge
   accepteert die niet automatisch;
2. een duurzaam hard-processreceipt dat als operationele admissionauthority is
   opgeslagen en exact aan receipt, authoritybestand en runtimeprofiel bindt;
3. provider/product-semantiek-, decoder-, grid-, nodata/undetect-, detector- en
   target-samplingcertificaten voor exact het product en tijdstip;
4. onafhankelijke meteorologische identity/speed-truth en held-out
   skillvalidatie; de huidige replays zijn robustness/shadowbewijs;
5. correcte target-footprint-/pixelintersectie en uncertaintybeleid, niet
   alleen de desired point/radius-identiteit;
6. grondwaarheid- en full-scanbewijzen waar het facttype die vereist;
7. gecertificeerde freshness/expirybeslisser inclusief providerklok en
   ontvangstketen;
8. centrale cross-store lifecycle/fencing, publication-reconciliatie en
   crash/leasebewijzen;
9. een resource-/thermische soak op de NUC die HA en Zigbee2MQTT aantoonbaar
   soepel houdt;
10. expliciete menselijke go-live-admission en afzonderlijke live wiring.

Tot al deze certificaten bestaan, blijft de echte branch fail-closed en is
`FactPublicationBinding` uitsluitend een niet-geconsumeerd testresultaat.

