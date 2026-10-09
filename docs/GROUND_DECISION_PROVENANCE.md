# Ground-decision provenancecontract

Status: lokaal/shadow bewezen; niet live gewired of gedeployed.

## Probleem

Een opgeslagen grondbeslissing is niet reproduceerbaar wanneer alleen
station-ID, fysieke site en independence group overblijven. De uitkomst hangt
ook af van de exacte versie van:

1. de registryprojectie die tijdens de beslissing beschikbaar was;
2. de bovenliggende geverifieerde PhysicalSite-catalogus;
3. iedere aliasmapping waarmee een providerstation aan een fysieke site werd
   gekoppeld.

Zonder die drie identiteiten kan een latere cataloguscorrectie de betekenis
van een historische beslissing stil wijzigen.

## Getypeerde doorvoer

`PhysicalSiteCatalog.to_ground_fusion_registry()` draagt de volledige
catalogus-SHA-256 en elke aliasmapping-SHA-256 over aan
`PhysicalSiteRegistry`. De registry berekent daarnaast een eigen SHA-256 over
een canoniek gesorteerde projectie van:

- registryversie;
- catalogusfingerprint;
- fysieke site-ID, coördinaat en independence group;
- genormaliseerde provider-/stationalias;
- mappingfingerprint.

Iedere `GroundMatchAssessment` bewaart de exacte registryversie,
registryfingerprint, catalogusfingerprint en gebruikte mappingfingerprint.
`decide_ground_confirmation()` vereist de registry zelf en weigert
assessments die niet exact bij die registry horen. De uiteindelijke
`GroundConfirmationDecision` bewaart de registry- en catalogusidentiteit plus
de unieke set gebruikte mappingfingerprints.

Handmatig samengestelde registries zonder geverifieerde mappingfingerprints
blijven mogelijk voor pure algoritmetests. Zij mogen niet als duurzaam
geverifieerde operationele beslissing worden opgeslagen.

## Duurzame evidence

`MeteorologicalEvidenceStore.append_ground_decision()` vereist voor nieuwe
records:

- `registry_fingerprint`: exact 64 lowercase hextekens;
- `catalog_fingerprint`: optioneel, maar indien aanwezig hetzelfde
  SHA-256-contract;
- `mapping_fingerprint`: verplicht voor iedere individuele groundmatch.

De store gebruikt contract
`stv3-ground-decision-provenance-v1`. Registry- en catalogusfingerprint worden
met de oorspronkelijke beslissingsreden in `reason_json` opgeslagen. Iedere
mappingfingerprint wordt met de oorspronkelijke inputreden in
`input_reason_json` opgeslagen. Beide zijn canonieke JSON en vallen daardoor
onder:

- de immutable recordhash;
- de content-addressed record-ID;
- de `input_set_sha256` van de volledige beslissingsset;
- de bestaande SQLite-integriteits- en immutabilitycontroles.

Een wijziging van registry, catalogus of mapping kan dus niet dezelfde
historische beslissing simuleren. `trace_forecast()` decodeert het contract
en toont de drie fingerprintlagen plus de ongewijzigde redenen. Oude
schema-v3-records zonder dit contract blijven leesbaar en krijgen expliciet
`None` als fingerprint; ontbrekende provenance wordt nooit aangevuld met een
aanname.

De opslag gebruikt voorlopig canonieke, hashgedekte JSON-enveloppen zodat geen
onveilige in-place migratie van bestaande schema-v3-databases nodig is. Een
latere schema-upgrade mag deze waarden naar afzonderlijk geïndexeerde kolommen
normaliseren, maar moet byte- en ID-behoud van de oude records aantonen.

## Lokaal bewijs

De tests bewijzen:

- fingerprintdoorvoer van catalogus naar registry en alias;
- deterministische registryfingerprint;
- fail-closed weigering van gemengde catalogus-/registryassessments;
- roundtrip van registry-, catalogus- en meerdere mappingfingerprints door een
  volledige forecasttrace;
- weigering van ontbrekende en niet-canonieke SHA-256-waarden;
- integriteitscontrole na opslag;
- byte-identieke HA-/workermodules.

Gerichte controles:

```text
python -c "import sys; sys.path.insert(0, r'.testdeps'); import pytest; raise SystemExit(pytest.main(['tests/test_ground_fusion.py','tests/test_physical_site_catalog.py','tests/test_meteorological_evidence_store.py','-q','-W','error']))"
```

## Open externe/live poorten

Dit contract bewijst niet dat de huidige catalogusinhoud meteorologisch of
operationeel correct is. Nog vereist:

- autoritatieve, gearchiveerde crosswalks voor alle operationele providers;
- geverifieerde station moves, sensorhistoriek, capability en
  independence groups;
- duurzame cataloguswijzigings- en resolutie-audit events;
- replay met echte globale groundrecords en immutable radarartefacts;
- gecontroleerde runtimewiring in HA/worker;
- live drift-, conflict- en rollbacktests.
