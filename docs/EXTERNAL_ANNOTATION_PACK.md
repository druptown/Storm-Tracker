# External annotation pack

## Doel en grens

`storm_tracker_worker.external_annotation_pack` maakt een reproduceerbare,
read-only inventaris van caller-geselecteerde meteorologische eventvensters.
De export maakt **geen** labels, kiest **geen** marges of drempels en berekent
**geen** validatiemetrieken. De afzonderlijke template is alleen een leeg
formulier voor externe annotatie.

Dit is een offline onderzoeksinstrument. Het is niet in de live worker of Home
Assistant-pipeline aangesloten en schrijft niets naar de kalibratie- of
evidencedatabase.

## Caller-owned protocol

Het request moet alle keuzes expliciet bevatten:

- een request- en brondatabase-identiteit;
- `UTC_UNIX_SECONDS`;
- protocol-ID, protocolversie en SHA-256 van de instructies;
- verplichte blinding, toegelaten externe brontypes en annotatorrollen;
- een volledig gespecificeerd event-held-out splitbeleid zonder defaults;
- per event: UTC-venster, alle bekende leakagegroepen, providers en gewenste
  evidenceklassen;
- per frame: tijd, grid, variabele en eenheid;
- targets en exact gevraagde labeltypes.

Frames worden nooit los gesplitst. Events die via een leakagegroep verbonden
zijn, blijven als één transitieve component in dezelfde train-, validatie- of
testpartitie.

## Read-only export

De exporter:

1. resolveert een bestaand SQLite-bestand;
2. weigert een niet-lege `-wal` of `-journal`, omdat dat geen zelfstandige
   immutable snapshot is;
3. berekent SHA-256 en bestandsgrootte vóór het openen;
4. opent via SQLite URI `mode=ro&immutable=1` en activeert `query_only`;
5. eist een geslaagde `integrity_check` en lege `foreign_key_check`;
6. leest uitsluitend de aangevraagde providers en eventvensters;
7. sluit de verbinding;
8. berekent SHA-256 en grootte opnieuw en weigert elke wijziging.

De pack legt de database-SHA, schema-SHA, tabelnamen en connection mode vast.
Elke evidence-reference bevat event, provider/targetscope, brontabel en
record-ID, waarnemings-/ontvangsttijd, record-SHA, eventuele artifact-SHA,
quantity, unit, grid en canonieke metadata.

Engine-output mag als verborgen auditcontext worden opgenomen, maar krijgt
altijd `engine_context_not_truth` en `annotator_visibility=prohibited`.
Legacy provider-overlays worden expliciet als niet-exacte upstream bytes
gemarkeerd. Ontbrekende evidence wordt per event/provider en, voor
grondmetingen, per exact site-target vastgelegd; niets wordt als wet/dry of
waarheid gepromoveerd.

## Immutabiliteit

De pack-SHA bindt request, protocol, split, bronmanifest, evidence,
missing-evidence en templates. Bij het opnieuw laden worden niet alleen de
SHA maar ook de semantische afleidingen gecontroleerd:

- split moet opnieuw exact uit events en splitbeleid volgen;
- evidence moet het eventvenster overlappen en binnen aangevraagde
  provider/target/kind-scope vallen;
- evidence-ID's moeten content-addressed zijn;
- missing-evidence en templates moeten exact opnieuw afleidbaar zijn;
- een template kan alleen zichtbare references uit zijn eigen event bevatten.

Een aanvaller kan een gemanipuleerde pack dus niet geldig maken door alleen
een nieuwe top-level SHA te berekenen.

## Externe invulling en fail-closed ingest

Een submission moet elk template exact eenmaal bevatten en de vaste
event/frame/target/kind/reference-identiteit ongewijzigd herhalen. Per bron
worden onder andere brontype, producer, dataset- en methodologieversie,
artifact-SHA, onafhankelijkheids- en blindingattestatie, annotatoren en de
exacte protocol-ID/versie/instructie-SHA vereist.

Per annotatie worden vastgelegd en gecontroleerd:

- source, annotator en protocolrol;
- start-, eind- en submissiontijd;
- blinding;
- de gesorteerde subset van pack-evidence die werkelijk werd gebruikt;
- label state en exact getypeerde payload;
- of een expliciete reden waarom geen label kon worden gemaakt.

Vaste tijden, units, grid- en targetvelden kunnen niet worden gewijzigd.
Engine-derived bronnen, een bronartifact gelijk aan de candidate-DB/engine
artifact, onbekende references, niet-onafhankelijke bronnen, ongeldige
blinding, extra velden, ontbrekende templates, duplicaten, `NaN` en
semantisch ongeldige payloads worden geweigerd.

Een unresolved template maakt geen truth-label aan. Het resultaat bevat:

- de SHA van de pack;
- een SHA van het getypeerde labelcorpus;
- provenance inclusief rol en gebruikte evidence;
- unresolved template-ID's;
- een `annotation_ingest_sha256` die corpus én provenance bindt.

## CLI

Export:

```text
python scripts/external_annotation_pack.py export \
  --database <snapshot.sqlite3> \
  --request <request.json> \
  --pack-out <pack.json> \
  --template-out <empty-template.json>
```

Ingest:

```text
python scripts/external_annotation_pack.py ingest \
  --pack <pack.json> \
  --submission <filled-template.json> \
  --output <ingest.json>
```

Outputbestanden worden met exclusieve create-semantiek geschreven; bestaande
bestanden worden nooit overschreven.

## Bewuste resterende grens

De ingest controleert de gedeclareerde SHA en provenancevelden, maar ontvangt
de bytes van een extern bronartifact niet. Voor vrijgave moet dat artifact
apart immutable worden gearchiveerd en moet zijn SHA onafhankelijk tegen
`source_artifact_sha256` worden gecontroleerd. De pack bevat ook references,
niet automatisch alle zware radar-/gridbytes; annotatietooling moet die via
de vastgelegde database-SHA en record-ID's tonen zonder candidate-output te
onthullen.

