# Blitzortung exact-raw workeringress (shadowcontract)

## Status

`storm_tracker_worker/blitzortung_raw_ingress.py` is een nieuwe, volledig
onbedrade workergrens. Hij vervangt de bestaande Home Assistant-owned
Blitzortung-provider nog niet en wordt niet geïmporteerd door de worker-service,
providerregistry, configuratie of HA-integratie.

De grens doet precies één ding: één reeds ontvangen MQTT-delivery als exacte
bytes duurzaam opslaan in de schema-3 `RawArtifactStore`, vóór enige
JSON-parse of meteorologische interpretatie. De module bevat daarom bewust
geen MQTT-client, netwerkcode, scheduler, retrylus, in-memory queue,
servicehandler of decoder.

`operational_admission` blijft letterlijk `false`.

## Waarom de huidige HA-stroom niet meteorologisch volledig is

De publieke Blitzortung-feed wordt via MQTT QoS 0 geleverd. Zo'n delivery is
best effort:

- de broker bevestigt de delivery niet;
- een disconnect of capaciteitsprobleem kan berichten verliezen;
- het ontbreken van een bericht bewijst niet dat er geen bliksem was;
- één ontvangen bericht zegt niets over de volledigheid van de stroom vóór of
  na dat bericht.

Daarom bindt iedere raw receipt exact:

```text
transport_completeness = best_effort_qos0_unacknowledged
absence_semantics      = unknown_transport_coverage
```

Geen downstreamstap mag een lege periode, ingressfout, opslagfout, disconnect
of capacity reject omzetten in `no_lightning`.

## Invoercontract

`BlitzortungMqttEnvelope` bevat alleen:

- de exacte immutable `payload: bytes`;
- SHA-256-identiteiten voor broker, sessie en delivery;
- het concrete ontvangen topic (geen `+` of `#` subscriptionwildcards);
- `qos`, dat exact integer `0` moet zijn;
- de exacte boolean `retain`-vlag;
- een eindig niet-negatief `received_at`;
- de SHA-256 van de actieve desired state;
- de SHA-256 van de actieve subscriptionset.

Broker-, sessie- en delivery-identiteiten zijn uitsluitend lower-case
SHA-256-waarden. Er bestaat geen vrije provenance-input en er zijn geen velden
voor tokens, wachtwoorden, brokercredentials of andere secrets. Het topic moet
een concreet publiek `blitzortung/1.1/...`-topic zijn en credentialvormige
topicsegmenten worden geweigerd.

Harde ingressgrenzen:

- payload: maximaal 256 KiB;
- topic: maximaal 512 UTF-8-bytes;
- alle identiteits- en statebindingen: exact één lower-case SHA-256;
- QoS anders dan 0: fail closed;
- bytearray, memoryview of andere mutable payloadvormen: fail closed.

Een lege MQTT-payload wordt wel exact opgeslagen. De ingress interpreteert
die niet als een lege observatieset en al helemaal niet als afwezigheid van
bliksem.

## Duurzame volgorde

De opslagvolgorde is bewust:

1. valideer uitsluitend envelopevorm, typen en harde bounds;
2. bereken een stabiele delivery key uit broker-, sessie- en
   delivery-identiteit;
3. roep `RawArtifactStore.store(...)` aan met de exacte payload;
4. commit daarna één schema-3 capture met rol `mqtt_payload`;
5. geef pas na beide commits een payloadvrije, immutable reference terug.

De module roept nergens `json.loads`, een Blitzortung-decoder of een
meteorologische classificatie aan. Een crash tussen stap 3 en stap 4 kan dus
een geldige losse raw receipt achterlaten, maar nooit een complete capture of
weerclaim. Een identieke retry hergebruikt de receipt en voltooit de capture.

De receiptprovenance bindt onder meer:

- topic, QoS, retain en received_at;
- het vaste MQTT 3.1.1-deliveryprofiel van deze ingressgrens;
- broker-, sessie- en delivery-identiteit;
- desired-state- en subscription-set-hash;
- `representation=upstream_native_bytes`;
- `capture_stage=before_decode`;
- transportonvolledigheid en ontbrekende broker acknowledgement;
- `payload_parsed=false`;
- `meteorological_interpretation=not_performed`;
- `operational_admission=false`.

## Idempotentie en conflicten

De raw fetch-identiteit is stabiel voor dezelfde combinatie van broker,
sessie en delivery. Daardoor geldt:

- exact dezelfde retry geeft dezelfde receipt, capture en reference;
- gewijzigde bytes onder dezelfde delivery-identiteit geven zichtbaar een
  `RawArtifactConflictError`;
- gewijzigd topic, tijdstip, retain, desired state of subscriptionset onder
  dezelfde delivery-identiteit conflicteert eveneens;
- dezelfde bytes onder een andere echte delivery-identiteit krijgen een
  nieuwe receipt/capture, terwijl content-addressed objectopslag de bytes mag
  dedupliceren.

De teruggegeven `BlitzortungRawIngressReference` bevat geen payload en geen
filesystempad. Hij bevat wel content-, receipt-, provenance-, manifest- en
capturehashes en heeft een eigen content-bound `reference_sha256`.
De publieke referenceconstructor herberekent die checksum en weigert iedere
veld- of claimmutatie; `frozen=True` alleen wordt niet als integriteitsbewijs
beschouwd.

## Capacity en fouten

Er is geen queue en er wordt niets stil gedropt. Alle fouten van de
`RawArtifactStore`, waaronder object-, byte-, receipt- en capturecapacity,
integriteit en filesystemfouten, propageren ongewijzigd. Een caller moet die
later als zichtbare ingress/storage failure publiceren.

Als de raw member al duurzaam is maar de capturecatalogus volloopt, blijft die
member bewaard. Er volgt geen reference en geen weerclaim. Na expliciet
capaciteitsherstel kan dezelfde delivery veilig opnieuw worden aangeboden.

## Nog geblokkeerd vóór operationele inzet

Deze component lost uitsluitend de raw durability-grens op. De volgende zaken
zijn nadrukkelijk nog niet gebouwd of bewezen:

1. een worker-owned MQTT-client met begrensde socket- en readbuffers;
2. een duurzame, disk-backed scheduling/backpressuregrens zonder stille drop;
3. een stabiele sessie- en delivery-identiteitsuitgifte rond QoS 0;
4. reconnect/disconnect/gap- en abonnementsovergangsreceipts;
5. capaciteitstelemetrie en operator-alarm vóór de grens wordt bereikt;
6. een geïsoleerde en begrensde JSON/Blitzortung-decoder die uitsluitend een
   geverifieerde schema-3 capture leest;
7. duplicaat- en event-timebeleid plus held-out validatie;
8. shadowreplay en langdurige fault/thermal/HA-Z2M soak;
9. gecontroleerde cutover waarbij de HA-owned paho-client en zijn onbegrensde
   `asyncio.Queue` pas na bewijs worden verwijderd.

Tot al deze gates dicht zijn, blijft deze module shadow-only, unwired en
`operational_admission=false`.

## Testbewijs

`tests/test_worker_blitzortung_raw_ingress.py` bewijst onder meer:

- exacte binaire bytes, inclusief ongeldige JSON, worden ongewijzigd gelezen;
- geen parser-, MQTT-, netwerk-, queue- of runtimewiring in de module;
- payloadvrije, immutable en content-bound references;
- idempotente retry en zichtbare payload-/metadataconflicten;
- volledige topic/QoS/retain/tijd/state/subscription/provenancebinding;
- harde payload-, topic-, type-, hash- en QoS-bounds;
- detectie van gemuteerde objectbytes;
- zichtbare object- en capturecapacity en zichtbare storage-`OSError`;
- afwezigheid kan nergens `no_lightning` worden.
