# Blitzortung worker-eventingress: inert durable authority

## Uitkomst en status

`storm_tracker_worker/blitzortung_event_ingress.py` vult de ontbrekende
control-plane rond de bestaande exact-bytegrens
`blitzortung_raw_ingress.py` in. De nieuwe authority is lokaal, begrensd,
append-only, crash-herstelbaar en exclusief proces-owned. Hij is nadrukkelijk
geen MQTT-client en is nergens live bedraad.

De operationele status blijft:

```text
ingress_id                       = lightning.blitzortung.event_ingress
ownership_status                 = ownership_blocked
production_retention_authorized = false
operational_admission            = false
network_wired                    = false
decoder_wired                    = false
```

De repository bevat geen extern verifieerbare authority voor het duurzame
bewaarrecht van de publieke Blitzortung-feed en evenmin een verifieerbare
connection/message-provenanceketen. Een publieke delivery wordt daarom
fail-closed geweigerd voordat payloadbytes naar de raw store gaan. De
authority legt wél een payloadvrije, duurzame intent- en refusalreceipt vast,
zodat de weigering nooit stil op `geen bliksem` kan lijken.

Alleen `synthetic_fixture_mode=true` met een content-bound
`BlitzortungRetentionDecision` met scope `synthetic_fixture_only` kan de
exact-byte- en queuelogica in tests activeren. Een lokaal document met scope
`public_blitzortung` kan productie nooit vrijgeven: deze implementatie heeft
bewust geen boolean-, hash- of configuratiebypass voor ontbrekende externe
verificatie.

## Afbakening

De authority bevat geen:

- socket, DNS, TLS, paho- of andere MQTT-client;
- Home Assistant-, Zigbee2MQTT-, service- of schedulerimport;
- decoder of JSON-parse van de MQTT-payload;
- lightning-event, droge-periode- of afwezigheidsprojectie;
- automatische retentieopschoning of raw-objectverwijdering;
- live configuratie, deploy- of cutoverpad.

Hij wordt niet geïmporteerd door `service.py`, `acquisition_runtime.py` of de
Home Assistant-integratie. De huidige eigenaar verandert dus niet door het
bestaan van deze kandidaat.

## Onveranderlijke QoS 0-semantiek

Iedere controlereceipt en ieder publiek resultaat bindt exact:

```text
transport_completeness      = best_effort_qos0_unacknowledged
absence_semantics           = unknown_transport_coverage
meteorological_interpretation = not_performed
operational_admission       = false
```

QoS 0 levert geen acknowledgement- of replayvolledigheid. Daarom betekenen
de volgende toestanden allemaal `coverage unknown`, nooit afwezigheid van
bliksem:

- een stil verbonden interval;
- callback-sequencegaten;
- disconnect/reconnect;
- queuebackpressure;
- raw- of control-storagefouten;
- process crash of cancellation;
- een geweigerde delivery;
- een lege payload;
- een lege decoderuitkomst, zodra later een decoder bestaat.

De privacyveilige diagnostics gebruikt daarom
`silence_interpretation=unknown_not_an_absence_observation` en bevat geen
topic, payload, filesystempad, broker-, sessie- of deliveryidentiteit.

## Exacte ontvangstgrens vóór decode

Een synthetisch geautoriseerde delivery doorloopt de volgende volgorde:

1. valideer immutable `bytes`, concrete publieke Blitzortung-topic, QoS exact
   integer `0`, retain exact boolean, tijd, SHA-identiteiten, sessiebinding en
   harde payload/topicgrenzen;
2. schrijf een content-addressed `delivery_intent` zonder payload maar met
   payloadhash, lengte en exacte transportfacts;
3. roep `persist_blitzortung_mqtt_envelope(...)` aan, dat de ongewijzigde bytes
   vóór enige parse als schema-3 raw receipt/capture bewaart;
4. schrijf pas daarna `delivery_accepted` met de payloadvrije,
   content-bound raw reference;
5. voeg uitsluitend die reference toe aan de duurzame FIFO-queue.

Er is geen payloaddecoder in deze module. Een queuereference kan dus niet
bestaan zonder een eerder gecommitteerde exact-bytecapture.

Een process crash tussen stap 2 en 4 laat een pending intent achter. Bij de
volgende exclusieve open wordt deze als `crash_gap` gesloten. Als de raw
capture al bestond, blijft hij intact; een identieke callerretry gebruikt de
idempotente raw receipt/capture opnieuw. Er wordt nooit een weerclaim
gefabriceerd.

## Append-only receiptketen

Elke transitie is één canonical ASCII-JSON-document in `receipts/`:

```text
{sequence:020d}.{receipt_sha256}.json
```

De `receipt_sha256` bindt alle velden. Iedere receipt bindt ook de SHA-256 van
de voorgaande receipt. Bij open valideert de authority:

- exact de toegestane veldenset en eventsoort;
- canonical JSON zonder duplicate keys;
- bestandsnaam tegen contenthash;
- een ononderbroken reeks vanaf 1;
- de volledige vorige-receipthashketen;
- alle queue-, sessie-, claim-, payload- en accountinginvarianten;
- geen symlinks of onbekende files in eigen receipt/stagingmappen.

Nieuwe receipts worden eerst in de eigen stagingmap geschreven en
ge-`fsync`'d, daarna atomair gepubliceerd en op Linux met directory-`fsync`
verankerd. Een pre-publish crash laat alleen een herkenbare `.pending` stage
achter. Alleen de exclusieve volgende eigenaar ruimt zo'n eigen stage op. Een
post-publish crash wordt bij heropenen uit de content-addressed receipt
gereconstrueerd.

Windows kan geen directory-`fsync` leveren. Tests injecteren daar een expliciet
developmentbeleid; diagnostics houdt `directory_fsync_supported` zichtbaar.
Een release-/soakbewijs moet op de Linux workerstorage worden uitgevoerd.

## Sessies, reconnect en volgorde

`begin_session(...)` levert een immutable, content-bound handle met:

- monotone lokale `epoch`;
- broker- en upstream-sessionhash;
- desired-state- en subscriptionsethash;
- connection-provenancehash;
- een eigen bindinghash.

Een nieuwe sessie sluit een nog actieve vorige sessie eerst duurzaam als
`session_replaced`. Disconnect, transportfout, desired-statewissel,
operatorstop, cancellation en process restart gebruiken uitsluitend vaste,
privacyveilige reason codes. Iedere sessiesluiting verhoogt expliciet het
transport-gapaccounting.

De volledige nieuwe sessiehandle en de resterende epochcapaciteit worden
gevalideerd vóór `session_replaced` wordt geschreven. Een ongeldige reconnect
of een uitgeputte epochcap kan de nog geldige huidige sessie dus niet
beëindigen of een kunstmatig transportgat veroorzaken.

Per sessie bindt de caller een monotone lokale `callback_sequence`:

- `sequence > previous + 1`: duurzame `sequence_gap` met exact aantal
  ontbrekende callbackposities; diagnostics telt zowel de gapintervallen als
  de som van alle ontbrekende posities;
- nieuwe delivery met lagere sequence: raw bewaren, queueën en apart als
  `out_of_order` tellen;
- identieke delivery-identiteit en identieke fingerprint: duurzame
  `delivery_duplicate`, geen tweede queue-item;
- dezelfde identiteit met andere bytes/transportfacts: duurzame
  `delivery_conflict`, drop/gap tellen en zichtbaar falen.

Dit is lokale callbackaccounting. Het kan upstream QoS 0-verlies vóór de
callback niet detecteren en claimt dat ook niet.

## Bounded queue, claims en cancellation

De queue bevat alleen payloadvrije raw references en heeft een harde
`max_queue_entries`. Bij volle queue:

1. wordt de ontvangen delivery als payloadvrije intent vastgelegd;
2. volgt een duurzame `delivery_refused(reason=queue_capacity)`;
3. worden `drops` en `transport_gaps` verhoogd;
4. worden de payloadbytes niet aan de raw store aangeboden;
5. krijgt de caller een zichtbare `BlitzortungEventIngressBackpressure`.

Er is geen eviction, onbeperkte in-memory buffer of stil verlies.

De queue heeft exact één FIFO-claim tegelijk. Claims hebben een begrensde
lease en een content-bound token dat ook de consumeridentityhash bindt. Alleen
dezelfde consumer krijgt een nog actieve claim opnieuw; een andere consumer
krijgt geen token of raw reference. Release en completion aanvaarden alleen
de exact gereconstrueerde immutable claim, zodat een caller geen afwijkend
queue-item in een verder geldige receipt kan laten publiceren.

Completion verwijdert alleen het geclaimde head-item, vóór de harde
leasegrens, en verifieert de schema-3 capture plus exacte objectbytes opnieuw
aan die destructieve grens. Lease expiry, consumer cancellation/failure en
process restart schrijven eerst een `claim_released`-receipt en maken
hetzelfde item opnieuw claimbaar. Er wordt in deze kandidaat niets
gedecodeerd.

Een noodzakelijke expiry-release mag de afgescheiden recoveryreserve
gebruiken. Nieuwe claims blijven daarna wel onder de normale intakegrens
vallen; een verlopen lease kan dus niet vast blijven zitten enkel omdat de
control-plane intussen de normale capaciteitslatchedrempel heeft bereikt.

## Capaciteit en retentie

De profile bindt harde caps voor:

- queue-items;
- controlereceipts;
- bewaarde payloadentries;
- totaal door deze ingress geaccepteerde payloadbytes;
- sessie-epochs;
- claimlease en locktimeout.

Dezelfde initiële receipt bindt daarnaast de raw-storeprofielhash, een
privacyveilige hash van de absolute raw-storeroot en de volledige
retention-decisionhash. Heropenen tegen een andere raw store, gewijzigde caps
of een andere retentionbeslissing faalt vóór recovery/mutatie; er is geen
stille policy- of storewissel.

Een control-receiptreserve blijft apart voor crash-, session- en
claimrecovery. Zodra normale intake die reserve zou raken, schrijft de
authority één `control_capacity_latched`-receipt en weigert verdere intake.

Retentie is bewust `append_only_no_reclamation`. De authority verwijdert geen
controlereceipts en trekt geen schema-3 raw referenties in. Als entry-, byte-
of receiptcaps worden bereikt, volgt refusal; geen truncatie, eviction of
rollover. Dit houdt de kandidaat veilig, maar blokkeert productie totdat een
cross-store reference-counted lifecycle/reclamationauthority bestaat.

Bij raw-store-capacity of een andere raw-writefout wordt, zolang de aparte
control-plane nog schrijfbaar is, een duurzame refusalreceipt geschreven en
de originele fout opnieuw opgegooid. Als dezelfde fysieke storage zowel raw
als controlereceipts niet meer kan schrijven, kan software geen duurzame
receipt beloven; daarom zijn storage-isolatie, reserve, alarm en een
operatorrunbook expliciete promotiegates.

## Ownership en privacy

Eén authority houdt zijn lockfile gedurende zijn volledige levensduur:

- een in-process root-ownerregistry weigert een tweede instance;
- een niet-blokkerende Windows byte-range lock of POSIX `flock` schermt
  andere processen af;
- iedere operatie verifieert dezelfde PID;
- alle mutaties worden daarnaast binnen één re-entrant operation lock
  geserialiseerd;
- `close()` neemt dezelfde operation lock en kan procesownership dus niet
  halverwege een durable offer of claimtransitie intrekken;
- na een onderbroken publish wordt de instance poisoned en moet hij sluiten en
  heropenen vóór verder werk.

Diagnostics bevat alleen status, caps, aantallen, queue-/claimdiepte,
sessionepoch en vaste promotion gates. Payload, topic, paden, vrije
errorteksten en identiteits-/credentialhashes worden niet geëxporteerd.

## Exacte promotiegates

`PROMOTION_GATES` is de onveranderlijke lijst die ook in diagnostics en het
content-bound candidatecontract verschijnt:

1. `externally_verified_source_retention_rights_authority_missing`;
2. `externally_verified_connection_and_message_provenance_authority_missing`;
3. `bounded_worker_mqtt_transport_not_wired`;
4. `durable_decoder_and_event_time_validation_missing`;
5. `raw_and_control_receipt_reclamation_authority_missing`;
6. `capacity_alarm_and_operator_runbook_missing`;
7. `disconnect_loss_and_thermal_ha_z2m_soak_missing`;
8. `exclusive_legacy_ha_to_worker_cutover_not_proven`.

Geen unit test, lokale hash, synthetische fixture of succesvolle raw capture
kan één van deze productiegates verwijderen.

## Adversarieel testbewijs

`tests/test_worker_blitzortung_event_ingress.py` dekt onder meer:

- publieke source-retentie fail-closed met duurzame, payloadvrije refusal;
- exacte binaire bytes vóór queue/claim en zonder parse of weerclaim;
- invalid topic/QoS/retain/hash/tijd/mutable/oversized input vóór intent;
- queue-full, retained-bytecap en control-receiptreserve;
- raw-storage-full met duurzame refusal en foutpropagatie;
- duplicate, identityconflict, out-of-order en callback-sequencegap;
- monotone reconnect/sessionepochs, ongeldige replacementrefusal zonder
  sessieverlies en stiltesemantiek;
- crash/cancellation na intent en na raw-persist;
- crash vóór en na atomaire receiptpublish;
- restartrecovery van pending intent, actieve sessie en actieve claim;
- consumer-bound claims, vervalste claimfieldrefusal en harde lease-expiry;
- raw-objectherverificatie tussen claim en completion;
- receipt-/objecttamper, hashketen en canonical JSON;
- gelijktijdige threadoffers, geserialiseerde close, tweede-ownerrefusal en
  PID-ownership;
- privacyveilige diagnostics en statische afwezigheid van live wiring.

De bestaande `tests/test_worker_blitzortung_raw_ingress.py` blijft daarnaast
het schema-3 exact-bytecontract en zijn idempotentie/conflicten bewijzen.
