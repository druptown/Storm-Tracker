# Pure HA desired-state builder — worker-only cutoverkandidaat

## Doel en afbakening

`custom_components/storm_tracker_v3/desired_state_builder.py` is een zuivere,
ongewirede contractbuilder. Hij zet een volledige Home Assistant-configuratie
om in precies de canonieke UTF-8 JSON-bytes die
`storm_tracker_worker.desired_state.parse_desired_state` accepteert.

De module importeert geen Home Assistant-, worker-, provider-, opslag-, MQTT-
of netwerkcode. Hij doet geen I/O, registreert geen entity of event, publiceert
niets en wijzigt geen runtime. De kandidaat is dus nog geen cutover.

## Contract

`build_desired_state` accepteert uitsluitend expliciet getypeerde velden voor:

- `producer_id`, positieve `authority_epoch`, niet-negatieve `generation` en
  `issued_at`;
- de CAS-koppeling `previous_state_sha256` (`null` of lowercase SHA-256);
- maximaal 64 targets;
- target-ID, NFC-displaynaam, soort, availability, latitude, longitude,
  positietijd, accuracy, observatieradius en optionele ISO alpha-2-landcode;
- ground-radius, lightning-bronmodus, model-guidance en afstand voor gedeelde
  regio's.

`build_desired_state_document` biedt dezelfde builder achter een volledig
gesloten rootobject, zodat ook extra rootvelden getypeerd falen. Target- en
settingsobjecten hebben eveneens exact gesloten sleutels.

Targets worden uniek op `target_id` gecontroleerd en daarna lexicografisch
gesorteerd. Getallen worden net als in de workerparser als eindige floats
gecanonicaliseerd. De encoder gebruikt exact `sort_keys=True`,
`ensure_ascii=False`, `allow_nan=False` en compacte separators. De onbewerkte,
ongecomprimeerde uitvoer mag maximaal 64 KiB zijn.

`DesiredStateEnvelope.envelope_sha256` is SHA-256 over exact die canonieke
bytes. `state_sha256` is een alias met de workerterminologie. Er bestaat geen
extra buitenste transportwrapper: de digest is de CAS-identiteit van het
desired-state-document zelf.

## Positietoestanden

Een beschikbaar target vereist `position_observed_at`. Een onbeschikbaar
target mag die tijd op `null` zetten. Workercontract v1 vereist in beide
gevallen begrensde latitude en longitude; bij unavailable zijn dat dus alleen
de laatste bekende configuratiecoördinaten, geen claim dat de positie current
is. `location_accuracy_m` blijft onafhankelijk optioneel.

De builder controleert syntaxis en canonicalisatie, niet de runtimeleeftijd van
een dynamische positie. Future-skew, staleness, authority fencing, generation-
progressie en de werkelijke CAS-head blijven terecht transactieregels van de
workerstore op ontvangsttijd.

## Fail-closed grenzen

Afkeur volgt vóór er transportbytes beschikbaar zijn voor:

- extra of ontbrekende velden, niet-tekstuele keys en dubbele target-ID's;
- 65 of meer targets;
- booleans op integer-/getalvelden, NaN, Infinity en waarden buiten de exacte
  workergrenzen;
- invalid latitude/longitude, timestamp, accuracy, radius, landcode, targetkind
  of runtimesetting;
- niet-NFC tekst, randspaties en controltekens;
- secret-vormige veldnamen, Bearer-/private-key-/JWT-/credentialassignmenttekst
  en secretsegmenten in IDs;
- een ongeldige previous SHA of canonical output boven 64 KiB.

De secretscan gebruikt bewust geen entropiegok: willekeurige gebruikersnamen
worden niet als credential bestempeld. De gesloten schema's maken normale
credentialvelden onmogelijk; herkenbare credentialvormen in toegestane tekst
worden aanvullend geweigerd.

## Testbewijs

`tests/test_desired_state_builder.py` bewijst onder meer:

- byte- en SHA-pariteit met de actuele workerparser;
- stabiele bytes bij omgekeerde targetvolgorde;
- current en unavailable/last-known posities;
- 64 targets geaccepteerd en de 65e vóór encoding geweigerd;
- unieke targets, alle numerieke grenzen, landcode en settings;
- extra velden, NFC-fouten en credentialoppervlakken;
- detached resultaatbytes en exacte CAS-metadata;
- de 64-KiB-grens op ongecomprimeerde canonieke bytes;
- AST-isolatie van HA, worker, provider, netwerk, MQTT, filesystem en SQLite;
- afwezigheid van publicatie-, registratie- of I/O-API's.

Operationele wiring, MQTT-publicatie, eventregistratie en cutover blijven
expliciet buiten deze kandidaat en vereisen afzonderlijke review en tests.

