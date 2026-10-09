# Netatmo canonical ground decoder — shadowkandidaat v1

## Afbakening

`storm_tracker_worker/netatmo_ground_decoder.py` decodeert uitsluitend een
reeds complete en door `RawArtifactStore.load_capture_bundle_bytes` opnieuw
byte-exact geverifieerde schema-3-capture van `getpublicdata`. De module doet
geen netwerkcall en is niet gekoppeld aan service, registry, Home Assistant,
MQTT, dashboards of deploy. Operationele admission en ground-confirmation zijn
hard `false`.

## Bewuste meteorologische grens

De bereikbare officiële Netatmo Weather API-pagina publiceert momenteel geen
machine-verifieerbaar veldniveaucontract voor de legacyvelden `rain`,
`rain_live`, `rain_60min`, `rain_24h` en `type:["rain"]`. De veldnamen en
communityvoorbeelden zijn geen voldoende productspecificatie. Daarom doet deze
decoder nadrukkelijk **niet** alsof:

- een waarde `mm` of `mm/h` is;
- `rain` of `rain_live` bewezen instantane intensiteit is;
- `rain_60min` of `rain_24h` een bewezen representatief uur-/daginterval is;
- nul droog betekent, een positieve waarde nat betekent of een station radar
  bevestigt;
- stationenaantal dekking, betrouwbaarheid of confidence bewijst.

Elke expliciete bronvariabele blijft afzonderlijk herkenbaar als bijvoorbeeld
`netatmo_native.rain_live` of `netatmo_native.rain_60min`, maar de unit is exact
`netatmo_native_unit_unspecified`. De numerieke JSON-lexeme blijft ongewijzigd:
`0`, `0.10` en `1e-1` blijven drie exacte representaties. Expliciete JSON
`null` wordt `missing`; afwezigheid produceert geen gefabriceerd missing-fact.
Strings, negatieve neerslagwaarden, trace-/censorvormen en onbekende
`rain*`-velden falen gesloten zolang een officiële contractbinding ontbreekt.

Het providerrecordtijdstip wordt als alle drie de ground-tijdvelden
`observed_at == representative_interval_start == representative_interval_end`
gebonden. Dit is alleen een **record-time anchor**, geen claim van instantane
temporale support. Elk fact draagt daarom zichtbaar de flags:

- `netatmo.decoder.temporal_support_uncertified`;
- `netatmo.decoder.unit_semantics_uncertified`.

Een latere decoder-versie mag pas fysieke units en intervallen uitgeven nadat
een exact gepinde officiële productspecificatie die semantiek bewijst.

## Exacte bronbinding

De decoder accepteert alleen provider `netatmo`, het exacte native capture- en
responseproduct, precies één `response_json`-lid, `product_timestamp=null`, en
de native collector-, querycontext- en contenthashbinding. Daarna bewaart hij
per station/module/timestamp/upstream-regenvariabele één
`GroundObservationFact` met:

- exact Netatmo station-ID;
- module-ID in een RFC 6901-escaped bronlocator;
- exacte bronrecordbytes gebonden door `source_record_sha256`;
- alle schema-3 member-, receipt-, manifest- en capturehashes;
- exact numeriek coördinaat- en hoogtelekseem, of expliciet `unknown` hoogte;
- exacte provider-QC-scalars (`mark`, `quality`, `status`, `data_quality`);
- afzonderlijke received-, ingest-, record- en commit-tijdassen.

Voor directe velden is het gehashte record de exacte JSON-member van key tot
waarde. Voor `res` is het de exacte timestampmember met zijn volledige array;
de locator bindt aanvullend de exacte array-index. De raw bytes blijven de
enige bronautoriteit.

`capture.created_at` (nooit de huidige replaytijd) vormt de stabiele ingest-as.
Daardoor geeft een exacte retry dezelfde canonical factidentiteit. Iedere fact
wordt vooraf volledig gepland en gevalideerd. Daarna reverifieert
`CanonicalGroundEvidenceStore.commit_batch()` de schema-3 authority exact
eenmaal en commit alle facts in één `BEGIN IMMEDIATE`-transactie. Een fout,
deadline, conflict, capaciteitsfout of crash vóór de ene commit laat geen
duurzame prefix achter. Een crash erna wordt met dezelfde ordered receipts
idempotent hersteld.

## Grenzen en fail-closed gedrag

De eigen spanbewuste JSON-parser bewaart exacte byteposities en weigert onder
andere dubbele keys, BOM, NaN/Infinity, ongeldige UTF-8/escapes, trailing bytes,
onbekende authvelden, ambigue timestamps en mismatchende `type`/`res`-arrays.
Harde grenzen gelden voor 8 MiB response, nodes, diepte, objecten, arrays,
strings, 4096 stations, 64 modules per station, 2048 resulttijden per module,
16.384 facts en 512 KiB per gekozen bronrecord. Eén absolute monotone deadline
wordt voor/na raw-load, tijdens parsing/planning, tussen batchfacts en rond de
ene batchcommit bewaakt.

De raw-reader heeft zelf geen onderbreekbare deadline-API. Een defecte syscall
of vastgelopen reader vereist daarom nog steeds de afzonderlijke harde
child-process CPU/RSS/wall-isolatielaag vóór enige operationele admission.

## Testbewijs

`tests/test_worker_netatmo_ground_decoder.py` gebruikt de gesaneerde,
contractvormige golden
`tests/fixtures/netatmo_getpublicdata_ground_golden.json` en bewijst:

- meerdere stations, modules, tijden en directe plus `type/res`-records;
- zachte waarde, exacte nul en expliciet missing zonder wet/droogclaim;
- behoud van decimalen, exponentlexemen, coördinaten, hoogte, IDs, QC en
  record-SHA;
- altijd opaque unit en expliciet ongecertificeerde temporal support;
- stale tijden zonder filter of relabeling;
- duplicate-key, NaN, onbekende rainvorm, tijdambiguïteit en shape-afkeur vóór
  de eerste fact;
- raw-tamperdetectie, deadline- en feitengrens;
- één raw-reverificatie en één atomaire batchtransactie per decode;
- idempotente ordered retry en herstel na een postcommit-onderbreking;
- een onafhankelijke procesreplay en repositorybrede unwired status.

Dit is alleen lokaal/shadow bewijs. Er is niets geactiveerd, gedeployed of
herstart.
