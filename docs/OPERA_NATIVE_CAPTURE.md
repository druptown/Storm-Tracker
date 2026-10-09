# EUMETNET OPERA CIRRUS exact-byte capture v3

Status: **lokaal, geisoleerd, fail-closed en getest**. De kandidaat is niet als
worker-handler geregistreerd, wordt niet gepland, decodeert geen rasterwaarden
en is niet operationeel toegelaten. Deze wijziging past geen Home Assistant-,
registry-, service-, scheduler-, wiring- of deployconfiguratie aan.

Implementatie:
`storm_tracker_worker.opera_native:capture_opera_dbzh_hdf5`.

## Doel en harde afbakening

De kandidaat legt voor exact een canoniek vijfminutenslot exact een officiele
EUMETNET OPERA CIRRUS DBZH HDF5-revisie als oorspronkelijke bytes vast in de
schema-3 raw store. Daarna certificeert hij alleen de HDF5-containerstructuur en
de upstreammetadata. Hij leest geen DBZH- of QI-pixels en maakt geen uitspraak
over regen, cellen, motion, forecast of ground confirmation.

De bestaande handler `storm_tracker_worker.opera:opera_hdf5_fetch` blijft
ongewijzigd. De v3-kandidaat staat bewust niet in `PRODUCTION_JOB_HANDLERS` en
kan de actieve OPERA-route dus niet stilzwijgend vervangen.

## Officiele bron en productbetekenis

De officiele
[EUMETNET Open Radar Data-documentatie](https://eumetnet.github.io/openradardata-documentation/2-ORD-API-discovering-and-accessing-data/)
publiceert de anonieme CloudFerro-objectroute:

`https://s3.waw3-1.cloudferro.com/openradar-24h/YYYY/MM/DD/OPERA/COMP/OPERA@YYYYMMDDTHHMM@0@DBZH.h5`

Het broncontract bindt host, bucket, platform `0-20010-0-OPERA`, composiet
`COMP`, productcode `DBZH`, ODIM-HDF5 en anonymous/no-sign-toegang. Het
[CIRRUS Maximum Reflectivity Product Sheet](https://www.eumetnet.eu/wp-content/uploads/2024/06/OPERA_Max-Reflectivity_Product-Sheet_Ed-2.0.pdf)
beschrijft een pan-Europees maximum-reflectiviteitscomposiet van 4.400 bij
3.800 pixels op 1 km resolutie, elke vijf minuten, met een kwaliteitsraster.

De nominale composiettijd is niet de uniforme observatietijd van alle
bijdragende radars. Bovendien verklaart de echte upstream `/how/comment` dat
bepaalde DBZH-volumedata door Meteo France met Lukas-Kanade-advection zijn
nabewerkt. Daarom is de vaste tijdstatus:

`nominal_composite_time_input_window_not_uniform_observation_time`

en publiceert v3 altijd:

- `upstream_advected_contributions=true`;
- `motion_truth_eligible=false`.

Deze opname mag dus nooit rechtstreeks als motion-ground-truth dienen.

## Rechtenstatus

De echte container declareert exact:

`https://creativecommons.org/licenses/by/4.0/ (CC BY 4.0)`

De ORD-documentatie noemt eveneens CC BY 4.0. De algemene EUMETNET
OPERA-pagina vermeldt daarnaast een afzonderlijke licentieprocedure voor
gebruik door derde partijen. Tot EUMETNET het concrete operationele gebruik en
de vereiste attributie expliciet bevestigt, blijft de machineleesbare status:

`official_sources_conflict_requires_explicit_clearance`

Publieke technische toegang is geen operationele rechtenbeslissing.

## Canonieke job, revision en contractgebonden IDs

Een job bevat exact een veld:

```json
{"product_timestamp":1785672000}
```

Voor dit voorbeeld is de enige geldige job-ID:

`opera-dbzh-20260802T1200`

Het tijdstip moet een eindig geheel UTC-unix-secondengetal op een exacte
vijfminutengrens zijn. Object-key, job-ID en slot worden uitsluitend intern
afgeleid. Extra velden, een door de job aangeleverde URL, host, bucket, sleutel,
token of bytegrens falen voor de eerste netwerkactie.

V3 gebruikt deze contractversies:

- collector: `worker-opera-cirrus-dbzh-exact-raw-v3`;
- inspector: `opera-cirrus-live-golden-contract-v3`;
- certificatie: `opera-cirrus-certification-v3`.

De revision-token is de canonieke SHA-256 van de exacte ETag, version-id,
Content-Length en Content-Type. De fetch-identiteiten zijn contractgebonden:

- raw: `<job-id>:hdf5:<collector-version>:revision-<revision-token>`;
- capture: `<job-id>:capture:<certification-version>:revision-<revision-token>`;
- upstream-capture-ID bindt object-key, nominale tijd, revision-token,
  content-SHA-256, collector-, inspector- en certificatieversie.

Daardoor kan een v2-receipt/capture naast een v3-recertificatie van dezelfde
upstreamrevision bestaan. Gelijke bytes blijven content-addressed dedupliceren,
maar het oude bewijs wordt niet als nieuw contractbewijs hergebruikt.

## Revision-aware HTTP-transport

De bronrevision wordt voor de body met een strikte `HEAD` vastgesteld:

- alleen HTTP 200 van exact dezelfde vaste HTTPS-URL;
- geen redirects, proxygebruik, userinfo, alternatieve poorten, query of
  fragment;
- `Content-Encoding: identity`;
- alleen HDF5/octet-stream Content-Type;
- verplichte Content-Length van 1 byte tot en met 384 MiB;
- verplichte sterke, begrensde ETag;
- optionele, strikt gevalideerde S3 `x-amz-version-id`.

Een ETag is een opaque, hoofdlettergevoelige HTTP entity-tag. V3 valideert de
vorm maar bewaart de exacte upstreamtekst inclusief hoofdletters. Dezelfde
exacte waarde gaat naar `If-Match`, revision-token, provenance en de
HEAD/GET-gelijkheidscontrole. Er vindt geen lowercasing plaats.

De `GET` bevat `If-Match: <exacte HEAD ETag>`. ETag, version-id, Content-Type en
Content-Length moeten exact gelijk blijven. De body wordt met stukken van
maximaal 1 MiB naar een uniek tijdelijk bestand geschreven en tegelijk gehasht.
De reader vraagt maximaal `Content-Length + 1` bytes op. Een korte body, extra
byte, headerdrift, revisionrace of verlopen deadline verwijdert het tijdelijke
bestand en produceert geen receipt. Na succes worden filebuffer en filesystem
gesynchroniseerd.

## Deadline en concurrency

De hele job deelt een monotone deadline van 300 seconden. Wachten op locks,
store-initialisatie, HEAD, GET, hashing, HDF5-inspectie en commits telt mee.
Iedere HTTP-call gebruikt maximaal de resterende tijd met een bovengrens van
30 seconden.

Er zijn drie lagen:

1. de bestaande in-process capturelock;
2. een deadlinegebonden OS-filelock per canoniek jobslot, zodat twee processen
   dezelfde slotrevision niet gelijktijdig behandelen;
3. een globale deadlinegebonden OS-filelock rond de enige volledige
   schema-3-buffer plus `store()`, zodat ook verschillende processen nooit
   gelijktijdig meerdere grote OPERA-buffers aanmaken.

De lockbestanden staan in de afgeschermde raw-root onder `opera_locks`. De
acquisitie gebruikt niet-blokkerende OS-locking met korte polls en controleert
bij elke poll opnieuw dezelfde jobdeadline. Het slotlock omvat HEAD, resume,
download, inspectie en commit. De globale bufferlock is bewust smaller en omvat
alleen het grote-buffer/store-gedeelte.

## Exact-byte store en geheugenblokkade

Transport en HDF5-inspectie zijn file-backed. De huidige
`RawArtifactStore.store()` accepteert echter alleen `payload: bytes`. Vlak voor
`store()` wordt daarom nog exact een volledige buffer gelezen, op lengte en
SHA-256 gecontroleerd en direct na de store vrijgegeven.

Dit blijft een harde activatieblokkade:

`schema3_store_requires_one_complete_bytes_object`

De uitvoer vermeldt daarom steeds:

- `transport_streaming_to_file=true`;
- `hdf5_inspection_file_backed=true`;
- `schema3_full_object_buffer_required=true`;
- `operational_memory_eligible=false`;
- `operational_memory_certified=false`.

Voor productie is eerst een atomische file-/streaming-ingang voor schema 3 of
een afzonderlijk geslaagde NUC memory/thermal soak nodig.

## Durable validatieverdictcache en retries

Na de schema-3 raw commit valideert de inspector uitsluitend het duurzame
content-addressed bestand. Het resultaat wordt als kleine canonieke JSON onder
`opera_validation_verdicts` opgeslagen. De verdict-ID bindt:

- revision-token;
- raw content-SHA-256;
- inspectorversie;
- certificatiecontract.

Een verdict is maximaal 16 KiB, wordt via tempbestand plus file-fsync,
hardlink-create-if-absent en directory-fsync gepubliceerd en wordt nooit
overschreven. Een bestaande ID met andere canonieke inhoud is een conflict en
faalt gesloten. Bij het lezen worden bestandstype, bytegrens, UTF-8, exact
schema, alle contractvelden en canonieke JSON opnieuw gevalideerd.

Er zijn exact twee duurzame statussen:

- `validated_complete`, met de volledige gecertificeerde containeridentiteit;
- `deterministic_rejection`, met een begrensde foutcode en fouttekst.

Een deterministische HDF5-contractafkeur bewaart het raw object en receipt,
maakt geen capture en keert terug als een afgeronde, niet-herhaalbare uitkomst:

- `capture_state=validation_rejected_non_retryable`;
- `validation_status=deterministic_rejection`;
- `retryable=false`;
- `weather_state=unknown`.

Dezelfde revision en content worden bij een retry niet opnieuw door HDF5
geinspecteerd. Een succesvol verdict wordt voor de capturecommit gepubliceerd;
na een crash of commitfout kan de retry dus de capture afwerken zonder tweede
inspectie.

`TimeoutError` en echte filesystem/device-`OSError` zijn mogelijk transient en
worden nooit als deterministische verdict gecachet. Een retry inspecteert dan
opnieuw.

Een volledig gecertificeerde capture doet bij retry nog wel HEAD om de huidige
upstreamrevision te bewijzen. Daarna laadt `load_capture_bundle()` capture,
receipt en exact een geverifieerd raw object in een gecombineerde operatie.
Deze complete retry doet maximaal een raw-objectverificatie, geen extra
losstaande SHA-pass, geen GET en geen HDF5-inspectie.

## Exacte live-golden HDF5-boom

V3 accepteert exact deze interne hardlinkboom en geen extra objecten:

```text
/
|- what
|- where
|- how
`- dataset1
   |- what
   `- data1
      |- what
      |- data
      `- quality1
         |- what
         |- how
         `- data
```

Root heeft uitsluitend `Conventions=ODIM_H5/V2_4`. De groepen `dataset1`,
`data1` en `quality1` hebben geen attributen. Beide rasterdatasets hebben exact
de attributen `CLASS=IMAGE` en `IMAGE_VERSION=1.2`.

Top-level `/what` heeft exact:

- `object=COMP`;
- `version=H5rad 2.4`;
- `source=ORG:247`;
- strikte ASCII `date` en `time` voor de gevraagde nominale UTC-tijd.

Root `/how` heeft exact de volgende vaste velden en geen andere attributen:

- `comment`: de exacte Meteo-France/Lukas-Kanade-advectiontekst;
- `creator_email=support.opera@eumetnet.eu`;
- `creator_name=Meteo France`;
- `creator_type=institution`;
- `creator_url=https://meteofrance.fr`;
- `institution=EUMETNET`;
- `license=https://creativecommons.org/licenses/by/4.0/ (CC BY 4.0)`;
- `naming_authority=eu.eumetnet`;
- `publisher_email=support.opera@eumetnet.eu`;
- `publisher_name=EUMETNET`;
- `publisher_type=institution`;
- `publisher_url=https://eumetnet.eu`;
- `nodes`: een niet-lege, unieke, alfabetisch gesorteerde canonieke lijst van
  begrensde bronnode-IDs.

Omdat deelnemende radars per slot kunnen veranderen, wordt `nodes` niet aan een
vast aantal gebonden. V3 publiceert wel `node_count` en de SHA-256 van de
canonieke JSON-array als `nodes_sha256`.

Raster/projectie:

- `/where/xsize=3800`, `/where/ysize=4400`;
- `xscale=yscale=1000.0` meter;
- exacte LAEA-projectie en alle acht waargenomen hoekattributen;
- `/dataset1/what/product=MAX`;
- `prodname=OPERA CIRRUS maximum reflectivity composite`;
- eindtijd exact gelijk aan de nominale tijd;
- starttijd exact 599 seconden voor de eindtijd.

DBZH-metadata onder `/dataset1/data1/what`:

- `quantity=DBZH`;
- `gain=1.0`, `offset=0.0`;
- `nodata=-9999000.0`, `undetect=-8888000.0`.

QI-metadata:

- `/dataset1/data1/quality1/what` bevat exact `gain`, `offset`, `nodata`;
- waarden `1.0`, `0.0`, `-9999000.0`;
- geen verzonnen `quantity`-attribuut;
- `/dataset1/data1/quality1/how/task=pl.imgw.quality.qi_total`.

Beide rasters hebben exact shape `4400 x 3800`, dtype `<f8`, chunkshape
`760 x 880`, layout `CHUNKED` en een enkele DEFLATE/gzip-filter op niveau 9
met flags `1`. Shuffle, fletcher32 en scaleoffset zijn uitgeschakeld. Alle 30
verwachte chunks per raster moeten fysiek aanwezig zijn met `filter_mask=0`.

De DBZH- en QI-datasets moeten verschillende HDF5-objecttokens hebben. De
generieke boominspectie verbiedt elke hardlink-alias. Daarnaast worden alle
fysieke chunkbyte-intervallen van beide rasters samen gesorteerd en globaal op
overlap gecontroleerd. Geen DBZH-chunk mag dus opslag met een andere DBZH- of
QI-chunk delen.

De inspector leest geen meteorologische rasterwaarden.

## Algemene HDF5-veiligheidsgrenzen

Naast het exacte golden-profiel geldt fail-closed:

- uitsluitend interne hard links;
- geen soft/external links, virtual datasets of externe datasetopslag;
- maximaal 4.096 objecten, 1.024 datasets en 1.024 leden per groep;
- maximale groepsdiepte 16;
- maximaal 2 GiB logische data per dataset en totaal;
- maximaal 16 MiB per logische chunk;
- geen objectdtype of onverwachte DCPL-transformatie/filter;
- geen ontbrekende, overlappende, buitengrens- of filter-skipped chunks;
- elk groep- en datasetobject heeft een unieke objecttoken.

## Bewust ontbrekende meteorologische uitvoer

V3 levert geen pixelwaarden, regenintensiteit, accumulatie, dry-state, cellen,
polygonen, systeemgroepering, motion, forecast, ground confirmation,
observation count of operationele admission. Een geslaagde opname publiceert:

- `quantitative_semantics_eligible=false`;
- `operational_admission_eligible=false`;
- `operational_memory_eligible=false`;
- `decode_status=not_decoded`;
- `capture_state=captured_not_decoded`;
- `weather_state=unknown`.

## Verificatie op 2 augustus 2026

Geautomatiseerd:

- gerichte v3-suite: `107 passed`;
- gecombineerd met worker raw-artifacttests: `134 passed, 1 skipped`;
- `py_compile`: geslaagd;
- `git diff --check`: geslaagd (alleen reeds bestaande line-endingwaarschuwingen).

De tests dekken onder meer exacte job/key-afleiding, HEAD/GET-revisionbinding,
case-preserving ETags, bodygrenzen, deadlinepropagatie, OS-lockcontention,
globale bufferlocking, v2/v3-coexistentie, exact een raw-verificatie op complete
retry, durable success- en rejectionverdicts, geen caching van timeout/I/O,
exacte HDF5-boom, `/how`, datasetidentiteit, objecttoken-uniciteit en globale
DBZH/QI-chunkdisjunctie.

De bestaande read-only live golden is rechtstreeks vanuit het duurzame
content-addressed object met de v3-inspector herkeurd:

- object-key:
  `2026/08/02/OPERA/COMP/OPERA@20260802T0045@0@DBZH.h5`;
- grootte: `1.778.794` bytes;
- content-SHA-256:
  `80cb81b9c953a8fac9bcb8127a342aed0f93d0f203764caf8866fd0d244b471a`;
- nominale tijd: `2026-08-02 00:45:00 UTC`;
- inputvenster: `00:35:01-00:45:00 UTC`;
- 4.400 bij 3.800, `<f8`, chunks 760 bij 880, gzip-9;
- 30 fysiek aanwezige, globaal disjuncte chunks per raster;
- 162 canonieke bronnodes;
- `nodes_sha256=7d3c7ded0ea041952fbeefd111361d6f9e5d52c387480b1f0bb4d8721b196f26`;
- exacte creator-, publisher-, rights- en advectionmetadata;
- `upstream_advected_contributions=true`;
- `motion_truth_eligible=false`;
- v3 exact-contractinspectie: geslaagd.

Deze golden bewijst transport- en containeridentiteit. Hij bewijst niet de
juistheid van pixels, de kwantitatieve betekenis van QI, gebruiksrechten of
operationele geschiktheid.

## Blokkades voor wiring

Minimaal vereist blijven:

1. expliciete rechten- en attributiebevestiging;
2. een distributiegeschikte officiele real-world fixture met vaste hash;
3. onafhankelijke redteamreview van v3;
4. onafhankelijke redteam- en admissionreview van de inmiddels lokaal geteste,
   strikt ongewirede regionale DBZH/QI-decoderkandidaat;
5. event-held-out replay tegen nationale radar en grondmetingen;
6. schema-3 file-/streaming-opslag of geslaagde memory/thermal soak;
7. pas daarna afzonderlijke expliciete registry-, scheduler- en
   admissionwijzigingen.

Het afzonderlijke kwantitatieve decoderprofiel staat in
[`OPERA_QUANTITATIVE_DECODER_PROFILE.md`](OPERA_QUANTITATIVE_DECODER_PROFILE.md).
Dat profiel bewaart DBZH en QI zonder een ongecertificeerde rain-rateconversie
of operationele threshold te verzinnen.
