# METAR canonical-grounddecoder

Status: shadow-only kandidaat, niet gewired, niet operationeel toegelaten en
niet gedeployed.

## Grens en autoriteit

`storm_tracker_worker/metar_ground_evidence.py` accepteert uitsluitend de
identiteit van een complete `RawArtifactStore` schema-3-capture van
`aviationweather_current_metars_capture`. De decoder haalt capture, receipt en
exacte `metars.cache.csv.gz`-bytes uitsluitend via
`load_capture_bundle_bytes()` op. Provider, product, fetch/upstream-identiteit,
memberrol `csv_gzip`, hashes, ontvangsttijd, content-type en de volledige
secretvrije native provenance moeten exact overeenkomen. Een ontbrekende of
gemanipuleerde authority faalt vóór er een grondfact wordt geschreven.

De gzip wordt incrementeel en strict UTF-8 uitgepakt. Compressed bytes,
uncompressed bytes, expansieratio, fysieke/logische recordlengte, veldlengte,
rijen, stations, facts, totale canonieke factbytes, weer-/remarktokens en één
absolute monotone deadline hebben vaste bovengrenzen. Tweede gzipleden,
trailing data, ongesloten quotes, schema-afwijking en iedere verschoven rij
falen gesloten. Het positionele schema moet exact 44 kolommen hebben, inclusief
de vier herhaalde `sky_cover`/`cloud_base_ft_agl`-paren.

## Facts, geen grondbeslissingen

De output gaat rechtstreeks naar `CanonicalGroundEvidenceStore` en blijft
`candidate_only_unadmitted`. Er worden geen nat-, droog-, regen-, dekking-,
confidence- of grondbevestigingsclaims gemaakt.

- Ieder exact AWC-`wx_string`-token wordt een afzonderlijk categorisch
  `present_weather`-fact. `-RA`, `SN`, `BR` en `VCSH` blijven dus vier
  verschillende tokens.
- `VC` krijgt expliciet `station_relation=vicinity_not_at_station`; het is
  nooit neerslag op het station.
- Een leeg `wx_string` is `missing`, niet nul en niet droog. Met `PWINO`, de
  AWC-sensor-offkolom, `no_signal`, een onherkende token, stale of future tijd
  wordt de waarde expliciet `unknown`.
- ICAO, exacte positie-/hoogtelexemen en known/unknown-toestand blijven
  bewaard. Een half of ongeldig coördinatenpaar wordt niet aangevuld maar
  `unknown`.
- `AUTO`, `COR`, de acht AWC-statuskolommen, `AO1/AO2/AO2A`, `PWINO`, `PNO`,
  `FZRANO`, `TSNO`, `RVRNO`, `VISNO_*`, `CHINO_*` en `$` blijven als
  `provider_qc` staan. Ze worden niet omgezet in een oordeel over bruikbaarheid.
- De bronlocator bevat de data-rij, het geselecteerde veld/token en SHA-256 van
  het exacte UTF-8 `raw_text`. De volledige gzip-SHA en receipt-SHA blijven via
  de schema-3 memberbinding onafhankelijk verifieerbaar.
- `observed_at`, member-`received_at`, deterministic capture-`ingested_at`,
  representatief interval en store-`committed_at` blijven afzonderlijke assen.

## Neerslagremarks

De officiële AWC-productspecificatie zegt dat `Prrrr` water equivalent sinds
de vorige METAR betekent. De huidige cache bevat die vorige rapporttijd niet.
De decoder bewaart daarom `P0000`, `P0001`, `P////` en hun exacte AWC
`precip_in`-lexeem als `metar_hourly_precipitation_remark`, met onder andere
expliciete `trace` voor `P0000`, maar publiceert **geen** kwantitatief interval
en trekt nooit blind 3600 seconden af. Omissie krijgt expliciet
`not_zero_not_dry`.

`6RRRR` kan 3 of 6 uur betekenen. Een kwantitatief interval ontstaat alleen
voor één routine-`METAR`, wanneer het exacte UTC-observatie-uur 03/09/15/21
(3 uur) of 00/06/12/18 (6 uur) aanwijst, precies één passende AWC-decimaalkolom
gevuld is en raw digits en exact decimal lexeme numeriek overeenkomen.
`6////` houdt een bekend interval met onbekend bedrag. `60000` blijft unknown,
omdat zero/trace-semantiek niet voor iedere wereldwijde bron gecertificeerd is.
SPECI, verkeerd uur, dubbele groepen of kolomconflict worden alleen als raw
remark bewaard; er wordt niet geraden.

Een enkele geldige `7RRRR` met overeenkomend `pcp24hr_in` krijgt exact het
voorgaande 24-uursinterval. `7////` is unknown. `70000`, dubbele of conflicterende
groepen blijven raw remarks.

Bronnen voor deze semantiek:

- [AWC product information](https://aviationweather.gov/help/data/), secties
  Weather, Precipitation en Sensor Status;
- [AWC Data API](https://aviationweather.gov/data/api/), voor de wereldwijde
  current-cache en updatecadans;
- [NWS ASOS User's Guide](https://www.weather.gov/media/asos/aum-toc.pdf), voor
  `P0000` als trace en de 3-/6-uurrapporturen.

Een cache-update per minuut is alleen fetchcadans, nooit een meetinterval.

## Revisies en retry

Alleen volledig identieke 44-veldrecords met dezelfde ICAO/tijd worden
samengevouwen. Verschillende revisies op dezelfde ICAO/tijd blijven allemaal
bestaan, krijgen revision group/rank-QC en worden niet via last-wins vervangen.
De raw-text-SHA en tokenlocator maken hun factidentiteit verschillend.

Alle structuur-, dedup-, tijd- en capaciteitscontroles eindigen vóór de eerste
append. De concrete bounded factsequence gaat daarna in één
`commit_batch()`-aanroep naar de store. Die reverifieert de volledige schema-3
capture exact eenmaal en schrijft alle nieuwe facts, memberlinks en QC-rijen in
één `BEGIN IMMEDIATE`-transactie. Een conflict, limiet, deadline, fout of crash
vóór commit laat geen duurzame prefix achter. Exact bestaande en gemengd
bestaande/nieuwe retries behouden de oorspronkelijke receiptvolgorde en voeren
één durability barrier uit.

## Testbewijs en resterende gates

`tests/test_worker_metar_ground_evidence.py` dekt categorische groepen,
vicinity, leeg versus sensor-unavailable, `P0000/P0001`, 3/6/24 uur,
SPECI/ambiguïteit, stale/future, exacte lexemen, positie/elevatie, revisies,
exacte retry, raw tamper, deadline, malformed CSV en resourcebommen. De lokale
echte `metars_current.csv.gz` wordt volledig als schema-3 authority geladen en
bouwt alle facts onder dezelfde bounds.

Open vóór operationele toelating:

1. provider-/landprofielen voor wereldwijd verschillende METAR-additives;
2. een historisch vorige-rapportbewijs voordat `Prrrr` kwantitatief kan worden;
3. hard child CPU/RSS/wall-isolatie, NUC-soak en snapshot-only HA-projectie;
4. held-out ground events en expliciete stationrepresentativiteit/coverage;
5. onafhankelijke toelating van een latere fusionlaag. Deze decoder zelf mag
   nooit een grondbevestiging afgeven.
