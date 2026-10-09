# NOAA Enterprise Rain Rate native capture

Status: geïsoleerde worker-only kandidaat, lokaal getest, niet aan de service
gekoppeld, niet operationeel toegelaten en niet gedeployed.

## Bron en meteorologische betekenis

De kandidaat gebruikt uitsluitend de publieke NOAA Enterprise Rain Rate
(ERR)-bucket `noaa-enterprise-rainrate-pds` in AWS S3. NOAA/NESDIS STAR
[beschrijft deze bron](https://www.star.nesdis.noaa.gov/smcd/emb/ff/SCaMPR.php)
als een globaal NetCDF-product tussen 60°Z en 70°N, met een raster van circa
2 km, een cadans van tien minuten en uitsluitend **instantane**
neerslagsnelheid. De schatting combineert infraroodinformatie van GOES,
Meteosat en Himawari en wordt tegen microwave-neerslagschattingen
gekalibreerd. Dit blijft satelliet-QPE: het is geen grondradar en geen
grondbevestiging.

De officiële NOAA/NCEI-catalogus vermeldt dat ERR als gridded NetCDF-4 wordt
geleverd en op pixelfniveau instantane rainfall rate bevat:
[NESDIS Level 3 Blended Enterprise Rainfall Rates](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C01710).
NOAA's operationele productmelding legt de productsleutel vast als
`RRQPE-INST-GLB-\d_v1r1_blend_s\d{15}_e\d{15}_c\d{15}.nc`:
[PDA product- en bestandsnamen](https://www.ospo.noaa.gov/data/messages/2025/03/MSG_20250303_2131.html).

## Vast S3-contract

Er worden geen URL, host, bucket, sleutel of regio uit een workerjob
overgenomen. Het netwerkpad is volledig vast:

- origin: `https://noaa-enterprise-rainrate-pds.s3.amazonaws.com`;
- listing: één `ListObjectsV2` voor één UTC-dagprefix, `max-keys=1000` en
  `encoding-type=url`;
- object: precies het door die bewaarde listing geselecteerde pad;
- uitsluitend HTTPS, standaardpoort 443 en exact de vaste hostname;
- geen redirects, systeemproxy, userinfo, fragment, alternatieve host of
  queryparameter;
- identity transport encoding, hard begrensde body, totale deadline en
  exact HTTP 200;
- voor het object zijn `Content-Length`, `ETag` en `Last-Modified` verplicht
  en moeten ze exact overeenstemmen met `Size`, `ETag` en `LastModified` uit
  de listing.

Dit volgt het primaire
[Amazon S3 `ListObjectsV2`-contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html),
dat `Key`, `LastModified`, `ETag`, `Size`, `StorageClass` en optionele
checksummetadata in XML definieert. De vaste virtual-hosted origin volgt het
[S3 virtual-hosted URL-contract](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html).

## Exact-byte volgorde

`storm_tracker_worker/noaa_err_native.py` hanteert deze harde volgorde:

1. lees één begrensde S3-listingresponse;
2. sla de **exacte XML-bytes** op in `RawArtifactStore` schema 3;
3. parse pas daarna de XML;
4. verwerp DTD, entities, verkeerde namespace, onbekende of dubbele velden,
   truncatie, niet-canonieke URL-encoding, onveilige sleutels en incomplete
   partial blends;
5. selecteer de nieuwste volledige `GLB-5`-blend op producttijd;
6. download exact die vaste S3-sleutel en bind key, ETag, LastModified, Size,
   checksumvelden en HTTP-metadata;
7. sla de **exacte NetCDF-4-bytes** op;
8. open pas daarna de HDF5-container voor een metadata-/structuurcontrole;
9. commit één complete capture met required roles `listing_xml` en `netcdf4`.

Een ongeldige XML blijft hoogstens een listingreceipt. Een verkeerde of
beschadigde NetCDF blijft hoogstens twee raw receipts. Geen van beide wordt
een complete capture, en geen fout wordt ooit als `droog` geïnterpreteerd.

De listingreceipt gebruikt de exacte bodyhash als onderdeel van zijn
upstreamidentiteit, omdat een S3-listing een veranderlijk control-plane
document is. Het NetCDF-receipt gebruikt daarentegen de stabiele bucket+key
als upstreamidentiteit: dezelfde sleutel mag later niet stilzwijgend andere
bytes bevatten.

## Waarom uitsluitend `GLB-5`

De publieke daglisting bevat voor eenzelfde tijdslot soms `GLB-2`, `GLB-3`,
`GLB-4` en `GLB-5`. Een selectie op alleen `LastModified` kan daardoor een
later aangeleverde maar incomplete tussenblend kiezen. De kandidaat accepteert
daarom uitsluitend `GLB-5` en bevestigt structureel zowel de vijf
platformnamen als `MonitoringMetaData.Number_of_Input_Files=5`. Als het
volledige product ontbreekt, faalt de capture gesloten; ze valt niet terug op
een gedeeltelijke blend.

## Structurele inspectie zonder pixelread

De inspecteur leest uitsluitend HDF5-objectmetadata en attributen:

- HDF5/NetCDF-4-signatuur, interne hard links en zelf-contained opslag;
- `CF-1.6`, `RRQPE_INST_GLB`, productieomgeving en NOAA Level 3;
- productstart, producteinde en creatietijd tegenover de bestandsnaam;
- geografie `-60..70`, `-180..179,98`, resolutie `0,02°`;
- `Rows=6501`, `Columns=18000`;
- `RRQPE`: signed `int16`, gzip-gechunked, schaal `0,1`, offset `0`,
  `_FillValue=-9990`, eenheid `mm/h`;
- `DQF`: signed `int8`, gzip-gechunked en `_FillValue=-1`;
- de vijf platformen GOES-19, GOES-18, Himawari-9, MSG2 en MSG3.

Geen `dataset[...]`, slice, sample, minimum, maximum of andere pixelwaarde
wordt gelezen. De structurele identiteit bewijst dus alleen dat het verwachte
containerproduct is gevangen. Ze bewijst nog geen geldige kalibratiedecode,
DQF-behandeling, pixelgeometrie, lokale neerslag, cellen, tracking of forecast.

## Echte publieke golden

Op 2 augustus 2026 is het volledige nieuwe pad read-only uitgevoerd tegen de
publieke NOAA/S3-bron, zonder het bestand lokaal als fixture te bewaren. De
daglisting voor `2026/07/18` bevatte 397 objecten en 160.718 exacte XML-bytes.
De kandidaat selecteerde:

`BLEND/RainRate-Blend-INST/2026/07/18/23/RRQPE-INST-GLB-5_v1r1_blend_s202607182350000_e202607182359599_c202607190008336.nc`

Gebonden bewijs:

- listing SHA-256:
  `68bfc38e88aba740d52b2c1cad02415ae2aefe90a27ab475cebce91b2a810e49`;
- ETag: `"6ff682687fc8b2e28749a45efab19099"`;
- LastModified: `2026-07-19T00:09:28.000Z`;
- objectgrootte: 4.965.137 bytes;
- object SHA-256:
  `81f82eabc0d3c4810407d155c54799738cea716732e53054dc525ab90ee28e8f`;
- HTTP Content-Type: `binary/octet-stream`;
- alle hierboven beschreven raster-, dtype-, chunk-, eenheids-, tijd- en
  platformfeiten zijn via metadata bevestigd zonder een pixelread.

## Retry- en foutcontract

Receipt- en capture-ID's worden deterministisch uit het job-ID afgeleid.

- crash na listingopslag: retry leest dezelfde XML uit de raw store en list
  niet opnieuw;
- crash na NetCDF-opslag: retry leest beide payloads uit de raw store en doet
  geen netwerkverzoek;
- complete retry: gebruikt één capturebundle-validatie, leest en SHA-verifieert
  ieder raw lid exact één keer, hergebruikt diezelfde bytes voor XML/HDF5 en
  blijft volledig offline;
- uitsluitend deterministische, leesbare profielafkeur wordt immutable
  gecachet op content-SHA, S3-key, ETag en inspector-v2; corruptie, HDF5-I/O,
  timeout en andere tijdelijke fouten worden nooit gecachet;
- capture zonder listingreceipt of gewijzigde identiteit: harde fout;
- disk-full, corruptie, verkeerde contentlengte, headerdrift, XML-drift of
  HDF5-drift: harde fout, nooit een weerstatus.

Inspector-v2 bindt de zes rastergetallen exact als float32 en vereist daarnaast
`Number_of_Missing_Inputs=0` plus vijf unieke inputbestanden die één-op-één aan
GOES-19, GOES-18, Himawari-9, MSG2 en MSG3 gekoppeld zijn. Leesbare
profielafwijking is typed `NoaaErrNativeUnsupportedError`; onleesbare HDF5 is
typed `NoaaErrNativeCorruptionError`. Alleen het eerste type is deterministisch
en niet-retrybaar. Het corruptietype blijft retrybaar omdat dezelfde publieke
foutklasse ook een tijdelijke HDF5-/opslag-I/O-fout kan vertegenwoordigen.

De jobpayload moet exact `{}` zijn. Hierdoor kunnen ook geen trackergebieden,
alternatieve URL's, tokens of andere netwerkparameters via de queue worden
ingevoerd.

## Uitvoer en gesloten gates

Het resultaat bevat alleen begrensde, payloadvrije bewijsmetadata en:

- `weather_state=unknown`;
- `decode_status=not_decoded`;
- `capture_state=captured_not_decoded`;
- `quantitative_semantics_eligible=false`;
- `operational_admission_eligible=false`.

Er zijn geen areas, observations, cellen, overlays, regen-/droogbesluiten,
grondbevestigingen of forecasts. De bestaande
`enterprise_rainrate_fetch`-servicehandler blijft ongewijzigd; een expliciete
regressietest bewaakt dit. Registrymetadata
vermeldt de module uitsluitend als niet-gewirede kandidaat.

Voor operationele toelating blijven minstens nodig: een productspecifieke
pixeldecoder, correcte DQF- en missing-value-semantiek, grid-/pixelgeometrie,
raw-gebonden decoded evidence, replay en golden truth, producttijd-high-watermark,
worker resource/thermal soak en de snapshot-only Home Assistant-cutover.

De inmiddels read-only geverifieerde DQF-bitsemantiek, exacte live hashes,
noord-naar-zuid rasteroriëntatie en harde eisen voor een begrensde regionale
decoder staan afzonderlijk in
[`NOAA_ERR_QUANTITATIVE_PROFILE.md`](NOAA_ERR_QUANTITATIVE_PROFILE.md). Die
verificatie verandert de capturestatus niet: er is nog geen decoder gewired en
er is geen operationele toelating.
