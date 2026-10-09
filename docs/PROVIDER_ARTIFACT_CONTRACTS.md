# Provider-artifactcontracten — shadowaudit v1

Status: **shadow-only, niet live gekoppeld**  
Schema: `provider-artifact-contract-v1`  
Modules:

- `custom_components/storm_tracker_v3/engine/provider_artifact_contracts.py`
- `storm_tracker_worker/tracking_core/engine/provider_artifact_contracts.py`

De twee modules zijn byte-identiek. Ze doen geen netwerk-I/O, schrijven niets
naar de evidence store en worden nergens door de runtime geïmporteerd. Ze
beschrijven uitsluitend welke upstream bytes later vóór decode moeten worden
vastgelegd en welke context nodig is voor wetenschappelijk reproduceerbare
replay.

## 1. Harde betekenis van de contractvelden

Elk contract bevat expliciet:

- provider, werkelijk product en meteorologische quantity;
- de gezaghebbende upstream artifactset;
- de capture boundary: altijd de exacte bytes vóór decompressie, JSON-, image-,
  HDF5-, NetCDF- of CSV-decode;
- de record-ID-strategie en alle vereiste, niet-geheime componenten;
- toegestane media types;
- bron van de nominale tijd;
- native CRS/projectie, unit, gain/offset, nodata, undetect en QC;
- native resolutie en dimensies;
- verantwoordelijke parser en parserschemaversie;
- licentie, attribution en lokale retention class;
- of exacte raw replay volledig of alleen onder voorwaarden mogelijk is;
- wat het huidige transport al beschikbaar heeft en wat nog ontbreekt.

`UNKNOWN` betekent letterlijk dat het feit niet is bewezen. Het krijgt geen
waarde. `ARTIFACT_METADATA` betekent dat de waarde bij ingest uit het exacte
artifact moet worden gelezen en bewaard. Een dynamisch feit wordt dus nooit
ingevuld met een gok, parserdefault of ontvangsttijd.

De klokregel is voor alle bronnen gelijk: source time en receipt time blijven
afzonderlijk bewaard. Ontbrekende, onparseerbare of toekomstige source time
wordt kwaliteits-/quarantaine-informatie. Ze wordt nooit stil vervangen door
de klok van Home Assistant of de worker.

## 2. Volledigheidsaudit

De contractregistry is exact gelijk aan:

`IMPLEMENTED_RADAR_PROVIDERS ∪ IMPLEMENTED_GROUND_PROVIDERS`

Dat zijn 12 entries uit de radarregistry en 3 grondbronnen. De test faalt zowel
bij een ontbrekende als bij een extra provider.

Belangrijke classificatiebevindingen:

1. De audit vond dat `meteolux` ten onrechte in
   `IMPLEMENTED_RADAR_PROVIDERS` stond. De implementatie heeft uitsluitend
   `Capability.NOWCAST` en retourneert geen neerslagraster. De registry is
   gecorrigeerd: MeteoLux blijft uitsluitend `model_guidance` voor Luxemburg
   en zit niet in de raw radar-/grondcontractset. Een regressietest bewaakt de
   scheiding tussen model guidance, radar en grondvalidatie.
2. `noaa_enterprise_rainrate`, `noaa_goes_rrqpe` en `hsaf_h40b` staan technisch
   in de radarregistry, maar zijn satelliet-afgeleide neerslagschattingen, geen
   weerradar. Ook dat staat expliciet in hun rol.
3. Geen enkele provider bewaart vandaag al de volledige upstream artifactset
   duurzaam. Een gzip-JSON workerresultaat is afgeleide data en telt niet als
   raw artifact.

## 3. Huidige raw-beschikbaarheid per provider

Betekenis van de statussen:

- `EXACT_BYTES_TRANSIENT`: de exacte decode-input bestaat tijdelijk als `bytes`
  in het huidige proces, maar wordt niet duurzaam blootgesteld.
- `MULTI_ARTIFACT_TRANSIENT`: meerdere gekoppelde artifacts bestaan tijdelijk;
  de volledige set en ontbrekende leden worden niet atomair bewaard.
- `DECODED_BODY_ONLY`: het transport laat JSON rechtstreeks decoderen; de
  exacte body is niet beschikbaar voor de evidence-laag.
- `WORKER_DERIVED_ARTIFACT_ONLY`: de worker downloadt de raw bytes, maar geeft
  alleen afgeleide gzip-JSON aan Home Assistant terug.

| Provider | Gezaghebbend raw artifact | Nu beschikbaar | Raw replay na correcte capture | Resterende transportactie |
|---|---|---|---|---|
| `kmi` | forecastcatalogus + geselecteerde radarimage | `EXACT_BYTES_TRANSIENT` voor image; catalogus al gedecodeerd | voorwaardelijk | catalogus en image linken; URL-query en appcredential verwijderen |
| `knmi` | canonieke WMS GetMap PNG + niet-geheime requestcontext | `EXACT_BYTES_TRANSIENT` | voorwaardelijk | PNG, responseheaders en canonieke WMS-velden vastleggen |
| `dwd_radolan` | volledige RV-tar + gekozen `_000-hd5`-member | `WORKER_DERIVED_ARTIFACT_ONLY` | volledig | tar vóór extractie uit worker blootstellen |
| `meteofrance_radar` | DPRadar ODIM-HDF5 body | `EXACT_BYTES_TRANSIENT` | volledig | body vóór ODIM-parser vastleggen; OAuth nooit opnemen |
| `dpc_radar` | catalogus-JSON + gekoppelde SRI GeoTIFF | `MULTI_ARTIFACT_TRANSIENT` | voorwaardelijk | catalogus en TIFF atomair linken |
| `aemet_radar` | officiële OpenData-envelope + exacte `datos`- en `metadatos`-responses | `EXACT_BYTES_TRANSIENT` voor de ongedocumenteerde legacybundel | voorwaardelijk | drie officiële responses atomair bewaren; tijd/grid/`ESCALA`/NoData/QC certificeren |
| `met_office_radar` | gekozen S3 ODIM-HDF5 object | `WORKER_DERIVED_ARTIFACT_ONLY` | volledig | exact object en bucket-relative key uit worker blootstellen |
| `opera` | gekozen OPERA DBZH ODIM-HDF5 object | `WORKER_DERIVED_ARTIFACT_ONLY` | volledig | object vóór crop/decode blootstellen |
| `rainviewer` | manifest + volledige geordende tileset | `MULTI_ARTIFACT_TRANSIENT` | voorwaardelijk | manifest, alle tiles en missing-tile receipts als één set bewaren |
| `noaa_enterprise_rainrate` | gekozen globale NetCDF-4 object | `WORKER_DERIVED_ARTIFACT_ONLY` | volledig | exact NetCDF/key vóór regionaal venster blootstellen |
| `noaa_goes_rrqpe` | gekozen GOES NetCDF-4 object per satelliet | `EXACT_BYTES_TRANSIENT` | volledig | elk object/key en mislukte satellietreceipts bewaren |
| `hsaf_h40b` | gekozen `.nc.gz` FTP-bestand | `EXACT_BYTES_TRANSIENT` | volledig | compressed bytes + basename bewaren, zonder FTP-credentials |
| `netatmo` | volledige `getpublicdata` JSON-body | `EXACT_BYTES_TRANSIENT` en bounded vóór decode | volledig | exacte body, SHA-256 en secretvrije receiptcontext duurzaam bewaren |
| `synoptic` | volledige Time Series JSON-body | `EXACT_BYTES_TRANSIENT` en bounded vóór decode | volledig | exacte body, SHA-256 en secretvrije receiptcontext duurzaam bewaren |
| `metar` | volledige `metars.cache.csv.gz` body | `EXACT_BYTES_TRANSIENT` | volledig | gzipbody en receiptheaders vóór `_decode` bewaren |

“Volledig” betekent dat de bytes samen met de contractueel genoemde
recordcontext volstaan om dezelfde parser opnieuw te draaien. Het betekent niet
dat de huidige parser meteorologisch al volledig gevalideerd is.

## 4. Feitelijke prioriteitsaudit

### KMI

De runtime gebruikt een KMI-appcatalogus en daarna een gerenderde afbeelding.
De image bevat geen bewezen native CRS, fysieke unit of QC-layer. De huidige
vaste geografische bbox en kleurenmapping zijn implementatiesemantiek, geen
bewijs van de oorspronkelijke radargeometrie of regenrate. Daarom blijven
native CRS, native resolutie, undetect en QC `UNKNOWN`. De catalogustijd is
externe metadata en moet samen met de image worden bewaard.

Het huidige transport bouwt een credential-bearing query. De contractlaag
verbiedt volledige URL's en querystrings in record-ID's. Het bestaande
hard-coded appcredential mag nooit in evidence, metadata, logs of IDs belanden.
Endpoint-specifieke hergebruik- en raw-retentietermen zijn nog niet bewezen;
retentie blijft daarom op `CONTRACT_REVIEW_HOLD`.

Primaire verwijzingen: [KMI appservice](https://app.meteo.be/services/appv4/),
[KMI juridische informatie](https://www.meteo.be/en/about-rmi/legal-information).

De officiële KMI-productaudit heeft daarnaast RADQPE geïdentificeerd als de
beoogde vNext-bron: 1-km Belgian Lambert 2008, een vijfminuten-rain-rateproduct
en GeoTIFF/ODIM-HDF5 met native tijd-, grid-, quantity- en
processingmetadata. Dat verandert het bestaande `kmi`-appcontract niet stil:
`kmi_radqpe` moet een afzonderlijk, versioned provider-/artifactcontract
houden met eigen toegang, licentie, fixtures en replay. De eerste
fail-closed shadowversie bestaat inmiddels, maar onderscheidt het native
product uitdrukkelijk van de publiek bereikbare en semantisch nog niet
bewezen `kmi_public_radar_composite`. Zie
`docs/KMI_PROVIDER_PRODUCT_AUDIT.md` en
`docs/KMI_RADQPE_SHADOW_ADAPTER.md`.

### RainViewer

RainViewer levert een JSON-manifest en XYZ-PNG-tiles. Een reproduceerbare
opname is dus een artifactset, niet één PNG. De officiële Weather Maps
documentatie zegt bovendien dat `frame.time` de framegeneratietijd is en niet
noodzakelijk de scantijd van elke bijdragende radar. Het contract noemt die
beperking expliciet. Er is geen per-pixel bron/QC of afzonderlijke
nodata/undetectwaarde.

De publieke API vereist attribution en is volgens de actuele voorwaarden
bedoeld voor persoonlijk, educatief en kleinschalig communitygebruik, zonder
SLA. Daardoor blijft raw retention op review hold; publieke toegang alleen is
geen bewaartoestemming.

Primaire verwijzingen: [API-overzicht](https://www.rainviewer.com/api.html),
[Weather Maps API](https://www.rainviewer.com/api/weather-maps-api.html),
[voorwaarden](https://www.rainviewer.com/terms.html).

### NOAA Enterprise Rain Rate

De officiële NOAA/NCEI datasetbeschrijving classificeert ENTRAIN als
instantaneous rainfall-ratekaarten, als NetCDF-4 met opgenomen attributen en
een nominale 2 km grid. In het artifact blijven `time_coverage_start`,
geospatial axis metadata, `RRQPE` scale/offset/fill en `DQF` gezaghebbend. De
parser aanvaardt momenteel alleen `DQF=0`.

De worker heeft de exacte NetCDF tijdelijk, maar bewaart alleen een afgeleid
JSON-artifact. Raw replay is pas werkelijk beschikbaar nadat object key en
NetCDF vóór windowing en decode zijn vastgelegd.

Primaire verwijzingen: [NOAA/NCEI ENTRAIN metadata](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C01710),
[publieke bucket](https://noaa-enterprise-rainrate-pds.s3.amazonaws.com/).

### OPERA

OPERA gebruikt HDF5 volgens het OPERA Data Information Model. `what` bevat
product/tijd/quantity, `where` projectie/dimensies/schaal en `how` aanvullende
product- en kwaliteitsmetadata. De Storm Tracker-parser gebruikt DBZH
gain/offset, nodata, undetect en `quality1`. Al die waarden blijven
`ARTIFACT_METADATA`; ze mogen niet providerbreed worden hard-coded.

De huidige worker bewaart de regionale afgeleide cellen/overlay, niet het
volledige HDF5-object. Open toegang via de CloudFerro-bucket bewijst op zichzelf
geen redistributie- of onbeperkte-retentietoestemming; dat juridische gat blijft
expliciet open.

Primaire verwijzingen: [EUMETNET OPERA/ODIM](https://www.eumetnet.eu/observations/),
[ODIM H5 2.4.1](https://www.eumetnet.eu/wp-content/uploads/2024/10/ODIM_H5_v2.4.1_final.pdf),
[huidige objectstore](https://s3.waw3-1.cloudferro.com/).

### DWD RADOLAN/RADVOR

De runtime haalt `composite_rv_LATEST.tar` en kiest uitsluitend het actuele
`_000-hd5`-member. DWD beschrijft RV als vijfminutenaccumulatie in mm; de parser
decodeert ACRR met artifact gain/offset en rekent die met factor 12 om naar
mm/h. Projectie, dimensies, pixelspacing, nodata en undetect komen uit ODIM
metadata.

DWD Open Data valt onder CC BY 4.0/GeoNutzV met bronvermelding. De originele
tar is geschikt voor volledige replay, maar de worker publiceert vandaag enkel
afgeleide gzip-JSON.

Primaire verwijzingen: [DWD RV Open Data](https://opendata.dwd.de/weather/radar/composite/rv/),
[RADOLAN/RADVOR-formaat](https://www.dwd.de/DE/leistungen/radolan/radolan_info/radolan_radvor_op_komposit_format_pdf.pdf),
[DWD datapolitiek](https://www.dwd.de/DE/derdwd/datenpolitik.html).

## 5. Overige bronverificatie

- KNMI publiceert de relevante radardatasets via Data Platform/WMS en vermeldt
  CC BY 4.0: [KNMI radar/WMS-catalogus](https://dataplatform.knmi.nl/dataset/?tags=Radar&tags=WMS).
- Météo-France DPRadar gebruikt een ODIM-HDF5 productrespons, maar
  product-/abonnementstermen moeten nog als duurzaam licentierecord worden
  vastgelegd: [DPRadar productroute](https://public-api.meteofrance.fr/public/DPRadar/v1/mosaiques/METROPOLE/observations/LAME_D_EAU/produit).
- Met Office wordt uit een publieke S3-objectlijst gelezen; de precieze
  feedlicentie en raw-retentie zijn nog niet bevestigd:
  [objectstore](https://met-office-radar-obs-data.s3.eu-west-2.amazonaws.com/).
- DPC gebruikt een catalogus plus SRI-download:
  [radar-API](https://radar-api.protezionecivile.it/),
  [radarportal](https://radar.protezionecivile.it/).
- AEMET beschrijft het nationale product als laagste-PPI-reflectiviteit in dBZ,
  niet als rain-rate. De per-file `ESCALA`-metadata bepaalt de RGBA-naar-waarde-
  intervallen; de bestaande byteklassen `1..8` zijn dus geen gecertificeerde
  legenda. De officiële OpenData-route vereist een envelope met afzonderlijke
  `datos`- en `metadatos`-responses. Hergebruik en lokale bewijsretentie zijn
  toegestaan met bronvermelding en behoud van de meegeleverde metadata:
  [radarhelp](https://www.aemet.es/en/eltiempo/observacion/radar/ayuda),
  [OpenData-specificatie](https://opendata.aemet.es/AEMET_OpenData_specification.json),
  [juridische kennisgeving](https://www.aemet.es/es/nota_legal).
- GOES RRQPE komt uit NOAA's open GOES-objectstore:
  [Registry of Open Data on AWS](https://registry.opendata.aws/noaa-goes/).
- H40B komt via de geauthenticeerde H SAF FTP-route; productspecifieke
  licentie/retentie moet nog worden geregistreerd:
  [H SAF](https://hsaf.meteoam.it/).
- Synoptic documenteert dat `precip_intervals` neerslag tussen opeenvolgende
  observaties is, niet een instantane rate:
  [Time Series](https://docs.synopticdata.com/services/time-series),
  [Precipitation Service](https://docs.synopticdata.com/services/precipitation-service-explained).
- AviationWeather adviseert de minuutververste gzipcache voor alle huidige
  METARs:
  [Data API en cache](https://aviationweather.gov/data/api/),
  [METAR productinformatie](https://aviationweather.gov/help/data/).
- Netatmo vereist nog een productspecifieke opslag-/redistributiereview:
  [`getpublicdata` documentatie](https://dev.netatmo.com/apidocumentation/weather#getpublicdata).

## 6. Redactie- en record-ID-regels

`validate_redacted` faalt gesloten voor:

- velden zoals authorization, password, client secret, API key en access- of
  refreshcredential;
- Bearer- en Basic-authwaarden;
- JWT-achtige waarden;
- URL-userinfo;
- signed URLs en gevoelige queryparameters, inclusief AWS signatures;
- assignmentvormen zoals `access_token=...`.

Record-ID-materiaal is strenger: volledige URL's, querystrings en fragmenten
zijn altijd verboden, ook als ze ogenschijnlijk geen geheim bevatten.
Bucket-relative keys, archiefmembernamen, station/report-ID's, nominale tijd en
SHA-256-digests zijn wel toegestaan. `derive_record_id` hasht de canonieke
velden deterministisch; de upstream locator staat nooit leesbaar in het ID.

## 7. Open licentie- en retentiegaten

Raw retention mag nog niet live worden ingeschakeld voor:

- KMI appservice;
- Météo-France DPRadar;
- DPC;
- Met Office S3-feed;
- OPERA/OpenRadar objectstore;
- RainViewer;
- H SAF H40B;
- Netatmo;
- Synoptic en de afzonderlijke bijdragende stationsnetwerken.

Voor DWD en KNMI is attribution-retentie contractueel ingevuld. NOAA ERR,
GOES RRQPE en AviationWeather/METAR staan in de publieke
replay-evidenceklasse, met bronvermelding. Ook voor die bronnen moet een live
retentie-implementatie nog quota, lifecycle, ontvangstbewijs, checksum,
corruptie-afhandeling en verwijderbeleid krijgen; deze shadowlaag doet dat
bewust niet.

## 8. Veilige implementatievolgorde na deze shadowfase

1. Bewaak de gecorrigeerde MeteoLux-taxonomie en bevestig alle open licenties.
2. Voeg per transport een bounded capture vóór decode toe, zonder de parser of
   bronselectie te veranderen.
3. Bewaar body-SHA-256, media type, receipt time en secretvrije recordcontext.
4. Leid voor KMI, RainViewer en DPC de volledige expected memberlijst
   onafhankelijk uit de upstream catalogus/requestset af en lever de complete
   set aan de inmiddels beschikbare atomische `RawArtifactStore.store_set()`;
   ontbrekende members blijven vóór iedere write fail-closed.
5. Verifieer raw replay byte-for-byte in offline tests.
6. Koppel pas daarna shadow writes aan de evidence store en voer quota-,
   crash-, corruptie- en retentietests uit.

Tot die stappen slagen, blijft `SHADOW_ONLY = True` en verandert deze laag geen
enkele live fetch, selectie, observatie of forecast.
