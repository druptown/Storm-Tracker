# AEMET national radar: fail-closed productgate

Status: uitsluitend lokaal en volledig ongewired. Deze audit verandert geen
service, scheduler, providerregistry, configuratie, versie of deploybestand.
Er is geen live wiring en er wordt geen AEMET-pixel als weerfeit toegelaten.

## Besluit

De huidige workerjob `aemet_radar_fetch` is **geen gecertificeerde
kwantitatieve decoder**. Het officiële product is een nationaal mozaïek van de
laagste PPI-reflectiviteit, uitgedrukt in dBZ-intervallen. Het is dus niet als
neerslagintensiteit (mm/u) gecertificeerd. AEMET zegt bovendien expliciet dat
reflectiviteit en neerslagintensiteit niet één-op-één overeenkomen.

Daarom is in
`storm_tracker_worker/aemet_product_gate.py` een provider-specifieke,
fail-closed evidencegate toegevoegd. Die module doet geen netwerk-I/O, leest
geen pixels, schrijft niets en wordt nergens operationeel geïmporteerd. Zowel
capturecandidate, kwantitatieve decode, rain-rate, detectie, forecast als live
admission blijven `False`.

## Wat AEMET zelf wél bevestigt

1. Het composiet voor Spanje (schiereiland en Balearen, zonder Canarische
   radar) combineert de laagste PPI-reflectiviteitsscans.
2. De grootheid is reflectiviteit en de legenda-intervallen zijn dBZ.
3. De download is GeoTIFF in EPSG:4326.
4. Per bestand hoort veld `ESCALA` de RGBA-kleuren aan de intervallen van de
   meteorologische variabele te koppelen.
5. Donkergrijs duidt gebieden zonder dekking van een operationele radar aan.
6. De gedocumenteerde OpenData-route is
   `GET /api/red/radar/nacional`, vereist een `api_key`, noemt een periodiciteit
   van 30 minuten en retourneert een JSON-envelope met verplichte velden
   `datos` en `metadatos`.
7. AEMET staat commercieel en niet-commercieel hergebruik, reproductie,
   distributie en transformatie toe. Bronvermelding, behoud van aanwezige
   update-/voorwaardenmetadata, correcte betekenis en geen suggestie van
   sponsoring blijven verplicht.

## Exacte blokkeergaten

De gate houdt de volgende keys bewust gesloten:

- `rain_rate_interpretation`: reflectiviteit is geen unieke rain-rate; een
  vaste Marshall-Palmeromzetting is geen universele meteorologische waarheid.
- `exact_grid_geometry`: EPSG:4326 is bekend, maar dimensies, resolutie,
  pixel-support, axisvolgorde, oorsprong en affine transformatie zijn niet als
  vast productspec voor deze revisie bewezen.
- `pixel_scale_mapping`: de huidige code gebruikt samplewaarden `1..8` als
  intensiteitsklassen en negeert `ESCALA`; AEMET documenteert juist dat de
  bestandsspecifieke RGBA-naar-intervalmapping uit `ESCALA` moet komen.
- `timestamp_timezone`: een raster heeft datum en tijd, maar geen geaudite
  bron bewijst dat `YYYYMMDDHHMM` in `down_radw…` UTC is.
- `observation_support`: scanstart, scaneinde, composiettijd, midpoint en het
  temporele supportinterval zijn niet vastgelegd.
- `nodata_noecho_coverage`: donkergrijs betekent geen operationele dekking,
  maar de exacte sample/RGBA-codes voor NoData, geen echo en geen dekking zijn
  niet contractueel vastgelegd.
- `pixel_qc_semantics`: AEMET beschrijft keuze op orografische blokkering en
  afstand, maar niet een volledige pixel-QC-laag of codelegenda.
- `legacy_transport_contract`: de bestaande job gebruikt
  `https://www.aemet.es/es/api-eltiempo/radar/download/compo`; de officiële
  OpenData-specificatie beschrijft in plaats daarvan de geauthenticeerde
  `/api/red/radar/nacional`-route en legt het tar/member/cadanscontract van de
  bestaande route niet vast.
- `complete_artifact_set`: de officiële succesrespons verplicht `datos` én
  `metadatos`, maar de bestaande job bewaart envelope, datarespons en
  metadatarespons niet atomair als exacte schema-3-bytes vóór decode.
- `official_golden_artifact`: er is nog geen actuele officiële
  envelope/data/metadata-set met gecontroleerde checksums als golden fixture.

De algemene hergebruikstoelating is dus géén blocker; de bovenstaande
meteorologische en artefactcontracten zijn dat wel.

## Vereiste veilige vervolgstap

Pas wanneer alle gates met een actuele officiële productset bewijsbaar zijn,
mag een nieuwe capturekandidaat worden gemaakt. Die moet via de gedocumenteerde
OpenData-route exact en atomair bewaren:

1. `opendata_envelope_json`;
2. `opendata_data_response`;
3. `opendata_metadata_response`.

De `api_key` blijft uitsluitend transportgeheim en mag nooit in raw bytes,
receiptprovenance, record-ID, foutmelding of log belanden. Selectie-URL's uit
`datos` en `metadatos` moeten in geheugen worden gevalideerd; decode mag pas na
duurzame commit van de complete set. Daarna moet de decoder `ESCALA`,
GeoTIFF-georeferentie, timestamp/support, coverage/no-echo/NoData en QC exact
valideren. Zonder één van die feiten blijft de weerstatus `unknown`.

## Uitsluitend officiële primaire bronnen

- AEMET radarhelp:
  https://www.aemet.es/en/eltiempo/observacion/radar/ayuda
- AEMET OpenData OpenAPI-specificatie:
  https://opendata.aemet.es/AEMET_OpenData_specification.json
- AEMET juridische hergebruikvoorwaarden:
  https://www.aemet.es/es/nota_legal

Auditdatum: 2 augustus 2026.
