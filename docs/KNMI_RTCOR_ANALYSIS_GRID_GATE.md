# KNMI RTCOR/NL25 analysis-grid gate

Status: **withheld**, worker-only, volledig ongewired, niet gescheduled en niet
gedeployed. Er wordt geen `AffineGridTransformCertificate` uitgegeven.

## Besluit

De gridgetallen zijn bekend, maar het sterkere bewijs dat nodig is om ze als
exacte pixelranden van iedere echte `RAD_NL25_COR_NA`-payload te certificeren is
nog niet gesloten. Daarom bevat
`storm_tracker_worker/knmi_rtcor_analysis_grid_gate.py` uitsluitend een
fail-closed gate. De mapping met bekende waarden heet bewust
`informational_geometry`; ze is geen adapter, geen transformreceipt en geen
operationele toelating.

Dit onderscheid is belangrijk. Een array met vorm `765 x 700` bewijst geen CRS,
geen positie van pixel `(0,0)`, geen asrichting en geen pixelondersteuning. Ook
een synthetisch HDF-bestand dat alle verwachte attributen kopieert, bewijst niet
dat een officiële KNMI-payload dezelfde autoriteit heeft.

## Wat de primaire bronnen wel bewijzen

### Datasetidentiteit en meteorologische betekenis

De officiële [KNMI Data Platform-catalogus][catalog] beschrijft
`nl_rdr_data_rtcor_5m/1.0` als een HDF5-grid met vijfminutenaccumulaties uit
radar, gecorrigeerd met regenmeters. De peer-reviewed
[ESSD-productbeschrijving][essd] bevestigt de vijfminuten-QPE, een raster van
ongeveer 1 km² en dat de KNMI HDF5-bestanden geografische metadata bevatten.

Dit is product- en hoeveelheidsbewijs. Het is geen volledig pixel-edge-contract.

### HDF 3.6-semantiek

De officiële [KNMI HDF5-specificatie v3.6][hdf36] zegt onder meer:

- HDF-bestanden zijn zelfbeschrijvend en benodigde metadata zit in het bestand;
- `image_data` is opgeslagen als `Nrows x Ncolumns`;
- `geo_number_columns` is de x-dimensie en `geo_number_rows` de y-dimensie;
- `geo_column_offset` en `geo_row_offset` zijn de offsets van pixel `(0,0)`;
- `geo_pixel_def` bepaalt waar de geolocatie binnen de pixel ligt;
- `DISPLAY_ORIGIN` hoeft alleen aanwezig te zijn bij een afwijkende oriëntatie.

Dezelfde bron geeft echter geen productgebonden numeriek NL25-profiel. Ook
schrijft ze alleen voorbeelden `CENTRE` en `LU`; ze definieert niet formeel dat
`LU` exact de buitenste linker/bovenrand is in de betekenis die de gesloten
analysis-gridtransform vereist.

### HDF 3.8 NL25-profiel

De officiële [KNMI HDF5-specificatie v3.8][hdf38] publiceert voor NL25:

| Veld | Officiële gedrukte waarde |
|---|---:|
| kolommen | 700 |
| rijen | 765 |
| pixelparameters | `X,Y` |
| eenheden | `KM,KM` |
| pixeldefinitie | `LU` |
| kolomoffset | 0,0 km |
| rijoffset | 3649,9814 km |
| pixelgrootte x | +1,0000035 km |
| pixelgrootte y | −1,0000049 km |
| projectie | stereografisch, `lat_0=90`, `lon_0=0`, `lat_ts=60` |
| ellipsoïde | `a=6378,388 km`, `b=6356,906 km` |

De gedrukte hoekvolgorde is zuidwest, noordwest, noordoost, zuidoost. Als — en
alleen als — `LU` officieel als buitenste linker/bovenrand wordt bevestigd,
zouden de rekenkundige rasterranden zijn:

- west: 0,0 km;
- noord: 3649,9814 km;
- oost: `700 × 1,0000035 = 700,00245 km`;
- zuid: `3649,9814 − 765 × 1,0000049 = 2884,9776515 km`.

De gate bewaart deze waarden uitsluitend als informatie en noemt de aanname in
de veldnaam (`...if_lu_is_outer_edge...`).

## Waarom het certificaat nu terecht dicht blijft

1. **Twee officiële CRS-autoriteiten spreken elkaar tegen.** De actuele
   datasetcatalogus (edition 4) publiceert `a=6378,14 km, b=6356,75 km`; de HDF
   3.8-NL25-bijlage publiceert `a=6378,388 km, b=6356,906 km`. Het verschil is
   veel groter dan afronding. De code mag niet zelf een winnaar kiezen.

2. **Tagversie en numeriek profiel zijn niet productgebonden gesloten.** De
   lokaal genoteerde live payload declareerde HDF-tag 3.6. De exacte
   NL25-getallen komen uit de 3.8-bijlage. V3.6 beschrijft de velden en zegt dat
   haar wijzigingen backwards compatible zijn, maar bevat zelf niet het latere
   NL25-profiel.

3. **De live golden is niet herverifieerbaar aanwezig.** Lokaal zijn alleen de
   bestandsnaam `RAD_NL25_RAC_RT_202608012345.h5`, grootte 39.925 bytes en
   SHA-256 `a138f091fb61181d2f83e5892e0ba5db19088211d3fc26ccff41bb90b9927d8d`
   genoteerd. De officiële bytes en een contentgebonden geografisch receipt
   zitten niet in de validatiecorpus. De notitie kan daarom de bronstrijd niet
   oplossen.

4. **`LU` is geen formeel pixel-edge-bewijs.** De betekenis is aannemelijk,
   maar het gevraagde betrouwbaarheidsniveau laat geen afkortingsinterpretatie
   toe.

5. **De huidige decoder bindt `DISPLAY_ORIGIN` niet.** Hij valideert vorm,
   X/Y, offsets, pixelgrootte, `LU`, hoeken en projectiestring, maar niet de
   expliciete/standaard beeldoriëntatie van alle drie semantische beelden.

6. **De kilometre-CRS is nog niet aan één metre-CRS gebonden.** De transform
   rekent in meters. Vermenigvuldigen met 1000 is rekenkundig triviaal, maar de
   certificate-ID moet ook exact verklaren welke CRS-string, ellipsoïde,
   asvolgorde en round-tripgolden daarbij horen.

## Machineleesbaar contract

`tests/fixtures/knmi_rtcor_analysis_grid_authority.json` legt de afzonderlijke
autoriteiten vast zonder ze samen te voegen. De lokale observatie heeft de
klasse `local_unretained_live_observation`, niet `official_*`. De evaluator:

- begrenst diepte, objecten, records, tekst en canonieke JSON-grootte;
- geeft altijd status `withheld`, `certificate=None` en
  `official_certificate_eligible=false`;
- meldt ontbrekende, dubbele, vreemde of synthetische evidence apart;
- behoudt iedere wetenschappelijke blocker met de exact vereiste vervolgbewijs;
- heeft een aparte issuefunctie die altijd een
  `KnmiRtcorGridCertificateWithheldError` werpt.

Een testfixture met alle goede getallen kan de officiële gate dus niet openen.
Tests gebruiken geen netwerk en de module komt nergens voor in service,
scheduler, configuratie of Home Assistant.

## Exacte vervolgstappen om de gate later te openen

1. Bewaar minimaal één huidige officiële RTCOR-HDF als schema-3 raw evidence en
   maak een begrensd receipt van de volledige `/geographic`,
   `/geographic/map_projection` en oriëntatie-attributen van image1–3.
2. Verkrijg of vind een productgebonden KNMI-correctie die het verschil tussen
   catalogus- en HDF-ellipsoïde beslecht.
3. Bind officieel of via een door KNMI gepubliceerd voorbeeld `LU` aan de exacte
   buitenste pixelrand van array `[0,0]`.
4. Laat de decoder afwezig/standaard upper-left of expliciet `UL` voor alle
   semantische lagen verifiëren en elke andere oriëntatie weigeren.
5. Definieer één canonieke metrische CRS en verifieer rand- en
   round-tripcoördinaten met een onafhankelijk golden.
6. Bouw daarna een **nieuwe**, gereviewde certificateversie. De huidige gate mag
   niet worden afgezwakt om groen te worden.

[catalog]: https://dataplatform.knmi.nl/dataset/nl-rdr-data-rtcor-5m-1-0
[hdf36]: https://cdn.knmi.nl/knmi/pdf/bibliotheek/knmipubIR/IR2009-01.pdf
[hdf38]: https://cdn.knmi.nl/system/data_center_publications/files/000/072/301/original/hdftag38.pdf?1739444428=
[essd]: https://essd.copernicus.org/articles/17/4715/2025/
