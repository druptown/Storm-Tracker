# KMI official delivery gate

Status: `external_contract_blocked`  
Auditdatum: 2026-08-09  
Operationele admission: `false`

Dit document voorkomt dat drie verschillende KMI-producten opnieuw als één
kwantitatieve radarbron worden behandeld. Een bereikbare afbeelding of
GeoTIFF is niet automatisch een gevalideerde neerslaghoeveelheid.

## Besluit per product

| Product | Bewezen levering | Bewezen meteorologische betekenis | Toegelaten gebruik |
|---|---|---|---|
| Native RADQPE ODIM-HDF5/GeoTIFF | Geen openbaar deliverycontract gevonden | Ja: officieel kwantitatief QPE-product | Alleen na schriftelijk delivery- en retentiecontract |
| `radar:belgian_rainfall_composite` WCS | Anoniem, actueel, vijfminutencadans | Nee: `GRAY_INDEX`, geen neerslageenheid en geen eenduidige nodata | Exact-raw semantische shadow-audit; nooit mm/u afleiden |
| app-v4 beelden | Gerenderde mobiele-appservice | Categorische visualisatie | Gedecommissioneerd voor acquisitie en kwantitatieve analyse |
| RADCLIM/RADFLOOD21 | Historische officiële datasets | Kwantitatieve accumulaties | Replay en validatie, nooit als live product |

## Live publieke WCS-audit

De officiële KMI GeoServer leverde anoniem een tijdspecifieke GeoTIFF voor
`radar__belgian_rainfall_composite`. De gecontroleerde catalogus declareerde:

- vijfminutencadans en een rollend venster van ongeveer dertien uur;
- raster `892 x 538` in EPSG:4326;
- één `float32`-band met veldnaam `GRAY_INDEX`;
- een stralingseenheid, niet `mm`, `mm/h` of reflectiviteit;
- `NaN` als nil-waarde.

De ontvangen TIFF was 4.194.720 bytes met SHA-256
`f78a841fc50105698bd4beb2709a8396a2789b756bd4afd20d3e0466c88d9252`.
Hij bevatte geen embedded nominale tijd, quantity, neerslageenheid of
GDAL-nodata-tag. Er kwamen zowel negatieve waarden als `-999` voor. Daarom mag
`-999` niet op basis van observatie als nodata worden vastgelegd en mag de
numerieke band niet als neerslagsnelheid worden geïnterpreteerd.

De audit gebruikte uitsluitend de officiële endpoints:

- [WMS-capabilities](https://opendata.meteo.be/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities)
- [WCS-capabilities](https://opendata.meteo.be/geoserver/ows?service=WCS&version=2.0.1&request=GetCapabilities)
- [WCS DescribeCoverage](https://opendata.meteo.be/geoserver/ows?service=WCS&version=2.0.1&request=DescribeCoverage&coverageId=radar__belgian_rainfall_composite)

## Native RADQPE

De officiële [RADQPE User Guide](https://radli.meteo.be/uploads/media/63d3e7525ab68/radqpe-userguide.pdf?v20241107-0827=)
bevestigt het inhoudelijk gewenste product: Belgian Lambert 2008, een
kilometerresolutie, UTC, vijfminutenproducten en machineleesbare GeoTIFF- en
ODIM-HDF5-uitvoer. De gids beschrijft ook historische beschikbaarheid op
aanvraag.

Nog niet publiek en exact bewezen zijn:

- endpoint en authenticatiemethode;
- actuele gridafmetingen en oorsprong;
- response-, deadline- en rate-limieten;
- actuele HDF5 `quantity`, `gain`, `offset`, `nodata`, `undetect` en QC-velden;
- productie-, retentie-, publicatie- en afgeleide-verwerkingsrechten.

Zonder die gegevens mag de worker geen transportbudget, decoderprofiel of
operationele cadence verzinnen.

## Juridische gate

De officiële Open Data-voorwaarden staan hergebruik met bronvermelding toe,
terwijl de algemene KMI-dienstenvoorwaarden voor bepaalde gegevens eigen
gebruik en schriftelijke toestemming voor verspreiding of toegevoegde waarde
vereisen. De publieke radarcoverage heeft geen datasetspecifieke metadata die
deze tegenstelling oplost en publiceert geen gegarandeerde retentieduur.

Daarom blijven `retention_eligible=false` en
`source_reclaim_authorized=false` hard totdat KMI schriftelijk bevestigt welk
contract op het gekozen product van toepassing is.

## Minimale promotieroute

1. Verkrijg van KMI één exact contract voor native RADQPE met endpoint,
   authenticatie, limieten, cadence, replayretentie, attributie en rechten op
   afgeleide verwerking.
2. Commit de ontvangen originele bytes in de worker vóór iedere decode.
3. Lees quantity, schaal, offset, nodata, undetect, projectie en producttijd
   uitsluitend uit het ontvangen artefact en het getekende productcontract.
4. Draai meerdere echte weersituaties, held-out groundvergelijking,
   restart/power-loss en resource-soak.
5. Open meteorologische admission pas na onafhankelijke review; publieke WCS
   en app-v4 mogen nooit als stil alternatief voor native RADQPE dienen.

De huidige catalogussplitsing is daarmee correct: app-v4 is gedecommissioned,
publieke WCS blijft `unknown_quantity`, en native RADQPE blijft
`external_contract_blocked`.
