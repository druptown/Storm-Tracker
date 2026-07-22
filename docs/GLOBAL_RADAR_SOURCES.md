# Wereldwijde catalogus van lokale neerslagbronnen

Status: eerste geverifieerde inventaris, 18 juli 2026.

De machineleesbare selectievolgorde staat in
`custom_components/storm_tracker_v3/provider_policy.json`. De volgorde per
datatype betekent: continentale/operationele basis, nationale verificatie of
verfijning, daarna globale fallback. Een bronnaam in de matrix betekent niet
automatisch dat de provider al geïmplementeerd is.

Voorbeeld Duitsland:

| Datatype | Basis | Lokale meerwaarde | Fallback |
|---|---|---|---|
| Neerslagradar | OPERA | DWD RADOLAN | RainViewer |
| Bliksem | Blitzortung | — | EUMETSAT LI |
| Grondvalidatie | Netatmo | — | Open-Meteo |

EUMETSAT LI is dus geen neerslagradar. Het is de overkoepelende optische
bliksembron voor het relevante Europese lengtegraadgebied.

## Selectieregels

Een bron komt alleen in aanmerking wanneer hij officieel, gratis toegankelijk
en voldoende actueel is voor operationele neerslagdetectie. Een publieke
webkaart is niet automatisch een herbruikbare databron. Licentie, technische
toegang en georeferentie moeten afzonderlijk bevestigd zijn.

Elke nationale provider is standaard `sleeping`. Hij wordt alleen geactiveerd
als een actieve RegionEngine zijn dekkingspolygoon raakt. Alle engines delen
dezelfde download en parser. Na vertrek van de laatste engine volgt een korte
cooldown en daarna opnieuw `sleeping`.

Statusmodel: `sleeping`, `initializing`, `active`, `stale`, `rate_limited`,
`error` en `unsupported`.

## Bevestigde operationele kandidaten

| Regio | Officiële bron | Product/toegang | Formaat | Toegang | Eerste oordeel |
|---|---|---|---|---|---|
| Europa | EUMETNET OPERA via OpenRadar | Europese radarcomposiet | ODIM HDF5 | publiek S3 | Reeds actief; continentale basisbron |
| België | KMI | nationaal radarbeeld | beeld/raster | publiek | Reeds actief; nationale validatie |
| Nederland | KNMI Data Platform / ADAGUC | radar-WMS | PNG via WMS | gratis sleutel | Provider bestaat; UI-config ontbreekt |
| Duitsland | [DWD RADOLAN](https://opendata.dwd.de/climate_environment/CDC/help/RADOLAN/Unterstuetzungsdokumente/) | nationale composieten | RADOLAN binair/HDF5 | anoniem | Zeer sterke eerstvolgende provider |
| Frankrijk | [Météo-France Open Data](https://donneespubliques.meteofrance.com/) | actuele radarproducten | API/raster | gratis account | Sterk; exacte actuele productroute nog vastleggen |
| Oostenrijk | [GeoSphere Austria Dataset API](https://dataset.api.hub.geosphere.at/v1/docs/index.html) | open meteorologische datasets | API/raster | anoniem waar publiek | CC BY 4.0; radarcollectie nog exact selecteren |
| Italië | [ItaliaMeteo SRI](https://www.dati.gov.it/node/view-dataset/dataset?id=3216398d-5acd-4028-8654-81aced35ed78) | Surface Rainfall Intensity, 5 min | raster/GRIB via MeteoHub | gratis registratie | Nationale composiet, CC BY 4.0 |
| Luxemburg | [MeteoLux HVD](https://www.meteolux.lu/fr/actualites/mise-a-disposition-de-donnees-de-fortes-valeurs-hvd/) | Findel minuutmetingen | JSON API | anoniem | Geen eigen open radar gevonden; wel grondvalidatie |
| Verenigd Koninkrijk | [Met Office UK radar](https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/data/pwms037-038_uk_radar_data.pdf) | UK 1 km rain rate | NIMROD | abonnement/voorwaarden te bevestigen | Technisch sterk, gratis operationele toegang nog onbevestigd |
| Verenigde Staten en territoria | [NOAA MRMS](https://vlab.noaa.gov/web/osti-r2o/mrms) | realtime multi-radar mozaïeken | GRIB2 via HTTP | anoniem | Zeer sterk; CONUS, Alaska, Hawaii, Caribbean, Guam |
| Canada | [MSC GeoMet](https://api.weather.gc.ca/?f=html) | realtime radar/precipitation | OGC API/WMS | anoniem | Zeer sterk; server-side uitsnede mogelijk |
| Australië | [Bureau of Meteorology radar feeds](https://www.bom.gov.au/catalogue/data-feeds.shtml) | individuele radars, 5 min; nationaal mozaïek | georefereerde beelden | publiek, gebruiksvoorwaarden controleren | Sterke kandidaat |
| Japan | [JMA radar/nowcast](https://www.data.jma.go.jp/developer/weatherdataguide/appendix/1-1-c.html) | analyse 250 m–1 km, 5 min; nowcast | PNG/GRIB2 | webdata publiek; bulkroute apart | Sterk, parser/georeferentie onderzoeken |
| Zuid-Korea | [KMA URL API](https://data.kma.go.kr/download/downloadUrlApi.do) | site- en composietradar | bestanden via URL API | gratis account, 1000 calls/dag | Bruikbaar met rate-budget |
| Taiwan | [CWA composietradar](https://opendata.cwa.gov.tw/dataset/observation/O-A0058-005) | 3600×3600 composiet, 10 min | georefereerbare PNG | gratis account/API-sleutel | Sterk; vaste lon/lat-bounds gepubliceerd |
| Hongkong | [HKO radar](https://www.hko.gov.hk/en/wxinfo/radars/radar_range1.htm) | 64/128/256 km, 6 min | beeld/KML | publiek | Bruikbaar als beeldgeometrie en hergebruik toegestaan zijn |
| Colombia | [IDEAM radar](https://www.ideam.gov.co/nuestra-entidad/servicio-de-pronosticos-y-alertas/guia-de-descarga-y-visualizacion-de-datos-de-radar-meteorologico-de-ideam) | vier dual-pol radars, 5 min | Vaisala IRIS RAW via publiek S3 | anoniem | Zeer waardevol maar zware volumeparser; live actualiteit verifiëren |
| Singapore | [NEA realtime rainfall](https://data.gov.sg/collections/1459/view) | minuutmetingen per station | JSON API | anoniem | Geen open radar gevonden; uitstekende grondvalidatie |
| Noord-Italië | [MeteoTrentino radar](https://dati.meteotrentino.it/) | radar GeoTIFF | GeoTIFF webservice | publiek voor GeoTIFF | Regionale aanvulling op ItaliaMeteo |
| Zuid-Tirol | [Civil Protection radar](https://www.dati.gov.it/node/view-dataset/dataset?id=14fb3e59-68f4-4e53-9d51-69d2143dc0f5) | 15 beelden, 5 min | JSON/XML + beelden | publiek | Regionale validatie Alpen/noord-Italië |

## Niet als lokale radar classificeren

- EUMETSAT LI en NOAA GOES GLM zijn bliksemsatellieten, geen neerslagradars.
- NOAA GOES-18/19 ABI RRQPE is wÃ©l een operationele satellietschatting van
  neerslagsnelheid voor Amerika (2 km, 10 minuten) en dient als lage-prioriteit
  fallback na lokale radar/RainViewer.
- RainViewer is een wereldwijde aggregatiefallback, geen officiële nationale bron.
- Historische radararchieven zonder realtime stroom zijn nuttig voor tests en
  kalibratie, maar niet voor operationele detectie.
- Stations-API's zoals MeteoLux en Singapore NEA leveren grondwaarheid en
  mogen radar bevestigen, maar maken zelf geen neerslagpolygonen.

## Verzamelproviders eerst

Voordat een nationale parser wordt gebouwd, controleren we altijd of dezelfde
data al betrouwbaar via een regionale verzamelprovider beschikbaar is.

| Verzamelprovider | Werkelijke rol | Gevolg voor Storm Tracker |
|---|---|---|
| EUMETNET OPERA / MeteoGate | Meer dan 160 radars uit 33 Europese lidstaten; 1 km maximum-reflectiviteit om de 5 minuten, plus rain-rate en accumulatiecomposieten | Blijft de primaire Europese download. Nationale bronnen alleen toevoegen voor betere kwaliteit, lagere latency, lokale gaten of onafhankelijke verificatie |
| NOAA MRMS | Kwaliteitsgecontroleerde multi-radar/multi-sensorcomposieten voor de VS en territoria; bevat ook invoer uit Canadese radars | MRMS verkiezen boven tientallen individuele NEXRAD-parsers; MSC GeoMet blijft nuttig voor volledige Canadese dekking en validatie |
| WMO WIS2 | Wereldwijde catalogus, MQTT-notificaties en HTTP-downloads van door nationale diensten gepubliceerde datasets | Zeer kansrijke toekomstige discovery-/transportlaag, maar nog geen uniforme wereldwijde radarcomposiet. Per dataset blijven dekking, licentie en formaat relevant |
| WIS2 Global Cache | Cachet uitsluitend WMO-`core` data zonder toegangsbeperking | Kan downloads centraliseren wanneer radar als `core` wordt gepubliceerd; `recommended` data blijft bij de nationale uitgever en kan toegangseisen hebben |
| AWS Open Data Registry | Catalogus/hosting voor onder meer NEXRAD, FMI en IDEAM | Handige uniforme S3-transportlaag, maar geen geharmoniseerd radarproduct of wereldcomposiet |
| RainViewer | Commerciële wereldwijde radaraggregatie met publieke API | Behouden als globale fallback/visuele referentie; niet gebruiken als nationale grondwaarheid |

### Selectieregel

Een nationale bron wordt alleen geïmplementeerd als hij aantoonbaar minstens
één voordeel biedt boven de verzamelprovider: hogere ruimtelijke resolutie,
lagere publicatielatentie, betere quality flags, dekking van een gat, een
bruikbare nowcast of onafhankelijke bevestiging van zwakke echo's. Anders
blijft hij uitsluitend in de catalogus en wordt geen extra provider gebouwd.

## Verificatiewachtrij

De volgende meteorologische diensten hebben radarwebkaarten of open-data-
portalen, maar hun gratis machine-to-machine toegang, licentie of georeferentie
moet nog officieel worden bevestigd voordat we een provider plannen:

- Spanje (AEMET), Portugal (IPMA), Zwitserland (MeteoSwiss)
- Denemarken (DMI), Zweden (SMHI), Noorwegen (MET Norway), Finland (FMI)
- Polen (IMGW), Tsjechië (CHMI), Slowakije (SHMÚ), Slovenië (ARSO), Kroatië (DHMZ)
- Griekenland, Roemenië, Bulgarije, Turkije en de Baltische staten
- Mexico, Brazilië, Argentinië, Chili, Peru en overige Latijns-Amerikaanse netten
- India, Bangladesh, Pakistan, Thailand, Maleisië, Indonesië en Filipijnen
- Nieuw-Zeeland en eilandstaten in de Pacific
- Zuid-Afrika en overige Afrikaanse nationale diensten

## Implementatievolgorde

1. Generieke provider-lifecycle: polygondekking, gedeelde fetch, cooldown,
   rate-budget, statusdiagnostiek en automatische slaapstand.
2. DWD RADOLAN als eerste nieuwe parser en referentie-implementatie.
3. NOAA MRMS en MSC GeoMet voor grote gratis Noord-Amerikaanse dekking.
4. KNMI UI-aansluiting, Météo-France, GeoSphere Austria en ItaliaMeteo.
5. BOM, JMA, CWA en KMA.
6. Regionale beeldbronnen alleen als licentie en georeferentie hard bevestigd zijn.

Een provider gaat pas in productie na een live-downloadtest, ouderdomscontrole,
geometriecontrole tegen het officiële beeld, parserfixtures en een bewezen
`active -> sleeping -> active` lifecycle zonder dubbele downloads.
