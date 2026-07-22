# Storm Tracker V3 — Versiegeschiedenis

# 0.4.86

- Detecteer expliciet wanneer de gevraagde RegionEngine-radius door de rand van
  het OPERA-composiet wordt afgeknipt.
- Laat RainViewer operationeel overnemen wanneer OPERA aan zo'n dekkingsrand
  alleen onbevestigde cellen toont maar RainViewer wel lokale neerslag ziet.
- Behoud OPERA wanneer de randcellen onafhankelijk bevestigd zijn.
- Gebruik voor zichtbare en verborgen bliksemsymbolen exact dezelfde
  geografische bliksemhull; de schakelaar wijzigt alleen nog de symbolen.

# 0.4.85

- Publiceer goedkope persistente collectortellers voor frames, rasterpunten,
  vergelijkingen, bronnen, regio's, databereik en totale SQLite/WAL-grootte.
- Toon deze status in het technische paneel van de multi-targetkaart, inclusief
  de omvang van de laatste schrijfbatch.
- Herstel de tellers bij het opstarten rechtstreeks uit de database en vermijd
  daarna dure volledige tellingen tijdens iedere vijfminutencyclus.
- Voeg een afzonderlijke rode pulserende bliksemenvelop toe rond inslagen van
  de laatste vijf minuten, ook buiten de radarregen; de regenfootprint zelf
  blijft exact ongewijzigd.
- Cluster bliksemzones regionaal en markeer diagnostisch of radarneerslag zich
  binnen 25 km bevindt of dat het een `lightning_only`-zone is.

# 0.4.84

- Leg alle regionale providerframes, inclusief geldige droge frames, permanent
  vast in een afzonderlijke SQLite-database onder Home Assistant `.storage`.
- Bewaar per frame de bron, geografische enginesleutel, producttijd en alle
  bezette 0,10-gradenrasterpunten met maximale intensiteit en kwaliteit.
- Bewaar iedere exacte paarsgewijze vergelijking append-only voor latere
  offline analyse; er geldt bewust geen automatische retentie of kalibratie.
- Gebruik WAL, transacties en schrijven buiten de HA-eventloop, en plaats een
  mislukte batch terug in het geheugen voor een volgende poging.

# 0.4.83

- Vervang de OPERA-centrische kalibratie door regionale kruisvalidatie tussen
  alle gelijktijdig beschikbare operationele neerslagproviders.
- Rapporteer scores per providerpaar en RegionEngine zonder een bron vooraf als
  absolute waarheid te behandelen.
- Koppel wachtende vergelijkingsframes aan zowel runtime-engine als stabiele
  geografische sleutel, zodat een verplaatsende persoon nooit met frames uit
  zijn vorige regio wordt vergeleken.
- Houd kalibratie observerend: de gemeten scores wijzigen nog geen operationele
  filters of providerdata.

# 0.4.82

- Isoleer de OPERA- en RainViewer-fetches per RegionEngine: regio's worden
  parallel verwerkt en iedere fetch heeft een eigen harde timeout, zodat een
  trage buitenlandse regio de overige targets niet ophoudt.
- Voeg een circuit breaker toe aan nationale providers: na drie opeenvolgende
  fouten volgt vijftien minuten cooldown en daarna één gecontroleerde proef.
- Markeer een bronwissel tien minuten lang als overgang en verlaag tijdens die
  periode de prognosezekerheid met tien procentpunten. De echte pixels van de
  nieuwe bron blijven ongewijzigd; incompatibele radarrasters worden niet
  kunstmatig gemiddeld.
- Activeer een regionale luchtdruktrend pas na dertig minuten aaneengesloten
  historie van minstens drie vergelijkbare stations. Tijdens de opwarming
  blijft de trend expliciet `onvoldoende_data`/`initializing`.

# 0.4.81

- Begrens iedere nationale providerpoll afzonderlijk tot twintig seconden en
  haal onafhankelijke nationale providers parallel op.
- Begrens daarnaast de vier volledige fasen van de vijfminutencyclus, zodat
  een hangende externe verbinding de volgende cyclus nooit onbeperkt blokkeert.
- Voeg harde timeouts en foutdiagnostiek toe aan EUMETSAT LI en NOAA GOES GLM.
- Leg cold-startgedrag vast: ontbrekende drukhistory blijft neutraal als
  `onvoldoende_data`, een lege Open-Meteo-cache blijft `initializing` en er
  worden geen drukval, confidence of regenobservaties verzonnen.
- Behoud het bestaande globale Netatmo-entitycontract: dezelfde unique ID,
  altijd beschikbaar, `unknown` in plaats van een misleidende nulwaarde zolang
  geen betrouwbare 60-minutentrend bestaat, en uitsluitend gekoppeld aan home.

# 0.4.80

- Audit de volledige providerketen van geografische activatie tot kaartoverlay,
  inclusief lokale radar, continentale fallback, satelliet, bliksem en
  grondvalidatie.
- Activeer gedeelde KMI- en KNMI-radar op basis van alle RegionEngines in
  plaats van uitsluitend de thuislocatie.
- Beschouw een geldig droog KNMI-frame als gezond en bepaal de gezondheid van
  nationale radar uit de werkelijke producttijd in plaats van de polltijd.
- Behandel MeteoLux en ItaliaMeteo uitsluitend als validatie/nowcast; zij
  kunnen OPERA of een echte lokale realtime-radar niet meer blokkeren.
- Isoleer Open-Meteo-provider, modelgrid, resultaat en routing per
  RegionEngine, zodat verre targets geen gegevens met thuis delen.
- Vervang gelijktijdige losse polls door één vergrendelde providercyclus:
  lokale bronnen, radarvergelijking, bronselectie en daarna grondvalidatie.
- Voeg end-to-end contracttests toe voor providerfrequentie, regionale
  isolatie, hiërarchie, routing, overlays en bliksemscheiding.

# 0.4.79

- Isoleer Netatmo-stations, luchtdrukhistoriek en druktrends per RegionEngine;
  een verre testtracker kan daardoor nooit meer zijn Miami-data aan Belgische
  targets of de thuissensor doorgeven.
- Poll alle actieve regio's gelijktijdig met gedeeld OAuth-tokenbeheer en start
  na een verre targetverplaatsing meteen een regionale Netatmo-meting.
- Houd de bestaande globale Netatmo-sensor expliciet gekoppeld aan `zone.home`
  en publiceer diagnostiek over de afzonderlijke regionale trends.
- Migreer de bestaande globale drukhistoriek veilig naar de thuisregio en
  bewaar voortaan alle RegionEngine-histories in dezelfde HA-opslag.

# 0.4.71

- Voeg H SAF H40B toe als gedeelde Meteosat-neerslagfallback voor Europa,
  Afrika, het Middellandse Zeegebied en aangrenzende Atlantische regio's.
- Download maximaal één centraal full-diskframe per cyclus en decodeer alleen
  de geografische vensters rond RegionEngines die satellietfallback nodig
  hebben; lokale radar en OPERA laten de provider slapen.
- Verwerk de echte H40B NetCDF-projectie, instantane `rr`-neerslagintensiteit,
  `qind`-pixelkwaliteit, bronpixels, componentfootprints en producttijd.
- Laat H40B een lege RainViewer-regio overnemen wanneer satellietneerslag wel
  aanwezig is en gebruik H40B daarnaast als onafhankelijke OPERA-bevestiging.
- Neem iedere zes uur een beperkte kalibratieprobe en vergelijk alleen frames
  met exact dezelfde nominale minuut; lokale officiële radar blijft referentie.
- Voeg H SAF FTP-credentials toe aan de configuratie-UI en verbeter de aparte
  toegangstest met host-, pad-, TLS- en FTP-diagnose.

# 0.4.70

- Publiceer de gegeorefereerde lokale OPERA-DBZH-pixels via hetzelfde
  intensiteitsruncontract als alle nationale rasterproviders.
- Beperk de OPERA-kaartlaag tot cellen die door de bestaande kwaliteits- en
  corroboratiefilter zijn aanvaard; afgekeurde clutter wordt niet getekend.
- Voeg KNMI API- en WMS-sleutels toe aan de UI-configuratie en geef ze vanuit
  een config entry daadwerkelijk aan de KNMI-provider door.
- Voeg een Météo-France APPLICATION_ID toe en vernieuw het kortlevende OAuth2-
  toegangstoken automatisch; een tijdelijk handmatig token blijft fallback.

# 0.4.69

- Publiceer echte intensiteitsruns voor alle beschikbare bronrasters: KMI,
  KNMI, DWD, AEMET, DPC, RainViewer, Météo-France en Met Office.
- Koppel iedere kaartoverlay aan de actieve radarprovider van de geselecteerde
  RegionEngine; providers zonder bronpixels blijven eerlijke contourdata.
- Breid de kaartlegenda uit tot zes neerslagklassen.
- Cluster zichtbare blikseminslagen om het radarbeeld leesbaar te houden.
- Laat bij verborgen inslagen de getroffen neerslagpixels knipperen wanneer
  daar in de voorbije vijf minuten actieve bliksem werd gemeten.

# 0.4.68

- Publiceer voor DPC Italië de werkelijk natte bronpixels als korte,
  gegeorefereerde rasterruns per intensiteitsniveau; droge gaten blijven leeg.
- Teken de operationele radarlaag met een KMI/Buienradar-achtige kleurenschaal
  en verberg uniforme componentvullingen standaard wanneer rasterdata bestaat.
- Voeg een kaartknop `techniek` toe waarmee interne WeatherSystem- en
  radarcelcontouren afzonderlijk kunnen worden gecontroleerd.
- Ondersteun `MultiPolygon` volledig in de custom kaart en houd de rasterlaag
  gekoppeld aan de geselecteerde RegionEngine en diens actieve provider.

# 0.4.67

- Behoud maximaal 2048 geordende punten van gesloten bronpixelringen in de
  kaartfeed in plaats van ze willekeurig tot 48 punten en lange koorden terug
  te brengen.
- Publiceer WeatherSystems met meerdere actuele broncontouren als GeoJSON
  `MultiPolygon`, zodat losse regenvelden niet meer door een kunstmatige
  convexe oranje driehoek worden verbonden.

# 0.4.66

- Vervang regelmatige rasterpuntbemonstering door echte 4-connected
  neerslagcomponenten met gesloten buitenranden op bronpixelniveau.
- Pas hetzelfde rastercontract toe op KMI, KNMI, DPC Italië, DWD Duitsland,
  AEMET Spanje, RainViewer en nationale ODIM-producten zoals Météo-France en
  Met Office; OPERA behield zijn bestaande exacte pixelgrenzen.
- Geef iedere component deterministische cel- en parent-ID's per radarframe,
  zodat opeenvolgende frames betrouwbaar kunnen worden geteld en gematcht.
- Match WeatherSystems tegen hun werkelijke footprints en bouw hun geometrie
  uit recente broncontouren in plaats van kettingen van regelmatige punten.
- Behoud gesloten bronringen in GeoJSON zonder ze opnieuw tot een kunstmatige
  convexe driehoek of veelhoek te reduceren.

# 0.4.65

- Kies radarbronnen per RegionEngine in vaste volgorde: operationele lokale
  bron, gevalideerde OPERA-observaties en daarna RainViewer.
- Laat afgekeurde ruwe OPERA-cellen de RainViewer-fallback niet langer
  blokkeren; bronkeuze en diagnose gebruiken nu aanvaarde cellen per engine.
- Gebruik dezelfde bronbeslissing voor routering, kaart en targetstatussen.
- Meld `alleen_bliksem` wanneer actuele bliksem aanwezig is zonder een
  bevestigde neerslagcel, in plaats van dit als `droog` weer te geven.
- Normaliseer gelokaliseerde landnamen en landcodes voor deterministische
  selectie van nationale providers.

# 0.4.64

- Bouw OPERA-celpolygonen uit de werkelijke buitenranden van de onderliggende
  1 km-rasterpixels in plaats van grof bemonsterde footprint-punten.
- Valideer de polygonoppervlakte tegen het aantal bronpixels en val bij
  ambigue of meervoudige topologie veilig terug op een punt.
- Vernieuw vorm en geografische positie bij ieder radarframe, zodat groei,
  krimp en verplaatsing op de kaart de waarnemingen volgen.

# 0.4.63

- Schakel een RegionEngine gericht naar RainViewer wanneer de lokale OPERA-
  uitsnede nul echo's bevat maar RainViewer binnen dezelfde radius actuele
  neerslag waarneemt.
- Bewaar aantallen radarobservaties per engine en gebruik die als operationeel
  dekkingsbewijs, zonder werkelijk droge OPERA-regio's onnodig te vervangen.

# 0.4.62

- Maak OPERA- en RainViewer-verwerking werkelijk geografisch onafhankelijk per
  actieve RegionEngine, zodat verre Life360- en testtargets tegelijk radardata
  krijgen.
- Deel de grote OPERA-downloadcache tussen de regionale uitsneden en ruim
  providers van verdwenen engines automatisch op.
- Bepaal providergezondheid, bronkeuze, diagnose en `radar_covered` per engine
  in plaats van vanuit de Belgische thuisprovider.

# 0.4.61

- Pas wijzigingen van de bliksembronmodus live toe via de Options Flow, zonder
  Home Assistant opnieuw te starten.
- Stop de Blitzortung MQTT-provider daadwerkelijk in `satellite_test` en start
  EUMETSAT LI/GOES GLM onmiddellijk voor diagnostische verificatie.
- Start Blitzortung en zet satellietproviders terug in stand-by wanneer opnieuw
  naar `auto` wordt geschakeld.

# 0.4.60

- Publiceer per WeatherSystem gestructureerde MCS-diagnostiek met afzonderlijke
  vorm-, intensiteits-, duur- en continuïteitscriteria.
- Log en publiceer iedere MCS-statusovergang via
  `storm_tracker_v3_mcs_transition`, inclusief de reden voor bevestiging of
  afwijzing en de gemeten waarden.
- Evalueer MCS-systemen over alle dynamische RegionEngines in plaats van alleen
  de actieve legacy-engine.
- Bewaar maximaal vijftien minuten aan recente Blitzortung-, EUMETSAT LI- en
  GOES GLM-observaties in een compacte, afzonderlijke GeoJSON-laag.
- Toon bliksem als schakelbare sterrenlaag met ouderdomskleuren en brontooltip,
  visueel gescheiden van neerslagcellen en WeatherSystem-polygonen.

# 0.4.59

- Selecteer de operationele radarbron afzonderlijk per dynamische RegionEngine.
- Gebruik voor Italië de actuele, officiële DPC Protezione Civile SRI-radar van 1 km en 5 minuten.
- Gebruik voor Spanje de publieke officiële AEMET-composietbundel met actuele EPSG:4326-GeoTIFFs, zonder API-key.
- Val per regio terug op OPERA en daarna RainViewer wanneer de lokale radar ontbreekt, ongezond of verouderd is.
- Publiceer bron, reden en leeftijd per engine in sensoren, GeoJSON en de multi-targetkaart.
- Reserveer satelliet-neerslag als latere gap-fallback; EUMETSAT LI wordt nadrukkelijk niet als neerslagradar misbruikt.

# 0.4.58

- Herken niet-JSON-antwoorden van Open-Meteo ondanks een HTTP-successtatus.
- Probeer ItaliaMeteo ICON-2I automatisch opnieuw met alleen neerslag wanneer LPI niet geleverd kan worden.
- Laat een forecaststoring de officiële Italiaanse radarcatalogus en lifecycle niet langer volledig blokkeren.

# 0.4.57

- Voeg GeoSphere Austria INCA toe als slapende officiële 1 km-nowcastvalidatie.
- Voeg ItaliaMeteo-ARPAE ICON-2I via Open-Meteo toe voor lokale neerslag- en bliksemverwachting, naast de gratis Radar SRI DPC-catalogus.
- Italiaanse dagbundels worden niet als realtime radar gebruikt wanneer ze achterlopen; OPERA/RainViewer blijft dan operationeel.

# 0.4.56

- Voeg slapende officiële radarproviders toe voor Groot-Brittannië (Met Office) en Frankrijk (Météo-France).
- Voeg MeteoLux toe als lokale, publieke nowcast-validatiebron voor Luxemburg.
- Gebruik nationale radars voorlopig veilig als OPERA-corroboratie en toon hun lifecycle-diagnostiek.

# 0.4.55

- Selecteert bij meerdere buien per target de operationeel belangrijkste
  dreiging in plaats van uitsluitend de dichtstbijzijnde bevestigde echo.
- Actuele neerslag blijft altijd prioritair; daarna volgen voorspelde rake en
  randpassages, naderende systemen, zijwaartse passages en wegtrekkende buien.
- Publiceert `selected_reason` zodat zichtbaar en testbaar is waarom een systeem
  voor een persoon werd gekozen.
- Droge statussen publiceren nu ook expliciete lege prognosevelden, zodat het
  dashboard en automatiseringen een stabiel attribuutcontract houden.

# 0.4.54

- Nieuwe conservatieve passageprognose per target tot 90 minuten vooruit:
  verwacht tijdstip, kleinste contourafstand, verwachte intensiteit en een
  numerieke zekerheidsscore.
- Intensiteit wordt uit de recente dBZ-trend geëxtrapoleerd en begrensd tot
  maximaal 10 dBZ verandering om uitschieters te vermijden.
- Prognoses verschijnen alleen bij bevestigde Matige/Hoge bewegingsvectoren;
  onbetrouwbare of te verre projecties blijven expliciet onbeschikbaar.

# 0.4.53

- Persoons- en targetstatussen publiceren nu de actuele plaats, het adres en
  de ISO-landcode voor weergave op de dashboardtegels.
- Plaats en land worden privacyvriendelijk lokaal bepaald via
  `/homeassistant/www/places.json`; targetcoördinaten verlaten Home Assistant niet.
- Een Life360-plaatsnaam (zoals `Thuis`) krijgt voorrang als label, terwijl de
  lokale plaatsendatabase de landcode aanvult.

# 0.4.52

- Bevestigde systemen krijgen bij een Matige of Hoge bewegingsvector een
  expliciete hoofdstatus: `naderend`, `wegtrekkend` of `langs_trekkend`.
- `bevestigd` betekent voortaan dat de bui echt is, maar de bewegingsvector
  nog onvoldoende betrouwbaar is voor een richtingstatus.
- ETA en waarschuwingen blijven uitsluitend gekoppeld aan `naderend`.

# 0.4.51

- Eerste veilige fase van radar-autokalibratie: ruwe OPERA-cellen worden op
  een gemeenschappelijk geografisch rooster vergeleken met het actuele
  visuele KMI-radarbeeld.
- OPERA- en KMI-frames worden maximaal drie uur apart in historiek gehouden;
  vergelijking gebeurt uitsluitend bij exact dezelfde nominale radarminuut,
  ongeacht welke bron als eerste binnenkomt.
- Nieuwe `sensor.stv3_radar_autokalibratie` toont overlap, precisie, recall,
  F1-score, vermoedelijke valse OPERA-cellen en gemiste KMI-neerslag.
- De kalibratie is strikt observerend en wijzigt geen operationele filtering.
- Een ongewijzigd KMI-frame wist niet langer de laatst geldige observaties;
  ook een vers droog frame blijft bruikbaar als negatieve referentie.

# 0.4.50

- De actuele radarcontour wordt langs de betrouwbare bewegingsvector
  geprojecteerd en classificeert de passage per target als raak, rand of mist.
- Publiceert afstand tot de voorspelde contour en een conservatieve
  onzekerheidsmarge voor waarschuwingen.

# 0.4.49

- Per target worden de minimale afstand tot de voorspelde centrumlijn en het
  tijdstip van dichtste passage berekend bij een Matige of Hoge bewegingsvector.

# 0.4.48

- EUMETSAT LI probeert bij een tijdelijke 404 automatisch oudere recente
  catalogusproducten in plaats van de volledige fallbackcyclus te verliezen.
- NOAA GOES-18/19 blijft in slaapstand zolang geen actieve RegionEngine binnen
  de respectieve satellietdekking ligt.

# 0.4.47

- Machineleesbare landen-/datatypebronmatrix toegevoegd met afzonderlijke
  keuzes voor radar, bliksem en grondvalidatie.
- Aggregator-first beleid vastgelegd: lokale providers worden alleen gebouwd
  wanneer zij aantoonbare meerwaarde boven de continentale bron leveren.
- Wereldwijde catalogus uitgebreid met verzamelproviders en verificatiewachtrij.

# 0.4.46

- DWD RADOLAN/RADVOR RV toegevoegd als eerste lifecycle-provider: publieke
  1 km HDF5-radar, vijfminutenactualiteit en automatische slaapstand.
- DWD-data wordt als nationale vergelijkingsbron gebruikt om OPERA-echo's te
  bevestigen en niet rechtstreeks als tweede operationele stormbron ingevoerd.
- Nieuwe lifecycle-sensor toont status, relevante engines, polltijd, aantallen
  en fouten per locatiegebonden provider.

# 0.4.45

- Generieke lifecyclecontroller toegevoegd voor locatiegebonden providers met
  gedeelde activatie, slaapstand, cooldown, poll-lock en diagnostiek.
- Meerdere RegionEngines die dezelfde bron nodig hebben delen één provider en
  één fetch; terugkerende engines tijdens cooldown veroorzaken geen herstart.

# 0.4.44

- Satellietpolls publiceren nu altijd diagnostiek naar de bliksemsensor, ook
  wanneer geen enkele flash binnen een actieve RegionEngine valt.
- De sensor toont per satellietprovider laatste poll, opgehaalde en aanvaarde
  flashes en een eventuele fout; testmodus pollt meteen na het opstarten.

# 0.4.43

- Configuratieoptie toegevoegd om satellietbliksem tijdelijk te forceren,
  zodat EUMETSAT en NOAA GOES end-to-end getest kunnen worden terwijl
  Blitzortung verbonden blijft.
- In satelliettestmodus worden binnenkomende Blitzortung-observaties genegeerd;
  de normale automatische modus blijft standaard en ongewijzigd.

# 0.4.42

- `STV3 Fictieve tracker locatie` en `STV3 Region Engines` verversen nu ook
  bij verplaatsing van een secundair target naar een nieuwe RegionEngine.
- Verhelpt stale Luxemburg-coördinaten in de UI nadat de testtracker naar
  Miami werd verplaatst; de onderliggende engine-routing was al correct.

# 0.4.41

- NOAA GOES-18 en GOES-19 GLM toegevoegd als gratis, sleutelvrije
  bliksemfallback voor Amerika en de Stille Oceaan.
- De provider verwerkt anonieme NOAA S3-NetCDF-bestanden van twintig seconden,
  dedupliceert bestanden en haalt bij activering maximaal vier minuten in.
- EUMETSAT, GOES-19 en GOES-18 hebben vaste overlappingsgrenzen zodat dezelfde
  optische flash niet door meerdere satellieten dubbel wordt verwerkt.
- Azië tussen 80°O en 145°O blijft bewust vrij voor een latere FY-4-provider.

# 0.4.40

- EUMETSAT MTG Lightning Imager toegevoegd als gratis fallback wanneer de
  Blitzortung MQTT-broker niet verbonden is.
- Tijdelijke EUMETSAT-tokens worden automatisch aangemaakt en vernieuwd;
  consumer key en secret zijn via de configuratie-opties instelbaar.
- Alleen de kleine NetCDF BODY-entry wordt gedownload, met een harde limiet van
  10 MiB, productdeduplicatie en weigering van data ouder dan 30 minuten.
- Zodra Blitzortung opnieuw verbindt, stopt de satellietfallback automatisch.

# 0.4.39

- De multi-targetkaart toont per geselecteerd target alleen diens gekoppelde RegionEngine, radarcellen, systemen, vectoren en targets; overlappende engines worden niet langer door elkaar getekend.
- Bewegingsvectoren verschijnen alleen bij een bevestigd systeem met minstens vier meetpunten, tien minuten historie, een fit van 0,60 en minimaal matige betrouwbaarheid.

# 0.4.38

- De OPERA-diagnostiek telt bruikbare KMI-, KNMI- en RainViewer-referenties nu uit de werkelijke verificatieset; KMI staat niet langer foutief hard op nul.
- De legacy fictieve-trackerlocatiesensor toont de actuele geconfigureerde testtracker, inclusief beschikbaarheid en toegewezen RegionEngine, in plaats van de HA-thuislocatie.

# 0.4.36

- De GeoJSON-kaart publiceert alleen radarcellen van de actieve operationele
  radarbron. Oude RainViewer-fallbackcellen blijven intern kort beschikbaar
  voor lifecyclebeheer, maar verschijnen niet meer op de kaart zolang OPERA de
  actieve bron is.
- RainViewer gebruikt fijnere pixelsampling en een realistischere
  pixeloppervlakte, zodat fallbackcellen minder grof wegen in clustering en
  visualisatie.
- Lage OPERA-quality wordt alleen nog door compacte RainViewer-echo's vanaf
  intensiteit 2 binnen 12 km bevestigd; lichte of ruimtelijk losse pixels mogen
  geen volledige OPERA-cel meer valideren.

## v0.4.28

- Eerste backwards-compatible multi-targetlaag: naast de bestaande fictieve
  tracker kunnen meerdere personen of vaste locaties worden geregistreerd.
- Elk target krijgt een stabiele eigen neerslagstatussensor en wordt gekoppeld
  aan een gedeelde of aparte RegionEngine op basis van de sharing distance.
- Operationele observaties worden via de StormManager naar alle relevante
  regio-engines gerouteerd.

## v0.4.27

- Bewegingshistoriek van radarsystemen wordt nu samen met de MCS-snapshot
  bewaard en na een Home Assistant-herstart onmiddellijk hersteld.
- Oude snapshots zonder bewegingshistoriek worden automatisch uit hun
  radarcellen en parent-footprints gereconstrueerd.
- Meerdere updates van hetzelfde radarproduct vervangen hetzelfde centroidpunt
  in plaats van kunstmatige nul-tijdsintervallen aan de regressie toe te voegen.

## v0.4.26

- Nieuwe `sensor.stv3_neerslagstatus` vat de operationele toestand samen als
  `droog`, `waargenomen`, `bevestigd` of `naderend`.
- De sensor verkiest bevestigde systemen boven eenmalige echo's en publiceert
  afstand, impactpunt, dBZ, radardekking, beweging, ETA en luchtdruktrend in
  één stabiel datacontract voor dashboard en automatiseringen.

## v0.4.25

- Sluimerende systemen blijven intern kort beschikbaar voor lifecyclebeheer,
  maar worden niet langer als actieve storms naar Home Assistant gepubliceerd.
- Systemen tonen nu `tracking_status`, het aantal opeenvolgende radarframes en
  het tijdstip van het laatste radarframe.
- Eén radarframe krijgt status `waargenomen`; vanaf twee aansluitende frames is
  het systeem `bevestigd`. Daardoor kan het dashboard tijdelijke echo's herkenbaar
  scheiden van aanhoudende neerslag.
- Naderingssnelheid en ETA van radarsystemen worden onderdrukt tot het systeem
  door minstens twee opeenvolgende frames bevestigd is.

## v0.4.24

- OPERA `qi_total` is niet langer de enige zelfstandige toelatingsroute:
  meteorologisch plausibele cellen van minstens 50 km², gemiddeld minstens
  20 dBZ en met een piek van minstens 30 dBZ worden als `structured_echo`
  aanvaard, ook bij een lage quality-score.
- Zwakke brede echo's blijven kruisbronbevestiging vereisen; de foutcel nabij
  België van circa 12-14 dBZ wordt dus niet opnieuw toegelaten.
- Diagnostiek onderscheidt quality-, structuur- en kruisbronacceptatie.

## v0.4.23

- OPERA-cellen kunnen niet langer door de groene KMI-basiskaart worden
  bevestigd; het KMI-beeld wordt niet als onafhankelijke pixelreferentie
  gebruikt zolang kaartkleuren en regenkleuren niet betrouwbaar gescheiden zijn.
- De opaak-witte KNMI WMS-achtergrond (intensiteit 1) telt niet langer als
  neerslagbevestiging; alleen echte KNMI-radarkleuren vanaf intensiteit 2 en
  RainViewer-neerslag mogen lage-kwaliteit OPERA-echo's bevestigen.
- OPERA-diagnostiek toont voortaan zowel het ruwe als bruikbare aantal
  corroboratiereferenties.

## v0.4.22

- Netatmo-luchtdruk wordt per station over twee uur gevolgd; regionale
  drukverandering gebruikt de mediaan van gepaarde stations en is daardoor
  niet gevoelig voor onderlinge hoogteverschillen.
- Nieuwe sensor `sensor.stv3_netatmo_luchtdruktrend` toont de verandering over
  60 minuten, trends over 15/30/60 minuten, stationsdekking en snelle drukval.
- Onrealistische drukwaarden en sprongen worden geweigerd; een trend verschijnt
  pas zodra minstens drie stations een bruikbare historische vergelijking geven.
- De twee uur drukhistoriek wordt in Home Assistant-opslag bewaard, zodat een
  herstart de opgebouwde trend niet wist.

## v0.4.21

- De dichtstbijzijnde-storm-sensor blijft beschikbaar wanneer een nieuw
  stormsysteem nog geen snelheidsvector heeft; lege naderingssnelheid wordt
  nu veilig als `null` gepubliceerd.

## v0.4.20

- Bewegingskwaliteit gebruikt nu zowel het aantal meetpunten als de tijdspanne
  en regressie-fit, zodat een korte of grillige reeks minder vertrouwen krijgt.
- De dichtstbijzijnde-storm-sensor toont koers, windroosrichting,
  naderingssnelheid en of het systeem werkelijk naar de tracker beweegt.
- Een ETA wordt alleen berekend uit de snelheidscomponent richting de tracker;
  zijwaarts bewegende en wegtrekkende systemen krijgen geen misleidende ETA.
- Bewegingspunten, historie en fitkwaliteit zijn als diagnostische attributen
  beschikbaar op de stormsensoren.

## v0.4.19

- Blitzortung abonneert niet langer op de volledige wereldfeed, maar gebruikt
  een gededupliceerde unie van geohash-topics rond alle actieve RegionEngines.
- Regiowissels verversen subscriptions op dezelfde gedeelde MQTT-client.
- MQTT-disconnects worden direct gedetecteerd en krijgen exponentiële backoff
  van één tot vijftien minuten, met jitter om proxy-rate-limits te respecteren.
- Foutmeldingen tonen voortaan de foutklasse en een bruikbare reden in plaats
  van een lege `()`-melding iedere 25 seconden.

## v0.4.18

- De bestaande Open-Meteo Gear-sensor wordt nu daadwerkelijk bij Home
  Assistant geregistreerd, zodat `sensor.stv3_open_meteo_gear` niet langer
  als een oude herstelde, maar niet-beschikbare entiteit blijft staan.

## v0.4.17

- RainViewer gebruikt voortaan de werkelijke frame-timestamp uit het manifest
  in plaats van het lokale verwerkingstijdstip.
- Radarframes ouder dan twintig minuten worden als ongezond geweigerd en
  kunnen niet langer als operationele fallback of OPERA-bevestiging dienen.
- De RainViewer-sensor toont poll-, succes- en frametijd, frameleeftijd,
  providergezondheid, foutreden, foutenteller en het gebruikte framepad.
- Manifestproblemen en herstel worden zichtbaar gelogd zonder elke vijf
  minuten dezelfde waarschuwing te herhalen.

## v0.4.16

- Open-Meteo-grid verlaagd van 948 naar 324 modelpunten; OPERA blijft de
  fijnmazige primaire radarbron.
- Succesvolle Open-Meteo-resultaten worden 30 minuten gecachet en niet opnieuw
  als verse OFE-observaties verwerkt.
- HTTP 429 activeert exponentiÃ«le backoff van 30 minuten tot maximaal zes uur
  en respecteert een numerieke `Retry-After`-header.
- `wet_locations_now` wordt nu werkelijk geleverd voor grondverificatie.

## v0.4.15

- De `StormManager` bezit nu de werkelijk gebruikte StormEngine/OFE-runtime;
  `region_manager.py` is niet langer ongebruikte architectuurcode.
- Sharing distance en observatieradius zijn onafhankelijk configureerbaar.
- Een target buiten de sharing distance krijgt een nieuwe dynamische
  RegionEngine; een lege engine wordt automatisch verwijderd.
- De globale Blitzortung-stream wordt naar alle relevante actieve engines
  gerouteerd zonder een extra listener per regio.
- Compacte MCS-framehistoriek wordt met Home Assistant `Store` per geografische
  runtime-regio bewaard en na een restart hersteld.
- Volledige OPERA-rasters worden nooit persistent opgeslagen.
- Nieuwe diagnostische entiteit `sensor.stv3_region_engines` toont actieve
  engines, centra, observatieradius, gekoppelde targets en WeatherSystems.

## v0.4.14

- Radar-gebaseerde MCS-classificatie toegevoegd met afzonderlijke statussen
  `candidate` en `confirmed`.
- Een kandidaat vereist minstens twee kernen van 40 dBZ, minstens één kern van
  50 dBZ en een convectieve span van minstens 100 km.
- Bevestiging gebeurt pas wanneer de criteria drie uur aaneengesloten gelden;
  korte onderbrekingen tot twintig minuten zijn toegestaan.
- Zes uur parent-framehistoriek wordt compact per WeatherSystem bijgehouden.
- Nieuwe entiteit `sensor.stv3_mcs_detectie` toont kandidaten, bevestigde MCS'en
  en de gebruikte metriek/criteria.

## v0.4.13

- Eén brede OPERA-component wordt als parent-WeatherSystem behouden, met de
  adaptief gesegmenteerde zware kernen als afzonderlijke RadarCells.
- Parent- en cell-ID's, oppervlaktes en compacte footprints blijven door de
  Observation Pipeline tot in de WeatherSystem Engine beschikbaar.
- Afstand tot de dichtstbijzijnde storm gebruikt voortaan de dichtste lokale
  RadarCell-footprint in plaats van uitsluitend de systeemcentroid.
- Diagnostiek toont per OPERA-cel ook de parent-component en parentoppervlakte.

## v0.4.12

- Buitensporig grote OPERA-componenten worden adaptief opgesplitst rond
  afzonderlijke zware regenkernen.
- Lichte regen mag nog vijf rasterpixels rond een kern meegroeien, maar kan
  verschillende kernen niet opnieuw aan elkaar verbinden.
- Kleine en gelijkmatig lichte regengebieden behouden de bestaande 8 dBZ-
  gevoeligheid.

## v0.4.11

- OPERA-observaties bewaren voortaan een compacte footprint van werkelijk
  bezette rasterblokken.
- RainViewer en nationale radars kunnen een lage-quality OPERA-cel nu over de
  volledige celvorm bevestigen, ook wanneer het centroid ver weg ligt.
- De footprint gebruikt echte celpixels en geen brede bounding box, zodat lege
  ruimte naast een langgerekte bui geen valse bevestiging veroorzaakt.

## v0.4.10

- RainViewer draait parallel als verificatiebron wanneer OPERA gezond is.
- Lage OPERA-quality kan nu door recente RainViewer-neerslag bevestigd worden.
- Bij OPERA-uitval wordt dezelfde RainViewer-fetch als fallback hergebruikt.
- Een ongewijzigd RainViewer-frame hergebruikt zijn observaties in plaats van
  tijdelijk nul observaties te publiceren.

## v0.4.9

- Alle providerobservaties worden begrensd tot het actuele monitoringsgebied.
- Wereldwijde Blitzortung-inslagen buiten de ingestelde radarradius worden genegeerd.
- Bij een trackerverplaatsing verdwijnen WeatherSystems buiten de nieuwe regio.
- Wachtende OFE-observaties uit de vorige regio worden bij de wissel gewist.
- De sensor `STV3 Actieve Radarbron` publiceert opnieuw zijn runtimewaarde.

## Overzicht per module

| Module | Huidig | Omschrijving |
|---|---|---|
| `__init__.py` | v0.2.0 | Hoofdsetup, providers, polling, fictieve tracker |
| `sensor.py` | v0.1.0 | HA sensoren voor alle providers |
| `const.py` | v0.1.0 | Constanten (DOMAIN) |
| `providers/base.py` | v0.1.0 | Plugincontract (Capability, CoverageResult, ProviderPlugin, ProviderRegistry) |
| `providers/opera.py` | v0.1.0 | OPERA/MeteoGate S3 radar provider (heel Europa, 1km², HDF5) |
| `engine/observation.py` | v0.1.0 | Observation dataclass |
| `engine/observation_fusion_engine.py` | v0.1.0 | OFE: deduplicatie, batching |
| `engine/storm.py` | v0.1.0 | Storm dataclass |
| `engine/storm_engine.py` | v0.1.0 | Clustering, merge, lifecycle, regressie |
| `engine/region_manager.py` | v0.5.0 | Dynamische RegionEngines per ProjectionTarget |
| `geometry/bounding_box.py` | v0.1.0 | Bounding box berekeningen |
| `geometry/distance.py` | v0.1.0 | Haversine afstandsberekening |
| `geometry/geocode.py` | v0.1.0 | Plaatsnaam lookup |
| `geometry/hull.py` | v0.1.0 | Convex hull berekening |
| `plogger/provider_logger.py` | v0.2.0 | CSV logging per provider |
| `providers/blitzortung.py` | v0.6.0 | Blitzortung MQTT provider |
| `providers/kmi.py` | v0.3.0 | KMI radar provider |
| `providers/knmi.py` | v0.2.0 | KNMI WMS radar + nowcast provider |
| `providers/netatmo.py` | v0.2.0 | Netatmo grondstations provider |
| `providers/open_meteo.py` | v0.3.0 | Open-Meteo grid provider |
| `providers/rainviewer.py` | v0.1.0 | RainViewer tile provider |

---

## Changelog per module

### `__init__.py`
- **v0.2.0** — Providers volgen fictieve tracker locatie; herinitialisatie bij verplaatsing >1km; CoreState/EVENT_HOMEASSISTANT_STARTED pattern; cross-trigger verwijderd
- **v0.1.0** — Eerste versie; providers hardcoded op home_lat/lon

### `sensor.py`
- **v0.1.0** — Eerste versie; sensoren voor alle providers; async_added_to_hass fix voor unavailable state

### `providers/blitzortung.py`
- **v0.6.0** — paho-mqtt MQTTv311 exact zoals bestaande HA-integratie; geen filter op afstand
- **v0.5.0** — aiomqtt (timeout problemen)
- **v0.4.0** — directe WebSocket (SSL + beleidsproblemen)
- **v0.3.0** — factory-patroon + supports()
- **v0.2.0** — Observation model
- **v0.1.0** — state listener op HA-integratie (afhankelijk van externe integratie)

### `providers/kmi.py`
- **v0.3.0** — volledige radarplaatjes downloaden via URI uit animatiesequentie; dynamische afmetingen; ww weercode uitlezen
- **v0.2.0** — correcte API: getForecasts + lat/lon + User-Agent: be.meteo.app
- **v0.1.0** — eerste versie (getIncaList zonder lat/lon → lege response)

### `providers/knmi.py`
- **v0.2.0** — correcte dataset/layer namen (radar_forecast_2.0/precipitation_nowcast); aparte WMS API key; alle 25 nowcast tijdstappen
- **v0.1.0** — eerste versie (verkeerde dataset/layer namen → 404)

### `providers/netatmo.py`
- **v0.2.0** — volledige parsing: rain_live, rain_5min, wind_strength, wind_angle, gust_strength, pressure, temperature, humidity per station
- **v0.1.0** — eerste versie (alleen rain_live)

### `providers/open_meteo.py`
- **v0.3.0** — lat/lon van natte punten bijgehouden in wet_locations_now/forecast; timezone als array fix
- **v0.2.0** — timezone als array (vereist door Open-Meteo POST API); minutely_15 nowcast 90min
- **v0.1.0** — eerste versie (timezone als string → 400 error)

### `providers/rainviewer.py`
- **v0.1.0** — eerste versie; 3×3 tile grid; pixel → lat/lon conversie

### `plogger/provider_logger.py`
- **v0.2.0** — non-blocking via async_add_executor_job; locaties in open_meteo log
- **v0.1.0** — eerste versie (blocking I/O in event loop)

### `engine/region_manager.py`
- **v0.5.0** — volledig dynamische RegionEngines zonder vaste landsgrenzen; providers bepalen zelf dekking via supports()
- **v0.4.0** — ProjectionTarget-terminologie
- **v0.3.0** — één canonieke blitz_entity per regio (verouderd)
- **v0.2.0** — geografische regio's op basis van land (verouderd)
- **v0.1.0** — eerste opzet

### `engine/observation_fusion_engine.py`
- **v0.1.0** — eerste versie; deduplicatie per type/locatie; 1s batching; 1u buffer

### `engine/storm_engine.py`
- **v0.1.0** — eerste versie; clustering, merge, lifecycle, regressie, geocoding

### `engine/storm.py`
- **v0.1.0** — eerste versie; Storm dataclass met centroid, beweging, geometrie

### `geometry/*`
- **v0.1.0** — eerste versie van alle geometrie helpers

---

## Bekende issues / TODO

- [ ] OFE nog niet verbonden met providers in `__init__.py`
- [ ] StormEngine nog niet geactiveerd
- [ ] RegionEngine/StormManager nog niet geïntegreerd
- [ ] ProjectionEngine (ETA, passage) nog niet gebouwd
- [ ] Multi-persoon tracking nog niet geïmplementeerd
- [ ] Open-Meteo polling interval: 120 min (tijdelijk na rate limit issues)
- [ ] Versioned ZIP backups ontbreken nog

### `providers/base.py`
- **v0.1.0** — eerste versie; Capability, CoverageArea, CoverageResult, ProviderContext, ProviderPlugin Protocol, ProviderRegistry met strategie per capability

### `providers/opera.py`
- **v0.1.0** — eerste versie; OPERA DBZH composiet via MeteoGate S3 bucket; HDF5 parsing via h5py/numpy/pyproj; component labeling uit PoC; Observations per regencel
# v0.4.4

- OPERA discovery controleert recente, deterministische vijfminutenbestanden
  nu eerst rechtstreeks met HTTP HEAD.
- De brede S3-listing blijft beschikbaar als fallback.
- Extra regressietest voor discovery wanneer listing een actueel bestand mist.
- Testomgeving bevat nu alle providerafhankelijkheden en een portable
  `radar_policy`-fixture.
# v0.4.5

- OPERA `qi_total` wordt niet langer als harde quality-filter gebruikt; live
  CIRRUS-producten rapporteerden `0.0` voor echte neerslagpixels.
- Detectiedrempel verlaagd van 10 naar 8 dBZ.
- Minimum celoppervlakte verlaagd van 9 naar 5 pixels/km².
- Actieve detectiedrempels toegevoegd aan de OPERA-sensordiagnostiek.
# v0.4.6

- Standaard OPERA/radarradius verhoogd van 200 naar 300 km om cellen boven
  Drenthe/Overijssel vanuit de thuisregio tijdig te volgen.
- Werkelijke radius en WGS84-bbox worden voortaan getoond als attributen van
  `sensor.stv3_opera_observaties`.
# v0.4.7

- `sensor.stv3_opera_observaties` toont maximaal 40 ruwe OPERA-cellen met
  coördinaten, afstand, oppervlakte, dBZ, quality en pixelcount.
- Diagnostieklijst is begrensd om state-attributen compact te houden.
- Cellen in de verre hoeken van de geprojecteerde bbox worden nu met een echte
  grootcirkelafstand begrensd tot `radius_km`.
## v0.4.29

- Home Assistant `home` is voortaan altijd het primaire target; de locatie komt
  bij UI-installaties rechtstreeks uit de algemene HA-configuratie.
- Nieuwe configuratie- en optiesflow voor Life360 `device_tracker`-entiteiten.
- De fictieve tracker is niet langer verplicht of primair, maar een optioneel
  en duidelijk gemarkeerd testtarget.
- Bestaande YAML-configuraties blijven ondersteund tijdens de migratieperiode.
## v0.4.30

- Nieuwe compacte GeoJSON-kaartfeed voor targets, RegionEngines,
  weersystemen, systeemhulls, lokale RadarCells en bewegingsvectoren.
- Radarcellen zijn begrensd en hulls worden compact bemonsterd om te vermijden
  dat Home Assistant-state-attributen onbeheersbaar groot worden.
- De kaartfeed heeft een expliciete schemaversie en meldt wanneer cellen zijn
  afgekapt, zodat kaartclients betrouwbaar kunnen degraderen.
## v0.4.31

- De volledige GeoJSON staat niet langer in een sensorattribuut dat Recorder
  bij iedere radarupdate zou kunnen opslaan.
- Nieuwe geauthenticeerde feed op `/api/storm_tracker_v3/geojson` voor
  dashboardkaarten en andere kaartclients.
- `sensor.stv3_kaart_geojson` publiceert uitsluitend compacte metadata,
  featureaantallen en het endpoint.
## v0.4.32

- Orden OPERA RadarCell-puntwolken eerst als convex hull; hierdoor verdwijnen
  de lange blauwe zigzagdiagonalen op de GeoJSON-kaart.
- Maak de kaartmodule ASCII-veilig zodat keuzelijst, zoomknoppen en metadata
  niet langer met mojibake worden weergegeven.
- Cluster targets op vrijwel dezelfde locatie tot een leesbaar label in plaats
  van acht overlappende namen.
# 0.4.35

- Publiceert in de kaartlaag alleen radarcellen uit het nieuwste radarframe; historische detecties blijven intern beschikbaar voor beweging en opvolging.
- Knipt lage-kwaliteit OPERA-footprints terug tot de punten die ruimtelijk door RainViewer worden bevestigd, zodat een kleine echte bui geen volledige foutieve megacel meer aanvaardt.

# 0.4.34

- Herstelt het RainViewer v2-tegelpad door de verplichte afbeeldingsgrootte toe te voegen.
- Decodeert Universal Blue-radarpixels op transparantie en kleur in plaats van algemene helderheid; grijze antwoordpixels kunnen OPERA niet langer foutief bevestigen.

# 0.4.33

- Vereist onafhankelijke radarcorroboratie voor OPERA-cellen met lage kwaliteit, ook wanneer hun reflectiviteit en oppervlakte meteorologisch plausibel lijken.
- Verwijdert daardoor sterke maar onbevestigde OPERA-clutter, zoals de foutieve cellen boven Noord-Nederland en Noord-Frankrijk.
# 0.4.37

- KMI-radarpunten vanaf intensiteit 2 kunnen OPERA opnieuw onafhankelijk bevestigen; basiskaartartefacten met intensiteit 1 blijven uitgesloten.
- Stormvlakken en bewegingsvectoren zonder radarcel uit het actuele frame van de actieve bron verdwijnen uit de GeoJSON-kaart.
