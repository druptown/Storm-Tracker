# Dynamische regionale rasterfusie

**Datum:** 25 september 2026  
**Status:** goedgekeurd
**Basis:** `2026-09-20-thin-operational-pipeline-design.md`

## Doel

Storm Tracker vormt per actieve waarschuwingsregio één geografisch correct
neerslagveld uit alle actuele, `Ready` radarproviders die die regio werkelijk
dekken. Verschillende projecties, rasterafmetingen en pixelresoluties worden
niet als afbeeldingen uitgerekt, maar op één fysiek gegeorefereerd regionaal
analyseraster geprojecteerd. Celherkenning, tracking en
voetafdrukvoorspelling verwerken daarna het gefuseerde veld.

De providerselectie blijft volledig dynamisch. Er bestaat geen vaste lijst
van KMI, KNMI, DWD, OPERA of RainViewer: de locatie, providerdekking,
productgeschiktheid, actualiteit en kwaliteit bepalen per cyclus welke bronnen
meedoen.

Alle toepasselijke bronnen worden beoordeeld, maar per analyserastercel doen
maximaal de drie hoogst scorende, kwantitatieve radarbronnen mee aan de
numerieke fusie. Dit voorkomt dat sterk gecorreleerde composieten de confidence
kunstmatig verhogen of dat een groot aantal middelmatige bronnen één goede
lokale radar overstemt. Overige radars blijven beschikbaar als fallback en
voor vergelijking; bliksem, satelliet, grondstations en modellen behouden hun
eigen meteorologische rol en worden niet als extra regenradar gemiddeld.

## Niet-doelen

- Geen wereldwijd raster en geen permanente rastercache.
- Geen nieuwe RAW-, audit-, receipt-, authority- of manifestinfrastructuur.
- Geen automatische herkalibratie van brongewichten in deze fase.
- Geen samenvoeging van verschillende meteorologische grootheden alsof ze
  uitwisselbaar zijn.
- Geen verwijdering van de huidige per-providertracking voordat live
  acceptatie is geslaagd.

## Beschouwde aanpakken

### A. Regionaal canoniek raster — gekozen

Maak voor iedere actieve `ThinAcquisitionRegion` een lokaal metrisch raster en
projecteer iedere toepasbare `ThinRadarField` daarop. Dit bewaart fysieke
afstanden en oppervlakken, maakt per-pixelfusie mogelijk en levert één invoer
voor celtracking.

### B. Alleen polygonen samenvoegen — afgewezen

Overlappende providerpolygonen zouden zonder rasterprojectie gecombineerd
kunnen worden. Dat is eenvoudiger, maar verliest intensiteitsverdeling,
no-data, resolutie en de mogelijkheid om cellen opnieuw op consistente
drempels te detecteren.

### C. Native rasters behouden en alleen per punt bemonsteren — afgewezen

Dit volstaat voor regenstatus op één target, maar niet voor één gezamenlijke
buivorm, cross-provideridentiteit of fysiek consistente tracking.

## Invoercontract

Iedere radaradapter blijft een `ThinRadarField` leveren met:

- `provider_id` en `product_id`;
- `observed_at`;
- bron-CRS;
- oorsprong en x/y-resolutie in de eenheden van dat CRS;
- tweedimensionale regenintensiteit in `mm/h`;
- een afzonderlijk `valid_mask`.

De fusielaag ontvangt daarnaast het actuele `ThinRegionPlan` en de
providerresultaten. Alleen radarvelden waarvan de provider voor de regio
geografisch toepasbaar is en waarvan de actuele runtime-status bruikbaar is,
worden kandidaat.

Een provider zonder kwantitatief radarveld kan wel gewone
`NormalizedObservation`-resultaten blijven leveren, maar doet niet mee aan de
ruimtelijke rasterfusie.

## Canoniek regionaal analyseraster

Voor iedere actieve regio wordt deterministisch één `RegionalAnalysisGrid`
gemaakt:

- centrum: het stabiele centrum van `ThinAcquisitionRegion`;
- dekking: de volledige `observation_radius_km` rondom dat centrum;
- projectie: lokaal azimutaal-equidistant, gecentreerd op de regio;
- nominale resolutie: de fijnste bruikbare bronresolutie, begrensd tot minimaal
  500 meter;
- capaciteit: maximaal 250.000 rastercellen; wanneer nodig wordt de
  pixelgrootte deterministisch verhoogd totdat het maximum wordt gerespecteerd;
- identiteit: hash van regio-id, centrum, straal, projectie, resolutie en
  dimensies.

Een lokaal metrisch CRS voorkomt breedtegraadafhankelijke vervorming en werkt
ook buiten Europa. Een stabiel regiocentrum voorkomt dat kleine GPS-wijzigingen
telkens een andere grididentiteit veroorzaken.

## Ruimtelijke normalisatie

Elke geldige bronpixel wordt via zijn eigen rastertransformatie en CRS naar
het regionale analyseraster gebracht. Er wordt nooit op bron-arrayafmetingen
geschaald.

Regels:

- regenintensiteit in `mm/h` wordt oppervlaktegewogen/conservatief
  geprojecteerd;
- reflectiviteit in `dBZ` wordt niet rechtstreeks gemiddeld; adapters zetten
  ze vóór fusie om naar een lineaire of kwantitatieve regenmaat;
- geldigheids-, dekking- en kwaliteitsmaskers gebruiken nearest-neighbour;
- bron-`no-data` blijft `no-data` en wordt nooit nul neerslag;
- pixels buiten brondekking krijgen geen gewicht;
- nul neerslag binnen aantoonbaar geldige brondekking blijft een geldige nul.

De projectie levert per provider een `RegionalRadarLayer` met exact dezelfde
grididentiteit en afmetingen, plus waarde-, validiteits- en kwaliteitsvelden.

## Tijdnormalisatie

Een fusiecyclus kiest één `evaluated_at`. Per provider wordt alleen het meest
recente niet-toekomstige radarveld binnen zijn toegestane leeftijd gebruikt.
De echte `observed_at` blijft behouden.

De eerste versie interpoleert niet tussen tijden en verplaatst oude velden
niet kunstmatig. Leeftijd verlaagt het gewicht. Een te oud veld wordt met een
vaste reden uitgesloten. Zo wordt schijnnauwkeurigheid vermeden.

## Kwaliteitsgewogen fusie

Per analyserastercel worden alleen geldige providerwaarden gecombineerd. Het
gewicht bestaat uit afzonderlijk rapporteerbare factoren:

- runtime-/decoderkwaliteit;
- productkwaliteit en kwantitatieve geschiktheid;
- leeftijd ten opzichte van `evaluated_at`;
- native bronresolutie tegenover de analyseresolutie;
- beschikbare kwaliteitsmaskers;
- later, maar niet in deze fase: empirische kalibratiescore uit PostGIS.

De kandidaten worden per pixel deterministisch op dit samengestelde gewicht,
actualiteit en provider-id gerangschikt. Alleen de beste drie dragen numeriek
bij. Niet-geselecteerde geldige kandidaten worden in provenance als
`lower_ranked_standby` vermeld en tellen niet mee voor confidence. Het aantal
bronnen op zichzelf verhoogt confidence dus niet onbeperkt.

De eerste implementatie gebruikt vaste, verklaarbare basisgewichten en een
monotone leeftijdsafbouw. Eén bruikbare bron blijft geldig met lagere
fusieconfidence. Tegenstrijdige waarden worden begrensd en als
`provider_disagreement` in provenance vastgelegd; een bron wordt niet stil
verwijderd.

Uitvoer is één `FusedRegionalRadarField` met:

- het canonieke grid;
- gefuseerde `rain_rate_mm_h`;
- geldigheidsmasker;
- confidence per pixel;
- bijdragerbitmasker of compacte providerlijst per pixel;
- bron- en uitsluitingssamenvatting voor dashboard/PostGIS.

## Aansluiting op tracking en voorspelling

Het gefuseerde veld wordt de primaire invoer voor de bestaande
`ThinRadarTracker`. Detecties en footprints krijgen een stabiele
`fusion:<region_id>`-bronidentiteit. Daardoor ontstaat per regio één
gezaghebbende celset in plaats van meerdere cellen voor dezelfde bui.

De bestaande per-providertracking blijft gedurende de overgang beschikbaar
als expliciete fallback wanneer:

- geen gefuseerd veld kan worden gevormd;
- slechts een niet-projecteerbaar product beschikbaar is;
- de fusielaag een begrensde fout rapporteert.

Fallback wordt in snapshot, GeoJSON en PostGIS zichtbaar gemarkeerd; hij mag
niet stil als gefuseerde uitkomst worden voorgesteld.

De bestaande richting-, snelheid-, footprint-, entry- en exitberekening wordt
niet opnieuw gebouwd. Zij ontvangt de gefuseerde cellen via hetzelfde
`StormCell`-contract.

## PostGIS en MQTT

De bestaande resultaatopslag blijft leidend. PostGIS bewaart aanvullend per
fusiecyclus:

- region-grididentiteit en rastermetadata;
- deelnemende en uitgesloten providers met reden;
- samengevatte dekking en confidence;
- de uit het gefuseerde veld afgeleide cellen en tracks via de bestaande
  tabellen.

Het volledige raster wordt in deze fase niet duurzaam opgeslagen. Dit voorkomt
onnodige groei; genormaliseerde observaties, fusiesamenvatting, cellen,
footprints en voorspellingen volstaan voor het live product. RAW/replaybeleid
blijft een afzonderlijke beslissing.

MQTT publiceert alleen compacte metadata en de bestaande GeoJSON-resultaten,
niet de rastermatrix.

## Foutafhandeling

- Een onprojecteerbaar of ongeldig radarveld wordt alleen voor die regio en
  cyclus uitgesloten.
- Eén defecte provider verhindert geen fusie met de overige providers.
- Geen geldige pixels levert `onvoldoende_data`, nooit een droogmelding.
- Een capaciteitsschending vergroot deterministisch de pixelgrootte; zij kapt
  niet willekeurig een deel van de regio af.
- Longitude-overgang bij ±180 graden en hoge breedtegraden worden door het
  lokale CRS afgehandeld en krijgen expliciete tests.
- Alle uitsluitingen gebruiken vaste reden-codes zonder payloads of secrets.

## Teststrategie

1. Kleine synthetische rasters met verschillende CRS'en, resoluties en
   afmetingen worden op exact dezelfde fysieke gridcellen geprojecteerd.
2. `no-data` en geldige nul worden aantoonbaar verschillend behandeld.
3. Twee overlappende bronnen leveren één gewogen veld met reproduceerbare
   provenance.
4. Vier of meer overlappende bronnen bewijzen dat per pixel alleen de beste
   drie bijdragen en de rest als standby zichtbaar blijft.
5. Een verouderde of geografisch niet-toepasselijke bron draagt niet bij.
6. Capaciteitsbegrenzing behoudt de volledige regio en vergroot alleen de
   resolutie.
7. Een gefuseerde bewegende footprint levert via de bestaande tracker één cel,
   één bewegingslijn en dezelfde entry-/exitcontracten.
8. Zonder bruikbaar fusieveld blijft de bestaande per-providerfallback werken.
9. PostGIS- en snapshottests bewijzen de fusiesamenvatting zonder volledige
   rasterpayload.

## Acceptatiecriteria

- Twee providers met verschillende native afmetingen en CRS'en produceren voor
  dezelfde regio lagen met exact dezelfde grididentiteit en shape.
- Fysieke objectpositie en oppervlakte blijven binnen één analysepixel correct;
  er vindt geen beeldvervorming door array-scaling plaats.
- Geldige nul, ontbrekende dekking en ongeldige data blijven drie verschillende
  toestanden.
- De providerkeuze verandert automatisch met de regio en bevat geen vaste
  landenlijst.
- Eén meteorologische bui die door meerdere providers wordt gezien verschijnt
  als één primaire gefuseerde celset.
- Richting, snelheid, footprintpassage, PostGIS-opslag en MQTT blijven werken
  via de bestaande contracten.
- Workerheartbeat en een niet-radar snapshot wachten niet op rasterfusie.
- CPU-zware projectie/fusie blijft binnen de bestaande limiet van twee
  rekenthreads.
- De huidige per-providertracking blijft aantoonbaar beschikbaar als
  gemarkeerde fallback totdat live acceptatie de gefuseerde route vrijgeeft.
