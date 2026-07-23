# Storm Tracker V3 — providerarchitectuur 0.4.91

Dit document beschrijft de effectief geïmplementeerde providerstructuur van
Storm Tracker V3 0.4.91. Het onderscheidt operationele radar, fallbackdata,
bliksem, validatiebronnen en bronnen die alleen beleidsmatig voor toekomstige
uitbreiding zijn voorzien.

## 1. Hoofdstructuur

Iedere bewaakte locatie is een `ProjectionTarget`:

- de vaste Home Assistant-thuislocatie;
- iedere geconfigureerde Life360-persoon;
- de fictieve testtracker.

Een target wordt gekoppeld aan een `RegionEngine`. Targets die voldoende dicht
bij elkaar liggen delen dezelfde engine. Een verder gelegen target krijgt een
eigen engine met een eigen:

- geografisch observatiegebied;
- StormEngine en Observation Fusion Engine;
- radarbronkeuze;
- OPERA- en RainViewer-provider;
- Netatmo-stationset en luchtdrukhistoriek;
- weersystemen, bewegingen, verwachtingen en targetprojecties.

Hierdoor kunnen bijvoorbeeld België en Miami gelijktijdig worden bewaakt zonder
dat waarnemingen, druktrends of modeldata tussen beide regio's worden vermengd.
Open-Meteo is bewust centraal en targetgericht: één broker bundelt alle unieke
targetlocaties, maar publiceert ieder antwoord weer uitsluitend bij het
bijbehorende target.

## 2. Gecoördineerde providercyclus

Elke vijf minuten draait één vergrendelde providercyclus. Een nieuwe cyclus
wordt overgeslagen wanneer de vorige nog loopt. De volgorde is bewust vast:

1. actieve RegionEngines bepalen en locatiegebonden providers synchroniseren;
2. relevante officiële nationale providers wakker maken en ophalen;
3. KMI- en KNMI-radar ophalen als minstens één engine hun dekking nodig heeft;
4. OPERA, RainViewer en toepasselijke satellietfallbacks ophalen;
5. per RegionEngine de beste gezonde neerslagbron kiezen;
6. alleen waarnemingen van die gekozen bron naar de juiste engine routeren;
7. de bijbehorende echte rasteroverlay publiceren;
8. Netatmo als grondvalidatie per RegionEngine verwerken;
9. Open-Meteo centraal als modelbegeleiding per uniek target verwerken.

De vaste volgorde voorkomt dat een fallback al gekozen wordt voordat een lokale
bron klaar is. De provider-lifecycle zet een nationale provider alleen aan als
minstens één actieve RegionEngine binnen zijn dekking valt. Na vijf minuten
zonder toepasselijke engine gaat hij van cooldown naar sleeping.

Elke nationale provideraanroep heeft bovendien een harde timeout van twintig
seconden en onafhankelijke nationale providers worden parallel opgehaald. Ook
de volledige fasen zijn begrensd: nationaal 25 seconden, radarvergelijking 60
seconden, operationele radar 120 seconden en grond-/modelcontext 30 seconden. Eén
hangende TCP-verbinding kan de vijfminutencyclus daardoor niet onbeperkt
vasthouden. EUMETSAT LI en GOES GLM hebben elk een aparte limiet van 30 seconden.

OPERA en RainViewer worden per RegionEngine parallel en geïsoleerd opgehaald.
RainViewer krijgt maximaal twintig seconden per regio en OPERA maximaal
veertig seconden. Gedeelde nationale en full-diskproducten worden niet per
regio opnieuw gedownload.

Een nationale provider die drie opeenvolgende cycli faalt of time-out gaat,
opent zijn circuit breaker gedurende vijftien minuten. Daarna krijgt hij één
gecontroleerde proef; succes sluit het circuit en reset de foutenteller, een
nieuwe fout opent opnieuw de cooldown. De bronhiërarchie gaat ondertussen
meteen verder naar de eerstvolgende gezonde fallback.

## 3. Operationele neerslagproviders

| Provider | Dekking | Data | Runtimefrequentie | Rol |
|---|---|---|---:|---|
| KMI | België en ruime randzone | Officiële radarpixels, componenten, intensiteit en footprint | 5 min | Primaire lokale radar voor BE |
| KNMI | Nederland en randzone | Actuele officiële radarpixels plus 0–120 min nowcast | 5 min | Primaire lokale radar voor NL |
| DWD RADOLAN | Duitsland | Officiële composietradar, componenten en rasteroverlay | 5 min wanneer nodig | Primaire lokale radar voor DE |
| Météo-France Radar | Frankrijk | Officiële radarcomposieten met OAuth-vernieuwing | 5 min wanneer nodig | Primaire lokale radar voor FR |
| Met Office Radar | Groot-Brittannië | Officiële ODIM/HDF5-radar | 5 min wanneer nodig | Primaire lokale radar voor GB |
| DPC Radar | Italië | Protezione Civile-raster, intensiteit en footprints | 5 min wanneer nodig | Primaire lokale radar voor IT |
| AEMET Radar | Spanje | Officiële GeoTIFF-radar | 5 min wanneer nodig | Primaire lokale radar voor ES |
| OPERA | Europa | Europees radarcomposiet, gefilterde cellen en aanvaarde pixels | 5 min per toepasselijke engine | Eerste radarfallback in Europa |
| RainViewer | Wereldwijd waar frames bestaan | Radarraster, intensiteit, componenten en footprints | 5 min per engine | Generieke radarfallback |
| H SAF H40B | Meteosatgebied | Satelliet-afgeleide neerslagsnelheid en kwaliteitsindex | geëvalueerd in cyclus van 5 min; product typisch 20–30 min vertraagd | Laatste Meteosat-neerslagfallback en kalibratiebron |
| NOAA GOES RRQPE | Amerika/Pacific | Satelliet-afgeleide neerslagsnelheid | geëvalueerd in cyclus van 5 min | Satellietfallback waar GOES beschikbaar is |

Een droog maar geldig radarframe blijft een gezonde bron. Gezondheid wordt
bepaald uit de werkelijke producttijd en niet uit het aantal gevonden buien of
alleen het tijdstip van de HTTP-aanvraag. Een verouderd product kan daardoor
niet onbeperkt een actuele fallback blokkeren.

## 4. Grondvalidatie en modelbegeleiding

| Provider | Dekking | Data | Frequentie | Gebruik |
|---|---|---|---:|---|
| Netatmo | Wereldwijd waar publieke stations bestaan | Regen, luchtdruk, temperatuur, vochtigheid en wind | 5 min per RegionEngine | Grondbevestiging en regionale druktrend |
| Open-Meteo | Wereldwijd | 19 modelvelden: neerslag, kans, CAPE, LPI, CIN, Lifted Index, druk, wolken, vriesniveau en wind op 700/850 hPa | cyclus elke 5 min, echte API-cache 30 min | Modelbegeleiding en historische verificatie; nooit radar, grondwaarheid of celvorming |
| MeteoLux | Luxemburg | Lokale nowcast/modelinformatie | 5 min wanneer nodig | Modelbegeleiding; nooit operationele radar |
| GeoSphere Austria | Oostenrijk | INCA-puntnowcast in stappen van 15 minuten | 5 min wanneer nodig | Modelbegeleiding/nowcast; nooit operationele radar |
| ItaliaMeteo | Italië | ARPAE-modelverwachting en dagelijkse/historische radarcatalogus | 5 min wanneer nodig | Modelbegeleiding; nooit realtime-radar |

Netatmo is strikt per RegionEngine geïsoleerd. Open-Meteo gebruikt één gedeelde
broker die targets op praktisch dezelfde modelcel dedupliceert. Het antwoord
wordt daarna per target opgeslagen en nooit naar de Observation Fusion Engine
gerouteerd. Open-Meteo staat daarom in de beleidsmatrix onder
`model_guidance`, niet onder `ground_validation`. De bestaande globale
Netatmo-luchtdruksensor blijft voor
compatibiliteit bestaan, maar toont uitsluitend de trend van `zone.home`.

Bij een cold start zonder bruikbare opslaghistoriek meldt de druktrend
`onvoldoende_data`. Delta's blijven leeg en `rapid_pressure_fall` blijft false;
er wordt dus geen drukval of confidencewijziging verzonnen. De legacy-sensor
behoudt dezelfde unique ID en blijft beschikbaar, maar heeft state `unknown`
in plaats van een foutieve `0` totdat minstens drie stations een vergelijkbaar
60-minutenvenster leveren. Open-Meteo publiceert vóór de eerste succesvolle
respons `INITIALIZING`, met onbekende neerslagwaarden. De eerste zeven
kwartierstappen behouden de bestaande 90-minutensamenvatting; dertien stappen
leveren daarnaast drie uur modelcontext en de uurlijkse velden zes uur.
Ontbrekende optionele velden blijven `unknown` en worden nooit als nul
geïnterpreteerd. HTTP 429, time-outs,
verouderde cache, laatste poging, laatste succes en dataleeftijd zijn apart
zichtbaar. Daardoor kan ontbrekende modeldata nooit als droog worden gelezen.

## 5. Bliksemproviders

| Provider | Dekking | Data | Frequentie | Rol |
|---|---|---|---:|---|
| Blitzortung | Wereldwijd | Individuele blikseminslagen met positie en tijd | realtime verbinding | Primaire bliksembron |
| EUMETSAT LI | Europa, Afrika en Meteosatgebied | Satellietgedetecteerde flashes | 2 min | Fallback wanneer Blitzortung niet bruikbaar is |
| NOAA GOES GLM | Amerika en Pacific | Satellietgedetecteerde flashes van GOES-18/19 | 1 min | Fallback voor toepasselijke lengtegraden |

In normale automatische modus blijven de satellietbliksembronnen standby zolang
Blitzortung verbonden is. In satelliettestmodus wordt Blitzortung live gestopt
zonder Home Assistant te herstarten.

Bliksem en regen blijven verschillende datatypes. Een inslag:

- wordt alleen naar RegionEngines binnen bereik gerouteerd;
- verschijnt als afzonderlijk bliksemevent op de kaart;
- kan een bestaande neerslagfootprint als actief onweer markeren;
- creëert nooit zelfstandig een fictieve regenbui.

## 6. Bronhiërarchie per RegionEngine

De effectieve neerslagkeuze is:

1. gezonde officiële lokale realtime-radar van het land;
2. OPERA binnen zijn Europese dekking;
3. RainViewer als operationele radarfallback;
4. H SAF H40B wanneer RainViewer geen lokale echo heeft en H40B wel neerslag ziet;
5. NOAA GOES RRQPE in toepasselijke Amerikaanse/Pacific-regio's;
6. geen operationele radar wanneer geen bron gezond en actueel is.

Bij een RegionEngine die targets uit meerdere nationale radargebieden deelt,
wordt niet willekeurig één nationaal product gekozen. De engine valt dan terug
op de overkoepelende bron, doorgaans OPERA.

Landelijke primaire radar:

| Land | Primaire lokale radar | Daarna |
|---|---|---|
| België | KMI | OPERA → RainViewer → H SAF |
| Nederland | KNMI | OPERA → RainViewer → H SAF |
| Duitsland | DWD RADOLAN | OPERA → RainViewer → H SAF |
| Frankrijk | Météo-France | OPERA → RainViewer → H SAF |
| Groot-Brittannië | Met Office | OPERA → RainViewer → H SAF |
| Italië | DPC Radar | OPERA → RainViewer → H SAF |
| Spanje | AEMET | OPERA → RainViewer → H SAF |
| Luxemburg | geen volwaardige lokale realtime-radar | OPERA → RainViewer → H SAF; MeteoLux valideert alleen |
| Oostenrijk | geen geïntegreerde lokale realtime-radar | OPERA → RainViewer → H SAF; GeoSphere valideert alleen |
| Griekenland en overige OPERA-landen | geen eigen geïntegreerde lokale radar | OPERA → RainViewer → H SAF |
| Verenigde Staten | nog geen NOAA MRMS-runtime | RainViewer → GOES RRQPE |
| Overige wereld | geen geïntegreerde lokale radar | RainViewer en beschikbare satellietfallback |

## 7. Van brondata naar weersysteem

Alle operationele waarnemingen krijgen een uniform `Observation`-contract met
onder andere:

- datatype;
- bron;
- latitude en longitude;
- producttimestamp;
- intensiteit en kwaliteit;
- oppervlakte;
- cel- en systeemidentiteit;
- werkelijke footprintpunten indien beschikbaar.

De `StormManager` routeert elke observation uitsluitend naar RegionEngines
waarvan het observatiegebied de positie omvat. De Observation Fusion Engine
combineert radarcellen, regenmeters en bliksem. De StormEngine onderhoudt de
historiek en berekent daarna onder andere:

Bij een wissel van radarbron blijft het raster van de nieuwe bron exact intact.
Omdat verschillende providers niet gegarandeerd dezelfde projectie, resolutie
of intensiteitsschaal gebruiken, worden hun pixels niet blind gemiddeld. De
runtime markeert de bronwissel tien minuten lang als overgang en verlaagt de
prognosezekerheid tijdelijk met tien procentpunten. Dit dempt analytische
sprongen zonder een kunstmatige neerslagfootprint te tekenen.

De passieve kalibratie is multi-provider en regionaal. Elk beschikbaar frame
van KMI, KNMI, DWD, Météo-France, Met Office, DPC, AEMET, OPERA, RainViewer,
H SAF of GOES wordt alleen gekoppeld aan een andere bron met exact dezelfde
nominale minuut én dezelfde geografische RegionEngine-sleutel. De sensor
rapporteert overlap, gemiste en extra bezette rastervakken, precision, recall
en F1 per providerpaar. Deze scores zijn voorlopig diagnostisch en wijzigen
geen operationele filtering.

Alle invoer voor latere kalibratie wordt onbeperkt bewaard in
`.storage/storm_tracker_v3_calibration.sqlite3`. De database bevat afzonderlijke
tabellen voor bronframes, hun bezette 0,10-gradenrasterpunten en paarsgewijze
vergelijkingsresultaten. Ook droge frames worden opgeslagen. SQLite gebruikt
WAL en transactionele batches buiten de Home Assistant-eventloop; er is bewust
geen retentie, aggregatie of automatische filteraanpassing actief.

- actuele afstand tot elk target;
- naderend, wegtrekkend, passerend of stationair;
- bewegingsvector en snelheid;
- verwachte dichtste passage;
- aankomsttijd indien voldoende betrouwbaar;
- actuele en veranderende intensiteit;
- confidence en broninformatie;
- convectie- en MCS-status.

## 8. Outputs

De runtime publiceert:

- een algemene neerslagstatus voor thuis;
- een afzonderlijke neerslagstatus per persoon/testtracker;
- targetlocatie, plaats, adres, landcode en landnaam;
- afstand, richting, beweging, passage en aankomsttijd;
- intensiteit, intensiteitstrend en confidence;
- regionale luchtdruktrend en snelle drukval;
- actieve radarbron en reden van bronkeuze per RegionEngine;
- provider-lifecycle, gezondheid, productleeftijd en fouten;
- GeoJSON met targets, weersystemen, rasteroverlays en bliksem;
- de multi-targetkaart met echte bronpixels waar de provider die levert;
- gebeurtenissen voor dashboardupdates en latere waarschuwingen.

De regionale luchtdruktrend heeft na een cold start een opwarmfase. Er zijn
minstens dertig minuten aaneengesloten meetpunten nodig van drie vergelijkbare
Netatmo-stations; gaten groter dan vijftien minuten maken een station tijdelijk
ongeschikt. Tot aan die voorwaarden blijft de trend `onvoldoende_data` en wordt
geen snelle drukval of extra confidence afgeleid.

## 9. Geïmplementeerd versus gepland

De providerpolicy bevat al geografische plaatsen voor toekomstige lokale
bronnen zoals NOAA MRMS, MSC GeoMet, BOM, JMA, CWA Taiwan en IDEAM. Deze namen
zijn nog geen garantie dat de bijbehorende runtimeprovider bestaat. Tot hun
implementatie gebruikt het systeem in die regio's alleen de hierboven als
operationeel beschreven bronnen.

De technische weergave van de multi-targetkaart toont de actuele
databasegrootte, aantallen frames, rasterdatapunten, vergelijkingen, bronnen en
regio's, plus de laatste schrijfbatch. Deze tellers worden incrementeel
bijgehouden zodat een grote database niet iedere vijf minuten volledig wordt
gescand.

Recente bliksem wordt daarnaast als een afzonderlijke geclusterde envelop
gepubliceerd. Deze rode zone pulseert onafhankelijk van de neerslagpixels:
boven een radarecho betekent ze onweer in de bui, zonder onderliggende
regenkleur betekent ze bliksemactiviteit zonder bevestigde radarneerslag. De
zone gebruikt inslagen tot vijf minuten oud, een buffer van twaalf kilometer
en verandert nooit de geometrie of intensiteit van het radarraster.

## 10. Testdekking in 0.4.85

De provider-audit omvat tests voor:

- parsing, georeferentie, timestamps en rastercomponenten;
- coverage en slaap/activatielifecycle;
- lokale bronselectie en fallbackvolgorde;
- droog versus defect/verouderd radarframe;
- routing naar de juiste RegionEngine;
- afzonderlijke Netatmo- en Open-Meteo-state per engine;
- gekoppelde rasteroverlay van de gekozen bron;
- scheiding tussen bliksem- en neerslagrouting;
- één geordende vijfminutencyclus zonder concurrerende providerpolls.

De suite controleert daarnaast harde providertimeouts, fasebegrenzing,
cold-startneutraliteit en het backwards-compatible statecontract van de
globale Netatmo-luchtdruksensor.
