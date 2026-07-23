# pySTEPS-nowcastworker voor Storm Tracker V3

Status: architectuurvoorstel; nog niet operationeel in 0.4.94.

## Doel

De huidige StormEngine volgt cellen en leidt beweging af uit componenten en
centroïden. Dat blijft kwetsbaar wanneer een bui vervormt, splitst, groeit of
geen voldoende rechte baan aflegt. pySTEPS is bedoeld voor korte
neerslag-nowcasts op volledige radarrasters. Het berekent een ruimtelijk
bewegingsveld en kan vervolgens deterministische of probabilistische
neerslagvelden extrapoleren.

Officiële documentatie:

- https://pysteps.github.io/
- https://pysteps.readthedocs.io/
- https://gmd.copernicus.org/articles/12/4185/2019/

## Waarom geen AppDaemon of custom-integrationproces

Optische stroming, FFT-cascades en ensembles zijn CPU- en geheugenintensief.
Ze horen niet in de Home Assistant-eventloop en evenmin in AppDaemon, dat voor
automatiseringslogica is bedoeld. Een mislukte of te grote berekening mag Home
Assistant niet blokkeren of opnieuw in geheugennood brengen.

Daarom wordt pySTEPS later verpakt als een zelfstandige Home Assistant App
(container) op dezelfde NUC. Storm Tracker blijft een gewone integratie.

## Gegevensstroom

1. De integratie haalt de gekozen radarbron op en normaliseert de echte
   georeferentie.
2. Alleen een begrensde uitsnede rond een RegionEngine wordt naar de lokale
   worker gestuurd.
3. De worker bewaart maximaal drie tot zes regelmatige rasterframes.
4. De worker berekent eerst een optisch bewegingsveld.
5. Fase 1 levert een deterministische extrapolatie of S-PROG-nowcast.
6. Fase 2 kan na validatie een klein STEPS-ensemble leveren.
7. De integratie ontvangt compacte resultaten:
   - voorspelde neerslagrasters per tijdstap;
   - overschrijdingskansen per intensiteitsdrempel;
   - onzekerheidscontouren;
   - kwaliteits- en rekendiagnostiek.
8. Targetprojectie, waarschuwingen, dashboard en historie blijven in de
   Storm Tracker-integratie.

## Isolatie en terugval

- uitsluitend lokaal verkeer via een interne HTTP-API of `/share`;
- harde requesttimeout;
- maximaal één berekening tegelijk per worker;
- vaste rasterafmetingen en maximaal aantal RegionEngines per taak;
- containerlimieten voor CPU en geheugen;
- geen onbeperkte opslag van ensemblevelden;
- healthcheck en circuit breaker;
- bij storing blijft de bestaande StormEngine werken;
- een pySTEPS-resultaat onderdrukt nooit een actuele lokale radar- of
  bliksemwaarschuwing.

## Gefaseerde invoering

### Fase A — invoercontract

- bronrasters naar één projectie en resolutie omzetten;
- tijdstappen op exact dezelfde nominale minuten uitlijnen;
- no-data, clutter en intensiteit uniform behandelen;
- rasterkwaliteit per provider in de kalibratiedatabase bewaren.

### Fase B — deterministische worker

- Lucas–Kanade of een andere gevalideerde optische-stroommethode;
- 0–90 minuten extrapolatie;
- vergelijken met de eerstvolgende werkelijke radarframes;
- nog geen productie-waarschuwingen.

### Fase C — operationele probabilistische nowcast

- klein STEPS-ensemble;
- kans op overschrijding van lichte, matige en zware neerslag;
- passagekans en onzekerheid per target;
- alleen activeren nadat de verificatiescores aantoonbaar beter zijn dan de
  huidige trajectlogica.

### Fase D — modelblending

Wind op 700/850 hPa en andere Open-Meteo-modelbegeleiding kunnen een zwakke
bewegingsprior leveren wanneer radartracking onvoldoende is. pySTEPS ondersteunt
ook blending met NWP-velden, maar dat wordt pas overwogen nadat de
radar-extrapolatie stabiel en meetbaar gevalideerd is.

## Minimale veilige startconfiguratie op de NUC

- één workerproces;
- één deterministische nowcast tegelijk;
- maximaal drie invoerframes bij de eerste proef;
- rasteruitsnede per RegionEngine, geen volledige Europese composiet;
- geen ensemble totdat CPU-, geheugen- en looptijdmetingen beschikbaar zijn;
- alle uitvoer als experimenteel markeren.
