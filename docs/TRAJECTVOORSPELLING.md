# Adaptieve trajectvoorspelling

Storm Tracker V3 gebruikt vanaf versie 0.4.88 niet langer uitsluitend een
rechte extrapolatie van het laatste gemeten neerslagcentrum. Per WeatherSystem
worden twee lichte modellen met elkaar vergeleken:

1. constante snelheid: een rechte baan;
2. constante versnelling: een parabool die een geleidelijke koers- of
   snelheidsverandering kan beschrijven.

Het versnellingsmodel wordt alleen operationeel wanneer:

- minstens zes radarcentroids en minstens tien minuten historie beschikbaar
  zijn;
- de berekende versnelling en snelheid meteorologisch plausibel blijven;
- een rollende hindcast op de laatste maximaal drie waarnemingen duidelijk
  beter is dan die van de rechte lijn.

Bij alle andere gevallen blijft het rechte model actief. Dit voorkomt dat
ruis, providerwissels of kleine vormveranderingen onterecht als een bocht
worden geinterpreteerd.

## Passage en onzekerheid

De gekozen baan verschuift de meest recente echte radarcontour minuut per
minuut tot maximaal 90 minuten vooruit. Het systeem rapporteert:

- `eta_basis: radarcontour` wanneer de verschoven waargenomen contour het
  target werkelijk raakt;
- `eta_basis: onzekerheidscorridor` wanneer alleen de conservatieve
  onzekerheidszone het target bereikt;
- `passage_classification: mist` wanneer ook de corridor het target niet
  bereikt.

De corridor groeit met de gemeten hindcastfout, de horizon en de omvang van de
versnelling. Een corridor-ETA is daardoor bewust minder exact dan een
contour-ETA.

## Brongebruik

De bewegingshistorie gebruikt radarcentroids zolang radar beschikbaar is.
Bliksem kan een traject alleen dragen als expliciete fallback voor een systeem
zonder radarhistorie. Bliksem op een flank mag de geometrie van een
radar-neerslaggebied niet verschuiven.

## Verificatie

Elke providercyclus bewaart de eigen thuisverwachting samen met
`sensor.neerslagverwachting_gemiddeld` en
`sensor.neerslagverwachting_totaal`. Iedere werkelijk door de
waarschuwingsautomatisering aangevraagde melding wordt daarnaast als
onveranderlijke snapshot opgeslagen. Dit maakt latere vergelijking van ETA,
passage, intensiteit, modelkeuze en werkelijk gemeten regen mogelijk zonder de
operationele bronkeuze automatisch te wijzigen.

## Geheugengrens

Het model gebruikt alleen standaard Python, maximaal twintig centroidpunten
per WeatherSystem en maximaal negentig projectiestappen per target. De
groeiende SQLite-database staat op schijf; alleen de actuele schrijfbatch zit
kort in het geheugen. De NUC met 12 GB RAM biedt ruimte voor een latere,
afzonderlijk begrensde optical-flowlaag, maar versie 0.4.88 laadt bewust geen
volledige pySTEPS-stack in het Home Assistant-proces.

## Onderbouwing

- pySTEPS beschrijft optical flow, semi-Lagrangiaanse advection en de
  beperkingen van Lagrangiaanse persistentie:
  <https://gmd.copernicus.org/articles/12/4185/2019/>
- Een constant-versnellings-Kalmanmodel is eerder toegepast op
  stormidentificatie en -tracking:
  <https://journals.ametsoc.org/view/journals/atot/26/3/2008jtecha1153_1.xml>
- NOAA documenteert dat een lineaire SCIT-extrapolatie niet volstaat voor
  niet-lineaire stormbeweging:
  <https://training.weather.gov/wdtd/courses/rac/documentation/rac25-products.pdf>

Storm Tracker V3 is geen gecertificeerd waarschuwingssysteem. Officiele
waarschuwingen en lokaal zicht op de situatie blijven altijd leidend.

## Dichtstbijzijnde rand versus gevolgd systeem

De operationele veiligheidsafstand en de prognose hoeven niet over hetzelfde
stormobject te gaan. Een nieuwe echo van één frame kan dichterbij liggen,
terwijl een verder systeem met meerdere consistente frames als enige een
bruikbare beweging heeft.

Daarom publiceert de targetstatus afzonderlijk:

- `nearest_precipitation_storm_id`, randafstand, centrumafstand en frames;
- `tracked_system_storm_id`, randafstand, centrumafstand en frames;
- `nearest_precipitation_is_tracked_system`;
- `distance_system_relation`.

`distance_km` blijft altijd de kleinste werkelijk waargenomen buirand en is dus
de conservatieve veiligheidsmaat. ETA, passage en baanmodel horen bij
`tracked_system_storm_id`.

## Persistente bronwisselprofielen

Vanaf versie 0.4.89 wordt een bronwissel niet meer als volledig historieloos
behandeld. Iedere gelijktijdige providervergelijking levert twee afzonderlijke
richtingen op, bijvoorbeeld KMI naar OPERA én OPERA naar KMI. De database
onthoudt per RegionEngine en als globale fallback:

- detectieverhouding en extra nat oppervlak;
- verhouding van het natte oppervlak;
- F1-score;
- gemiddelde intensiteitsafwijking op overlappende rastercellen;
- kleine geografische rasterverschuiving;
- verschil in producttijd.

Bij minstens twaalf bruikbare natte vergelijkingen kan zo'n profiel de
tijdelijke confidence-straf en duur van de overgang aanpassen. Zonder
voldoende historie blijft de vaste veilige standaard van tien procentpunten
gedurende tien minuten actief. Het profiel corrigeert in deze fase geen
radarpixels of berekende afstanden.
