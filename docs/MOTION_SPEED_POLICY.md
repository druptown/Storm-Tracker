# Bewegingsbeleid zonder meteorologisch snelheidsplafond

Status: lokale implementatie voor roadmapgat P0-C01; niet live gedeployed.

## Beslisregel

Een waargenomen weersysteem of bewegingsmodel wordt niet afgewezen omdat een
berekende snelheid of versnelling boven een vaste km/u- of km/u²-waarde ligt.
Twee geldige posities met oplopende waarnemingstijd blijven een gemeten
intervalvector. Meer frames mogen de kwaliteit, consistentie en
hindcastvaardigheid bepalen, maar niet een vooraf gekozen maximumsnelheid.

## Identiteitsassociatie

De legacy `StormEngine` gebruikt geen `180 km/u`-poort meer. Binnen de reeds
begrensde ruimtelijke kandidaatselectie blijft een radarobservatie koppelbaar,
ongeacht de snelheid die uit afstand/tijd volgt. Tegenstrijdige bronidentiteit
binnen exact hetzelfde radarframe blijft wel een geldige uitsluitingsgrond.

De HA-engine en de worker tracking-core bevatten dezelfde broncode voor
`storm_engine.py` en `trajectory.py`.

## Observatiebereik

`CoverageArea.horizon_km` is een expliciete data-/rekenstraal van de caller.
De ongebruikte helper die deze straal met een standaard van `100 km/u`
berekende, is verwijderd. Een observatie buiten een gekozen regionaal bereik
betekent alleen dat ze niet is opgehaald of verwerkt door die region-engine;
het is geen uitspraak dat een weersysteem meteorologisch ongeldig is.

## Begrensde rasterberekening

De rastermatcher heeft nog steeds een harde resourcebegrenzing, maar die is
uitsluitend in rekeneenheden geformuleerd:

- het geometrische centrum van de natte cellen bepaalt het midden van het
  zoekvenster;
- dat midden mag op elke geldige rastersprong liggen en heeft dus geen
  snelheidslimiet;
- per as worden maximaal 12 cellen rondom dat midden onderzocht;
- daardoor worden per framepaar maximaal `25 × 25 = 625` kandidaten getest,
  onafhankelijk van de gemeten km/u;
- een beste kandidaat op een kunstmatige vensterrand wordt behouden met
  `translation_censored=true`, open onzekerheid en
  `status=motion_indeterminate`;
- zo'n gecensureerde vector stuurt geen deterministische nowcast totdat een
  ruimere of betere methode hem oplost.

De bestaande contractwaarde `method=bounded_grid_match_v1` blijft behouden
voor compatibiliteit. `search_policy=centroid_guided_fixed_cell_window_v2`
maakt de gewijzigde zoekstrategie expliciet.

## Modelkeuze

Het constant-versnelde model wordt uitsluitend gekozen wanneer de
één-frame-hindcast aantoonbaar beter is dan constante snelheid. De voormalige
grenzen op versnelling en snelheid op +60/+90 minuten zijn verwijderd.
Een onopgeloste of gecensureerde historische vector kan geen
versnellingsmodel activeren.

## Legacy nowcast en virtuele probes

De laatste verborgen `2–120 km/u`-poort is eveneens verwijderd uit:

- de pySTEPS-rastervector die een nog rijpende legacy storm tijdelijk kan
  begeleiden;
- de selectie en projectie van roaming virtuele grondprobes.

Iedere eindige, niet-negatieve snelheid blijft daar bruikbaar; er bestaat geen
meteorologisch maximum. Ongeldige getallen (`NaN`, oneindig of een negatieve
snelheidsrepresentatie) blijven een numerieke contractfout en geen
weersafwijzing. Roaming projecties gebruiken een grootcirkelbestemming in
plaats van een lineaire latitude/longitude-offset, zodat ook grote
verplaatsingshypothesen geografisch geldig blijven. Een vector met onvoldoende
identity- of uncertaintybewijs kan nog steeds als `motion_indeterminate`
worden gemarkeerd, maar wordt nooit uitsluitend wegens zijn snelheid
verwijderd.

## Operationele grens

De worker voert `motion_preflight` als operationele job uit, maar de integratie
slaat het resultaat alleen op en toont het via de pySTEPS-preflightsensor. De
sparse nowcast is nog niet aangesloten op waarschuwingen of de primaire
trackerbeslissing. De gelijknamige evaluator in de HA-engine is een
onbedrade referentie-/pariteitsimplementatie.

De legacy engine en trajectory zijn wel productiepadcode, maar deze lokale
wijzigingen worden pas live na een expliciete deploybeslissing, volledige
regressietest en soak-validatie.
