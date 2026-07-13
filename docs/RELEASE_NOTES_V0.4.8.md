# Storm Tracker V3 v0.4.8

## OPERA kruisvalidatie

- OPERA-cellen met quality >= 0.5 mogen zelfstandig WeatherSystems voeden.
- OPERA-cellen met lagere quality worden alleen aanvaard wanneer een recente
  KMI- of KNMI-radarpixel binnen 25 km dezelfde neerslag bevestigt.
- Onbevestigde lage-quality echo's blijven zichtbaar in diagnostiek, maar
  creëren geen WeatherSystems meer.
- Diagnostiek toont raw, aanvaard, bevestigd en afgewezen aantallen en per cel
  de verificatiestatus.
- Het laatste OPERA-resultaat blijft beschikbaar wanneer een poll nog hetzelfde
  vijfminutenproduct aantreft; de sensor springt daardoor niet meer tijdelijk
  naar nul.

## Validatie

- 179 tests geslaagd.
- OPERA-projectie gecontroleerd tegen alle vier de officiële rasterhoeken.
