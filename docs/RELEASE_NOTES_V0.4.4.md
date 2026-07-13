# Storm Tracker V3 v0.4.4

## Opgelost

OPERA kon op de NUC terugvallen op een oud S3-object, hoewel een actueel
Europees DBZH-composiet beschikbaar was. De provider controleert nu eerst de
deterministische OPERA-bestandsnamen van de recente vijfminutenintervallen met
HTTP `HEAD`. De bestaande S3-listing blijft behouden als fallback.

## Validatie

- 172 unit-tests geslaagd.
- Live OPERA-smoketest geslaagd: actuele discovery, download en ODIM-HDF5-
  structuur bevestigd.
- RainViewer blijft automatisch de operationele fallback wanneer OPERA niet
  vers of bereikbaar is.

## Deployment

Pak de zip uit naar `custom_components/storm_tracker_v3` en herstart Home
Assistant. Controleer daarna de systeemlog op `OPERA cache bijgewerkt` en
`OPERA: <n> cellen`.
