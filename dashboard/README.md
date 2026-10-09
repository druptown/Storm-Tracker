# Storm Tracker-dashboard

`stv3-multi-target-map.js` is de bron van de inline Home Assistant-resource
`custom:stv3-multi-target-map`. De kaart:

- leest de geauthenticeerde feed `/api/storm_tracker_v3/geojson`;
- valt in `worker_only` automatisch terug op `zone.home`,
  `device_tracker.life360_*` en de fictieve tracker wanneer die legacy-feed
  niet geregistreerd is;
- centreert op thuis, een Life360-persoon of de fictieve testtracker;
- toont RegionEngines, weersysteemhulls, lokale RadarCells en
  bewegingsvectoren;
- gebruikt OpenStreetMap-rastertegels met origin-referrer en zichtbare
  bronvermelding, zonder extra HACS-kaart.

De operationele kaartresource wordt via de Home Assistant-resource-API beheerd.
Na een bronwijziging moet de inline resource worden bijgewerkt en de frontend
hard worden vernieuwd.
