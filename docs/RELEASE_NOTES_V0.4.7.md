# Storm Tracker V3 v0.4.7

Voegt inspecteerbare ruwe OPERA-cellen toe aan de attributen van
`sensor.stv3_opera_observaties`. Hiermee kan een realtime kaartcel exact worden
vergeleken met OPERA, onafhankelijk van clustering en WeatherSystem-merging.

Daarnaast wordt na de rechthoekige rastercrop een echte cirkelfilter toegepast.
Hierdoor kunnen cellen in de projectiehoeken niet langer buiten de ingestelde
radius in de WeatherSystem Engine terechtkomen.
