# Storm Tracker V3 v0.4.3

Deze tussenrelease maakt de radar-runtime veilig voor verdere praktijktests.

## Operationele radarstrategie

- Binnen OPERA-dekking: OPERA primary.
- Bij ontbrekende, stale of foutief geparste OPERA-data: RainViewer fallback.
- KMI en KNMI: vergelijking en sensoren, geen invoer naar WeatherSystems.

Een droog maar geldig OPERA-product blijft gezond. `0 cellen` veroorzaakt dus geen
onnodige fallback. Alleen discovery-, freshness-, download- of parseproblemen doen dat.

## Nog niet in deze release

- compacte polygongeometrie per radarcel;
- volledige dynamische RegionEngine-runtime voor meerdere ProjectionTargets;
- config-entry/unload-migratie.

Deze onderdelen volgen afzonderlijk omdat ze datamodellen en configuratielifecycle
wijzigen. Ze zijn niet nodig om dubbele radarverwerking nu al te stoppen.

## Controle na installatie

- `sensor.stv3_actieve_radarbron` is normaal `opera`;
- `sensor.stv3_opera_observaties` bevat alleen de lokale radius;
- KMI/KNMI-sensoren blijven data tonen;
- bij OPERA-storing schakelt de bron naar `rainviewer`;
- actieve storms verschijnen niet dubbel door meerdere radarproviders.
