# Storm Tracker V3 v0.4.6

De realtime kaart toont een bui rond Drenthe/Overijssel. Vanaf de huidige
trackerlocatie bij Mechelen ligt die zone deels buiten een radius van 200 km.
De standaard radarradius is daarom verhoogd naar 300 km (een uitsnede van
ongeveer 600 bij 600 km).

De OPERA-sensor publiceert nu `radius_km` en de werkelijk gebruikte `bbox`,
zodat de dekking op de NUC rechtstreeks controleerbaar is.
