# Live bronaudit NAS-HA — 24 september 2026

## Afbakening

Deze audit controleert uitsluitend de bronlaag:

1. actuele brondata ontvangen;
2. geografisch toepassen op de juiste tracker/regio;
3. converteren naar het interne observatiemodel;
4. zichtbaar maken op de kaart;
5. opslaan in PostGIS.

Beweging, bronfusie en passagevoorspelling vallen buiten deze audit.

## Live bewezen

| Bron | Testlocatie | Bewijs |
|---|---|---|
| DWD RADOLAN | 53.5392, 7.8526 (NW-Duitsland) | Actuele beelden van 11:35 en 11:40; genormaliseerde regencellen op kaart en in PostGIS. |
| KNMI RTCOR | 53.5392, 7.8526 | Actuele beelden van 11:40 en 11:45; genormaliseerde regencellen op kaart en in PostGIS. |
| OPERA DBZH | Duitsland en Middellandse Zee | Actuele Europese reflectiviteit; genormaliseerde contouren op kaart en in PostGIS. |
| Blitzortung | 34.2, 12.2 (Middellandse Zee) | Regionale MQTT-topics wijzigden van 50 naar 60. Er kwamen 21 verse inslagen binnen; 13 waren zichtbaar op de kaart binnen 15 minuten. PostGIS bevat de inslagen met doel `test_tracker`. |
| Open-Meteo | 34.2, 12.2 | Nieuwe waarden op exact de testcoördinaten: regen 0,4 mm/u, kans 53% en druk 1021,7 hPa; opgeslagen in PostGIS. |
| METAR | Mondiaal | EDDW/Bremen, KJFK/New York, SBGR/São Paulo, HKJK/Nairobi, RJTT/Tokio en YSSY/Sydney leverden elk actuele observaties via de regionale stationselectie. Iedere cyclus bleef foutloos en schreef met `postgis=current`. |
| AEMET composietradar | Madrid/Spanje | Worker `1.18.34` downloadde de officiële publieke composietbundel, decodeerde 29 radarobjecten, rapporteerde `ready/operational/current` zonder fout of reden en voltooide de live cyclus met `postgis=current`. De AEMET-specifieke versheidsgrens is 45 minuten wegens de aantoonbare publicatievertraging van de bundel; overige radargrenzen blijven ongewijzigd. |
| Météo-France DPRadar | Parijs/Frankrijk | Worker `1.18.35` haalde het officiële 500 m LAME_D_EAU-HDF5-product op, verkreeg en cachete de OAuth-token uitsluitend in geheugen, decodeerde de nationale radargrid en rapporteerde `ready/operational` zonder cyclusfout. De geteste omgeving rond Parijs was droog (`no_observations`); de cyclus sloot af met `postgis=current`. |

## Nog niet bewezen of geblokkeerd

| Bron | Status | Feitelijke reden |
|---|---|---|
| Netatmo | niet actueel | Worker meldt `operational/no_observations`; laatste opgeslagen waarneming is van 23 september. Een groene verbinding bewijst hier geen actuele data. |
| Wunderground | geblokkeerd | Geen API-sleutel; adapter meldt `blocked_api_key`. |
| KMI RADQPE | geblokkeerd | Nog geen API-toegang; status `api_required/external_access_required`. |

## Niet aangesloten op de dunne live runtime

De huidige worker maakt adapters aan voor KMI, OPERA, KNMI, DWD, AEMET, Météo-France,
Open-Meteo, METAR, Netatmo, Wunderground en Blitzortung. Andere dashboardbronnen — onder
meer Met Office, RainViewer, EUMETSAT/GOES/HSAF/NOAA,
Synoptic en validatiecatalogi — hebben in deze runtime geen actieve adapter.
Een trackerverplaatsing kan die bronnen daarom niet operationeel maken.

## Vastgestelde bronlaagproblemen

1. Een locatieverplaatsing wordt pas na ongeveer drie minuten door de worker
   overgenomen. Tot een nieuwe providerfetch kunnen eerder opgehaalde
   doelwaarden tijdelijk bij de nieuwe locatie blijven staan.
2. METAR werkt met een rapporterend station, maar HTTP 204 voor een station
   zonder actueel rapport moet als geldige lege respons worden behandeld.
3. Netatmo levert momenteel geen actuele observaties ondanks een groene
   verbindingsstatus.

## Gereedheid versus actuele locatie

Worker `1.18.35` houdt voor iedere afgewerkte provider twee afzonderlijke
statussen bij. `Ready` blijft zichtbaar wanneer de tracker buiten het
geografische bereik staat; de actuele toestand wordt dan blauw als
`Buiten bereik` weergegeven. Dit is live gecontroleerd met AEMET na herstel
van de fictieve tracker naar thuis.

## Mondiale cataloguscorrectie

Worker `1.18.30` verwijdert de kunstmatige Europagrens voor METAR en
Blitzortung. Beide bronnen zijn mondiaal toepasbaar, maar blijven per actieve
trackerregio begrensd: METAR selecteert alleen nabije stations en Blitzortung
abonneert alleen de regionale geohashtopics. Open-Meteo, Netatmo en
Wunderground waren al zonder geografische catalogusgrens geregistreerd.
