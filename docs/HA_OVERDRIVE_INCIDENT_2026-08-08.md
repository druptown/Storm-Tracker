# HA-overdrive-incident van 2026-08-08

Status: bevestigd live incident; harde releasegate; geen deploybesluit.

## Samenvatting

De actieve Storm Tracker-integratie blokkeerde Home Assistant tijdens een
providercyclus lang genoeg om de frontend en poort 8123 onbereikbaar te maken.
Dit was geen dashboardprobleem en geen bewijs van een drukke workerqueue. De
live installatie draaide integratie `1.11.1` in het legacy runtimepad, terwijl
worker `1.14.0` online was met queue-diepte nul en zonder actieve job op het
moment dat HA opnieuw bereikbaar werd.

De operator herstartte Home Assistant nadat het systeem opnieuw bereikbaar
was. Metingen na die herstart horen daarom niet bij de oorspronkelijke
incidenttijdlijn en mogen niet als herstel- of regressiebewijs worden gebruikt.

## Alleen-lezen waarnemingen

Alle tijden zijn `Europe/Brussels` op 2026-08-08.

- 22:04:15: TCP-poort 8123 niet bereikbaar.
- 22:04:38: TCP-poort 8123 nog niet bereikbaar.
- 22:04:48: TCP-poort 8123 opnieuw bereikbaar.
- De frontend meldde tijdens de uitval `Verbinding verbroken. Opnieuw
  verbinden...`.
- Het integratiescherm rapporteerde Storm Tracker V3 `1.11.1` met 34 entiteiten.
- `sensor.stv3_worker_status` rapporteerde worker `1.14.0`, `online`, queue nul,
  geen actieve job en 24 cumulatieve failed jobs.
- Het HA-logboek meldde om 22:08:02:
  `Providerfase radar overschreed harde timeout van 120 seconden`.
- In hetzelfde tijdsvenster liepen ook MQTT-PUBACK, Supervisor en andere
  integraties in time-outs. Dat is compatibel met globale HA-starvation en niet
  met een geïsoleerde dashboardfout.
- Recorder waarschuwde dat de attributen van
  `sensor.stv3_actieve_storms` groter waren dan 16.384 bytes en daarom niet
  opgeslagen konden worden.

Vanaf 22:04:46–22:04:47 bevatte het log opnieuw setup- en deprecatiemeldingen
van meerdere integraties; rond 22:05:01 werden ook triggers opnieuw
geïnitialiseerd. Dat is consistent met een zojuist gestart HA Core-proces en
maakt een automatische Core-restart tijdens de eerste uitval aannemelijk. Een
proces-ID of uptime-opname van vóór de uitval ontbreekt echter, zodat dit als
onderbouwde inferentie en niet als bewezen crash wordt geregistreerd.

De minimale bewezen onbereikbaarheid is 23 seconden. De werkelijke duur kan
langer zijn, omdat de eerste controle pas na de menselijke waarneming begon.
De radarstage zelf bereikte volgens het HA-logboek haar harde grens van 120
seconden.

## Codecorrelatie

Het legacy pad in `custom_components/storm_tracker_v3/__init__.py`:

- plant `_poll_all` iedere vijf minuten;
- plant `_poll_netatmo` en `_poll_goes_glm` iedere minuut;
- plant `_poll_eumetsat_li` iedere twee minuten;
- start bij initialisatie onmiddellijk een volledige `_poll_all`;
- voert binnen `_poll_all` achtereenvolgens nationale providers,
  radarvergelijking, radar, gronddata, modelcontext, virtuele probes,
  observation flush en calibrationopslag uit;
- kan een extra volledige cyclus starten na een target- of regiowijziging.

De mutex in de HA-workerbridge serialiseert uitsluitend MQTT-workerrequests.
Zij begrenst niet de provider-, radar-, fusion-, object-, sensorattribuut- en
opslagwerkzaamheden die het legacy integratiepad zelf in Home Assistant blijft
uitvoeren. De queuebugfix kon dit incident daarom principieel niet oplossen.

## Operationele conclusie

Tot aan een apart gevalideerde worker-only release hoort de legacy-integratie
uitgeschakeld te blijven. De worker mag afzonderlijk draaien zolang hij geen
HA-latency veroorzaakt. Een live fallback van worker-only naar legacy is
verboden: ontbrekende workerdata moet zichtbaar `unknown/stale` worden.

## Harde releasecriteria

Een volgende kandidaat mag pas live worden aangeboden wanneer alle onderstaande
punten samen bewezen zijn:

1. HA registreert in worker-only mode geen provider-, radar-, station-, model-,
   lightning-, GeoJSON-, calibration- of sciencepolls.
2. HA doet geen meteorologische SQLite-, decompressie-, raster-, object- of
   fusionbewerking; het verwerkt uitsluitend desired state, een begrensde
   workerheartbeat en een immutable operationele snapshot.
3. Worker-only en legacy kunnen nooit tegelijk eigenaar zijn; ontbrekende of
   ongeldige runtime mode faalt gesloten.
4. De worker plant, verwerkt en publiceert zelf; een operationele snapshot wordt
   pas als gepubliceerd beschouwd na echte MQTT QoS1 PUBACK.
5. Snapshotexpiry, workeruitval, corrupte payload en authoritywissel geven in HA
   alleen `unknown/stale`, nooit lokale meteorologische fallback.
6. Storm-, track-, ground- en kaartpayloads zijn hard begrensd; geen HA-entiteit
   overschrijdt de recorderlimiet van 16.384 attributenbytes.
7. Een representatieve co-locatiesoak bewijst dat HA en Zigbee2MQTT bereikbaar
   en responsief blijven tijdens providerdownload, decode, tracking, ground
   fusion, snapshotpublicatie, retry, workerrestart en backlogverwerking.
8. De soak bevat expliciete metingen van HA-eventloopvertraging, MQTT-PUBACK,
   Z2M-latency, CPU, RSS, temperatuur en poort-8123-bereikbaarheid, met een
   voor de test vastgelegde idle-baseline.
9. Een failure in iedere workerstage beïnvloedt alleen die stage en kan nooit
   opnieuw rekenwerk naar HA verplaatsen.
10. Deploy, migratie en rollback worden pas na een afzonderlijke eindvalidatie
    uitgevoerd volgens `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`.

## Niet bewezen door dit incident

Deze waarneming bewijst niet welke individuele provider of decoder het grootste
aandeel CPU of geheugen gebruikte. Zij bewijst wel voldoende dat het volledige
legacy uitvoeringsmodel ongeschikt is: één radarstage kon HA globaal blokkeren
terwijl de workerqueue leeg was. Verdere optimalisatie van dat legacy pad is
daarom geen releaseoplossing; de oplossing is de reeds geplande worker-only
cutover.
