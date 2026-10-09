# Acquisition query authority

Status: worker-side shadowcontract; niet live gewired en niet meteorologisch
toegelaten.

## Probleem dat dit contract sluit

Het acquisitieplan bevat bewust alleen content-addressed regio- en target-ID's.
Locatiegebonden providers hebben echter coördinaten nodig. Wanneer een handler
zelf coördinaten uit een losse payload zou aanvaarden, kan die buiten de actuele
Home Assistant-targets opvragen en bestaat er geen bewijs dat de opgehaalde
bytes bij de actuele desired state horen.

`storm_tracker_worker/acquisition_query_authority.py` accepteert daarom exact:

1. een canonieke `DesiredState`;
2. het daaruit deterministisch herbouwbare `AcquisitionPlan`;
3. één volledig gevalideerde cadence-slot-`AcquisitionJob` uit dat plan.

Alle drie worden opnieuw opgebouwd en onderling vergeleken. Alleen daarna
worden queryrecords afgeleid. Een vervalste in-memory state, gewijzigde regio,
job uit een ander plan of job uit een ander slot faalt gesloten.

## Privacy en identiteit

- Displaynamen en onbewerkte target-ID's staan niet in de authoritybytes,
  providerpayloads of diagnostiek.
- Targetreferenties zijn aan de actuele desired-state-SHA gebonden.
- `query_key` gebruikt een gechunkte Base32-code van zijn inhoudchecksum. De
  aparte native job-ID wordt uitsluitend door de publieke pure functie
  `safe_native_job_id()` uit de volledige canonieke `acquisition:<64hex>`-
  oorsprong afgeleid. Beide gebruiken een letterlijke niet-hex scheiding
  tussen de chunks. Daardoor kan zelfs Synoptics separator-verwijderende
  secretscanner nooit een 32-hex token reconstrueren. De oorspronkelijke
  acquisition-job blijft afzonderlijk in de authority gebonden.
- De volledige authority bevat noodzakelijke coördinaten, maar diagnostiek bevat
  uitsluitend hashes en begrensde aantallen.
- Credentialwaarden maken geen deel uit van dit contract en blijven uitsluitend
  in de workeromgeving.

## Providersemantiek

### RainViewer

De payload wordt alleen voor `radar.rainviewer.visual` afgegeven. Exact één
planregio wordt omgezet naar de bestaande native vorm
`regions[{center_lat, center_lon, horizon_km}]`. Web-Mercator- en 1.000-km-
grenzen worden vóór de handler gecontroleerd. De horizon is targetdekking, geen
bewijs dat radar op iedere pixel beschikbaar is.

### Netatmo en Synoptic

De benodigde grondradius is niet de veel grotere radarhorizon. Zij is exact de
grootste afstand van het regiocentrum naar een target plus de ingestelde
`ground_radius_km` en `location_accuracy_m`, naar buiten afgerond. Dezelfde
locatieonzekerheid wordt in de RainViewerhorizon verwerkt. Daardoor kan een
gedeelde query alleen worden gebruikt wanneer hij ieder target werkelijk dekt;
ontbrekende precisie wordt nooit stil als dekking voorgesteld. Een `person` of
`device_tracker` zonder `location_accuracy_m` faalt gesloten; alleen een
statisch geconfigureerd of exact fictief testpunt mag `null` als exacte
configuratiepositie gebruiken.

- Netatmo faalt boven 20 km gesloten.
- Synoptic faalt boven 50 km gesloten.
- Synoptic projecteert het centrum exact op de effectieve vijf-decimale
  wirecoördinaat en telt de geodetische centrumverschuiving vóór outward
  rounding bij de radius. De wirequery kan de geautoriseerde cirkel dus niet
  door afronding verkleinen.
- Een regio die deze grens overschrijdt moet in provider-specifieke subjobs
  worden opgesplitst; het contract verkleint de radius nooit stil en kiest geen
  willekeurig target.
- `local` wordt alleen gebruikt wanneer alle targets statisch zijn; zodra een
  persoon of device tracker deelneemt is de query `roaming`.

### Open-Meteo

Het shadowcontract levert een begrensde batch van ieder exact geautoriseerd
targetpunt met een pseudonieme query-key. Open-Meteo blijft modelbegeleiding,
geen radar- of ground-truthbron. Er bestaat nog geen admitted worker-native
handler; dit batchcontract is daarom nog geen activatie.

Een volledige batch wordt na opbouw en opnieuw vóór diagnostiek gevalideerd.
Verwijderen, herschikken, dupliceren of wijzigen van authorities/blocks verandert
de batchchecksum of verbreekt de state/plan/tijd/jobbinding en faalt gesloten.
Een verlopen of toekomstig mobiel target levert alleen een deterministisch
block voor zijn eigen regiojobs; verse onafhankelijke regio's blijven beschikbaar.

De volledige dormant activation/child/recoverybatch en haar resterende
promotion gates staan in `docs/ACQUISITION_LOCATION_SHADOW_BATCH.md`.

## Bewijs

Gerichte en aangrenzende teststand op 2026-08-09:

- 27/27 authoritytests groen met warnings-as-errors;
- 16/16 geïsoleerde location-shadow activation/child/recoverytests groen;
- 310/310 gecombineerd groen voor authority, acquisitionplan en de bestaande
  RainViewer-, Netatmo- en Synoptic-native captures;
- `py_compile` en `git diff --check` groen.

Gedekt zijn determinisme, plan/job/state-tamper, providergrenzen, volledige
multi-targetdekking, veilige native ID's, batchtamper en regiogebonden
freshness, native payloadcompatibiliteit, uitputtende Synoptic-radiusafronding,
RainViewer/Netatmo-geometriematrices, privacy, locatieverandering, verkeerde
workload en inert/no-live-wiring.

## Resterende gates

1. Provider-specifieke ground-subjobfanout met vooraf gereserveerde totale
   byte-/artifact-/wall-capaciteit voor regio's groter dan 20/50 km.
2. Een exact worker-native Open-Meteo raw capture- en semantiekcontract.
3. Koppeling via de centrale acquisition/recoveryauthority; niet rechtstreeks
   vanuit Home Assistant of een providerhandler.
4. Crash/restart, langdurige retentie en NUC/HA/Z2M-soak na de uiteindelijke
   wiring.
5. Meteorologische admission blijft onafhankelijk van query-autorisatie.
