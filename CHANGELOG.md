# Storm Tracker V3 — Versiegeschiedenis

## integration 1.14.144 / worker 1.18.57

- Gebruik de actuele regionale radarwaarde op de exacte doelpositie voor de
  huidige regenstatus. Daardoor kan de kaart niet langer regen tonen terwijl
  dezelfde locatie uitsluitend door een droge modelpuntschatting als `droog`
  wordt weergegeven.
- Behoud regionale radarprovenance en producttijd zonder de onderliggende
  observaties opnieuw per doelpersoon op te slaan.

## integration 1.14.144 / worker 1.18.56

- Bewaar iedere algemene bronobservatie en iedere fysieke stationsmeting nog
  maar één keer, onafhankelijk van het aantal gevolgde personen.
- Koppel brongegevens aan de actieve waarschuwingsregio en bewaar de actuele
  doelpersoon-regiotoewijzing met de exacte positie voor afzonderlijke
  aankomst-, vertrek- en intensiteitsvoorspellingen.
- Begrens operationele PostGIS-historiek tot zes maanden zonder de
  stationscatalogus, sensormogelijkheden of importerprogressie te verwijderen.

## integration 1.14.141 / worker 1.18.52

- Scheid de 70 km-waarschuwingshorizon van het radaranalysebereik. Regionale
  radarvelden worden voortaan tot 250 km rond iedere gevolgde locatie
  geprojecteerd en gefuseerd, terwijl waarschuwingen hun bestaande horizon
  behouden.
- Houd het grotere raster begrensd tot maximaal 250.000 analysepixels en laat
  directe unit-testregio's zonder expliciete radardekking hun bestaande
  lokale straal behouden.

## integration 1.14.141 / worker 1.18.51

- Behoud de door de dunne worker gepubliceerde regionale regenrasters in het
  beveiligde kaartendpoint. De integratie voegt eventuele legacy-overlays nog
  samen, maar overschrijft actuele workerrasters niet langer met een lege map.

## integration 1.14.140 / worker 1.18.51

- Houd de laatst voltooide regionale rasterlaag zichtbaar terwijl een nieuwer
  providerframe wordt gefuseerd en vervang haar pas atomair na voltooiing.
  Daardoor verdwijnt de regenlaag niet meer tussen snelle worker-cycli.

## integration 1.14.140 / worker 1.18.50

- Laat een reeds gestarte regionale fusiejob altijd voltooien wanneer tijdens
  de berekening nieuwere providerframes arriveren. Daardoor kan de snelle
  2-secondenruntime de fusie niet meer permanent verhongeren; na voltooiing
  wordt in de volgende cyclus vanzelf het nieuwste inputframe verwerkt.

## integration 1.14.140 / worker 1.18.49

- Publiceert naast de herkende buicellen ook de volledige gefuseerde regionale
  regenlaag vanaf 0,1 mm/u, zodat lichte neerslag en de werkelijke omvang van
  regenbanden zichtbaar blijven zonder de wetenschappelijke celdrempels te
  verlagen.
- Begrens de displaylaag adaptief tot een vast totaal aantal rechthoekruns;
  gefragmenteerde rasters worden uitsluitend voor weergave met max-pooling
  gecomprimeerd en blijven binnen het bestaande GeoJSON-transportbudget.

## integration 1.14.140 / worker 1.18.48

- Projecteert geografisch toepasselijke kwantitatieve radarvelden conservatief
  naar één stabiel lokaal metrisch raster per waarschuwingsregio en fuseert per
  pixel deterministisch maximaal de drie beste actuele bronnen.
- Houdt de zware fusie buiten de Home Assistant-eventloop op maximaal twee
  rekenthreads; zolang fusie loopt of faalt blijft de bestaande per-provider
  tracking als expliciet gemarkeerde fallback beschikbaar.
- Publiceert en bewaart uitsluitend compacte regionale fusieprovenance in de
  operationele snapshot, GeoJSON-metadata en PostGIS; rasterwaarden en maskers
  blijven uit de control-plane en database.

## integration 1.14.129 / worker 1.18.27

- Splits radarcontouren waar een binnenring de buitenrand in exact één
  rasterhoek raakt. OPERA-, KNMI- en DWD-cellen blijven daardoor geldige
  PostGIS-geometrieën zonder hun werkelijke buivorm tot een rechthoek te
  vereenvoudigen.

## integration 1.14.129 / worker 1.18.26

- Voltooit de config-entry-cleanup met de ontvangen workerkaart, kaartstatus
  en eenmalige ontvangstmarker. Ook na een reeds ontvangen live kaart kan de
  integratie daardoor volledig unloaden en opnieuw laden.

## integration 1.14.128 / worker 1.18.26

- Ruim bij een config-entry-unload ook de compacte passage- en
  validatiecaches plus de entry-lokale HTTP-marker op. Een normale
  integratie-reload kan daardoor niet meer ten onrechte falen met
  `globale runtime is bezet`.

## integration 1.14.127 / worker 1.18.26

- Houd de eenmalig opgebouwde offline stationscatalogus blijvend bruikbaar.
  Alleen expliciet niet-operationele stations worden uitgesloten; de
  versheidscontrole van live meetwaarden blijft afzonderlijk streng.

## integration 1.14.126 / worker 1.18.25

- Accepteer Life360-doelen zonder gerapporteerde locatie-accuraatheid; de dunne
  cyclus gebruikt dan een neutrale buffer van 0 km in plaats van te falen.

- Voegt een hervatbare offline catalogus toe voor de volledige officiële
  METAR-stationset, begrensde Netatmo-tegels en bevestigde Wunderground-PWS'en.
- Selecteert live per gedeelde regio alleen het kleinste bruikbare lokale
  stationset en dedupliceert iedere fysieke stationpoll.
- Voorspelt de volledige radarvoetafdruk met afzonderlijke binnenkomst- en
  vertrektijden; de oude middelpuntlijn blijft alleen expliciete fallback.
- Valideert passages met Netatmo, METAR en Wunderground en bewaart hit, miss,
  vals alarm, te vroeg en te laat idempotent in PostGIS.
- Levert handmatige offline replay/calibratie zonder provider- of
  netwerkaanroepen en publiceert compacte passages/validatietellers via MQTT.

## integration 1.14.124 / worker 1.18.23

- Made the thin runtime import boundary real instead of test-order dependent:
  its radar tracker now constructs the bounded CellTrack policy locally and no
  longer imports worker-only, RAW or acquisition infrastructure.
- Batch all normalized observations of one queued PostGIS write into one
  `executemany` call, retaining the same idempotent upsert keys while avoiding
  hundreds of individual cursor roundtrips.

## integration 1.14.123 / worker 1.18.22

- Start de geldigheidsduur van een operationele snapshot pas wanneer de
  volledige cyclus, inclusief PostGIS-projectie, klaar is voor publicatie.
  Een zware eerste projectie kan daardoor geen reeds verlopen snapshot meer
  naar Home Assistant sturen.

- Groepeer de vaste thuislocatie en nabije Life360-personen in één stabiele
  thuisregio met afzonderlijke toetreed- en uittreedgrenzen.
- Gebruik complete-link clustering voor externe personen en verplaats een
  extern regiocentrum pas na 15 km, zodat kleine GPS-bewegingen geen nieuwe
  provideropvragen veroorzaken.
- Behoud een tijdelijk onbeschikbare Life360-locatie maximaal vijftien minuten
  als expliciet stale en verwijder haar daarna automatisch.
- Laat gedeelde targets één lokale Netatmo-bounding box gebruiken, begrens
  grondmetingen tot de lokale grondradius en dedupliceer iedere fysieke meting
  per target.
- Gebruik dezelfde regio-identiteit in providerrequests, snapshots en de kaart
  en verwerp ruimtelijke weerdata buiten alle actieve waarschuwingsregio's.

## integration 1.14.109 / worker 1.17.125

- Projecteer de al begrensde celtrackingdiagnostiek van de worker veilig naar
  de Home Assistant-workerstatus.
- Toon op Monitoring hoeveel stormcellen actief, gevolgd en bewegingsklaar
  zijn en hoeveel daarvan een aankomstkandidaat voor een doel opleveren.

## integration 1.14.108 / worker 1.17.125

- Verhelp de koude-startrace waarbij Home Assistant zijn eerste desired state
  net na de eerste lege providercyclus publiceert: de worker haalt dan meteen
  de inmiddels due bronnen op vóór de zware herprojectie van bewaarde data.
- Behoud gecertificeerde radarregen of reflectiviteit wanneer alleen een
  optionele motion-/ETA-kandidaat inconsistent blijkt; uitsluitend die
  verrijking wordt dan fail-closed geweigerd en apart geteld.

## integration 1.14.108 / worker 1.17.124

- Verwerk unieke kwantitatieve brondecodes begrensd met twee parallelle
  CPU-lanes in plaats van volledig sequentieel.
- Laat een recoverybatch dezelfde provider nooit dubbel tegelijk decoderen;
  alleen diens nieuwste terminale capture wordt verwerkt.
- Verkort zo de fase tussen raw acquisitie en de volgende rijke snapshot,
  zonder de afgesproken limiet van twee zware rekenthreads te overschrijden.

## integration 1.14.108 / worker 1.17.123

- Houd tijdens een Life360-livenesswissel niet alleen de geldigheid, maar ook
  de laatste rijke provider- en meetprojectie vast wanneer de worker eerst een
  exacte tijdelijke `awaiting_meteorological_evidence`-fallback publiceert.
- Accepteer de eerstvolgende rijke snapshot onder de nieuwe authority meteen;
  de bewaarde vorige snapshot blijft ondertussen gewoon aan zijn bestaande
  harde TTL gebonden.
- Beperk deze uitzondering tot het exact gevalideerde lege handoffcontract;
  echte locatie- en instellingswijzigingen blijven onmiddellijk fail-closed.

## integration 1.14.107 / worker 1.17.123

- Behoud een nog geldige operationele weersnapshot tijdens een zuivere
  Life360-livenessvernieuwing, ook wanneer die vernieuwing midden in een
  blokkerende providerbatch aankomt.
- Laat de bestaande snapshot daarbij gewoon op zijn oorspronkelijke
  geldigheidsdeadline verlopen en accepteer nieuwe workerresultaten pas onder
  de nieuw bevestigde desired-state-authoriteit.
- Behoud fail-closed gedrag voor echte locatie-, beschikbaarheids- en
  instellingswijzigingen.

## integration 1.14.106 / worker 1.17.123

- Publiceer een nieuwe Life360/desired-state-authoriteit vóór de zware
  providerbatch start, zodat een blokkerende decoder de veilige herbinding
  niet meer minutenlang kan uitstellen.
- Hergebruik bij een zuivere livenessvernieuwing de bestaande exacte radar-,
  model- en grondfusie; een echte locatie- of instellingswijziging publiceert
  eerst een fail-closed onbekende toestand en wordt daarna opnieuw berekend.
- Behoud de bestaande periodieke snapshotverversing tijdens asynchrone zware
  verwerking als tweede veiligheidslaag.

## integration 1.14.106 / worker 1.17.122

- Plan na een vroege, semantisch gelijke Life360-update automatisch één
  desired-state-renewal op de tienminutengrens, ook wanneer geen tweede
  tracker-event exact op die grens aankomt.
- Bewaar daarbij uitsluitend de nieuwste livenessvelden; de bestaande
  locatiegeometrie en rijke radar-, model- en grondfusie blijven behouden.
- Annuleer of herplan de timer bij unload, echte semantische wijziging en een
  nieuwe geslaagde apply, zodat geen renewalbacklog kan ontstaan.

## integration 1.14.105 / worker 1.17.121

- Hergebruik de reeds berekende radar-, model- en grondfusie wanneer een
  periodieke Life360-lease alleen observatietijden vernieuwt en de volledige
  samplinggeometrie en instellingen exact gelijk blijven.
- Publiceer de rijke resultaten daardoor meteen opnieuw onder de nieuwe
  desired-state-authority, in plaats van iedere tien minuten alle behouden
  kwantitatieve captures opnieuw te decoderen.
- Behoud de veiligheidsgrens: een echte locatie-, beschikbaarheids-,
  dekkings- of instellingswijziging start nog altijd een volledige,
  fail-closed herberekening.

## integration 1.14.104 / worker 1.17.120

- Publiceert een reeds gecertificeerde, voor een doel relevante radartrack nu
  ook als operationele stormcel in `ui_storms`, naast de bestaande ETA.
- Geeft per gepubliceerde cel de echte metrische oppervlakte, snelheid,
  bewegingsrichting, producttijd en regio door; niet-gekalibreerde kans en
  reflectiviteit blijven bewust leeg.
- Bindt de extra celkenmerken aan hetzelfde bron-, certificaat- en
  procesbewijs als de toegelaten ETA en houdt lagekwaliteitsfragmenten
  fail-closed buiten de resultaten.

## integration 1.14.103 / worker 1.17.119

- Verlaagt de productie-RAW-admissionclaim van 80 GiB naar 48 GiB. De
  onveranderde 128 GiB fysieke schema-limiet blijft bestaande data leesbaar
  houden, terwijl de volledige cadence-, Blitzortung- en HA-reserve nu binnen
  de werkelijk vrije ruimte van de 160 GB NAS-HA VM past.
- Migreert bestaande 80 GiB- en oudere 96 GiB-bootstrapconfiguraties exact en
  atomair zonder de historische RAW-catalogus opnieuw te auditen.
- Voegt regressiedekking toe voor de live NAS-HA-opslagenvelop en beide veilige
  migratiepaden.

## integration 1.14.102 / worker 1.17.118

- Verwijdert de tweede lineaire koude-startcontrole: reeds duurzaam
  gecommitteerde captures worden bij herstel uitsluitend aan hun gevalideerde
  catalogusmanifest en exacte capacity-commit gekoppeld, zonder alle
  historische providerbestanden opnieuw te lezen en hash'en.
- Behoudt volledige fail-closed recovery voor onafgewerkte transacties en
  verplichte SHA-256-verificatie bij iedere echte payloadread of verwerking.
- Voegt een regressiepoort toe die een payloadread tijdens het herstel van een
  reeds gecommitteerde capture expliciet laat falen.

## integration 1.14.101 / worker 1.17.117

- Vervangt de lineaire byte-voor-byte hercontrole van de volledige historische
  raw-store bij iedere workerstart door een begrensde catalogus-, SQLite- en
  capaciteitsaudit. Daardoor blijft de koude start nagenoeg onafhankelijk van
  de omvang van de bewaarde raw-corpus.
- Behoudt de inhoudsintegriteit fail-closed: iedere raw-payload wordt nog altijd
  met zijn opgeslagen SHA-256 gecontroleerd voordat verwerking of replay hem
  mag gebruiken.
- Verhardt de release-rollback tegen onmiddellijke inode-hergebruik op Linux:
  een gepubliceerd artefact wordt alleen verwijderd wanneer zowel identiteit
  als inhoudshash nog exact overeenkomen.

## integration 1.14.100 / worker 1.17.116

- Splitst providergereedheid van de effectieve status op de huidige
  trackerlocatie. Een bron met een historisch bewezen, volledige capture blijft
  `Ready`, ook wanneer ze nu buiten geografisch bereik, verouderd of tijdelijk
  defect is.
- Herstelt `Ready` na een herstart vanuit de payloadvrije kalibratie-index en
  markeert de directe Blitzortung-ingress ready zodra zijn live MQTT-transport
  bewezen actief is; een stille bliksemperiode wordt niet als storing gezien.
- Activeert de al aanwezige directe Blitzortung-sidecar uitsluitend bij de
  exacte expliciete raw-retentieauthority uit de workeroptie.
- Toont op het monitoringdashboard per bron `Ready/Nog niet ready` gevolgd door
  de actuele gekleurde status: groen, geel, oranje, rood of blauw.

## integration 1.14.99 / worker 1.17.115

- Zet het nog niet beschikbare native KMI RADQPE-product bewust op
  `api_required`; het blijft niet-dispatchbaar, telt niet meer als storing in
  het operationele overzicht en kan later met gerichte API-authority worden
  geactiveerd.
- Laat de bestaande rechtstreekse regionale Blitzortung MQTT-ingang zijn live
  readiness in de providerstatus projecteren. Een verbonden raw-ingang wordt
  operationeel getoond zonder stilte als afwezigheid van bliksem te
  interpreteren en zonder fictieve weerdata te maken.
- Toon `api_required` afzonderlijk in het monitoringdashboard.

## integration 1.14.98 / worker 1.17.110

- Herstel de live Open-Meteo-verwerkingsketen door de strikte decoder te
  verbinden met het werkelijk actieve public-source capturecontract en de rol
  `response_bytes`; de oudere, niet-live capturevorm blijft replaybaar.
- Controleer vóór iedere modeluitkomst opnieuw provider, product, rol,
  transportprovenance, queryaantal, request-/transport-/contentidentiteit en
  daarna het volledige gesloten Open-Meteo-responsschema.
- Log bij een geweigerde modelcapture uitsluitend foutklasse en een vaste
  veilige redencode, zodat een volgende contractafwijking diagnoseerbaar is
  zonder coördinaten of vrije providertekst te lekken.
- Behoud op de dedicated NAS expliciet full-parallel acquisitie voor alle
  werkelijk due bronnen; CPU-zware verwerking blijft tot twee rekenthreads
  begrensd.
- Behoud raw-only verzamelen wanneer modelbegeleiding uit staat, maar laat
  die data dan nooit doorstromen naar operationele neerslagclaims.

## integration 1.14.98 / worker 1.17.107

- Ondersteun de actuele Open-Meteo-meerpuntsrespons waarin antwoord twee en
  verder een exact oplopende `location_id` bevatten.
- Behoud replaycompatibiliteit met historische responses zonder
  `location_id`; gemengde, dubbele of verkeerd geordende indexen blijven
  fail-closed.
- Valideer de volledige achtlocatieketen live van publieke HTTP-capture tot
  acht targetgebonden modeluitkomsten.

## integration 1.14.98 / worker 1.17.106

- Accepteer voor Open-Meteo `lifted_index` exact beide bekende
  dimensieloze eenheidsmarkeringen: de historische waarde `undefined` en de
  sinds september 2026 live teruggegeven lege waarde.
- Houd alle andere hourly-eenheden gesloten en ongewijzigd zodat een echte
  semantische API-afwijking nog steeds fail-closed wordt geweigerd.
- Behoud daarmee zowel de replaybaarheid van historische captures als de
  acquisitie en verwerking van nieuwe Open-Meteo-data.

## integration 1.14.98 / worker 1.17.105

- Verwerk de reeds opgeslagen, exacte Open-Meteo-respons na een tweede
  volledige schemacontrole tot targetgebonden modelresultaten.
- Publiceer positieve actuele modelneerslag als geschatte mm/u en een positief
  90-minutensignaal als `neerslag mogelijk over X min`, met expliciete
  Open-Meteo-provenance en zonder extra provideroproep.
- Behoud nulwaarden fail-closed: modelmatig geen neerslag wordt zichtbaar als
  begeleiding, maar nooit als onafhankelijk bewezen droogte.
- Bind iedere modelcapture aan exact dezelfde desired-state zodat een
  Life360- of fictieve-trackerverplaatsing nooit oude puntdata op de nieuwe
  locatie kan projecteren.

## integration 1.14.91 / worker 1.17.97

- Begrens `dedicated_nas_fast` hard tot twee geërfde logische CPU's. Wanneer
  die kernel-affinity al gelijk aan de ingestelde 200%-grens is, wordt de
  defectgevoelige userspace-`cpulimit --include-children` niet meer gestart.
  Alle workerchildren erven hetzelfde kernelplafond; de overige VM-CPU's
  blijven buiten de worker-affinity.
- Voorkom daarmee de live waargenomen `process_group.c`-race waardoor
  `cpulimit` stopte en de worker terecht maar onnodig fail-closed eindigde.
- Breid de geografische plannertest uit tot Brussel, Amsterdam, Parijs,
  Rostock, Manchester, Madrid, Rome en Chicago. De selectie volgt aantoonbaar
  het werkelijke providerdekkingsvlak, ook waar dat landsgrenzen overlapt.

## integration 1.14.86 / worker 1.17.92

- Behandel de volledige PostGIS-status `waiting` als een geldige, niet-kritieke
  projectieachterstand. Een tijdelijk onbereikbare database of uitgestelde
  retry sluit de worker-admission niet langer ten onrechte fail-closed.
- Voeg een regressietest toe voor precies de live combinatie van een bewaarde
  projectieachterstand, foutcode en volledige projectiediagnostiek.

## integration 1.14.85 / worker 1.17.91

- Start nieuwe actuele bronacquisitie in de geïsoleerde `source-v2`-epoch zodat
  een groeiende historische raw-catalogus de workerstart niet langer blokkeert.
  De vorige `source`-epoch en alle PostGIS-history blijven onaangeroerd bewaard.
- Migreer uitsluitend de exacte gekende vorige productieconfiguratie naar deze
  epoch; afwijkende of gemanipuleerde configuraties blijven fail-closed.

## integration 1.14.84 / worker 1.17.90

- Verplaatst de potentieel lange controle van de bewaarde rawhistoriek naar de
  service-opstart ná de MQTT-verbinding. De worker publiceert daardoor meteen
  zijn actuele versie en herstelstatus, terwijl jobtoelating strikt gesloten
  blijft totdat de volledige integriteitscontrole geslaagd is.
- Geeft een volledige parallelle providerbatch 615 seconden voor providerwerk,
  procesafbouw en de opslagcontroles samen. Elke afzonderlijke provider blijft
  hard begrensd op maximaal 300 seconden en CPU-zware verwerking op twee
  rekenthreads.
- Migreert uitsluitend de exacte vorige 315-secondenconfiguratie atomair naar
  de nieuwe grens, zonder de reeds gevalideerde pipelinehistoriek tijdens de
  bootstrap nogmaals te auditen.

## integration 1.14.83 / worker 1.17.89

- Hergebruikt bij een gewone workerherstart de reeds onafhankelijk gepinde en
  gevalideerde worker-onlyconfiguratie en autoriteit.
- Voert de volledige cryptografische historie-audit alleen nog uit bij de
  eerste materialisatie of een exacte configuratiemigratie. Daardoor groeit
  de herstarttijd niet langer mee met de bewaarde kalibratiehistoriek en kan
  de worker meteen zijn heartbeat bereiken.
- Blijft fail-closed bij configuratiedrift, een gewijzigde autoriteit of een
  afwijkende store-, lock- of authoritylocatie.

## integration 1.14.82 / worker 1.17.88

- Bindt de bestaande, bevestigde radarceltracks aan de actuele
  doelobservaties en publiceert hun vroegste doelkruising als een mogelijke
  neerslagdreiging met ETA.
- Houdt die radar-ETA bewust ongekalibreerd (`possible`, zonder
  kanspercentage). De sterkere claims `expected`, `none` en een gekalibreerde
  kans blijven achter hun onafhankelijke validatiepoorten.
- Laat lage-betrouwbaarheidsfragmenten, verouderde tracks en tracks zonder
  exact overeenkomend radarreceipt fail-closed op `unknown` staan.

## integration 1.14.82 / worker 1.17.87

- Publiceert na een Life360- of testtrackerwijziging onmiddellijk een veilige
  snapshot die aan de nieuwe locatieautoriteit gebonden is, vóór de worker de
  bewaarde grond- en radarhistoriek opnieuw verwerkt.
- Voorkomt zo dat alle dashboardbronnen tijdens een tragere herberekening op
  `desired_state_transition` en `onbekend` blijven staan; de verdere
  herberekening blijft op de begrensde achtergrondpaden doorlopen.

## integration 1.14.82 / worker 1.17.86

- Bind OPERA CIRRUS acquisition to the previous fully closed five-minute
  product slot while retaining the current scheduler job identity. This
  removes the publication-lag 404 race and preserves exactly-once receipts.
- Validate the OPERA slot binding across a UTC day boundary and against the
  live public object store; the real HDF5 capture now commits successfully.

## integration 1.14.81 / worker 1.17.85

- Herstelt de actuele publieke KMI app-v4-route: lege maar geldige standaard-
  responseheaders worden behouden en redirects blijven beperkt tot de exacte
  hosts `app.meteo.be` en `cdn.meteo.be`.
- Behandelt een inconsistente kwantitatieve multi-radarset als ingehouden
  verrijking in plaats van als fatale workerfout. De betrokken bronclaims
  leveren geen meteorologische waarheid, terwijl acquisitie, RAW-opslag,
  PostGIS en operationele snapshots blijven doorlopen.
- Live bewezen op NAS-HA met KMI app-v4 `operational`, RAW/PostGIS 672/672,
  snapshot 1737 `current`, 22 voltooide jobs, queue 0 en algemene
  worker-runtimefailures 0.

## integration 1.14.76 / worker 1.17.80

- Maakt de 26 full-parallel providerlanes werkelijk parallel: I/O-gebonden
  exact-raw children gebruiken niet langer het historische globale
  singletonslot dat de NUC moest beschermen. Per workload blijft maximaal één
  request actief en elke child behoudt zijn eigen harde CPU-, geheugen- en
  tijdgrens.
- Houdt CPU-zware decode en bronfusie op hun afzonderlijke begrensde pad. Het
  korte spawnvenster wordt bovendien geserialiseerd, zodat gelijktijdige
  providerchildren nooit elkaars gesloten environment of credentials kunnen
  erven.
- Corrigeert Open-Meteo multi-target requests door voor iedere coördinaat een
  eigen `elevation=nan` te sturen. De provider accepteert de exacte batch nu
  met HTTP 200 in plaats van ze tot aan de schedulerdeadline te weigeren.
- Herbindt de gesloten raw-releasevalidator aan de exacte, reeds in worker
  `1.17.79` uitgeleverde service-, compositie- en retention-guardbronnen. De
  bronpins, materializerpin en afgeleide fixture zijn opnieuw als één
  consistente fail-closed set bewezen.

## integration 1.14.75 / worker 1.17.79

- Isoleert een gelijktijdige, herstelbare desired-statewissel tot de betrokken
  full-parallel providerlane. Een nieuwe locatie- of configuratiehead kan de
  oude planning daardoor veilig vervangen zonder de volledige batch of worker
  af te breken.
- Onherstelbare desired-state-inconsistentie blijft strikt fail-closed; alleen
  de drie reeds toegestane tijdelijke toestanden (`changed_during_plan`,
  `future` en `stale`) gebruiken het herstelpad.
- Behoudt full-parallel provider-I/O en maximaal twee CPU-zware rekenthreads.

## integration 1.14.74 / worker 1.17.78

- Geeft in full-parallel NAS-modus ieder product opnieuw zijn volledige,
  productspecifieke harde I/O-deadline. De oude korte probe van 20 seconden
  beschermde vroeger één seriële NUC-lane, maar hield nu legitieme DWD-, KMI-
  en andere 45–90 secondenbronnen onnodig tegen.
- Behoudt alle harde byte-, artifact-, proces- en productspecifieke
  tijdslimieten en de limiet van twee CPU-zware rekenthreads.
- Registreert bij een onverwachte batchfout uitsluitend een gesloten
  foutcategorie en de exceptionklasse; exceptiontekst, providerpayload en
  credentials blijven buiten log en dashboard.

## integration 1.14.73 / worker 1.17.77

- Isoleert een gelijktijdig schedulerconflict of een tijdelijke
  scheduler-klokafwijking tot de ene betrokken providerlane. De durable lease
  blijft fail-closed en wordt via het bestaande herstelpad opnieuw geprobeerd;
  de overige full-parallel providers blijven doorlopen en de complete worker
  wordt hiervoor niet meer herstart.
- Publiceert voor zo'n geïsoleerde lane uitsluitend een vaste technische
  reden (`scheduler_conflict` of `scheduler_clock`) en nooit exceptiontekst,
  providerpayload of credentials.
- Beschermt de bestaande tweedeling `Resultaten` / `Monitoring` in de
  dashboard-updater. Een volgende gerichte dashboardupdate kan daardoor niet
  meer ongemerkt de live tweevoudige indeling vervangen door de oude ene view.

## integration 1.14.72 / worker 1.17.76

- Publiceert de begrensde, payloadvrije full-parallel acquisitiediagnostiek via
  de bestaande workerstatus: modus, effectieve lanegrens, laatste batchgrootte,
  batchduur en eventuele vaste ongezondheidsredenen.
- Toont die toestand rechtstreeks in `Systeem en verwerking`, zodat live
  zichtbaar is of de worker full-parallel ophaalt en hoeveel providerjobs de
  laatste batch werkelijk bevatte.
- Behoudt de in 1.17.75 herstelde batchconsumptie; KMI-WCS en DWD groeiden in
  dezelfde live parallelle batch van 304 naar 306 RAW/PostGIS-captures, met
  queue 0 en nul mislukte workerjobs.

## integration 1.14.71 / worker 1.17.75

- Behoudt voortaan ieder terminaal resultaat van een full-parallel
  acquisitiebatch. De service consumeert niet langer alleen de laatst
  afgeronde providerjob, zodat alle geslaagde captures naar bronstatus,
  kalibratie-index, PostGIS en de productspecifieke verwerking doorstromen.
- Houdt de bestaande compacte `latest_tick_result` voor diagnostiek, maar
  publiceert intern daarnaast de volledige begrensde batch. De seriële
  legacyroute blijft compatibel en per workload blijft maximaal één request
  tegelijk actief.

## integration 1.14.70 / worker 1.17.74

- Heropent de lokale KMI/DWD-prioriteitscohort bij iedere adaptieve
  doorschuivende batch. Daardoor kan een volle set van vijf trage algemene
  jobs de nieuwe Belgische cadence-slots niet meer achter de deadline duwen.
- Schakelt op expliciet verzoek de vijf-lane herstelmodus uit op de geïsoleerde
  NAS: koude start, herstel en normale werking starten meteen alle werkelijk
  due workloads parallel. Per-workload exclusiviteit en maximaal twee
  CPU-zware decode-/fusiethreads blijven behouden.

## integration 1.14.69 / worker 1.17.73

- Herstelt de publieke KMI-WCS-radar die sinds de latest/default-timefix nog
  vóór HTTP werd afgewezen. De activatiematrix en childdispatcher gebruiken
  nu allebei exact het lege requestcontract; GeoServer kiest daardoor opnieuw
  zelf de laatst beschikbare radarmosaic. Een echte predeploycapture legde
  4.194.720 bytes `image/tiff` volledig vast in 1,88 seconde.
- Laat frisse providerchildprocessen alleen nog read-only aansluiten op de
  reeds door de parent bewezen RAW-catalogus. Zij herhalen niet langer per
  parallel request `BEGIN IMMEDIATE`, `integrity_check` en een volledige
  filesystemscan. Ontbrekende of conflicterende opslag blijft fail-closed en
  alleen de parent mag schema of mappen creëren en de volledige audit doen.
- Voegt een gesloten contracttest toe die iedere uitvoerbare activatieregel
  met het child-payloadcontract vergelijkt, zodat deze KMI-drift niet opnieuw
  stil kan terugkeren als generieke `decoder_failed`/`deadline_expired`.

## integration 1.14.68 / worker 1.17.72

- Herstelt crash-gepubliceerde, content-addressed RAW-objecten vóór de
  retention-audit door uitsluitend hun geverifieerde SHA-256, grootte en
  objectrecord opnieuw te catalogiseren. De bytes worden niet verwijderd en
  krijgen geen verzonnen provider-, receipt- of captureclaim; een latere
  normale providerretry kan de juiste provenance veilig koppelen.
- Laat deze startuprecovery alleen onder de exclusieve workerfence lopen en
  blokkeert fail-closed bij stagingresten, een ongeldige objectnaam, een
  hash-/groottemismatch of een mislukte nacontrole.

- Benoemt bij een RAW-storeblokkade uitsluitend de vaste, payloadvrije
  invariant-ID's. Daardoor kan de live blijvende opslagmismatch gericht worden
  hersteld zonder de fail-closed guard te versoepelen of private paden en data
  in logs op te nemen.

- Herstelt de live gevonden retention-stop na een parallelle acquisitiebatch.
  Een eenmalig veranderde opslagmeting krijgt voortaan exact één volledige,
  stabiele hercontrole; alleen een herhaald falende integriteitsproef stopt de
  worker nog fail-closed en vereist een herstart.
- Logt bij een echte retention-integriteitsfout uitsluitend de vaste
  controlefase, zonder paden, brondata of credentials, zodat een volgende
  storing direct tot ledger, RAW-store, event-ingress of filesystem kan worden
  herleid.

- Voert per adaptieve batch exact één schedulereconcile uit. Daardoor kan een
  snelle lane nooit meer de nog actieve lease van een trager providerproduct
  laten verlopen; de live gevonden `scheduler_retry_conflict` is afgedekt met
  een regressietest die bewust de leasegrens overschrijdt.

- Herstelt de live gevonden parallelle scheduler-klokrace: timestampbepaling
  en schedulertransities gebeuren nu onder dezelfde korte metadata-lock, zodat
  omgekeerd eindigende providerrequests nooit als een echte klokregressie
  worden geïnterpreteerd. Providerdownloads en verwerking blijven parallel.

- Vervangt de ene seriële acquisitielane door adaptieve, rollende parallelle
  acquisitie. Koude start en herstel gebruiken vijf lanes; na een gezonde
  batch worden alle werkelijk verschuldigde bronproducten tegelijk toegelaten.
- Valt bij deadline misses, stijgende gemiddelde looptijd, capacity/lock-busy
  of een groeiende commitachterstand onmiddellijk terug naar vijf lanes en
  schaalt pas na een gezonde cooldown van vijf minuten opnieuw op.
- Voorkomt dubbele gelijktijdige requests voor hetzelfde bronproduct, maar
  laat verschillende producten van één provider — zoals KMI-AWS en KMI-radar —
  wel parallel lopen. De bestaande Belgische prioriteitsvolgorde en fairness
  blijven ook bij sequentiële ticks behouden.
- Houdt CPU-zware decode en bronfusie onafhankelijk van de netwerkparalleliteit
  hard begrensd op twee rekenthreads en twee toegewezen CPU-cores.

## integration 1.14.62 / worker 1.17.66

- Reserveert in de seriële acquisitielane afwisselend een lokale
  prioriteitsbeurt voor KMI-AWS, DWD RADOLAN RV en de publieke KMI-WCS-radar,
  terwijl algemene bronnen via de tussenliggende beurt eerlijk blijven lopen.
- Bewijst de wijziging live op NAS-HA: worker en integratie zijn actueel,
  desired-state voor acht doelen is zonder fout toegepast, KMI-AWS werd opnieuw
  `operational` en capture 241 werd meteen als PostGIS-projectie bevestigd.
- Legt vast dat deploybackups de HA Recorder-database en de volledige
  heropbouwbare worker-runtime uitsluiten. PostGIS blijft buiten HA-backups en
  krijgt pas een eigen logische backupgate wanneer het unieke,
  niet-heropbouwbare data gaat bewaren.

## integration 1.14.60 / worker 1.17.64

- Bewaart voortaan per provider het laatste terminale acquisitieresultaat in
  de runtimeprojectie, in plaats van alleen het resultaat van de allerlaatste
  globale workerjob te tonen.
- Herstelt na een workerherstart zowel geverifieerde captures als de laatst
  bekende providerfouten. Daardoor valt een bron met een bewezen fout niet
  meer ten onrechte terug op `awaiting_first_capture`.

## integration 1.14.59 / worker 1.17.63

- Herstelt snelle worker-first snapshotpublicatie bij een gelijktijdige
  Life360/desired-state-wissel: een veilig superseded resultaat wordt nu eerst
  opnieuw gebonden aan de actuele autoriteit en daarna uit reeds bewaarde
  captures gepubliceerd, zonder provider-I/O te herhalen.
- Behoudt de begrensde fail-closedbescherming bij werkelijk eindeloze
  authority-churn en voegt een directe regressietest voor de restage-route toe.

## integration 1.14.58 / worker 1.17.62

- Versnelt uitsluitend `dedicated_nas_fast` van minimaal 30 naar minimaal
  2 seconden tussen providerjobs. Daardoor kan een volledige ronde over alle
  bronnen in minuten in plaats van ongeveer een kwartier worden afgewerkt.
  `shared_ha` en `dedicated_nas` blijven hard op minstens 30 seconden staan.
- Geeft het snelle profiel, de 200%-CPU-grens en de effectieve providercyclus
  correct door aan de integratie en de workerstatussensor.

## integration 1.14.57 / worker 1.17.61

- Voegt het expliciete, opt-in profiel `dedicated_nas_fast` toe. De worker mag
  daarin maximaal twee volledige CPU-cores gebruiken (`cpulimit 200`) met
  instelbare nice-prioriteit, terwijl de bestaande CPU-affiniteit nog altijd
  één volledige fysieke core buiten de Storm-worker houdt. De conservatieve
  profielen `shared_ha` en `dedicated_nas` blijven ongewijzigd.
- Verruimt uitsluitend voor deze NAS-grenstest de App-schemagrens tot 200;
  `provider_cycle_seconds: 30` blijft het technische acquisitieminimum.

## integration 1.14.57 / worker 1.17.60

- Geeft trackrasters een stabiele, uitsluitend geometrische identiteit. De
  inhoudsgebonden DWD-gridcertificaathash blijft volledig gevalideerd, maar
  reset de celtracker niet langer bij ieder nieuw radarframe. Daardoor kunnen
  opeenvolgende DWD-scans eindelijk dezelfde cel bevestigen en een
  bewegingsvector opbouwen.

## integration 1.14.57 / worker 1.17.59

- Herstelt voor worker-only de bewezen DWD RADOLAN RV-versheidsgrens van
  vijftien minuten. Geldige, pas opgehaalde DWD-frames worden daardoor niet
  langer vóór de kwantitatieve neerslagverwerking als verouderd verworpen.

## integration 1.14.57 / worker 1.17.58

- Corrigeert de historische AWN-ledgeridentiteit van het ongeldige `awn` naar
  de werkelijk bewaarde provider-ID `ambientweather`. Hierdoor kan de worker
  de bestaande capaciteitsdatabase opnieuw openen zonder captures te wissen;
  AWN blijft volledig buiten de uitvoerbare broncatalogus.

## 1.14.57 / worker 1.17.57

- Behoudt AWN uitsluitend als niet-uitvoerbare historische ledgercapaciteit,
  zodat bestaande auditrijen leesbaar blijven; er bestaat geen AWN-workload,
  acquisitie of dashboardentiteit meer.

## 1.14.56 / worker 1.17.56

- Migreert oude worker-only bootstrapconfiguraties uitsluitend wanneer alle
  opslag-, limiet-, retention- en authorityvelden exact gelijk blijven en
  alleen de afgeleide cataloguslidmaatschappen verschillen.

## 1.14.55 / worker 1.17.55

- Migreert bij de eerste start uitsluitend de exact herkenbare bewaarde
  worker-only bootstrapconfig van de verwijderde AWN-bron atomisch naar de
  nieuwe 28-bronnencatalogus. Onbekende of handmatig gewijzigde configuraties
  blijven fail-closed.
- Behoudt de providerprojectie- en AWN-verwijderingsfix van 1.14.54/1.17.54,
  maar voorkomt dat de geldige cataloguswijziging de Appstart blokkeert.

## 1.14.54 / worker 1.17.54

- Herstelt de gezamenlijke providerprojectie: iedere veilige workerfallback
  bevat nu de volledige gesloten catalogus als expliciet `unknown`, zodat HA
  niet langer alle bronentiteiten als `provider_snapshot_missing` presenteert.
- Herkent die complete fallback nog steeds als tijdelijk en vervangt hem na
  Life360-/desired-statewissels automatisch door de rijke, behouden
  providerstatussen zonder nieuwe provider-I/O.
- Verwijdert AWN/Ambient Weather Network volledig uit actieve workerconfig,
  acquisitie, raw routing, snapshotentiteiten en dashboardgeneratie. Bestaand
  historisch auditmateriaal wordt niet destructief verwijderd.
- De actieve catalogus telt daardoor 27 cadencebronnen plus één Blitzortung-
  ingress: 28 expliciete bronposities.

## 1.14.50 / worker 1.17.49

- Behoudt de seriële providerfairness op het stabiele workload-ID wanneer een
  bewegende Life360-tracker een nieuwe, positiegebonden desired state maakt.
  Hierdoor begint de algemene bronnenrij niet telkens opnieuw en kunnen lagere
  prioriteiten zoals de publieke KMI-WCS-bron hun eerste raw capture bereiken.
- Generieke schedulerclients zonder `workload_id` blijven exact op hun
  bestaande `intent_id` roteren. DWD houdt zijn begrensde kwantitatieve
  voorrang, maar kan de eerste capture van andere toepasselijke bronnen niet
  meer door targetbeweging laten verhongeren.

## 1.14.49 / worker 1.17.48

- Vraagt voor de publieke Belgische KMI-radarcomposiet de nieuwste werkelijk
  beschikbare WCS-dekking op. KMI publiceert die composiet met vertraging;
  een exact huidig vijfminutenslot leverde daardoor structureel HTTP 404 op.
- Bewaart de ontvangen TIFF nog steeds byte-exact met canonieke aanvraag,
  transportbewijs, inhoudshash en atomair capturemanifest. De inhoud blijft
  semantisch `unknown_unverified` totdat een afzonderlijke decoder en
  toelatingscontrole zijn bewezen.
- Borgt de geografische grens expliciet: de Noord-Amerikaanse Synoptic-bron
  wordt niet voor een Belgische tracker ingepland en wordt afzonderlijk met
  een toepasselijke Amerikaanse testlocatie gevalideerd.

## 1.14.48 / worker 1.17.47

- Behoudt het expliciet secretvrije redenveld van een mislukte workerjob in de
  HA-brug. Daardoor herkent de desired-statecontroller een gelijktijdige
  statewissel als CAS-conflict en herbouwt hij automatisch in plaats van stil
  geblokkeerd te blijven tot een workerherstart.
- Logt in de worker hetzelfde begrensde redenveld naast de foutklasse, zodat
  een toekomstige raw-integriteitsfout diagnosticeerbaar is zonder payloads,
  tokens of credentials te loggen.

## 1.14.47 / worker 1.17.46

- Houdt ook in de volledige bronverzamelmodus iedere nationale, regionale en
  satellietbron achter zijn catalogusgrens. `all_sources` schakelt alle
  toegelaten bronklassen in, maar omzeilt nooit land- of bounding-boxdekking.
- Corrigeert dezelfde grens voor globaal gededupliceerde maar geografisch
  beperkte controles, zoals de Italiaanse historische catalogusvalidatie.
- Borgt dit met een landenmatrix voor België, Spanje, Italië, de Verenigde
  Staten en het Verenigd Koninkrijk; mondiale bronnen blijven overal
  beschikbaar, terwijl niet-toepasselijke bronnen geen acquisitiejob krijgen.

## 1.14.46 / worker 1.17.45

- Publiceert een begrensde, payloadvrije inventaris van de ruwe
  kalibratie-index: bron-ID, provider, captureaantal en laatste ontvangstmoment.
  Daardoor is zichtbaar welke bronnen werkelijk exacte upstreamdata hebben
  opgeslagen, los van hun strengere meteorologische toelatingsstatus.
- Weigert ongeldige, dubbele of te grote broninventarissen aan de HA-grens en
  houdt de volledige lijst onder het bestaande attributenbudget.

## 1.14.45 / worker 1.17.44

- Laat de hoofdtoestand van de workerstatussensor de begrensde MQTT-heartbeat
  volgen. Een tijdelijk ingehouden meteorologische snapshot kan de zichtbare
  workerstatus daardoor niet meer ten onrechte op `unknown` zetten.
- Behoud de afzonderlijke snapshottoestand als `snapshot_worker_state`, zodat
  connectiviteit en productgeldigheid beide zichtbaar en uitlegbaar blijven.

## 1.14.44 / worker 1.17.44

- De workerstatussensor luistert nu ook naar verse worker-heartbeats, zodat
  bron en terminale reden onmiddellijk verschijnen zonder op een volgende
  meteorologische snapshot te wachten.

## 1.14.43 / worker 1.17.44

- Toont de laatst verwerkte workerbron en de begrensde terminalreden in de
  workerstatus, zodat bron-per-bron livevalidatie mogelijk is zonder raw
  payloads, credentials of omvangrijke add-onlogs te openen.
- Houdt de compacte Home Assistant-workerstatus onder de vaste 1 KiB-grens en
  laat lege pingvelden weg totdat er werkelijk pingbewijs beschikbaar is.

## 1.14.42 / worker 1.17.43

- corrigeert de live bewezen `retention_guard_envelope_invalid`: de guard
  behandelt 128 GiB voortaan uitsluitend als fysieke, bestaande
  storecompatibiliteit en reserveert 96 GiB als nieuwe retentieclaim;
- vereist binnen die claim nog steeds beide maximale catalogi, de volledige
  eerstvolgende providercyclus en 2 GiB beschermde hostruimte;
- bewaart alle bestaande captures en databases ongewijzigd en fail-closed.

## 1.14.41 / worker 1.17.42

- maakt ook de vaste worker-first abortcode en de streng gevalideerde
  operationele reason-code zichtbaar, zodat een fail-closed start zonder
  vrije fouttekst of credentials exact kan worden herleid;
- behoudt alle opslag- en NAS-belastingsgrenzen van 1.17.40.

## 1.14.40 / worker 1.17.41

- voegt één begrensde, geheimvrije bootstrapdiagnose toe: alleen de
  exceptionklasse en een streng gevalideerde vaste reason-code worden gelogd;
  vrije fouttekst, configuratie en credentials blijven volledig afgeschermd;
- behoudt de 128 GiB fysieke RAW-compatibiliteit, de 96 GiB retentieclaim en
  het dedicated-NAS profiel van 50% CPU, nice 10 en twee rekenthreads.

## 1.14.39 / worker 1.17.40

- behoudt de fysieke RAW-store en catalogi op hun bestaande 128 GiB-schema,
  zodat alle reeds opgeslagen providerdata byte-identiek leesbaar blijft;
- begrenst uitsluitend de nieuwe retentie- en admissionclaim tot 96 GiB. Met
  beide catalogi en 2 GiB beschermde hostruimte blijft zo ruim 30 GiB vrije
  marge op de 160 GB NAS-VM over;
- migreert zowel de oorspronkelijke 128 GiB-configuratie als de exact herkende
  mislukte 96 GiB-tussenconfiguratie atomair naar het nieuwe v5-contract,
  zonder captures, databanken of providerbytes te verplaatsen of te wissen;
- behoudt het dedicated-NAS profiel van 50% CPU, nice 10 en twee rekenthreads.

## 1.14.38 / worker 1.17.39

- tussenrelease die de fysieke RAW-capaciteit naar 96 GiB probeerde te
  migreren; de live cutover werd fail-closed geweigerd omdat de bestaande
  catalogi terecht aan hun oorspronkelijke 128 GiB-schema gebonden bleven;
- liet bestaande captures, databanken en providerbytes intact;
- behoudt het dedicated-NAS profiel van 50% CPU, nice 10 en twee rekenthreads.

## 1.14.37 / worker 1.17.38

- koppelt iedere geverifieerde providercapture aan een duurzame, idempotente
  exact-raw kalibratie-index; de grote providerbytes blijven uitsluitend in de
  content-addressed raw-store en worden niet gedupliceerd;
- toont live hoeveel raw captures, bronnen en providers al geïndexeerd zijn en
  maakt zichtbaar of de worker een volledige multi-bronsnapshot of een
  fail-closed fallback publiceert;
- behoudt het dedicated-NAS profiel van 50% CPU, nice 10 en twee rekenthreads.

## 1.14.36 / worker 1.17.37

- opent tijdens een `worker_only`-start eerst de MQTT-jobadmission en stelt de
  zware herprojectie van bewaarde METAR- en radarproducten uit tot de normale
  laag-prioritaire tick. Daardoor kan Home Assistant de gewenste toestand
  binnen zijn vaste timeout claimen, terwijl de bewaarde data daarna gewoon
  verder wordt verwerkt;
- behoudt het dedicated-NAS profiel van 50% CPU, nice 10 en twee rekenthreads.

## 1.14.35 / worker 1.17.36

- maakt de gewenste-toestandhandshake op de technische sensor begrensd en
  geheimvrij zichtbaar, zodat een NAS-startupfout exact als controllerstatus,
  foutcode en vaste tellers kan worden vastgesteld zonder polling;
- behoudt de targetgebonden METAR-projectie en het dedicated-NAS profiel van
  50% CPU, nice 10 en twee rekenthreads.

## 1.14.34 / worker 1.17.35

- behoudt iedere wereldwijde AviationWeather-METAR-capture volledig en
  byte-identiek in de ruwe workeropslag, maar projecteert alleen stations
  binnen de actuele Home Assistant-targetregio's naar canonieke groundfacts;
- negeert onbeschikbare targets en stations zonder geldige coordinaten voor
  die afgeleide projectie, zonder ze als droog of als ontbrekende bron te
  interpreteren;
- start een nieuwe, doelgebonden ground-evidencegeneratie naast de bewaarde
  volle wereldwijde database. Een echte wereldwijde replay met de Belgische en
  Duitse testregio bleef onder 4 MiB en alle 111 ground/runtime-regressietests
  slaagden;
- behoudt het dedicated-NAS profiel van 50% CPU, nice 10 en twee rekenthreads.

## 1.14.33 / worker 1.17.34

- voegt een geheimvrije live-diagnose toe voor de vaste canonieke
  ground-databankgrenzen, zodat een METAR-capaciteitsfout exact onderscheidt
  tussen batch-, rij- en fysieke databasecapaciteit;
- behoudt de dedicated-NAS rekenlimiet van 50%, nice 10 en twee rekenthreads.

## 1.14.32 / worker 1.17.33

- omzeilt de sinds eind augustus 2026 defecte CARTO-kaarttegels van Home
  Assistant 2026.8.3 door het testdashboard de eigen OSM-kaart te laten
  gebruiken. In `worker_only` valt die kaart zonder extra backendwerk veilig
  terug op `zone.home`, alle Life360-trackers en de fictieve tracker;
- toont het actieve workerprofiel, de CPU-limiet, nice-waarde en het vaste
  aantal rekenthreads op de workerstatussensor zodat het NAS-dashboard de
  werkelijke belasting kan weergeven;
- verruimt uitsluitend de tijdslimiet van de laag-prioritaire wereldwijde
  METAR-decode en zijn atomaire databasecommit van 120 naar 300 seconden;
  alle byte-, rij-, station- en feitenlimieten blijven ongewijzigd.

## 1.14.31 / worker 1.17.32

- Corrigeert de live op de NAS gevonden `MetarGroundDeadlineError`: de
  wereldwijde METAR-decode krijgt maximaal 120 seconden in plaats van 30
  seconden. De byte-, rij-, feiten- en geheugengrenzen blijven ongewijzigd.
- Voegt twee expliciete rekenprofielen toe. `shared_ha` blijft hard begrensd op
  25% CPU en minimaal nice 15; `dedicated_nas` laat uitsluitend de Storm-worker
  tot maximaal 50% CPU en minimaal nice 10 gaan. Beide profielen houden twee
  rekenthreads aan en reserveren een fysieke core voor Home Assistant.
- Publiceert het actieve rekenprofiel, CPU-plafond, nice-waarde en threadaantal
  in de begrensde workerstatus, zodat het dashboard de werkelijk toegepaste
  instellingen kan tonen.
- Registreert de beheerde fictieve tracker via retained moderne MQTT-discovery
  en initialiseert hem zo nodig op de HA-thuiscoordinaten. Daardoor blijft de
  tracker na een herstart bestaan zonder de verouderde `device_tracker.see`-
  actie en kan de integratie niet meer in `setup_retry` blijven hangen wegens
  ontbrekende coordinaten.

## 1.14.30 / worker 1.17.31

- Verifieert de Met Office-trackingketen op acht echte opeenvolgende publieke
  ODIM RATE-scans over 105 minuten en de UTC-daggrens, telkens over de volledige
  native grid in 45 tegels onder de vaste limiet van 100.000 pixels. De replay
  bereikte 463 bevestigde bewegingsklare trackobjecten, 56 passagekandidaten en
  50 toekomstige geometrische testpuntaankomsten; forecast, waarschuwing en
  operationele admission bleven uit.
- Corrigeert een door die echte replay gevonden floating-pointfout in de
  Lucas-Kanade-covariantie. Numerieke afronding in de twee off-diagonalen wordt
  na de bestaande asymmetrie-/PSD-controle exact gecanonicaliseerd, zodat een
  geldige providerframe niet langer de trackingcyclus kan stoppen.
- Voegt een begrensde, hashgebonden Met Office capture/replay-opdracht toe. Ze
  valideert het gesloten native ODIM-profiel, bewaart exacte content-addressed
  bronbytes en publiceert nooit naar MQTT of Home Assistant.
- Vernieuwt de fail-closed RAW-requirements-v2-pins voor de actuele volledige
  workerinventaris, service/composition/retention-bronnen en het geheime-vrije
  optionssnapshot. De bijbehorende transactionele poort is opnieuw volledig
  groen; capaciteits- en admissionclaims zijn niet verruimd.
- Er is in deze stap niets op de NUC, Home Assistant of Zigbee2MQTT gestart,
  herstart of gedeployed.

## 1.14.29 / worker 1.17.30

- Verwerkt begrensde metrische DWD- en Met Office-radarregio's nu over
  opeenvolgende scans tot duurzame celidentiteit en dezelfde-track beweging.
  Alleen bevestigde rain-shields met minstens twee echte scans leveren een
  schaduwvenster voor aankomst en vertrek; dit blijft zonder kanspercentage,
  waarschuwing of operationele admission.
- Koppelt canonieke METAR-, Netatmo- en Synoptic-feiten met bekende positie
  geometrisch aan exact dezelfde track en hetzelfde laatste scaninterval.
  Providersemantiek en fysieke site-identiteit blijven fail-closed
  ongeinterpreteerd totdat onafhankelijke catalogusbewijzen bestaan.
- Breidt worker-readiness uit met expliciete tracking-, schaduw-ETA- en
  grondmatchdiagnostiek, inclusief harde werkbudgetten en een checksum-gebonden
  trackingdatabase van maximaal 128 MiB.
- Bewijst de keten lokaal op een verzegelde echte DWD-reeks van 36 scans:
  72 van 72 tegelovergangen gingen vooruit, met maximaal 29 bevestigde
  bewegingsklare tracks en toekomstige testpuntkruisingen zonder admission.
- Corrigeert de ene Synology Compose-route: standaard `worker_only`,
  automatische samenhangende bootstrap onder het persistente NAS-volume, lage
  CPU-prioriteit, PID-/geheugengrenzen en een vastgelegde niet-destructieve
  rollback. Er is in deze stap niets live gedeployed.

## 1.14.28 / worker 1.17.29

- Sluit de volledige heropbouwbare worker-only runtimecache coherent uit van
  Home Assistant-backups. Hierdoor komen raw captures, afgeleide radarbestanden
  en de bijbehorende runtime-indexen niet langer als circa 0,8-0,9 GB
  wegende appdata in iedere backup terecht.
- Behoudt de appopties en credentials in backups en voorkomt een inconsistente
  halve restore: niet alleen raw bestanden, maar de volledige runtime met de
  verwijzende tijdelijke databases wordt samen uitgesloten. Na herstel bouwt
  de worker die cache opnieuw op binnen de onveranderde 25%-CPU-limiet.

## 1.14.27 / worker 1.17.28

- Houdt de operationele snapshot ook actueel tijdens langdurige kwantitatieve
  radarherberekeningen, zowel bij workerstart als na een Life360-/doelwijziging.
  De vijfminuten-TTL kan daardoor niet meer verlopen tussen provideracquisitie
  en de uiteindelijke doelprojectie.
- Publiceert tijdens zo'n herberekening uitsluitend een conservatieve
  statussnapshot. Gedeeltelijk opgebouwde radarobservaties worden pas zichtbaar
  nadat de volledige seriële herberekening klaar is.
- Verandert niets aan de harde NUC-bescherming: de Storm-worker blijft maximaal
  25% CPU gebruiken, nice 15, twee rekenthreads en zonder tijdelijke boost.

## 1.14.26 / worker 1.17.27

- Vernieuwt de operationele snapshot-TTL ook wanneer de seriële acquisitielaan
  nog op een langlopende providerjob wacht. Het dashboard vervalt daardoor niet
  meer na vijf minuten naar `worker niet actueel` terwijl DWD veilig onder de
  lage NUC-CPU-limiet doorrekent.
- Houdt provideracquisitie exact serieel: de onderhoudspuls herpubliceert alleen
  de reeds opgebouwde, gevalideerde snapshot en start geen tweede providerjob.

## 1.14.25 / worker 1.17.26

- Legt op de gedeelde Home Assistant-NUC een harde Storm-workergrens van 25%
  totale CPU vast; Home Assistant en Zigbee2MQTT worden zelf niet begrensd.
- Stopt de worker fail-closed wanneer zijn `cpulimit`-proces wegvalt of wordt
  gepauzeerd. Een tijdelijke onbeperkte CPU-boost kan daardoor niet meer via
  de workerlauncher worden uitgevoerd.
- Maakt 25% ook de enige toegelaten HA App-configuratiewaarde en legt het
  verbod op limiterbypasses vast in de vaste deploy- en herstelprocedure.

## 1.14.24 / worker 1.17.25

- Houdt een gecertificeerde radarpixel onder de detectiedrempel voortaan als
  een actuele, gekwalificeerde observatie bij het doel. De kaart toont daarmee
  `geen positieve neerslag waargenomen` in plaats van de bron volledig weg te
  gooien; dit blijft uitdrukkelijk geen droogclaim en de regenintensiteit blijft
  leeg.
- Bewaart het onderscheid tussen een exacte nulpixel, `undetect`, `nodata`,
  buiten bereik en een onbeschikbare locatie in reden, provenance en UI-
  attributen. Alleen de eerste twee mogen de beperkte niet-positieve
  radarobservatie ondersteunen.

## 1.14.23 / worker 1.17.24

- Maakt een gecertificeerde exacte nulmeting op een actuele radar zichtbaar als
  `geen_positieve_neerslag_waargenomen`, met de concrete provider en producttijd.
  De status blijft uitdrukkelijk geen droogclaim: `rain_rate_mm_h` blijft leeg en
  onafhankelijk grond-/full-scanbewijs blijft vereist voor `droog`.
- Onderscheidt een echte nulpixel strikt van `undetect`, `nodata`, buiten bereik
  en een onbeschikbare locatie; die laatste toestanden blijven `onvoldoende_data`.

## 1.14.22 / worker 1.17.23

- Voorkomt dat een positieve radarwaarneming de worker fail-closed stopt in de
  laatste 30 seconden van de bronfreshness. De worker en de snapshot gebruiken
  nu exact dezelfde publicatieveiligheidsmarge: de bijna verlopen bron wordt
  veilig `stale` en de neerslagwaarde blijft tijdelijk `unknown` tot een verse
  capture beschikbaar is.

## 1.14.22 / worker 1.17.22

- Voorkomt dat de primaire DWD-capture op een begrensde, trage host iedere
  seriële workerbeurt opnieuw inneemt: na één DWD-voorrangsbeurt krijgt een
  nog ongeprobeerde toepasselijke bron verplicht de volgende eerlijke beurt.
  Er blijft exact één providerjob tegelijk actief; HA en Z2M krijgen geen extra
  Storm Tracker-concurrentie.
- Publiceert de integratieversie als vast, veilig attribuut op de technische
  workersensor zodat het dashboard de live versie kan tonen zonder opnieuw een
  verouderd hardcoded versienummer te bevatten.

## 1.14.0 / worker 1.17.0

- Activeert de echte `worker_only`-serviceroot: 27 uitvoerbare bronnen worden
  uitsluitend door de worker opgehaald en alle 29 catalogusbronnen blijven met
  hun exacte current/stale/error/blocked/not-applicable toestand zichtbaar.
- Publiceert na iedere geverifieerde capture een begrensde operationele
  snapshot via de bestaande MQTT-brug; Home Assistant draait geen providers,
  decoders, meteorologische engines of Storm Tracker-databases meer.
- Decodeert en certificeert DWD RADOLAN-RV, Met Office RATE en NOAA Enterprise
  Rain Rate tot echte positieve doelmetingen. KNMI RTCOR wordt wel volledig
  verwerkt maar blijft doelpixel-fail-closed zolang zijn gridautoriteit niet
  gecertificeerd is; nul of ontbrekende data wordt nooit als droog verzonnen.
- Voegt een idempotente HA-bootstrap toe die de worker-only config en immutable
  authority lokaal onder `/data` materialiseert wanneer nog geen pins bestaan.
  Nieuwe HA-installaties kiezen worker-only standaard; legacy blijft expliciet
  beschikbaar als rollbackmodus.
- Bewijst de productieketen met echte providerfixtures en een end-to-end
  snapshotscenario waarin de worker een positieve regenwaarde als `raining`
  publiceert. De worker blijft op twee rekenthreads, nice 15 en met een vrije
  HA-core draaien.

## 1.13.3 / compatibele worker 1.16.2 (niet mee gedeployed)

- Herstelt een uitsluitend expliciet selecteerbare normale-HA-runtime die de
  bestaande providers, OFE, lokale StormEngine, nowcast en kaartuitvoer zonder
  worker kan uitvoeren. `worker_only` blijft de standaard; er bestaat geen
  impliciete fallback en YAML blijft afgewezen.
- Marshal alle MQTT-status-, job- en snapshotscallbacks naar de Home
  Assistant-eventloop voordat zij HA-state of futures wijzigen.
- Voegt opt-in eigen-modelwaarschuwingen toe met strikte freshness-, passage-,
  confidence- en forecastgates, persistente deduplicatie/cooldown/escalatie en
  een expliciete claim dat het geen officiële waarschuwing is.
- Declareert opnieuw de exacte `h5py`, `numpy` en `pyproj`-requirements die de
  lokaal uitgevoerde radarproviders tijdens legacy-setup importeren; een verse
  gewone-HA-installatie leunt daardoor niet op toevallig aanwezige packages.
- De gewone-HA-installatieroute installeert uitsluitend de integratiezip. De
  sealed worker 1.16.2 dient alleen als ongewijzigde build-evidencebasis en
  wordt niet geüpload, uitgepakt, geïnstalleerd of gestart.

## 1.13.2 / worker 1.16.2

- Maakt RAW-capaciteit expliciet profielgebonden: de standaard release gebruikt
  27 cadencebronnen, schakelt Blitzortung-eventingest uit en claimt daarvoor geen
  opslag. Met 2 GiB operationele hostreserve is de minimale doelvloer
  20.694.695.936 bytes; het volledige eventprofiel blijft afzonderlijk
  fail-closed.
- Voegt een read-only retentieguard toe die eigenaarschap, padidentiteit,
  cadence-store, ledger, catalogi en fysieke vrije ruimte voor iedere cyclus
  herbewijst. De guard verwijdert niets en sluit ingest bij drift of uitputting.
- Levert een expliciete, standaard uitgeschakelde offline reclamationcontroller
  met operatorgebonden PREPARE, COMMIT, APPLY en ABORT boven de bestaande
  crash-herstelbare vijfrollenketen. Live reclamation blijft uit.
- Scheidt credentials en RAW-retentieopt-ins volledig van de Home Assistant-
  worker-only config flow; de Supervisor Worker App blijft de enige eigenaar.
- Voegt content-addressed offline replaybewijs, echte deterministische
  nowcast-baselineruns en een geïsoleerde shadow-factketen toe. Alle
  meteorologische admission-, publicatie-, dashboard- en waarschuwingclaims
  blijven uit zonder extern certificaat.
- Voegt een begrensd technisch workerdiagnosecontract en een event-driven HA-
  sensor/kaart toe. Onbewezen provider-, freshness- of opslagvelden blijven
  expliciet `unknown` en worden nooit als meteorologische waarheid getoond.
- Vernieuwt materializer, target-preflight en RAW-releasevereisten naar gesloten
  v2-contracten. Targetmaterialisatie, live canary en deployment zijn nog niet
  uitgevoerd en blijven externe gates.

## 1.13.1 / worker 1.16.1

- Voegt een expliciete `raw_capture_shadow`-runtime toe waarin de worker 27
  cadencebronnen exact-byte en crash-herstelbaar kan vastleggen, met OPERA als
  begrensde streamingcapture en Blitzortung als afzonderlijke opt-in event-ingang.
- Houdt Home Assistant in `worker_only` vrij van providers, decoders,
  meteorologische berekeningen en Storm Tracker-databasewerk; RAW-data gaat
  nooit via MQTT.
- Houdt downstreamconsumers, meteorologische toelating, dashboardoutput,
  waarschuwingen en automatische reclamation bewust uit. Capaciteitsuitputting
  stopt fail-closed en KMI native RADQPE blijft zichtbaar geblokkeerd.
- Levert een target-side configuratie-/capacitymaterializer, een reproduceerbare
  vierde RAW-requirementsmarker en een fail-closed resettool die HA-history,
  configuratie, credentials, rollbackpakketten en releasebewijs beschermt.
- Deze release-identiteit is uniek, maar doelactivatie en deployment zijn niet
  uitgevoerd. Een later, extern vertrouwd doelbewijs blijft vereist.

## 1.13.0 / worker 1.16.0

- Legt de offline worker-first reclamationroute vast als een duurzame
  prepare-commit-apply-keten voor lifecycle, execution, scheduler, ruwe
  artefacten en capaciteit, met exacte replaybare beslissingen en receipts.
- Versterkt de bron- en registry-authoriteit, append-only retentionbewijzen,
  schema-inventory, lifecyclefences en semantische tombstones zodat stale,
  vervalste of partieel toegepaste reclamation fail-closed blijft.
- Houdt deze architectuur buiten de live serviceroute: service wiring en
  production-ready blijven uit. Deze versiedelta bouwt of deployt niets en
  wijzigt geen live Home Assistant- of MQTT-toestand.

## 1.12.0 / worker 1.15.0

- Verliest geen observatiebatch meer wanneer de worker time-out, een ongeldig
  resultaat terugstuurt of de OFE-callback wordt onderbroken: dezelfde batch
  blijft FIFO bewaard en wordt opnieuw of lokaal verwerkt.
- Valideert workersnapshots vóór toepassing en vervangt trackingstate atomair;
  een beschadigde workersnapshot kan de geldige lokale trackingstate niet
  wissen.
- Bindt trackingprotocol v4 aan een stabiele batch-ID, exacte input- en
  statehashes en een startrevision. Een laat workerresultaat kan een nieuwere
  trackerstate niet overschrijven en een retry past dezelfde batch niet
  tweemaal toe.
- Behandelt `max_storms` uitsluitend als legacy capaciteitsrichtwaarde en
  gooit geen echte bliksem- of radarobservaties meer stil weg.
- Verhoogt de worker-startstate van 30 naar 512 tracks en bewaakt met een
  continuïteitstest dat meer dan dertig onafhankelijke tracks de
  workerroute niet permanent uitschakelen.
- Berekent afstand tot de volledige radarfootprint: binnen een polygoon is de
  afstand nul en buiten wordt de dichtstbijzijnde rand gebruikt in plaats van
  alleen polygonvertices.
- Groepeert Netatmo-, Synoptic- en METAR-bevestiging per storm-ID. Twee natte
  stations bij verschillende weersystemen kunnen samen geen systeem
  bevestigen, overlappende footprints blijven expliciet ambigu en de
  targetsensor gebruikt score en provenance van het geselecteerde systeem.
- Dedupliceert hetzelfde fysieke grondstation over METAR en Synoptic, koppelt
  de meting aan het dichtstbijzijnde radarframe in meettijd en laat
  sensor-propertyreads geen auditrecords meer creëren.
- Geeft queue-full, identificeerbare ongeldige en verlopen workerjobs altijd
  een terminale MQTT-status en herstelt processtatussen consequent naar
  `idle`.
- Behandelt een normale MQTT QoS1-redelivery niet meer als terminale
  `duplicate`-fout: queued/running wordt opnieuw gemeld en een voltooid
  resultaat wordt uit de begrensde statuscache teruggegeven zonder
  herberekening.
- Verwerkt bij een samengevoegd weersysteem alle losse radarcontouren van
  hetzelfde tijdstip afzonderlijk. Een station onder een tweede component
  kan het juiste systeem bevestigen, terwijl de lege ruimte tussen
  componenten nooit een fictieve footprint wordt.
- Voert idempotente lokale trackingbatches op een working copy uit. Een
  precommitfout laat daardoor ook geen reeds gepubliceerde `Storm`-referentie
  half gemuteerd achter; succesvolle commits behouden wel dezelfde
  objectidentiteit.
- Plant een batch na `CancelledError` in een reeds gestarte OFE-callback
  automatisch opnieuw met exact dezelfde batch-ID.
- Voorkomt dat één deterministisch foutrecord alle nieuwere observaties
  blokkeert: retries gebruiken begrensde exponentiële backoff, gemengde
  batches worden atomair gesplitst en blijvend foute records blijven
  zichtbaar en handmatig retrybaar in quarantaine in plaats van stil te
  verdwijnen. Geldige siblings resetten het opeenvolgende foutbudget; een
  apart callback- en wandtijdbudget verdeelt pathologische isolatie
  verliesvrij over automatische cooldown-epochs.
- Registreert `storm_tracker_v3.retry_quarantined_observations` als
  operatorservice en publiceert pending, retry, cooldown en quarantaine per
  RegionEngine.
- Sluit de runtime geordend af: provideradmission dicht, actieve lifecyclefetch
  annuleren, cycli uitwachten, calibrationdata flushen en pas daarna OFE's
  sluiten en trackingstate opslaan. Recente Home Assistant-versies gebruiken
  hiervoor een awaited shutdown job.
- Voert workerjobs strikt serieel uit in één persistent `spawn`-childproces.
  Een absolute deadline, childcrash of shutdown stopt het childproces hard,
  geeft iedere betrokken job een terminale toestand en laat de queue daarna
  met een schoon childproces verdergaan.
- Importeert zware workerhandlers lazy in het childproces, behoudt hun caches
  tussen normale jobs en publiceert reconcileerbare
  accepted/terminal/completed/failed/expired/cancelled-counters.
- Maakt terminale workerstatus onomkeerbaar: een laat queued/running-bericht
  kan een reeds voltooide, gefaalde, verlopen of geannuleerde job niet opnieuw
  openen.
- Schrijft iedere genormaliseerde observation vóór zware verwerking naar een
  afzonderlijk durable SQLite-journal met expliciete ingest-, batch-,
  attempt-, transition-, quarantine- en trackingstateversies.
- Commit trackingstate en batchreceipt atomair met revision-CAS en herstelt
  ontvangen, prepared en staged werk vóór nieuwe providers of routes worden
  toegelaten. Een permanente regionregistry voorkomt dat kleine GPS-jitter
  een nieuwe opslagidentiteit maakt.
- Bewaart workeracceptatie en terminale resultaten in SQLite onder `/data`.
  Stabiele job-ID's zijn aan een requestdigest gebonden; workerrestart kan een
  resultaat replayen en achtergelaten werk expliciet terminaliseren.
- Geeft de worker-receiptdatabase een expliciete schemaversie, verplichte
  kolom-/indexcontrole en `integrity_check`. Een onbekend, onvolledig of
  vooruitgelopen schema wordt fail-closed geweigerd en nooit stil
  overschreven.
- Wacht bij Blitz-ingest de async duurzame-ontvangstbarrière af in plaats van
  observaties via een los fire-and-forget-taskvenster door te geven.
- Introduceert een volledig geïsoleerde CellTrack-shadowprimitive. Twee extern
  gekoppelde detecties leveren exact hun gemeten intervalsnelheid en een 4×4
  statecovariantie, zonder snelheidslimiet, smoothing of WeatherSystem-motion.
- Bouwt voor twee detectieframes een complete, deterministische
  association-candidategraph: ieder celpaar blijft auditbaar, ook bij extreme
  verplaatsing. De geometrische NLL is alleen een ongekalibreerd
  rangschikkingskenmerk; er wordt nog geen identiteit of probability gekozen.
- Voegt een expliciet synthetische CellTrack-contractreplay met 24 frames toe.
  Die bewaart alle 92 kandidaat-edges voor twee cellen, controleert exacte
  intervalsnelheden en markeert zelf dat globale assignment en echt weer nog
  niet gevalideerd zijn.
- Introduceert een nog niet aangesloten, content-addressed
  `RawArtifactStore` voor exacte upstream bytes. Blob en immutable
  providerreceipt worden atomair geschreven; een upstream record-ID kan niet
  stil naar andere bytes of provenance gaan verwijzen en onbekende schema's
  worden niet overschreven.
- Breidt de lokale validatie uit tot `809 passed, 1 skipped`, inclusief echte
  abrupte subprocesscrashes op iedere durable receive- en commitgrens.

## 1.11.0 / worker 1.13.0

- Introduceert Observation-contract v2 met afzonderlijke meet- en
  ontvangsttijd, expliciete categorische neerslag, eenheden, interval,
  parserversie, kwaliteitsvlaggen en begrensde ruwe provenance.
- Koppelt grondbevestiging aan verse stations binnen de radarvoetafdruk van
  hetzelfde WeatherSystem en gebruikt een brongewogen confidence.
- Bewaart grondwaarnemingen, kwaliteitsbeoordelingen en beslissingen in
  schema 12 zonder bestaande kalibratiehistorie te wissen.
- Maakt HA en worker gelijk voor METAR/Synoptic/Netatmo en publiceert de
  runtimeversie, Observation-contractversie en build-ID in de heartbeat.

## 1.10.2

- Verwijdert de hardcoded whitelist uit de opslagroute voor nationale
  radarproviders.
- Bewaart automatisch iedere actieve lifecycle-provider die een echte
  raster-overlay met brontijdstip aanlevert; punt-nowcasts zonder raster
  worden niet kunstmatig als radarframe opgeslagen.

## 1.10.1

- Reset uitsluitend de afzonderlijke Storm Tracker-kalibratiedatabase bij de
  schema-v11-migratie; de algemene Home Assistant-database blijft onaangeroerd.
- Bewaart ieder ontvangen radarframe vóór filtering of throttling verliesvrij
  op de oorspronkelijke gedecodeerde providerresolutie.

## 1.10.0

- Verhoogt de worker-rasterresolutie bronadaptief naar circa 500 meter voor
  hoogwaardige nationale radar en circa 1 kilometer voor samengestelde radar.
- Behoudt satellietproducten op hun werkelijke grovere bronresolutie om
  schijnnauwkeurigheid te vermijden.
- Verhoogt de veilige workerlimiet naar 640.000 rastercellen en 24.000 compacte
  kaartsegmenten, terwijl HA uitsluitend de begrensde kaartlaag ontvangt.
- Herstelt de kaartweergave van gesloten 5-punts workerpolygonen; oude
  4-puntsruns blijven compatibel.
- Maakt Synoptic functioneel gelijkwaardig aan Netatmo voor beschikbare
  luchtdruk, temperatuur, vochtigheid, wind en windstoten, en neemt
  Synoptic-stations mee in de regionale drukvaldetectie.

## 1.9.0

- Verwerpt geïsoleerde centroid-sprongen via een robuuste residucontrole
  voordat snelheid, versnelling of kromming worden berekend.
- Publiceert begrensde versnelling, bochtstraal en afgewezen meetpunten als
  compacte workerdiagnostiek.
- Bewaart splits- en merge-lineage in stormobjecten en over herstarts heen,
  met parent-, merge- en generatierelaties.
- Houdt lineaire beweging als conservatieve fallback wanneer het complexere
  model geen aantoonbare hindcastwinst heeft.
- Geeft de baan op het dashboard in begrijpelijke termen weer.

## 1.8.0

- Bouwt regionale 0–60-minutenradarrasters en compacte kaartlagen volledig
  in de externe worker, via begrensde en geverifieerde artifacts.
- Verwijdert `frame_from_overlay` en framepersistentie uit de algemene
  Home Assistant-executor; bij transportfalen blijft het laatst geldige
  resultaat actief zonder zware lokale berekeningsfallback.
- Valideert de landenpolicy tegen de werkelijk geïmplementeerde radarbronnen
  en verwijdert niet-bestaande nationale providers uit de actieve prioriteit.
- Voegt workerstatus toe voor de operationele regionale rasteropbouw.

## 1.7.0

- Voegt NOAA Enterprise Rain Rate BLEND toe als wereldwijde
  satellietneerslagbron tussen 60° ZB en 70° NB.
- De externe worker downloadt en verwerkt het globale NetCDF4-product;
  Home Assistant ontvangt uitsluitend compacte observaties en kaartsegmenten.
- Combineert automatisch Himawari-9, GOES-18/19 en Meteosat-dekking via het
  officiële NOAA-blendproduct met ongeveer 2 km resolutie en 10-minutencadans.
- Gebruikt de satellietlaag als worker-first terugval wanneer gewone radar geen
  bruikbare echo levert, met behoud van H SAF en GOES als verdere terugval.
- Behoudt tot 12.000 satellietsegmenten voor een veel gedetailleerdere kaart
  en ondersteunt maximaal zestien gelijktijdige regio's. De downloadgrootte,
  productleeftijd en absolute kaartpayload blijven hard begrensd.
- Begrenst NumPy, OpenBLAS, OpenMP en aanverwante workerberekeningen op
  maximaal twee CPU-threads.

## 1.5.0

- Maakt Synoptic operationeel als Noord-Amerikaanse grondwaarheidsbron.
- Gebruikt 20 km voor lokale bevestiging en een begrensde 50-km-zoekzone
  voor reizende verkenners; alleen stations nabij het voorspelde buipad
  kunnen een concrete passage bevestigen.
- Synoptic-ophaling, caching en rate limiting draaien volledig in de worker.
  Home Assistant leest uitsluitend compacte workerresultaten.
- Combineert Synoptic en Netatmo in dezelfde grondbevestigings- en
  verkennerlogica, met behoud van de bron in resultaten en databasehistoriek.
- Voegt een aparte Synoptic-sensor en workerheartbeat toe met aantallen
  beschikbare, recente en natte stations.

## 1.4.2

- Voegt een afgeschermde Synoptic/CWOP-capabilityprobe aan de worker toe:
  maximaal 50 stations, maximaal 20 km, uitsluitend recente neerslagvelden.
- De probe publiceert geen token, station-ID, MAC-adres of exacte locatie.
- Voegt een afzonderlijk `synoptic_token`-veld aan de add-onconfiguratie toe.
- NOAA/NWS is als sleutelloze kandidaat getest, maar niet operationeel gemaakt
  wegens vertraging en ontbrekende uurregenwaarden bij de nabijgelegen stations.

## 1.4.0

- De externe worker bewaart maximaal vier opeenvolgende radarframes en kan
  daardoor behalve een rechte baan ook een aantoonbare versnelling of
  vertraging in de 0-60-minutennowcast verwerken.
- Het versnellingsmodel wordt alleen operationeel wanneer een onafhankelijke
  een-frame-terugtest minstens 0,08 IoU beter scoort dan de lineaire baan en
  alle snelheids- en versnellingsgrenzen respecteert.
- Bij te weinig frames, instabiele beweging of onvoldoende modelwinst blijft
  de veilige lineaire nowcast automatisch actief.
- AWN is onderzocht als publieke grondbron, maar niet ingebouwd: de officiele
  API ontsluit alleen stations die met een gebruikerssleutel zijn gekoppeld
  en ondersteunt geen publieke 20-km-stationzoekopdracht voor verkenners.

## 1.3.0

- Rondt workerfase 5 af: Home Assistant start bij workeruitval geen zware
  lokale radar-, tracking-, bewegings-, verkenner-, kalibratie- of
  GeoJSON-berekening meer.
- Het laatst geldige workerresultaat blijft zichtbaar tijdens een korte
  workerstoring; de status meldt expliciet `worker_onbeschikbaar` zonder een
  misleidende lokale fallback te activeren.
- DWD/RADOLAN, Met Office en OPERA downloaden of segmenteren geen zwaar
  radarproduct meer in Home Assistant Core.
- Integratie en worker gebruiken vanaf deze release versie 1.3.0.

## 1.2.1

- Fase 4: GeoJSON-opbouw verhuist naar de externe worker. HA publiceert
  uitsluitend de laatst gevalideerde workercollectie en bouwt lokaal alleen
  nog een begrensde fallback wanneer de worker niet beschikbaar is.
- Sensor en beveiligde HTTP-feed hergebruiken dezelfde kaartcache; hierdoor
  ontstaat niet langer bij ieder HTTP-verzoek een nieuwe zware kaartopbouw
  in Home Assistant Core.
- Vanaf deze release gebruiken integratie en worker hetzelfde versienummer.

## 0.4.160

- Voegt worker 1.2.0 met een volledig geïsoleerde fase-3-replay toe. De
  replay test beweging, 0–60-minutennowcast, tracking, verkenners,
  grondbevestiging en radarkalibratie zonder live sensoren, kaartlagen of
  meldingen te wijzigen.
- Herstelt het behoud van opeenvolgende radarframes wanneer een provider
  voor hetzelfde object een stabiele cel- of systeem-ID hergebruikt. Iedere
  frame-overdracht behoudt nu de volledige bewegingshistoriek.
- Dezelfde correctie staat bytegelijk in de integratie en de worker, zodat
  herstart- en worker-firsttracking identiek blijven.

## 0.4.159

- Rondt workerfase 3 af: de worker bouwt een begrensde deterministische
  raster-nowcast van 0 tot 60 minuten met stappen van vijf minuten.
- Verplaatst regionale radarvergelijking en kalibratiescores naar worker
  1.1.0; HA bewaart alleen compacte resultaten en biasprofielen in de
  secundaire SQLite-database.
- Bewegingsanalyse en kalibratie wachten niet meer binnen de providerfase op
  een MQTT-antwoord. Het laatst geldige resultaat blijft actief en een
  begrensde lokale fallback vangt workeruitval op.
- De pySTEPS- en autokalibratiesensoren tonen nowcastgereedheid,
  workerstatus, fallback en ontvangen vergelijkingen.

## 0.4.157

- Verkenners gebruiken alleen stormobjecten waarvan het laatste radarframe
  maximaal 15 minuten oud is.
- Een actieve verkenner op een verouderd stormobject wordt van de kaart
  verwijderd en als `radarbeeld_verouderd` in de kalibratiedatabase bewaard.
- Dezelfde versheidscontrole draait identiek in de integratie en worker 1.0.3.

## 0.4.156

- Netatmo OAuth heeft nog maar één eigenaar: de externe worker. HA start
  niet langer tegelijk een tweede token-refresh wanneer de workercache
  wordt opgebouwd.
- Herhaalde workerjobs kunnen een door Netatmo geroteerd refresh-token
  niet meer overschrijven met het oudere configuratietoken.
- Tijdens een korte workerverversing blijven geldige grondmetingen
  maximaal 15 minuten beschikbaar voor regenbevestiging.

## 0.4.155

- RainViewer kan een officiële radarbron niet langer verdringen op basis van
  RainViewer-overlap alleen; kwaliteitsfallback vereist onafhankelijke
  grondbevestiging.
- De kaart toont bij verkenners de radarbron van hun regio.
- Worker 1.0.1 herstelt de afstandsmodule en ondersteunt een volledige,
  begrensde operationele trackingbatch.

## 0.4.154

- Vervangt de oude stormstatus-hashcontrole door zelfstandige tracking in
  worker 1.0.0. HA en worker starten per batch met dezelfde volledige
  trackinghistorie en dezelfde ruwe radar-, bliksem- en regenobservaties.
- Geeft nieuwe stormobjecten deterministische ID's. Na twee inhoudelijk
  identieke batches wordt tracking automatisch worker-first; bij timeout of
  fout blijft een begrensde lokale fallback beschikbaar.
- Verplaatst Netatmo OAuth-refresh en zowel lokale als reizende 20-km-query's
  naar de worker, met vijf minuten rate limiting en vijftien minuten
  last-good-cache. HA start deze taken op de achtergrond en wacht er nooit op
  binnen een providerfase.
- Verplaatst verkennerplanning, trajecthercontrole, evaluatie en selectie van
  de reizende query naar de worker. Alleen bij ontbrekend workerresultaat
  gebruikt HA de lokale fallback.
- Herstelt verkennertellers en recente uitkomsten uit de secundaire database
  na een herstart. Trackingpariteit en omschakelstatus worden daar eveneens
  per batch opgeslagen.
- Verwijdert afgewerkte en geannuleerde verkenners onmiddellijk van de live
  kaart. Hun resultaten blijven wel beschikbaar in de database en de
  tekstdiagnostiek.
- Gebruikt uitsluitend de begrijpelijke workerstatussen `operationeel`,
  `fallback` en `transportcontrole`; de misleidende tracking- en
  verkennerlabels `(vergelijking)` verdwijnen.

## 0.4.153

- Herbeoordeelt iedere actieve virtuele regenverkenner tegen de actuele
  stormbaan. Een punt achter de bui of buiten de actuele passagecorridor
  wordt afgesloten als `traject_gewijzigd` en verdwijnt van de kaart.
- Sluit verkenners van verdwenen stormobjecten expliciet af. Daardoor blijven
  bijvoorbeeld geen Schotse punten actief wanneer dezelfde RegionEngine
  ondertussen opnieuw voor Italië wordt gebruikt.
- Stuurt dezelfde begrensde stormmomentopname naar worker 0.10.0, zodat HA en
  worker onafhankelijk exact dezelfde annuleringen kunnen vergelijken.
- Bewaart iedere annulering als verificatieresultaat in de secundaire
  database; alleen de verouderde actieve kaartmarkering wordt verwijderd.

## 0.4.152

- Voegt een begrensde worker-shadow toe voor de evaluatie van actieve
  virtuele regenverkenners. Alleen de maximaal acht actieve verkenners en
  hun relevante Netatmo-stations verlaten HA; planning en operationele
  beslissingen blijven tijdens deze validatiefase lokaal.
- Begrensde worker-time-out van vier seconden past volledig binnen de
  vijfsecondenfase, zodat geen loslopende taak na een fase-time-out resteert.
- Benoemt de bestaande tracking-shadow correct als stormstatusvergelijking:
  deze valideert overdracht en continuïteit, niet nog het volledige
  contourkoppelingsalgoritme.

# 0.4.147

- Herken de Nederlandstalige landcode `VK` als Groot-Brittannië, zodat een
  Britse RegionEngine de bestaande Met Office-radar werkelijk selecteert.
- Toon de HDF5-worker expliciet als `Met Office HDF5 (VK)` in plaats van de
  te algemene naam `HDF5-radar`.
- Start de veilige OPERA-migratie in worker 0.4.0: de worker downloadt en
  segmenteert dezelfde lokale HDF5-slice in schaduwmodus, waarna celgetal en
  volledige celdigest met de bestaande verwerking worden vergeleken.
- Verplaats ook de nog lokale OPERA-parser uit de algemene HA-executor naar
  een eigen begrensde OPERA-thread.

# 0.4.146

- Fase 2 uitgebreid met een generieke HDF5-radarjob in worker 0.3.0.
- De Britse Met Office ODIM-HDF5-provider gebruikt nu worker-first voor
  download en decoding, met een eigen lokale fallbackthread.
- De bestaande DWD-artifactvalidatie is herbruikbaar gemaakt voor andere
  radarproviders.

# 0.4.145

- DWD/RADOLAN gebruikt nu de externe rekenservice als operationele radarbron.
- Workerartifacts worden vóór gebruik gecontroleerd op pad, grootte, hash,
  inhoud, observatieaantal en frameleeftijd.
- Bij een onbeschikbare of ongeldige worker valt DWD automatisch terug op de
  bestaande, begrensde lokale verwerking.

# 0.4.144

- Corrigeer de DWD/RADOLAN-projectie met de echte linkerbovenhoek uit de
  ODIM-HDF5-metadata. De vroegere berekening plaatste Duitse regenpixels
  foutief op 57–66 graden noorderbreedte, waarna het regiofilter alle
  waarnemingen verwijderde.
- Pas exact dezelfde projectie toe in Home Assistant en de zelfstandige
  worker, met een regressietest op productiehoekcoördinaten.

# 0.4.143

- Gebruik bij een droog DWD-frame het tijdstip van de radaroverlay voor de
  workervergelijking. Daardoor worden twee geldige lege resultaten niet langer
  ten onrechte als verschillend gemarkeerd.

# 0.4.142

- Start fase 2 van de zelfstandige rekenservice met DWD/RADOLAN in
  schaduwmodus: de worker downloadt en decodeert het HDF5-frame buiten Home
  Assistant en schrijft het grote resultaat atomair als gecomprimeerd artifact.
- Vergelijk frame-ID, observatieaantal en radarcel-ID's met de bestaande
  HA-provider zonder de operationele radaruitkomst al om te schakelen.
- Toon per werkelijk geregistreerd workerproces een afzonderlijke heartbeat,
  werkstatus, taakduur en foutstatus op het Storm Tracker-dashboard.

# 0.4.141

- Behoud de metadata van de automatische worker-ping wanneer een retained of
  periodieke worker-heartbeat binnenkomt, zodat `last_ping_state` zichtbaar
  blijft en de end-to-end bewaking aantoonbaar `completed` kan rapporteren.

# 0.4.140

- Wacht de asynchrone MQTT-publicatie van de worker-heartbeat correct af,
  zodat iedere automatische end-to-end ping werkelijk wordt verzonden en
  Home Assistant geen `coroutine was never awaited`-waarschuwing meer geeft.

# 0.4.139

- Voeg fase 1 van de zelfstandige Storm Tracker-worker toe: een containerbaar
  proces met begrensde wachtrij, maximaal één actieve taak, deadlines,
  idempotente taak-ID's en een versieerbaar MQTT-contract.
- Publiceer retained workerstatus en een end-to-end heartbeat. Home Assistant
  bewaakt dit via `sensor.stv3_worker_status`; de worker voert in deze fase
  uitsluitend een veilige ping uit.
- Laat bestaande providers en berekeningen voorlopig actief. DWD/RADOLAN en
  HDF5 verhuizen pas in fase 2, nadat transport en statusbewaking operationeel
  gevalideerd zijn.

# 0.4.138

- Beperk KNMI-nowcast tot de 19 vijfminutenstappen van 0 tot 90 minuten en
  haal ze begrensd parallel op met maximaal vijf gelijktijdige WMS-requests.
  De vroegere 120-minutenreeks verdubbelde netwerk- en decodeerwerk zonder
  bruikbare meerwaarde buiten de operationele 90-minutenhorizon.
- Decodeer KMI- en KNMI-radarbeelden in workerthreads in plaats van op de
  Home Assistant-eventloop. Dit voorkomt dat 512×512-pixelverwerking de
  frontend en API tijdelijk onbereikbaar maakt.

# 0.4.137

- Vervang de resterende KMI-, KNMI-, RainViewer-, Netatmo- en Open-Meteo-CSV
  door gededupliceerde providersamenvattingen per locatie en minuut in de
  secundaire SQLite-database (schema 10).
- Verwijder de losse providerlogger uit het runtimepad. Daarmee verdwijnen
  meerdere kleine executor-schrijfjobs per cyclus en staat alle analyseerbare
  providerhistorie transactioneel en geïndexeerd op één plaats.
- Behoud radarframes, Open-Meteo-detailverificatie en bliksemaggregaten in hun
  gespecialiseerde tabellen; de providersamenvatting is compact en bedoeld
  voor snelle trend- en tuningqueries.

# 0.4.136

- Sla identieke radarframes en providervergelijkingen niet opnieuw op. Dit
  voorkomt iedere vijf minuten het verwijderen en herinvoeren van duizenden
  rasterpunten en onnodige herberekening van providerbias.
- Vervang de onbegrensde ruwe `blitzortung.csv` door compacte aggregaten per
  minuut en geografische rastercel in de secundaire kalibratiedatabase.
  Aantallen, tijdvenster, locatie, bron en betrokken regio's blijven bewaard,
  zonder miljoenen losse CSV-regels en executor-schrijfjobs.
- Behoud de historische databasesamenvatting bij opstart en ververs die ieder
  uur; metingen op de echte database tonen dat deze queries samen circa
  12 milliseconden kosten en geen bereikbaarheidspiek veroorzaken.

# 0.4.135

- Publiceer de radarautokalibratiesensor als compacte samenvatting in plaats
  van bij iedere vijfminutencyclus de volledige databaseanalyse, alle
  targettellingen en biasprofielen door HA-state, WebSocket en Recorder te
  laten serialiseren.
- Behoud alle detailmetingen, profielen en fasehistorie ongewijzigd in de
  secundaire kalibratiedatabase. De sensor toont nog steeds de actuele score,
  laatste providervergelijking, opslagstatus en totalen.

# 0.4.134

- Vectoriseer de omzetting van radarvierhoeken naar het pySTEPS-raster met
  NumPy. De native rechthoekvullingen geven de Python GIL vrij, zodat een nieuw
  RainViewer-frame Home Assistant niet meer langdurig onbereikbaar maakt.
- Schrijf bij een nieuw radarframe alleen de gewijzigde frames naar de
  herstartcache. De volledige zesframescache per regio wordt niet meer iedere
  cyclus opnieuw naar Python-lijsten en JSON omgezet.
- Materialiseer sparse pySTEPS-cellen vectorieel naar NumPy voor de
  bewegingsanalyse.

# 0.4.132

- Verplaats de zware DWD RADOLAN-archiefparse naar een eigen executor met
  maximaal één worker, zodat deze de algemene Home Assistant-executor niet
  kan uitputten.
- Bewaar een nog lopende DWD-parse na een provider-timeout. Een volgende
  vijfminutencyclus wacht op dezelfde taak en start geen tweede parse-thread.
  Het resultaat wordt alsnog overgenomen zodra de verwerking klaar is.

# 0.4.131

- Herstel onafgewerkte virtuele verkenners tot 24 uur na hun voorspelde
  controlemoment, zodat verlopen controles na een herstart eerst een expliciet
  `niet_verifieerbaar` eindresultaat krijgen voordat ze worden opgeruimd.
- Behoud de oorspronkelijke forecast in de verificatiedatabase en voorkom dat
  het databaseherstel de afsluitlogica voortijdig omzeilt.

# 0.4.130

- Sluit een virtuele regenverkenner uiterlijk dertig minuten na het
  voorspelde controlemoment af wanneer het gekozen Netatmo-station ontbreekt
  of geen actuele meting meer levert.
- Bewaar zo'n controle als `niet_verifieerbaar`, verwijder hem uit de actieve
  verkenners en tel hem niet als droge voorspelling. Ook uit de database
  herstelde oude verkenners worden hierdoor automatisch opgeschoond.
- Publiceer afzonderlijk hoeveel controles niet verifieerbaar waren.

# 0.4.126

- Laat de virtuele regenverkenner ieder radarsysteem met minstens twee geldige
  bewegingsframes onderzoeken, ook bij lage of nog onvoldoende operationele
  betrouwbaarheid. Gebruik daarvoor een afzonderlijke passieve
  vectorprojectie die nooit ETA's of waarschuwingen beïnvloedt.
- Geef onzekere systemen juist meer kalibratieprioriteit en behoud de
  gemeten betrouwbaarheid als label bij forecast en uitkomst.
- Maak actieve verkenners selecteerbaar als kaartcentrum, naast Home en de
  gewone trackers.

# 0.4.125

- Voeg passieve virtuele regenverkenners toe die voorspelde radarpassages
  achteraf met regenmeters controleren en forecast plus uitkomst in de
  kalibratiedatabase bewaren.
- Behoud per regio de vaste Netatmo-query van 20 km en voeg maximaal één
  gedeelde reizende query toe voor België, Italië en toekomstige regio's.
  Lopende verificaties krijgen voorrang; anders kiest de verkenner automatisch
  de buibaan met de hoogste kalibratiewaarde.
- Beperk de reizende query tot eenmaal per tien minuten en houd haar volledig
  buiten operationele ETA's, waarschuwingen en stormdetectie.
- Publiceer actieve verkenners, hun route en recente nat/droog-uitkomsten in
  de GeoJSON-kaartfeed. Voeg begrijpelijke statusteksten, resterende minuten
  en timingafwijkingen toe aan de verkennerssensor en kaartlabels.

# 0.4.124

- Kalibreer de nieuwe betrouwbaarheidsscore conservatiever op basis van de
  eerste live regencyclus: een randpassage kan maximaal `Matig` scoren en een
  hindcastfout boven 5 km begrenst de totaalscore. Een fout van 6,52 km wordt
  daardoor niet langer als hoge betrouwbaarheid gepresenteerd.

# 0.4.123

- Publiceer `nadert_regio` wanneer een systeem wel naar de omgeving beweegt
  maar de targetcorridor mist. Markeer dit expliciet met
  `local_passage_expected=false` en `warning_eligible=false`.
- Laat de meldingspipeline uitsluitend het centrale `warning_eligible`-veld
  gebruiken, zodat een voorspelde misser nooit als lokale regenwaarschuwing
  kan worden verstuurd.
- Publiceer een uitlegbare betrouwbaarheidsscore van 0–100 met afzonderlijke
  componenten voor tracking, frameopbouw, beweging, hindcast, passage en
  beschikbare radarbron.

# 0.4.122

- Ververs de pySTEPS-preflightsensor onmiddellijk wanneer een dynamische regio
  door een tracker wordt toegevoegd. Daardoor verschijnt de reeds berekende
  herstartaudit van iedere regio direct, zonder te wachten op een volgend
  radarbron-event.

# 0.4.121

- Voer cacheherstel en stormobject-audit ook uit op het exacte moment dat een
  secundaire regio later door een tracker wordt aangemaakt. Dit dekt Italië
  ook wanneer de tracker tijdens de eerste HA-startlus nog niet beschikbaar
  was.

# 0.4.120

- Herstel na Home Assistant-start ook de pySTEPS-rastercache van secundaire
  regio's die pas door hun tracker worden aangemaakt. Bouw daarna de
  stormobject-herstartaudit opnieuw op, zodat zowel België als Italië altijd
  expliciet `hersteld`, `ontbrekend_stormobject` of
  `geen_recente_radarcache` publiceren.

# 0.4.119

- Bewaak de ETA-continuïteit per target voor hetzelfde gevolgde stormsysteem.
  Een nieuwe radarcontour mag een bijna verstreken ETA niet meer terugzetten
  naar tientallen minuten en zo een dubbele vroegwaarschuwing veroorzaken.
- Laat echte systeemwissels en normale kleine ETA-correcties ongemoeid en
  publiceer bij een begrensde sprong ook de ruwe ETA en de reden als
  diagnostische attributen.
- Herstel RainViewer-stormobjecten na een Home Assistant-herstart met dezelfde
  lifecycletermijnen als tijdens de normale runtime. Een object van tien
  minuten oud blijft daardoor actief en matchbaar, behoudt zijn ID, frameketen,
  vector en ETA in plaats van als nieuw éénframe-object te herbeginnen.
- Controleer bij iedere herstart of recente pySTEPS-radarframes ook een
  overeenkomstig hersteld stormobject hebben. De pySTEPS-preflight publiceert
  per regio `hersteld`, `ontbrekend_stormobject` of
  `geen_recente_radarcache`, inclusief de herstelde systeem-ID's.

# 0.4.117

- Bouw de GeoJSON-kaartcollectie nog maar één keer per samengevoegde
  sensorupdate en hergebruik die snapshot voor state en attributen.
- Vervang de herhaald geneste bliksemclustering door een begrensde
  connected-components-berekening die ieder puntenpaar hoogstens één keer
  vergelijkt. Dit vermindert blokkering van de Home Assistant-hoofdthread
  zonder de transitieve clustervorming te veranderen.

# 0.4.116

- Laat een betrouwbare naderende bui de operationele ETA- en
  waarschuwingsfocus overnemen van een actuele maar wegtrekkende contour. De
  dichtstbijzijnde waarneming blijft daarnaast afzonderlijk zichtbaar.
- Publiceer alle relevante naderende systemen in `approaching_systems`, met
  afstand, ETA, passage, naderingssnelheid, vertrouwen en selectie-indicatie.
  Daardoor blijft een volgende bui gevolgd wanneer de vorige nog wegtrekt.

# 0.4.115

- Laat de echte bewegingsrichting primeren op een geometrische contourpassage:
  een brede radarcontour die het target op het huidige tijdstip nog raakt,
  maar waarvan het systeem aantoonbaar wegtrekt, wordt niet langer als
  `naderend` met ETA 0 gepubliceerd.
- Behoud voor zo'n aflopende passage de actuele contourafstand en
  passage-informatie, maar verwijder de aankomst-ETA en blokkeer de verwachting
  met `systeem_trekt_weg`.

# 0.4.114

- Voeg naast iedere brede regionale Netatmo-opvraging altijd een klein lokaal
  vak van 20 km toe. Voeg beide resultaten op station-ID samen, zodat de
  publieke API nabijgelegen stations niet meer kan verliezen door de grote
  regionale bounding box.
- Bewaar `rain_60min` als echte uuraccumulatie naast de bestaande afgeleide
  vijfminutenwaarde. Lokale actuele regenbevestiging blijft gebaseerd op
  `rain_live`, zodat recente maar inmiddels gestopte regen geen fout-positieve
  bevestiging veroorzaakt.

# 0.4.113

- Herstel de opstartbarrière uit 0.4.112: de wachttimer is geen door Home
  Assistant gevolgde setup-taak meer. Daardoor ontstaat geen wederzijdse
  blokkering tussen `Opstarten aan het afronden` en het wachten op de status
  `running`; de eerste Storm Tracker-cyclus start daarna als achtergrondtaak.
- Laat twee of meer natte Netatmo-stations binnen 10 km lokale regen meteen
  bevestigen. De status wordt dan `bevestigd`, met ETA 0 en
  `netatmo_grondmeting` als basis, in plaats van een melding dat neerslag nog
  onderweg is.
- Behandel targets zonder beschikbare coördinaten veilig tijdens de opstart.

# 0.4.112

- Zet alle provideractiviteit achter een centrale, niet-blokkerende
  Home Assistant-startbarrière. Blitzortung, intervalpolls, de eerste volledige
  providercyclus en diagnostische satellietpolls starten pas wanneer HA
  werkelijk `running` is.
- Controleer de startbarrière iedere 180 seconden. Bij een timeout blijft Storm
  Tracker gepauzeerd en wordt een waarschuwing gelogd; de integratie blijft
  wachten en belast of blokkeert de HA-opstart niet.
- Laat ook live gewijzigde bliksembroninstellingen tijdens de opstart alleen
  opslaan; de effectieve providerwissel volgt pas nadat HA gestart is.

# 0.4.111

- Voeg aan de algemene en alle target-neerslagstatussen een locatiegerichte
  Netatmo-samenvatting toe: stations en natte stations binnen 10 km,
  dichtstbijzijnde station, afstand en mm/u van de dichtstbijzijnde natte
  meter, lokale maxima/gemiddelden en dataouderdom.
- Maak het bewijsniveau expliciet: één nat particulier station is een
  `regen_aanwijzing`; pas minstens twee onafhankelijke natte stations binnen
  10 km leveren `regen_bevestigd`. Het ontbreken van een nat station wordt
  niet gelijkgesteld aan bewezen droog weer.

# 0.4.110

- Bereken de actuele buirand uitsluitend uit radarcellen van hetzelfde nieuwste
  frame dat op de kaart wordt getoond. Oudere cellen blijven beschikbaar voor
  track- en bewegingshistorie, maar kunnen niet langer de huidige afstand,
  systeemselectie of ETA sturen.
- Negeer radarsystemen zonder cellen in het actuele frame bij de
  neerslagselectie. Dit voorkomt sprongen zoals een zichtbare bui boven de
  locatie terwijl de status ten onrechte een buirand op tientallen kilometers
  meldt.

# 0.4.109

- Voert de onafhankelijke KMI- en KNMI-kalibratiepolls parallel uit. Daardoor
  stapelen netwerkvertragingen van beide bronnen niet langer op tot de harde
  `radar_comparison`-timeout, terwijl beide bronnen volledig beschikbaar
  blijven voor vergelijking en autokalibratie.

# 0.4.108

- Gebruik de begrensde pySTEPS-rastervector als vroege bewegingsfallback
  wanneer de klassieke centroidvector nog onvoldoende data heeft. Vanaf twee
  frames verschijnt een voorlopige vector en ETA met expliciete
  betrouwbaarheidsscore; die veroorzaakt nog geen harde waarschuwing.
- Laat een vanaf drie frames geverifieerde vector met voldoende IoU en
  consistentie de operationele ETA, prognose en bestaande waarschuwingstrappen
  voeden. Klassieke beweging met matig of hoog vertrouwen blijft altijd
  primeren.
- Bewaar maximaal zes recente pySTEPS-rasters per regio in de bestaande
  secundaire SQLite-database (schema 8) en herstel na herstart uitsluitend
  frames die jonger dan dertig minuten zijn en exact bij bron en regiogrid
  passen. Een deploy hoeft daardoor niet opnieuw vanaf een lege framebuffer
  op te warmen.

# 0.4.107

- Houd radarsystemen minstens vijftien minuten actief en dertig minuten
  herstelbaar. Dit overbrugt de tienminutencadans van RainViewer, zodat
  bevestigde regen niet halverwege een frame-interval tijdelijk als droog
  verschijnt en systeem-ID, radarframehistorie, bewegingsvector en ETA niet
  bij ieder nieuw frame opnieuw vanaf nul beginnen.
- Behoud de bestaande vijf- en vijftienminutenlifecycle voor systemen zonder
  radarhistorie, zodat bliksemruis niet langer actief blijft.

# 0.4.106

- Voeg in de configuratie-UI de live schakelbare analysemodus `Full send` /
  `Thermal throttle` toe. Full send voert pySTEPS en providerkalibratie iedere
  cyclus volledig uit; thermal throttle bewaart de stille kwartier- en
  temperatuurbegrenzing. Nieuwe en bestaande installaties starten standaard
  in full-sendmodus.
- Vectoriseer de pySTEPS-rastermatching met NumPy en analyseer maximaal twee
  RegionEngines tegelijk via de HA-executor. Dit verdeelt de native
  rasterberekening over hoogstens twee cores zonder agressieve all-corelast;
  de stateful kalibratieobserver blijft bewust serieel.
- Laat een gezonde lokale radarbron niet langer blind primeren wanneer
  minstens dertig onafhankelijke vergelijkingen aantonen dat ze minder dan
  25% van de natte RainViewer-cellen ziet. Schakel dan uitlegbaar over op
  RainViewer; dit herstelt precies het in Godinne bevestigde detectiegat.
- Begrens de observerende providerkalibratie tot één exact kwartierframe.
  Operationele radar, stormdetectie en waarschuwingen blijven ongewijzigd
  iedere vijf minuten draaien.
- Sla experimentele kalibratie en pySTEPS-preflight tijdelijk over wanneer de
  gemeten CPU-temperatuur al 75 °C of hoger is.
- Bereken kleine biasverschuivingen zonder per kandidaat een tijdelijke set te
  bouwen en aggregeer intensiteitsverschillen zonder een tussenlijst.

# 0.4.105

- Verminder de CPU-piek van pySTEPS-gridmatching: bouw de waargenomen
  radarcellen nog maar eenmaal per framepaar op en tel IoU per verschuiving
  zonder voor iedere kandidaat meerdere grote tijdelijke sets te alloceren.
- Hergebruik de vertaalmatch van een ongewijzigd framepaar in de volgende
  cyclus. Hierdoor wordt de vorige vector niet telkens opnieuw berekend.

# 0.4.104

- Herken de daadwerkelijke System Monitor-entiteiten van deze installatie voor
  CPU-belasting en processortemperatuur.
- Splits de radartelemetrie per regio verder uit in rasternormalisatie en
  pySTEPS-preflight, zodat de resterende thermische piek exact aan één
  rekenstap gekoppeld kan worden.

# 0.4.103

- Bereken de neerslagsamenvatting per target nog maar eenmaal per HA-statewrite
  en hergebruik dezelfde snapshot voor state en attributen. Dit halveert het
  zwaarste bevestigde geometriewerk in de eventloop.
- Zoek het dichtstbijzijnde radarpunt zonder voor iedere radarcel een tijdelijke
  kandidatenlijst aan te leggen.
- Meet per providerfase looptijd, resultaat, CPU, CPU-temperatuur, geheugen,
  load en fanwaarde en bewaar die in de secundaire database (schema 7), zodat
  systeempieken rechtstreeks aan Storm Tracker-verwerking gekoppeld worden.

# 0.4.102

- Toon voor bevestigde systemen ook bij lage bewegingsconfidence de waargenomen
  richting als `naderend`, `wegtrekkend` of `langs_trekkend`; de afzonderlijke
  ETA- en waarschuwingsgates blijven conservatief.
- Rapporteer bij een wegtrekkend systeem expliciet `systeem_trekt_weg` als
  reden waarom geen verwachting beschikbaar is, in plaats van de gebruiker
  alleen een generieke melding over onvoldoende bewegingsbetrouwbaarheid te geven.
- Publiceer `direction_reliable` los van `eta_reliable`, zodat dashboards een
  onzekere richting zichtbaar kunnen kwalificeren zonder haar te verbergen.

# 0.4.101

- Voeg een fysiek begrensde rastermatching toe aan de pySTEPS-preflight. De
  volledige neerslagvorm bepaalt voortaan de experimentele vector; de oude
  intensiteitscentroid blijft als vergelijkende nulmeting beschikbaar.
- Vereis twee opeenvolgende, voldoende consistente vectoren voordat een
  preflightmeting `ready` wordt. Lage overlap, sterke richtingswissels en
  onrealistische zwaartepuntverschuivingen worden observerend gemarkeerd als
  `unstable_motion`.
- Bewaar methode, confidence, raster-IoU, vectorconsistentie, groei/krimp en
  centroidbaseline via een niet-destructieve database-upgrade naar schema 6.

# 0.4.100

- Bewaar iedere pySTEPS-preflightmeting voortaan ook in de secundaire
  kalibratie- en verificatiedatabase. Per regio, bron en nominale minuut worden
  status, gebruikte frames, translatiessnelheid, richting, verificatie-IoU en
  reden opgeslagen.
- Migreer de SQLite-database niet-destructief naar schema 5. Bestaande frames,
  vergelijkingen, targetverificaties en biasprofielen blijven behouden.

# 0.4.99

- Begrens de HA- en WebSocketbelasting van bliksemfeeds zonder inslagen te
  verliezen. Alle observaties worden onmiddellijk intern verwerkt, terwijl
  sensorupdates per venster worden samengevoegd en bij hoge invoerdruk
  automatisch vertragen van 2 naar 5 of 10 seconden.
- Ververs de GeoJSON-kaart via een afzonderlijk venster van 10 seconden en
  schrijf Blitzortung-logregels per batch in één executor-job. Hierdoor groeit
  het aantal HA-events en schrijfopdrachten niet langer lineair mee met een
  actieve onweersbui.

# 0.4.98

- Publiceer de compacte observatiesensor `STV3 pySTEPS Preflight`. De toestand
  is het aantal regio's met een bruikbare experimentele bewegingsschatting; de
  attributen bevatten per regio richting, snelheid en verificatiescore. Er
  worden geen rasterwaarden opgeslagen en de sensor blijft strikt observerend.

# 0.4.97

- Voeg een strikt observerende pySTEPS-preflight toe. Zij meet uit maximaal
  drie opeenvolgende genormaliseerde radarframes een experimentele translatie
  en toetst die tegen het volgende echte frame met een overlapscore. De uitkomst
  blijft diagnose; ze beïnvloedt geen traject, ETA, waarschuwing of sensor.

# 0.4.96

- Voeg de observerende invoerlaag voor de toekomstige pySTEPS-worker toe.
  Geselecteerde radaroverlays worden per RegionEngine op een vast, begrensd
  raster en op nominale minuten genormaliseerd; per regio blijven hoogstens zes
  frames in het geheugen. De buffer stuurt niets naar OFE, StormEngine of
  waarschuwingen en publiceert alleen compacte diagnostiek.

# 0.4.95

- Publiceer onmiddellijk een `storms_updated`- en targetupdate wanneer een lege
  RegionEngine na een trackerbeweging wordt verwijderd. Daardoor verdwijnt een
  storm uit de oude regio meteen uit `STV3 Actieve Storms` en blijven ook de
  interne MCS-statussen niet als verweesde cache-items achter.

# 0.4.94

- Behandel de OPERA-QI voortaan als vertrouwenssignaal in plaats van als harde
  regen/geen-regenbeslissing. Verse, sterke en ruimtelijk samenhangende echo's
  uit het officiële OPERA-composiet blijven operationeel zichtbaar, ook
  wanneer de samengestelde kwaliteitswaarde laag of onbekend is.
- Behoud de strenge clutterbescherming voor zwakke lage-kwaliteitsecho's, maar
  vergelijk daarvoor volledige OPERA- en nationale radarfootprints. Een
  DPC/RainViewer-cel binnen een brede OPERA-footprint kan zo het werkelijk
  overlappende deel bevestigen zonder een volledige zwakke megacel vrij te
  geven.
- Voeg regressiedekking toe voor het onweerscomplex Venetië–Rijeka dat door
  DPC en Blitzortung werd bevestigd maar door de oude OPERA-puntfilter te klein
  werd weergegeven.

# 0.4.93

- Werk de veilige database-analyse voor `sample_types` en `target_samples`
  onmiddellijk bij zodra een nieuwe verificatiesnapshot is opgeslagen. Daardoor
  verschijnt ook de later geïnitialiseerde fictieve tracker meteen als achtste
  target, zonder maximaal twaalf batches op de volgende volledige analyse te
  wachten. Herhaalde upserts tellen niet dubbel.
- Koppel de interne ItaliaMeteo ICON-2I-modelaanvraag aan dezelfde
  `open_meteo_enabled`-optie als de centrale broker. Wanneer Open-Meteo uitstaat,
  blijft alleen de officiële ItaliaMeteo-radarcatalogus actief en worden geen
  verborgen Open-Meteo-verzoeken meer uitgevoerd; de lifecycle-diagnostiek
  publiceert dan expliciet `forecast_status: disabled`.

# 0.4.92

- Voer schema v4 van de kalibratiedatabase in en wis bij de upgrade eenmalig
  alle oudere frames, vergelijkingen, verificaties en afgeleide biasprofielen.
  De oude dataset kon provideractivatiemarges met echte radardekking
  verwarren en was daardoor niet wetenschappelijk zuiver herstelbaar.
- Bewaar per bronframe de expliciete geografische dekking en vergelijk twee
  providers alleen nog binnen minstens zestig procent gemeenschappelijk
  RegionEngine-gebied. Een lege KMI-rand bij Milaan kan daardoor niet meer als
  een gemiste Italiaanse bui worden gescoord.
- Leg iedere vijf minuten voor elk beschikbaar target een afzonderlijke
  verificatiesnapshot vast op de actuele GPS-coördinaten, inclusief eigen
  verwachting, ETA, passage, confidence, bronbeslissing en kandidaatmelding.
  De twee Buienradar-verificaties blijven correct beperkt tot thuis.
- Publiceer veilige database-analyses per sampletype, target, bron en
  providerpaar, plus schemaversie, laatste resetreden en resetmoment.
- Zet Open-Meteo standaard uit. De broker doet dan geen netwerkverzoeken en
  publiceert expliciet `DISABLED`; inschakelen blijft optioneel via de
  integratie-opties.
- Laat `STV3 Actieve Storms` alle RegionEngines tellen in plaats van alleen de
  thuisengine en geef bij een gezond leeg KMI-frame `droog` weer in plaats van
  het misleidende `geen data`.

# 0.4.91

- Classificeer Open-Meteo expliciet als `model_guidance` in plaats van
  grondvalidatie; modeldata blijft buiten radarcelvorming en kan lokale radar
  of bliksem niet onderdrukken.
- Breid de doelgerichte aanvraag uit naar negentien compacte stormvelden:
  neerslag, neerslagkans, CAPE, Lightning Potential, Lifted Index, convectieve
  inhibitie, druk, wolken, vriesniveau, windstoten en wind op 700/850 hPa.
- Behoud de bestaande 90-minutenwaarden en voeg drie uur kwartierbegeleiding
  en zes uur uurlijkse modelcontext toe.
- Bewaar ontbrekende modelvelden als onbekend in plaats van nul en publiceer
  per target welke variabelen werkelijk beschikbaar waren.
- Breid targetstatussen, de Open-Meteo-gearsensor, CSV en de permanente
  kalibratiedatabase uit met de nieuwe modelbegeleiding.
- Leg de veilige architectuur voor een latere, geïsoleerde pySTEPS-nowcastworker
  vast; pySTEPS draait niet in Home Assistant of AppDaemon.

# 0.4.90

- Vervang de 324-puntencirkel per RegionEngine door één centrale,
  targetgerichte Open-Meteo-broker.
- Dedupliceer personen op praktisch dezelfde locatie tot één modelpunt en
  gebruik de gedocumenteerde multi-coordinate GET-interface.
- Beperk de Open-Meteo-forecast werkelijk tot zeven kwartierstappen; de oude
  code nam onbedoeld het maximum over de volledige standaardforecast.
- Houd Open-Meteo volledig uit radarcelvorming en de Observation Fusion
  Engine: het is modelcontext en verificatie, geen radarwaarneming.
- Maak `initializing`, `rate_limited`, `stale`, time-outs, laatste poging,
  laatste succes, dataleeftijd en fouttellers expliciet zichtbaar. Een nooit
  opgehaalde beginwaarde kan daardoor niet meer als droog weer doorgaan.
- Publiceer de modelverwachting per target in de bestaande neerslagstatussen
  en bewaar iedere succesvolle targetforecast in CSV én de permanente
  kalibratiedatabase.

# 0.4.89

- Maak in iedere targetstatus expliciet zichtbaar of de dichtstbijzijnde
  neerslagrand en het voor prognose gevolgde systeem hetzelfde stormobject
  zijn, inclusief beide storm-ID's en beide frametellers.
- Migreer de kalibratiedatabase naar schema v3 en leid uit de bestaande
  historische bronvergelijkingen onmiddellijk richtingsgebonden
  bronwisselprofielen af.
- Bewaar per bronrichting en geografische RegionEngine de detectieverhouding,
  extra nat oppervlak, oppervlakteschaal, F1-score, intensiteitsbias,
  rasterverschuiving en productlatentie; onderhoud daarnaast een globale
  fallback per bronpaar.
- Gebruik een voldoende gevuld profiel bij een providerwissel om de tijdelijke
  betrouwbaarheidsmarge en overgangsduur conservatief aan te passen. Zonder
  profiel blijft de bestaande marge van tien procentpunten gedurende tien
  minuten behouden.
- Laat de geleerde profielen in deze eerste operationele fase uitsluitend op
  betrouwbaarheid werken: officiële radarpixels, afstanden en intensiteiten
  blijven ongewijzigd.
- Publiceer aantallen biassamples, biasprofielen en maximaal twintig compacte
  profielsamenvattingen via de bestaande autokalibratiesensor.

# 0.4.88

- Vergelijk voor ieder bevestigd weersysteem een rechtlijnig traject met een
  constant-versneld traject en kies de gebogen baan alleen wanneer een
  rollende hindcast aantoonbaar beter is.
- Gebruik uitsluitend radarcentroids voor de neerslagbaan zolang radarframes
  beschikbaar zijn; bliksem blijft alleen een expliciete fallback en trekt de
  regenbaan niet meer kunstmatig naar een actieve flank.
- Projecteer de actuele tweedimensionale radarcontour over 90 minuten en
  rapporteer afzonderlijk een exacte contour-ETA en een conservatieve ETA via
  de groeiende onzekerheidscorridor.
- Laat een gevalideerde gebogen passage als `naderend` gelden, ook wanneer de
  laatste momentane snelheidsvector nog zijwaarts wijst.
- Bewaar iedere vijf minuten de eigen thuisverwachting samen met de twee
  Buienradar-neerslagverwachtingssensoren in de permanente
  kalibratiedatabase.
- Bewaar na een geslaagde notificatie een onveranderlijke waarschuwingssnapshot
  met target, ETA-basis, passage, bron, intensiteit, baanmodel en hindcastfout,
  zodat latere sensorupdates de historische melding niet vervormen.
- Maak de Observation Fusion Engine voor verificatie deterministisch leeg
  voordat de cyclus wordt opgeslagen; hierdoor bevat een verificatierecord
  nooit onbedoeld de vorige providercyclus.
- Breid de collectortellers uit met verificatie- en waarschuwingssamples.

# 0.4.87

- Scheidt de dichtstbijzijnde actuele buienrand van het systeem dat voor
  beweging en prognose wordt gevolgd.
- `distance_km` blijft de operationele veiligheidsafstand en verwijst nu altijd
  naar de dichtstbijzijnde waargenomen neerslagrand.
- Voegt afzonderlijke rand- en centrumafstanden toe voor de dichtstbijzijnde
  neerslag en het gevolgde systeem.
- Markeert een ETA pas als betrouwbaar bij een bevestigde bewegingsvector met
  minstens matige betrouwbaarheid.
- Rapporteert waarom een prognose geblokkeerd blijft en toont de onderliggende
  hoeveelheid, tijdspanne en fitkwaliteit van de bewegingsmetingen.

# 0.4.86

- Detecteer expliciet wanneer de gevraagde RegionEngine-radius door de rand van
  het OPERA-composiet wordt afgeknipt.
- Laat RainViewer operationeel overnemen wanneer OPERA aan zo'n dekkingsrand
  alleen onbevestigde cellen toont maar RainViewer wel lokale neerslag ziet.
- Behoud OPERA wanneer de randcellen onafhankelijk bevestigd zijn.
- Gebruik voor zichtbare en verborgen bliksemsymbolen exact dezelfde
  geografische bliksemhull; de schakelaar wijzigt alleen nog de symbolen.

# 0.4.85

- Publiceer goedkope persistente collectortellers voor frames, rasterpunten,
  vergelijkingen, bronnen, regio's, databereik en totale SQLite/WAL-grootte.
- Toon deze status in het technische paneel van de multi-targetkaart, inclusief
  de omvang van de laatste schrijfbatch.
- Herstel de tellers bij het opstarten rechtstreeks uit de database en vermijd
  daarna dure volledige tellingen tijdens iedere vijfminutencyclus.
- Voeg een afzonderlijke rode pulserende bliksemenvelop toe rond inslagen van
  de laatste vijf minuten, ook buiten de radarregen; de regenfootprint zelf
  blijft exact ongewijzigd.
- Cluster bliksemzones regionaal en markeer diagnostisch of radarneerslag zich
  binnen 25 km bevindt of dat het een `lightning_only`-zone is.

# 0.4.84

- Leg alle regionale providerframes, inclusief geldige droge frames, permanent
  vast in een afzonderlijke SQLite-database onder Home Assistant `.storage`.
- Bewaar per frame de bron, geografische enginesleutel, producttijd en alle
  bezette 0,10-gradenrasterpunten met maximale intensiteit en kwaliteit.
- Bewaar iedere exacte paarsgewijze vergelijking append-only voor latere
  offline analyse; er geldt bewust geen automatische retentie of kalibratie.
- Gebruik WAL, transacties en schrijven buiten de HA-eventloop, en plaats een
  mislukte batch terug in het geheugen voor een volgende poging.

# 0.4.83

- Vervang de OPERA-centrische kalibratie door regionale kruisvalidatie tussen
  alle gelijktijdig beschikbare operationele neerslagproviders.
- Rapporteer scores per providerpaar en RegionEngine zonder een bron vooraf als
  absolute waarheid te behandelen.
- Koppel wachtende vergelijkingsframes aan zowel runtime-engine als stabiele
  geografische sleutel, zodat een verplaatsende persoon nooit met frames uit
  zijn vorige regio wordt vergeleken.
- Houd kalibratie observerend: de gemeten scores wijzigen nog geen operationele
  filters of providerdata.

# 0.4.82

- Isoleer de OPERA- en RainViewer-fetches per RegionEngine: regio's worden
  parallel verwerkt en iedere fetch heeft een eigen harde timeout, zodat een
  trage buitenlandse regio de overige targets niet ophoudt.
- Voeg een circuit breaker toe aan nationale providers: na drie opeenvolgende
  fouten volgt vijftien minuten cooldown en daarna één gecontroleerde proef.
- Markeer een bronwissel tien minuten lang als overgang en verlaag tijdens die
  periode de prognosezekerheid met tien procentpunten. De echte pixels van de
  nieuwe bron blijven ongewijzigd; incompatibele radarrasters worden niet
  kunstmatig gemiddeld.
- Activeer een regionale luchtdruktrend pas na dertig minuten aaneengesloten
  historie van minstens drie vergelijkbare stations. Tijdens de opwarming
  blijft de trend expliciet `onvoldoende_data`/`initializing`.

# 0.4.81

- Begrens iedere nationale providerpoll afzonderlijk tot twintig seconden en
  haal onafhankelijke nationale providers parallel op.
- Begrens daarnaast de vier volledige fasen van de vijfminutencyclus, zodat
  een hangende externe verbinding de volgende cyclus nooit onbeperkt blokkeert.
- Voeg harde timeouts en foutdiagnostiek toe aan EUMETSAT LI en NOAA GOES GLM.
- Leg cold-startgedrag vast: ontbrekende drukhistory blijft neutraal als
  `onvoldoende_data`, een lege Open-Meteo-cache blijft `initializing` en er
  worden geen drukval, confidence of regenobservaties verzonnen.
- Behoud het bestaande globale Netatmo-entitycontract: dezelfde unique ID,
  altijd beschikbaar, `unknown` in plaats van een misleidende nulwaarde zolang
  geen betrouwbare 60-minutentrend bestaat, en uitsluitend gekoppeld aan home.

# 0.4.80

- Audit de volledige providerketen van geografische activatie tot kaartoverlay,
  inclusief lokale radar, continentale fallback, satelliet, bliksem en
  grondvalidatie.
- Activeer gedeelde KMI- en KNMI-radar op basis van alle RegionEngines in
  plaats van uitsluitend de thuislocatie.
- Beschouw een geldig droog KNMI-frame als gezond en bepaal de gezondheid van
  nationale radar uit de werkelijke producttijd in plaats van de polltijd.
- Behandel MeteoLux en ItaliaMeteo uitsluitend als validatie/nowcast; zij
  kunnen OPERA of een echte lokale realtime-radar niet meer blokkeren.
- Isoleer Open-Meteo-provider, modelgrid, resultaat en routing per
  RegionEngine, zodat verre targets geen gegevens met thuis delen.
- Vervang gelijktijdige losse polls door één vergrendelde providercyclus:
  lokale bronnen, radarvergelijking, bronselectie en daarna grondvalidatie.
- Voeg end-to-end contracttests toe voor providerfrequentie, regionale
  isolatie, hiërarchie, routing, overlays en bliksemscheiding.

# 0.4.79

- Isoleer Netatmo-stations, luchtdrukhistoriek en druktrends per RegionEngine;
  een verre testtracker kan daardoor nooit meer zijn Miami-data aan Belgische
  targets of de thuissensor doorgeven.
- Poll alle actieve regio's gelijktijdig met gedeeld OAuth-tokenbeheer en start
  na een verre targetverplaatsing meteen een regionale Netatmo-meting.
- Houd de bestaande globale Netatmo-sensor expliciet gekoppeld aan `zone.home`
  en publiceer diagnostiek over de afzonderlijke regionale trends.
- Migreer de bestaande globale drukhistoriek veilig naar de thuisregio en
  bewaar voortaan alle RegionEngine-histories in dezelfde HA-opslag.

# 0.4.71

- Voeg H SAF H40B toe als gedeelde Meteosat-neerslagfallback voor Europa,
  Afrika, het Middellandse Zeegebied en aangrenzende Atlantische regio's.
- Download maximaal één centraal full-diskframe per cyclus en decodeer alleen
  de geografische vensters rond RegionEngines die satellietfallback nodig
  hebben; lokale radar en OPERA laten de provider slapen.
- Verwerk de echte H40B NetCDF-projectie, instantane `rr`-neerslagintensiteit,
  `qind`-pixelkwaliteit, bronpixels, componentfootprints en producttijd.
- Laat H40B een lege RainViewer-regio overnemen wanneer satellietneerslag wel
  aanwezig is en gebruik H40B daarnaast als onafhankelijke OPERA-bevestiging.
- Neem iedere zes uur een beperkte kalibratieprobe en vergelijk alleen frames
  met exact dezelfde nominale minuut; lokale officiële radar blijft referentie.
- Voeg H SAF FTP-credentials toe aan de configuratie-UI en verbeter de aparte
  toegangstest met host-, pad-, TLS- en FTP-diagnose.

# 0.4.70

- Publiceer de gegeorefereerde lokale OPERA-DBZH-pixels via hetzelfde
  intensiteitsruncontract als alle nationale rasterproviders.
- Beperk de OPERA-kaartlaag tot cellen die door de bestaande kwaliteits- en
  corroboratiefilter zijn aanvaard; afgekeurde clutter wordt niet getekend.
- Voeg KNMI API- en WMS-sleutels toe aan de UI-configuratie en geef ze vanuit
  een config entry daadwerkelijk aan de KNMI-provider door.
- Voeg een Météo-France APPLICATION_ID toe en vernieuw het kortlevende OAuth2-
  toegangstoken automatisch; een tijdelijk handmatig token blijft fallback.

# 0.4.69

- Publiceer echte intensiteitsruns voor alle beschikbare bronrasters: KMI,
  KNMI, DWD, AEMET, DPC, RainViewer, Météo-France en Met Office.
- Koppel iedere kaartoverlay aan de actieve radarprovider van de geselecteerde
  RegionEngine; providers zonder bronpixels blijven eerlijke contourdata.
- Breid de kaartlegenda uit tot zes neerslagklassen.
- Cluster zichtbare blikseminslagen om het radarbeeld leesbaar te houden.
- Laat bij verborgen inslagen de getroffen neerslagpixels knipperen wanneer
  daar in de voorbije vijf minuten actieve bliksem werd gemeten.

# 0.4.68

- Publiceer voor DPC Italië de werkelijk natte bronpixels als korte,
  gegeorefereerde rasterruns per intensiteitsniveau; droge gaten blijven leeg.
- Teken de operationele radarlaag met een KMI/Buienradar-achtige kleurenschaal
  en verberg uniforme componentvullingen standaard wanneer rasterdata bestaat.
- Voeg een kaartknop `techniek` toe waarmee interne WeatherSystem- en
  radarcelcontouren afzonderlijk kunnen worden gecontroleerd.
- Ondersteun `MultiPolygon` volledig in de custom kaart en houd de rasterlaag
  gekoppeld aan de geselecteerde RegionEngine en diens actieve provider.

# 0.4.67

- Behoud maximaal 2048 geordende punten van gesloten bronpixelringen in de
  kaartfeed in plaats van ze willekeurig tot 48 punten en lange koorden terug
  te brengen.
- Publiceer WeatherSystems met meerdere actuele broncontouren als GeoJSON
  `MultiPolygon`, zodat losse regenvelden niet meer door een kunstmatige
  convexe oranje driehoek worden verbonden.

# 0.4.66

- Vervang regelmatige rasterpuntbemonstering door echte 4-connected
  neerslagcomponenten met gesloten buitenranden op bronpixelniveau.
- Pas hetzelfde rastercontract toe op KMI, KNMI, DPC Italië, DWD Duitsland,
  AEMET Spanje, RainViewer en nationale ODIM-producten zoals Météo-France en
  Met Office; OPERA behield zijn bestaande exacte pixelgrenzen.
- Geef iedere component deterministische cel- en parent-ID's per radarframe,
  zodat opeenvolgende frames betrouwbaar kunnen worden geteld en gematcht.
- Match WeatherSystems tegen hun werkelijke footprints en bouw hun geometrie
  uit recente broncontouren in plaats van kettingen van regelmatige punten.
- Behoud gesloten bronringen in GeoJSON zonder ze opnieuw tot een kunstmatige
  convexe driehoek of veelhoek te reduceren.

# 0.4.65

- Kies radarbronnen per RegionEngine in vaste volgorde: operationele lokale
  bron, gevalideerde OPERA-observaties en daarna RainViewer.
- Laat afgekeurde ruwe OPERA-cellen de RainViewer-fallback niet langer
  blokkeren; bronkeuze en diagnose gebruiken nu aanvaarde cellen per engine.
- Gebruik dezelfde bronbeslissing voor routering, kaart en targetstatussen.
- Meld `alleen_bliksem` wanneer actuele bliksem aanwezig is zonder een
  bevestigde neerslagcel, in plaats van dit als `droog` weer te geven.
- Normaliseer gelokaliseerde landnamen en landcodes voor deterministische
  selectie van nationale providers.

# 0.4.64

- Bouw OPERA-celpolygonen uit de werkelijke buitenranden van de onderliggende
  1 km-rasterpixels in plaats van grof bemonsterde footprint-punten.
- Valideer de polygonoppervlakte tegen het aantal bronpixels en val bij
  ambigue of meervoudige topologie veilig terug op een punt.
- Vernieuw vorm en geografische positie bij ieder radarframe, zodat groei,
  krimp en verplaatsing op de kaart de waarnemingen volgen.

# 0.4.63

- Schakel een RegionEngine gericht naar RainViewer wanneer de lokale OPERA-
  uitsnede nul echo's bevat maar RainViewer binnen dezelfde radius actuele
  neerslag waarneemt.
- Bewaar aantallen radarobservaties per engine en gebruik die als operationeel
  dekkingsbewijs, zonder werkelijk droge OPERA-regio's onnodig te vervangen.

# 0.4.62

- Maak OPERA- en RainViewer-verwerking werkelijk geografisch onafhankelijk per
  actieve RegionEngine, zodat verre Life360- en testtargets tegelijk radardata
  krijgen.
- Deel de grote OPERA-downloadcache tussen de regionale uitsneden en ruim
  providers van verdwenen engines automatisch op.
- Bepaal providergezondheid, bronkeuze, diagnose en `radar_covered` per engine
  in plaats van vanuit de Belgische thuisprovider.

# 0.4.61

- Pas wijzigingen van de bliksembronmodus live toe via de Options Flow, zonder
  Home Assistant opnieuw te starten.
- Stop de Blitzortung MQTT-provider daadwerkelijk in `satellite_test` en start
  EUMETSAT LI/GOES GLM onmiddellijk voor diagnostische verificatie.
- Start Blitzortung en zet satellietproviders terug in stand-by wanneer opnieuw
  naar `auto` wordt geschakeld.

# 0.4.60

- Publiceer per WeatherSystem gestructureerde MCS-diagnostiek met afzonderlijke
  vorm-, intensiteits-, duur- en continuïteitscriteria.
- Log en publiceer iedere MCS-statusovergang via
  `storm_tracker_v3_mcs_transition`, inclusief de reden voor bevestiging of
  afwijzing en de gemeten waarden.
- Evalueer MCS-systemen over alle dynamische RegionEngines in plaats van alleen
  de actieve legacy-engine.
- Bewaar maximaal vijftien minuten aan recente Blitzortung-, EUMETSAT LI- en
  GOES GLM-observaties in een compacte, afzonderlijke GeoJSON-laag.
- Toon bliksem als schakelbare sterrenlaag met ouderdomskleuren en brontooltip,
  visueel gescheiden van neerslagcellen en WeatherSystem-polygonen.

# 0.4.59

- Selecteer de operationele radarbron afzonderlijk per dynamische RegionEngine.
- Gebruik voor Italië de actuele, officiële DPC Protezione Civile SRI-radar van 1 km en 5 minuten.
- Gebruik voor Spanje de publieke officiële AEMET-composietbundel met actuele EPSG:4326-GeoTIFFs, zonder API-key.
- Val per regio terug op OPERA en daarna RainViewer wanneer de lokale radar ontbreekt, ongezond of verouderd is.
- Publiceer bron, reden en leeftijd per engine in sensoren, GeoJSON en de multi-targetkaart.
- Reserveer satelliet-neerslag als latere gap-fallback; EUMETSAT LI wordt nadrukkelijk niet als neerslagradar misbruikt.

# 0.4.58

- Herken niet-JSON-antwoorden van Open-Meteo ondanks een HTTP-successtatus.
- Probeer ItaliaMeteo ICON-2I automatisch opnieuw met alleen neerslag wanneer LPI niet geleverd kan worden.
- Laat een forecaststoring de officiële Italiaanse radarcatalogus en lifecycle niet langer volledig blokkeren.

# 0.4.57

- Voeg GeoSphere Austria INCA toe als slapende officiële 1 km-nowcastvalidatie.
- Voeg ItaliaMeteo-ARPAE ICON-2I via Open-Meteo toe voor lokale neerslag- en bliksemverwachting, naast de gratis Radar SRI DPC-catalogus.
- Italiaanse dagbundels worden niet als realtime radar gebruikt wanneer ze achterlopen; OPERA/RainViewer blijft dan operationeel.

# 0.4.56

- Voeg slapende officiële radarproviders toe voor Groot-Brittannië (Met Office) en Frankrijk (Météo-France).
- Voeg MeteoLux toe als lokale, publieke nowcast-validatiebron voor Luxemburg.
- Gebruik nationale radars voorlopig veilig als OPERA-corroboratie en toon hun lifecycle-diagnostiek.

# 0.4.55

- Selecteert bij meerdere buien per target de operationeel belangrijkste
  dreiging in plaats van uitsluitend de dichtstbijzijnde bevestigde echo.
- Actuele neerslag blijft altijd prioritair; daarna volgen voorspelde rake en
  randpassages, naderende systemen, zijwaartse passages en wegtrekkende buien.
- Publiceert `selected_reason` zodat zichtbaar en testbaar is waarom een systeem
  voor een persoon werd gekozen.
- Droge statussen publiceren nu ook expliciete lege prognosevelden, zodat het
  dashboard en automatiseringen een stabiel attribuutcontract houden.

# 0.4.54

- Nieuwe conservatieve passageprognose per target tot 90 minuten vooruit:
  verwacht tijdstip, kleinste contourafstand, verwachte intensiteit en een
  numerieke zekerheidsscore.
- Intensiteit wordt uit de recente dBZ-trend geëxtrapoleerd en begrensd tot
  maximaal 10 dBZ verandering om uitschieters te vermijden.
- Prognoses verschijnen alleen bij bevestigde Matige/Hoge bewegingsvectoren;
  onbetrouwbare of te verre projecties blijven expliciet onbeschikbaar.

# 0.4.53

- Persoons- en targetstatussen publiceren nu de actuele plaats, het adres en
  de ISO-landcode voor weergave op de dashboardtegels.
- Plaats en land worden privacyvriendelijk lokaal bepaald via
  `/homeassistant/www/places.json`; targetcoördinaten verlaten Home Assistant niet.
- Een Life360-plaatsnaam (zoals `Thuis`) krijgt voorrang als label, terwijl de
  lokale plaatsendatabase de landcode aanvult.

# 0.4.52

- Bevestigde systemen krijgen bij een Matige of Hoge bewegingsvector een
  expliciete hoofdstatus: `naderend`, `wegtrekkend` of `langs_trekkend`.
- `bevestigd` betekent voortaan dat de bui echt is, maar de bewegingsvector
  nog onvoldoende betrouwbaar is voor een richtingstatus.
- ETA en waarschuwingen blijven uitsluitend gekoppeld aan `naderend`.

# 0.4.51

- Eerste veilige fase van radar-autokalibratie: ruwe OPERA-cellen worden op
  een gemeenschappelijk geografisch rooster vergeleken met het actuele
  visuele KMI-radarbeeld.
- OPERA- en KMI-frames worden maximaal drie uur apart in historiek gehouden;
  vergelijking gebeurt uitsluitend bij exact dezelfde nominale radarminuut,
  ongeacht welke bron als eerste binnenkomt.
- Nieuwe `sensor.stv3_radar_autokalibratie` toont overlap, precisie, recall,
  F1-score, vermoedelijke valse OPERA-cellen en gemiste KMI-neerslag.
- De kalibratie is strikt observerend en wijzigt geen operationele filtering.
- Een ongewijzigd KMI-frame wist niet langer de laatst geldige observaties;
  ook een vers droog frame blijft bruikbaar als negatieve referentie.

# 0.4.50

- De actuele radarcontour wordt langs de betrouwbare bewegingsvector
  geprojecteerd en classificeert de passage per target als raak, rand of mist.
- Publiceert afstand tot de voorspelde contour en een conservatieve
  onzekerheidsmarge voor waarschuwingen.

# 0.4.49

- Per target worden de minimale afstand tot de voorspelde centrumlijn en het
  tijdstip van dichtste passage berekend bij een Matige of Hoge bewegingsvector.

# 0.4.48

- EUMETSAT LI probeert bij een tijdelijke 404 automatisch oudere recente
  catalogusproducten in plaats van de volledige fallbackcyclus te verliezen.
- NOAA GOES-18/19 blijft in slaapstand zolang geen actieve RegionEngine binnen
  de respectieve satellietdekking ligt.

# 0.4.47

- Machineleesbare landen-/datatypebronmatrix toegevoegd met afzonderlijke
  keuzes voor radar, bliksem en grondvalidatie.
- Aggregator-first beleid vastgelegd: lokale providers worden alleen gebouwd
  wanneer zij aantoonbare meerwaarde boven de continentale bron leveren.
- Wereldwijde catalogus uitgebreid met verzamelproviders en verificatiewachtrij.

# 0.4.46

- DWD RADOLAN/RADVOR RV toegevoegd als eerste lifecycle-provider: publieke
  1 km HDF5-radar, vijfminutenactualiteit en automatische slaapstand.
- DWD-data wordt als nationale vergelijkingsbron gebruikt om OPERA-echo's te
  bevestigen en niet rechtstreeks als tweede operationele stormbron ingevoerd.
- Nieuwe lifecycle-sensor toont status, relevante engines, polltijd, aantallen
  en fouten per locatiegebonden provider.

# 0.4.45

- Generieke lifecyclecontroller toegevoegd voor locatiegebonden providers met
  gedeelde activatie, slaapstand, cooldown, poll-lock en diagnostiek.
- Meerdere RegionEngines die dezelfde bron nodig hebben delen één provider en
  één fetch; terugkerende engines tijdens cooldown veroorzaken geen herstart.

# 0.4.44

- Satellietpolls publiceren nu altijd diagnostiek naar de bliksemsensor, ook
  wanneer geen enkele flash binnen een actieve RegionEngine valt.
- De sensor toont per satellietprovider laatste poll, opgehaalde en aanvaarde
  flashes en een eventuele fout; testmodus pollt meteen na het opstarten.

# 0.4.43

- Configuratieoptie toegevoegd om satellietbliksem tijdelijk te forceren,
  zodat EUMETSAT en NOAA GOES end-to-end getest kunnen worden terwijl
  Blitzortung verbonden blijft.
- In satelliettestmodus worden binnenkomende Blitzortung-observaties genegeerd;
  de normale automatische modus blijft standaard en ongewijzigd.

# 0.4.42

- `STV3 Fictieve tracker locatie` en `STV3 Region Engines` verversen nu ook
  bij verplaatsing van een secundair target naar een nieuwe RegionEngine.
- Verhelpt stale Luxemburg-coördinaten in de UI nadat de testtracker naar
  Miami werd verplaatst; de onderliggende engine-routing was al correct.

# 0.4.41

- NOAA GOES-18 en GOES-19 GLM toegevoegd als gratis, sleutelvrije
  bliksemfallback voor Amerika en de Stille Oceaan.
- De provider verwerkt anonieme NOAA S3-NetCDF-bestanden van twintig seconden,
  dedupliceert bestanden en haalt bij activering maximaal vier minuten in.
- EUMETSAT, GOES-19 en GOES-18 hebben vaste overlappingsgrenzen zodat dezelfde
  optische flash niet door meerdere satellieten dubbel wordt verwerkt.
- Azië tussen 80°O en 145°O blijft bewust vrij voor een latere FY-4-provider.

# 0.4.40

- EUMETSAT MTG Lightning Imager toegevoegd als gratis fallback wanneer de
  Blitzortung MQTT-broker niet verbonden is.
- Tijdelijke EUMETSAT-tokens worden automatisch aangemaakt en vernieuwd;
  consumer key en secret zijn via de configuratie-opties instelbaar.
- Alleen de kleine NetCDF BODY-entry wordt gedownload, met een harde limiet van
  10 MiB, productdeduplicatie en weigering van data ouder dan 30 minuten.
- Zodra Blitzortung opnieuw verbindt, stopt de satellietfallback automatisch.

# 0.4.39

- De multi-targetkaart toont per geselecteerd target alleen diens gekoppelde RegionEngine, radarcellen, systemen, vectoren en targets; overlappende engines worden niet langer door elkaar getekend.
- Bewegingsvectoren verschijnen alleen bij een bevestigd systeem met minstens vier meetpunten, tien minuten historie, een fit van 0,60 en minimaal matige betrouwbaarheid.

# 0.4.38

- De OPERA-diagnostiek telt bruikbare KMI-, KNMI- en RainViewer-referenties nu uit de werkelijke verificatieset; KMI staat niet langer foutief hard op nul.
- De legacy fictieve-trackerlocatiesensor toont de actuele geconfigureerde testtracker, inclusief beschikbaarheid en toegewezen RegionEngine, in plaats van de HA-thuislocatie.

# 0.4.36

- De GeoJSON-kaart publiceert alleen radarcellen van de actieve operationele
  radarbron. Oude RainViewer-fallbackcellen blijven intern kort beschikbaar
  voor lifecyclebeheer, maar verschijnen niet meer op de kaart zolang OPERA de
  actieve bron is.
- RainViewer gebruikt fijnere pixelsampling en een realistischere
  pixeloppervlakte, zodat fallbackcellen minder grof wegen in clustering en
  visualisatie.
- Lage OPERA-quality wordt alleen nog door compacte RainViewer-echo's vanaf
  intensiteit 2 binnen 12 km bevestigd; lichte of ruimtelijk losse pixels mogen
  geen volledige OPERA-cel meer valideren.

## v0.4.28

- Eerste backwards-compatible multi-targetlaag: naast de bestaande fictieve
  tracker kunnen meerdere personen of vaste locaties worden geregistreerd.
- Elk target krijgt een stabiele eigen neerslagstatussensor en wordt gekoppeld
  aan een gedeelde of aparte RegionEngine op basis van de sharing distance.
- Operationele observaties worden via de StormManager naar alle relevante
  regio-engines gerouteerd.

## v0.4.27

- Bewegingshistoriek van radarsystemen wordt nu samen met de MCS-snapshot
  bewaard en na een Home Assistant-herstart onmiddellijk hersteld.
- Oude snapshots zonder bewegingshistoriek worden automatisch uit hun
  radarcellen en parent-footprints gereconstrueerd.
- Meerdere updates van hetzelfde radarproduct vervangen hetzelfde centroidpunt
  in plaats van kunstmatige nul-tijdsintervallen aan de regressie toe te voegen.

## v0.4.26

- Nieuwe `sensor.stv3_neerslagstatus` vat de operationele toestand samen als
  `droog`, `waargenomen`, `bevestigd` of `naderend`.
- De sensor verkiest bevestigde systemen boven eenmalige echo's en publiceert
  afstand, impactpunt, dBZ, radardekking, beweging, ETA en luchtdruktrend in
  één stabiel datacontract voor dashboard en automatiseringen.

## v0.4.25

- Sluimerende systemen blijven intern kort beschikbaar voor lifecyclebeheer,
  maar worden niet langer als actieve storms naar Home Assistant gepubliceerd.
- Systemen tonen nu `tracking_status`, het aantal opeenvolgende radarframes en
  het tijdstip van het laatste radarframe.
- Eén radarframe krijgt status `waargenomen`; vanaf twee aansluitende frames is
  het systeem `bevestigd`. Daardoor kan het dashboard tijdelijke echo's herkenbaar
  scheiden van aanhoudende neerslag.
- Naderingssnelheid en ETA van radarsystemen worden onderdrukt tot het systeem
  door minstens twee opeenvolgende frames bevestigd is.

## v0.4.24

- OPERA `qi_total` is niet langer de enige zelfstandige toelatingsroute:
  meteorologisch plausibele cellen van minstens 50 km², gemiddeld minstens
  20 dBZ en met een piek van minstens 30 dBZ worden als `structured_echo`
  aanvaard, ook bij een lage quality-score.
- Zwakke brede echo's blijven kruisbronbevestiging vereisen; de foutcel nabij
  België van circa 12-14 dBZ wordt dus niet opnieuw toegelaten.
- Diagnostiek onderscheidt quality-, structuur- en kruisbronacceptatie.

## v0.4.23

- OPERA-cellen kunnen niet langer door de groene KMI-basiskaart worden
  bevestigd; het KMI-beeld wordt niet als onafhankelijke pixelreferentie
  gebruikt zolang kaartkleuren en regenkleuren niet betrouwbaar gescheiden zijn.
- De opaak-witte KNMI WMS-achtergrond (intensiteit 1) telt niet langer als
  neerslagbevestiging; alleen echte KNMI-radarkleuren vanaf intensiteit 2 en
  RainViewer-neerslag mogen lage-kwaliteit OPERA-echo's bevestigen.
- OPERA-diagnostiek toont voortaan zowel het ruwe als bruikbare aantal
  corroboratiereferenties.

## v0.4.22

- Netatmo-luchtdruk wordt per station over twee uur gevolgd; regionale
  drukverandering gebruikt de mediaan van gepaarde stations en is daardoor
  niet gevoelig voor onderlinge hoogteverschillen.
- Nieuwe sensor `sensor.stv3_netatmo_luchtdruktrend` toont de verandering over
  60 minuten, trends over 15/30/60 minuten, stationsdekking en snelle drukval.
- Onrealistische drukwaarden en sprongen worden geweigerd; een trend verschijnt
  pas zodra minstens drie stations een bruikbare historische vergelijking geven.
- De twee uur drukhistoriek wordt in Home Assistant-opslag bewaard, zodat een
  herstart de opgebouwde trend niet wist.

## v0.4.21

- De dichtstbijzijnde-storm-sensor blijft beschikbaar wanneer een nieuw
  stormsysteem nog geen snelheidsvector heeft; lege naderingssnelheid wordt
  nu veilig als `null` gepubliceerd.

## v0.4.20

- Bewegingskwaliteit gebruikt nu zowel het aantal meetpunten als de tijdspanne
  en regressie-fit, zodat een korte of grillige reeks minder vertrouwen krijgt.
- De dichtstbijzijnde-storm-sensor toont koers, windroosrichting,
  naderingssnelheid en of het systeem werkelijk naar de tracker beweegt.
- Een ETA wordt alleen berekend uit de snelheidscomponent richting de tracker;
  zijwaarts bewegende en wegtrekkende systemen krijgen geen misleidende ETA.
- Bewegingspunten, historie en fitkwaliteit zijn als diagnostische attributen
  beschikbaar op de stormsensoren.

## v0.4.19

- Blitzortung abonneert niet langer op de volledige wereldfeed, maar gebruikt
  een gededupliceerde unie van geohash-topics rond alle actieve RegionEngines.
- Regiowissels verversen subscriptions op dezelfde gedeelde MQTT-client.
- MQTT-disconnects worden direct gedetecteerd en krijgen exponentiële backoff
  van één tot vijftien minuten, met jitter om proxy-rate-limits te respecteren.
- Foutmeldingen tonen voortaan de foutklasse en een bruikbare reden in plaats
  van een lege `()`-melding iedere 25 seconden.

## v0.4.18

- De bestaande Open-Meteo Gear-sensor wordt nu daadwerkelijk bij Home
  Assistant geregistreerd, zodat `sensor.stv3_open_meteo_gear` niet langer
  als een oude herstelde, maar niet-beschikbare entiteit blijft staan.

## v0.4.17

- RainViewer gebruikt voortaan de werkelijke frame-timestamp uit het manifest
  in plaats van het lokale verwerkingstijdstip.
- Radarframes ouder dan twintig minuten worden als ongezond geweigerd en
  kunnen niet langer als operationele fallback of OPERA-bevestiging dienen.
- De RainViewer-sensor toont poll-, succes- en frametijd, frameleeftijd,
  providergezondheid, foutreden, foutenteller en het gebruikte framepad.
- Manifestproblemen en herstel worden zichtbaar gelogd zonder elke vijf
  minuten dezelfde waarschuwing te herhalen.

## v0.4.16

- Open-Meteo-grid verlaagd van 948 naar 324 modelpunten; OPERA blijft de
  fijnmazige primaire radarbron.
- Succesvolle Open-Meteo-resultaten worden 30 minuten gecachet en niet opnieuw
  als verse OFE-observaties verwerkt.
- HTTP 429 activeert exponentiÃ«le backoff van 30 minuten tot maximaal zes uur
  en respecteert een numerieke `Retry-After`-header.
- `wet_locations_now` wordt nu werkelijk geleverd voor grondverificatie.

## v0.4.15

- De `StormManager` bezit nu de werkelijk gebruikte StormEngine/OFE-runtime;
  `region_manager.py` is niet langer ongebruikte architectuurcode.
- Sharing distance en observatieradius zijn onafhankelijk configureerbaar.
- Een target buiten de sharing distance krijgt een nieuwe dynamische
  RegionEngine; een lege engine wordt automatisch verwijderd.
- De globale Blitzortung-stream wordt naar alle relevante actieve engines
  gerouteerd zonder een extra listener per regio.
- Compacte MCS-framehistoriek wordt met Home Assistant `Store` per geografische
  runtime-regio bewaard en na een restart hersteld.
- Volledige OPERA-rasters worden nooit persistent opgeslagen.
- Nieuwe diagnostische entiteit `sensor.stv3_region_engines` toont actieve
  engines, centra, observatieradius, gekoppelde targets en WeatherSystems.

## v0.4.14

- Radar-gebaseerde MCS-classificatie toegevoegd met afzonderlijke statussen
  `candidate` en `confirmed`.
- Een kandidaat vereist minstens twee kernen van 40 dBZ, minstens één kern van
  50 dBZ en een convectieve span van minstens 100 km.
- Bevestiging gebeurt pas wanneer de criteria drie uur aaneengesloten gelden;
  korte onderbrekingen tot twintig minuten zijn toegestaan.
- Zes uur parent-framehistoriek wordt compact per WeatherSystem bijgehouden.
- Nieuwe entiteit `sensor.stv3_mcs_detectie` toont kandidaten, bevestigde MCS'en
  en de gebruikte metriek/criteria.

## v0.4.13

- Eén brede OPERA-component wordt als parent-WeatherSystem behouden, met de
  adaptief gesegmenteerde zware kernen als afzonderlijke RadarCells.
- Parent- en cell-ID's, oppervlaktes en compacte footprints blijven door de
  Observation Pipeline tot in de WeatherSystem Engine beschikbaar.
- Afstand tot de dichtstbijzijnde storm gebruikt voortaan de dichtste lokale
  RadarCell-footprint in plaats van uitsluitend de systeemcentroid.
- Diagnostiek toont per OPERA-cel ook de parent-component en parentoppervlakte.

## v0.4.12

- Buitensporig grote OPERA-componenten worden adaptief opgesplitst rond
  afzonderlijke zware regenkernen.
- Lichte regen mag nog vijf rasterpixels rond een kern meegroeien, maar kan
  verschillende kernen niet opnieuw aan elkaar verbinden.
- Kleine en gelijkmatig lichte regengebieden behouden de bestaande 8 dBZ-
  gevoeligheid.

## v0.4.11

- OPERA-observaties bewaren voortaan een compacte footprint van werkelijk
  bezette rasterblokken.
- RainViewer en nationale radars kunnen een lage-quality OPERA-cel nu over de
  volledige celvorm bevestigen, ook wanneer het centroid ver weg ligt.
- De footprint gebruikt echte celpixels en geen brede bounding box, zodat lege
  ruimte naast een langgerekte bui geen valse bevestiging veroorzaakt.

## v0.4.10

- RainViewer draait parallel als verificatiebron wanneer OPERA gezond is.
- Lage OPERA-quality kan nu door recente RainViewer-neerslag bevestigd worden.
- Bij OPERA-uitval wordt dezelfde RainViewer-fetch als fallback hergebruikt.
- Een ongewijzigd RainViewer-frame hergebruikt zijn observaties in plaats van
  tijdelijk nul observaties te publiceren.

## v0.4.9

- Alle providerobservaties worden begrensd tot het actuele monitoringsgebied.
- Wereldwijde Blitzortung-inslagen buiten de ingestelde radarradius worden genegeerd.
- Bij een trackerverplaatsing verdwijnen WeatherSystems buiten de nieuwe regio.
- Wachtende OFE-observaties uit de vorige regio worden bij de wissel gewist.
- De sensor `STV3 Actieve Radarbron` publiceert opnieuw zijn runtimewaarde.

## Historische prototypebijlage — niet actueel

> De volgende moduleversies en TODO's zijn een bewaarde momentopname uit de
> eerste prototypefase. Ze beschrijven niet de huidige architectuur of
> releasegereedheid. Alleen de release-items boven deze bijlage, manifest- en
> addonversies zijn operationeel gezaghebbend.

### Historisch overzicht per module

| Module | Huidig | Omschrijving |
|---|---|---|
| `__init__.py` | v0.2.0 | Hoofdsetup, providers, polling, fictieve tracker |
| `sensor.py` | v0.1.0 | HA sensoren voor alle providers |
| `const.py` | v0.1.0 | Constanten (DOMAIN) |
| `providers/base.py` | v0.1.0 | Plugincontract (Capability, CoverageResult, ProviderPlugin, ProviderRegistry) |
| `providers/opera.py` | v0.1.0 | OPERA/MeteoGate S3 radar provider (heel Europa, 1km², HDF5) |
| `engine/observation.py` | v0.1.0 | Observation dataclass |
| `engine/observation_fusion_engine.py` | v0.1.0 | OFE: deduplicatie, batching |
| `engine/storm.py` | v0.1.0 | Storm dataclass |
| `engine/storm_engine.py` | v0.1.0 | Clustering, merge, lifecycle, regressie |
| `engine/region_manager.py` | v0.5.0 | Dynamische RegionEngines per ProjectionTarget |
| `geometry/bounding_box.py` | v0.1.0 | Bounding box berekeningen |
| `geometry/distance.py` | v0.1.0 | Haversine afstandsberekening |
| `geometry/geocode.py` | v0.1.0 | Plaatsnaam lookup |
| `geometry/hull.py` | v0.1.0 | Convex hull berekening |
| `plogger/provider_logger.py` | v0.2.0 | CSV logging per provider |
| `providers/blitzortung.py` | v0.6.0 | Blitzortung MQTT provider |
| `providers/kmi.py` | v0.3.0 | KMI radar provider |
| `providers/knmi.py` | v0.2.0 | KNMI WMS radar + nowcast provider |
| `providers/netatmo.py` | v0.2.0 | Netatmo grondstations provider |
| `providers/open_meteo.py` | v0.3.0 | Open-Meteo grid provider |
| `providers/rainviewer.py` | v0.1.0 | RainViewer tile provider |

---

### Historische changelog per module

### `__init__.py`
- **v0.2.0** — Providers volgen fictieve tracker locatie; herinitialisatie bij verplaatsing >1km; CoreState/EVENT_HOMEASSISTANT_STARTED pattern; cross-trigger verwijderd
- **v0.1.0** — Eerste versie; providers hardcoded op home_lat/lon

### `sensor.py`
- **v0.1.0** — Eerste versie; sensoren voor alle providers; async_added_to_hass fix voor unavailable state

### `providers/blitzortung.py`
- **v0.6.0** — paho-mqtt MQTTv311 exact zoals bestaande HA-integratie; geen filter op afstand
- **v0.5.0** — aiomqtt (timeout problemen)
- **v0.4.0** — directe WebSocket (SSL + beleidsproblemen)
- **v0.3.0** — factory-patroon + supports()
- **v0.2.0** — Observation model
- **v0.1.0** — state listener op HA-integratie (afhankelijk van externe integratie)

### `providers/kmi.py`
- **v0.3.0** — volledige radarplaatjes downloaden via URI uit animatiesequentie; dynamische afmetingen; ww weercode uitlezen
- **v0.2.0** — correcte API: getForecasts + lat/lon + User-Agent: be.meteo.app
- **v0.1.0** — eerste versie (getIncaList zonder lat/lon → lege response)

### `providers/knmi.py`
- **v0.2.0** — correcte dataset/layer namen (radar_forecast_2.0/precipitation_nowcast); aparte WMS API key; alle 25 nowcast tijdstappen
- **v0.1.0** — eerste versie (verkeerde dataset/layer namen → 404)

### `providers/netatmo.py`
- **v0.2.0** — volledige parsing: rain_live, rain_5min, wind_strength, wind_angle, gust_strength, pressure, temperature, humidity per station
- **v0.1.0** — eerste versie (alleen rain_live)

### `providers/open_meteo.py`
- **v0.3.0** — lat/lon van natte punten bijgehouden in wet_locations_now/forecast; timezone als array fix
- **v0.2.0** — timezone als array (vereist door Open-Meteo POST API); minutely_15 nowcast 90min
- **v0.1.0** — eerste versie (timezone als string → 400 error)

### `providers/rainviewer.py`
- **v0.1.0** — eerste versie; 3×3 tile grid; pixel → lat/lon conversie

### `plogger/provider_logger.py`
- **v0.2.0** — non-blocking via async_add_executor_job; locaties in open_meteo log
- **v0.1.0** — eerste versie (blocking I/O in event loop)

### `engine/region_manager.py`
- **v0.5.0** — volledig dynamische RegionEngines zonder vaste landsgrenzen; providers bepalen zelf dekking via supports()
- **v0.4.0** — ProjectionTarget-terminologie
- **v0.3.0** — één canonieke blitz_entity per regio (verouderd)
- **v0.2.0** — geografische regio's op basis van land (verouderd)
- **v0.1.0** — eerste opzet

### `engine/observation_fusion_engine.py`
- **v0.1.0** — eerste versie; deduplicatie per type/locatie; 1s batching; 1u buffer

### `engine/storm_engine.py`
- **v0.1.0** — eerste versie; clustering, merge, lifecycle, regressie, geocoding

### `engine/storm.py`
- **v0.1.0** — eerste versie; Storm dataclass met centroid, beweging, geometrie

### `geometry/*`
- **v0.1.0** — eerste versie van alle geometrie helpers

---

### Historische TODO-status — afgesloten als actuele checklist

Deze checklist is niet meer bruikbaar als gatenlijst: de genoemde
engine-/multi-targetonderdelen zijn inmiddels geïntegreerd en de huidige
releasegates staan in
`docs/STORM_TRACKER_ROADMAP_METEOROLOGISCH_BETROUWBAAR.md`. De regels hieronder
blijven uitsluitend staan om de prototypegeschiedenis niet te herschrijven.

- [ ] OFE nog niet verbonden met providers in `__init__.py`
- [ ] StormEngine nog niet geactiveerd
- [ ] RegionEngine/StormManager nog niet geïntegreerd
- [ ] ProjectionEngine (ETA, passage) nog niet gebouwd
- [ ] Multi-persoon tracking nog niet geïmplementeerd
- [ ] Open-Meteo polling interval: 120 min (tijdelijk na rate limit issues)
- [ ] Versioned ZIP backups ontbreken nog

### `providers/base.py`
- **v0.1.0** — eerste versie; Capability, CoverageArea, CoverageResult, ProviderContext, ProviderPlugin Protocol, ProviderRegistry met strategie per capability

### `providers/opera.py`
- **v0.1.0** — eerste versie; OPERA DBZH composiet via MeteoGate S3 bucket; HDF5 parsing via h5py/numpy/pyproj; component labeling uit PoC; Observations per regencel
# v0.4.4

- OPERA discovery controleert recente, deterministische vijfminutenbestanden
  nu eerst rechtstreeks met HTTP HEAD.
- De brede S3-listing blijft beschikbaar als fallback.
- Extra regressietest voor discovery wanneer listing een actueel bestand mist.
- Testomgeving bevat nu alle providerafhankelijkheden en een portable
  `radar_policy`-fixture.
# v0.4.5

- OPERA `qi_total` wordt niet langer als harde quality-filter gebruikt; live
  CIRRUS-producten rapporteerden `0.0` voor echte neerslagpixels.
- Detectiedrempel verlaagd van 10 naar 8 dBZ.
- Minimum celoppervlakte verlaagd van 9 naar 5 pixels/km².
- Actieve detectiedrempels toegevoegd aan de OPERA-sensordiagnostiek.
# v0.4.6

- Standaard OPERA/radarradius verhoogd van 200 naar 300 km om cellen boven
  Drenthe/Overijssel vanuit de thuisregio tijdig te volgen.
- Werkelijke radius en WGS84-bbox worden voortaan getoond als attributen van
  `sensor.stv3_opera_observaties`.
# v0.4.7

- `sensor.stv3_opera_observaties` toont maximaal 40 ruwe OPERA-cellen met
  coördinaten, afstand, oppervlakte, dBZ, quality en pixelcount.
- Diagnostieklijst is begrensd om state-attributen compact te houden.
- Cellen in de verre hoeken van de geprojecteerde bbox worden nu met een echte
  grootcirkelafstand begrensd tot `radius_km`.
## v0.4.29

- Home Assistant `home` is voortaan altijd het primaire target; de locatie komt
  bij UI-installaties rechtstreeks uit de algemene HA-configuratie.
- Nieuwe configuratie- en optiesflow voor Life360 `device_tracker`-entiteiten.
- De fictieve tracker is niet langer verplicht of primair, maar een optioneel
  en duidelijk gemarkeerd testtarget.
- Bestaande YAML-configuraties blijven ondersteund tijdens de migratieperiode.
## v0.4.30

- Nieuwe compacte GeoJSON-kaartfeed voor targets, RegionEngines,
  weersystemen, systeemhulls, lokale RadarCells en bewegingsvectoren.
- Radarcellen zijn begrensd en hulls worden compact bemonsterd om te vermijden
  dat Home Assistant-state-attributen onbeheersbaar groot worden.
- De kaartfeed heeft een expliciete schemaversie en meldt wanneer cellen zijn
  afgekapt, zodat kaartclients betrouwbaar kunnen degraderen.
## v0.4.31

- De volledige GeoJSON staat niet langer in een sensorattribuut dat Recorder
  bij iedere radarupdate zou kunnen opslaan.
- Nieuwe geauthenticeerde feed op `/api/storm_tracker_v3/geojson` voor
  dashboardkaarten en andere kaartclients.
- `sensor.stv3_kaart_geojson` publiceert uitsluitend compacte metadata,
  featureaantallen en het endpoint.
## v0.4.32

- Orden OPERA RadarCell-puntwolken eerst als convex hull; hierdoor verdwijnen
  de lange blauwe zigzagdiagonalen op de GeoJSON-kaart.
- Maak de kaartmodule ASCII-veilig zodat keuzelijst, zoomknoppen en metadata
  niet langer met mojibake worden weergegeven.
- Cluster targets op vrijwel dezelfde locatie tot een leesbaar label in plaats
  van acht overlappende namen.
# 0.4.35

- Publiceert in de kaartlaag alleen radarcellen uit het nieuwste radarframe; historische detecties blijven intern beschikbaar voor beweging en opvolging.
- Knipt lage-kwaliteit OPERA-footprints terug tot de punten die ruimtelijk door RainViewer worden bevestigd, zodat een kleine echte bui geen volledige foutieve megacel meer aanvaardt.

# 0.4.34

- Herstelt het RainViewer v2-tegelpad door de verplichte afbeeldingsgrootte toe te voegen.
- Decodeert Universal Blue-radarpixels op transparantie en kleur in plaats van algemene helderheid; grijze antwoordpixels kunnen OPERA niet langer foutief bevestigen.

# 0.4.33

- Vereist onafhankelijke radarcorroboratie voor OPERA-cellen met lage kwaliteit, ook wanneer hun reflectiviteit en oppervlakte meteorologisch plausibel lijken.
- Verwijdert daardoor sterke maar onbevestigde OPERA-clutter, zoals de foutieve cellen boven Noord-Nederland en Noord-Frankrijk.
# 0.4.37

- KMI-radarpunten vanaf intensiteit 2 kunnen OPERA opnieuw onafhankelijk bevestigen; basiskaartartefacten met intensiteit 1 blijven uitgesloten.
- Stormvlakken en bewegingsvectoren zonder radarcel uit het actuele frame van de actieve bron verdwijnen uit de GeoJSON-kaart.
## 0.4.129

- Maakt de kandidaatselectie voor virtuele verkenners deterministisch bij
  exact gelijke ETA en afstand. De station-ID is nu de beslissende derde
  sorteersleutel, zodat Python nooit meer twee `Observation`-objecten probeert
  te vergelijken.

## 0.4.128

- Verwijdert de achtergebleven oude `virtual_probe_update`-eventregel uit
  de Netatmo-poll. Die verwees na de v0.4.127-opsplitsing naar een verplaatste
  variabele en stopte daardoor de providercyclus vóór de onafhankelijke
  verkennerevaluatie.

## 0.4.127

- Virtuele regenverkenners worden nu in een afzonderlijke, korte fase
  geëvalueerd op basis van de laatst geldige Netatmo-cache. Een timeout van
  Netatmo of Open-Meteo kan daardoor een geplande verificatie niet meer
  annuleren of de verkennersensor laten bevriezen.
- Netatmo en Open-Meteo starten nog steeds parallel, maar hebben voortaan
  afzonderlijke timeouts zodat een trage modelaanvraag de grondmeting niet
  meer annuleert.
# v0.4.133

- Verplaatst de opbouw van begrensde pySTEPS-radarrasters volledig uit de
  Home Assistant-eventloop.
- Verplaatst ook de volledige framecache-serialisatie naar de executor; voordien
  gebeurde alleen de uiteindelijke SQLite-write buiten de hoofdthread.
- Voegt een regressiecontract toe dat beide zware bewerkingen buiten de
  eventloop houdt.
# v0.4.148

- Maak OPERA operationeel worker-first: download, HDF5-segmentatie en de
  compacte kaartlaag worden volledig door worker 0.5.0 opgebouwd.
- Valideer gedeeld artifactpad, gecomprimeerde grootte, SHA-256, frameleeftijd,
  celinhoud en celdigest voordat HA de workeruitvoer gebruikt.
- Behoud de lokale OPERA-download en parser uitsluitend als begrensd vangnet
  wanneer de worker ontbreekt, faalt of ongeldige uitvoer levert.
- Toon OPERA voortaan als operationeel workerproces in plaats van vergelijking.
# v0.4.149

- Start fase 3 met een begrensde bewegingsanalyse in worker 0.6.0.
- Vergelijk per regio de workeruitkomst observerend en exact met de bestaande
  HA-preflight; waarschuwingen en ETA blijven tijdens deze validatiefase lokaal.
- Publiceer een afzonderlijke heartbeat voor het bewegingsproces en begrens
  frameaantal, rastergrootte, natte cellen, looptijd en MQTT-payload.
# v0.4.151

- Start de migratie van stormobject-tracking met een strikt begrensde
  workervergelijking. Per regio worden maximaal dertig compacte tracks en
  twaalf radarmomenten per track gevalideerd en deterministisch gehasht.
- Toon Stormtracking als afzonderlijk workerproces. De bestaande HA-tracking
  blijft operationeel totdat meerdere cycli exacte pariteit bewijzen.

# v0.4.150

- Maak de gevalideerde rasterbewegingsanalyse worker-first.
- Valideer methode, status en frameaantal voordat HA de workeruitkomst gebruikt;
  val bij timeout, contractfout of ongeldige uitvoer begrensd lokaal terug.
- Verwijder de verouderde OPERA-compatibiliteitstaak en toon alleen nog de
  operationele OPERA- en bewegingsprocessen op het dashboard.
## 0.4.158

- Verwijdert actieve verkenners zodra een verse lege radarscan bewijst dat hun
  stormobject verdwenen is, maar behoudt ze tijdens een echte providerstoring.
- Dwingt maximaal twee actieve verkenners per bui af over opeenvolgende cycli,
  in plaats van de limiet per afzonderlijke cyclus toe te passen.
- Worker 1.0.4 bevat exact dezelfde verkennerlogica als de HA-fallback.
# 1.6.0

- Volgt betrouwbare baanversnelling en -kromming tot 60 minuten en tekent
  het gekozen gebogen traject op de kaart.
- Herkent ruimtelijk uiteenvallende radarcomponenten als afzonderlijke
  kindbuien met bewaarde afstamming.
- Voegt radarbuien alleen samen bij verenigbare richting en aantoonbare
  bron- of footprintcontinuïteit.
- Bewaart split- en mergegebeurtenissen in de secundaire verificatiedatabase.
## 1.9.1

- Bewaart de laatste geldige compacte stormsnapshot wanneer een trackingbatch
  tijdelijk leeg is, zodat een recente radarcache na een herstart opnieuw aan
  dezelfde storm-ID kan worden gekoppeld.
- Expliciet verwijderde regio's worden nog steeds volledig opgeschoond; oude
  snapshots blijven bovendien onder de bestaande strikte leeftijdsgrenzen
  vallen.
# 1.14.110 / 1.18.0

- Voeg de directe `thin_operational` runtime toe: geografische bronselectie,
  één intern observatiemodel, fusie, celtracking/voorspelling, PostGIS en MQTT.
- Start MQTT en heartbeat vóór providerwerk; verwijder RAW-, receipt-, audit- en
  schedulerinfrastructuur uit het nieuwe operationele pad.
- Behoud het bestaande HA desired-state- en snapshotcontract voor een
  rollbackbare omschakeling op uitsluitend NAS-HA.

# 1.14.110 / 1.17.128

- Heartbeat en operationele transport starten nu vóór iedere opslagcontrole.
  De eerste providercyclus voert de begrensde controle uit voordat een capture
  wordt toegelaten en activeert alleen bij een concrete inconsistentie orphan
  recovery plus de diepe audit.

# 1.14.110 / 1.17.127

- Workerstart gebruikt voortaan de begrensde live integriteits- en
  capaciteitscontrole en blokkeert niet meer op een volledige audit van het
  groeiende RAW-corpus. Alleen een concrete opslag-inconsistentie activeert
  orphan recovery plus één diepe audit; verdacht materiaal blijft daarbij
  fail-closed.

# 1.14.110 / 1.17.126

- Bundelt Open-Meteo voortaan in één aanvraag voor alle actuele targets, zodat
  een tweede regio de provenancecontrole en voorspellingen niet meer breekt.
- Negeert achtergebleven aankomstkandidaten van tijdelijk onbeschikbare
  trackers zonder geldige thuisvoorspellingen en kaartcellen te wissen.
- Logt een begrensde foutcode wanneer radarbronfusie veilig wordt ingehouden.
