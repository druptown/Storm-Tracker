# Storm Tracker V3 — gatenlijst databetrouwbaarheid

Datum audit: 2026-07-29

## Status van dit document

Dit is de **historische gatenaudit van de 1.11-baseline**. Woorden als
“Huidig” hieronder beschrijven de code op het moment van die audit en zijn
niet automatisch de actuele werkboomstatus. De gezaghebbende actuele stand,
ticketafhankelijkheden en releasegates staan in:

- `STORM_TRACKER_ROADMAP_METEOROLOGISCH_BETROUWBAAR.md`;
- `P0_HARDENING_CANDIDATE_1.12.0_WORKER_1.15.0.md`;
- `WERKSESSIE_2026-07-29_TOT_2130.md`.

Lokale wijzigingen na deze audit, nog niet live vrijgegeven:

| Auditgat | Actuele lokale stand |
|---|---|
| G01/G02 | groundmatching is per storm-ID, actuele losse radarcontour en meettijd; bronafhankelijke freshness is toegevoegd; echte New York-/groundreplays blijven open |
| G03/G04/G05 | Observation-contract v2, afzonderlijke observed/received-tijd, categorische neerslag en Synoptic-intervalprovenance zijn lokaal ingevoerd; wereldwijde echte-data-audit blijft open |
| G07/G14 | naast de bestaande schema-12 beslislaag bestaat nu een afzonderlijke append-only `CanonicalGroundEvidenceStore` voor het laagste station-/tijd-/waarde-/QC-feit, op iedere read gebonden aan een complete schema-3 raw capture; ongewirede Netatmo-, METAR- en Synoptic-decoders vullen uitsluitend candidate facts zonder grondbeslissing. Hun bounded factsets committen nu atomisch met één raw-authority-read en één SQLite-transactie; overige decoders, sitecrosswalks, fusion en volledige foreign-keyreplay blijven open |
| G10 | een ongewirede Netatmo-kandidaat bewaart de exacte `getpublicdata`-response vóór JSON-inspectie en decodeert station/module/tijd/getal/QC uitsluitend als opaque canonical ground facts met expliciet ongecertificeerde unit- en tijdondersteuning; officiële veldsemantiek, fysieke units/intervallen, een geautoriseerde golden en iedere regen-/droog-/admissionbeslissing blijven open |
| G13 | de content-addressed RawArtifactStore schema 3 is nu de bedoelde enige raw authority. KMI appv4 en DWD RV bereiken die store reeds in de lokale worker-only werkboom; daarnaast bestaan afzonderlijke ongewirede exact-byte capturekandidaten voor RainViewer, KNMI, Met Office, Météo-France, OPERA, NOAA ERR, GOES RRQPE G18/G19, H SAF H40B, DPC SRI, Synoptic, METAR en Netatmo. Een ongewirede, append-only globale/providergebonden capaciteitsledger is met 29 tests bestand tegen cross-process oversubscription en crash-/retrygrenzen; atomische koppeling aan echte raw-publicatie, schijfreconciliatie, quota/retentie en de resterende providers blijven open |
| G13/HA-workloaddekking | het content-addressed worker-cutovermanifest bewijst dat de eerdere workerjobregistry niet alle HA-netwerkactiviteit omvatte. Een nieuwe ongewirede acquisitieplanner beschrijft 25 begrensde, contentgebonden workerworkloads en houdt MeteoLux, GeoSphere Austria, ItaliaMeteo en Blitzortung zichtbaar fail-closed. Voor Blitzortung bestaat inmiddels een schema-3 exact-QoS0-payloadgrens, maar MQTT-client, duurzame backpressure, gapreceipts en decoder ontbreken; de push blijft HA-owned. Alleen desired-state/CAS, één bounded bridge en O(1)-snapshotprojectie mogen na cutover in HA blijven |
| G13/AEMET | officiële AEMET-bronnen bewijzen dat het nationale product laagste-PPI-reflectiviteit in dBZ is en dat de per-file `ESCALA`-metadata de RGBA-naar-waarde-intervallen bevat. De oude byteklassen `1..8` zijn daarom expliciet geen kwantitatieve legenda. Hergebruik/retentie is toegestaan met AEMET-attributie en metadatabehoud, maar exacte tijdsupport, grid, `ESCALA`, NoData/no-echo/dekking, pixel-QC en een complete officiële envelope/`datos`/`metadatos`-golden ontbreken. Een ongewirede productgate houdt capture, detectie, rain-rate, forecast en admission gesloten |
| G13/P1-N01 | een ongewirede canonical-radar-fieldbrug laadt uitsluitend via de raw-herverifiërende `CanonicalEvidenceStore`-readroute en herverifieert daarna handle, SHA, artefactschema, capture, decoder/config, quantity/unit, tijd, native grid, maskers en QC. KNMI en DWD worden alleen bij exact 300 s als expliciete intervalgemiddelde rate omgerekend; NOAA blijft instantane satelliet-QPE; Met Office bewaart nominale rate plus niet-uniform inputvenster en ontbrekende QC; OPERA DBZH blijft hard uitgesloten zonder Z-R-certificaat. DWD heeft nu een afzonderlijk officieel DWD+ODIM-certificaat voor zijn exacte native DE1200-pixelranden en een end-to-end compositor tot lossless shields/cores met exhaustieve pixelaccounting en coöperatieve deadline/objectgrenzen; KNMI blijft fail-closed wegens strijdige officiële ellipsoïdes en onvoldoende pixel-edgebewijs. Harde child-isolatie, held-out skill, cross-providerreprojection, NUC-soak, admission en wiring blijven open |
| G15 | Observation-contract v2 is aanwezig |
| G16/G17 | providerresolver- en HA/workerpariteit hebben regressietests; een echt gedeeld package blijft gewenst |
| G20 | worker publiceert runtimeversie en build-ID; Supervisorweergave alleen blijft geen codebewijs |

Geen rij in deze tabel is een live vrijgave. Niet genoemde gaten blijven open
of vereisen een nieuwe gerichte audit. De oorspronkelijke tekst hieronder is
bewust ongewijzigd gebleven als bewijs van de baseline.

## Doel

Deze audit volgt de volledige keten:

`upstream bron → worker/provider → Observation → fusie/tracker → kalibratieopslag → diagnostiek`

Prioriteiten:

- **P0** — kan nu een inhoudelijk verkeerde weerconclusie veroorzaken;
- **P1** — verhindert betrouwbare kalibratie, replay of herleidbaarheid;
- **P2** — technische inconsistentie of onvolledige diagnostiek.

## Bevestigde gaten

### P0 — grondbevestiging

#### G01. Grondbevestiging controleert geen overlap met dezelfde bui

- Huidig: `build_local_netatmo_summary()` telt natte stations binnen 10 km
  van de tracker. Twee natte stations betekenen bevestiging.
- Ontbreekt: overlap met radarcontour, gevolgd `storm_id`, bewegingsrichting
  en tijdsvenster van die specifieke bui.
- Impact: regen van een andere lokale cel kan de gevolgde bui bevestigen.
- Herstel: station-observatie eerst ruimtelijk en temporeel aan een
  WeatherSystem koppelen; bevestiging per `(target_id, storm_id)`.
- Acceptatie: een nat station buiten de gevolgde contour/corridor kan nooit
  die bui bevestigen, maar blijft wel zichtbaar als regionale waarneming.

#### G02. Verouderde METAR-waarnemingen kunnen bevestiging beïnvloeden

- Huidig: worker accepteert METAR tot 120 minuten oud. HA bewaart bij een
  cachemiss oude METAR-observaties eveneens tot twee uur. De lokale
  samenvatting voert zelf geen leeftijdsfilter uit.
- Impact: twee oude regenmeldingen binnen 10 km kunnen actuele regen
  bevestigen.
- Herstel: bronafhankelijke versheidsclassificatie; voor bevestiging een
  veel korter actueel venster dan voor historische diagnostiek.
- Acceptatie: verouderde waarnemingen blijven opgeslagen, maar hebben nul
  bevestigingsgewicht.

#### G03. Netatmo gebruikt ophaaltijd als meettijd

- Huidig: `_parse_observations(data, time.time())` zet alle stations op het
  moment waarop de API-query voltooid is.
- Ontbreekt: de werkelijke tijd van `measure.res`/dashboardmeting.
- Impact: oude Netatmo-data lijkt vers; afstand-in-tijd tot radar en bui is
  niet betrouwbaar.
- Herstel: timestamp per gebruikte meetwaarde uitlezen en zowel
  `observed_at` als `received_at` bewaren.
- Acceptatie: een opnieuw opgehaalde oude meting wordt niet jonger.

#### G04. METAR-categorie wordt als fictieve hoeveelheid opgeslagen

- Huidig: elke actuele neerslagcode wordt vertaald naar `rain_mm=0.1`,
  `rain_5min=0.1` en `rain_60min=0.1`.
- Impact: categorische aanwezigheid wordt voorgesteld als drie
  kwantitatieve metingen met verschillende betekenissen.
- Herstel: afzonderlijke velden voor `precipitation_present`,
  fenomeencode en intensiteitsklasse; hoeveelheden alleen vullen wanneer
  METAR werkelijk een hoeveelheid levert.
- Acceptatie: categorieën worden nooit meer als mm/u of accumulatie
  behandeld.

#### G05. Synoptic-intervalwaarden verliezen hun semantiek

- Huidig: alle `precip_intervals*`-waarden worden verzameld; maximum wordt
  zowel `rain_mm` als `rain_5min`, en alle recente waarden worden opgeteld
  tot `rain_60min`.
- Ontbreekt: intervalduur, sensorset, rapporttype en onderscheid tussen
  accumulatie en snelheid.
- Impact: dubbel tellen en onjuiste eenheden zijn mogelijk.
- Herstel: variabelenaam en intervalmetadata expliciet parseren; alleen
  vergelijkbare intervallen combineren.
- Acceptatie: iedere afgeleide waarde vermeldt bronveld, interval en eenheid.

#### G06. Alle grondbronnen krijgen impliciet hetzelfde gewicht

- Huidig: `rain_mm >= 0.1` en twee lokale stations volstaan, ongeacht
  provider, leeftijd, meettype of stationskwaliteit.
- Impact: twee categorische METARs wegen hetzelfde als twee actuele
  regenmeters; gecorreleerde stations gelden als onafhankelijke bevestiging.
- Herstel: observation-quality-score met componenten voor actualiteit,
  meettype, afstand, bronkwaliteit, onafhankelijkheid en stormoverlap.
- Acceptatie: de eindscore en alle deelredenen zijn zichtbaar en opgeslagen.

### P1 — verliesvrije opslag en replay

#### G07. Geen tabel met afzonderlijke grondwaarnemingen

- Huidig: `provider_cycle_samples` bewaart hoofdzakelijk aantallen en
  aggregaten.
- Ontbreekt: één historisch record per station, meettijd en provider.
- Impact: grondbeslissingen zijn niet reproduceerbaar.
- Herstel: append-only `ground_observations` met stabiele unieke sleutel.
- Acceptatie: iedere gebruikte grondwaarneming kan exact worden teruggelezen.

#### G08. Ruwe METAR-data wordt weggegooid

- Huidig: `wx_string` en `raw_text` bestaan tijdens decoding maar verdwijnen
  vóór het workerresultaat.
- Impact: achteraf is niet te bewijzen waarom iets als regen gold.
- Herstel: ruwe code, actuele weergroep en parserresultaat verliesvrij
  bewaren.
- Acceptatie: `FZRANO`, `RAE...` en `-RA` zijn in replay opnieuw te
  interpreteren zonder een nieuwe download.

#### G09. Ruwe Synoptic-data wordt weggegooid

- Huidig: alleen genormaliseerde waarden verlaten de worker.
- Ontbreekt: originele variabelenaam, sensorset, tijdreekswaarde,
  kwaliteitsvelden en SUMMARY-status.
- Impact: parserwijzigingen kunnen niet op historische data worden getest.
- Herstel: relevante originele stationpayload of een verliesvrij begrensd
  subset opslaan.

#### G10. Ruwe Netatmo-meetreeksen worden weggegooid

- Huidig: per station blijft alleen één samengestelde Observation over.
- Ontbreekt: module-ID, type, originele `res`-timestamp en gebruikte ruwe
  waarden.
- Impact: verkeerde modulekeuze/timestamp kan niet worden gereconstrueerd.
- Herstel: gebruikte bronvelden plus station/module provenance opslaan.

#### G11. METAR ontbreekt in `provider_cycle_samples`

- Huidig: Netatmo en Synoptic schrijven cyclusaggregaten; METAR alleen
  live-diagnostiek.
- Impact: zelfs aantallen en cachegedrag van METAR ontbreken historisch.
- Herstel: METAR-cyclusregistratie per RegionEngine, inclusief station-ID’s,
  versheidsverdeling en interpretatietotalen.

#### G12. Synoptic-cycli missen station-ID’s en tijdsverdeling

- Huidig: stations, natte stations, maximum en cachetype.
- Ontbreekt: betrokken station-ID’s, oudste/nieuwste meting en afwijsredenen.
- Impact: aggregaten zijn niet controleerbaar.

#### G13. “Originele radarframes” zijn genormaliseerde provideroverlays

- Huidig: `original_radar_frames` bewaart de provider-`overlay` verliesvrij.
- Nuance: dit is verliesvrij ten opzichte van onze overlay, niet
  noodzakelijk ten opzichte van upstream bytes/pixels. Providers kunnen al
  decimeren, clusteren, omprojecteren of intensiteit reduceren vóór opslag.
- Impact: sommige parser-, projectie- en resolutiefouten zijn niet replaybaar.
- Herstel: per provider documenteren welk upstream niveau wordt opgeslagen;
  waar haalbaar bronpayload/object-key + checksum en decode-metadata bewaren.
- Acceptatie: per provider is expliciet bewezen of replay vanaf upstream
  resolutie mogelijk is.
- Lokale kandidaat: `RawCapacityLedger` reserveert bytes en entries vóór een
  write en commit actuals plus content-, receipt- en capturehashes append-only.
  Dit voorkomt bewezen gelijktijdige oversubscription in de ledger, maar nog
  niet op de fysieke store zolang publicatie en ledgercommit niet één herstart-
  en power-lossbestendig protocol vormen. On-disk reconciliatie en OS-quota
  blijven daarom releasegates.

#### G14. Kwaliteitsbeslissingen worden niet als zelfstandig record opgeslagen

- Huidig: forecastsnapshot bevat een eindstatus; providerbias bewaart
  radarscores.
- Ontbreekt: inputobservaties, regels, deelgewichten en afwijs-/verlaagredenen
  van grondfusie.
- Impact: een verkeerde conclusie is niet volledig uitlegbaar.
- Herstel: `observation_quality_assessments` en
  `ground_confirmation_decisions`.

### P1 — contracten en consistente uitvoering

#### G15. `Observation` kan provenance en kwaliteit niet dragen

- Huidig model bevat locatie, één timestamp, meetwaarden, station-ID en bron.
- Ontbreekt: `received_at`, ruwe referentie, eenheden, meetinterval,
  parserversie, kwaliteitsvlaggen, interpretatiereden en bronrecord-ID.
- Impact: benodigde informatie gaat al vóór opslag verloren.
- Herstel: versie 2 van het Observation-contract, compatibel serialiseerbaar
  in HA en worker.

#### G16. HA- en worker-virtual-probecode behandelen METAR verschillend

- Huidig: HA herkent prefixes `synoptic` en `metar`; worker-`probe_core`
  herkent alleen Synoptic en noemt al het overige Netatmo.
- Impact: workerdiagnostiek en verificaties kunnen METAR verkeerd labelen.
- Herstel: één gedeelde providerresolver en contracttest over alle bronnen.

#### G17. Geen contracttest die HA- en workerlogica gelijk houdt

- Huidig: delen van `tracking_core` zijn kopieën; afwijking wordt niet
  structureel geblokkeerd.
- Impact: fixes kunnen slechts aan één kant landen.
- Herstel: gedeeld package of synchronisatie-/equivalentietest in CI.

#### G18. Generieke grondvelden dragen nog Netatmo-namen

- Voorbeelden: `netatmo_local_confirmed`,
  `netatmo_nearest_rain_station_id`, `netatmo_data_age_seconds`.
- Impact: betekenis is misleidend zodra METAR en Synoptic zijn samengevoegd;
  consumenten kunnen verkeerde aannames doen.
- Herstel: nieuwe generieke `ground_*`-velden; oude namen tijdelijk als
  expliciet gedepricieerde aliases.

### P2 — diagnostiek en beheer

#### G19. Diagnosesensoren tonen alleen totalen

- Huidig: stationaantal, nat aantal, regio en cache.
- Ontbreekt: stationlocaties, leeftijd, ruwe/afgeleide status,
  kwaliteitsreden en afstand tot relevante trackers.
- Herstel: begrensde per-regio diagnose met aantallen plus de relevante
  stationrecords; volledige historie blijft in database.

#### G20. Worker-versiemetadata is niet betrouwbaar zichtbaar

- Huidig: Supervisor kan na lokale rebuild de oude versie blijven tonen.
- Impact: niet ondubbelzinnig bewijsbaar welke code actief is.
- Herstel: worker publiceert runtimeversie, build-ID en bronchecksum in
  heartbeat/status; dashboard toont deze waarden.

#### G21. Cacheleeftijd en observatieleeftijd worden niet overal gescheiden

- Huidig: diagnostics mengen `received_at`, `queried_at`, cacheleeftijd en
  stationtimestamp.
- Impact: een verse API-response met oude meetdata lijkt soms “vers”.
- Herstel: vaste termen en velden:
  `observed_at`, `received_at`, `fetched_at`, `cache_age_seconds`,
  `observation_age_seconds`.

#### G22. Afwijs- en parsefouten worden vooral geteld noch bewaard

- Huidig: ongeldige stationrecords worden vaak met `continue` overgeslagen.
- Impact: stille dataverliezen en providerwijzigingen blijven onzichtbaar.
- Herstel: begrensde reason counters per provider/cyclus en voorbeeldrecords
  zonder credentials of persoonsgegevens.

## Gecontroleerd aanwezig

Deze onderdelen bestaan wel en hoeven niet opnieuw ontworpen te worden:

- radarvergelijkingen en providerbiasprofielen;
- forecastverificaties per tracker met statussnapshot;
- pySTEPS/motion-preflightmetingen;
- systeemperformantiemetingen;
- bliksemaggregaten;
- verliesvrije opslag van de door providers aangeleverde overlay;
- workerprocessen, queue- en heartbeatdiagnostiek;
- RegionEngine-scoping voor radar- en grondqueries.

## Aanbevolen herstelvolgorde

1. **Contract v2:** provenance, tijden, ruwe referentie en kwaliteit toevoegen.
2. **P0-semantiek:** Netatmo-tijd, METAR-categorie, Synoptic-intervallen en
   versheidsregels corrigeren.
3. **Database:** grondwaarnemingen en kwaliteitsbeslissingen append-only
   opslaan.
4. **Stormkoppeling:** grondstation aan radarcontour/WeatherSystem koppelen.
5. **Fusie:** verklaarbare gewogen bevestiging in plaats van `twee stations`.
6. **Pariteit:** HA/worker-resolver en gedeelde logica gelijkmaken.
7. **Diagnostiek:** generieke velden, runtime build-ID en per-stationaudit.
8. **Replay:** historische ruwe data opnieuw interpreteren en uitkomsten
   vergelijken voordat scores operationeel worden.

## Definitie van klaar

Een trackeruitkomst is pas betrouwbaar herleidbaar wanneer één replayrecord
kan tonen:

1. welke bronrecords beschikbaar waren;
2. hun originele waarden, locaties en tijden;
3. hoe iedere waarde werd geïnterpreteerd;
4. welke kwaliteitsweging iedere observatie kreeg en waarom;
5. aan welk WeatherSystem zij gekoppeld werd;
6. hoe daaruit de trackerstatus, grondbevestiging en forecast ontstonden.
