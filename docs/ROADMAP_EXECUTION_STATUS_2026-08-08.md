# Storm Tracker V3 — uitvoerstatus roadmap

> **Historische, sealed momentopname.** Deze status is bewust niet herschreven
> na de lokale codefreeze van 2026-08-11. Gebruik voor de actuele kandidaat-,
> test- en activatiegrenzen `ROADMAP_EXECUTION_STATUS_2026-08-11.md`. Cijfers en
> open punten hieronder blijven alleen bewijs van de toestand op dit eerdere
> meetmoment.

Bijgewerkt: 2026-08-09  
Status: lokale ontwikkelkandidaat; niet vrijgegeven en niet live gedeployed

## Onveranderlijke vrijgaveregel

Een groene unit- of contracttest maakt een meteorologische methode nog niet
operationeel betrouwbaar. Een resultaat mag pas naar Home Assistant wanneer de
volledige bron-, decodeer-, grid-, detectie-, tracking-, ground-, forecast- en
publicatieketen dezelfde actuele desired-state-autoriteit draagt en alle
toepasselijke toelatingsbewijzen bezit. Ontbrekend, verlopen, ambigu, corrupt of
niet-gecertificeerd bewijs resulteert in `unknown`/`indeterminate`, nooit in een
verzonnen droge of veilige toestand.

## Lokaal bewezen fundament

- Home Assistant heeft een expliciete worker-only modus met exclusieve runtime,
  lichte snapshotprojectie en lazy legacy-imports. In die modus behoren geen
  providerfetch, radardecode, tracking, groundfusion, kalibratie of databaseklus
  op de HA-eventloop te draaien.
- Workerresultaten zijn aan de exacte actuele desired-state SHA-256 gebonden.
  Een trackerwijziging maakt de vorige projectie onmiddellijk stale; retained
  MQTT-data van een oudere trackerconfiguratie kan niet opnieuw current worden.
- Operationele snapshots hebben een begrensde geldigheid, receiver-clockskew-
  controle, QoS 1/PUBACK-bevestiging, een crash-durable outbox en begrensde
  snapshotretentie.
- De worker-first coördinator heeft één cross-process eigenaar, exact zes
  content-addressed storebindingen en één seriële completionlane. Een tweede
  proces, subclass of herstart kan dezelfde lane niet tegelijk bezitten; de
  eigendomslease wordt pas vrijgegeven nadat processor-, store- en
  snapshotmutaties werkelijk tot stilstand zijn gekomen.
- De worker houdt een seriële queue, gebruikt maximaal twee rekenthreads, draait
  met verlaagde OS-prioriteit en reserveert minstens één volledige fysieke core
  buiten zijn CPU-affiniteit voor Home Assistant/Zigbee2MQTT.
- Exact-raw acquisitie, capaciteitreservering, child-processisolatie en duurzame
  execution receipts bestaan als gesloten shadow-runtime. Alleen per expliciet
  toegelaten provider/productcombinatie kan een raw handler worden gekozen;
  overige workloads blijven zichtbaar geblokkeerd.
- Elf providerworkloads hebben nu zo'n exacte shadow-raw binding: METAR,
  AEMET-composiet, DWD RV, KNMI RTCOR, DPC SRI, Météo-France DPRADAR,
  NOAA Enterprise Rain Rate, GOES-18 RRQPE, GOES-19 RRQPE, H SAF H40B en
  Met Office UK RATE. Dit zijn elf lokale shadowregels en nul standaard live-
  wiring in `service.main()`. Multi-role captures binden elk
  catalogus-/listing-/objectlid afzonderlijk én als canonieke aggregate aan
  child, recovery en capacity ledger. Zij eindigen bewust bij
  `captured_not_decoded`, `unknown`, `admission=false`.
- Een provider-neutrale terminal-raw→completion-router heeft een gesloten
  consumercatalogus voor exact die elf shadowworkloads. Hij herverifieert
  terminal raw-, recovery-, execution-, capacity- en desired-statebewijs en
  publiceert hoogstens één begrensd lifecycle-event per seriële aanroep. DWD
  behoudt voorlopig zijn bestaande bytepayload; KNMI, NOAA ERR en Met Office
  gebruiken content-addressed manifestreferenties. De router is inert en niet
  door service, scheduler, HA of MQTT geïmporteerd. De KNMI RTCOR-consumer
  herverifieert inmiddels de volledige lifecycle-, desired-, scheduler-, raw-,
  recovery-, execution- en capacityketen in parent én hard begrensde child,
  bewaart output- en processreceipts vóór acknowledgement en blijft factless/
  `admission=false`. De NOAA ERR- en Met Office-consumers hebben dezelfde
  stale/finale desired-, lease-, fysieke evidence- en acknowledgementgrenzen;
  zes overige rawroutes worden door een generieke hard-child manifestaudit
  volledig herhasht. De METAR-consumer commit en herverifieert daarnaast het
  volledige canonieke ground-factset. Daarmee zijn alle elf actieve shadow-
  rawroutes consumer- of auditmatig afgedekt, nog steeds zonder live wiring.
- Alle in de acquisitiecatalogus genoemde credentialvereisten hebben nu een
  gesloten package-inventaris. AEMET is als ontbrekende add-onoptie toegevoegd;
  AWN, KNMI, Synoptic, H SAF en EUMETSAT binden hun vaste environment- of
  bundelautoriteit; Météo-France bindt zijn twee ondersteunde OAuth-vormen;
  Netatmo bindt uitsluitend de drie parent-brokerseeds en heeft bewust geen
  statische access-tokenoptie. De extern geblokkeerde KMI RADQPE-secret heeft
  even bewust geen configureerbare autoriteit.
- De Netatmo parent-OAuthbroker valideert en roteert credentials nu zonder
  providerfoutbody's te lekken. De tokenstate is begrensd, strict-schema,
  symlinkvrij en crash-durable vervangen voordat een vluchtige access-token
  aan een killable child mag worden aangeboden. De acquisition-kandidaat blijft
  geblokkeerd totdat die parent→child-grens zelf is aangesloten en bewezen.
- AWN heeft nu een afzonderlijke exact-native workercapture voor uitsluitend de
  apparaten die door de account zijn geautoriseerd. Beide API-sleutels blijven
  buiten payloads en opslag; de byte-exacte JSON is private raw evidence en
  cumulatieve regenvelden leveren nadrukkelijk geen instantane regenclaim.
- Locatiegebonden acquisitie heeft nu een afzonderlijke content-addressed
  query-authority. Die herbouwt de volledige desired-state, het acquisitieplan
  en de exacte cadencejob voordat een coördinaat RainViewer, Netatmo, Synoptic
  of een toekomstige Open-Meteo-workerhandler mag bereiken. Displaynamen en
  target-ID's blijven uit childpayloads en diagnostiek. Een te grote gedeelde
  groundregio wordt niet stil verkleind maar faalt gesloten met een expliciete
  provider-specifieke opsplitsingsvereiste.
- Het cutovermanifest v4 bindt alle 27 cadence-acquisitieworkloads plus het
  afzonderlijke Blitzortung-event-ingresscontract exact één-op-één aan een
  worker-eigenaar. Ook AWN en de canonicale provider-ID's worden rechtstreeks
  tegen de acquisitiecatalogus gecontroleerd; een nieuwe, dubbele of vergeten
  providerworkload maakt de releasecheck rood. Dit is dekkingsbewijs, geen
  meteorologische admission.
- De acquisitionbronketen heeft een content-addressed sealed export met
  cross-binding van raw-, scheduler-, execution-, capacity- en downstreambewijs.
  Exportplannen en brondata worden vlak vóór de archive-commit opnieuw
  geverifieerd. Dit is bewust géén delete-token: alle oorspronkelijke bytes en
  receipts blijven staan zolang een cross-store atomaire prune-CAS ontbreekt.
- Een afzonderlijke machineleesbare reclamation-gap-authority bindt nu het
  hervalideerde exportplan aan de sealed archivehead en hasht de exacte vijf
  participantgaten plus de ontbrekende cross-store restartcoordinator. De enige
  mogelijke uitkomst blijft `withheld`; dit is compleet gapbewijs, geen
  bronvrijgave.
- De veilige preparehelft van die reclamationstap bestaat nu offline: een
  single-owner/OS-fenced coordinatorjournal, vijf durable sidecar prepare-CAS-
  receipts, exacte sealed resolvers, immutable decision/eventhashes en raw
  child/reference-counting zijn getest tot `PREPARED`. Source-local apply,
  consumerregistry/epoch-root en raw unlinkrestartjournal ontbreken nog;
  `COMMITTED`/`APPLIED` blijven daarom hard machineleesbaar geblokkeerd. Er is
  geen brondelete, capacityrelease of live wiring toegevoegd.
- De radar→analysis-grid→cellen→CellTrack-keten en de cell-specifieke
  groundfusion bestaan als ongewirede shadowketens. De scienceketen heeft een
  crash-atomaire CAS-head, checksummed checkpoints, exacte tail-replay,
  begrensde retentie en een hard afbreekbare spawn-processgrens; een
  2.100-frameproef bleef op circa 242 kB en herstartte exact over revision
  512→513. Groundfusion heeft eveneens een gesloten processgrens en een
  tweefasen sealed-archief vóór hot pruning. Geen van beide mag een
  operationele snapshotclaim genereren zolang meteorologische admission en
  held-out bewijs ontbreken.
- Deterministische nowcastbaselines, STEPS/LINDA-challengercontracten,
  event-gegroepeerde verificatiemetrics en kanskalibratiecontracten zijn lokaal
  testbaar. STEPS/LINDA zijn nog geen operationele provider van kansen.
- De operationele vrijgave-SLO's hebben nu een strikt, content-addressed
  bewijscontract. Alleen complete en onderling vergelijkbare baseline- en
  kandidaatseries kunnen HA-lag, Z2M-latentie en -verlies, queueleeftijd,
  snapshotintegriteit, CPU-beleid, thermische throttling en opslagruimte naar
  releasebewijs omzetten; hiaten blijven `indeterminate`.

## Opnieuw uitgevoerde onafhankelijke controles

De volgende gerichte controles zijn op 2026-08-08 en 2026-08-09 opnieuw
uitgevoerd:

| Controle | Resultaat | Betekenis |
|---|---:|---|
| HA worker-only, bridge, desired-state en snapshotviews | 131 groen | lichte HA-keten en autoriteitswissels |
| workerservice, MQTT-publicatie, scheduler en CPU-topologie | 85 groen | lifecycle, PUBACK, queue en tweethreadbeleid |
| nowcastbaselines, STEPS/LINDA-contract en verificatie | 78 groen | algoritmische contracten; geen skillbewijs |
| validatiecorpus, externe labels en wetenschappelijke gates | 71 groen | eventgescheiden bewijs en harde HA/Z2M-release-SLO's |
| celassociatie, CellTrack, lineage en motionidentity | 66 groen | complete kandidaten; geen vaste snelheidsafkeur |
| operationeel SLO-bewijscontract en release-mapping | 37 groen | volledige series en vergelijkbare runs vereist; nog geen live soakbewijs |
| worker-first eigendom, lifecycle en lineariseerbaarheid | 255 groen; 241 onafhankelijk herhaald | één lane, crashreconciliatie en desired-statewissels; nog ongewired |
| science v5 multi-scan en geïsoleerde replay | 57 + 32 groen | technisch deterministisch; meteorologisch slechter dan v4.1 |
| DWD `+000` sequence, decode, process, grid, handoff en lifecycle | 133 onafhankelijk groen | 36 officiële observaties; geen motion-skillclaim |
| acquisition sealed bronexport | 30 onafhankelijk groen | vervalsing/crash/opslagvol fail-closed; bronreclamatie hard uit |
| provider-/cutoverdekking en acquisitiecatalogus | 66 gericht groen | 27 cadenceworkloads + 1 event ingress en 21 legacy-providermodules exact één eigenaar; KMI/validation/GLM-gaten blijven zichtbaar |
| AWN exact-native accountcapture | 18 groen | raw JSON, privacy, retry/crash en tamper; grondsemantiek/admission nog dicht |
| acquisition query-authority + native payloadcompatibiliteit | 27 gericht; 310 gecombineerd groen | exacte state/plan/job/tijd- en batchbinding; veilige native ID; stale mobiele regio per job geblokkeerd; te grote groundregio fail-closed |
| KMI/Houston multi-provider shadowreplay | 7 onafhankelijk; 109 gecombineerd groen | 44 frames en 6.426/6.426 kandidaatparen; structureel bewijs, geen motion- of admissionclaim |
| multi-role providercapture + crashrecovery | 248 groen; 2 optionele live-GOES-skips; runtime 33 groen | DPC, NOAA ERR, GOES-18/19 en Met Office exact raw; gedeelde provider- en globale batchcapaciteit fail-fast; geen decode/admission/live wiring |
| AEMET + Météo-France exact-raw shadowbatch | 40 runtime; 60 + 1 optionele live-skip native; 122 aangrenzend groen | beide providerketens exact aan child/recovery/capaciteit gebonden; decode, rechten, held-out bewijs en live admission blijven dicht |
| H SAF H40B exact-raw shadowbinding | 212 groen + 1 optionele live-skip; 66 core onafhankelijk groen | elfde actieve shadowregel met exact twee rollen, 66 MiB en 90 s; plain FTP, geautoriseerde golden, decodersemantiek, held-out bewijs en soak blijven dicht |
| Blitzortung durable event-ingress | 181 gecombineerd; 78 ingress/raw onafhankelijk groen | sessie-epochs, bounded queue, gaps, leases en crashrecovery bewezen; rechten, worker-MQTT, decoder, reclamation en live wiring blijven dicht |
| terminal raw→completion-router | 15 gericht; 177 lifecycle/recovery/consumerregressies groen | gesloten catalogus 11/11, exacte bestaande-completion-reproof en exact één seriële publicatie; volledig inert en alle providerconsumers blijven ongewired |
| KNMI RTCOR completionconsumer | 20 gericht; 159 gecombineerd groen | echte Windows-spawn, stale/finale desired-rejection, exacte kwantitatieve decode, fysieke raw/evidence/artifact-reproof, disjuncte stores en output-/processreceipt vóór ack; forged-output, crash/restart en resourcecaps fail-closed; factless en ongewired |
| NOAA ERR completionconsumer | 26 gericht; 189 gecombineerd groen | exact één kwantitatieve decode plus afzonderlijke hard-child finale authorityreproof; stale/finale desired- en leasebarrières, target-regioauthority, fysieke raw/evidence/artefactreproof, exact ConsumptionReceipt, disjuncte stores, pagecap en immutable output zijn fail-closed; factless en ongewired |
| generieke raw-manifestauditconsumer | 27 gericht; 128 gecombineerd groen | exact 6/6 AEMET/DPC/Météo-France/GOES18/GOES19/HSAF; hard-child volledige raw-SHA, tweede pre-ack reproof, stale desired-rejection, forged-output/crash/concurrency/caps; audit-only en ongewired |
| Met Office UK RATE completionconsumer | 25 gericht; 117 gecombineerd groen; 1 optionele live-skip | exact drie rollen/listing- en objectprovenance, kwantitatieve decode, fysieke evidence/artefactreproof, desired-/leasebarriers, durable receipts en hard-resourcecaps; factless en ongewired |
| METAR canonical-ground completionconsumer | 23 gericht; 231 gecombineerd groen | echte spawn, exact raw/job/recovery/lifecyclebewijs, canonieke ground-factset + processreceipt vóór ack, fysieke raw/ground-reproof, desired-/lease-/cancel-/deadlinebarrières, crash/concurrency/poison/caps/privacy; groundconfirmation en admission blijven false, ongewired |
| permanente lifecycle-quarantaine | 64 lifecycle/quarantaine; 87 router/consumerregressies groen | schema-v3 `QuarantineTerminalResult` bindt quarantine- én ConsumptionReceipt atomair vóór cursor, alleen vijf eventgebonden permanente redenen; transient/system/cap/deadline blijft retrybaar; v1/v2-archiefcompatibiliteit, v3 seal/prune/replay, tamper en terminaltopologie fail-closed |
| provider-neutrale completion-outputarchiefkern | 34 gericht + 34 onafhankelijk; 96 aangrenzend groen | constructor-bound source-reproofauthority, append-only seals, DWD-specifieke checksumreproof, CAS/crash/tamper/caps en 24-uurs hot-windowplanning; nog geen providerintegratie, hot prune of bronreclamatie |
| immutable multi-provider replaycorpus | verifier + 7 adversariële selftests groen | 9 content-addressed objecten en 8.621.601 ingebedde providerbytes voor NOAA ERR, METAR en GOES-18/19; DWD/KMI/Houston hashgebonden gerefereerd; KNMI/Met Office contractfixtures strikt buiten de echte-providerlane |
| Open-Meteo exact-raw shadowcapture | 67 gericht; 308 finale blockercombinatie groen | strikte authority/query/shape/units/UTC-cadence, raw-before-inspect, tamper/restart en canonieke CC BY-attributionpolicy; activation/recovery/router blijven exact 11/11/11 omdat het globale 19-variabelen-JSON-contract geen vaste upstreammodelbron bewijst; machineleesbare blocker + 4 regressies groen |
| Open-Meteo quota-/termsauthority | 30 gericht; 97 capture/query/usage en 211 breed groen; 1 platformskip | exacte `2*N` requestcharge, vier UTC-vensters, one-shot permit, 900-s cadence en duurzame vijf-locatiegrens; crash/CAS/clockrollback/terms-latch/retentie/caps bewezen; activatie blijft atomair dicht tot een vaste officiële modelbronauthority en volledige child/recovery/routerbinding bestaan |
| worker-first service/compositiongrens | 247 onafhankelijk groen | opt-in factory, legacy-heavy wederzijds uitgesloten, executor 105 s binnen vaste 120-s servicelane; standaard uit |
| provider-secretinventaris + Netatmo parent-OAuth | 51 + 134 groen | alle catalogusvereisten hebben een expliciete add-on/externe autoriteit; OAuth-fout, rotatie en statecorruptie lekken niet; Netatmo nog niet gepromoveerd |

Dit zijn deelbatches. De volledige suite, foutinjectie, langdurige resource-soak
en releaseverificatie moeten na de laatste integratiewijziging opnieuw worden
uitgevoerd.

## Beschikbare echte replaydata en wetenschappelijke grens

De read-only kalibratiebackup
`storm_tracker_calibration_20260730_092754.sqlite3` is 146.243.584 bytes en bleef
bij de bestaande inventarisatie hash-identiek. De inventaris meldt onder meer:

- 2.230 opgeslagen provider-overlays;
- 475 unieke radartijdstippen van 2026-07-28 21:20 UTC tot
  2026-07-30 07:20 UTC, hoofdcadans 300 seconden;
- 72 genormaliseerde pySTEPS-cacheframes op 36 unieke tijdstippen;
- een afzonderlijke echte KMI GeoTIFF-reeks van exact 20 opeenvolgende frames;
- een Houston/RainViewer-cache van 24 frames, door een cadence- en gridbreuk
  expliciet opgesplitst in drie coherente cohorten van 12, 2 en 10 frames;
- een verzegelde providerreplayauthority met echte NOAA ERR-, METAR/AWC- en
  GOES-18/19-bytes. Het canonieke manifest heeft SHA-256
  `99500ab54f19196c0115176f31115039ec9b992d3e34b54235e91d726e06d5e9`;
  DWD/KMI/Houston blijven als bestaande, afzonderlijk hashgebonden authorities
  gerefereerd en worden niet opnieuw gekopieerd;
- 36 officiële DWD RV-basistijden op vijfminutencadans, waarvan uitsluitend
  het `+000`-observatielid als observatie mag dienen; forecastleads `+005` tot
  `+120` blijven afzonderlijk gehasht en uitgesloten van motionreplay;
- 5 positieve, maar trigger-geselecteerde grondoutcomes;
- geen bruikbare onafhankelijk gelabelde droge groundoutcomes;
- geen opgeslagen passageprobabilities;
- geen onafhankelijk beoordeelde celidentiteit/lineage-truth.

Daarom is deze data geschikt voor determinisme, continuïteit, capaciteit,
restartreplay en exploratieve bewegingsvergelijking. Zij is onvoldoende om
trackingnauwkeurigheid, snelheidserror, false-clear-rate, Brier-score of
gekalibreerde passagekansen wetenschappelijk vrij te geven. Provider-overlays
zijn bovendien niet gelijk aan de oorspronkelijke upstream bytes.

Op exact dezelfde 36 genormaliseerde KMI-cacheframes bleef v4.1 de minst
slechte shadowbaseline. De v5 multi-scan-MAP-challenger verhoogde het aantal
intervallen boven 200 km/u van 120 naar 131, de mediaan van 239,43 naar
262,20 km/u en p95 van 1.134,19 naar 1.149,66 km/u. V5 is daarom bewust niet
gepromoveerd of aan CellTrack/WeatherSystem teruggebonden. Dit is een negatief
maar bruikbaar resultaat: determinisme of complexere associatie is zonder
onafhankelijke identity- en speed-truth geen bewijs van betere beweging.

De officiële DWD-sequence verwerkte 36/36 `+000`-frames met nul failures,
retries, resource-terminals of achtergebleven leases/children. Het bronmanifest
heeft SHA-256 `20335e6c50b35d57dd111bea7e3670f515e933da5239ba39e74ccebc41030852`;
het sequencemanifest `e94b8f14393fb0e2076fd54ff7b0a2c0133416cd1cde87bb18acd01df5eda48a`
en het procesmetricsmanifest
`7e501592a64faf9bece7af064a425a2d7a3f96719ae90847f7b9753a9a308b60`.
Alle 39 exportbestanden kwamen overeen met hun contenthash. Onder de verlaagde
960-MiB-processgrens bedroeg p95 13,94 s wall, 13,64 CPU-s, 223,84 MiB echte
peak RSS en 679,42 MiB kernel-boundarymemory. Dit is corpus-/testhostbewijs,
geen live-NUC-SLO en nog geen motion-skillbewijs.

Een afzonderlijke multi-provider replaymatrix heeft de 20 KMI-GeoTIFFs en alle
24 Houstonframes door dezelfde v4/advected shadowketen gevoerd. KMI verantwoordde
419/419 kandidaatparen; Houston 6.007/6.007. Van de gezamenlijke 6.426 paren
gebruikten 1.068 exact gebonden advected evidence en 5.358 een expliciete
unadvected fallback; geen kandidaat werd door snelheid, afstand of nul-IoU
verwijderd. Iedere cohort herstartte en duplicate-replayde exact. NOAA ERR
decodeerde daarnaast 88.101/88.101 pixels; OPERA bleef correct withheld door
verouderde raw provenance, KNMI door ontbrekende raw HDF5. Alle matrixclaims
blijven `admission=false`, `motion=false` en `snapshot=false`. Het verzegelde
rapport heeft bestands-SHA-256
`7f408e1855e7ea18946af74bdf9e04d6764d80fbdfc4a1a253d01a890dfe1857`.

De eerste gesloten science-replay op het België-grid eindigde correct als
`indeterminate_no_observable_cells`: alle 36 gecertificeerde detectieframes
bevatten bij de vooraf vastgelegde 1,0 mm/u-objectdrempel nul cellen. Zowel
v4.1 als v5 produceerde daarom nul tracks, snelheden, sprongen of systemen;
nodata/undetect werd nergens als droog ingevuld. Evidence-JSON SHA-256 is
`2694c8826e0a90cf4ef0c0abeaea9c17ff063b43c217d6c8a5f850cad8cd981f`
en het rapport
`ee688e42c6fe86a11393138897f82b15b53f5d555d2f8102c489fbfe55a93285`.
Dit bewijst fail-closed gedrag, niet dat er in het volledige DWD-raster geen
regen bestond. De afzonderlijke, vooraf vastgelegde bronuitgelijnde Duitse
regen-ROI is inmiddels eveneens uitgevoerd. De 850x700 bron-ROI werd zonder
capverhoging area-weighted naar 350x425 cellen van 2 km getransformeerd. Nodata,
undetect en mixed support bleven missing; 291 van 24.820 ruwe bronpixels van
minstens 1 mm/u vielen in mixed targetcellen en werden fail-closed onthouden.
De 2-km-replay is daardoor geografisch volledig voor de geselecteerde ROI,
maar verandert object- en coretopologie en is geen 1-km-truth.

V4.1 voltooide dezelfde 36 sealed `+000`-observaties met exacte candidate-
accounting, expliciete herstarts 20->21 en 32->33 en een byte-identieke durable
duplicate. De eindstate bevatte 57 tracks en 696 intervalhypotheses. Hun mediaan
was 32,44 km/u, p95 633,62 km/u en maximum 1.637,99 km/u; 128 intervallen lagen
boven 200 km/u. Dit is negatief continuiteitsbewijs, geen snelheidswaarheid en
geen reden voor promotie. De onafhankelijke rasterreferenties werden vooraf
vastgezet en niet voor tuning gebruikt: de eerste->laatste aggregate support-
centroid gaf descriptief 145,92 km/u, terwijl masked phase correlation in 31
van 35 intervallen een 2-km oostwaartse verschuiving per vijf minuten (24 km/u)
vond. De vier extreme phase-correlationuitbijters hadden slechts 3,65-4,59%
overlap en blijven expliciete low-support uncertainty.

De v5 multi-scan-challenger werd op frame 19 door de vaste 60-s CPU-cap
afgebroken (`cpu_limit_exceeded`, 60,377112 s). Frame 19 werd niet gecommit en
de duurzame head bleef revision 18. Er volgde geen retry, capverhoging of
algoritmetuning. V5 is op deze ROI dus uitsluitend resource-negatief bewijs,
geen voltooid trackingresultaat. JSON-evidence SHA-256 is
`74b0f1678f8a1d1e5270bd1c21ec7ca773e231b0b316929bc74daac2f44332fd`;
het Markdownrapport heeft SHA-256
`eace489e1b654e20095b4259c8de46ad8f0344dd965ba00783b565cbc3a83d1c`.
De volledige grenzen en hashes staan in
`docs/SCIENCE_SHADOW_DWD_GERMANY_ROI_REPLAY.md`. Admission blijft expliciet
false; WeatherSystem is niet hergebonden en niets is live gepubliceerd.

## Actuele toelatingsgrenzen

- DWD RV en KNMI RTCOR zijn exacte raw-only shadowkandidaten. Voor DWD bestaat
  bovendien een gesloten `+000`-decode→canonical-grid→cells-completionketen
  met hard-processreceipt. Raw-opslag en technische decode blijven echter geen
  bewijs dat het meteorologische veld of de afgeleide identiteit operationeel
  is toegelaten.
- OPERA blijft geblokkeerd zolang het grote full-objectpad, de productrechten en
  de volledige decodeer-/QI-/Z-R-certificatie niet zijn opgelost.
- KMI RADQPE/public composite blijft voor operationele kwantitatieve conclusies
  geblokkeerd zolang productsemantiek en decoderketen niet exact gecertificeerd
  zijn. De 20-frame GeoTIFF-reeks mag wel expliciet exploratief worden gebruikt.
- Netatmo, Synoptic en METAR mogen alleen bijdragen binnen hun eigen
  meetsemantiek, tijdvenster, fysieke site, representativiteit en
  sensorcapaciteit. Cumulatieve neerslag is geen instantane regenintensiteit;
  een droge conclusie vereist expliciete absence-capability.
- Netatmo- en Synoptic-coördinaten zijn nu exact state/plan/job-gebonden, maar
  regio's die respectievelijk meer dan 20 of 50 km nodig hebben vereisen nog
  provider-specifieke subjobs met vooraf gereserveerde totale capaciteit.
- De nieuwe science- en groundketens blijven shadow totdat processisolatie,
  retentie/restart, held-out meteorologische replays en admissioncertificaten
  aantoonbaar compleet zijn.

## Nog te sluiten vóór een veilige kandidaatdeploy

1. Een echte cross-store reclamatieauthority bouwen zonder crashrecovery,
   executionreceipts, gedeelde raw-objectreferenties of capaciteitsboekhouding
   te breken. Een read-only sealed bronexport is nu bewezen, maar geeft hard
   `source_reclaim_authorized=false`; de oorspronkelijke raw-, scheduler-,
   execution- en capacityauthorities blijven daarom terecht append-only. De
   nieuwe machineleesbare reclamation-gap-authority bindt plan en archivehead,
   geeft eveneens uitsluitend `withheld` en benoemt per participant exact de
   ontbrekende prepare-CAS, committed apply, archive-resolver en gezamenlijke
   restartcoordinator. Dat sluit het inventarisatiegat, niet het pruninggat.
   De offline preparecoordinator sluit inmiddels het restartbare
   `PREPARING`->`PREPARED`-deel met vijf sidecar-CAS-receipts en exacte sealed
   resolvers. Het positieve pruninggat blijft ongewijzigd: geen source-local
   applytransacties, complete consumerregistry/epoch-root of raw unlinkjournal,
   dus geen `COMMITTED`, `APPLIED`, delete of capacityrelease. Ook de nieuwe
   completion-outputstores mogen niet als onbeperkt archief worden behandeld:
   bij de huidige cadences is 4.096 receipts voor een vijfminutenprovider circa
   14,2 dagen, terwijl de gedeelde 8.192-row manifestauditstore voor drie
   vijfminuten- en drie tienminutenroutes in circa 6,3 dagen volloopt. Vóór
   live wiring is daarom een checksummed sealed outputarchief met hot-window-
   replay, crashatomische prune en archive-backed exacte resolver verplicht;
   een grotere NAS-cap verschuift dit gat alleen.
2. De ketens onder één worker-owned planner/lifecycle koppelen zonder parallelle
   rekenroutes of meteorologische verwerking in Home Assistant. De eigendoms-
   en runtimegrens en een inert production-like DWD-composition-root bestaan
   nu. Desired-stateproductie, acquisitionplanner, science/ground en de echte
   servicelifecycle blijven nog expliciet ongewired.
3. Alleen toegelaten, committed workerfeiten naar een rijke operationele
   snapshot projecteren; alle overige velden blijven `unknown`.
4. Foutinjectie uitvoeren voor proceskill, hostrestart, MQTT-disconnect,
   duplicate/out-of-order input, corrupte SQLite, opslagvol en clockskew.
5. De bestaande echte reeksen read-only door de nieuwe keten spelen en
   continuïteit, sprongen, lineage-accounting en resourcegrenzen rapporteren
   zonder truthclaim.
6. Nieuwe onafhankelijk gelabelde weersgebeurtenissen verzamelen voor echte
   tracking-, ground- en forecastskillmeting.
7. Een representatieve NUC-soak uitvoeren met HA- en Zigbee2MQTT-latentie,
   queueleeftijd, CPU, geheugen, temperatuur, databasegroei en MQTT-liveness.
   De meetseries moeten door het content-addressed contract in
   `storm_tracker_worker/operational_slo_evidence.py` worden gevalideerd; het
   bestaan van die validator is zelf geen geslaagde soak. Een mogelijke
   verplaatsing van de worker naar een DS1618+ is afzonderlijk afgebakend in
   `docs/NAS_WORKER_DEPLOYMENT_DS1618_PLUS.md`; ook daar blijft een echte
   co-location-, filesystem-, power-loss- en thermal soak verplicht.
8. De volledige testsuite schoon uitvoeren en een release-evidencebundel met
   hashes, versies, migratie-, deploy- en rollbackproef opleveren.

## Go-livebeleid

Er wordt vanuit deze ontwikkelstand niets live gedeployed. Een latere kandidaat
wordt eerst offline/replay getest, daarna in worker-only shadow, vervolgens als
fictieve tracker-canary en pas na expliciete eindvalidatie gefaseerd vrijgegeven.
De basislijn- en soakmeting volgt daarbij verplicht
`docs/OPERATIONAL_SLO_MEASUREMENT_PROTOCOL.md`.
