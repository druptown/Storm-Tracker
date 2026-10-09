# Worker acquisition plan — ongewirede contractkandidaat

Status: **candidate-only, niet gewired, niet operationeel**.

Implementatie: `storm_tracker_worker/worker_acquisition_plan.py`

Tests: `tests/test_worker_acquisition_plan.py`

## Doel en grens

Deze laag vertaalt precies één canoniek gevalideerde
`stv3-worker-desired-state-v1`-snapshot naar een deterministisch plan voor
worker-owned acquisitie. Het plan beschrijft radar-, satelliet-, grond-, model-
en bliksemworkloads, maar importeert of roept geen providerhandler aan. Het
bevat geen netwerkcode en is nergens geregistreerd in `service.py`, de
autonome scheduler, de capture registry, Home Assistant of configuratie.

De contractlaag doet vier dingen:

1. de desired-state opnieuw valideren en cryptografisch binden;
2. trackers deterministisch in begrensde acquisitieregio's onderbrengen;
3. globale, dekkingsbrede en regionale workloadscopes dedupliceren;
4. per cadansslot precies identificeerbare jobs, deadlines, budgetten en
   terminale receipts afleiden, met maximaal één in-flight job.

Dit is geen decoder-, QC-, fusion-, admission-, tracking- of forecastlaag. Een
`contract_ready_unwired` workload is uitsluitend planbaar; die status zegt
niets over meteorologische operationele geschiktheid.

## Inputauthority en snapshotbinding

`build_worker_acquisition_plan(snapshot)` accepteert uitsluitend een echt
`DesiredState`-object. Een mapping of zelf samengesteld lookalike-object wordt
afgekeurd. De planner parseert `snapshot.canonical_bytes` opnieuw via het
bestaande desired-statecontract en vergelijkt het volledige gereconstrueerde
object en `state_sha256` met het aangeleverde object.

Het plan bindt expliciet:

- desired-statecontract, producer, authority epoch en generation;
- `snapshot_sha256`;
- de gesloten acquisitiecatalogus via `catalog_sha256`;
- de immutable cadence-/deadline-/budgetregels via
  `cadence_profile_sha256`;
- iedere regio via `region_sha256`;
- iedere intent via `intent_sha256`;
- het hele canonieke plan via `plan_sha256`.

Er is geen runtimeconfigoverride. Een wijziging aan catalogus, cadans,
deadline, prioriteit of budget verandert de profiel- en planchecksums.

## Regiovorming en deduplicatie

Alleen `available=True` targets worden opgenomen. Targets zijn al canoniek op
`target_id` gesorteerd door het desired-statecontract. De planner gebruikt een
deterministische complete-link clustering: een target mag uitsluitend in een
bestaande cluster wanneer zijn afstand tot **ieder** bestaand clustermember
binnen `region_sharing_distance_km` valt. Daardoor kan een keten van nabije
trackers niet ongemerkt één duizenden kilometers lang gebied vormen.

Het regioncenter is een sferisch gemiddelde, niet een naïef gemiddelde van
lengtegraden. De queryradius is het maximum van:

`afstand(center, target) + observation_radius_km`

Globale producten worden één keer gepland. Nationale of satellietdekkingen
worden één keer per vaste coverage-scope gepland. Regionale bronnen worden één
keer per gedeelde acquisitieregio gepland. De deduplicatiesleutel is exact
`(workload_id, scope_id)`; conflicterende content onder dezelfde sleutel faalt
gesloten.

Harde grenzen:

- maximaal 64 acquisitieregio's;
- maximaal 512 intents;
- maximaal 1 MiB canoniek plan;
- geen onbegrensde vrije payloads.

## Gesloten workloadcatalogus

De catalogus bevat contractregels voor deze workloadfamilies:

| Klasse | Planbare providers/productgroepen |
|---|---|
| radar | AEMET, DPC SRI, DWD RADOLAN RV, KNMI RTCOR, Météo-France DPRadar, Met Office UK RATE, OPERA DBZH/QI, RainViewer visual + coverage; KMI is drie afzonderlijke geblokkeerde producten |
| satelliet | NOAA GOES-18/19 ABI RRQPEF, NOAA Enterprise Rain Rate GLB-5, H SAF H40B |
| grond | Ambient Weather Network authorized devices, AviationWeather METAR-cache, Netatmo getpublicdata, Synoptic Time Series |
| model/validatie | Open-Meteo native forecastrespons; geblokkeerde GeoSphere/MeteoLux location validation; geblokkeerde ItaliaMeteo historical catalog en ICON-2I-via-Open-Meteo modelquery |
| bliksem | EUMETSAT MTG LI; NOAA GOES-18/19 GLM blijft geblokkeerd tot dynamic-member recovery |

De namen hierboven zijn acquisitiecontracten. Ze vormen geen bewijs dat een
live handler, decoder of admissiongate voltooid is.

Negen productworkloads blijven altijd zichtbaar in `blocked_capabilities`,
maar kunnen nooit een intent of job opleveren:

| Product | Fail-closed reden |
|---|---|
| KMI appv4 | categorische visualisatie, gedecommissioneerd en niet admitted |
| KMI publieke WCS | onbekende quantity/unit/nodata; geen productieretentie |
| KMI native RADQPE | externe delivery-/credentialauthority en uitvoerbare limieten ontbreken |
| MeteoLux | location/model-validationcontract niet bewezen |
| GeoSphere | location/model-validationcontract niet bewezen |
| ItaliaMeteo historiek | dagelijks duplicaat van DPC SRI, geen realtime radar |
| ItaliaMeteo model | duplicaat Open-Meteo-modelpad zonder enkelvoudige authority |
| GOES-18 GLM | dynamische memberrecovery ontbreekt |
| GOES-19 GLM | dynamische memberrecovery ontbreekt |

De module controleert bij import dat deze geblokkeerde workloadset exact
compleet is. Stil weglaten of per ongeluk dispatchbaar maken is daarmee een
harde fout.

Blitzortung staat niet in deze cadencecatalogus. Het aparte
`lightning.blitzortung.event_ingress`-contract declareert MQTT push,
`cadence_seconds=null` en `no_absence_claim`. Het is exact één keer eigendom in
het cutovermanifest, maar blijft geblokkeerd op workerownership, backpressure,
gapreceipts, decoderbewijs en soak.

De vijf oude workload-id's hebben uitsluitend niet-transparante
rebuildmigraties. Vooral `radar.kmi.radqpe` kan niet als alias worden gebruikt:
die oude naam wordt expliciet gesplitst in appv4 visual, public-WCS unknown en
native RADQPE. Zie `PROVIDER_CATALOG_TRUTH_2026-08-09.md`.

## Secrets

Plan, jobs, diagnostics en receipts bevatten nooit secretwaarden. Workloads
verwijzen uitsluitend naar abstracte identifiers met het gesloten patroon
`worker_secret.<naam>`, bijvoorbeeld `worker_secret.knmi_api_key`.

De dispatcher krijgt alleen een verzameling *beschikbare requirement-ID's*.
Onbekende identifiers, tokens, sleutels of andere waarden worden afgekeurd.
Een job met ontbrekende requirement krijgt zichtbaar
`secret_requirement_unavailable`; de job wordt niet naar ready gepromoveerd.
De latere executionlaag moet de werkelijke waarde uit worker-owned secret
storage halen zonder die aan plan, job-id of receipt toe te voegen.

## Cadansslot, deadline, budget en identiteit

Voor iedere intent en `as_of` geldt:

```text
slot_index   = floor(as_of / cadence_seconds)
scheduled_for = slot_index * cadence_seconds
deadline_at  = scheduled_for + deadline_seconds
```

`deadline_seconds` is altijd positief en nooit groter dan de cadence. Bij
`as_of >= deadline_at` wordt de job zichtbaar geblokkeerd als
`deadline_expired`; hij kan niet in de ready queue komen.

Iedere job bindt:

- plan- en snapshotchecksum;
- intent-id en intentchecksum;
- workloadklasse, provider en product;
- scope, regio's, targets, landen en eventueel satelliet-id;
- cadence, slotindex, slottijd en absolute deadline;
- prioriteit;
- maximum transportbytes, artifactaantal en wall-clockseconden;
- uitsluitend abstracte secret-requirements.

De identityhash levert `acquisition:<sha256>`; een tweede `job_sha256` bindt
die identity aan het volledige record. Exact dezelfde snapshot en hetzelfde
cadansslot leveren exact dezelfde job. Een nieuw slot of gewijzigde snapshot
levert een andere job.

## Capaciteit en seriële dispatch

`build_worker_acquisition_dispatch` produceert een deterministische,
prioriteitsgesorteerde prefix binnen vier harde grenzen:

- jobaantal;
- som van gereserveerde transportbytes;
- som van gereserveerde artifactaantallen;
- som van gereserveerde wall-clockseconden.

Zodra de eerstvolgende job een grens overschrijdt, sluit de capaciteit voor de
rest van die planning. De gehele staart blijft zichtbaar als
`dispatch_capacity_exhausted`; lager geprioriteerd werk kan dus niet langs een
te zware hogere prioriteit springen.

`WorkerAcquisitionDispatcher.claim_next()` geeft maximaal één job vrij.
Herhaalde of gelijktijdige threadclaims krijgen dezelfde in-flight identity.
Pas na een geldige terminale receipt, of na het verstrijken van de deadline,
kan een andere job worden geclaimd. `serial_concurrency` is hard `1`.

## Receipts, replay en idempotency

Een terminale receipt bindt `plan_sha256`, `job_id`, `job_sha256`, outcome en
`completed_at`. Mogelijke outcomes:

- `succeeded`: vereist een SHA-256 van de echte artifact/capture receipt en
  verbiedt een errorcode;
- `terminal_failed`: vereist een begrensde errorcode en verbiedt een artifact;
- `deadline_expired`: vereist een begrensde errorcode en een tijdstip op of na
  de jobdeadline.

Iedere receipt heeft een eigen `receipt_sha256`. Exacte duplicaten dedupliceren;
twee verschillende terminale receipts voor dezelfde job zijn een conflict.
Een geldige huidige-slotreceipt onderdrukt de job bij replay. Een nieuwe
dispatcher met hetzelfde plan en dezelfde receipts levert dezelfde volgende
job. Een crash vóór ontvangst van de receipt levert bewust dezelfde job
opnieuw; de latere handler moet de stabiele `job_id` als idempotencykey gebruiken.

Receiptsets zijn begrensd op 4096 records in deze ongewirede kandidaat.

## Geteste invarianten

De gerichte suite test onder meer:

- byte-identieke deterministische plannen en targetvolgorde-onafhankelijkheid;
- snapshot-, catalogus-, cadence-, regio-, intent-, job- en receiptbinding;
- complete-link regiovorming en scope-deduplicatie;
- alle vijf workloadklassen;
- expliciete fail-closed productsemantiek voor KMI, GeoSphere, MeteoLux,
  ItaliaMeteo en GOES GLM, plus afzonderlijk Blitzortung event-ingressownership;
- uitsluitend abstracte secret-requirements;
- stabiele cadence-slotidentiteit en strikte deadlineboundary;
- capaciteit als deterministische prefix;
- success/failure/deadline-receipts, exacte replay en conflictgedrag;
- strikt één in-flight job, ook bij gelijktijdige threadclaims;
- afwezigheid van netwerkcode, providerhandlers en live wiring;
- `ResourceWarning` en `PytestUnraisableExceptionWarning` als testfout.

## Open blockers vóór enige live wiring

1. **Geen durable outbox of receiptjournal.** De stateful dispatcher is bewust
   in-memory. Productiewiring vereist crash-durable, procesveilige opslag met
   fencing/leases en schemafingerprint, of een formeel bewezen binding naar de
   bestaande worker-outbox.
2. **Geen handlers.** Deze module controleert niet of iedere contractregel al
   een exact-native capture-implementatie heeft. Handlerselectie hoort pas na
   een aparte gesloten, geteste activeringsmatrix.
3. **Geen credentialresolver.** Alleen requirement-ID's bestaan; worker-owned
   resolutie, rotatie, least privilege en redaction moeten nog worden gebouwd.
4. **Coverage is planningmetadata.** Countrycodes en vaste bounding boxes zijn
   geen bewijs van actuele radardekking, providerbeschikbaarheid of kwaliteit.
   Officiële coverage footprints en versiebeheer moeten die regels vervangen.
5. **Cadans/budgetten zijn kandidaatprofielen.** Ze zijn begrensd en
   checksummed, maar nog niet gecertificeerd tegen provider-rate limits,
   echte payloadverdelingen, NUC thermiek, HA/Z2M latency of worst-case storingen.
6. **Geen catch-uphistoriek.** Alleen het actuele cadence-slot wordt gepland.
   Een gecontroleerd backfill/catch-upbeleid moet apart worden ontworpen.
7. **Geen cross-process serialiteit.** Threadclaims zijn geserialiseerd; een
   proceslock of durable lease-authority is verplicht vóór productie.
8. **Geen meteorologische admission.** Exact-native acquisitie en provenance
   maken een bron niet automatisch kwantitatief, representatief of betrouwbaar.
   Decoder-QC, held-out validatie en bron-/productgates blijven verplicht.
9. **Geen live wiring of soak.** Er is bewust niets gewijzigd aan service,
   scheduler, registry, config of Home Assistant. Een toekomstige cutover
   vereist regressietests, failure injection en langdurige NUC/HA/Z2M-soak.
