# Ground/visual location shadow batch

Status: **dormant, worker-only, fail-closed en niet live gewired**. Geen regel
in deze batch heeft `ACTIVE_SHADOW_RAW`; er is geen import vanuit service,
Home Assistant, MQTT of de autonome acquisitionruntime.

## Gesloten bereik

De batch bereidt uitsluitend de reeds bestaande exact-native captures voor:

| workload | handler | raw provider | rollen | harde envelop |
|---|---|---|---:|---:|
| `ground.netatmo.region` | `netatmo_getpublicdata_exact_raw_v1` | `netatmo` | `response_json` | 8 MiB / 1 / 45 s |
| `ground.synoptic.region` | `synoptic_timeseries_exact_raw_v1` | `synoptic` | `response_json` | 16 MiB / 1 / 45 s |
| `radar.rainviewer.visual` | `rainviewer_visual_region_exact_raw_v1` | `rainviewer` | manifest + 9 radar-PNG + 9 coverage-PNG | 38 MiB / 19 / 75 s |

KMI en alle andere providers zijn niet gewijzigd. De hoofdactivatiematrix
blijft de bestaande acht actieve handlers bevatten. De drie regels hierboven
staan alleen in `LOCATION_SHADOW_CANDIDATE_RULES`; hun child- en
recoverybindings zijn wel volledig geregistreerd en exact aan elkaar gelijk.

## Eén identiteit, twee veilige vormen

De scheduler/capacityledger behoudt altijd de canonieke oorsprong
`acquisition:<64 lowercase hex>`. De locatiehandlers mogen die vorm niet als
native ID bewaren, omdat een strikte credentialscanner zo'n volledig hex-token
niet veilig van een secret kan onderscheiden.

`safe_native_job_id(job_id)` is daarom de enige omzetting:

1. accepteer exact de canonieke oorsprong en niets anders;
2. Base32-codeer de volledige 256-bit oorsprongsdigest;
3. plaats een niet-hex `:z:`-scheiding tussen begrensde chunks;
4. gebruik die deterministische alias voor iedere native receipt en capture.

De query-authority gebruikt exact dezelfde pure functie. Childcode accepteert
geen alias uit een request. Recovery ontvangt evenmin een alias van een caller:
ze leidt hem na iedere start opnieuw af uit de oorspronkelijke ledgerjob en
eist dezelfde afleiding voor iedere rol en capture. Een andere oorsprong,
onveilige fallback, drift, attempt buiten de grens of aliascollision faalt
gesloten.

Synoptic is de enige rol met een meervoudige receiptsuffix:
`:response_json:attempt:N`, waarbij `N` exact 1..8 is. De capture blijft exact
`:capture`. De poging verandert de oorsprong of capture-identiteit niet.

## Locatie- en tijdauthority

`build_guarded_shadow_child_request` accepteert alleen een volledige,
opnieuw herbouwbare keten:

1. canonieke desired state;
2. het daaruit herbouwde acquisitionplan;
3. exact de huidige cadence-slotjob;
4. voor Netatmo, Synoptic en RainViewer exact de opnieuw herbouwde
   `AcquisitionQueryAuthority`;
5. worker-lokale secret-presence-authority en harde raw-storelimieten.

Voor de drie locatieproviders wordt alleen de minimale native
payload geprojecteerd. Het childrequest bevat daarnaast een checksummed guard
met job-/payload-/authorityhash, slottijd, deadline en eventueel het geldige
venster van een dynamische positie.

De child valideert deze guard twee keer met zijn eigen vertrouwde wall clock:
bij binnenkomst en opnieuw na lokale store-initialisatie, op de laatste grens
vóór de native handler netwerk-I/O kan starten. Een verlopen deadline,
toekomstige of verouderde dynamische positie, gewijzigde payload, vervalste
alias-hash of opnieuw checksummed forgery bereikt de provider niet.

## Meteorologisch fail-closed geometrycontract

- `location_accuracy_m` wordt aan de benodigde grondradius en RainViewerhorizon
  toegevoegd. Een `person`/`device_tracker` zonder accuracy faalt gesloten;
  `null` is alleen bij een statisch geconfigureerd of exact fictief testpunt
  een exacte configuratiepositie.
  Onzekerheid wordt dus nooit stil als extra dekking geïnterpreteerd.
- Synoptic gebruikt het effectieve vijf-decimale wirecentrum. De afstand tussen
  het authoritycentrum en dat wirecentrum wordt bij de radius opgeteld en de
  som wordt naar buiten afgerond. Overschrijding van 50 km vereist fanout.
- Netatmo gebruikt een naar buiten afgeronde sferische bounding box. Een radius
  boven 20 km, poolomsluiting of datumgrenskruising vereist providerfanout.
- RainViewer eist dat de volledige horizon binnen de bewezen 3x3 Web-Mercator-
  tegelunie én onder 1.000 km ligt. Poolrand of onvoldoende tegeldekking vereist
  fanout; een coverage-PNG blijft nodig om `geen echo` van `geen dekking` te
  onderscheiden.

Geen grens wordt geknipt, geen willekeurig target wordt gekozen en een block
wordt nooit als droog of als meteorologische waarneming gepubliceerd.

## Privacy en resultaatoppervlak

Coördinaten en volledige queryauthority bestaan alleen in de tijdelijke
providerpayload en in de bestaande exacte durable providerprovenance waar die
voor byte-/requestidentiteit noodzakelijk is. Het childresultaat en de
bounded diagnostiek bevatten uitsluitend workload-/handlerrolnamen, aantallen
en hashes. Ze bevatten nooit:

- oorsprongsjob of veilige native alias;
- target-/region-/resolveridentiteiten of displaynamen;
- query-key, authoritybytes of authorityhash;
- latitude, longitude, bounding box of radius;
- credential, token, application key of Authorization-header;
- payloadbytes, providerexception of upstream-ID.

Alle resultaten blijven `operational_admission_eligible = false` en
`weather_state = unknown`.

## Crash/recoverybewijs

De dormant recoverycatalogus legt per handler exact provider, product,
capturesuffix, alle rollen, productsuffix, contenttypes, bytegrens en native
provenancevorm vast. Recovery valideert echte Synoptic- en RainViewer-
provenance en upstreamidentiteiten, bindt de veilige alias terug aan precies
één oorspronkelijke capacityreservation en commit met CAS. Restartreplay is
idempotent. Een onbekende/missende/extra rol, ander product, attempt 0/9,
andere alias of andere reservation faalt gesloten.

Netatmo heeft hetzelfde dormant role/product/recoverycontract, maar wordt niet
uitgevoerd zolang de worker geen beoordeelde OAuth-tokenauthority bezit.

## Exacte promotion gates

1. **Synoptic:** expliciete goedkeuring van providervoorwaarden voor duurzame
   raw-retentie.
2. **RainViewer:** één expliciete rights/attribution/cache/load-goedkeuring
   volgens de Weather Maps API-voorwaarden.
3. **Netatmo:** dezelfde raw-retentiereview, plus een crash- en privacyveilige
   worker-side OAuth refresh/access-tokenauthority. De huidige add-on levert
   client-ID, client-secret en refresh-token, maar geen
   `STV3_NETATMO_ACCESS_TOKEN`; een token mag nooit via payload, receipt,
   diagnostiek of log worden aangeleverd.

Pas na de betreffende gate mag een regel atomisch met runtimecapaciteitsprofiel,
retentiebeleid en soak/canary naar `ACTIVE_SHADOW_RAW` promoveren. Dat is nog
niet gebeurd.

## Gericht bewijs

`tests/test_worker_acquisition_location_shadow.py` dekt contractpariteit,
dormantstatus, veilige-aliasdeterminisme en collision/forgery, accuracy,
Synoptic-wiredekking, deadline/dynamische freshness vlak vóór I/O,
datumgrens/pool/radius/tegel-fanout, resultaatprivacy, exact rolemateriaal en
echte restartrecovery voor Synoptic-attempts en alle 19 RainViewerrollen.
