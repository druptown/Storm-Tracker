# Worker ground-fusion shadow-runtime

Status: lokaal en shadow-only; niet vanuit `service.py`, Home Assistant, MQTT of
de operationele snapshotketen aangeroepen.

## Exacte keten

`ground_fusion_shadow_runtime.py` composeert uitsluitend bestaande autoriteiten:

1. `CanonicalGroundEvidenceStore.query()` herverifieert iedere groundfact tegen
   de schema-3 raw-capture;
2. `canonical_ground_fusion_bridge` laat alleen een exact
   provider/product/decoder/config-semantiekcertificaat door;
3. `PhysicalSiteCatalog` levert op het CellTrack-tijdstip de immutable
   `PhysicalSiteRegistry`, catalogusfingerprint en aliasmappingfingerprints;
4. een echte `ImmutableCellTrack`-detectie wordt met
   `bind_cell_track_footprint()` exact aan één WGS84-`CellTrackFootprint`,
   science-receipt, grid, transform, radarcertificaat en lineage gebonden;
5. `ground_fusion.py` beoordeelt ieder toegelaten station tegen exact die
   celgeometrie en dat tijdinterval;
6. een append-only SQLite-ledger bewaart een content-gehashte, privacy-veilige
   shadowreceipt.

Er is geen zoekactie naar een generiek weersysteem en geen stem van een station
buiten de exacte cel. Elke beslissing is apart gebonden aan target, track,
detection, footprint en tijdvenster.

Voor de science-shadowketen bestaat daarnaast
`bind_science_shadow_footprint()`. Die accepteert uitsluitend een vers
`ADVANCED` resultaat met `duplicate=false` en nog aanwezige evidence- en
transitionarrays. Track en hiërarchische detectie worden rechtstreeks uit de
durable `next_state` gehaald. Een restart/duplicate zonder arrays, coasting
track, receipt-/transitionafwijking of provenance-/proofafwijking faalt
gesloten en kan dus geen footprintbinding fabriceren.

## Meteorologische fail-closed regels

- Netatmo blijft `withheld` zolang de upstreamvelden geen exact officieel
  veld-/unit-/intervalcontract hebben. `rain_live`, `rain_60min` en cumulatieve
  regen worden niet onderling verwisseld.
- METAR present weather en Synoptic intervalaccumulatie behouden hun
  `MeasurementSemantics`, afgeleide hoeveelheid en unit in iedere disposition.
- Een droge stem vereist naast het semantiekcertificaat exact één actieve
  `SensorCapability` op dezelfde fysieke site, provider, station en
  certificaatprofiel met `can_observe_absence=true`. Zonder dat bewijs wordt de
  fact `withheld`, niet droog.
- Site- en aliasgeldigheid moeten het volledige observatie-interval omvatten.
- Future, stale, unavailable, unknown en geografisch/tijdelijk niet-passende
  feiten blijven expliciet uitgesloten en worden nooit negatieve evidence.
- Aliassen op dezelfde fysieke site/independence group tellen maximaal éénmaal.
  Een nat/droog conflict in één group maakt het runtime-resultaat
  `indeterminate`.
- Een stale/future target, stale/future CellTrack, te groot footprintvenster of
  een target buiten de expliciete relevantieafstand stopt vóór de groundquery.

## Volledigheid en grenzen

De query vraagt `max_source_facts + 1` (hard maximaal 1000). Minder dan de
sentinel bewijst een volledige scan van het expliciet opgeslagen
`observed_from/through`-venster. Bij de sentinel wordt geen subset gefuseerd:
alle geretourneerde facts krijgen disposition `overflow`, de niet-verwerkte
ondergrens wordt opgeslagen en de uitkomst is `indeterminate`.

Facts, sites, aliases, encoded accountingbytes, receiptbytes, databasebytes,
receiptrijen, lookback, footprintduur, target-/celouderdom, afstand en één
absolute monotone deadline zijn begrensd. Iedere geretourneerde source index
krijgt exact één van:

- `used`;
- `duplicate`;
- `outside`;
- `stale`;
- `withheld`;
- `overflow`;
- `indeterminate`.

Er bestaan geen silent drops. Als zelfs de minimale volledige
overflowaccounting niet binnen de bytegrens past, faalt de operatie getypeerd
zonder weatherreceipt.

## Duurzame receipt en privacy

De receipt bevat hashes/identiteiten van targetsnapshot, science-run,
transition, CellTrack/detection, grid, radar-/transform-/geometriecertificaten,
footprint, registry, catalogus, mapping, dry capability, groundcertificaten en
ieder canonical fact/receipt. Ook trackrevisies, parent/child/event-lineage,
queryvenster en alle dispositions blijven aanwezig.

Niet opgeslagen worden targetcoördinaten, station-ID's, raw tekst, URLs,
headers, tokens of foutdetails. Diagnostiek leest en verifieert receipts
streaming en geeft alleen begrensde aggregaten terug.

Exacte retry, concurrerende retry en restart replay gebruiken de immutable
request-SHA en geven dezelfde receipt terug. Attempt- of requestconflicten,
schema-/payloadtamper, SQLite-capaciteit en deadlinefouten falen gesloten.

## Sealed retention en rotatie

`GroundFusionShadowReceiptStore` gebruikt schema 2 met één monotone
receiptsequence en een checksummed archive-anchor. Het retentionbeleid bindt
immutable:

- de archiefnamespace;
- het aantal nieuwste receipts dat hot moet blijven;
- rows en bytes per segment;
- maximumaantal segmenten en archived receipts;
- de totale sealed archive-bytegrens.

De nieuwste hot window, inclusief de laatste sequence/head, wordt nooit als
rotatiekandidaat geselecteerd. De oudste kandidaten moeten één contiguous
sequence vanaf de vorige archive-anchor vormen; een gap of fork stopt de
rotatie.

Elk sealed segment is canonical ASCII-JSON met de byte-exacte oorspronkelijke
receiptpayloads in base64, per-payload SHA-256, sequence, een content-SHA,
file-SHA, vorig segment-ID, runtimeprofiel en retentionpolicyfingerprint. Het
bestand wordt eerst exclusief als temp geschreven, geflusht, met `fsync()`
gesynchroniseerd, in de directory verankerd, atomisch met `os.replace()`
gepubliceerd en opnieuw via directory-fsync verankerd.

Pruning is bewust tweefasig:

1. het sealed bestand bestaat byte-exact en duurzaam;
2. segmentmanifest, archived lookupindex en anchor-CAS worden samen met
   `synchronous=FULL` gecommit en de databasedirectory krijgt een barrier;
3. pas in een nieuwe transactie wordt ieder exact overeenkomend hot receipt
   verwijderd; een SQLite-trigger weigert verwijderen zonder immutable
   archive-index;
4. er draait geen `VACUUM` in het hot pad.

Een crash na bestandspublicatie maar vóór manifestcommit laat de hot receipts
staan. De volgende identieke rotatie mag alleen het deterministisch exact
gelijke orphansegment hergebruiken. Een crash na manifestcommit maar vóór
prune laat een expliciete hot/archive-overlap; restart herverifieert file,
manifest, index en payloads en hervat daarna pas de prune. Partial files,
onverwachte bestanden, ontbrekende files, checksumdrift, manifesttamper,
chain-gaps, gedeeltelijke prune, disk-full en elke row-/bytecap falen gesloten.

Binnen het hot window blijft een retry exact dezelfde receipt teruggeven.
Buiten het window resulteert een normale retry expliciet in
`GroundFusionShadowWindowExpired(segment_id)`; hij wordt nooit stil als een
nieuwe beslissing uitgevoerd. Alleen `load_archived()` levert dan na volledige
segmentverificatie de exacte receipt. Diagnostiek bevat uitsluitend begrensde
aantallen, bytes, sequence en anchorhash, nooit archiefpaden of extra
identiteiten.

## Restore- en incidentrunbook

Een restore vereist als één set: de hot SQLite-database, alle gemanifesteerde
`.sealed.json`-segmenten, exact hetzelfde runtimeprofiel en exact dezelfde
`GroundFusionShadowRetentionPolicy`. Voer vóór lookup altijd
`audit_archive()` uit. Die controleert de volledige segmentketen, manifestset,
lookupindex, hot/archive-sequencegrens, payloads en anchor.

- Verwijder of hernoem een `.tmp` of onverwacht sealed bestand nooit
  automatisch. Stop writers, maak eerst een forensische kopie en bepaal of het
  om de deterministische volgende orphan gaat.
- Bij checksum-, gap-, fork- of manifestfout: geen prune, geen handmatige
  anchorupdate en geen weatherclaim. Herstel vanuit een bewezen complete
  database-plus-archiefkopie.
- Segment retirement of overschrijven bestaat niet. Wanneer totale
  archivecapaciteit bereikt is, stopt rotatie fail-closed; offsite export en
  een gecertificeerd lifecyclebeleid blijven extern.
- Volledige rehydratie van archived receipts terug naar een nieuwe hot ledger
  is bewust geen online functie. De huidige restoreboundary is geverifieerde
  archived lookup; een disaster-rebuildtool en operatorprocedure moeten apart
  worden toegelaten.
- POSIX-directory-fsync is vereist. Op Windows is er geen equivalente
  standaardbarrier en weigert de default rotatie; tests gebruiken uitsluitend
  een expliciet geïnjecteerde barrier. Een platformadapter mag pas na apart
  durabilitybewijs worden toegelaten.

## Harde process-isolatiecandidate

`ground_fusion_shadow_process_isolation.py` is een aparte, niet-live
parent-runner rond `DurableGroundFusionShadowRuntime`. Hij gebruikt uitsluitend
de bestaande `HardDecoderExecutor` met verplichte `spawn`-context. Op Windows
legt die parent een Job Object op met wall/CPU/process-memory en
kill-on-close; op Linux gebruikt hij een nieuwe process group, kernel rlimits,
parent-side `/proc`-accounting en SIGTERM/SIGKILL voor de volledige group. Een
platform zonder bewezen boundary levert expliciet `boundary`, nooit een
in-process fallback.

Iedere run is gebonden aan een byte-exacte canonical-JSON authority met SHA-256.
Die authority noemt absolute, reeds genormaliseerde paden en exact hun rollen:

- canonical-ground SQLite met een hash van immutable schema plus manifest;
- de transactionele ground-receiptstore;
- de sealed archivedirectory;
- alleen eventuele extra read-paden die expliciet in dezelfde authority staan.

De child herleest en verifieert die authority zelf. Een geauditeerd
factorycontract moet verklaren dat het alleen deze paden opent, raw facts via
de canonical raw-reverificationketen leest, geen descendants start en alleen
de transactionele receipt als durable effect heeft. Na constructie vergelijkt
de child runtimeprofiel, groundlimits, canonical-storepad, receiptstorepad en
de exact opnieuw berekende request-SHA. Een afwijking is
`corrupt_contract`.

IPC bevat geen groundfacts, canonical rows, stationlijsten of objectarrays.
Het command bevat alleen de begrensde target/cell/footprint/proofbinding,
authority-/factory-/runtimehashes en expliciete limits. Zowel command als
request hebben een SHA-256. Het childresultaat bevat alleen checksummed,
begrensde receiptmetadata: hashes, candidate-state, completeness/overflow en
dispositionaantallen. Paden, targetnamen/coördinaten, station-ID's, fouttekst,
tokens en secrets komen niet in parentdiagnostiek.

Na elk einde (`success`, deadline, CPU, RSS, output overflow, crash of cancel)
vertrouwt de parent IPC niet. Hij opent de exact geautoriseerde receiptstore
opnieuw en reconcilieert attempt plus request-SHA. Een archived retry wordt via
`load_archived()` byte/checksum/manifest-gecontroleerd. Daardoor geeft een kill
vóór commit `not_committed`; een kill ná de atomic commit kan nog steeds de
exacte receipt als `committed` terugvinden. Restart of concurrerende retry
levert daarna dezelfde receipt en voert de meteorologie niet opnieuw uit.

De huidige processboundary is nog steeds shadow-only. Open blockers zijn een
echte power-loss/kill-soak op het uiteindelijke Linux/HA-filesystem, bewijs voor
alle production factory/raw-artifactpaden, langdurige RSS/CPU-soak en de reeds
genoemde globale site-, semantiek-, footprint- en held-out meteorologische
admissiongates. De Windows spawn/Job Object-tests bewijzen procesgedrag, niet de
POSIX archive-fsync/power-lossduurzaamheid van het uiteindelijke systeem.

## Geen operationele claim

Ook een intern `confirmed` shadowresultaat houdt hard vast:

- `meteorological_admission_granted=false`;
- `operational_confirmation_claim=false`;
- `operational_snapshot_claim_generated=false`;
- admission status `shadow_candidate_unadmitted`.

Operationele wiring vereist nog echte held-out replay, gevalideerde globale
site/capabilitycatalogi, admissioncertificaten voor geografische
footprintafleiding, production-soak/admission van de nieuwe processcandidate,
een geverifieerd offsite retention-/disaster-restorepad en een aparte
eindvalidatie.

## Closed-world METAR factorycandidate

`metar_ground_shadow_factory.py` levert nu een production-like, maar nog
shadow-only factory voor de harde processrunner. Hij bindt precies acht
authoritypaden voor canonical DB, receipt DB, sealed archive, schema-3 raw
store plus volledig checksummed treemanifest, site-/capabilitycatalogus,
semantiekcertificaten en target/cell/footprintauthority. Een child-local audit
hook weigert andere paden, netwerk en procescreatie; raw en canonical content
worden na open opnieuw byte-exact gecontroleerd.

De lokale echte AWC-cache bouwde 10294 canonical facts. Het exacte
bewijsvenster leverde 56 volledig verantwoorde facts, alle `withheld`: 29 door
ontbrekende onafhankelijke fysieke-sitealias en 27 door niet-gecertificeerde
variabelen. De uitkomst bleef `indeterminate` en nooit operationeel bevestigd.
Details, exacte API en blockers staan in
`docs/METAR_GROUND_CLOSED_WORLD_PROOF.md`.
