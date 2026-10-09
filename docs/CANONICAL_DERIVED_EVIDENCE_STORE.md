# Canonieke derived-evidence provenance

## Status en harde grens

`storm_tracker_worker/canonical_evidence_store.py` is een nieuwe, geïsoleerde
provenancefundering. Ze voert geen decode uit, bepaalt geen regen of droogte en
is niet aan de operationele worker-service, scheduler, providers of Home
Assistant gekoppeld. Geïsoleerde shadowdecoders mogen er structurele receipts
mee bouwen, maar de store kan geen operationele of meteorologische
admissionclaim leveren.

De schema-3 `RawArtifactStore` blijft de enige authority voor upstreamdata. De
nieuwe store:

- roept uitsluitend de structurele methode
  `load_capture_receipt(capture-...)` aan;
- bewaart geen raw payload, blob of bestandspad;
- wijzigt of kopieert geen bestaande raw- of `MeteorologicalEvidenceStore`-
  data;
- bewaart alleen begrensde canonieke resultaatmetadata en hashes in een eigen
  SQLite-bestand.

## Intakecontract v2

Contract: `stv3-worker-canonical-derived-evidence-v2`.

Voor een decoder begint, moet de producer twee begrensde identiteiten duurzaam
toewijzen:

- `decoder_job_id`: exact `decoder-job-<64 lowercase hex>`;
- `attempt_id`: exact `decoder-attempt-<64 lowercase hex>`.

Deze waarden mogen niet uit het decoderresultaat, de evaluatietijd of de
outputhash worden afgeleid. Een decoder-run noemt daarnaast expliciet:

- de echte `capture-*`-identiteit, `capture_receipt_sha256` en ieder exact
  capturelid met `member_id`, `role`, `receipt_id`, `content_sha256`,
  `receipt_sha256` en `received_at`;
- decoderidentiteit en -versie plus `config_sha256`;
- provider, product, producttijd, maximale ontvangtijd en evaluatietijd;
- outcome en een outcome-gebonden reason code;
- bij `decoded_complete`: quantity, unit, native CRS, griddefinitie,
  native coverage en een opaque `artifact:`-kandidaathandle met SHA-256.

Onbekende velden worden geweigerd. Een kandidaathandle kan geen Windows-,
POSIX- of URL-pad zijn. De opgegeven captureleden moeten exact gelijk zijn aan
alle leden van de opnieuw geverifieerde schema-3 capture. Producttijd,
provider, product en ontvangtijd moeten eveneens exact overeenkomen.

## Structurele kandidaat is geen meteorologische waarheid

`decoded_complete` betekent uitsluitend dat alle gedeclareerde velden
structureel aanwezig en intern consistent zijn. De receipt biedt daarom alleen:

- `is_structurally_complete_candidate`;
- `non_operational_candidate_artifact_handle`.

Er bestaat bewust geen `is_meteorological`, geen
`downstream_artifact_handle` en geen operationele admissionstatus. Ook een
syntactisch geldige outputhash bewijst niet dat het artifact bestaat, correct is
gedecodeerd of meteorologisch bruikbaar is.

De outcomes `decode_error`, `unsupported` en `incomplete`:

- zijn nooit een structureel complete kandidaat;
- mogen geen quantity, unit, CRS, grid, coverage of outputhandle bevatten;
- leveren nooit impliciet `dry` of een ander neerslagbesluit.

Een toekomstige, afzonderlijke admissiongate moet minstens:

1. decoderidentiteit, versie, product en configuratie tegen een beheerde
   certificeringsregistry valideren;
2. het echte derived artifact uit een gezaghebbende artifactstore laden en de
   bytes tegen de gedeclareerde SHA-256 controleren;
3. quantity, unit, CRS, grid, coverage, producttijd, nodata en fysieke domeinen
   productspecifiek valideren;
4. kwaliteits- en onzekerheidsregels toepassen zonder ontbrekende data als
   droogte te interpreteren;
5. een aparte immutable admissionreceipt schrijven.

Pas die aparte receipt mag later een operationele consumer autoriseren. Deze
store alleen kan dat nooit.

## Idempotentie en resultaatidentiteit

Resultaat en retry-identiteit zijn bewust gescheiden:

- `result_sha256` is de schema-gebonden SHA-256 van alle resultaatfeiten,
  inclusief evaluatietijd en kandidaatoutput maar exclusief de vooraf
  toegewezen job- en attempt-ID;
- `run_id` bindt `decoder_job_id` en `attempt_id` aan die afzonderlijke
  resultaathash;
- `commit_id` wordt uitsluitend afgeleid van feiten die vóór de decode vast
  moeten staan: `decoder_job_id`, `attempt_id`, capture-ID en
  capture-receipthash, decoderidentiteit en -versie en `config_sha256`;
- `(decoder_job_id, attempt_id)` is daarnaast uniek in de store.

Een exacte retry retourneert dezelfde oorspronkelijke receipt zonder een
tweede rij. Een verloren resultaat opnieuw aanbieden met dezelfde job/attempt
maar een andere evaluatietijd, outcome, outputhandle of outputhash is een hard
conflict. Een bewuste nieuwe poging vereist vooraf een nieuwe `attempt_id` en
kan vervolgens een nieuwe revision vormen.

Runs met dezelfde capture/decoder/config vormen een append-only revision chain
met `previous_receipt_sha256`, `revision_sha256` en `receipt_sha256`. De
receipthash bindt tevens de schema-3 capture-receipt en manifest-SHA. De
capture-receipthash bindt in de raw authority ook fetch- en upstream-ID, zonder
die mogelijk pad- of URL-achtige waarden hierheen te kopiëren.

## Crash-atomisch schema en exacte admission

Storeschema 2 wordt met afzonderlijke `execute()`-statements volledig binnen
één expliciete `BEGIN IMMEDIATE`-transactie opgebouwd. Er wordt geen
`executescript()` gebruikt, omdat die in Python een open transactie impliciet
kan committen. Tabellen, indexen, manifestwaarden en append-only triggers zijn
daardoor samen oud/leeg of volledig nieuw na een procescrash.

De volledige genormaliseerde DDL van alle toegestane user tables, indexes en
triggers vormt één vaste `sqlite_schema`-allowlist en SHA-256-fingerprint. De
lege view-allowlist betekent dat iedere toegevoegde view eveneens faalt. De
fingerprint staat ook in het manifest. Een gelijknamige maar verzwakte trigger,
een ontbrekende index, extra object, gewijzigde constraint, kolom of DDL wordt
fail-closed geweigerd.

Iedere `commit()` opent eerst `BEGIN IMMEDIATE` en valideert binnen diezelfde
write-transactie opnieuw de exacte allowlist, DDL-fingerprint en het manifest,
vóór er een rij kan worden toegevoegd. Initialisatie voert daarna nog
`integrity_check` en `foreign_key_check` uit.

SQLite gebruikt `journal_mode=WAL`, `synchronous=FULL`, foreign keys, een harde
page-/databasegrens en append-only update/delete-triggers. De testmatrix bevat
echte `os._exit`-crashes na meerdere table/index/manifest/triggergrenzen, vóór
en na de schema-commit, commits vóór/na hun commitgrens en gelijktijdige
first-initialize-processen.

## Herverificatie en belasting

De raw capture wordt bij iedere eerste commit, gewone load en audit opnieuw via
de geïnjecteerde authority geverifieerd. Een audit voert daarnaast de volledige
SQLite-integriteitscontrole uit. Gewone loads controleren gericht schema,
receipt, hashes, revision chain en de betrokken raw capture, maar scannen niet
bij iedere call de volledige derived database.

Deze code is shadow-only en heeft geen service- of schedulerwiring. Ze mag later
alleen worker-side en buiten Home Assistant Core operationeel worden gekoppeld;
zware audits horen niet in een HA-, sensor- of snapshot-hot-path.

## Migratie-impact

Dit is storeschema 2 en contract v2 in een nieuw, nog niet operationeel
SQLite-bestand. Experimentele schema-1-bestanden worden bewust geweigerd; er is
geen impliciete in-place migratie die oude, resultaat-afhankelijke commit-ID's
als veilig kan behandelen.

Omdat deze fundering nog nergens live is gekoppeld, is er geen productie- of
HA-datamigratie nodig. Als een experimenteel v1-bestand toch bewaard moet
blijven, blijft het read-only als auditbron en wordt een afzonderlijk leeg
v2-bestand aangemaakt. Bestaande schema-3 raw data en de oude
`MeteorologicalEvidenceStore` worden niet gewijzigd, geïmporteerd of
geraadpleegd.
