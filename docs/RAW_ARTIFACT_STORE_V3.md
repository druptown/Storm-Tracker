# Canonieke worker RawArtifactStore v3

Status: **worker-only fundering, lokaal getest, nog niet live geactiveerd**  
Schema: `RAW_ARTIFACT_SCHEMA_VERSION = 3`  
Implementatie: `storm_tracker_worker/raw_artifacts.py`

## Besluit over de raw waarheid

Deze store is de enige beoogde operationele authority voor exact ontvangen
upstreambytes. De volgende oudere opslaglagen zijn uitsluitend legacy/shadow:

- `storm_tracker_worker/tracking_core/engine/raw_artifact_store.py` (schema 2);
- de byte-identieke HA-mirror van die schema-2-store;
- de eigen `meteorological_raw_*`-tabellen van de geïsoleerde
  `MeteorologicalEvidenceStore`.

Nieuwe provider-, radarcyclus- of forecastwiring mag die oudere lagen niet als
tweede operationele raw waarheid gebruiken. De derived-evidenceketen moet nog
naar cryptografisch gebonden verwijzingen naar deze v3-store migreren voordat
ze operationeel kan worden geactiveerd.

## Opslagmodel

De exacte response-bodybytes worden vóór decode opgeslagen als immutable
content-addressed objecten onder een worker-eigen root. SQLite bewaart alleen
de crash-durable catalogus en receipts. De store heeft:

- een harde grens per object en voor de totale objectinhoud;
- harde aantalsgrenzen voor objecten, stagingfiles, receipts, captures en
  capturemembers;
- een catalogusbestandsenvelop met een conservatieve rollback-journalreserve;
- een atomisch gepubliceerd, immutable storeprofiel dat alle limieten bindt,
  zodat twee processen dezelfde root nooit met verschillende grenzen openen;
- canonieke, secretvrije provenance met de verplichte claims
  `upstream_native_bytes` en `before_decode`;
- SHA-256 over content, provenance en de volledige immutable receipt;
- stable `raw-<sha256>`-receipt-ID's op provider/product/fetch-identiteit;
- stable `capture-<sha256>`-ID's voor complete rolegebonden captures;
- geen algemene delete-, truncate-, eviction- of synthetische fallback-API;
  alleen de expliciet geactiveerde, gesloten source-reclamationparticipant uit
  [RAW_ARTIFACT_RECLAMATION.md](RAW_ARTIFACT_RECLAMATION.md) kan onder een
  fysiek gehouden coordinatorbesluit exact bewezen bronobjecten verwijderen.

Capaciteit, corruptie, een onveilige provenanceclaim of hergebruik van een
fetch-ID voor andere feiten faalt gesloten. Zo'n fout is nooit bewijs van een
droge atmosfeer.

De standaardgrenzen zijn 128 MiB per native object, 8 GiB totale
object-/staginginhoud, 65.536 objectentries, 64 stagingentries, 262.144
receipts, 262.144 captures, 64 leden per capture en een
catalogusenvelop van 1 GiB. Een andere configuratie vereist een nieuwe,
expliciet gemigreerde store-root; ze kan niet stil op een bestaande root worden
toegepast.

Providers gebruiken afzonderlijke roots en mogen daardoor een aantoonbaar
productspecifiek profiel hebben. OPERA bindt bijvoorbeeld 384 MiB aan zijn
eigen root voor een volledig composiet. Binnen één root blijft profielmismatch
altijd een fout; roots van verschillende providers worden niet samengevoegd.

## Complete capture

`commit_capture()` publiceert pas nadat alle vereiste memberreceipts bestaan en
byte-for-byte zijn geverifieerd. Een partiële fetch mag individuele immutable
receipts nalaten, maar verschijnt nooit als complete capture.

De payloadvrije `RawArtifactCaptureReceipt` bindt:

- echte `capture_id`, provider, product en fetch-ID;
- secretvrije upstream-capture-identiteit;
- producttijd los van iedere ontvangsttijd;
- het canonieke capturemanifest;
- voor ieder uniek role/member: receipt-ID, content-SHA-256,
  receipt-SHA-256 en ontvangsttijd;
- één `capture_receipt_sha256` over al deze complete feiten.

`load_capture_receipt()` berekent en verifieert die view uit de bestaande
schema-3-rijen. Hiervoor was geen DDL- of datarewrite nodig.

## Begrensde verificatiebelasting

De integriteitscontrole blijft volledig, maar dezelfde immutable blob wordt
niet meer onmiddellijk tweemaal gehasht voor één logische read:

- `load_receipt_bytes()` retourneert receipt plus bytes, of `None` bij een
  ontbrekende identity, na exact één catalogusvalidatie en één volledige
  objectread/SHA-256-pass;
- `read_receipt_bytes()` biedt dezelfde ene pass maar werpt bij ontbreken;
- `read_bytes()` delegeert aan die gecombineerde primitive;
- `load_capture_bundle()` retourneert capture plus alle memberreceipts met
  exact één objectverificatie per lid;
- `load_capture_bundle_bytes()` retourneert in dezelfde ene pass ook de exacte
  memberbytes voor een begrensde decoder of complete-retryroute;
- een nieuwe `store()` of `commit_capture()` herleest na de reeds
  transactionele verificatie niet nogmaals dezelfde objecten alleen om het
  zojuist gecommitte receipt terug te geven. Na een verloren return verifieert
  de normale retry de authority uiteraard opnieuw.

Hierdoor blijft corruptiedetectie op iedere zelfstandige load bestaan, terwijl
grote radarbestanden geen dubbele of drievoudige SHA-/I/O-belasting per API-call
meer veroorzaken. Focused tests tellen de werkelijke objectreads per receipt en
capturemember. Dit is geen CPU-, RSS- of thermische productieclaim; die blijft
een aparte NUC-soakgate.

De snelle directoryfingerprintcache is uitsluitend telemetrie. Ze is nooit
toelatingsbewijs: iedere `store()` voert onder de geserialiseerde
cataloguswritertransactie opnieuw een volledige object-/staging-layout- en
bestandslengtescan
uit, ook bij een exacte retry. Daardoor kunnen in-place groei, teruggezette
directorytimestamps, onbekende entries of een reeds overschreden grens geen
nieuwe receipt autoriseren. Deze correcte maar lineaire scan blijft een
expliciete optimalisatiegate; een toekomstige sharded/quota-opslag mag hem pas
vervangen wanneer dezelfde grensgaranties adversarieel bewezen zijn.

SQLite draait bewust in `journal_mode=DELETE`, niet in WAL. WAL kon met een
langlevende reader fysiek boven `max_catalog_bytes` doorgroeien. De main
database krijgt daarom hoogstens een kwart van het catalogusbestandsenvelop; de rest is
gereserveerd voor het rollbackjournal en framing. Een reader blokkeert zo een
writer in plaats van een onbeperkte log te laten groeien. `database or disk is
full` wordt als een expliciete cataloguscapaciteitsfout genormaliseerd. Een
echte pinned-readertest bewijst dat de writer in DELETE-modus technisch faalt
zonder een `-wal` te creëren of een tweede receipt zichtbaar te maken.

## Operationele admission

`stv3-radar-cycle-request-v2` accepteert uitsluitend een complete schema-3
capture-reference. `RadarCycleStore` leest dezelfde `capture-*` receipt opnieuw
uit deze store en vergelijkt alle feiten exact vóór admission. De oude
schema-2 `RawArtifactSetReceipt` en `load_set_receipt()` zijn geen operationele
dependency meer.

Een capture bewijst alleen herkomst en volledigheid. Ze bewijst niet dat een
decoder, eenheid, projectie, kwaliteitslaag of meteorologische interpretatie
correct is. Daarom blijft KMI appv4 ondanks complete JSON+PNG-capture
`visualisation_only` en niet-operationeel. Iedere kwantitatieve provider krijgt
afzonderlijk een adaptercertificaat en replaygate.

## Transport- en privacygrens

Objectpaden en payloadbytes blijven in de worker. MQTT, operationele snapshots,
diagnostiek en Home Assistant ontvangen uitsluitend begrensde identifiers,
hashes, bron-/producttijden en statussen. Credentials, Authorizationheaders en
credentialdragende URL's horen in geen enkele raw receipt of captureprovenance.

## Nog vereist vóór live gebruik

1. Exact-bytecapture plus providercertificaat voor iedere productiebron.
2. Derived evidence verwijst cryptografisch naar v3 in plaats van eigen raw
   authority te spelen.
3. Decode, unitnormalisatie en gridreprojectie gebruiken uitsluitend opnieuw
   geverifieerde capturebytes.
4. Disk-full, corruptie, restart en echte power-lossmatrix op de NUC.
5. Retentie/archivering pas na bewezen committed lineage; nooit stil verlies.
6. Shadow/canary en HA/Z2M-latentiesoak vóór enige live cutover.

De huidige tests bewijzen point-in-time byte-integriteit en coöperatieve
procesveiligheid. Ze bewijzen nog geen hardwarematige onveranderlijkheid tegen
een geprivilegieerd proces dat dezelfde files rechtstreeks herschrijft, geen
werkelijke power-cut op ieder doelbestandssysteem en geen fysieke diskquota
buiten de beheerde object-, staging- en cataloguspaden. Die threat-model- en
filesystemmatrix blijft onderdeel van gate 4.
