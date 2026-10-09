# Shadow raw-capture admission

`artifact_capture_admission.py` is de kleine, fail-closed brug tussen:

- `provider_artifact_contracts.py`;
- `artifact_retention_planner.py`;
- de optionele single- of atomische multi-artifactaanroep naar
  `RawArtifactStore`.

De Home Assistant- en workermodule zijn byte-identieke mirrors. Er is geen live wiring en geen providertransport importeert deze module.

## Twee onafhankelijke uitkomsten

Iedere geldige request levert immutable, secretvrije
`CaptureDecision`-receiptmetadata. Die bevat provider, contractproduct,
revision-ID, nominale en ontvangen tijd, byteaantal, payload- of
artifactset-SHA-256, memberreceipts, providercontracthash,
contractregistryhash, retentieplanhash en decisionhash.

De twee uitkomsten mogen niet met elkaar worden verward:

1. `processing_allowed=True` is een harde invariant. Een echte
   weerobservatie mag verder worden verwerkt wanneer raw retention op `HOLD`,
   `ARCHIVE_REQUIRED`, `REJECT_CAPACITY` of een technische captureblokkade
   staat.
2. `durable_raw_write_authorized=True` ontstaat uitsluitend wanneer alle
   volgende feiten tegelijk waar zijn:
   - de bytes zijn volgens het providercontract exact upstream beschikbaar;
   - ze zijn aantoonbaar `BEFORE_PARSE` vastgelegd;
   - media type en artifactset zijn compleet;
   - de retentieplanner beslist `STORE`;
   - dezelfde revisionidentiteit is niet eerder met andere inhoud gebruikt.

`HOLD` of capaciteitsafwijzing is dus nooit een stil afkeuren van weerdata.

## Beschikbaarheidsklassen

- `UPSTREAM_EXACT_SINGLE`: één exact upstream body/object/archive.
- `UPSTREAM_EXACT_MULTI`: een volledige set exacte members.
- `DECODED_BODY_ONLY`: een reeds gedecodeerde body; geen raw artifact.
- `WORKER_DERIVED_ONLY`: een workerresultaat na decode/windowing; geen raw
  artifact.
- `UNKNOWN`: onbekende herkomst, altijd fail-closed voor raw writes.

Een bestaand kalibratieoverlay blijft expliciet
`provider_overlay_not_upstream_raw`. De laag noemt afgeleide JSON, overlays of
rasters nergens originele providerbytes.

## Ordering en secrets

De request draagt de echte bytes, maar de decision en het write-plan nooit.
Daar staan uitsluitend SHA-256, lengte, media type, memberrol en gehashte
store-identiteiten in. Volledige URL's, querystrings, fragments, signed URLs,
tokens, authorizationwaarden en gevoelige veldnamen worden door de bestaande
contractredactie geweigerd.

De laag parseert of transformeert niets. `AFTER_PARSE` en `UNKNOWN` ordering
houden raw storage dicht, maar laten verwerking door.

## Multi-artifactsets

Expected en ontvangen member-ID's worden vóór iedere write volledig
vergeleken. Ordering wordt gecanonicaliseerd; de artifactsethash is een hash
van het geordende membermanifest en wordt nooit voorgesteld als hash van
geconcateneerde upstream bytes.

De expected memberlijst is in deze pure laag een caller-aangeleverd feit. De
laag decodeert geen manifest om die lijst onafhankelijk af te leiden. Een live
transport moet daarom later bewijzen dat zijn expected lijst rechtstreeks uit
de volledige upstream catalogus/requestset komt.

`RawArtifactStore` schema v2 biedt naast de single-artifact `store()` een
`store_set()`-transactie. Die schrijft alle content-addressed blobs, immutable
memberreceipts, setmetadata en memberships binnen één `BEGIN IMMEDIATE` /
`COMMIT`-grens. De store sorteert members canoniek en bewaart zowel een eigen
manifest-SHA-256 als de admission-`decision_sha256` als provenancehash.

De executor controleert vóór de eerste write de decisionhash en bindt de
decision exact aan provider, receipt/revision, beide tijden, availability,
capture-order, expected memberlijst en iedere memberhash, lengte, rol en media
type. Hij controleert ook de actuele admission-/contractregistryhash,
providercontracthash, product- en capturestatus en leidt store-record-ID plus
parserversie opnieuw af. Daardoor kan een geldige `BEFORE_PARSE`-decision niet
worden hergebruikt met een vervangen `AFTER_PARSE`, anders geclassificeerde of
incomplete request. Voor een multi-set roept hij uitsluitend `store_set()` aan.
Een store zonder die capability wordt vóór een eventuele single `store()`-call
geweigerd. Na de transactie worden set-ID, provenancehash, manifesthash en alle
memberreceipts opnieuw gecontroleerd.

De store zelf herleest binnen dezelfde SQLite-transactie ook de single payload,
single receipt en de volledige set met alle memberpayloads vóór commit. Een
onverwachte trigger/view is geen toegelaten schemaobject; zelfs een trigger die
pas ná initialisatie out-of-band wordt toegevoegd en bytes tijdens de write
wijzigt, veroorzaakt rollback in plaats van een vals succesreceipt.

## Idempotentie en revisions

- Dezelfde store-identiteit en exact dezelfde bytes zijn idempotent in
  `RawArtifactStore`.
- Een nieuwe `revision_id` krijgt een afzonderlijke gehashte store-identiteit.
- Dezelfde receipt/revision met andere bytes wordt als
  `REVISION_CONFLICT` afgewezen voor raw storage.
- Een identieke revision die al in de caller-inventory staat levert
  `IDEMPOTENT_ALREADY_PRESENT` en veroorzaakt geen nieuwe write.

Bij een store-exception wordt geen vals executionreceipt gemaakt. Wanneer een
proces na de SQLite-commit maar vóór het teruggeven van de executionreceipt
crasht, levert een identieke retry dezelfde set en receipts terug. Foutinjecties
op zeven multi-setgrenzen bewijzen lokaal: nul nieuwe blobs/receipts/setrijen of
één volledig verifieerbare set, nooit een gedeeltelijke set.

## Open gaten

- Geen providertransport levert de request nog aan deze laag.
- Er is nog geen duurzame journal voor de admission-receiptmetadata.
- De capacitysnapshot en inventory zijn pure callerfeiten op plantijd. Voor
  live writes is nog een atomische, duurzame quotareservering nodig zodat twee
  gelijktijdige `STORE`-decisions niet samen de vrije ruimte of providerquota
  overschrijden.
- Schema v1 wordt bewust niet stil naar schema v2 gemigreerd; de store is nog
  shadow-only en weigert een oude of onbekende database bytebehoudend. Voor een
  toekomstige bestaande productie-installatie is eerst een afzonderlijke,
  checksum-behoudende offline migrator met rollbackbewijs nodig.
- Het huidige DPC-contract noemt catalogus plus GeoTIFF als set, terwijl de
  toegestane mediatypelijst de catalogus-JSON nog niet beschrijft; admission
  blijft daarvoor fail-closed tot het contract is verfijnd.
- Open contract-/licentiegaten blijven `HOLD`.
- Archiveren en vrijmaken na `ARCHIVE_REQUIRED` gebeurt niet in deze laag.
- `RawArtifactStore` is een vertrouwde interne primitive en herhaalt de
  secretredactie niet zelfstandig. Live gebruik moet uitsluitend via de
  admissionbrug verlopen of eerst een onafhankelijke store-side
  redactieverdediging krijgen.
- Er is geen deploy, configuratiewijziging of live raw capture uitgevoerd.
