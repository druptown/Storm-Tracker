# RawArtifactStore v2 — atomische exacte-byteopslag

> **Legacy/shadow:** dit document beschrijft de oudere schema-2-store en is
> geen operationele authority. De worker-only canonieke richting staat in
> [`RAW_ARTIFACT_STORE_V3.md`](RAW_ARTIFACT_STORE_V3.md). Nieuwe wiring mag
> schema 2 niet als tweede raw waarheid gebruiken.

Status: **lokale shadowprimitive, niet live gekoppeld**  
Schema: `RAW_ARTIFACT_SCHEMA_VERSION = 2`  
Setmanifest: `raw-artifact-set-manifest-v1`

De Home Assistant- en workermodule zijn byte-identieke mirrors:

- `custom_components/storm_tracker_v3/engine/raw_artifact_store.py`;
- `storm_tracker_worker/tracking_core/engine/raw_artifact_store.py`.

Geen providertransport of operationele worker-/HA-runtime importeert deze
store. Er is dus geen live fetch, databasewijziging, deploy of herstart uit deze
implementatie voortgekomen.

## 1. Bewaarde feiten

`raw_artifact_blobs` bewaart de exacte upstream bytes content-addressed:

- SHA-256 van de bytes als primaire sleutel;
- de bytes als SQLite-BLOB;
- exacte lengte;
- lokale creatietijd.

`raw_artifact_receipts` bindt één stabiele providerrecordidentiteit immutable
aan:

- provider en secretvrije upstream record-ID;
- blob-SHA-256;
- canonieke receipt-SHA-256 over identiteit, payloadchecksum, ontvangst- en
  nominale tijd, media type en parserversie;
- ontvangsttijd en afzonderlijke nominale brontijd;
- media type;
- parserversie;
- lokale creatietijd.

`raw_artifact_sets` bindt één stabiele providersetidentiteit immutable aan:

- een store-eigen canonieke membermanifest-SHA-256;
- een caller-gecontroleerde provenance-SHA-256, in de admissionbrug de
  `decision_sha256`;
- een setreceipt-SHA-256 over setidentiteit, manifest, provenance,
  samenvatting en tijden;
- memberaantal en totale bytegrootte;
- ontvangsttijd en nominale brontijd.

`raw_artifact_set_members` bewaart per set:

- canonieke `member_id`;
- meteorologische/transportrol;
- het exacte immutable memberreceipt.

De setmanifesthash omvat voor ieder, op `member_id` gesorteerd lid de rol,
secretvrije upstream record-ID, payload-SHA-256, lengte, media type en
parserversie. Hij is nadrukkelijk geen hash van aan elkaar geplakte payloads.

## 2. Atomische transacties

`store()` schrijft één blob en receipt in één `BEGIN IMMEDIATE`-transactie. Vlak
vóór commit leest hij receipt én payload opnieuw uit dezelfde transactie en
verifieert lengte, payload-SHA-256, deterministische receipt-ID en
receiptmetadatahash.

`store_set()` valideert eerst buiten de transactie de volledige callerinput:

1. provider-, set-, member- en recordidentiteiten;
2. exacte `bytes`-typen;
3. unieke member- en upstream record-ID's;
4. eindige ontvangst- en nominale tijden;
5. lowercase provenance-SHA-256;
6. canonieke memberordering en manifesthash.

Daarna schrijft één SQLite-transactie:

1. alle ontbrekende content-addressed blobs;
2. alle ontbrekende immutable memberreceipts;
3. precies één setreceipt;
4. alle memberships;
5. een herlezen en volledig tegen de callerinput geverifieerde setreceipt,
   inclusief iedere memberpayload en receipt;
6. de commit.

Een fout of procescrash vóór de commit rolt de hele set terug. Een crash na de
commit kan de callerreceipt verliezen, maar niet de set; een identieke retry
leest dezelfde immutable set terug.

De tests doden een apart proces abrupt op:

- na `BEGIN IMMEDIATE`;
- na het eerste memberreceipt;
- na alle memberreceipts;
- na de setrij;
- na alle memberships;
- direct vóór commit;
- direct na commit.

Na heropenen zijn de tellingen telkens exact `0/0/0` of `2/2/1` voor
blobs/receipts/sets en is de volledige payloadset bytegelijk replaybaar. Dit is
bewijs van de SQLite-transactiegrens; het simuleert niet afzonderlijk een
defect opslagmedium, kernelverlies van een bevestigde `fsync` of echte
`ENOSPC`.

## 3. Idempotentie en conflicten

Stabiele IDs zijn hashes van:

- single receipt: `provider_id + NUL + upstream_record_id`;
- setreceipt: `provider_id + NUL + upstream_set_id`.

Dezelfde identiteit, bytes en immutable provenance leveren het oorspronkelijke
receipt terug. Een andere ontvangsttijd bij identieke feiten maakt geen tweede
receipt. De volgende wijzigingen onder dezelfde identiteit falen gesloten:

- andere payloadbytes of payloadlengte;
- andere nominale tijd;
- ander media type of parserversie;
- andere memberrol, membership of record-ID;
- andere setprovenancehash.

Content-identieke bytes mogen wel door verschillende records of sets worden
gedeeld: er blijft één blob met meerdere immutable receipts.

`BEGIN IMMEDIATE` serialiseert concurrerende schrijvers. De gerichte
concurrentietests leveren voor zes gelijktijdige identieke setwrites exact één
set, twee receipts en twee blobs.

Ook de eerste schema-initialisatie is concurrency-safe gemaakt. Gelijktijdige
instanties kunnen vóór de commit allebei nog een leeg schema waarnemen. Alleen
de beperkte SQLite-races `database is locked` en `already exists` worden daarom
kort opnieuw geprobeerd; iedere poging leest en valideert daarna opnieuw het
volledige schema. Barrièregestuurde tests starten acht threads en zes
afzonderlijke processen tegelijk op hetzelfde nieuwe pad. Iedere initializer
eindigt met hetzelfde lege, gezonde schema v2; een onbekend of gedeeltelijk
schema valt niet onder de retry-admission en blijft fail-closed.

## 4. Replay en integriteit

`load_payload()` verifieert vóór teruggeven zowel lengte als SHA-256.

`load_set_receipt()` verifieert:

- alle foreign-keyjoins;
- provider en nominale tijd van ieder memberreceipt;
- iedere payloadlengte en SHA-256;
- memberaantal en totale bytegrootte;
- canonieke manifest-SHA-256;
- deterministische set-ID;
- het formaat van de provenance-SHA-256.

`load_set_payloads()` geeft alleen na die volledige setcontrole alle members in
canonieke volgorde terug.

`integrity_check()` rapporteert afzonderlijk:

- SQLite `integrity_check`;
- foreign-keyovertredingen;
- ontbrekende of onverwachte niet-interne SQLite-schemaobjecten;
- ontbrekende blobs;
- inhoudelijke blob-SHA-/lengtemismatches;
- setmanifest-/membership-/samenvattingsmismatches;
- receiptmetadata-/provenancechecksummismatches;
- een receipt-ID die niet exact overeenkomt met de deterministische hash van
  `provider_id + NUL + upstream_record_id`.

`healthy` is alleen waar als iedere categorie nul fouten bevat.

## 5. Schema-admission en fail-closed startup

Een bestaande database wordt eerst zonder WAL-configuratie gelezen en
structureel plus semantisch gevalideerd. Pas een volledig herkend en gezond
schema krijgt de duurzame schrijfpragmas:

- `journal_mode=WAL`;
- `synchronous=FULL`;
- `foreign_keys=ON`;
- `busy_timeout=20000`.

De validator eist exact de vijf tabellen en vijf benoemde indexen als volledige
allowlist van alle niet-interne `sqlite_master`-objecten: views en triggers zijn
dus niet toegestaan. Hij controleert daarnaast unique keys, foreign keys,
metadata-identiteit, schemaversie en de canonieke DDL van iedere tabel en
benoemde index. Daardoor vallen ook afwijkende SQLite-types, `NOT NULL`,
`CHECK`, `WITHOUT ROWID`, indexvorm of een extra partial-indexpredicate
fail-closed, zelfs als alle namen en kolommen nog overeenkomen. Extra,
ontbrekende, oude, toekomstige of corrupte schema's worden niet geadopteerd of
gerepareerd. Een test controleert byte-voor-byte dat een willekeurige bestaande
database na afwijzing dezelfde SHA-256 houdt.

Een gerichte red-teamtest plaatst na een reeds geslaagde initialisatie alsnog
een trigger die bij receipt-insert de blobbytes verandert. Zowel `store()` als
`store_set()` detecteren die wijziging vóór commit en rollen alle nieuwe
blobs/receipts/setrijen terug. `integrity_check()` rapporteert de trigger
tegelijk als schemaobjectmismatch; na heropenen weigert schema-admission de
database.

Schema v1 wordt bewust niet inline gemigreerd. De store is nog shadow-only. Als
er later een bestaande productie-store zou zijn, vereist v1 naar v2 een
afzonderlijke offline migrator met bron- en doelchecksums, restoretest en
rollbackbewijs.

## 6. Admissionbrug

De shadowbrug voert een multi-artifactbeslissing uitsluitend uit via
`store_set()`. Voor de eerste write controleert ze decisionintegriteit en bindt
ze de uitvoering exact aan provider, receipt/revision, beide tijden,
availability, capture-order, expected memberlijst en iedere
memberhash/lengte/rol/media type. Ze eist de actuele admission- en
providercontractversie en leidt iedere store-record-ID en parserversie opnieuw
uit het actuele contract af. Een oude, vervangen of handmatig vervalste
decision kan daardoor niet met een `AFTER_PARSE`, incomplete of anders
geclassificeerde request worden uitgevoerd. Een store zonder atomische
setcapability wordt geweigerd zonder een eventuele single-artifact `store()` aan
te roepen.

Na `store_set()` vergelijkt ze provider, stabiele setidentiteit,
provenancehash, memberaantal, bytegrootte en alle memberreceipts. De
`CaptureExecutionReceipt` bevat daarna zowel de store-set-ID en
store-manifest-SHA-256 als alle payloadreceipts en payloadchecksums.

## 7. Resterende live gates

De lokale primitive sluit nog geen productiegereedheid in. Voor live gebruik
blijven minimaal vereist:

- provider-per-provider licentie-/retentietoestemming;
- bounded capture vóór parse/decompressie in elk transport;
- onafhankelijke afleiding van de volledige expected memberlijst;
- duurzame admissionjournal en koppeling naar de meteorologische evidenceketen;
- een atomische quota-/capaciteitsreservering tussen admission en commit; een
  caller-capacitysnapshot alleen sluit concurrerende TOCTOU-overschrijding niet;
- quota-, archiverings-, restore- en gecontroleerde delete-executor;
- store-side secretredactie als tweede verdedigingslaag; momenteel levert
  uitsluitend de admissionbrug gehashte, secretvrije store-identiteiten en
  blijft direct storegebruik een vertrouwde interne API;
- echte `ENOSPC`, I/O-, WAL/SHM-, stroomverlies- en mediumfaultproeven;
- NUC-soak, disk-growthobservatie, recoverydiagnostiek en rollbacktest;
- expliciete deploytoestemming.

Tot die gates gesloten zijn, blijft deze implementatie shadow-only.
