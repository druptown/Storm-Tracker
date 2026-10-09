# Permanente quarantaine voor acquisition evidence

Status: lokaal, inert en fail-closed. Dit contract wijzigt geen provider,
consumer, service, Home Assistant- of MQTT-wiring.

## Doel

Een leased completion met permanent onbruikbare, eventgebonden evidence mag de
kop van een geordende stream niet eindeloos blokkeren. Zo'n completion is ook
geen succesvolle decode. `AcquisitionEvidenceLifecycleStore.quarantine_leased`
legt daarom een afzonderlijke `QuarantineReceipt` vast en gebruikt de bestaande
outboxstatus `consumed` uitsluitend als compatibele terminal transportstatus.
De semantische terminalsoort blijft expliciet `quarantined`.

De publieke return is een immutable, begrensde `QuarantineTerminalResult` met
zowel de `QuarantineReceipt` als de exact gekoppelde `ConsumptionReceipt`.
Daardoor kan een dispatcher event/producer/consumer/stream/sequence, beide
receiptchecksums en `consumption.output_sha256 == quarantine SHA` controleren
zonder database-internals te lezen. Er is geen circulaire hash: quarantine is
eerst zelfstandig gehasht; consumption bindt die hash als output.

## Gesloten permanent-foutcatalogus

Alleen deze codes zijn toegestaan:

- `completion_reference_integrity_failed`
- `source_raw_integrity_failed`
- `unsupported_completion_contract`
- `decoder_output_integrity_failed`
- `canonical_evidence_integrity_failed`

Cancellation, deadline/timeout, CPU- of geheugencap, resultaatcap, volle schijf
en fouten in een systeemstore hebben bewust geen quarantainecode. Ze blijven
retrybaar en mogen deze API niet gebruiken.

## Gebonden identiteit en privacy

De caller levert uitsluitend event-ID, lease capability, verwachte event-SHA,
een allowlisted reason en een 64-hex `evidence_fingerprint_sha256`. De receipt
bindt exact:

- event-ID en event-SHA;
- producer-receipt-SHA;
- consumer-ID;
- stream-ID en stream-sequence;
- evidence-fingerprint, reason en terminal timestamp.

De receipt bevat geen exceptiontekst, payload, URL, bestands- of databasepad.
Reasoncodes zijn vaste waarden; vrije fouttekst kan dus niet worden opgeslagen.
Diagnostics zijn alleen begrensde aggregaten en rapporteren onder meer
`terminal_kinds.quarantined` en `quarantine_receipts`.
Een integriteitsquery weigert cross-table overlap, een quarantine-child op een
niet-consumed event, een orphan child, een onjuiste outputbinding en een
consumed event zonder consumption-receipt. Een geldige leased receipt uit het
bestaande ack/rejection-crashvenster blijft zichtbaar als
`pending_terminal_receipts` en kan door reconciliatie worden afgerond.

## Atomische transactie

Voor een geldige, nog niet verlopen lease commit één `BEGIN IMMEDIATE`-transactie:

1. de immutable `quarantine_receipts`-child;
2. een gewone `ConsumptionReceipt` waarvan `output_sha256` exact de SHA van de
   quarantine-receipt is;
3. de outbox terminal als `consumed`, zonder resterende leasevelden;
4. de geordende consumer-cursor.

Er bestaat daardoor geen durable toestand waarin de cursor voorbij een event
staat zonder beide receipts, of waarin alleen een quarantine-child durable is.
De faultpoints `quarantine.before_receipt_commit`,
`quarantine.after_receipt_commit`, `quarantine.before_cursor_commit` en
`quarantine.after_cursor_commit` bestrijken alle logische crashvensters. De
eerste drie rollen de volledige transactie terug; de laatste representeert een
crash nadat alles durable is. Exact replay levert dezelfde
`QuarantineTerminalResult`; een andere reason, fingerprint of event-SHA is
conflict. Hot en archived replay leveren hetzelfde receiptpaar terug.

`claim_next` levert het event na cursorcommit niet opnieuw. De outbox CHECK hoeft
niet te worden herbouwd, want de compatibele persisted state blijft `consumed`.

## Success, rejection en quarantine zijn disjunct

Een consumed event heeft exact één van deze betekenissen:

- normaal: alleen de consumption-receipt;
- rejected: consumption plus rejection-receipt;
- quarantined: consumption plus quarantine-receipt.

Rejection en quarantine tegelijk is integriteitscorruptie. Zowel
`acknowledge` als `load_terminal_consumption` weigert een quarantined event
expliciet, hot én archived. Daardoor kan de quarantine-SHA nooit als normale
decoder-output worden hergebruikt. De bestaande desired-state rejectionroute
en normale succesroute behouden hun eigen receipts en checksums.

## Schema en sealed retention

Lifecycle-schema v3 voegt alleen de `quarantine_receipts`-childtable toe. Een
v1-store krijgt eerst de bestaande v2 rejection-child en daarna v3; een
v2-store krijgt alleen de nieuwe table. Producer-, event-, consumption- en
cursoridentiteiten worden niet herschreven.

Nieuwe sealed records gebruiken
`stv3-worker-acquisition-archived-completion-v3` en bevatten een exact
`terminal_kind` plus een optionele quarantine-child. Readers blijven v1-records
zonder terminal children en v2-records met optionele rejection-child valideren.
De v3-validator controleert child/event/producer/consumer/stream/sequence,
receiptchecksums en de consumption-outputbinding. Child-first prune verwijdert
quarantine, rejection en consumption vóór outbox en producer. Een archived
exact replay geeft hetzelfde terminale receiptpaar terug; chain-, segment-,
object- of receipt-tamper faalt gesloten.

## Verificatie

De gerichte tests bewijzen reason-, fingerprint-, event-, lease- en expirygates,
exact replay/conflict, twee-process concurrency, alle vier crashvensters,
cursoradvance zonder redelivery, hot tamper, v3 seal/prune/reopen/replay,
archive-tamper, v2→v3-migratie, v1/v2-readercompatibiliteit en begrensde
secretvrije diagnostics. De lifecycle blijft een evidence mirror: er wordt geen
source-object verwijderd, geen provider opnieuw opgehaald en geen live service
geactiveerd.
