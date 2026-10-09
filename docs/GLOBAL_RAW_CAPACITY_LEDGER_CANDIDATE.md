# Globale raw-capacity reservation ledger — shadowkandidaat v1

## Status en doel

`storm_tracker_worker/raw_capacity_ledger.py` is een volledig geïsoleerde,
lokale kandidaat voor toekomstige O(1)-admission over alle providerroots. De
module is niet gekoppeld aan `RawArtifactStore`, service, providers, Home
Assistant, MQTT, configuratie of deployment. `operational_admission_eligible`
blijft hard `false`.

De ledger lost één afgebakend probleem op: concurrerende schrijvers kunnen
vooraf logisch globale bytes en entries reserveren zonder daarvoor bij iedere
admission alle providerdirectories te scannen. De ledger schrijft zelf geen
raw bestand en beweert niet dat een filesystemobject bestaat.

## Immutable capaciteitsprofiel

`RawCapacityProfile` is volledig immutable en bindt in canonical JSON plus
SHA-256:

- globale byte- en entrylimiet;
- één gesorteerde, unieke lijst van maximaal 64 providers;
- per provider een byte- en entrylimiet die nooit groter is dan globaal;
- maximaal aantal reservationrows;
- totale SQLite main+DELETE-journal-envelope;
- begrensde SQLite busy timeout;
- `journal_mode=DELETE`, een main-databasereserve van maximaal een kwart van
  de fysieke envelope en het expliciete directory-fsyncbeleid;
- `automatic_reclaim=false` en admission false.

De eerste initialisatie schrijft dit profiel append-only in het manifest en in
de vaste provider-counterrows. Iedere latere instantie moet exact dezelfde
profilebytes aanbieden; zelfs een lokaal strengere limiet op dezelfde database
is drift en faalt vóór gebruik.

## Reservatie vóór de write

`reserve()` vereist een door de caller beheerde unieke `reservation_id` én een
provider-scoped `job_identity`, plus positieve requested bytes en entries. De
request krijgt een eigen SHA-256 en het durable RESERVED-receipt bindt die hash
met `created_at`.

Binnen één cross-process `BEGIN IMMEDIATE`-transactie gebeurt:

1. exacte retry- of jobconflictcontrole;
2. O(1)-controle van globale en providercounters;
3. insert van de immutable reservationrow;
4. atomische increment van globale en providercounters;
5. invariantcontrole en `synchronous=FULL` commit.

Een exacte retry retourneert hetzelfde receipt en telt nooit dubbel. Hergebruik
van dezelfde reservation of job met andere provider, omvang of entries is een
conflict. Een crash vóór SQLite-commit rolt row én counters samen terug. Een
crash erna laat een volledige RESERVED-row achter; die capaciteit blijft
onbeperkt meetellen en wordt nooit door tijd, restart of heuristiek vrijgegeven.

## Commit met exact bewijs

`commit()` bestaat alleen voor een reeds durable reservation en bindt:

- positieve werkelijke bytes en entries;
- exacte content-, raw-receipt- en capture-SHA-256;
- reservation-SHA, commit-tijd en een onafhankelijke commit-SHA.

Bij de overgang `RESERVED -> COMMITTED` worden reserved counters atomair
vervangen door actual committed counters. Als actual kleiner is, verdwijnt
uitsluitend de nog niet gebruikte reservatiemarge tijdens diezelfde bewezen
overgang. Als bytes of entries groter zijn, wordt de positieve delta binnen
dezelfde writertransactie opnieuw tegen globale én providercap gecontroleerd.
Bij onvoldoende capaciteit blijven row en counters exact RESERVED.

Een committed retry moet alle actual waarden en drie hashes exact herhalen.
Een COMMITTED-row kan niet terug naar RESERVED, gewijzigd of verwijderd worden.
Er bestaat bewust geen release-, delete-, expire-, abandon- of automatische
reconcile-API. Ook `audit()` reclaimt niets.

## Duurzame O(1)-counters en audit

Een singleton global-counterrow en één vaste row per provider bevatten:

- reservationrows;
- RESERVED- en COMMITTED-aantallen;
- reserved/committed bytes;
- reserved/committed entries.

`stats()` en `provider_stats()` lezen alleen deze begrensde rows en scannen de
reservationhistoriek niet. Iedere hot-path-mutatie verifieert niet-negativiteit,
countrelaties, providerlimieten, globale limieten en de som van maximaal 64
providerrows naar global.

`audit()` is uitsluitend read-only. Het voert schema-/manifest-/profielcheck,
`quick_check`, foreign-keycheck, hashvalidatie van iedere begrensde row en een
volledige hersom uit. Een mismatch wordt als integriteitsfout gerapporteerd;
er wordt niets gerepareerd of teruggewonnen.

## SQLite- en bestandgrenzen

- `journal_mode=DELETE`; ieder `-wal`- of `-shm`-bestand is verboden, ook leeg;
- `synchronous=FULL`;
- maximaal een kwart van `max_catalog_bytes` als SQLite main database via
  `max_page_count`, met de rest als ruime rollback-journalreserve;
- main en tijdelijk `-journal` worden met `lstat` als reguliere niet-symlink
  bestanden gecontroleerd en samen geteld;
- `SQLITE_FULL` en fysieke-envelope-uitputting worden getypeerd fail-closed;
- SQLite busy/locked wordt na de immutable begrensde timeout getypeerd;
- file-fsync na iedere nieuwe commit en exacte retry;
- POSIX parent-directory-fsync is verplicht; Python/Windows biedt die garantie
  hier niet en meldt `directory_fsync_supported=0` eerlijk.

## Bewuste grenzen — nog geen operationele quota-autoriteit

Deze ledger bewijst alleen zijn eigen logische, gehashte administratie. Hij kan
op zichzelf niet bewijzen:

- dat raw files werkelijk bestaan, byte-exact bij hun claims passen of nergens
  buiten de ledger geschreven zijn;
- allocated blocks, sparse-filegedrag, compressie, filesystemmetadata,
  hardlinks, quota of beschikbare schijfruimte;
- dat een geprivilegieerde externe actor database- en raw bestanden niet samen
  heeft gemanipuleerd;
- dat `actual_entries` bij iedere toekomstige writer exact dezelfde fysieke
  definitie gebruikt;
- dat een oude RESERVED-write definitief afwezig is.

Voor operationele admission blijven daarom minimaal verplicht:

1. één volledige startup-reconciliation van alle bestaande providerroots en
   schema-3 catalogi tegen een leeg/gecontroleerd ledgerhead;
2. private single-writer roots die alleen via reserve-before-write bereikbaar
   zijn;
3. OS/filesystemquota als onafhankelijke harde fysieke begrenzing;
4. een atomair protocol dat raw publicatie en ledgercommit exact koppelt;
5. periodieke read-only reconciliation/audit en power-cut/soak op het echte
   filesystem;
6. expliciet extern cryptografisch bewijs vóór ooit een gestrande RESERVED-row
   zou mogen worden gecompenseerd. Deze kandidaat biedt die reclaimfunctie niet.

## Testbewijs

`tests/test_worker_raw_capacity_ledger.py` bewijst onder meer:

- immutable profile, DELETE journal, catalogus- en providergrenzen;
- exacte reserve-/commitretry en mismatched conflicts;
- globale plus per-provider capacity en atomische actual-deltarecheck;
- kleinere actual als enige geldige vrijgave van reservatiemarge;
- committed rows die niet te deleten of terug te draaien zijn;
- O(1)-counterinvarianten en read-only volledige audit;
- profile drift, schema-/countertamper en echte `SQLITE_FULL`-normalisatie;
- twee onafhankelijke processen die globale oversubscription voorkomen;
- twee gelijktijdige commits die elk boven hun reservatie groeien, waarvan
  maximaal één mag slagen wanneer beide samen de globale cap overschrijden;
- een durability-barrier die ook bij exacte reserve- en commitretries opnieuw
  wordt uitgevoerd; een barrierfout weigert een succesvol returnresultaat maar
  verandert geen enkele durable row of counter;
- echte `os._exit` vóór en na reservecommit en na evidencecommit;
- blijvend meetellen van crash-RESERVED en COMMITTED capaciteit;
- afwezigheid van release/abandon/reconcile en iedere operationele wiring.

Dit is uitsluitend lokaal/shadow bewijs. Er is niets gedeployed, herstart of
geactiveerd.
