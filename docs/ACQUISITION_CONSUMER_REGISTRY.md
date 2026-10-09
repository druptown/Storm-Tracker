# Acquisition consumer registry en reclamation closure

`storm_tracker_worker/acquisition_consumer_registry.py` beheert de gesloten
downstream-consumergrens van de terminal-raw lane. De authority verwijdert geen
bronbytes en voert geen retention-actie uit. Retention-v2 gebruikt haar als
constructor-gebonden stabiele store-authority en houdt de actuele epoch vast
bij seal, request, PREPARE en COMMIT. Dit is alleen lokale/offline
authoritywiring; er is geen service-, main-, HA- of MQTT-wiring.

## Gesloten membership

Een registry-root wordt uitsluitend gebouwd uit de bestaande
`TERMINAL_RAW_CONSUMER_CATALOG` en de volledige completion-output archive
source/reproof-authority. De root accepteert exact deze elf workloads:

- `ground.metar.global`
- `radar.aemet.composite`
- `radar.dpc.sri`
- `radar.dwd.radolan_rv`
- `radar.knmi.rtcor`
- `radar.meteofrance.dpradar`
- `radar.metoffice.uk_rate`
- `satellite.goes18.rrqpe`
- `satellite.goes19.rrqpe`
- `satellite.hsaf.h40b`
- `satellite.noaa.err`

Per workload bindt de canonical root exact de consumer-ID, stream-ID,
consumer-bindinghash, archive-source-profilehash, resolver-ID, resolverhash,
source-store-identityhash en reproofcontract. Toevoegen, weglaten, dupliceren
of verwisselen van één consumer of resolver faalt gesloten.

## Durable epochs en constructorbinding

`AcquisitionConsumerRegistryAuthority.ensure_epoch(...)` schrijft een nieuwe
root en monotone epoch atomair, of retourneert de byte-exacte huidige receipt.
Een historische root kan niet opnieuw current worden. Database en owner-lock
zijn single-link regular files; metadata, roots en epochs zijn immutable; de
store gebruikt `foreign_keys=ON`, `synchronous=FULL`, DELETE-journal en vaste
page-, root- en epochcaps.

`authority.verifier(...)` bindt eerst de actuele live exact-11 membership en
store-instance. `bind_current_epoch(...)` levert daarna een immutable
`AcquisitionConsumerRegistryBoundEpochVerifier` voor exact:

- registry-root;
- huidige epoch-root en epoch-receipt;
- persistente store-instance en live database/owner-lockidentity.

`verify_held()` weigert een nieuwe epoch, storevervanging, schemawijziging,
ownerverlies of checksumverschil. De publieke contextmanager
`hold_current_epoch()` houdt daarnaast de authority-RLock en een SQLite
`BEGIN IMMEDIATE` over de volledige critical section, yieldt de bound verifier
en herbewijst root, epoch, receipt, store-instance, pad en durable owner voor
release. Retention-v2 bindt de stabiele registry-authority als constructor-
dependency en gebruikt deze current hold bij seal/request/PREPARE/COMMIT; losse
rootstrings vormen geen authority.

## Consumer-closure receipt

Voor iedere workload/job in een registry-gebonden PREPARED-request wordt een
immutable `AcquisitionConsumerClosureReceipt` gemaakt. De receipt bindt:

- candidate-, workload- en acquisition-jobidentity;
- de exacte registry member en registry root/epoch/receipt/store identity;
- lifecycle event-, producer-, consumption- en terminal-identity;
- normale terminalkind `consumed`, outputhash, streamsequence en hot/archive
  storage-identiteit;
- het vereiste completion-output archive profile, resolver, source-store en
  reproofcontract uit de registry;
- de werkelijk gevalideerde sealed source-retention archive anchor, segment en
  record.

De receipt bevat geen payload, proof, pad, URL, token of fouttekst. Iedere
receipt en beide archivebindingen hebben een eigen canonical SHA-256.
Retention-v2 bewaart de closurebytes in de sealed candidate-set. Seal
reconstrueert ze live onder `hold_current_epoch()`; request/PREPARE/COMMIT eisen
dat exact die epoch nog current is. Na durable COMMITTED zijn candidatepagina's
historische resolvers: zij controleren de gepinde registry-root, epoch, member
en receipt plus de byte-exacte v1-records, zonder de actuele registry of reeds
geprunede lifecycle opnieuw te raadplegen. Zo kan een stale request niet
COMMITTED worden en kan een geldige lifecycle-first APPLY wel roll-forwarden.

## Activatiegrens

De registry zelf heeft geen delete-, unlink-, VACUUM-, participant- of
coordinatorimplementatie. Zij levert alleen membership-, epoch- en
closureauthority aan de lokale retention/reclamationgrens. De bewezen native
prepare/apply/abortpaden zijn niet opgenomen in worker-servicecomposition;
geen live bronreclamatie of productieactivatie wordt hier geclaimd.
