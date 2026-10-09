# Acquisition-bronretentie en sealed export

Status: lokaal bewezen v1 sealed export plus v2 current candidate-authority;
offline vijfrollenapply bewezen, geen servicewiring of live toelating

## V2 participantgrens (niet live)

`AcquisitionSourceRetentionArchiveV2` gebruikt een afzonderlijke root en is de
fysiek gehouden `SourceReclamationCandidateAuthority` voor het frozen protocol
met SHA-256
`58b1d897b12949ece553dc91742e5bd9e5aaa0507f3205225cb71a65b3e1dfe3`.
Participantviews komen uitsluitend uit
`acquisition_source_reclamation_views.py`; de archive implementeert geen eigen
of afgeleide viewvariant.

De constructor bindt create-once de bestaande v1
`AcquisitionSourceRetentionArchive` en de stabiele
`AcquisitionConsumerRegistryAuthority`. `authority.json` bindt de registry-
storeidentity/padwitness en de v1-root/source-binding, maar bewust geen actuele
epoch of v1-head. Dezelfde v2-authority blijft daardoor geldig over append-only
registry-epochs en v1-generaties; de v1-, registry- en v2-roots moeten fysiek
disjunct zijn.

De veilige constructievolgorde doorbreekt de authoritycyclus expliciet:

1. open de vijf exacte source-storeobjecten en maak een nog lege v2 archive;
2. maak de coordinator met die archive als candidate-authority;
3. bind raw create-once aan candidate- en decision-authority en maak de vijf
   native participants;
4. roep `bind_source_participants_v2(...)` eenmaal aan in de frozen apply-order;
5. seal kandidaten en maak pas daarna een reclamationrequest.

De bind accepteert voor raw alleen exact hetzelfde `RawArtifactStore`-object en
voor de vier adapters alleen een adapter waarvan `_store` exact het reeds
geinjecteerde object is. Rollen, candidate-authority, gedeelde
coordinator-authority en vijf onderling verschillende persistente
source-storeidentiteiten worden create-once vastgelegd en bij iedere fysieke
v2-operatie tegen dezelfde objecten en authorities bewezen. De geordende
identityroot zit in metadata, segment, manifest, anchor en HEAD. Een andere
objectset, authority of identity faalt gesloten; er is geen pad-hashfallback.

Sealen gebeurt onder `consumer_registry_authority.hold_current_epoch()` en een
gehouden actuele v1-anchor. Iedere closure wordt byte-exact opnieuw opgebouwd
uit het actuele registry-member, het v1-record en de nog levende lifecycle-
terminal. De candidate-set pint registry-root, epoch, epochreceipt en
storeidentity plus de exacte v1-anchor/segment/recordbytes. Een successor mag
dezelfde volledige candidate-ID-inventory alleen onder een strikt hogere epoch
sealen; de v1-anchor mag gelijk blijven of moet aantoonbaar append-only
descendant zijn. Same-epoch overlap, partiele overlap en een niet-descendente
anchor worden geweigerd.

`create_reclamation_request_v2(...)` en
`hold_current_request_authority_v1(...)` houden de actuele registry-epoch vast
en herbewijzen request, set en gepinde v1-records voor en na hun critical
section. Een stale PREPARED-request blijft abortbaar maar kan niet COMMITTED
worden. Een vooruitgeschoven v1-head maakt het request niet stale zolang de
gepinde historische anchor onder dezelfde stabiele v1-authority resolveert.
De processlokale v1-archivelock is alleen voor dezelfde owner-thread reentrant,
zodat een geneste candidatepagina de currentness-hold niet inkort. Andere
threads blijven uitgesloten en de OS/file-lock blijft iedere andere process
over de volledige buitenste critical section fencen.

Raw bytes staan uitsluitend in checksum-genoemde objectfiles. Metadata en de
maximaal 64 records van `hold_candidate_page_v1(...)` bevatten alleen begrensde
referenties en protocolbindings, nooit raw bytes, lokale paden, tokens of
dependencyfouttekst. Candidatepagina en audit zijn historische resolvers: zij
herbewijzen sealed closurebytes, gepinde registryfacts en v1-records zonder de
actuele lifecycle of epoch te raadplegen. Alleen unieke objecten in de werkelijk
teruggegeven slice worden voor en na yield gehasht, onder de harde 128-MiB-cap.
Daardoor blijft roll-forward mogelijk na lifecycle-first prune of v1-headappend.

Publicatie gebruikt een duurzaam `PENDING.json`-journal. Restart ziet daardoor
alleen de oude of de volledig nieuwe generatie; incomplete targets worden
teruggerold en een gepubliceerde HEAD wordt afgerond. `audit_v2()` loopt de
volledige anchor/manifest/segment/candidate/metadata/reference/objectketen,
exacte file-inventory en fysieke caps. Caps, tamper, symlinks, onbekende files,
clock rollback en identitydrift veranderen de HEAD niet en falen gesloten.

Deze grens maakt prepare/apply/abort door de afzonderlijke vijf participants
mogelijk, maar is zelf geen delete-engine en is nergens aan service, Home
Assistant, MQTT of deployment gekoppeld.

## Historische v1-beslissing

De volgende withheld-beslissing blijft de norm voor het v1-exportcontract en
voor iedere caller die alleen v1-bewijs bezit. Zij beschrijft niet langer de
mogelijkheden van de afzonderlijke v2-authority en mag geen v2-applyreceipt
vervangen of opwaarderen.

Binnen het historische v1-contract was veilige unified pruning niet
bewijsbaar. Het v1-deel van `acquisition_source_retention.py` implementeert
daarom uitsluitend:

1. een read-only, begrensde eligibilityscan;
2. een byte-exacte content-addressed sealed export;
3. een hashketen met immutable manifestgeneraties en een atomisch vervangen
   durable anchor;
4. archive-aware lookup en expliciet `window_expired`;
5. privacy-minimale auditdiagnostiek.

Iedere record bevat hard `source_reclaim_authorized=false` en
`reclaim_withheld_reason=cross_store_atomic_prune_contract_unavailable`.
De v1-export-API bevat geen call die een bronreceipt, capture, schedulerrow,
executionreceipt, capacitycommit of downstreamreceipt kan verwijderen.

De afzonderlijke, eveneens inerte
`acquisition_source_reclamation_authority.py` zet dit resterende gat om in een
canonieke machineleesbare authority. Die bindt een volledig hervalideerd plan
aan de durable sealed-archivehead, hasht de exacte ontbrekende participant-API's
en kan uitsluitend `status=withheld`, `reclaim_authorized=false` en
`source_release_performed=false` retourneren. Zie
`ACQUISITION_SOURCE_RECLAMATION_AUTHORITY.md` voor het sealed historische
v1-contract en `ACQUISITION_SOURCE_RECLAMATION_APPLY_AUTHORITY.md` voor de
actuele v2 prepare/apply/resolver- en coordinatorgrens.

## Waarom live bronreclamatie nog dicht blijft

De actuele v2-grens heeft nu wel een duurzame consumerregistry, een
retention-backed current candidate-authority, drie native controlparticipants,
een raw participant met reference-counted unlinkjournal en een lifecycle-v4-
participant. De coordinator bindt de vijf rollen fysiek en kan lokaal de exacte
keten PREPARED -> COMMITTED -> APPLIED uitvoeren. Een stale registry-epoch
wordt vóór COMMITTED geweigerd; een abort bewaart de bronfeiten en een exacte
hogere-epoch-successor kan daarna veilig verder. Zie
`ACQUISITION_SOURCE_RECLAMATION_APPLY_AUTHORITY.md` en
`ACQUISITION_CONTROL_RECLAMATION.md`.

Een sealed v1-exportanchor alleen blijft echter nooit een delete-token. Alleen
de volledige v2-current hold, vijf preparefences, duurzame coordinatorbeslissing
en geordende participantapply vormen samen offline reclamationauthority. Ook
dat bewijs is nog geen toestemming voor live gebruik: raw en lifecycle kunnen
via portable Python een externe hardlinkmaker niet crash-atomair uitsluiten,
en de doelomgeving heeft nog geen exclusief directory-ownerbeleid, geaudite
directorybarrier, echte power-lossmatrix of resource-/thermal soak doorlopen.

## Eligibilitycontract

Een kandidaat is alleen `sealed_export_eligible` wanneer dezelfde exacte
identiteiten opnieuw uit alle stores zijn geladen en gevalideerd:

- een complete raw-v2 capture uit RawArtifactStore schema 3, inclusief iedere
  memberreceipt en de SHA-256 van de exacte bytes;
- precies één terminale `completed_shadow_raw` executionhead én de volledige,
  checksum-geverifieerde attemptgeschiedenis;
- een terminale schedulerproof met state/transition `completed`, dezelfde
  schedulerjob en hetzelfde attemptnummer;
- een `COMMITTED` capacityreceipt waarvan content-, rawreceipt-, capture- en
  commit-SHA, byte- en entryaantal exact overeenkomen;
- één normale terminale AcquisitionEvidenceLifecycle consumptionreceipt,
  samen met zijn producerreceipt, payload en complete sourceproof;
- dezelfde acquisitionjob, streamsequence, bronbytes en ingebedde scheduler-,
  execution-, capacity-, rawreceipt- en captureproofs;
- een terminale watermark ouder dan de ingestelde minimale hot-windowleeftijd;
- een onveranderde aggregate source-view vóór en na de scan.

Een plan bewaart positieve records uitsluitend als immutable canonieke bytes.
Vóór enig archivebestand wordt gewijzigd, herberekent export de plan-SHA en
controleert hij schema, typen, caps, limitsfingerprint, source-binding en de
één-op-éénrelatie tussen dispositions en records. Daarna moet de actuele
source-view nog exact de plan-view zijn en wordt iedere positieve record opnieuw
uit de vijf echte stores opgebouwd. Een gemuteerde nested record, een volledig
opnieuw gehashte forge of een stale plan is dus geen archiefauthority.

Ready, leased, rejected, superseded, stale/hot, ontbrekend, onbekend of
onvolledig bewijs wordt withheld. Een bestaande maar tegenstrijdige hash of
binding is integrity failure, niet merely “niet eligible”.

De enige toegevoegde bronstore-API is
`AcquisitionEvidenceLifecycleStore.load_terminal_consumption(stream, seq)`.
Die retourneert hot of archived één exact producer/consumptionpaar met payload
en proof. Rejection en niet-terminale states falen gesloten; cursorpositie of
objectaanwezigheid wordt nooit als consumption geïnterpreteerd.

## Sealed export en linearization

Archieflayout:

```text
archive-root/
  rotation.lock
  anchor.json
  staging/
  segments/<segment-sha256>.sealed.json
  manifests/<manifest-sha256>.manifest.json
```

Export gebeurt onder een processlokale mutex plus POSIX `flock` of Windows
`msvcrt` byte-range lock:

1. schrijf het segment naar staging, file-fsync, atomic replace en
   directory-fsync;
2. schrijf de volgende immutable manifestgeneratie op dezelfde wijze;
3. controleer de verwachte oude anchor-SHA opnieuw;
4. schrijf een nieuwe anchor naar staging, file-fsync, atomic replace en
   root-directory-fsync.

Alle namen zijn content-addressed. Het atomisch gepubliceerde `anchor.json` is
het linearization point. Een crash vóór die vervanging laat de oude generatie
actief; volledig geldige orphan segmenten/manifests mogen bij exact retry
worden hergebruikt. Een crash erna toont de nieuwe generatie. Partial staging
van uitsluitend dit archief wordt bij restart verwijderd en de stagingmap
wordt opnieuw gefsynct.

Audit herberekent anchor-, manifest-, segment-, record- en raw-objecthashes,
loopt de volledige manifest- en segmentketen, controleert ordinals, previous
heads, unieke kandidaten, row-/file-/bytecaps en valideert ook orphan files.
Een gap, fork, symlink, onbekend bestand, gewijzigde byte of verkeerde
source-binding faalt gesloten.

## Idempotency, lookup en caps

De kandidaatsleutel en record-SHA zijn immutable. Een retry met dezelfde
kandidaat en dezelfde proof retourneert `idempotent`; dezelfde kandidaat met
een andere record-SHA is integrity failure. Lookup met `include_archive=true`
verifieert het sealed segment opnieuw. Voor een bekende gearchiveerde kandidaat
met `include_archive=false` wordt expliciet
`AcquisitionSourceRetentionWindowExpired("window_expired")` opgegooid.

Caps gelden voor kandidaten per plan, executionhistory per job, records per
segment, segmentbytes, totale fysieke segmentbytes (orphan bytes inbegrepen),
segmenten/manifests, archivejobs en stagingfiles. Cap of ENOSPC verandert de
anchor niet. Vóór base64-encoding wordt de exacte canonieke metadataomvang plus
`4 * ceil(raw_bytes / 3)` en een worst-case segmentenvelope afgetoetst tegen de
segmentcap. Er draait geen `VACUUM` in deze keten.

## Platformdurability en privacy

File-fsync én directory-fsync zijn verplicht. Op Windows faalt de standaard
directorybarrier gesloten; tests of een production factory moeten een echte,
geaudite platformbarrier injecteren. Een no-op testbarrier is uitsluitend
faultmatrixtestbewijs en geen power-lossclaim voor productie.

Diagnostiek bevat alleen counts, booleans en one-way hashes. Geen job-ID,
station-ID, coördinaat, payload, pad, secret of fouttekst van dependencies wordt
geëxporteerd. Sealed records bevatten noodzakelijk de lokale provenance en
rawbytes, maar nooit `artifact_path`; ze worden niet via HA/MQTT gepubliceerd.

`RawArtifactStore.load_capture_bundle_bytes()` materialiseert vandaag wel het
volledige raw object in RAM voordat deze exporter zijn pre-encodingcheck kan
doen. Ook fresh exportvalidatie materialiseert de bron opnieuw. Daarom is dit
geen NUC-resource-admissionbewijs: live wiring vereist eerst streaming of een
hard begrensde process-isolationrunner met parent-enforced RSS/deadline/output.

## Recoveryrunbook

1. Stop een eventuele rotator; wijzig geen bronstore en verwijder niets.
2. Open dezelfde vijf source authorities en exact dezelfde archive-root.
3. Laat `initialize()` staging herstellen en voer `audit()` uit.
4. Bij alleen geldige orphans: bied exact hetzelfde plan opnieuw aan; content-
   addressing maakt de retry idempotent.
5. Bij tamper, gap, source-bindingdrift, onbekend stagingbestand of ontbrekende
   directory-fsync: houd export én reclamatie dicht en bewaar alle bytes voor
   forensische inspectie.
6. Bij `window_expired`: gebruik archive-aware lookup; recompute of een
   synthetische duplicate is verboden.
7. Bij cap/disk-full: vergroot of migreer het archief via een afzonderlijk,
   checksummed copy/auditproces. Verwijder geen source- of archivebestand op
   basis van dit contract.

## Resterende go-liveblockers

- exclusief filesystem-owner- en parentdirectorybeleid dat externe hardlink-
  creatie voor raw- en lifecycle-roots buiten de authoritygrens houdt;
- geaudite directory-fsync/barrier op het uiteindelijke POSIX-/addonfilesystem;
- echte host-power-loss-, disk-full-, WAL/SHM-, media- en corruptieproeven over
  iedere PREPARED/COMMITTED/APPLYING/APPLIED-grens;
- streaming raw reads of harde process-isolation vóór live gebruik;
- resource/thermal soak en centrale lifecycle/admissionwiring;
- onafhankelijke red-team-herhaling op het gebouwde releaseartefact;
- onafhankelijke security/privacyreview van langdurige rawarchieven.

De offline v2-flow kan aantoonbaar ruimte vrijmaken in zijn geïsoleerde
teststores, maar `SERVICE_WIRING_ENABLED` en `production_enable_ready` blijven
beide `false`. De live hot source-set blijft dus alleen begrensd door de
bestaande harde storecaps. Volraken is expliciet fail-closed; v1-export alleen
maakt geen ruimte vrij.
