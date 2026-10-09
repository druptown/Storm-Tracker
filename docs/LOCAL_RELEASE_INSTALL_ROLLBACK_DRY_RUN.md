# Lokale installatie- en rollbacksimulator

## Status en grens

Deze tool simuleert uitsluitend in een nieuwe lokale directory:

- veilige materialisatie van de bekende rollbackbaseline;
- veilige materialisatie van een evidence-gebonden releasekandidaat;
- component-atomische upgrade terwijl beide services verondersteld gestopt zijn;
- herstel van iedere gepubliceerde journalbeslissing;
- exacte rollback naar de baseline;
- behoud van alle generaties en bewijsbestanden.

De simulator maakt geen verbinding, kent geen live doelpad en installeert niets.
Op 2026-08-14 is met de huidige Phase-C-bron een nieuwe lokale uitvoering
voltooid. Het gehashte rapport rapporteert `simulation_completed=true`,
`local_simulation_passed=true`, een lege `local_acceptance_blockers`-lijst en
beide lokale output-authoritycodes onder
`local_acceptance_blockers_closed`. `deployment_performed=false`,
`network_performed=false` en `live_paths_touched=false`.

Voor die uitvoering is tweemaal byte-identiek een volledige current-source
kandidaat gebouwd onder de interne versies integration `1.13.0` en worker
`1.16.0`. Dit is uitsluitend een local-only Phase-C-testkandidaat. Omdat deze
versies al oudere RC1-bytes aanduiden, mag deze kandidaat niet worden gedeployed
of gedistribueerd; een echte release vereist een nieuwe unieke versie- en/of
buildidentiteit. Dit is geen aangepaste rollbackrelease: integration `1.11.0`
en worker `1.14.0` bleven byte-ongewijzigd als rollbackbaseline.

De lokale en Supervisor-simulatienamespaces zijn leeg, maar exact zeven externe
installatiegates blijven open. Daarom blijft `real_installation_ready=false`;
dit is geen Home Assistant-, Supervisor-, worker-, MQTT- of
productie-installatieclaim. De actuele artifact-, rapport-, rollback- en
bronprovenance staat in het
[post-RC1-statussupplement](ROADMAP_EXECUTION_STATUS_2026-08-11_POST_RC1.md).

Het evidencecontract is:

```text
stv3-local-release-install-rollback-dry-run-v1
```

## Schrijfgrens

De runroot moet voor een nieuwe uitvoering volledig afwezig zijn; recovery opent
uitsluitend een bestaande, eerder door de simulator gemaakte runroot. Beide
paden mogen alleen onder de repositorymap `.analysis` of onder de tijdelijke
map van het besturingssysteem liggen. Vóór de eerste runrootmutatie opent de
tool de toegestane root met de OS-native absolute no-reparsebootstrap, bewijst
de identiteit van die root en neemt daaronder een descriptor-relative vaste
proceslease. De lease-anchor, zijn parent en de toegestane root worden tijdens
de run opnieuw op identiteit bewezen.

Na die ene bootstrap accepteert de outputlaag alleen `RelativePath`-namen onder
reeds geopende `DirectoryAuthority`-handles. Iedere final wordt eerst als
identiteit-gepinde sibling gestaged en daarna met descriptor-relative
no-replace-semantiek gepubliceerd. De verwachte bronidentiteit, de
bestemmingsparent en de verbruikte bron plus uiteindelijke bestemming worden
opnieuw bewezen. Een hostile same-user die met eigen toegangsrechten tegen de
output racet, valt nadrukkelijk niet binnen deze lokale claim en blijft een
externe gate.

Externe inputpaden worden vóór capture nog steeds lexicaal met `lstat`
gecontroleerd. Symlinks, junctions/reparse points en een verschil tussen het
absolute lexicale pad en `resolve(strict=True)` worden geweigerd voordat een
input wordt geopend.

Alle inputbestanden worden als single-link reguliere bestanden geopend. Vóór de
capture worden lexicale metadata en de gebonden FD-identiteit
`device/inode/size/mtime` bewezen. Terwijl die bron-FD openblijft, maakt de
simulator één exclusieve, begrensde, gefsyncte en read-only spool onder de
runroot; SHA-256 wordt tijdens deze capture over de gelezen bytes opgebouwd.
Daarna worden bronmetadata en spoolwitness direct bewezen. Evidenceverificatie,
ZIP/TAR-scans, options parsing en alle extractie lezen uitsluitend die
spoolbytes. Aan het einde worden zowel de originele bronnen als de spools
opnieuw bewezen. De tool verwijdert niets.
Stage, backup, actieve en retained bomen en spools blijven beschikbaar voor
audit.

## Verplichte inputs

- het RC-evidencebestand met beide archieven in dezelfde directory;
- exact het rollback-integratiearchief dat door zijn allowlisted SHA-256 is
  gebonden;
- exact het rollback-workerarchief dat door zijn allowlisted SHA-256 is
  gebonden;
- een gesanitiseerde Supervisor-optionssnapshot;
- een nieuwe runroot onder `.analysis` of temp.

De vaste rollbackpolicy bindt integration `1.11.0` en worker `1.14.0` aan de
recoveryhashes uit de vaste herstelprocedure. De legacy integratie heeft geen
pair-build-ID; dit wordt als expliciete legacy-afwijking gerapporteerd.

## Gesanitiseerde optionssnapshot

Werkelijke credentialwaarden horen nooit in dit bestand. Niet-geheime
operationele waarden staan onder `options`; credentials worden alleen als
aanwezig/afwezig onder `secret_presence` vastgelegd. De worker-first-pins en
het configpad staan alleen in de lokale inputsnapshot en zijn private spool;
geen van die waarden komt in fouttekst, CLI-output of het JSON-rapport.

JSON met duplicate keys of niet-eindige waarden wordt geweigerd. `options` en
`secret_presence` moeten disjunct zijn en hebben vaste key-countcaps. Booleans
gelden niet als integers. De legacywaarden zijn exact `cpu_threads=2` en
`reserve_ha_core=true`; de operationele waarden en de complete worker-first-
activatiegroep hebben expliciete typen, grenzen en onderlinge invarianten.

```json
{
  "contract": "stv3-sanitized-supervisor-options-v1",
  "options": {
    "cpu_threads": 2,
    "heartbeat_seconds": 10,
    "log_level": "INFO",
    "queue_size": 16,
    "reserve_ha_core": true,
    "runtime_mode": "legacy",
    "worker_first_authority_sha256": "",
    "worker_first_config_path": "",
    "worker_first_config_sha256": "",
    "worker_nice": 15
  },
  "secret_presence": {
    "aemet_api_key": false,
    "eumetsat_consumer_key": false,
    "eumetsat_consumer_secret": false,
    "hsaf_password": false,
    "hsaf_username": false,
    "knmi_api_key": false,
    "meteofrance_api_token": false,
    "meteofrance_application_id": false,
    "netatmo_client_id": false,
    "netatmo_client_secret": false,
    "netatmo_refresh_token": false,
    "synoptic_token": false
  }
}
```

Met deze volledige union-snapshot modelleert de simulator de kandidaatprojectie
en het exacte herstel van de legacyprojectie. Het lokale resultaat is:

```json
{
  "supervisor_options_roundtrip_proven": true,
  "real_installation_ready": false
}
```

Voor de huidige overgang verdwijnen de 1.14-schema-keys `cpu_threads` en
`reserve_ha_core`, terwijl vier activatiekeys en acht credentialkeys in 1.16
bijkomen. De volledige union-snapshot sluit die lokale gate; een ontbrekend of
ongeldig veld blijft fail-closed. Dit is geen echte Supervisor-run: build/cache
en de doelinstallatie blijven externe blockers.

## Uitvoering

Voorbeeld vanuit de repositoryroot:

```powershell
python -I -B scripts/dry_run_release_install.py `
  --candidate-evidence .analysis/release-candidates/20260814-i1.13.0-w1.16.0-phase-c-rc2/storm_tracker_release_v1.13.0_worker_v1.16.0_evidence.json `
  --rollback-integration-archive storm_tracker_v3_v1.11.0_deploy.zip `
  --rollback-worker-archive storm_tracker_worker_v1.14.0_deploy.tar.gz `
  --sanitized-options-snapshot .analysis/release-install-inputs-20260814/sanitized-options.json `
  --run-root .analysis/release-install-dry-run-<nieuwe-unieke-run-id>
```

De hierboven genoemde RC2-map is uitsluitend lokaal testbewijs. Gebruik deze
bestanden niet voor distributie of installatie onder de hergebruikte
`1.13.0`-/`1.16.0`-labels.

De CLI retourneert nul wanneer de lokale logische state machine voltooit. Lees
voor automatisering altijd `local_simulation_passed`,
`local_acceptance_blockers`, `local_acceptance_blockers_closed` en
`real_installation_ready`, plus de drie exacte groepen onder
`blocker_namespaces`: `local_simulation`, `supervisor_installation` en
`external_installation`. Een volledig geslaagde Phase-C-run kan de lokale groep
sluiten en `local_simulation_passed=true` rapporteren; de externe groep blijft
open en houdt `real_installation_ready=false`. Een archief-, identiteit-,
extractie-, containment- of transactiefout retourneert niet-nul.

## Archief- en treecontract

De kandidaat wordt eerst tegen het bestaande release-evidence herlezen en
vervolgens strenger op installatiemetadata gecontroleerd:

- exacte entryvolgorde en -set;
- één verwachte pakketroot;
- geen traversal, backslashes, duplicate entries, links of speciale files;
- exacte filebytes, grootte, SHA-256 en begrensde aantallen/bytes;
- alle ZIP/TAR-memberbodies lopen door hetzelfde streamingcontract met een
  buffer van maximaal 1 MiB. De opgegeven membergrootte wordt vóór de eerste
  bodyread aan de filecap getoetst, SHA-256 en private-keymarkers (ook over een
  chunkgrens) worden incrementeel bewezen en exact één extra byte bewijst dat
  er geen overrun is. Legacy totalbudgetten worden eveneens vóór iedere
  memberread op basis van de gedeclareerde grootte geweigerd en daarna opnieuw
  aan de werkelijk gelezen grootte getoetst;
- een afzonderlijke `archive_member_count`-cap voor kandidaat- én
  legacyarchieven, naast caps voor inputbytes, memberbytes, totalbytes, UTF-8-
  padlengte, componentlengte en paddiepte;
- vaste JSON-caps voor bytes, nesting, nodes, key- en stringbytes en integers;
  floats en dus ook exponent-overflow zoals `1e999` worden volledig geweigerd;
- weigering van Windows device names, ADS/colon, control characters en
  trailing punt/spatie, ook wanneer de simulator op POSIX draait;
- exacte integratieversie, workerversie, manifest, worker `__version__` en
  pair-build-ID;
- identieke worker `options`- en `schema`-keysets;
- geen caches, databases, private keys of geneste archieven;
- kandidaatmodes `0644`, `run.sh` `0755`, directories `0755`;
- worker-TAR logisch `root:root`, uid/gid nul en deterministische timestamps.

De bekende legacy-archieven hebben `0666`/`0777`-metadata. Hun originele bytes
en hashes blijven behouden, maar de sandboxmaterialisatie normaliseert files
naar `0644`, scripts en directories naar `0755`. Op Windows is die POSIX-mode-
en ownershipcontrole alleen logisch; het report maakt dat zichtbaar.

Het interne statecontract `stv3-local-release-install-state-v2` bindt naast
ieder bestand ook de volledige canoniek gesorteerde directoryset in
`content_sha256`. Expliciete lege legacydirectories worden daardoor niet meer
afgeleid uit alleen bestandspaden: de ZIP/TAR-scan neemt ze op in het zegel, de
extractie bewijst de directoryset opnieuw en materialiseert haar, en iedere
fingerprint tijdens upgrade, rollback en recovery eist exact dezelfde set.
Een ontbrekende of toegevoegde lege directory maakt de generatie ongeldig. Het
report bevestigt dit met `legacy_empty_directory_canonicality_proven=true` en
per tree met `explicit_empty_directories_proven=true` plus de exacte lijst
`empty_directories`.

Bij extractie wordt iedere member eerst volledig via dat begrensde
streamingcontract naar een OS-beheerd tijdelijk bestand geschreven. Een
truncatie, overrun, hash-, CRC- of private-keyfout vindt daardoor plaats
voordat het uiteindelijke destinationpad wordt aangemaakt. De daaropvolgende
descriptorwrite handelt partial writes expliciet af. Het positieve reportbewijs
`streaming_archive_member_processing_proven=true` publiceert contract
`stv3-archive-member-stream-v1`, de 1-MiB-cap en de size/hash/marker/
overrunclaims. De Phase-C-outputauthority classificeert daarnaast afzonderlijk
pre-publicatie- en postpublicatiefouten. Als na publicatie de final-reproof, een
directorybarrier of het sluiten van de gepinde parent faalt, verwijdert zij
alleen de exact gepubliceerde identiteit, synchroniseert de parent en bewijst de
afwezigheid opnieuw. Een inmiddels geplaatste vervanging wordt nooit verwijderd;
een mislukte cleanup blijft een getypeerde fout en levert geen positief rapport
op. Een geslaagde geïntegreerde run rapporteert daarom
`destination_io_failure_cleanup_proven=true` en
`postpublication_final_cleanup_proven=true`.

Vóór elk van de acht standaardbibliotheekroutes valideert een zelfstandige
begrensde parser de ZIP-EOCD/central directory of de gzip/USTAR-headerstroom.
Membercount, pad-, extra- en commentvelden, per-member- en totaalgrootte zijn
begrensd voordat `ZipFile.infolist()` of `TarFile.getmembers()` kan draaien.
ZIP64 en PAX/GNU-extensies falen gesloten; het gepinde legacyprofiel met een
klassieke, gesigneerde ZIP-data-descriptor wordt exact geverifieerd. Preflight
en de aansluitende standaardbibliotheekparse delen één open filedescriptor en
de index wordt exact op path, kind, count en size aan `TreeRecord` gebonden.
Het report publiceert daarom
`bounded_streaming_archive_header_parsing_proven=true`. Het zelfstandige
archivebewijs claimt geen simulatorintegratie. De Phase-C-context telt voor een
nieuwe uitvoering exact acht benoemde inspectie-/validatie-/extractieroutes en
eist de exacte set; recovery verwerkt geen archive opnieuw en eist daarom exact
0/0 routes. Pas de geïntegreerde combinatie met bootstrap, lease, Phase A,
Phase B en postpublicatiecleanup mag de lokale blockers sluiten.

De kandidaatbouwer gebruikt voor de worker uitsluitend het vaste ASCII
USTAR-profiel; PAX of GNU-longname wordt nooit als uitweg geschreven. Tijdens
de broninventarisatie worden root, afgeleide directories en bestanden met de
daadwerkelijke `TarInfo.tobuf(USTAR_FORMAT)`-encoder op ASCII- en USTAR
name/prefix-representeerbaarheid bewezen. Dit gedeelde releaseprofiel geldt
voor beide source trees en faalt vóór de eerste stagingnaam of outputmutatie.

## Switch- en recoverymodel

Integratie en worker wonen in afzonderlijke directories en kunnen dus niet als
één filesystemoperatie worden omgezet. Het model is bewust:

- component-atomisch;
- niet pair-atomisch;
- alleen geldig terwijl beide services gestopt worden verondersteld;
- pair-coherent na normale voltooiing of recovery.

Iedere nieuwe uitvoering en iedere recovery neemt eerst een niet-blokkerende,
exclusieve OS-proceslease op een vaste anchor die cryptografisch aan het
canonieke run-rootpad is gebonden. De lease is al vastgehouden voordat de
runroot wordt aangemaakt of voor recovery wordt geopend. Een tweede proces en
een tweede eigenaar in hetzelfde proces falen daardoor vóór enige mutatie. De
descriptor is niet-overerfbaar; POSIX `flock` of Windows `msvcrt` geeft de lock
ook na een abrupt proceseinde automatisch vrij, zonder PID- of
stale-file-takeover. Uitvoering en recovery verifiëren de anchor-identiteit en
-inhoud terwijl de descriptor wordt vastgehouden. Het report bindt dezelfde
veilig gebootstrapte allowed-rootidentiteit aan de lease en legt dit vast in
`exclusive_process_lease_proven=true`,
`process_lease_integration_proven=true` en het begrensde
`exclusive_process_lease`-bewijsobject.

Voor iedere journalpublicatie bestaan afzonderlijke grenzen na pending-file-
fsync, na rename/publicatie en na directorybarrier. Iedere tree-rename heeft
grenzen vóór rename, na rename, na bron-parentbarrier en na doel-parentbarrier.
Diezelfde grenzen gelden voor de vier recovery-renames en de drie mogelijke
recovery-journalpublicaties. De testcontracten benoemen onafhankelijk 63
uitvoeringsgrenzen en 25 recoverygrenzen; iedere grens wordt zowel met een
exception als met een echt abrupt `os._exit(73)` afgedwongen.

Vóór een gepubliceerd `upgrade_commit_decided` herstelt recovery de baseline.
Na die beslissing voltooit recovery de kandidaat. Zodra `rollback_decided` is
gepubliceerd voltooit recovery altijd de baseline. Recovery is idempotent en
verwijdert geen incomplete pending files of generaties.

Iedere journal-read valideert naast de hashketen een vaste transition grammar
en exacte payloadschema's. Voor iedere component moet recovery op ieder moment
precies één door state gebonden baselinegeneratie en één kandidaatgeneratie in
een journalfase-geldige slotlayout aantreffen. Baselines mogen nooit in stage of
retained staan en kandidaten nooit in backup. Ontbrekende, dubbele,
verwisselde, fasevreemde of onbekende generaties worden fail-closed geweigerd.

## Bewust open gates

`directory_durability_proven` blijft ook na deze simulatie `false`. De tool
oefent waar beschikbaar lokale directorybarriers, maar bewijst geen
doelfilesystem, stroomuitval of mediafault.

De kandidaat-worker-TAR wordt vóór TAR-interpretatie fail-closed gecontroleerd
op de exacte canonieke gzip-header (deflate, geen flags, mtime nul,
compressieniveau 9 en OS-byte 255). Een niet-canonieke header is alleen voor
de legacy rollback-worker toegestaan: de simulator hasht dan de volledige
gespoolde archive opnieuw en eist een exacte overeenkomst met de lowercase
SHA-256 uit `LegacyRollbackPolicy`. Daarmee is de legacy-uitzondering aan één
expliciet toegestane archive gebonden. Het rapport publiceert dit positief als
`candidate_worker_gzip_header_canonicality_proven=true` en
`legacy_worker_noncanonical_gzip_header_hash_bound=true`.

Het lokale blockeruniversum bevat exact deze twee codes:

- `ancestor_and_final_output_path_race_exclusion_not_proven`;
- `descriptor_relative_output_authority_not_proven`.

Voor een volledig geslaagde Phase-C-uitvoering is
`local_acceptance_blockers=[]`, bevat
`local_acceptance_blockers_closed` beide codes en is
`local_simulation_passed=true`. Beide blockers blijven afhankelijk van dezelfde
fail-closed integratieketen: veilige absolute bootstrap, descriptor-relative
proceslease, Phase-A-mutaties, Phase-B-owned reads, de operation-aware
archive-routeverplichting en postpublicatiecleanup. Valt een vereiste schakel
weg, dan wordt geen volledige lokale sluiting geclaimd.

Supervisor-optionsmigratie staat afzonderlijk onder `supervisor_installation`;
met de volledige geldige union-snapshot is die lijst leeg.
Doelinstallatie, data/schema, hostile races, runtime stop/start,
Supervisor-build/cache, power-loss en de menselijke deploygoedkeuring staan
onder `external_installation`. Die laatste is machineleesbaar als
`human_deploy_approval_not_granted`; hij is dus niet slechts documentatieproza.

Het report gebruikt één canonieke namespacebron:

```json
{
  "blocker_namespaces": {
    "external_installation": ["hostile_same_user_output_races_not_excluded", "..."],
    "local_simulation": [],
    "supervisor_installation": []
  },
  "local_acceptance_blockers_closed": [
    "ancestor_and_final_output_path_race_exclusion_not_proven",
    "descriptor_relative_output_authority_not_proven"
  ],
  "local_simulation_passed": true,
  "real_installation_ready": false,
  "destination_absence_prechecked": true,
  "atomic_noreplace_publication_proven": true,
  "bounded_streaming_archive_header_parsing_proven": true,
  "streaming_archive_member_processing_proven": true,
  "output_authority": {
    "path_race_scope": {
      "external": {"hostile_same_user_output_races_excluded": false},
      "hostile_same_user_in_local_scope": false
    }
  }
}
```

`local_acceptance_blockers`, `supervisor_installation_blockers` en
`unresolved_external_gates` zijn exacte aliases van die drie lijsten.
`installation_preflight_blockers` is uitsluitend de gesorteerde afleiding
`<namespace>:<code>` en introduceert geen vierde bron.

### Atomaire publicatie zonder overschrijven

Vóór de nieuwe run-root of een recoverytransactie wordt gemuteerd, voert de
simulator in de parent van die run-root een begrensde capabilityprobe uit op
hetzelfde filesystem. Die probe bewijst zowel voor een regulier bestand als
voor een directory een geslaagde bron-naar-bestemmingpublicatie én de
weigering van een al bestaande bestemming. Alle pending-naar-final
journalpublicaties en alle generation-directorywissels gebruiken daarna
uitsluitend dezelfde fail-closed no-replace primitive. Er bestaat geen
check/rename-, replace-, copy/delete- of link/unlinkfallback.

De backend is Linux `renameat2(RENAME_NOREPLACE)`, macOS
`renameatx_np(RENAME_EXCL)` of Windows
`NtSetInformationFile(FileRenameInformation)` met de reeds geopende
destination-parent als `RootDirectory` en `ReplaceIfExists=false`. Een
ontbrekende of niet-ondersteunde backend blokkeert de run vóór de
run-rootmutatie. Het rapport bindt backend, probe-eigenschappen,
publicatiescopes en werkelijke journal-/directorytellers onder
`atomic_noreplace_publication` terug aan
`stv3-open-directory-authority-atomic-probe-v1` en
`stv3-descriptor-relative-atomic-adapter-v1`.

Het zelfstandige atomic-capabilitybewijs sluit geen simulatorblocker. In de
Phase-C-context wordt het gecombineerd met de veilig gebootstrapte allowed
root, de descriptor-relative lease, outputmutaties, owned reads, de exacte
archive-routeverplichting en getypeerde postpublicatiecleanup. Alleen die
geïntegreerde afleiding sluit de twee lokale blockers; hostile same-user-races
blijven extern en `real_installation_ready` blijft `false`.

## Worker-first Supervisor-optionsroundtrip

De gesanitiseerde snapshot dekt nu de volledige union van het legacy- en
kandidaatschema. De vier activatievelden `runtime_mode`,
`worker_first_config_path`, `worker_first_config_sha256` en
`worker_first_authority_sha256` vormen één complete groep onder `options`.
Voor `legacy` zijn het pad en beide pins exact leeg. Voor
`worker_first_canary` bevat alleen de lokale inputsnapshot (en zijn private
spool) het canonieke absolute POSIX-pad en de twee lowercase SHA-256-pins.
Behandel die bestanden daarom als gevoelige lokale bewijsbestanden. De waarden
worden nooit naar fouttekst, CLI-output of het JSON-rapport gekopieerd;
credentialwaarden blijven geheel afwezig en worden uitsluitend als booleaanse
presence vastgelegd.

De kandidaat moet alle vier configdefaults en schemaregels exact declareren.
De lokale migratie projecteert de union-snapshot naar het kandidaatschema,
houdt de verwijderde legacyvelden `cpu_threads` en `reserve_ha_core` apart en
herstelt bij rollback exact de oorspronkelijke legacyprojectie. Met een
volledige, geldige snapshot rapporteert de simulator daarom lokaal
`supervisor_options_roundtrip_proven=true` en een lege
`supervisor_installation`-blockerlijst. Een ontbrekend of verkeerd getypeerd
veld blijft fail-closed. Dit is geen echte Supervisor-run: build/cache,
stop/start en doelinstallatie blijven externe blockers en
`real_installation_ready` blijft `false`.
