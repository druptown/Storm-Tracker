# METAR closed-world ground-shadowbewijs

Status: production-like shadowcandidate, lokaal geaudit, niet live gekoppeld en
niet operationeel toegelaten. Dit document is geen meteorologisch
admissioncertificaat.

## Bewaarde echte bron en lokaal resultaat

De proef gebruikt de lokaal exact bewaarde Aviation Weather Center-cache
`metars_current.csv.gz`:

- bytes: `240220`;
- SHA-256: `959f4c8ba1d91aef2b7b4a3e5dbdc6c92ba2a98530acd0d4af8b0f80751f9ebf`;
- bestandstijd: `2026-07-29T11:48:09.2723334Z`;
- volledige canonical decode: `10294` facts;
- canonical SQLite in de bewijsrun: `128958464` bytes;
- canonical SQLite SHA-256:
  `f323cb5601d2f0981d5a24bcebe53a67d851d67ace36a85d05ba2683ecb03e0b`;
- raw capture-ID:
  `capture-dac2743588b5ae898b3e62079bb3fe7cff6a3dafcd6a7cc2df7f5c160f45d508`;
- exacte groundquery rond een werkelijk aanwezig regenrapport: `56` facts;
- completeness: volledig, geen overflow;
- accounting: `56 withheld`, `0 used`, `0 duplicate`, `0 outside`, `0 stale`,
  `0 overflow`, `0 indeterminate` op factniveau;
- redenen: `29 physical_site_alias_unverified` en
  `27 provider_variable_semantics_uncertified`; alle 56 hebben tevens
  `bridge_evidence_withheld`;
- candidate-uitkomst: `indeterminate`, `candidate_confirmed=false`;
- operationele uitkomst: altijd false.

Er is geen positieve meting gegenereerd. De present-weatherfacts komen uit de
exacte gzip. Het proefpolygon is daarentegen uitdrukkelijk
`fixture_only_unadmitted`, omdat geen passend bewaard science-v2-receipt met
CellTrack-geometriebewijs beschikbaar is. De lege fysieke-sitecatalogus
voorkomt dat de providercoördinaten uit diezelfde METAR-cache circulair tot
onafhankelijk geverifieerde sites worden gepromoveerd.

De run-specifieke lokale scope van de non-spawn bewijsrun was:

- raw-tree-manifest SHA-256:
  `c9f072592f407c9c0187fdca1fb4f79e3254b9654c2981435ff99cf1c2dcdba0`;
- raw tree: 2 directories, 3 files, `289681` bytes, tree-SHA
  `d5dd36d3267f4de3b50fbc1a4fa49950c39c6d69820e1e73b79790047cd53ead`;
- semantiekdocument SHA-256:
  `e30b2fc4143b50e41150e6365c88d8ba84790119870dadd31a34993404ab42ab`;
- footprintdocument SHA-256:
  `ee2ead3d140a693839cb0a560c07f48ed0720e9f29806e9e5d377a62269284de`;
- sitecatalogusdocument SHA-256:
  `ca1316f1c73525f581bb6572fd7068eaf67d1198cc91b6aeca6d2cd0890d0956`;
- authority-scope SHA-256:
  `be26e96537e72b76147e921edeaa45bdb8f19b56acf6d14391d2ea1a54168f5c`;
- runtime-profile SHA-256:
  `3d75a143278f024f50f06a486c0e104e89827d69dbaa1a530e83828d962c6507`;
- factory-contract SHA-256:
  `ee46817d8e452781092986c6dd34d9d0527332e62b6d3b524bb9874f363f3c91`;
- outer-authority SHA-256:
  `f8f7223e0f47f7381267e4e05f754ad373f23314841054b6a1da73f7f4ffd933`.

De document-, scope-, profile- en outer-authoritywaarden zijn
bewijsrun-specifiek: SQLite-receipttijden en het aantal toegelaten
fixturebindings zijn onderdeel van hun byte-exacte inhoud.
De broncache-SHA, bronbytes en canonical factcount zijn de stabiele fixture-
identiteit.

## Factory- en authority-API

De spawn-importable factory is:

```text
factory_id: stv3-metar-ground-closed-world-factory-v1
target: storm_tracker_worker.metar_ground_shadow_factory:build_closed_world_metar_runtime
```

Het vereiste `GroundShadowFactoryContract` zet alle vijf verklaringen exact:

```python
GroundShadowFactoryContract(
    factory_id="stv3-metar-ground-closed-world-factory-v1",
    target=(
        "storm_tracker_worker.metar_ground_shadow_factory:"
        "build_closed_world_metar_runtime"
    ),
    opens_only_authorized_paths=True,
    canonical_raw_reverification=True,
    hard_kill_safe=True,
    transactional_receipt_only=True,
    spawns_descendants=False,
)
```

De outer authority blijft het bestaande exacte contract
`stv3-ground-shadow-process-authority-v1`. Hij bevat precies acht en nooit
impliciete padrollen:

1. `canonical_ground_database`: exact absolute canonical SQLite; bestaande
   schema-/manifestidentity plus een extra byte-SHA in de factoryconfig;
2. `ground_receipt_database`: de enige mutable beslissingsstore;
3. `ground_archive_directory`: alleen sealed retentionsegmenten;
4. `read_directory`: de schema-3 raw-root;
5. `read_file`: canonical raw-tree-manifest met iedere relative path, size en
   file-SHA, plus entry-/byte-/treeaccounting;
6. `read_file`: fysieke-site-/alias-/capabilitycatalogus;
7. `read_file`: exacte METAR-semantiekcertificaten;
8. `read_file`: exact toegelaten target/cell/footprintbindings.

`factory_config.contract` is
`stv3-metar-ground-shadow-factory-config-v1` en heeft exact de volgende
velden:

```text
admission_blockers
authority_scope_sha256
canonical_content_sha256
capture_id
contract
footprint_document_sha256
footprint_label
fusion_policy
ground_evidence_limits
operational_admission_granted
raw_artifact_limits
raw_manifest_label
raw_store_label
raw_tree_manifest_sha256
semantics_document_sha256
semantics_label
site_catalog_document_sha256
site_catalog_label
```

De authority-scope hash bindt canonical content, raw-tree-manifest,
semantiekdocument, footprintdocument en de eigenlijke siteasserties. Die scope
zit in de immutable catalogusversie en daardoor in de bestaande
`runtime_profile_sha256`. Een gewijzigde raw/canonical/site/semantics/footprint-
set kan dus geen durable receipt uit een vorige scope accepteren.

De site-, semantics- en footprintdocumenten zijn canonical ASCII-JSON met een
exact schema en eigen contract. Sitecatalogusrecords worden terug opgebouwd
tot de bestaande `PhysicalSiteCatalog` en moeten daarna byte-semantisch gelijk
zijn aan `catalog.canonical_json`. Semantiekcertificaten worden terug opgebouwd
tot `GroundSemanticsCertificate` en hun fingerprint wordt opnieuw berekend.
Iedere footprintbinding omvat exact:

```text
cell_binding_sha256
decision_key_sha256
evaluated_at
footprint_geometry_sha256
footprint_proof_sha256
region_key_sha256
source_kind
target_snapshot_sha256
```

De runtime accepteert alleen `source_kind=preserved_science_receipt` of de
expliciet niet-toegelaten teststatus `fixture_only_unadmitted`. Iedere target-,
cell-, proof-, geometrie-, regio-, decision- of tijdsubstitutie faalt vóór de
groundquery en vóór een receiptcommit.

## Runner- en output-API

De parent roept uitsluitend de reeds bestaande harde runner aan:

```python
runner = GroundShadowIsolatedRunner(
    authority_path=absolute_authority_path,
    expected_authority_sha256=authority_sha256,
    factory_contract=factory_contract,
    limits=isolation_limits,
)
result = runner.execute(ground_fusion_shadow_request)
```

`GroundFusionShadowRequest` blijft de bestaande exacte target-/CellTrack-
binding. Er gaan geen facts, stations of rasters over IPC. Het parentresultaat
is `GroundShadowProcessResult` met:

```text
status
execution_outcome
detail_code
authority_sha256
durable_disposition
receipt_metadata
duplicate
isolation_enforced
child_exit_code
elapsed_seconds
meteorological_admission_granted=false
operational_confirmation_claim=false
operational_snapshot_claim_generated=false
```

Bij een committed run bevat `receipt_metadata` exact:

```text
accounting_counts
accounting_full_scan_complete
accounting_overflowed
accounting_source_fact_count
admission_status
authority_sha256
candidate_confirmed
candidate_state
cell_binding_sha256
contract
decision_key_sha256
metadata_sha256
meteorological_admission_granted
operational_confirmation_claim
operational_snapshot_claim_generated
receipt_sha256
request_sha256
runtime_profile_sha256
target_id_sha256
```

Parentreconciliatie tegen de hot store of een volledig geverifieerd sealed
archivesegment blijft beslissend na success, crash, kill, deadline of output-
overflow. Exacte retry/restart geeft dezelfde receipt; een archived retry
wordt niet meteorologisch herberekend.

## Gesloten I/O-grens

De production factory installeert in de spawned child een one-way Python audit
hook vóór hij de artifacts opent. Hij staat alleen toe:

- de exacte immutable files uit de acht rollen;
- precies de files uit het sealed raw-tree-manifest;
- expliciete SQLite rollbackjournals voor de geautoriseerde databases;
- de receipt-DB en zijn rollbackjournal;
- files onder de ene sealed archivedirectory;
- directorydescriptors die nodig zijn voor durabilitybarriers.

Socket-, subprocess-, `exec`-, `spawn`- en `system`-events worden geweigerd.
Niet-geautoriseerde file-, rename- of delete-events worden geweigerd. De
factory leest geen environment, credentialstore of secret. Fouttekst bevat
geen pad. Na raw- en canonical-storeinitialisatie worden hun immutable bytes
opnieuw volledig tegen manifest/SHA gecontroleerd.

Dit is defence in depth bovenop de parent-geautoriseerde SHA, spawn-boundary,
CPU/RSS/wall/outputgrenzen en durable receipt reconciliation. Een Python
audithook is geen algemene OS-sandbox; production admission vereist daarom nog
een NUC/Linux filesystem- en procesbeleidtest.

## Testbewijs

`tests/test_worker_metar_ground_shadow_factory.py` dekt:

- de volledige echte cachecapture en canonical decode;
- volledige dispositionaccounting in exact target/cell/tijdvenster;
- target/cell/footprintsubstitutie;
- extra raw files, raw-objecttamper en canonical DB-tamper;
- site-, semantics-, footprint- en authority-scopetamper;
- netwerk-/padweigering zonder padlek;
- echte spawn, restartduplicate, archived lookup;
- authoritytamper, parentdeadline en result-outputcapaciteit;
- afwezigheid van service/HA/MQTT/pickle/network/secret-ingress.

De volledige batch staat op `13 passed in 218.78s` onder `-W error`, inclusief
echte Windows-`spawn`, restartduplicate, sealed-archive lookup,
authoritytamper, parentdeadline en result-outputcapaciteit. De suite werd
sequentieel uitgevoerd wegens het gedeelde harde processlot. Dit is lokaal
regressiebewijs, geen blijvend productspecificatiegetal en geen operationele
admissionclaim.

De aansluitende brede ground-regressiebatch staat op `248 passed in 85.03s`
onder `-W error`. Die batch omvat de canonieke evidence-store, fysieke
sitecatalogus, fusion/bridge, durable runtime, sealed retention,
process-isolatie en METAR-grounddecoder. De bestaande importgrens laat daarbij
alleen deze exacte closed-world factory als nieuwe storeconsumer toe en blijft
HA, `service.py` en generieke provider-runtimes weigeren.

## Exacte admission-blockers

De lokale authority publiceert negen expliciete blockers en houdt
`operational_admission_granted=false`:

1. `certified_target_grid_geometry`;
2. `frozen_primary_source_capture` voor de geraadpleegde semantiekbronnen;
3. `held_out_meteorological_validation`;
4. `independent_alias_to_physical_site_mapping`;
5. `independent_meteorological_review`;
6. `independent_station_coordinates`;
7. `preserved_science_receipt`;
8. `sensor_absence_capability`;
9. `site_independence_group_evidence`.

Verder blijven vóór productie vereist: Linux/HA power-loss en kill-soak,
offsite archive-retention/disaster restore, langdurige CPU/RSS/thermische soak,
een OS-level closed-world filesystem/networkpolicy en de centrale lifecycle-/
admissionkoppeling. Geen van die ontbrekende bewijzen wordt door een testalias,
providercoördinaat of fictieve positieve meting vervangen.
