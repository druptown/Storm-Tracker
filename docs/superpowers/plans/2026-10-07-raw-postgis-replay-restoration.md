# Ruwe PostGIS-opslag herstellen — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. De gebruiker heeft voor deze opvolging uitvoering zonder subagents vastgelegd; behoud die keuze.

**Goal:** Bewaar ontvangen meteorologische broninhoud en exact gebruikte radarvelden in bestaande PostGIS, inclusief causale cyclus-/forecastbindings en aantoonbare opslaggaten.

**Architecture:** Een requestgebonden capturecontext verzamelt native inhoud vóór decode, zonder authenticatiegegevens. Een verliesloze veldcodec en additieve PostGIS-tabellen bewaren de inhoud; de bestaande begrensde writer/retryroute verwerkt opslag buiten de MQTT-eventloop. Een apart versiegebonden MQTT-rapport maakt native opslag en ontbrekende invoer zichtbaar zonder bestaande snapshotcontracten stil uit te breiden.

**Tech Stack:** Python, asyncio/contextvars, bestaande NumPy/psycopg/PostgreSQL/PostGIS, bestaande MQTT en NAS-HA MCP. Geen nieuwe dependencies of opslagdienst.

**Spec:** `/home/ikke/storm-tracker/docs/superpowers/specs/2026-10-07-raw-postgis-replay-restoration-design.md` — schriftelijk ontwerp goedgekeurd; uitvoering daarna goedgekeurd met "ga aan de slag". Bewaartermijn door gebruiker gewijzigd naar maximaal één maand op 7 oktober.

## Global Constraints

- Bestaande PostGIS is de enige duurzame meteorologische opslag.
- Alle meteorologische historiek maximaal één kalendermaand, inclusief bestaande metingen/voorspellingen en nieuwe RAW/radarvelden/replaybindings. Dit vervangt de eerdere afspraak zonder automatische RAW-verwijdering. Stationcatalogus, instellingen en deploy-/rollbackartefacten blijven referentiegegevens buiten deze retentie.
- Authenticatie-endpoints, OAuth-responses, tokens, cookies, requestheaders en wachtwoorden worden nooit gearchiveerd.
- Opslagfouten mogen bruikbare live weerpublicatie niet stoppen.
- Reconstructie gebruikt alleen invoer die op de gesimuleerde uitgiftetijd al ontvangen was.
- Geen extra polling, andere locatie, nieuwe infrastructuur of herbouw van de oude RAW/receipt/authorityketen.
- Geen productcode voor het afzonderlijke regenepisodeontwerp.
- Alleen NAS-HA `192.168.0.15` via `home_assistant_NAS` voor HA/App-bediening; nooit NUC. Geen Core/VM-herstart.
- Netatmo/importer/credentials en persoonlijke trackers ongemoeid; alleen transportcapture van bestaande Netatmo-weerresponses, geen adapterselectie-/authwijziging.
- Behoud `dedicated_nas_fast`, `cpu_limit_percent: 200`, `worker_nice: 0`, `cpu_threads: 2`, alle broncadansen en meteorologische drempels.
- Uitvoering start van exacte live104-bron; preserveer alle niet-gerelateerde dirty/staged wijzigingen. Geen brede/historische suites of opruiming.
- Maximaal bestaande `MAX_RETRY_CYCLES=8` vluchtige retrycycli. Een aanvullend totaalbudget van `512 * 1024 * 1024` unieke payload-/arraybytes voorkomt onbegrensde RAM-groei; dit is geen retentielimiet voor opgeslagen data. Overflow wordt expliciet geregistreerd, niet stil verwijderd of als compleet gemeld. Deze grens past een bestaande OPERA-responsecap van384MiB, maar is geen claim dat iedere gelijktijdige batch past.

## Review Focus

1. Twee parallelle regioverzoeken plus threadfetch houden hun eigen context; geen verkeerde targetbinding (taken1/3/4).
2. Token/loginresponse of token in query/redirect komt nooit als weerpayload terecht in DB of logs (taken1/3/4).
3. Decoderafwijzing en taakannulering na ontvangen bytes behouden de opname of een expliciet gat; geen verloren ontvangst door ontbrekende observaties (taken1/3).
4. Een cachetick/DB-retry vervalst geen ontvangsttijd; dezelfde bytes op een later echt ontvangstmoment krijgen wel een nieuwe receipt (taken2/5).
5. DB-uitval, RAM-overflow en workerstop leveren geen false-current, providerherfetch of stil verlies; retentie handhaaft alleen de expliciet gekozen maandgrens, zonder oudere migraties te wijzigen (taken1/2/5/6).

## Bestandsgrenzen en volgorde

Alle paden hieronder zijn relatief aan `/home/ikke/storm-tracker`.
Nieuwe kleine modules: `storm_tracker_worker/thin_raw_archive.py` (records/context/buffer/security), `thin_raw_field_codec.py` (verliesloze arrays), `thin_postgis_raw_archive.py` (SQL/roundtrip/migratie).
Bestaande orchestration blijft in `thin_provider_runtime.py`, `thin_operational_runtime.py`, `thin_postgis.py`, `thin_mqtt_service.py`. Alleen capturehooks in bestaande transportfuncties; geen herstructurering van de grote providerbestanden.
Taken1→2→3→4→5→6 zijn sequentieel; geen subagents. Elke taak eerst de nieuwe relevante test rood, minimale implementatie, dezelfde selectie groen. Commit alleen eigen taakbestanden/hunks; nooit `git add .` of bestaande staged wijzigingen meenemen.

### Task 1: Veilige immutable capturecontext en begrensde buffer

**Files:** Create `storm_tracker_worker/thin_raw_archive.py`; test `tests/test_thin_raw_archive.py`.

**Interfaces:**
- `RawCaptureContext`: frozen dataclass met `request_id: str`, `provider_id: str`, `product_id: str`, `requested_at: float`, `attempt: int`, `region_revision_ids: tuple[str,...]`, `target_positions: tuple[tuple[str,float,float,float|None],...]`, `code_revision: str`.
- `RawCapture`: frozen dataclass met `capture_id`, context, `transport`, `safe_source`, `received_at`, `product_valid_at: float|None`, `http_status: int|None`, `payload: bytes|None`, `content_type: str|None`, `disposition` en veilige `reason_code`.
- `capture_scope(context: RawCaptureContext, archive: RawArchiveBuffer) -> ContextManager[None]`; gebruikt ContextVar, met reset in finally; `asyncio.to_thread` draagt context over, geen module-global current-provider.
- `capture_received(source: str, payload: bytes, *, received_at: float, content_type: str|None = None, http_status: int|None = None, transport: str = 'http') -> str|None`; synchronous thread-safe enqueue van byteverwijzing, geen DB/hash/arrayserialisatie op eventloop.
- `RawArchiveBuffer(maximum_bytes: int = 512*1024*1024)` met `pending() -> tuple[RawCapture,...]`, `ack(capture_ids: tuple[str,...]) -> None`, `mark_gap(reason_code: str, context: RawCaptureContext|None) -> None`, `status() -> dict[str,Any]`.
- `safe_source_context(provider_id: str, source: str, params: Mapping[str,Any]|None) -> dict[str,Any]`: expliciete per-provider host/path/weerparameter-allowlist; onbekende route uitsluiten met reden, nooit generieke URL/config dump. Requestheaders/body worden niet als context bewaard.
- `weather_payload_allowed(provider_id: str, safe_source: Mapping[str,Any], payload: bytes, content_type: str|None) -> bool`: expliciete routegebonden media/envelopecontrole zonder meteorologische decode. Onverwachte login-/OAuthvorm of JSONauthvelden weigeren; onbekende vorm is excluded/gap met reden, niet blind raw capture. Malformed maar herkenbare meteorologische invoer blijft wel beschikbaar voor decoderonderzoek.

- [ ] Schrijf `test_parallel_contexts_and_to_thread_do_not_cross_bind`: receipts behouden exacte provider/regiorevisie; context buiten scope ontbreekt. `test_auth_response_and_secret_url_are_excluded`: authpayload geen receipt, secretquery/userinfo niet in status/records/repr. `test_buffer_overflow_records_gap_without_throwing_into_provider`: bytesbudget gerespecteerd, dropped_count groeit, status degraded.
- [ ] Run `.venv/bin/python -m pytest tests/test_thin_raw_archive.py -q`; verwacht falen op ontbrekende nieuwe API, niet op fixture/import van HA.
- [ ] Implementeer bovenstaande records/API, eindige tijdvalidatie, unieke ontvangst-ID's en thread-safe limieten. Bewaar onbekende producttijd als None; nooit requested_at daarvoor gebruiken. Een uitgesloten authroute telt apart, niet als ontbrekende meteorologische capture.
- [ ] Run dezelfde selectie; verwacht groen. Controleer dat status geen payloads/locatiegeometrieën/headers bevat.
- [ ] Commit alleen deze twee nieuwe bestanden: `feat: add scoped safe native capture buffer`.

### Task 2: Verliesloze veldcodec en additieve PostGIS-transactie

**Files:** Create `storm_tracker_worker/thin_raw_field_codec.py`, `thin_postgis_raw_archive.py`, `postgis_migrations/0009_raw_replay_archive.sql`; modify `thin_postgis.py`; test `tests/test_thin_raw_field_codec.py`, `tests/test_thin_postgis_raw_archive.py`.

**Interfaces:**
- `SerializedRadarField`: immutable `field_id: str`, `metadata: Mapping[str,Any]`, `arrays: Mapping[str,bytes]`, `array_descriptors: Mapping[str,Any]`.
- `encode_radar_field(field: ThinRadarField|ThinVisualRadarField, *, region_revision_id: str, confidence: ndarray|None = None, contributor_mask: ndarray|None = None) -> SerializedRadarField` en `decode_radar_field(record: SerializedRadarField) -> Mapping[str,Any]`.
- `.npy` verliesloos (`allow_pickle=False`) plus zlib; registreer dtype inclusief byteorder, shape, SHA256 van ongecomprimeerde arraybytes, CRS/affine, quantity/unit, identity/valid/receipt/available times en codecversie. Preserveer niet-finite invalid pixelwaarden/maskers; objectdtype weigeren. Visual levels blijven visual_classes, geen mm/h.
- `write_archive_batch(cursor: Any, batch: RawArchiveBatch) -> Mapping[str,int]`, aangeroepen binnen bestaande `_write_batch`-transactie; serialize/hash buiten eventloop.
- `RawArchiveBatch`: captures, serialized/native fieldjobs, cyclebindings en issuebindings; immutable snapshot. Buffer ack pas na succesvolle commit.
- `RawFieldJob`: frozen `field: ThinRadarField|ThinVisualRadarField`, `region_revision_id: str`, `confidence: ndarray|None`, `contributor_mask: ndarray|None`, `source_capture_ids: tuple[str,...]`; zijn arrays zijn archive-owned snapshots vóór later algoritme-/cachegebruik. Task5 maakt die snapshot buiten eventloop; task2 comprimeert hem tijdens batchwrite.
- `apply_raw_archive_migration(connection: Any) -> None`: voert exact0009 transactioneel/idempotent uit via bestaande bevoegde DB-verbinding, geen grants buiten nodige bestaande ingestrol; geen impliciete runtime escalation.

**Schema:** `stv3_raw_payload_v1(sha256 PRIMARY KEY, payload bytea, byte_count, created_at)`, `stv3_raw_capture_v1(capture_id PRIMARY KEY, payload_sha256 nullable FK, provider/product, requested_at, received_at, product_valid_at nullable, safe_context jsonb, disposition, reason_code)`; `stv3_raw_field_v1(field_id PRIMARY KEY, metadata jsonb, array_payload_refs jsonb)` met arrays als dedup payloads; `stv3_raw_field_source_v1(field_id,capture_id)`; `stv3_raw_cycle_v1(cycle_id PRIMARY KEY,evaluated_at,code_revision,policy jsonb,complete boolean,gaps jsonb)`; `stv3_raw_cycle_field_v1(cycle_id,region_revision_id,field_id,role)`; `stv3_raw_issue_field_v1(forecast_issue_id,cycle_id,region_revision_id,field_id)`. Alleen nieuwe tabellen/indexen/schema_migrations-entry, grant SELECT/INSERT aan bestaande storm_ingest; geen FK-cascade naar oude door retention verwijderbare issuetabellen.

Daarnaast `stv3_raw_code_revision_v1(code_revision PRIMARY KEY,source_tree_digest,worker_archive_sha256,integration_archive_sha256,safe_artifact_reference jsonb)`. Registreer bewezen candidateevidence/artifactreferentie bij deploy via bestaande DBverbinding; startup matcht zijn echte broninhoud/versie tegen die registratie. Geen zelfverwijzende archivehash in het archief inbouwen. Ontbrekende registratie betekent code_reference_missing en onvolledige replay, niet gokken met alleen versienummer.

- [ ] Schrijf `test_float_and_visual_field_roundtrip_preserves_bytes_dtype_masks_affine`: float32/float64, bool mask, NaN in invalid pixels, confidence/contributor_mask en reflectivityquantity exact behouden. `test_object_arrays_are_rejected_without_pickle`: objectdtype faalt veilig. `test_receipts_deduplicate_payload_not_receipt_time`: één payload/twee echte ontvangsten, oude tijd onveranderd na retry. `test_archive_transaction_rollback_keeps_buffer_and_existing_rows`: rollback geen ack. `test_archive_schema_is_additive_idempotent`: geen DROP of rewrite van reeds toegepaste migratie. Voeg afzonderlijke versiegebonden retentiemigratie toe voor één maand; gerichte grens-/deletevolgorde-/payloadreferentietests, geen eenmalige cleanup of schemawijziging buiten nieuwe retentie. Expiratie van noodzakelijke oude invoer mag geen false-complete recente replay opleveren.
- [ ] Run alleen deze twee nieuwe testbestanden; eerst aantoonbaar rood.
- [ ] Implementeer codec/SQL en voeg keyword-only `archive_batch: RawArchiveBatch|None = None` toe aan `ThinPostGISWriter.write_cycle`. Bestaande observatie-/station-/forecastwrites behouden; archivepayloads transactioneel, geen gedeeltelijk complete cycle bij ontbrekende bytes. Deque blijft max8; verloren entries registreren gap/drop met originele receiptcontext. Geen dubbele grote bytekopieën in retryqueue.
- [ ] Run dezelfde selectie plus nieuwe archive-gerelateerde tests in `tests/test_thin_postgis.py`; verwacht groen met legacy call zonder archive_batch ongewijzigd.
- [ ] Commit alleen eigen hunks: `feat: persist exact raw payloads and radar fields in PostGIS`.

### Task 3: Alle bestaande async HTTP-weerfetches vóór decode opnemen

**Files:** Modify `storm_tracker_worker/thin_provider_adapters.py:514-620`, `thin_provider_runtime.py:78-187`; test `tests/test_thin_raw_http_capture.py`, gerichte nieuwe tests in `tests/test_thin_provider_runtime.py`.

**Interfaces:**
- `AioHttpFetcher` krijgt optionele `archive: RawArchiveBuffer|None` zonder bestaande callsites te breken; eigen byte-readpad roept `capture_received` eenmaal vóór json/decode aan.
- `ThinProviderRuntime` krijgt optionele `archive`/contextfactory; `_one` maakt per retryattempt een immutable requestscope. Breid `ProviderResult` met defaults `raw_capture_ids: tuple[str,...]=()` en `archive_reason_codes: tuple[str,...]=()` uit; bestaande fixtures blijven geldig.
- `get_json` blijft wrapper rond één get_bytes capture. `post_json` archiveert alleen expliciet toegelaten weerresponses; Netatmo/MeteofranceOAuth nooit. Bestaande statuscodes/timeouts/retry/cadans behouden.

- [ ] Schrijf `test_json_capture_precedes_decoder_failure`: herkenbare meteorologische envelope met ongeldige verdere JSON exact aanwezig hoewel provider error; geen success claim. `test_unexpected_login_body_at_weather_endpoint_is_excluded`: JSONauthvelden/HTMLlogin geen rawpayload, expliciete reden. `test_json_wrapper_captures_once`: één HTTPrequest/receipt. `test_two_regions_same_provider_have_distinct_request_context`: respecteer bestaande providerlock maar context/finalisering per poging. `test_cancel_after_read_retains_receipt_or_explicit_gap`: geen ontvangstverlies in finally. `test_oauth_post_and_redirect_to_auth_never_capture_tokens`: geen payload, geen secretadres in output.
- [ ] Parametriseer `test_every_async_adapter_transport_has_explicit_capture_policy` over werkelijk `_build_provider_adapters`/catalogus: KMI APPV4/AWS/RADQPE, OPERA, KNMI, DWD, OpenMeteo/Icon2i, METAR, Netatmo weather, Wunderground weather, Meteolux, GeoSphere, ItaliaMeteo catalogus, AEMET, Meteofrance, DPC, MetOffice, RainViewer. Weersource/search/listing/metadata versus auth per route expliciet; blocked/geografisch niet-toepasselijk hoeft geen ontvangst te verzinnen. Fake transports, geen live polling in tests.
- [ ] Run `.venv/bin/python -m pytest tests/test_thin_raw_http_capture.py tests/test_thin_provider_runtime.py -k 'capture or archive' -q`; eerst rood.
- [ ] Implementeer hooks en safe routepolicies. Partial/oversized/HTTPfout registreert veilige status/gap, geen willekeurige fout/loginbody. Decoderafwijzing behoudt ontvangen toegelaten bytes zonder providerresultaat te forceren.
- [ ] Run dezelfde selectie; groen en exacte oude fetch/retry aantallen gelijk.
- [ ] Commit alleen eigen hunks: `feat: archive existing HTTP weather input before decoding`.

### Task 4: Synchrone satellietfetchers, FTP en regionale blikseminvoer

**Files:** Modify `storm_tracker_worker/thin_satellite_adapters.py`, `satellite_radar.py`, `lightning.py`, `provider_radar.py` (alleen gedeeld downloadpad), `noaa_err_native.py`, `thin_blitzortung.py`; test `tests/test_thin_raw_native_transport.py`.

**Interfaces:** Task1 `capture_scope/capture_received` werkt via ContextVar in bestaande to_thread jobs; transporthooks zijn inert zonder actieve thin capturecontext. Geen nieuwe fetchroute of mondiale feed.
- NOAA ERR: capture `_fetch_s3` ontvangen body vóór identityinspectie, listings/object met eigen identity; geen request-IDheaders opslaan.
- GOES RRQPE/GLM: capture bestaande `_bounded_url`/`_download_with_headers` vóór decode; dedup wrappers.
- H SAF: capture geassembleerde FTPdownloadbytes vóór `_hsaf_parse`, `transport='ftp'`, safe station/productpath, zonder login/sessiondetails. Cache hit verwijst naar originele capture, niet een nieuwe fictieve ontvangst.
- Eumetsat LI: `_bounded_request` weather search/product opnemen, tokenresponse uitsluiten.
- `ThinBlitzortungFeed._on_message`: vóór adapter.feed_message capture topic/bytes/receivedtime met snapshot van toepasselijke regionale subscriptions/targets, `transport='mqtt'`. Geen uitbreiding geohashes/subscriptions. Malformed geschikte eventbody behouden als rejected input.

- [ ] Schrijf parametrisch `test_native_input_is_saved_before_decode` voor alle genoemde paden; assert bytegelijkheid en oorspronkelijke tijd, zelfs parsefout. `test_ftp_and_mqtt_use_actual_source_scope`: twee scopes/threads geen kruisbesmetting, alleen bestaande subscriptions. `test_satellite_oauth_is_excluded`: token nooit opgeslagen. `test_native_cache_does_not_invent_new_receipt`: oorspronkelijke receipt behouden of expliciet cache_origin_missing als worker oude ongearchiveerde cache gebruikt.
- [ ] Run `.venv/bin/python -m pytest tests/test_thin_raw_native_transport.py -q`; eerst rood.
- [ ] Implementeer uitsluitend hooks/optionele contextoverdracht. Oude onafhankelijke native-capture-/artifactroutes niet activeren, aanpassen of opruimen. Zonder toegang/dekking blijft qualified=false.
- [ ] Run dezelfde selectie plus alleen direct geraakte thin_satellite/blitzortung regressies; groen, identieke fetchcount/catalogus/selectie.
- [ ] Commit alleen eigen hunks: `feat: capture native satellite and regional lightning input`.

### Task 5: Exact gebruikte velden, causale bindings en afzonderlijke opslagstatus

**Files:** Modify `storm_tracker_worker/thin_operational_runtime.py:785-1118`, `thin_regional_radar_runtime.py`, `thin_mqtt_service.py:432-550,715-765`, `thin_postgis.py`; tests `tests/test_thin_raw_cycle_bindings.py` en gerichte nieuwe tests `test_thin_mqtt_service.py`.

**Interfaces:**
- `ThinCycleResult` krijgt optionele `raw_archive_status: Mapping[str,Any]` als aparte waarde, bestaande exact-key snapshot ongewijzigd.
- Runtime bewaart `used_fields_by_region`: EXACT regional.field/nativefallback dat naar track_field/nowcast/map gaat, óók bij trackingafwijzing/motion0. Confidence/contributor_mask uit het echte completed fusionresultaat bewaren. Bewaar native inputs daarnaast, geen recompute ander veld voor opslag.
- `snapshot_field_job(field: ThinRadarField|ThinVisualRadarField, *, region_revision_id: str, confidence: ndarray|None, contributor_mask: ndarray|None, source_capture_ids: tuple[str,...]) -> RawFieldJob` kopieert arrays verliesloos via bestaande `asyncio.to_thread` vóór verbruik/later cachegebruik; geen mutation van originele arrayflags of algoritmeinput. Hash/compressie blijft in task2 writer.
- `RawArchiveBatch` uit task2 krijgt cycle_id/revisie, oorspronkelijke ontvangstrefs, gebruikte fieldIDs, bestaande daadwerkelijke forecast_issue_id's en effectieve niet-geheime policy/code-artifactdigest. Bind ook lege forecastissues/ontbrekende passages als cycleuitkomst; geen hindsight wissen.
- `raw_archive_status` schema_version1 bevat evaluated_at, status(current/degraded/disabled), current_cycle_id, native/field counts+bytes, queued_cycles, queued_bytes, dropped_count, reason_codes, qualified_provider_ids en explicit unqualified_provider_ids. Geen grote arrays of bronpayloads via MQTT.
- MQTTtopic exact `storm_tracker/v1/operational/raw_archive_status`, retain/qos1 via bestaande cyclepublicatie. Gecombineerde `postgis.status` alleen current wanneer originele writes EN huidige benodigde rawbindings bevestigd; veilige rawfailure degraded maar snapshot blijft gepubliceerd.

- [ ] Schrijf `test_binding_uses_consumed_field_not_later_async_completion`: beginfusionA, completionB na nowcast mag niet als gebruikte A worden gebonden. `test_field_snapshot_survives_later_source_array_mutation`: opgeslagen waarden/maskers exact de gebruikte eerdere input. `test_native_fallback_and_empty_motion_are_archived`: fieldbytes/masks aanwezig ondanks geen passage. `test_same_field_cached_cycle_new_binding_old_receipt`: één field/newcycles, receipt tijd onveranderd. `test_issue_and_empty_cycle_preserve_versions_and_original_position`: causale targets/revisies/beleid, alle uitgiften retained. `test_code_reference_requires_actual_source_and_archive_registration`: mismatched/missing registratie nooit complete.
- [ ] Schrijf `test_db_failure_and_buffer_overflow_publish_weather_with_degraded_archive`: MQTT werkt, geen providerrefetch, geen current/completenessclaim. `test_archive_topic_does_not_extend_snapshot_or_leak_payloads`: exact snapshotkeys blijven identiek, body bounded/safe. `test_causal_read_excludes_later_receipt_of_older_product`: productvalid eerder maar received later niet beschikbaar vóór receipt.
- [ ] Run `.venv/bin/python -m pytest tests/test_thin_raw_cycle_bindings.py tests/test_thin_mqtt_service.py -k 'archive or raw_binding' -q`; eerst rood.
- [ ] Implementeer orchestration. Bytes-/arraycompressie en SQL alleen in bestaande begrensde off-loop uitvoering; bestaande writerlock voorkomt concurrent DBwrites. Stop/shutdown maakt nog pending invoer expliciet onvolledig; geen false durable acknowledgment.
- [ ] Run dezelfde selectie en kleine legacy snapshot/provider-isolation regressies op gewijzigde callsites; groen. Geen volledige tracking-/replay-/historische suite.
- [ ] Commit eigen hunks: `feat: bind forecasts to exact archived inputs and expose raw status`.

### Task 6: NAS-deploy en echte read-back op gewone cadans

**Files:** Modify alleen geraakte versie/buildmarkers, `CHANGELOG.md`; create `docs/DEPLOY_2026-10-07_RAW_STORAGE_RESTORATION.md`; update `.analysis/live-quality-20261007.json` compact. Geen andere open fixes bundelen.

**Interfaces:** bestaande `scripts/build_release_candidate.py` en `scripts/install_ha_release_pair_ssh.py`; bestaande NAS MCP lifecycle; task2 migratie/readback via reeds bevoegde PostGISverbinding. Geen nieuwe sleutels/DBcredentials/managementroute.

- [ ] Bewijs de gerichte task1–5 selecties groen, noteer aantallen/tijd. Geen opslagherstelclaim op mocks; versie pas kiezen na read-only live versiecontrole, integreer uitsluitend eigen wijzigingen boven exacte104artifact. Stage buiten dirty checkout zodat unrelated edits niet worden meegedeployed.
- [ ] Lees runbook en actuele live status. Bewijs recente volledige backup en gerichte workerrollback, archivehashes/structuur/candidateevidence, huidige DBbehoud/ruimte en DBroleprivileges. Voer0009 alleen met bestaande bevoegde toegang uit. Ontbreekt DDLpermission: stop migratie en vraag bestaande DBbeheerder om0009, geen automatische nieuwe rechten/credentials. Additieve rollback laat nieuwe data/tabellen intact.
- [ ] Bouw met bestaande builder (`--integration-version` huidige1.14.156, nieuwe gecontroleerde workerversie), transfer naar `/backup/storm_tracker_deploy_runs`; registreer geverifieerde code/artefactdigests in task2 coderevisiontable. Unload integratie/stop alleen worker via NAS MCP, installer gevolgd door officiële Store reload/update/start. Verse workerheartbeat plus job_admission_open=true vóór integrationload. Geen Coreherstart; bij onverwachte noodzaak terugkoppelen, niet uitvoeren.
- [ ] Verifieer twee normale unieke Belgische radarproducten plus één normale groundrefresh; geen versnelde fetch. Lees echte bronpayload en alle opgeslagen gebruikte arrays terug: SHA/bytecount/array-byteorder/shape/mask/CRS/affine/quantity/times/sourcecontributions gelijk; issue/cycle verwijzen naar het werkelijk gebruikte veld. Observeer toepasselijke satelliet-/bliksemontvangst op normale cadans; niet-toepasselijke of nog niet ontvangen providers expliciet ongekwalificeerd, geen wereldroute/trackermove.
- [ ] Bevestig native bytes/fieldrows groeien, gecombineerde opslag current zonder queue/drop/gap voor getoetste cycli, workeronline/failures0 en Home/persoonlijke regiobindings behouden. Behoud historisch gat vóór activation; oude beschikbare captures niet voorstellen als complete passage. Bewaar/herstel `/homeassistant/mqtt_dump.txt` na elk observatiedump uit bestaande baseline. Sla compacte bewijsbestanden op, geen volledige GeoJSON/bronbytes in chat.
- [ ] Documenteer release/rollback/groeicijfers/gekwalificeerde bronnen/uitzonderingen en echte readback. Meld opslag hersteld alleen voor bewezen routes; bij mislukte acceptatie volg runbookrollback zonder DBwissen. Geen volledig replayrunner-, algoritmekalibratie- of90%-nauwkeurigheidsclaim.

## Zelfreview en overdracht

Specdekking: native HTTP/FTP/MQTT vóór decode taken3/4; privacy task1/3/4; exacte velden/bronbijdragen task2/5; additieve storage/retry/retentie task2; status/cycle/issue/causale ontvangst task5; gewone-cadans echte readback task6. Review Focus alle vijf met expliciete tests toegewezen. De plan-API's zijn nieuwe functies; bestaande ThinRadarField/ProviderResult/writer/ThinCycleResult blijven compatibel via defaults/keyword-only uitbreidingen.

Taak1 capturefundament geïmplementeerd/gecommit in geïsoleerde worktree, zeven gerichte tests groen; nog geen opslagdeploy. Uitvoering is goedgekeurd en blijft native in deze chat, zonder subagents. Regenopvolging is op gebruikersverzoek gestopt; fictieve tracker blijft op gebruikerspositie Groningen. De maandretentie is een expliciete scopewijziging door de gebruiker. Afzonderlijke episode-/raster-/timingsontwerpen zijn met dit opslagakkoord niet automatisch goedgekeurd.
