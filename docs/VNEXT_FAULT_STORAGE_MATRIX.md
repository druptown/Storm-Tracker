# Storm Tracker vNext — fail-closed fault- en opslagmatrix

Status: lokale shadow-kern, 2026-07-30. Deze matrix voert geen live
Home Assistant-wijzigingen, providerrequests, deploys, deletes of archivering
uit. Elke foutinjectie gebruikt een tijdelijke SQLite-database of een zuivere
plannerinput.

## Release-invarianten

1. Een onbekend, oud, onvolledig of corrupt schema wordt nooit automatisch
   geadopteerd, gemigreerd of gerepareerd.
2. Een afgewezen schema en corrupt incidentbewijs blijven byte-ongewijzigd.
3. Eén upstream revision, observatiebatch of workerjob heeft één stabiele
   identiteit. Een identieke replay is idempotent; andere bytes onder dezelfde
   identiteit zijn een conflict.
4. Een transactie is na een crash volledig afwezig of volledig duurzaam. Een
   half receipt, een half blob of een hybride trackinghead is verboden.
   Een multi-artifactset bevat na herstel alle blobs, memberreceipts,
   setmetadata en memberships of helemaal niets.
5. MQTT-status mag alleen vooruitgaan: `queued -> running -> terminal`.
   Duplicaten en vertraagde niet-terminale statussen mogen de duurzame receipt
   niet terugzetten. Een terminale status is definitief.
6. Ontbrekende MQTT-bevestiging wordt nooit als succes behandeld: de
   Home Assistant-brug publiceert exact één identieke QoS1-retry, eindigt met
   een expliciete timeout en ruimt de pending future op.
7. Startup-admission blijft dicht tot elke bestaande regio hersteld is én de
   duurzame integriteitscontrole slaagt.
8. Capaciteitsdruk verwijdert nooit data. De planner retourneert uitsluitend
   een auditeerbare `HOLD`, `REJECT_CAPACITY` of `ARCHIVE_REQUIRED`; alleen een
   latere, expliciete executor mag geverifieerd archiefmateriaal lokaal
   verwijderen.

## Deterministische matrix

| ID | Laag | Geïnjecteerde fout/volgorde | Vereist resultaat | Uitvoerbare dekking |
|---|---|---|---|---|
| SCH-01 | Evidence | bestaand schema v1 | initialize faalt; database byte-ongewijzigd | `test_existing_v1_like_database_fails_closed_without_mutation` |
| SCH-02 | Evidence | toekomstige schemaversie | initialize faalt; database byte-ongewijzigd | `test_unknown_schema_version_is_rejected_without_repair` |
| SCH-03 | Evidence | willekeurige bestaande HA-tabel | geen adoptie/tabellen erbij; byte-ongewijzigd | `test_unknown_existing_database_is_not_adopted` |
| SCH-04 | Evidence | niet-SQLite bytes | expliciete schemafout; originele bytes blijven behouden | `test_corrupt_database_fails_closed` |
| SCH-05 | Worker receipt | toekomstig schema of unversioned legacy-tabel | startup faalt vóór WAL-mutatie; bytes blijven behouden | `test_worker_receipt_store_rejects_unknown_schema_version`, `test_worker_receipt_store_rejects_unversioned_legacy_table` |
| SCH-06 | Worker receipt | niet-SQLite bytes | startup faalt; incidentbytes blijven behouden | `test_worker_receipt_store_rejects_corrupt_sqlite_without_mutation` |
| EVD-01 | Evidence | raw blobbytes gewijzigd | integriteitsrapport ongezond; replay weigert verkeerde SHA-256 | `test_integrity_audit_reports_exact_byte_corruption`, `test_semantic_payload_corruption_is_detected_before_replay` |
| EVD-02 | Evidence | source-revision-event verwijderd | provenance mismatch; integriteitsrapport ongezond | `test_integrity_audit_detects_missing_source_revision_event` |
| EVD-03 | Evidence | parent receipt ontbreekt | foreign-keyfout; geen orphanrecord toegevoegd | `test_missing_parent_foreign_key_fails_closed` |
| EVD-04 | Evidence | ground-decision-member verwijderd | bewijsset en forecastketen worden ongezond gemarkeerd | `test_integrity_audit_detects_missing_ground_decision_member` |
| RAW-01 | Raw artifact | identieke upstream redelivery | één receipt en één blob | `test_identical_upstream_receipt_is_idempotent`, `test_concurrent_redelivery_has_one_blob_and_one_receipt` |
| RAW-02 | Raw artifact | andere bytes onder bestaande upstream-ID | conflict; originele bytes en telling blijven behouden | `test_same_upstream_identity_with_other_bytes_fails_closed` |
| RAW-03 | Raw artifact | abrupt exit op elk transactioneel checkpoint | nul records of één compleet artifact; nooit half bewijs | `test_abrupt_process_exit_yields_nothing_or_complete_artifact` |
| RAW-04 | Raw artifact set | abrupt exit na begin/eerste member/alle members/setrij/memberships/vóór of na commit | nul blobs/receipts/sets of één volledig replaybare set; nooit gedeeltelijke membership | `test_abrupt_exit_never_exposes_a_partial_artifact_set` |
| RAW-05 | Raw artifact set | identieke, omgekeerd geordende of concurrerende redelivery | canonieke ordering; één set en immutable memberreceipts | `test_artifact_set_redelivery_is_order_independent_and_idempotent`, `test_concurrent_artifact_set_redelivery_commits_one_complete_set` |
| RAW-06 | Raw artifact set | gewijzigde bytes, rol, provenance of reeds conflicterend member | conflict en volledige rollback; originele feiten blijven replaybaar | `test_artifact_set_identity_conflict_preserves_original_set`, `test_member_conflict_rolls_back_every_new_member_and_set_row` |
| RAW-07 | Raw artifact set | geldige SQLite maar gewijzigde membershipmetadata | setintegriteit ongezond; replay en volgende initialize falen gesloten | `test_artifact_set_metadata_corruption_fails_closed_before_replay` |
| RAW-08 | Raw artifact receipt/set | ontvangst-/parser-/setmetadata gewijzigd zonder payloadwijziging | canonieke receiptchecksum faalt; replay en volgende initialize blijven dicht | `test_single_receipt_metadata_corruption_fails_closed`, `test_set_receipt_metadata_corruption_fails_closed` |
| CAP-01 | Retention | onbekende provider/licentie/attribution/quota | `HOLD` en `REVIEW_REQUIRED` | `test_unknown_provider_and_unknown_licence_fail_closed`, `test_attribution_and_provider_quota_are_explicit_admission_requirements` |
| CAP-02 | Retention | quota/reserve overschreden zonder veilige kandidaat | `REJECT_CAPACITY`; geen automatische delete | `test_archive_candidate_requires_every_safety_fact_and_reports_blocks`, `test_source_revisions_are_independently_counted_even_for_one_receipt` |
| CAP-03 | Retention | quota/reserve overschreden met geverifieerd archief | deterministisch `ARCHIVE_REQUIRED` met exacte kandidaten en bytes | `test_verified_archive_candidates_are_selected_deterministically`, `test_capacity_reserve_can_require_archive_and_never_auto_deletes` |
| CAP-04 | Retention | legal hold botst met capaciteit | `HOLD`, nooit stil verwerpen of verwijderen | `test_legal_hold_capacity_pressure_is_review_not_silent_rejection` |
| MQTT-01 | HA bridge | geen statusreceipt | één byte-identieke retry, expliciete `TimeoutError`, pending map leeg | `test_missing_worker_receipt_times_out_after_one_identical_retry` |
| MQTT-02 | HA bridge | terminal eerst, daarna queued/running/duplicate terminal | eerste terminale resultaat exact één keer; late status genegeerd | `test_duplicate_and_out_of_order_mqtt_statuses_complete_once` |
| MQTT-03 | Worker journal | queued-duplicate en running-daarna-queued | durable receipt blijft monotone en byte-inhoudelijk op nieuwste status | `test_worker_receipts_ignore_duplicate_and_out_of_order_transitions` |
| MQTT-04 | Worker | QoS1-redelivery na completed | terminal resultaat replay; handler niet opnieuw uitgevoerd | `test_completed_qos_redelivery_replays_result_without_reexecution`, `test_terminal_result_survives_worker_service_restart` |
| MQTT-05 | Worker | zelfde job-ID met andere payload | `idempotency_conflict`; geen tweede uitvoering | `test_durable_receipt_rejects_same_job_id_with_different_payload` |
| START-01 | HA | input tijdens durable recovery | alle sync/async/direct routes leveren nul af | `test_global_admission_rejects_direct_route_until_recovery_finishes` |
| START-02 | HA | integrity-check faalt of één regio is niet hersteld | admission blijft dicht | `test_integrity_failure_keeps_global_admission_closed`, `test_global_admission_requires_every_existing_region_recovered` |
| CRASH-01 | Durable ingest | exception of abrupt exit vóór/na receive-commit | observatie volledig afwezig of volledig herstelbaar | `test_receive_fault_is_either_fully_absent_or_fully_durable`, `test_abrupt_process_exit_during_receive_is_atomic` |
| CRASH-02 | Durable tracking | exception of abrupt exit op commit-checkpoints | staged+revision 0 of committed+revision 1; geen hybride staat | `test_commit_fault_never_creates_hybrid_state`, `test_abrupt_process_exit_during_commit_is_atomic` |
| CRASH-03 | Durable tracking | DB-commit vóór in-memory install, daarna restart | exact committed state/revision hersteld; replay is no-op | `test_crash_after_db_commit_before_memory_install_recovers_exact_state` |
| CRASH-04 | Worker | restart met accepted queued/running job | orphan wordt duurzaam `cancelled`; redelivery rekent niet opnieuw | `test_worker_restart_terminalizes_durable_accepted_backlog` |
| CRASH-05 | Worker process | actieve child crasht met volgende job in queue | één terminale failure; volgende job voltooit; queue niet vergiftigd | `test_child_crash_fails_one_job_and_does_not_poison_queue` |

## Lokale matrixrun

Gebruik een nieuwe `--basetemp` per run; hergebruik geen oude pytest-map:

```powershell
$env:PYTHONPATH='.testdeps'
python -m pytest `
  tests/test_meteorological_evidence_store.py `
  tests/test_raw_artifact_store.py `
  tests/test_artifact_capture_admission.py `
  tests/test_artifact_retention_planner.py `
  tests/test_durable_processing.py `
  tests/test_worker_bridge.py `
  tests/test_worker_service.py `
  tests/test_region_manager.py `
  -q -W error --basetemp .test-tmp-vnext-fault-storage-UNIEK
```

## Nog niet bewezen

- Echte `ENOSPC`, filesystem-I/O-fouten, een defect opslagmedium en
  stroomverlies tijdens fysieke `fsync` zijn nog niet op een geïsoleerd
  bestandssysteem geïnjecteerd.
- De retentionplanner is shadow-only. Er bestaat nog geen archive-executor;
  daardoor is een echte upload, checksum-herlezing en gecontroleerde lokale
  verwijdering bewust niet mogelijk.
- Brokerdisconnects, reconnects en packet-loss zijn met fakes getest, niet met
  een afzonderlijke Mosquitto fault-proxy.
- SQLite WAL/SHM-sidecarcorruptie en herstel na een volle WAL zijn nog geen
  afzonderlijke faultcases.
- Oude evidence-schema's worden bewust niet gemigreerd. Er moet eerst een
  offline, checksum-behoudende migrator met rollbackbewijs komen.
- Geen van deze lokale tests vervangt de geplande NUC-soak en zeven dagen live
  shadow-observatie.
