# Storm Tracker V3 — post-RC1-validatiestatus vanaf 2026-08-11

Status: aanvullende lokale validatie na de bevroren RC1; niet gedeployed en
niet operationeel geactiveerd.

Dit supplement vervangt of wijzigt de bevroren
[RC1-uitvoerstatus](ROADMAP_EXECUTION_STATUS_2026-08-11.md) en de
[lokale releasekandidaat](LOCAL_RELEASE_CANDIDATE_1.13.0_WORKER_1.16.0.md) niet.
De RC1-artefactbytes blijven ongewijzigd en `deployment=false` blijft gelden.

## Onveranderlijke RC1-grens

| Bewijs | Bevroren waarde |
|---|---:|
| Passed | 4,934 |
| Verklaarde skips | 15 |
| Collected nodes | 4,949 |
| Failures/errors/warnings | 0/0/0 |
| Deployment | `false` |

De som blijft exact `4,934 + 15 = 4,949`. Geen post-RC1-test, simulatorrun of
red-teamcontrole wordt aan deze inventory toegevoegd of gebruikt om de RC1-
artefacten achteraf een ruimere vrijgaveclaim te geven.

## Native vijfrollen-APPLY-crashmatrix

De aanvullende real-processmatrix gebruikt echte `os._exit`-afbreking op de
native vijfrollen-APPLY-grenzen en is volledig finaal uitgevoerd.

| Onderdeel | Finale uitkomst |
|---|---:|
| Native vijfrollen-APPLY-crashmatrix | 36/36 passed in `785.69s` |
| Aangrenzende native vijfrollenintegratie | 2/2 passed in `31.29s` |
| Warnings | 0 in beide runs |

Test-only provenance:

| Bestand | SHA-256 |
|---|---|
| [worker_reclamation_crash_child.py](../tests/helpers/worker_reclamation_crash_child.py) | `70b25aec5061ce2cc4af62e791e30a44e3ccf244b0acf6d628bd1bf173cd6e64` |
| [test_worker_acquisition_source_reclamation_native_five_role_crash.py](../tests/test_worker_acquisition_source_reclamation_native_five_role_crash.py) | `38efe26ae0afacb0e33b7d245e228a156df0446c60b86c69ea41e049274ddb7b` |

Deze uitslag bewijst uitsluitend de geteste lokale APPLY-crashgrenzen. De
aanvullende reclamation-, provider- en completionresultaten hieronder zijn
eveneens post-RC1-bewijs en veranderen de bevroren inventory niet.

## Native reclamation-PREPARE/COMMIT/ABORT en APPLIED-reproof

De exact gepinde reclamationmatrix is in zeven onderling disjuncte seriële
shards uitgevoerd. Samen zijn dat exact 24 PREPARE-, 2 COMMIT- en 24
ABORT-cases.

| Onderdeel | Finale uitkomst |
|---|---:|
| PREPARE | 24/24 passed |
| COMMIT | 2/2 passed |
| ABORT | 24/24 passed |
| Exacte totaalset | 50/50 passed in `1353.55s` |
| APPLIED replay-/reproofmatrix | 7/7 passed in `200.51s` |

De zeven shardtijden waren achtereenvolgens `234.42s`, `239.84s`, `219.51s`,
`179.46s`, `230.79s`, `191.28s` en `58.25s`, met respectievelijk
`8 + 8 + 8 + 8 + 8 + 8 + 2 = 50` cases. Iedere case beëindigt een echt lokaal
childproces via `os._exit(73)`, opent de duurzame authorities opnieuw en
bewijst uitsluitend de vastgelegde herstel-, replay- en fence-invarianten.

Test-only provenance:

| Bestand | SHA-256 |
|---|---|
| [worker_reclamation_prepare_abort_crash_child.py](../tests/helpers/worker_reclamation_prepare_abort_crash_child.py) | `11ea107d8b84de6b48e7c9dc046800815f90de37da40faf8c9e0222961a4a2ce` |
| [test_worker_acquisition_source_reclamation_native_prepare_abort_crash.py](../tests/test_worker_acquisition_source_reclamation_native_prepare_abort_crash.py) | `fdf4a8a34e8e16099155ee495c8c6b4f9b0f47ffe7156db257b1c91f9ac97c96` |
| [worker_reclamation_applied_reproof_crash_child.py](../tests/helpers/worker_reclamation_applied_reproof_crash_child.py) | `405cda8a29a7185082787c503da1b14876ac0f9d7ca2a74073ce59023f3582b5` |
| [test_worker_acquisition_source_reclamation_native_applied_reproof_crash.py](../tests/test_worker_acquisition_source_reclamation_native_applied_reproof_crash.py) | `c1d46c40444917c778de432e656568bd580177d31be4019deb145731c07b8f82` |

Dit is een lokale logische procescrashmatrix. Zij simuleert geen power-loss,
directorydurability of fysiek verlies van een reeds geretourneerde `fsync`.

## Native provider- en capturecrashmatrices

De volgende hermetische provider-/capturegrenzen zijn aanvullend met echte
abrupte childprocessen uitgevoerd. Er was geen netwerk- of livepad nodig.

| Groep | Gerichte matrix | Aangrenzende regressie |
|---|---:|---:|
| AEMET | 5/5 passed in `8.85s` | — |
| AWN + Open-Meteo | 6/6 passed in `10.32s` | 61/61 passed in `24.91s` |
| HSAF + Météo-France | 8/8 passed | 108 passed, 1 verklaarde live skip |
| DWD-capturehandoff + acquisition recovery | 7/7 passed | 39/39 passed |

De AEMET-set omvat drie opslaggrenzen en twee capturecommitgrenzen. De overige
groepen pinnen per provider de duurzame receipt-/capturetoestand vóór en na de
relevante commit, gevolgd door reopen, recovery en idempotente replay zonder
ongeoorloofde refetch of tellinggroei. De verklaarde live skip is geen pass en
levert geen livebewijs.

| Harnas/test | SHA-256 |
|---|---|
| [worker_capture_crash_child.py](../tests/helpers/worker_capture_crash_child.py) | `9317c9fef5e4f4b3b0befa9fb88e3d7e984331e78c1614d608cb77afa289ee9e` |
| [test_worker_aemet_native_capture.py](../tests/test_worker_aemet_native_capture.py) | `af55e85dc5af97c029ba69f9524d67ac26f409be4c60504dd5ceb3f5cc0099b9` |
| [awn_openmeteo_native_crash_child.py](../tests/helpers/awn_openmeteo_native_crash_child.py) | `637ef9e51f8468cb544a017edd276e3c9866ba85c5057053a91adec8e7dd7444` |
| [test_worker_awn_native_capture.py](../tests/test_worker_awn_native_capture.py) | `3970d52975a60cba4ee87a67b89c00557ebb1251334099cec6dcd44b86267550` |
| [test_worker_openmeteo_native_capture.py](../tests/test_worker_openmeteo_native_capture.py) | `25e71da8b3c49f6a6945778cc824be5d5c84a8e39dc79c87f993c53987d4917d` |
| [hsaf_meteofrance_os_exit73_child.py](../tests/helpers/hsaf_meteofrance_os_exit73_child.py) | `8a7ae1c79c3bb6db6f0d5e3997e1c436615b823f426ba655e461dfd083bcaeee` |
| [test_worker_hsaf_h40b_native_capture.py](../tests/test_worker_hsaf_h40b_native_capture.py) | `67e68c220947cbea2bc8b6a3455a3de4f460860453a12d92d4cbe732ed2d628c` |
| [test_worker_meteofrance_native_capture.py](../tests/test_worker_meteofrance_native_capture.py) | `587cdbc8ea1500e53f2e4947b8c8620b96a7ac829ec14a8677dead1d7ab0fa75` |
| [provider_crash_durable_bridge_child.py](../tests/helpers/provider_crash_durable_bridge_child.py) | `65870690d475f9cd0d582100dfe1aa65804b31ab5f53a201bb416691f3c82a4b` |
| [test_worker_dwd_rv_capture_handoff.py](../tests/test_worker_dwd_rv_capture_handoff.py) | `405064d2c1f6bbc1e52796573305ae59ae0d01d98ea077963e14e06e00fc0b7e` |
| [test_worker_acquisition_capture_recovery.py](../tests/test_worker_acquisition_capture_recovery.py) | `992d9871dcfadbe77830ebd623cba1dae992ceb04da60fedce800c3a197621cf` |

## Native completion- en routercrashmatrices

De lokaal uitgevoerde completiongroepen leveren samen 87/87 groene
faultpointcases:

| Groep | Finale uitkomst |
|---|---:|
| Completion-archivekern | 6/6 passed |
| Seriële dispatcher + gesloten-rootafwijzing | 14/14 + 3/3 = 17/17 passed |
| DWD + KNMI completionconsumers | 23/23 passed |
| METAR + Met Office completionconsumers | 20/20 passed |
| NOAA completionconsumer | 12/12 passed |
| Raw-manifest-auditconsumer | 9/9 passed |

Een onafhankelijke statische completionaudit vond nul P0-gaten. Alle gevonden
productiefaultcalls worden door de gepinde matrices afgedekt, inclusief de
archivecommitgrens die in een echt subprocess via `os._exit(91)` afbreekt.
Daarmee zijn de geïnventariseerde lokale completionfaultpoints gedekt; dit is
geen bewijs voor nog niet geïnstrumenteerde integratiegrenzen of externe
storagedurability.

| Harnas/test | SHA-256 |
|---|---|
| [completion_core_os_exit73_child.py](../tests/helpers/completion_core_os_exit73_child.py) | `af40d7447ec0ce0ee59b0c60005d2e3a955132da50d1d65e0a8fa0dcc81b0bfc` |
| [test_worker_completion_output_archive.py](../tests/test_worker_completion_output_archive.py) | `928974695869ed342912b14707616911b6ff22e1cbe42b5a49f88c56fccb9f9e` |
| [worker_serial_completion_dispatcher_os_exit73_child.py](../tests/helpers/worker_serial_completion_dispatcher_os_exit73_child.py) | `a3ffd3aafaa6e8b0208ad3b48252410bb1829ec5bf0068c459ae68ba9d1dcfcf` |
| [test_worker_serial_completion_dispatcher.py](../tests/test_worker_serial_completion_dispatcher.py) | `f21cc7fab42e84bf9e49227545fc79aa9e9c471e3c1c723b6a9f1dc0869e8368` |
| [completion_consumer_os_exit73_child.py](../tests/helpers/completion_consumer_os_exit73_child.py) | `ea41df7e8148c702093efe1e4b2378a3f28003067728f9c89e4302a3a5dab517` |
| [test_worker_dwd_rv_completion_consumer.py](../tests/test_worker_dwd_rv_completion_consumer.py) | `3431d1fbf97aa6326bf00a27d5142eaba627ec474e4f43efb0348bf79f266684` |
| [test_worker_knmi_rtcor_completion_consumer.py](../tests/test_worker_knmi_rtcor_completion_consumer.py) | `9b9898eb984e63d66a9eb1b6198187fb419075b89a920fcfc2bdea39239f4fc9` |
| [completion_consumers_os_exit73_child.py](../tests/helpers/completion_consumers_os_exit73_child.py) | `7c39ecadd32f3de4318f9d424a8f6c80cb56f0ab9fec8b808044fb322c3b44a0` |
| [test_worker_metar_ground_completion_consumer.py](../tests/test_worker_metar_ground_completion_consumer.py) | `54668f25068793e78945ea4a998387c1b08cb6182f3b33c2c0c5d3d59dfa5cbc` |
| [test_worker_met_office_rate_completion_consumer.py](../tests/test_worker_met_office_rate_completion_consumer.py) | `06a2ef600d1cd9bbb09d6fd867fc942af353e9cb56fb6927d1206c9ece506aab` |
| [noaa_raw_manifest_completion_os_exit73_child.py](../tests/helpers/noaa_raw_manifest_completion_os_exit73_child.py) | `93ef04dce49907d2de5a44138dccf033ee32c2f52ff985e07f0f772c88efd076` |
| [test_worker_noaa_err_completion_consumer.py](../tests/test_worker_noaa_err_completion_consumer.py) | `bbe95ab47aef1da8d1a98da7daf45e8389cac63becc6748718038a0f4e461e7c` |
| [test_worker_raw_manifest_audit_completion_consumer.py](../tests/test_worker_raw_manifest_audit_completion_consumer.py) | `290d8472bbd17f218669d9ab862dad02d61b87312badfc2266893afcdce06dd7` |

## Grens van dit aanvullende faultbewijs

De roadmapcheckbox voor de volledige exception- en abrupte
subprocessfaultmatrix blijft uit. De nieuwe lokale gates sluiten de gepinde
reclamation-, provider/capture- en completion/routergrenzen, maar nog niet:

- resterende acquisitionpaden en integratie-lifecycle-/routinggrenzen zonder
  een complete checkpointnaad;
- niet-geïnventariseerde exceptionpaden na alle laatste wijzigingen;
- echte `fsync`-/power-loss-, disk-full-, corruptie- en doel-OS-gates;
- live provider-, broker-, MQTT-, Home Assistant- en langdurige soakgates.

## Lokale installatie- en rollbacksimulator

De historische lokale simulator-/securitymatrix verzamelde 193 tests:
190 passed, 3 verklaarde platformskips, 0 failures en 0 warnings in `296.91s`.
Een afzonderlijke finale red-teamcontrole rapporteerde `PASS`.

De toen daadwerkelijk uitgevoerde lokale CLI-simulatie voltooide en publiceerde
een rapport, maar bleef bewust fail-closed:

```json
{
  "simulation_completed": true,
  "local_simulation_passed": false,
  "real_installation_ready": false,
  "deployment_performed": false,
  "network_performed": false,
  "live_paths_touched": false
}
```

Het daar vastgelegde historische rapport bevat 8 lokale blockers, 13
Supervisor-blockers en 7 externe blockers. Het staat uitsluitend lokaal als
`.analysis/release-install-dry-run-20260811-post-rc1-final/dry_run_report.json`
met SHA-256
`4e6103f48149bcebcac2bc91ac6740a4b4123007586e88fbebea0c62a1328c29`.

Na dat historische rapport is Phase C in de huidige bron geïntegreerd. De
allowed root wordt vóór iedere mutatie met een absolute no-reparsebootstrap en
gepinde identiteit geopend; dezelfde lineage draagt de descriptor-relative
proceslease. Phase-A-outputmutaties, Phase-B-owned reads, no-replace-publicatie
met verwachte bronidentiteit en getypeerde postpublicatiecleanup delen die open
authority. Een nieuwe uitvoering moet daarnaast exact de acht benoemde
archive-inspectie-/validatie-/extractieroutes bewijzen; recovery eist expliciet
0/0 omdat zij geen archive opnieuw verwerkt.

Het actuele broncontract behoudt de twee lokale blockercodes als gesloten
universum, maar een volledig geslaagde lokale uitvoering leidt nu
`local_acceptance_blockers=[]`, beide codes onder
`local_acceptance_blockers_closed` en `local_simulation_passed=true` af. De
current-source-run van 2026-08-14 heeft dit inmiddels afzonderlijk en gehashd
bevestigd; het historische rapport en zijn hashes blijven ongewijzigd bewijs
van uitsluitend de eerdere run. De hostile same-user-race blijft extern en ook
met lokale sluiting blijven
`real_installation_ready=false`, deployment, netwerk, live doelinstallatie,
runtime stop/start, Supervisor-build/cache, power-loss en menselijke approval
onbewezen.

### Actuele Phase-C-bronvalidatie van 2026-08-13

Na de finale freeze rapporteerde de gerichte Phase-C-matrix 64 passed en 2
skips in `258.66s`, het volledige simulatorbestand 265 passed en 4 skips in
`754.49s`, en de gecombineerde afhankelijkhedenmatrix 125 passed en 3 skips in
`4.23s`. De afzonderlijke compilecontrole en `git diff --check` waren eveneens
groen. Dit zijn bron- en testmetingen op de hieronder gehashte huidige bytes;
zij publiceerden geen nieuwe uitgevoerde CLI-simulatie, geen nieuw
`dry_run_report.json` en geen deploy-, netwerk- of live-installatiebewijs.

| Huidig Phase-C-bestand | SHA-256 |
|---|---|
| [release_install_dry_run.py](../release_install_dry_run.py) | `c04c5b4a1325d69768688735e994e1978f5592f015eee30b298ad5112c872b19` |
| [test_release_install_dry_run.py](../tests/test_release_install_dry_run.py) | `146b41bfb5cc3c4b6aeaa3dd1e597389512a75e58b25e84df9d175f009a4fb25` |
| [dry_run_release_install.py](../scripts/dry_run_release_install.py) | `79912ef1717061618d9688b1fcab86d498e19b5dc73da25ec1d4238a10e6910b` |
| [descriptor_relative_authority.py](../descriptor_relative_authority.py) | `a621de8656355db573319c3105355996e462317e9a79a892d05704eeb3979858` |
| [release_install_output_authority.py](../release_install_output_authority.py) | `108396ac09d28adfbdf1e6764379e9722ccb3010ea27e1306162761e8518fe95` |

Deze actuele bronprovenance vervangt of herinterpreteert de historische
CLI-report-, testtelling- en hashsecties hieronder niet. Het historische rode
rapport blijft uitsluitend bewijs van die eerdere run. De daarvan gescheiden
current-source-run van 2026-08-14 staat hieronder.

### Current-source local-only kandidaat en CLI-run van 2026-08-14

Voor deze lokale Phase-C-controle is een volledige kandidaat uit de huidige
bron gebouwd en onafhankelijk gereproduceerd. Beide builds leverden exact
dezelfde drie bestanden. De interne versies `1.13.0` en `1.16.0` zijn alleen
voor deze lokale test hergebruikt; omdat oudere RC1-bytes dezelfde versies
dragen, zijn deze bytes **niet deploybaar of distribueerbaar**. Een echte
release vereist een nieuwe unieke versie- en/of buildidentiteit.

Primaire root:
`.analysis/release-candidates/20260814-i1.13.0-w1.16.0-phase-c-rc2`

Reproductieroot:
`.analysis/release-candidates/20260814-i1.13.0-w1.16.0-phase-c-rc2-repro`

| Current-source local-only artifact | Bytes | SHA-256 |
|---|---:|---|
| `storm_tracker_release_v1.13.0_worker_v1.16.0_evidence.json` | 44842 | `686879dbf9f40557e809545a9aa7e772b79e37cd504045ce4f04af0719ad1941` |
| `storm_tracker_v3_v1.13.0_deploy.zip` | 643073 | `b2dd45e1bcd21c345295703bd693552fdec224deb4860501974eadf267cf6c62` |
| `storm_tracker_worker_v1.16.0_deploy.tar.gz` | 1907437 | `211952159907f12dc5a2a6b367f93d88a8492e096343a73951954486943024d6` |

De daadwerkelijke evidencebestandhash is
`686879dbf9f40557e809545a9aa7e772b79e37cd504045ce4f04af0719ad1941`;
deze mag niet worden verward met een intern canoniek evidenceveld dat de
builder apart rapporteert.

De gebruikte rollbackbytes bleven ongewijzigd:

| Onaangeroerde rollbackbaseline | Bytes | SHA-256 |
|---|---:|---|
| `storm_tracker_v3_v1.11.0_deploy.zip` | 258113 | `50bc31f665eb8896fb5e2f75cfaa072d853942671f6d1d2728eb9ba813e1e260` |
| `storm_tracker_worker_v1.14.0_deploy.tar.gz` | 88983 | `b9f4736e056ad433e9086e47287515ce2ca5789d5f641b13d6d9abaa0b6a6b9f` |

De volledige gesanitiseerde optionssnapshot stond op
`.analysis/release-install-inputs-20260814/sanitized-options.json`, 742 bytes,
SHA-256
`cc050ca4e4b48e5bb0fbf6b9a37d3530db39a8311d96918f837414523fc0a931`.

De verse CLI-run voltooide met exitcode 0. Het rapport staat op
`.analysis/release-install-dry-run-20260814-phase-c-cli-2/dry_run_report.json`,
23175 bytes, SHA-256
`bdb75617a4f89c431de626398b831783fb6fd0dbe301a7829de5608386f55f1e`.

```json
{
  "simulation_completed": true,
  "local_simulation_passed": true,
  "local_acceptance_blockers": [],
  "local_acceptance_blockers_closed": [
    "ancestor_and_final_output_path_race_exclusion_not_proven",
    "descriptor_relative_output_authority_not_proven"
  ],
  "deployment_performed": false,
  "network_performed": false,
  "live_paths_touched": false,
  "real_installation_ready": false
}
```

De namespaces `local_simulation` en `supervisor_installation` zijn leeg. Exact
deze zeven externe gates blijven open:

- `actual_target_installation_not_exercised`
- `data_and_schema_rollback_not_validated`
- `hostile_same_user_output_races_not_excluded`
- `human_deploy_approval_not_granted`
- `runtime_service_stop_and_start_not_exercised`
- `supervisor_build_and_cache_not_exercised`
- `target_filesystem_power_loss_not_exercised`

Er is geen netwerk gebruikt, geen live pad aangeraakt en geen deployment
uitgevoerd. Daarom blijft `real_installation_ready=false`.

De kandidaatbouwer schrijft workerarchives alleen als ASCII USTAR en bewijst
root, afgeleide directories en bestanden met de echte USTAR name/prefix-encoder
tijdens broninventarisatie, vóór enige stagingwrite. PAX/GNU/ZIP64 blijven dus
gesloten aan zowel de schrijf- als leesgrens. De finale strict gecombineerde
header-/memberstream-/artefact-/simulatormatrix was **293 passed, 4 verklaarde
platformskips** in `464.42s`, met warnings als errors. Dit is post-RC1-bewijs;
het is historische pre-Phase-C-validatie en wijzigt geen RC1-byte,
deploystatus of live systeem.

De bij die historische validatie gehashte simulatorbronnen zijn:

Deze SHA-256-waarden horen bij de bytes op dat historische meetmoment. De links
wijzen naar de huidige werkboom en hoeven na Phase C dus niet meer met die oude
hashwaarden overeen te komen.

| Bestand | SHA-256 |
|---|---|
| [release_install_dry_run.py](../release_install_dry_run.py) | `0473b8b84a464f7e65eebc6dc73bf3d93ee6d14adbb3abf2dd08d30b4c757e13` |
| [test_release_install_dry_run.py](../tests/test_release_install_dry_run.py) | `c22ffbdd023c35087a17efccb6e95c682419ecc8debc5152a96149522c55c4e6` |
| [bounded_archive_headers.py](../storm_tracker_worker/bounded_archive_headers.py) | `187bd8547b538b834218e4ccb171c6e47892cccf6ce43f7ea1bf04c2d5a0adec` |
| [test_bounded_archive_headers.py](../tests/test_bounded_archive_headers.py) | `b48aab1e5b2ef7dd92766333a45099244e52ed0b8af7de493759e7a1cbcec116` |
| [release_artifacts.py](../storm_tracker_worker/release_artifacts.py) | `ed5623b0500df91dc973f08bf04aaf8e4de34b14023969944e4686d5149e7124` |
| [test_release_artifacts.py](../tests/test_release_artifacts.py) | `1c87ac7c92d8385ab2dbd69cf229c45211dfd4106ac42362413a7500a888fcb0` |
| [LOCAL_RELEASE_INSTALL_ROLLBACK_DRY_RUN.md](LOCAL_RELEASE_INSTALL_ROLLBACK_DRY_RUN.md) | `460c9b9917693b1c8941eb18fa17a83ed6bb770e4a2b35eeb15102493a7ec685` |

Het hierboven gehashte historische rapport blijft uitsluitend bewijs van de
eerdere fail-closed run. De afzonderlijk gehashte current-source-run van
2026-08-14 bewijst nu de lokale Phase-C-uitvoering, maar sluit geen externe
installatie- of vrijgavegate.

## Lokale DWD-only worker-first servicecanary

Na RC1 is een afzonderlijk, standaard uitgeschakeld servicecodepad lokaal
gebouwd en getest. Een ontbrekende modus of `legacy` behoudt exact de bestaande
workerroot. Alleen `worker_first_canary` mag de nieuwe root selecteren en vereist
voor enige store- of MQTT-constructie een canoniek configuratiepad, de exacte
config-SHA-256 en de bestaande authority-SHA-256. Er is geen fallback naar de
legacyroot na een geselecteerde canaryfout.

De canary laat exact `radar.dwd.radolan_rv` toe. Legacy handlers,
legacy-receiptstore, autonome legacy-scheduler en procesexecutor zijn in deze
modus wederzijds uitgesloten. `live_wiring=true` beschrijft alleen dat de lokale
servicecomposition intern bereikbaar is; het is geen productieclaim. De
diagnostiek blijft `production_enable_ready=false`,
`meteorological_admission=false`, science-shadow, factless en zonder source- of
automatic reclamation.

De eerste gerichte activeringsgate van deze werkronde was in één gezamenlijke
strict run 204/204 groen in `73.15s`:

| Gate | Uitkomst |
|---|---:|
| Uitvoerbare packaging/runtimegate | 12/12 passed |
| Canonieke activatieloader | 34/34 passed |
| Selector, echte constructor en echte authority/fence-root | 22/22 passed |
| Fail-closed start/tick/stop/hard-exit | 23/23 passed |
| Canarycomposition plus v1-contracten | 51/51 passed |
| Default-root unwired regressiewaarborgen | 7/7 passed |
| Aangrenzende legacyservice | 55/55 passed |

De echte lokale rootproef doorloopt loader, gepinde authority, DWD-only builder,
OS-fence, een hermetische MQTT-connectfout, begrensde stop en heracquire van
dezelfde fence. Een afzonderlijke Git-Bash-proef voert de exact uit `run.sh`
geëxtraheerde runtimegate uit en bereikt daarna `python -m
storm_tracker_worker.service` en de echte loader. Zij gebruikt geen netwerk en
stopt bewust vóór MQTT op een ontbrekende tijdelijke config.

Dit is nog geen gebouwde-addon-image-, broker-, provider-, Home Assistant- of
doel-OS-smoke. Er is niets gedeployed of operationeel geactiveerd; de bevroren
RC1-inventory hierboven wordt niet met deze 204 nodes verhoogd.

### Securityhardening van 2026-08-13

De eerdere 204/204-gate is daarna transparant gesupersedet door een strict
gecombineerde canarymatrix van **242/242 passed**. De beveiligingsvolgorde van
de v2-servicecanary is daarbij aangescherpt: na de read-only pin- en
authoritycontroles wordt eerst de niet-wachtende OS-fence verkregen en pas
daarna worden mutable stores geconstrueerd of geaudit. Een bezette fence geeft
daarom nul storeconstructies en nul storemutaties; iedere fout na acquire sluit
gedeeltelijke onderdelen en geeft de fence weer vrij.

Een MQTT-disconnect sluit voor de worker-first canary onmiddellijk readiness,
jobadmission en de actieve subscriptionauthority. Iedere succesvolle reconnect
krijgt een nieuwe transportgeneratie. Admission kan daarna uitsluitend opnieuw
openen op verse, exact geldige runtimediagnostiek waarvan het bewijs binnen die
nieuwe generatie begon en eindigde; een proof van vóór disconnect of over een
disconnect/reconnectgrens wordt verworpen.

De DWD hard-decoderchild erft geen ambient MQTT- of providercredentials. De
parent vernauwt de environment al aan de synchrone OS-spawngrens tot een vaste
technische allowlist en herstelt zijn eigen environment daarna exact. De child
herhaalt dezelfde scrub vóór targetimport of requestdecode. In
`worker_first_canary` verwijdert `run.sh` bovendien vóór `exec` alle niet-DWD-
providersecrets en het legacy receiptpad. De `legacy`-modus en de standaard
hard-decoderpolicy blijven inhoudelijk ongewijzigd.

De afzonderlijke strikte bewijslijnen zijn:

| Gate | Uitkomst |
|---|---:|
| Gecombineerde canary/securitymatrix | 242/242 passed |
| Acquisitionruntime | 93/93 passed |
| Definitieve volledige hard-decoderfile | 21/21 passed in `14.98s` |
| Gerichte OS-spawn-boundary | 1/1 passed |
| Packaging en `.dockerignore` | 28 passed, 1 verklaarde Windows-skip |

De eerdere bewijsstand bestond uit 20/20 voor de toen volledige
hard-decoderfile plus een afzonderlijke gerichte spawn-boundarycase van 1/1.
Daarna is de volledige hard-decoderfile, nu inclusief die regressie, opnieuw als
één matrix gedraaid en 21/21 groen bevonden in `14.98s`. Ook deze latere tests
worden niet bij de
bevroren RC1-inventory opgeteld en leveren geen deploy-, live-, broker-,
provider-, Home Assistant-, addon-image- of productieclaim.

## Gates die open blijven

De roadmapcheckbox voor “migratie-, deploy- en rollbackartefacten zijn gebouwd
en droog getest” blijft uit. De lokale simulator sluit geen van deze grenzen:

- echte Supervisor-options-export, -migratie en -restore;
- Supervisor-build- en imagecache;
- echte service-stop/start en statuscontrole;
- Home Assistant- en workerintegratie op een herstelbaar testdoel;
- data- en schemacompatibiliteit van echte rollback;
- doel-OS-filesystemownership, directorydurability en power-loss;
- live provider-, broker-, MQTT-, Home Assistant- en hostfaults;
- afzonderlijke menselijke deploygoedkeuring.

Er is geen host, adres, endpoint of oude doelomgeving genoemd, benaderd of
gebruikt. De post-RC1-validatie voerde geen netwerkactie uit en wijzigde geen
live pad.
