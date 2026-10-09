# P0-W01 — worker-effectcontract en hard-deadlinebewijs

Datum lokale verificatie: 2026-07-30

## Scope en conclusie

De productieworker heeft voor ieder aangeboden jobtype exact één immutable
`HandlerEffectContract`. Productie-admission weigert ontbrekende, onbewezen en
niet-hard-kill-veilige contracten. De enige niet-killbare precursor is Netatmo
OAuth-refresh/credentialrotatie; die gebeurt in de ouder vóór de job naar het
killbare child-proces gaat.

Dit is lokaal bewezen voor codevolgorde, fail-closed gedrag, secretredactie,
receiptbinding en deadlinebudget. Het is geen bewijs voor container-SIGKILL,
host-stroomverlies, echte providerstoringen of de duurzaamheid van het
onderliggende `/data`- en `/share`-medium.

## Productiemapping

| Jobtype | Effectklasse | Voorbereiding/compensatie |
|---|---|---|
| `ping` | pure/ephemeral compute | childstate weggooien; exact request herhalen |
| `dwd_radolan_fetch` | bounded GET + job-id atomic artifact replace | exact job-id herhalen |
| `odim_hdf5_fetch` | bounded GET + job-id atomic artifact replace | exact job-id herhalen |
| `opera_hdf5_fetch` | bounded GET + job-id atomic artifact replace | exact job-id herhalen |
| `motion_preflight` | pure/ephemeral compute | childstate weggooien; exact request herhalen |
| `tracking_operational` | pure/ephemeral compute | childstate weggooien; exact request herhalen |
| `virtual_probe_operational` | pure/ephemeral compute | childstate weggooien; exact request herhalen |
| `netatmo_fetch` | parent-committed OAuth + idempotente GET | rotatie eerst duurzaam opslaan; daarna query herhalen |
| `synoptic_fetch` | bounded idempotente GET | exact request herhalen |
| `metar_fetch` | bounded idempotente GET | exact request herhalen |
| `enterprise_rainrate_fetch` | bounded GET + job-id atomic artifact replace | exact job-id herhalen |
| `calibration_operational` | pure/ephemeral compute | childstate weggooien; exact request herhalen |
| `phase3_replay` | pure/ephemeral compute | childstate weggooien; exact request herhalen |
| `geojson_operational` | pure/ephemeral compute | childstate weggooien; exact request herhalen |
| `radar_frame_operational` | job-id atomic artifact replace | exact job-id herhalen |

`PRODUCTION_JOB_HANDLERS` en `PRODUCTION_JOB_EFFECT_CONTRACTS` worden bij
module-import op identieke keysets gecontroleerd. De contractmapping is een
`MappingProxyType`; ieder contract is een frozen dataclass.

## Netatmo kill-window

1. Het originele MQTT-object en zijn requestdigest bevatten alleen jobtype en
   publieke payload.
2. De ouder valideert de credentialreference en controleert het resterende
   deadlinebudget.
3. Bij refresh wordt een geroteerde refreshcredential eerst naar een tijdelijk
   bestand geschreven, geflusht en met `fsync` gecommit.
4. Het bestand krijgt modus `0600`, wordt atomair vervangen en op Linux wordt
   ook de directory-entry met `fsync` gecommit.
5. Pas daarna komt het access token in `Job.execution_context` en wordt het
   child-proces aangeroepen.
6. `execution_context` is uitgesloten van repr, vergelijking, requestdigest en
   receipts.
7. Childresultaten en foutteksten worden vóór MQTT/status/receipt/logging
   recursief geredigeerd op gevoelige keys, bekende secretwaarden, Bearer- en
   token-querypatronen.

Een corrupte persistente tokenstate en een mislukte atomic replace zijn
fail-closed. Na een mislukte rotatie wordt geen access token vrijgegeven en
wordt bij een volgende poging eerst dezelfde rotatie opnieuw duurzaam
vastgelegd; er volgt niet meteen een tweede OAuth-refresh.

Een refresh start alleen als het deadlinebudget groter is dan twee gebonden
HTTP-timeouts plus twee seconden: één timeout voor OAuth en één gereserveerde
timeout voor de child-query. Een reeds geldig gecachet access token slaat de
refresh over; de algemene parent-deadline blijft de child begrenzen.

## Status- en receiptcontract

Workerstatus publiceert:

- `effect_contracts_required`;
- `effect_contract_coverage_complete`;
- per jobtype contract-ID, effectklasse, voorbereiding, compensatie,
  verificatiestatus en `hard_kill_safe`.

Receipts binden `job_id` aan jobtype, publieke-payload-digest, deadline en
monotone status. Parent-only executionmaterial wordt niet opgeslagen. De
secretregressie controleert daarnaast de ruwe SQLite-bytes en alle
gepubliceerde MQTT-payloads.

## Lokaal bewijs

Uitgevoerd met warnings als errors:

```text
tests/test_worker_service.py
tests/test_worker_netatmo.py
tests/test_worker_protocol.py
tests/test_worker_artifacts.py

53 passed in 16.25s
```

Aanvullend groen:

- `py_compile` op service, Netatmo en de gewijzigde tests;
- `git diff --check` op alle P0-W01-bestanden.

De regressies bewijzen onder meer:

- strict fail-closed bij missing/unsafe/unverified effectcontract;
- volledige productiemapping;
- broker-token afwezig uit requestdigest, MQTT, receipt en raw SQLite;
- duurzame parent-preparation vóór child-dispatch;
- persistence failure zichtbaar en fail-closed zonder tweede refresh;
- corrupt tokenstate fail-closed vóór netwerkgebruik;
- refresh+query-deadlinebudget;
- directory-fsync vóór de broker een geroteerd token teruggeeft;
- machineleesbare statuscontracten;
- bestaande hard-deadline-, shutdown-, protocol-, receipt- en artifacttests.

## Open externe gates

Voor productie-DoD blijven echte fault-injectionproeven nodig:

- container `SIGKILL` op elk punt vóór/tijdens/na OAuth-response en
  directory-fsync;
- host power-cut en remountcontrole op het echte `/data`-medium;
- brokerdisconnect/QoS1-redelivery tijdens queued/running/terminal publicaties;
- echte Netatmo timeouts, tokenrotatie, 401/429/5xx en half-open sockets;
- disk-full, read-only filesystem, I/O-fout en corrupte directory-entry;
- hard-kill tijdens `/share`-artifactwrite, inclusief orphan-tempinventaris en
  directory-durability;
- NUC-soak met herhaalde deadlines, restarts en receipt-recovery.

Tot die gates slagen is P0-W01 lokaal technisch onderbouwd, maar niet als
volledig live/power-loss-gevalideerd afgetekend.
