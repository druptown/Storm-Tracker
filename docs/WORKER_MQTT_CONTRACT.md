# Storm Tracker Worker MQTT-contract

Contractversie: `1`

Topicbasis: `storm_tracker/v1`

| Topic | Richting | Retained | Doel |
|---|---|---:|---|
| `worker/status` | worker naar HA | ja | online/offline, actieve taak en queue |
| `jobs/request` | HA naar worker | nee | begrensde taakopdracht |
| `jobs/{job_id}/status` | worker naar HA | nee | taakstatus |

Elk bericht bevat `contract_version`, `message_type` en `timestamp`. Een
taakopdracht bevat daarnaast een UUID `job_id`, `job_type`, absolute
`deadline` en een object `payload`.

De worker verwerkt maximaal één taak tegelijk. De wachtrij is configureerbaar
van 1 tot 16 en staat standaard op 16. Verlopen opdrachten krijgen
`expired`; queue-full en identificeerbare ongeldige aanvragen krijgen
`failed`. Iedere accepted job doorloopt normaal:

`queued → running → completed|failed|expired`.

MQTT QoS1 mag hetzelfde request opnieuw afleveren. Een duplicate job-ID is
daarom geen foutstatus: de worker herpubliceert de actuele `queued`/`running`
status of een van de 32 recentste, maximaal 24 uur bewaarde terminale
resultaten. De handler wordt niet opnieuw uitgevoerd.

De worker publiceert iedere tien seconden status en gebruikt een retained
MQTT-will met status `offline`.

## Trackingpayload v4

`tracking_operational` vereist aanvullend:

- `tracking_protocol_version: 4`;
- een 32-tekens UUID4-hex `batch_id`;
- `start_revision` als niet-negatieve integer;
- exacte observation- en startstate-aantallen;
- SHA-256 over het canonical observation-envelope;
- SHA-256 over het canonical, op `storm_id` gesorteerde state-envelope.

De response echoot deze binding, bevat `result_revision =
start_revision + 1`, `snapshot_count` en een SHA-256 over de volledige
toegepaste snapshots. HA herberekent alle hashes en vervangt state alleen via
compare-and-swap op `start_revision`. Protocol v3 of een mismatch activeert
de lokale idempotente fallback.

De JSON-canonicalisatie gebruikt UTF-8, gesorteerde objectkeys, compacte
separators en weigert NaN/Infinity. Observatievolgorde blijft behouden;
top-level snapshots worden op `storm_id` gesorteerd.
