# Thin HA worker-only runtime — ongewirede cutoverkandidaat

## Status

`custom_components/storm_tracker_v3/worker_only_runtime.py` is een volledig
ongewirede compositiekandidaat. Geen bestaand integratiebestand importeert de
module. Er is dus geen live registratie, platformsetup, migratie, deploy of
versiewijziging uitgevoerd.

De kandidaat brengt uitsluitend vijf reeds geïsoleerde control-plane
onderdelen samen:

1. `worker_bridge` voor het begrensde MQTT-transport;
2. `desired_state_builder` voor gesloten canonicalisatie;
3. `DesiredStateController` voor claim/snapshot/CAS-apply;
4. één `OperationalSnapshotCache` als immutable worker-readmodel;
5. snapshot-only sensorkandidaten achter één gedeelde eventhub.

Er is geen providerimport, radaradapter, kalibratie, tracking, geometrie,
nowcast, SQLite, file-I/O, HTTP, executor, coordinator of lokale
meteorologische fallback in deze runtime.

## Gesloten input

De constructor accepteert reeds genormaliseerde `config_snapshot`- en
`target_snapshot`-waarden. `config_snapshot` heeft exact deze velden:

```text
producer_id
unique_id_prefix
home_target_id
sensor_target_ids
settings
```

`settings` en iedere target worden opnieuw fail-closed gevalideerd door de
bestaande desired-state-builder. Daardoor gelden exact hetzelfde secretschema,
dezelfde types, coördinatengrenzen, targetsoorten, settingsgrenzen, 64-KiB-
documentgrens en maximaal 64 targets als aan de workerzijde.

`home_target_id` moet in de targetsnapshot bestaan. De gesorteerde unieke
`sensor_target_ids` moeten eveneens bestaande targets zijn, mogen home niet
dubbelen en zijn beperkt tot 60. Samen met workerstatus, home-neerslag,
stormtelling en regiotelling levert dat maximaal 64 entities.

Na `prepare()` bewaart de runtime alleen detached, genormaliseerde bounded
kopieën. Referenties naar de callercontainers worden verwijderd, zodat een
latere externe list- of dictmutatie het geheugengebruik niet kan laten groeien.

## Lifecycle

### `prepare()`

`prepare()` is synchroon, idempotent en doet geen I/O. De eerste call maakt
precies één van elk:

- operational snapshot cache;
- desired-state controller;
- runtime-eventhub;
- immutable tuple van snapshot-only sensorkandidaten.

Volgende calls retourneren hetzelfde `PreparedWorkerOnlyRuntime`-object. Er
wordt nog geen bridge opgezet, eventlistener geopend, workerjob verstuurd of
entity geregistreerd. De entities blijven gewone kandidaten met `hass=None`.

### `async_start()`

De expliciete start voert één keer, in deze volgorde, uit:

1. één eventsubscription openen voor de gedeelde eventhub;
2. één worker-bridge opzetten;
3. één desired-state controller starten.

Gelijktijdige of herhaalde startcalls delen dezelfde starttask en maken niets
dubbel aan. De eventhub bezit de subscription gedurende de hele
runtime-lifecycle. Wanneer een toekomstig platform entities aan de hub hangt,
openen die geen tweede listener.

De module registreert de entities zelf nooit. Een latere cutoverwiring moet de
tuple expliciet aan het HA-sensorplatform aanbieden.

### Updates en reconnect

`async_update(config_snapshot=..., target_snapshot=...)` accepteert één nieuwe
volledige state en stuurt die door naar de controller. Producer, unique-ID-
prefix, home-ID en entityset zijn lifecycle-identiteit en mogen niet live
veranderen. Settings, targetposities, availability en de overige gewenste
workerstate mogen wel veranderen.

De controller coalescet updates, houdt maximaal één apply in-flight en bewaart
bij een offline worker alleen de nieuwste bounded state als `dirty`.
`async_worker_reconnected()` activeert de bestaande idempotente
claim/snapshot/retrypath; er wordt nooit een legacy-startup aangeroepen.

### `async_unload()`

Unload is idempotent en verwijdert of annuleert, ook tijdens een gedeeltelijke
start:

- de controller/debounce/in-flight workerrequest;
- alle door deze bridge-setup toegevoegde MQTT/interval-cleanups;
- de ene eventhubsubscription;
- alle entityreferenties in de eventhub;
- de runtime-starttask.

Cleanup gebeurt best-effort voor alle onderdelen; een fout in één cleanup
voorkomt niet dat de andere listeners en tasks worden verwijderd.

## Injecteerbare grenzen

Voor tests en een latere reviewed wiring zijn drie functies injecteerbaar:

- `bridge_setup(hass, cache) -> cleanup`;
- de async worker-`requester` met dezelfde API als
  `worker_bridge.async_request_worker_job`;
- `event_subscribe(hass, callback) -> cleanup`.

De standaard bridgeadapter plaatst uitsluitend de gedeelde cache in de
bestaande bridgedata, roept één `async_setup_worker_bridge` aan en onthoudt
alleen de nieuw toegevoegde cleanupcallbacks. Een reeds aanwezige andere cache
wordt fail-closed geweigerd. De standaard eventhook luistert uitsluitend naar
het operational-snapshot-update-event.

Elke setup- en subscriptionhook moet een callable cleanup retourneren. Zowel
synchrone als awaitable setup/cleanupresultaten worden ondersteund.

## Offline en sensorsemantiek

Wanneer de bridge een worker offline markeert:

- een lege cache blijft `unknown`;
- een bestaande snapshot wordt atomisch `stale`;
- current neerslag, grondbevestiging, dreiging en collectieclaims degraderen
  via het bestaande snapshotcontract naar onbekend/onvoldoende data;
- een nog niet toegepaste desired-state blijft `dirty`;
- geen provider of lokale berekening wordt gestart.

Sensorproperties lezen alleen hun al geprojecteerde immutable view. Een read is
O(1), start geen request, itereert niet over snapshotcollecties en doet geen
I/O. Alleen één snapshot-event vernieuwt de maximaal 64 bounded entityviews.

## Grenzen

| Onderdeel | Harde grens |
|---|---:|
| Desired-state targets | 64 |
| Snapshot-sensorentities | 64 |
| Extra targetsensoren naast de vier basisentities | 60 |
| Operational snapshot envelope | 64 KiB |
| Desired-state document | 64 KiB |
| Runtime-eventsubscriptions | 1 |
| Worker-bridges per runtime | 1 |
| Desired-state controllers per runtime | 1 |
| Eventhubs per runtime | 1 |
| In-flight desired-state updates | 1 |
| Lokale provider-/meteorologische jobs | 0 |

Diagnostics bevatten alleen lifecycle, aantallen, limieten, cache-revision en
availability plus de reeds privacy-minimale controllerdiagnostics. Config,
target-ID's, namen, coördinaten, settings en ruwe foutteksten ontbreken.

## Testbewijs

`tests/test_worker_only_runtime.py` bewijst met geïnjecteerde fake hooks en een
fake async workerrequester:

- pure en idempotente prepare zonder registratie of calls;
- herhaalde start met exact één bridge, subscription, controller en eventhub;
- één listener voor alle later aangehechte entities;
- offline `unknown/stale` plus een dirty controller zonder fallback;
- coalesced update en reconnect naar een schone CAS-head;
- dubbele unload en unload tijdens een geblokkeerde controllerstart;
- verwijdering van listener, bridgecleanup, entityrefs en tasks;
- acceptatie van precies 64 targets/entities en afwijzing van de 65e;
- O(1) sensorreads zonder hook, request of setup;
- bounded diagnostics zonder targetdata of coördinaten;
- AST- en objectgraafisolatie van legacy sensorstartup, providers, engine,
  calibration, `radar_frame_worker`, worker radar client, SQLite, filesystem,
  HTTP en executoroppervlakken;
- afwezigheid van `async_setup_entry` en `async_setup_platform`.

