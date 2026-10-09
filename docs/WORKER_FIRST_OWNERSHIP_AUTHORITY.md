# Worker-first ownership- en authorityfence

Status: verplicht geïntegreerd in de geïsoleerde
`WorkerFirstRuntimeCoordinator`. Niet geïmporteerd door `service.py`, niet
live gekoppeld en geen operationele admission.

## Authority

`worker_first_ownership.py` accepteert uitsluitend canonical ASCII-JSON met
contract `stv3-worker-first-ownership-authority-v1` en een vooraf vertrouwde
outer SHA-256. Het document bindt exact:

- één `consumer_id` en één `stream_id`;
- één absolute, bestaande, niet-symlinkende lock-anchor;
- precies deze store-rollen, elk met absoluut canonical pad, objectsoort en
  immutable schema-/profile-identiteit:
  - `acquisition_completion_store` (`directory`);
  - `desired_state_store` (`file`);
  - `operational_fact_store` (`file`);
  - `operational_snapshot_store` (`file`);
  - `pipeline_output_store` (`file`);
  - `snapshot_outbox_store` (`file`).

De interne `authority_scope_sha256` bindt dezelfde consumer-, stream-, lock-
en storevelden. Mutable databasebytes zijn bewust geen store-identiteit: de
gebonden SHA is de immutable schema/runtimeprofile-identiteit die de
composition factory voor die store heeft vastgesteld. Padobjecten worden bij
load, acquire en iedere `assert_held()` opnieuw tegen hun oorspronkelijke
filesystemidentiteit gecontroleerd.

De lockfile bevat exact de constante canonical anchorbytes en nooit PID,
timestamp, ownernaam of secrets. Maak hem eenmalig met
`initialize_worker_first_lock_file()`; die helper overschrijft of truncateert
een bestaande anchor nooit.

## OS-ownership

Acquire is nonblocking:

- POSIX: `flock(fd, LOCK_EX | LOCK_NB)`;
- Windows: `msvcrt.locking(fd, LK_NBLCK, 1)` op byte nul.

Na de lock worden anchorbytes via dezelfde locked descriptor geverifieerd.
Door descriptorclose of processcrash geeft het OS de lock vrij; er bestaat
geen stale takeover. Een aanvullende modulebrede padregistry voorkomt ook dat
base class en subclass binnen hetzelfde proces tegelijk eigenaar worden.
Processen horen met spawn/exec te starten; descriptors zijn non-inheritable.

## Stabiele coordinator-injectie

Een toekomstige production-composition root gebruikt de boundary als volgt:

```python
fence = WorkerFirstOwnershipFence(
    authority_path,
    expected_authority_sha256=trusted_authority_sha256,
)
lease = fence.acquire()
store_bindings = {
    role: (component_absolute_path, component_identity_sha256)
    for role in REQUIRED_STORE_ROLES
}
runtime = WorkerFirstRuntimeCoordinator(
    # overige expliciete lifecycles en authorities ...
    ownership_lease=lease,
    store_bindings=store_bindings,
    consumer_id=configured_consumer_id,
    stream_id=configured_stream_id,
)
```

De constructor accepteert geen optionele ownershipmodus: hij valideert exact
consumer, stream en alle zes store-identiteiten tegen de reeds gehouden lease.
Iedere desired/store/receipt-, processor- en snapshotgrens assert de OS-fence
vóór en na de operatie. Een geannuleerde executorcall blijft geregistreerd tot
haar echte future klaar is. Stop geeft de lease pas vrij nadat processor, alle
overige mutable futures en snapshotoperaties nul zijn en een expliciete schone
`stopped`-snapshotstatus is ontvangen. Een mislukte snapshotstop houdt de
authority vast en kan via een volgende `async_stop()` veilig worden herprobeerd.
Controltime-outs en externe cancellation veranderen dit niet: de waiter kan
begrensd terugkeren, maar de echte future/taak blijft in de ownershipaccounting.
Stop sluit admission en signaleert de processor voordat zij maximaal haar eigen
korte stopbudget op de runtimegate wacht. Een late completion mag de fence pas
vrijgeven nadat ook runtimeflow en snapshotauthority werkelijk quiescent zijn.

De relevante stabiele leasevelden zijn `consumer_id`, `stream_id`,
`authority_sha256`, `authority_scope_sha256`, `binding`, `assert_held()`,
`assert_coordinator_binding()`, `diagnostics()` en `release()`/`close()`.
Een coordinator mag geen heavy work, claim, reject, publicatie of ack uitvoeren
zonder een succesvolle `assert_held()` aan iedere durable grens. De teller
`acknowledged` betekent uitsluitend normale output-ack; `rejected` uitsluitend
duurzame reject en `terminalized` de som van beide cursorterminalisaties.

Diagnostiek bevat alleen statuscodes, backend, aantallen en one-way hashes van
consumer, stream en stores. Ruwe identifiers, paden, coördinaten, station-ID's
of secrets worden niet opgenomen.

## Grenzen die deze fence niet oplost

Deze boundary bewijst alleen ownership en configuratie-identiteit. Voor
go-live blijven afzonderlijk vereist:

- hard-process CPU/RSS/wall/output/process-tree-isolatie;
- een geaudite productiecomposition die de zes concrete store-identiteiten uit
  hun factories afleidt en de trusted authority-SHA buiten mutable stores houdt;
- end-to-end crash/faulttests over de volledige acquire→runtime→releaseketen;
- Linux/HA filesystem-, power-loss- en resource/thermal soak.
