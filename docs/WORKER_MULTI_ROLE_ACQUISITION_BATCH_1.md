# Worker multi-role acquisition batch 1

## Scope and safety boundary

This change connects the isolated child-process side of five credential-free,
exact-native acquisition workloads:

- `radar.dpc.sri`;
- `satellite.noaa.err`;
- `satellite.goes18.rrqpe`;
- `satellite.goes19.rrqpe`;
- `radar.metoffice.uk_rate`.

Acquisition is not meteorological admission. Every native function still ends
at `captured_not_decoded`, `weather_state=unknown` and
`operational_admission_eligible=false`. No rain, dry, motion, cell or forecast
fact is created here. The activation matrix remains the only execution
authority; a child dispatcher entry on its own cannot schedule or run work.

During implementation the five rules deliberately remained
`blocked_unwired/multi_role_recovery_contract_not_connected`. They became
`active_shadow_raw/exact_multi_role_shadow_capture_audited` only after the
closed recovery catalog contained their exact receipt, capture and authority
validators and the executor/service timeout hierarchy was raised and tested
as one closed boundary. This prevents a complete raw capture from escaping
capacity accounting after a parent/child crash.

The longest admitted rule has a 105-second wall. The isolated executor has the
same hard ceiling and the enclosing service acquisition lane is fixed at 120
seconds, leaving 15 seconds for parent-side reconciliation and cancellation.
Callers cannot lower or expand that lane. These are still shadow-raw rules:
service wiring and operational/meteorological admission remain off.

Runtime preflight validates capacity both per handler and as one complete
active-handler batch. Providers shared by more than one rule are summed first;
in particular GOES-18 and GOES-19 require a joint `noaa_goes` envelope of at
least 40 MiB and six entries. The global ledger, raw-store byte/object/receipt/
capture caps and reservation-row cap must also retain one worst-case member of
every active rule simultaneously. A profile that can run either GOES rule but
not both therefore fails at startup instead of degrading later to a predictable
`raw_capacity_exhausted` cycle.

## Exact role and resource contracts

| workload | raw provider | required durable roles | per-role hard bounds | reservation |
|---|---|---|---|---|
| DPC SRI | `dpc_radar` | `catalog_json`, `geotiff` | 2 MiB, 25 MiB | 32 MiB / 2 / 75 s |
| NOAA Enterprise Rain Rate | `noaa_enterprise_rainrate` | `listing_xml`, `netcdf4` | 1 MiB, 32 MiB | 36 MiB / 2 / 75 s |
| GOES-18 RRQPEF | `noaa_goes_rrqpe` | `listing_current`, `listing_previous`, `netcdf4` | 2 MiB, 2 MiB, 16 MiB | 20 MiB / 3 / 105 s |
| GOES-19 RRQPEF | `noaa_goes_rrqpe` | `listing_current`, `listing_previous`, `netcdf4` | 2 MiB, 2 MiB, 16 MiB | 20 MiB / 3 / 105 s |
| Met Office UK RATE | `met_office` | `listing_current`, `listing_previous`, `odim_hdf5` | 2 MiB, 2 MiB, 20 MiB | 32 MiB / 3 / 90 s |

The Met Office native path now owns one cooperative 75-second monotonic
deadline across both listing requests, the selected object request, durable
writes and structural inspection. Each socket timeout is shortened to the
remaining shared budget and is independently capped at five seconds, so one
blocking socket operation cannot consume the 15-second parent margin after a
deadline checkpoint. This replaces the unsafe former possibility of three
independent 30-second waits plus unbounded local work under a 90-second parent
wall. A timeout can leave exact partial receipts, but never a fabricated
complete capture; the next bounded attempt reuses those durable receipts.

GOES handler identity fixes the satellite before the provider import. G18 and
G19 share a provider and capture product but use different receipt/capture
suffixes (`:G18:*` and `:G19:*`). Neither the request nor a provider response
can select or swap the satellite.

The child validates all of the following against `RawArtifactStore` after the
native function returns:

1. exact result state and non-admission state;
2. exact role set, without missing or additional members;
3. durable capture ID, provider, product, fetch suffix, manifest hash and
   capture-receipt hash;
4. for every role: deterministic fetch suffix, provider, raw product, allowed
   content type, role byte ceiling, receipt ID, content hash, receipt hash and
   capture-member binding;
5. summed bytes and entry count against the aggregate handler reservation.

For the three pre-existing single-role handlers, their legacy scalar digest is
preserved. Their child evidence path now also requires the exact durable
capture product, manifest hash, raw product and allowed content type before it
can return success; crash recovery independently repeats provider-specific
receipt and authority validation.

## Canonical role-set evidence

The child and crash recovery call the same public
`recovery_evidence_digest()` implementation. A role-sorted binding fingerprint
covers handler, workload, ledger provider, raw provider, capture product,
capture suffix, total transport ceiling and every role's product, suffix,
content types, byte ceiling and provenance kind.

For a multi-role capture:

- `content_sha256` is a canonical digest of binding fingerprint plus each
  role's raw product, content hash and exact size;
- `receipt_sha256` additionally binds each role's receipt ID and receipt hash;
- `capture_sha256` remains the raw store's independent capture-receipt hash,
  which binds the complete capture authority and product timestamp.

Consequently equal bytes from a different provider, product, role, satellite
or authority cannot collide at the evidence-contract level. The returned child
result contains only bounded counters, IDs and SHA-256 digests. It never
contains raw payload bytes, upstream URLs/keys, coordinates, credentials,
exception text or provider provenance.

## Tests

`tests/test_worker_acquisition_multi_role_child.py` proves:

- exact aggregate parity between child and crash recovery for all five
  handlers;
- stable canonical role order even when the native result/store insertion
  order is reversed;
- no upstream authority, payload or secret sentinel in child output;
- fail-closed rejection of a wrong capture product, wrong member product,
  missing role, forged content hash and unexpected provider result;
- exact activation scope/budget/role/satellite binding;
- fixed G18/G19 dispatch independent of caller data.

Native-provider tests remain the authority for transport, exact-byte retention,
container structure and provider-specific upstream/provenance contracts. This
batch does not replace those tests and does not claim decoded or held-out
meteorological quality.
