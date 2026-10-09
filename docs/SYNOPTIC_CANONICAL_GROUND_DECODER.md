# Synoptic canonical ground decoder — shadow contract

Status: local/shadow candidate only. This decoder is not wired into the worker
service, scheduler, MQTT, Home Assistant, ground confirmation, fusion,
admission or any live fetch path.

Implementation:
`storm_tracker_worker/synoptic_ground_decoder.py`

Golden and adversarial tests:
`tests/test_worker_synoptic_ground_decoder.py` and
`tests/fixtures/synoptic_timeseries_ground_golden.json`

## Purpose and non-goals

The decoder replays one already durable Synoptic Time Series response into the
append-only `CanonicalGroundEvidenceStore`. It provides candidate observations
with exact raw provenance so later scientific work can evaluate them.

It does **not**:

- call Synoptic or any other network service;
- infer rain/no-rain, wet/dry, coverage, confidence or ground confirmation;
- select, merge, average or sum alternative provider sensor sets;
- interpret a QC flag as automatic invalidity;
- turn missing, `null`, `false`, an empty value or an absent field into zero;
- treat a cumulative request-window series as an instantaneous rain rate;
- turn the first returned interval value into a made-up interval;
- admit any fact to the operational engine.

## Primary semantic sources

The contract follows Synoptic's current first-party documentation:

- Time Series: <https://docs.synopticdata.com/services/time-series>
- Precipitation Service Explained:
  <https://docs.synopticdata.com/services/precipitation-service-explained>
- JSON Output Format:
  <https://docs.synopticdata.com/services/json-output-format>
- Synoptic Data QC:
  <https://docs.synopticdata.com/services/mesonet-data-qc>
- Data disclaimer: <https://synopticdata.com/data-disclaimer/>

The Time Series documentation states that `precip=1` replaces raw
precipitation variables with derived `precip_intervals_set_*` and
`precip_accumulated_set_*` variables. Interval values represent precipitation
between consecutive observations; accumulated values are the cumulative sum
for the requested period. Complete intervals at request boundaries are
significant, and `all_reports=1` can return multiple underlying precipitation
reports. The QC documentation explicitly warns that the existence of a flag
does not necessarily mean the observation is invalid or inaccurate. Synoptic's
disclaimer labels the data and services provisional.

## Accepted authority chain

The entry point is:

```python
decode_synoptic_ground_capture(
    capture_id,
    raw_store=raw_store,
    ground_store=ground_store,
    deadline_at=absolute_monotonic_deadline,
)
```

Only a complete schema-3 `RawArtifactStore` capture is accepted. Its exact
shape is:

- provider: `synoptic`;
- capture product: `synoptic_timeseries_query_capture`;
- one member with role and member ID `response_json`;
- member product: `synoptic_timeseries_json_response`;
- JSON content type;
- no capture-level product timestamp, because individual stations and samples
  have their own observation timestamps.

The raw store reload verifies capture receipt, manifest, member receipt,
payload size and payload SHA-256 before decode. The decoder then independently
checks:

- the capture/member job binding;
- the exact content hash and byte length;
- the complete credential-free query context;
- `vars=precip`, `precip=1`, `all_reports=1`, `sensorvars=1`;
- `qc=on`, `qc_remove_data=off`, `qc_flags=on` and all requested QC sources;
- metric precipitation units, JSON output, `recent=60` and the station limit;
- collector, collector version, structure inspector version, request attempt,
  context hash and upstream identities;
- the native structure verifier's precipitation, sensor metadata and QC
  attestations.

The token itself is neither accepted nor reconstructed by this decoder.

Every canonical fact binds all capture members and the selected source member.
Its `source_record_sha256` is the SHA-256 of the exact station-object byte span;
its JSON pointer identifies the precise sensor set and array index. The complete
response bytes remain bound by the member content SHA-256.

## Exact JSON handling and bounds

A dedicated bounded UTF-8 JSON parser retains exact number lexemes and byte
spans. It rejects duplicate keys, non-finite/non-JSON numbers, trailing bytes,
UTF-8 BOMs, malformed escapes, credential-like keys and ambiguous structure.

Hard limits exist for:

- response bytes;
- tree nodes and depth;
- object members, array items, key and string sizes;
- stations, samples and sensor sets;
- output facts and per-fact QC entries;
- exact station record size;
- inline native QC representation;
- one absolute monotonic deadline shared by load, parse, plan and commit.

All facts are planned and validated against the closed canonical ground
contract before the first commit. One bounded `commit_batch()` then reverifies
the complete schema-3 raw authority exactly once and appends every new fact,
member link and QC row in one `BEGIN IMMEDIATE` transaction. A conflict,
capacity failure, deadline, exception or crash before that commit leaves no
durable prefix. Exact and mixed existing/new retries return ordered receipts
idempotently and use one durability barrier. Reusing the same observation
identity for different bytes, value, QC or semantics conflicts atomically.

## Station and sensor-set identity

The provider `STID` is preserved exactly as `provider_station_id` and in the
site identity `synoptic:<STID>`. `MNET_ID`, when present, remains visible as
`synoptic-mnet:<MNET_ID>`. Latitude and longitude preserve their exact decimal
text.

Synoptic documents `ELEVATION` in feet while the canonical site contract needs
metres. The decoder does not silently convert or round it. Canonical elevation
is therefore `unknown`; the exact provider feet value is retained as provider
metadata beside every fact.

For each station, the exact set of precipitation arrays in `OBSERVATIONS` must
equal the exact set registered under the corresponding `SENSOR_VARIABLES`
branch. Every set name is embedded in the canonical variable and repeated in a
provider-QC fact. Sensor metadata is retained in compact, semantically exact
JSON and remains protected by the station-record hash.

No set is preferred. For example, `_set_1d` and `_set_2d` generate separate
facts even when they have the same station and timestamp. This prevents an
unjustified sum, merge or “best sensor” choice.

## Interval semantics

For `precip_intervals_set_*`, an element at index `i > 0` is represented as:

- observation time: `date_time[i]`;
- representative interval start: `date_time[i-1]`;
- representative interval end: `date_time[i]`;
- exact decimal: the original JSON number lexeme;
- semantic marker: `between_consecutive_provider_observations`.

The provider time axis must be strictly increasing. The interval is not called
instantaneous and is not assumed to be a universal hour. Special METAR reports
can make reporting intervals shorter.

At index zero the preceding provider timestamp is not present. Even if the
response contains a number there, the decoder emits a canonical `unknown`
measurement with a degenerate observation anchor. The original token is kept
only in `synoptic.decoder.interval_start_unknown`, explicitly marked unusable as
a canonical interval amount. It is never interpreted as zero, dry or a rate.

## Request-window cumulative semantics

Each `precip_accumulated_set_*` element remains a separate
`request_window_cumulative_series` value. The request uses `recent=60`, but the
durable context does not include an exact request-start timestamp. The
canonical representative start and end are therefore both the observation
timestamp only as a record anchor, accompanied by
`synoptic.decoder.request_window_start_unbound`.

That degenerate anchor is not a claim that the value is instantaneous. The
decoder never relabels this series as `rain_live`, `mm/h`, “last fixed hour” or
an independent accumulation interval.

## Missing and unexpected values

- JSON numbers remain `observed` with the exact decimal lexeme, including an
  exact zero and scientific notation.
- JSON `null` becomes `missing`.
- The first interval becomes `unknown` because its temporal start is absent.
- JSON `false`, text (including `""` or `"0"`), malformed arrays and absent
  sets fail closed before any fact commit.

Consequently, no missing or placeholder representation can become zero or a
dry observation.

## QC and provisional status

Every fact contains:

- `synoptic.provider.provisional=true`;
- the exact sensor-set identity and sensor metadata;
- every applied QC test name from `QC_SUMMARY`;
- the exact response-level flagged counter;
- the relevant exact `QC_FLAGGED` value for the sample;
- recognized exact native `QC` or observation-QC payloads when present;
- the applicable interval or request-window semantic marker.

A `true` QC flag leaves a numeric measurement numeric and observed. It does not
automatically change the measurement to missing or invalid. That preserves the
provider evidence while postponing any use-case-specific QC decision to a
separate, reviewed scientific policy.

## Isolation and current limitations

The module imports storage and exact-capture code only. It has no HTTP client,
service callback, MQTT dependency or Home Assistant dependency. No existing
service, scheduler, admission, dashboard, configuration or version file refers
to its entry point.

Known limitations are explicit:

- this is candidate evidence, not operational ground truth;
- no station siting, calibration or network-quality model is applied;
- no spatial representativeness test relative to a radar pixel or tracker is
  made;
- no request-boundary interval is reconstructed;
- cumulative-series request start remains unknown;
- feet elevation is preserved but not transformed to metres;
- no multiple-set reconciliation policy exists by design;
- provider QC is retained, not yet converted into an application-specific
  acceptance policy;
- no live capture has been fetched or admitted by this work.

These limitations prevent the decoder from making a stronger meteorological
claim than the exact provider response supports.
