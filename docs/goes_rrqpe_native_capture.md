# NOAA GOES RRQPEF exact-native capture candidate

Status: isolated candidate only. Nothing in this document or in
`storm_tracker_worker/goes_rrqpe_native.py` is registered in the worker service,
provider registry, decoder, admission path, integration, or roadmap.

## Scope and authority

The candidate covers the current operational pair independently:

- GOES-18 / GOES-West, anonymous bucket `noaa-goes18`, product prefix
  `ABI-L2-RRQPEF`.
- GOES-19 / GOES-East, anonymous bucket `noaa-goes19`, product prefix
  `ABI-L2-RRQPEF`.

NOAA/NCEI identifies ABI-L2-RRQPE as a NetCDF-4, Full Disk, 2 km fixed-grid
rainfall-rate product with data-quality information and a nominal ten-minute
interval. NCEI links the GOES-18 and GOES-19 public S3 buckets directly:

- <https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc%3AC01517>
- <https://www.ncei.noaa.gov/products/goes-terrestrial-weather-abi-glm>

The NOAA GOES-R Product Definition and Users' Guide (PUG), volume 5, revision
2.2, defines the Level-2 filename start/end/create fields, modes 3/4/6,
NetCDF-4 delivery, RRQPE, DQF, and the geostationary fixed-grid metadata:

- <https://www.goes-r.gov/products/docs/PUG-L2%2B-vol5.pdf>

OSPO reports GOES-18 as operational West and GOES-19 as operational East:

- <https://www.ospo.noaa.gov/operations/goes/status.html>

OSPO's product-validation page reports the GOES-18 Rainfall Rate/QPE product at
Full maturity effective 2025-01-04 and the GOES-19 product at Provisional
maturity effective 2025-02-05:

- <https://www.ospo.noaa.gov/operations/goes/product-quality-overview/index.html>

Those maturity labels are preserved as provenance only. They are never
converted to a source-quality score, decoder permission, forecast permission,
or operational-admission decision.

## Exact capture transaction per satellite

`capture_latest_goes_rrqpe(..., satellite=18|19)` requires an exact empty job
payload. It accepts no URL, token, credential, region, key, or product override.
For a first attempt it derives two adjacent UTC-hour prefixes:

1. `ABI-L2-RRQPEF/YYYY/DDD/HH/` for the previous hour;
2. the same prefix for the current hour.

For each prefix it issues a fixed-host anonymous `ListObjectsV2` request with
`max-keys=1000` and `encoding-type=url`. The exact XML bytes are bounded and
stored as separate schema-3 receipts. Both XML responses must be durable before
the first XML parse or selection.

The strict selector verifies bucket, namespace, prefix, non-truncation,
`KeyCount`, storage class, size, ETag, satellite, product, ABI mode, and the
filename's observation-start, observation-end, and creation timestamps. It
selects the latest object by observation end, creation time, and key. It does
not use lexical filename ordering as a substitute for parsed time.

The selected object is fetched from the same fixed satellite bucket with
`If-Match`. `Content-Length`, ETag, Last-Modified, size, content type, endpoint,
identity encoding, and the listing evidence must agree. The complete original
NetCDF-4 bytes are stored as the third schema-3 receipt before HDF5 is opened.

The three roles are:

- `listing_previous` — exact earlier-hour ListObjectsV2 XML response;
- `listing_current` — exact current-hour ListObjectsV2 XML response;
- `netcdf4` — exact selected original ABI-L2-RRQPEF NetCDF-4 object.

Only after all three receipts exist does the store atomically commit a complete
capture manifest. Dynamic listing snapshots include their payload SHA-256 in
the upstream identity, so a later response for the same active hour cannot be
misrepresented as an immutable earlier listing.

## Metadata-only structure inspection

The inspector never indexes or slices a dataset and never reads RRQPE, DQF, x,
or y values. It checks and records only HDF5 links, dataset metadata, global and
variable attributes, dimensions, dtypes, chunks, compression, and storage
layout.

The pinned current profile requires:

- `RRQPE` and `DQF`, both 5424 × 5424;
- `RRQPE` unsigned 16-bit, gzip, chunks 24 × 5424;
- `DQF` unsigned 8-bit, gzip, chunks 48 × 5424;
- fixed-grid `x` and `y`, signed 16-bit, length 5424, in radians;
- `goes_imager_projection` with `grid_mapping_name=geostationary`, sweep `x`,
  height 35,786,023 m, GRS-80 axes, and origin longitude −137° for G18 or −75°
  for G19;
- filename, `dataset_name`, platform, orbital slot, ABI mode, observation start,
  observation end, and creation time in agreement;
- only hard-linked, internally stored, non-virtual root datasets under explicit
  rank, dimension, attribute-count, member-count, and byte limits.

The raw object is the authoritative preservation of every upstream field. The
result additionally preserves hashes of the complete root-member structure,
global attribute-name set, DQF attribute-name set, and DQF flag-metadata
attributes without assigning meteorological meaning to the codes.

## Failure, retry, and capacity behavior

- One absolute monotonic deadline covers lock wait, both listings, object
  transport, durable stores, inspection, and manifest commit.
- A process-wide thread lock and a filesystem byte lock serialize captures and
  capacity-sensitive publication across processes.
- HTTP redirects, proxies, transport compression, endpoint changes, missing or
  mismatched object metadata, oversized bodies, short bodies, and overlong
  headers fail closed.
- The public transport builds no Authorization, cookie, session-token, or
  credential header and follows no caller-controlled URL.
- Receipt identities are deterministic by job, satellite, and role. Retry uses
  already durable members and derives the second prefix from the first durable
  prefix rather than reselecting from a new wall clock.
- A complete retry is network-free. A crash after manifest commit returns the
  same complete capture.
- Capacity errors from `RawArtifactStore` are propagated. There is no eviction,
  truncation, partial manifest, alternate product, or decoded fallback.

## Verification

Normal adversarial suite:

```powershell
$env:PYTHONPATH=(Resolve-Path '.\.codex-test-tmp-v0440').Path
& 'C:\Users\drupt\scoop\apps\python313\current\python.exe' -m pytest -q `
  'tests/test_worker_goes_rrqpe_native_capture.py' `
  --basetemp '.analysis/pytest-goes-rrqpe-native' `
  -W error::ResourceWarning `
  -W error::pytest.PytestUnraisableExceptionWarning
```

Optional live official-S3 golden for both satellites:

```powershell
$env:STV3_RUN_GOES_RRQPE_NATIVE_LIVE='1'
& 'C:\Users\drupt\scoop\apps\python313\current\python.exe' -m pytest -q `
  'tests/test_worker_goes_rrqpe_native_capture.py' `
  -k 'optional_live_golden_public_s3'
```

On 2026-08-02 the live golden observed Mode-6 objects for both satellites,
including original sizes of 1,700,073 bytes (G18) and 1,533,063 bytes (G19),
the 5424 × 5424 RRQPE/DQF structure, and the two official projection origins.
This is evidence that the capture profile matches those current objects, not a
claim that future files cannot change.

## Deliberately unresolved evidence gaps

1. Individual DQF flag codes and their operational acceptance policy are not
   certified. Their upstream metadata is preserved, but no code is interpreted.
2. No RRQPE pixel decoder, scale/offset application, wet-mask rule, spatial
   crop, reprojection, cell extraction, or forecast adapter exists here.
3. No held-out comparison against radar, gauges, or ground truth has certified
   quantitative skill, bias, displacement, stratiform performance, or forecast
   utility for either satellite.
4. Mode 6 is live-golden verified. The official filename contract permits modes
   3 and 4, but no current held-out native sample for those modes is pinned.
5. The current filename parser does not normalize the PUG's theoretical leap
   second value `SS=60`; such an object fails closed instead of being guessed.
6. S3 ETag is revision evidence, not treated as a cryptographic checksum. The
   raw store independently computes SHA-256 over every exact response body.
7. The candidate buffers one bounded original object before schema-3 store and
   HDF5 metadata inspection. Its live worker memory/CPU/thermal behavior is not
   certified, and it must not be wired until that resource gate is measured.
8. NOAA may revise NetCDF metadata, compression, dimensions, or distribution
   behavior. The pinned profile intentionally fails closed until a new official
   sample and contract review justify an update.

Consequently this candidate proves exact capture and structural identity only.
It does not prove meteorological correctness, decoding correctness, or readiness
for operational use.
