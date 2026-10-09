# KNMI RTCOR quantitative decoder candidate

Status: isolated worker-only candidate; not registered, not scheduled, not
admitted, not deployed.

## Scientific authority and scope

The decoder targets only the official KNMI Data Platform product
`nl_rdr_data_rtcor_5m/1.0`, captured by
`storm_tracker_worker/knmi_native.py` as exact schema-3 HDF5 bytes. The
[ESSD 2025 product paper](https://essd.copernicus.org/articles/17/4715/2025/)
states that this is a five-minute, approximately 1 km gauge-adjusted QPE
product; `/image1/image_data` contains precipitation accumulation,
`/image2/image_data` contains the quality index and `/image3/image_data`
contains the adjustment-factor field. The
[KNMI HDF tag specification](https://cdn.knmi.nl/system/data_center_publications/files/000/072/301/original/hdftag38.pdf?1739444428=)
defines the calibration, sentinel and NL25 georeferencing metadata used by the
file itself.

The implementation does not infer rain, dry weather, a threshold, cells,
systems, motion or a forecast. It does not convert the five-minute
accumulation to a rate. It also does not turn the native quality index into a
fixed confidence or operational-admission score.

## Raw authority boundary

`decode_knmi_rtcor_capture(...)` accepts only a verified schema-3 capture with:

- provider `knmi`;
- product `knmi_nl_rdr_data_rtcor_5m_capture`;
- exactly one member role `hdf5`;
- raw product `knmi_nl_rdr_data_rtcor_5m_hdf5`;
- the exact collector/version used by `knmi_native`;
- native-before-decode provenance, official dataset name/version/license and
  explicit `quantitative_semantics_verified_after_decode=false`;
- filename `RAD_NL25_RAC_RT_YYYYmmddHHMM.h5`;
- matching raw receipt, member receipt, content SHA-256, capture receipt,
  product time, upstream filename identity and upstream capture identity.

The raw store returns capture, receipt and exact HDF5 bytes through one
`load_capture_bundle_bytes()` pass. The decoder therefore performs one full
raw-object read/SHA verification instead of separate receipt and payload
passes; the independent canonical-evidence commit deliberately verifies the
raw authority once more. Missing or cryptographically inconsistent raw
evidence raises an integrity error and creates neither derived artifact nor
canonical decoder receipt.

## Exact HDF product contract

The decoder requires internal hard links only, rejects soft/external links,
virtual or external dataset storage, and bounds HDF size, object count, group
depth, raster shapes and decoded bytes.

The product tree is deliberately not a byte-for-byte allowlist of every
unrelated internal metadata node. Additional nodes are tolerated only when
they are bounded, traversable, internal hard-linked HDF objects; this preserves
forward compatibility with non-semantic producer metadata. They cannot supply
any decoded value. The three semantic datasets must occur at their exact paths
and must have three distinct low-level HDF object identities, so an auxiliary
dataset cannot be aliased and interpreted twice.

The semantic profile is deliberately exact:

- `/overview/product_group_name = RAD_NL25_COR_NA`;
- `/overview/hdftag_version_number = 3.6` (the actual RTCOR payload tag, not
  the 3.8 version of the generic documentation);
- exactly three image groups and an exact 300-second start/end interval;
- all image product names equal `RAD_NL25_COR_NA`;
- `image1`: `PRECIP_[MM]`, little-endian/native unsigned 16-bit;
- `image2`: `QUALITY_[-]`, unsigned 8-bit;
- `image3`: `ADJUSTMENT_FACTOR_[DB]`, unsigned 8-bit;
- all three datasets are `765 x 700`, gzip-compressed full-raster chunks with
  HDF image literals `CLASS=IMAGE` and `IMAGE_VERSION=1.2`;
- every dataset has exactly one actually allocated full-raster chunk, positive
  physical storage and no implicit unwritten fill-only raster;
- the low-level dataset-creation pipeline contains exactly the known deflate
  filter with a valid compression level and no additional filter; the sole
  chunk has `filter_mask=0`, proving that deflate was not skipped;
- the live upstream inconsistency is bound explicitly: image2 and image3 are
  uint8 but declare `image_bytes_per_pixel=2` and the corresponding two-byte
  `image_size`. The decoder trusts neither declaration in isolation.

An unknown product, alternative formula, changed sentinel, transposed array,
signed/big-endian primary array, missing auxiliary layer, uncompressed layer,
changed tag or changed time interval is rejected. It is not reinterpreted by a
generic radar fallback.

## Calibration and masks

Each layer retains its raw array, a float64 physical array and three separate
uint8 masks (`valid`, `missing`, `out_of_image`). Invalid physical positions
contain only the technical fill 0.0 and may never be consumed without their
mask.

| Layer | Exact HDF formula | Missing | Out of image | Published physical field |
|---|---:|---:|---:|---|
| precipitation | `GEO=0.010000*PV+0` | 65534 | 65535 | 5-minute accumulation, mm |
| quality | `GEO=0.004*PV+0` | 254 | 255 | upstream-native dimensionless index |
| adjustment | `GEO=0.16*PV-20.24` | 254 | 255 | delivered adjustment factor, dB |

The formulas are applied only to valid pixels. No clipping is performed. In
particular, the decoder preserves every formula-valid quality value even if a
later scientific layer may need to interpret its domain.

The ESSD production description says the spatial gauge-adjustment field is
already applied to the most recent five-minute precipitation field. Therefore
image3 is published as separate diagnostic evidence and is **not** applied to
image1 again. Doing so would double-correct the official QPE. This is also
machine-bound in artifact metadata: precipitation carries
`upstream_gauge_adjusted=true` and
`upstream_adjustment_state=gauge_adjusted_by_knmi_before_delivery`; adjustment
carries `may_reapply_to_image1=false` and
`reapplication_policy=never_reapply_image1_already_gauge_adjusted`. Artifact
verification requires those exact values.

## Native NL25 grid binding

The HDF must carry the documented NL25 metadata:

- 700 columns, 765 rows, axis parameters `X,Y`, units `KM,KM`;
- pixel definition `LU`, hence array row 0 is the north/upper edge;
- column direction west-to-east and row direction north-to-south;
- column offset 0.0 km and row offset 3649.9814 km;
- pixel sizes +1.0000035 km and -1.0000049 km;
- corner order southwest, northwest, northeast, southeast with exact normative
  values `(0,49.362064)`, `(0,55.973602)`, `(10.856453,55.388973)`,
  `(9.009301,48.895306)`;
- `STEREOGRAPHIC` and
  `+proj=stere +x_0=0 +y_0=0 +lat_0=90 +lon_0=0 +lat_ts=60 +a=6378.388 +b=6356.906`.

Float32 versus float64 HDF attribute storage is accepted only within a
precision-sized tolerance. Once validated, the artifact binds the normative
decimals, so an encoding-rounding detail cannot create two apparent grid
identities. Grid and native-bbox definitions receive independent SHA-256
identities.

## Immutable candidate artifact

The format `stv3-worker-knmi-rtcor-quantitative-raster-v1` contains canonical
JSON metadata plus 15 fixed-order arrays in a deterministic bounded zlib
payload. Every section records shape, dtype, offset, length, semantics and
SHA-256. Metadata binds capture/manifest/receipt, raw HDF SHA and filename,
decoder identity/version/config SHA, product interval, all formulas and
sentinels, and full native grid/coverage.

The complete artifact SHA-256 is its filename and opaque handle
`artifact:knmi-rtcor-quantitative:<sha256>`. Publication uses a fully written,
fsync'd staging file and an exclusive hard link. Existing content is reverified
and never overwritten. Per-object bytes, total bytes, object count (4096 by
default) and staging count (8 by default) are independent hard bounds; only
exact content-addressed object and staging filenames are accepted. The capacity
check, staging, link, final verification and durable staging cleanup run under
one per-store thread and OS file lock, making capacity accounting atomic across
concurrent threads and processes. The artifact is parsed and all hashes, masks, formulas,
source bindings and grid
metadata are reverified before canonical evidence can be committed.

The first opener also publishes an atomic `.store-profile.json`. Its canonical
bytes permanently bind the artifact schema, suffix, total-byte limit, object
limit and staging limit to that root. Publication uses an fsync'd staging file,
an exclusive hard link and directory fsyncs; interruption before the link leaves
no profile, while interruption after the link is adopted only when the retry is
byte-identical. Any later instance configured with different limits fails
closed. A bounded authoritative layout scan now precedes both new publication
and exact retry, so retry cannot bypass an unknown entry or an already exceeded
store.

The byte boundary is deduplicated logical `st_size` across object and staging
inodes. It is not a claim about allocated filesystem blocks, compression or
remaining disk space; a filesystem quota and free-space monitor remain separate
operational gates. Likewise, the documented fsync ordering proves the intended
code boundaries but does not replace the real NUC/filesystem power-loss matrix.

Thread- and process-lock acquisition is non-blocking and repeatedly checks the
same monotonic decoder deadline. A direct store call without a decoder
checkpoint receives the same bounded 60-second default. Lock contention can
therefore no longer wait forever, although the parent-enforced subprocess
deadline remains mandatory as the hard outer boundary.

The entrypoint also carries one absolute monotonic cooperative deadline,
defaulting to 60 seconds, across raw loading, HDF decoding, artifact encoding,
publication, verification and canonical commit. Expiry is rechecked after each
heavy boundary and cannot produce canonical evidence. This cannot pre-empt a
stuck native HDF5/zlib call, so a parent-enforced subprocess deadline plus
CPU/RSS isolation remains a mandatory admission gate.

Only after that verification does `CanonicalEvidenceStore` v2 receive
`decoded_complete`, and only through its explicit
`non_operational_candidate_artifact` field. This is structural evidence, not
an admission decision.

## Rejection and fault behaviour

Known semantic/structural failures append only a payload-free `unsupported`,
`incomplete` or `decode_error` canonical receipt. Quantity, unit, CRS, grid,
coverage and artifact are null. Semantic/structural rejections occur before
artifact publication. HDF library failures originating while opening links,
groups, attributes, dataspaces, filter metadata or raster chunks are normalized
to a bounded `decode_error` reason; exception text and HDF bytes never enter
canonical evidence. Publication faults clean staging and precede
`decoded_complete`; capacity
exhaustion fails instead of downsampling or discarding a layer.

Decoder job IDs and attempt IDs must be preassigned SHA-256 identities. A
retry of the same capture and decoder configuration yields the same artifact;
distinct attempts remain separate append-only canonical receipts.

## Separately pinned live evidence

The earlier read-only native-capture investigation pinned this upstream file:

| Fact | Pinned value |
|---|---|
| filename | `RAD_NL25_RAC_RT_202608012345.h5` |
| exact size | 39,925 bytes |
| SHA-256 | `a138f091fb61181d2f83e5892e0ba5db19088211d3fc26ccff41bb90b9927d8d` |
| product group | `RAD_NL25_COR_NA` |
| primary parameter | `PRECIP_[MM]` |
| HDF tag | `3.6` |
| interval | 2026-08-01 23:40-23:45 UTC |
| primary shape/dtype/storage | `765 x 700`, uint16, gzip |

Those exact bytes were verified at capture time but are not retained as a
distributable repository fixture. Consequently this table is evidence for the
native identity gate, **not** a claim that the new quantitative decoder has
already replayed that live SHA. The decoder tests use exact golden-shaped
synthetic HDF5 files carrying the full calibration/geography contract. A fresh
read-only replay of a current raw capture, with its SHA pinned before decode,
remains a gate before shadow scheduling or admission review.

## Test evidence and closed gates

`tests/test_worker_knmi_rtcor_quantitative.py` covers the valid three-layer
chain, formula values, separate masks, immutable retry, product/tag/time/grid/
projection drift, formula and sentinel substitution, layer absence, shape,
endianness, compression, corrupt HDF, wrong capture roles, raw tamper, derived
tamper, rehashed metadata forgery, HDF soft/external-resource indirection,
forbidden/omitted filter execution, unallocated chunks, shared dataset-object
aliases, bounded deterministic truncation and compressed-chunk corruption,
normalized HDF library faults, staging failure, atomic parallel disk-capacity
enforcement, object/staging count exhaustion, unknown store entries, missing
capture, deadline expiry before decode and after one raw-authority pass, stable
IDs and side-effect-free import.

The adversarial store tests additionally cover immutable profile mismatch,
profile interruption before and after publication, exact-retry layout
validation and deadline expiry while a separate process owns the store lock.

Focused validation after this store hardening:

- KNMI RTCOR decoder suite: `73 passed`;
- shared real-process lock/profile suite: `6 passed`;
- combined DWD, KNMI and process suite: `113 passed`.

These are functional regression results on the current development host, not a
power-loss result or a production NUC soak claim.

No service, provider registry, radar cycle, admission policy, Home Assistant
path, deployment file or live installation is changed by this candidate.
