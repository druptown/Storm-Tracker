# NOAA ERR quantitative decode profile

Status: locally implemented and verified hardened v3 decoder candidate; not
scheduled, not admitted, not wired to Home Assistant and not deployed.

## Why this profile exists

The exact-byte capture in `storm_tracker_worker/noaa_err_native.py` proves
which NOAA NetCDF-4 object was received, but deliberately reads no raster
values. A quantitative decoder may only be built from semantics that can be
bound to the captured file itself or to primary NOAA evidence. This document
records the currently verified profile and keeps every remaining inference
outside the operational path.

NOAA/NESDIS STAR describes Enterprise Rain Rate (ERR) as a global,
instantaneous satellite precipitation-rate estimate on a 0.02 degree grid,
updated every ten minutes and covering 60 degrees south through 70 degrees
north:

- <https://www.star.nesdis.noaa.gov/smcd/emb/ff/SCaMPR.php>
- <https://www.star.nesdis.noaa.gov/smcd/emb/ff/index.php>

ERR is an infrared/microwave-calibrated satellite QPE. It is neither weather
radar nor an in-situ ground observation. A successful decode must therefore
retain semantic class `satellite_precipitation_rate_product` and can never by
itself produce ground confirmation.

## Exact live object profile

On 2 August 2026 the exact native-capture path was replayed read-only into the
local shadow store `.analysis/noaa-err-live-golden`. The selected object was:

`BLEND/RainRate-Blend-INST/2026/07/18/23/RRQPE-INST-GLB-5_v1r1_blend_s202607182350000_e202607182359599_c202607190008336.nc`

The exact NetCDF-4 bytes are 4,965,137 bytes with SHA-256
`81f82eabc0d3c4810407d155c54799738cea716732e53054dc525ab90ee28e8f`.
The exact S3 listing has SHA-256
`68bfc38e88aba740d52b2c1cad02415ae2aefe90a27ab475cebce91b2a810e49`.
The shadow capture ID is
`capture-fec706864ed74e1c3cdeebe29851cf60300d763e4751d878af613b89978fc760`.

The file itself declares and the candidate decoder binds exactly:

- `Conventions=CF-1.6`, `title=RRQPE_INST_GLB` and NOAA Level 3;
- start `2026-07-18T23:50:00Z`, end `23:59:00Z`, instantaneous quantity;
- global attributes latitude `-60..70`, longitude `-180..179.98`, resolution
  `0.02` degree;
- shape `(6501, 18000)` with array axis order `row,column`;
- `RRQPE` signed int16, `_FillValue=-9990`, `scale_factor=0.1`,
  `add_offset=0`, unit `mm/h`;
- `DQF` signed int8, `_FillValue=-1`, `valid_range=[0,3]`;
- `DQF.flag_masks=[1,2]` and, in the same order,
  `bad_quality` and
  `degraded_due_to_LZA_or_latitude_threshold_exceeded`;
- five complete blend inputs: GOES-19, GOES-18, Himawari-9, MSG2 and MSG3.

The hardened profile requires the six geospatial numbers as exact IEEE-754
float32 values. It also binds the live NetCDF dimension-scale graph: `Rows`
has dimension ID 1, `Columns` ID 0, both RRQPE and DQF declare coordinates
`[1,0]`, and their axis references must resolve to those exact two unique HDF5
objects. The fixed-byte `CLASS`/`NAME` attributes are respectively `S15` and
`S63`, while scale IDs and raster coordinates are signed int32. The scale
`REFERENCE_LIST` is the live 16-byte compound layout (object reference at
offset 0, signed int32 dimension at offset 8); each raster `DIMENSION_LIST` is
an exact variable-length object-reference array. `MonitoringMetaData` must
report five files,
zero missing inputs and
five unique filenames matching respectively GOES-19 (`g19`), GOES-18 (`g18`),
Himawari-9 (`h09`), MSG2 (`met9`) and MSG3 (`met10`). A contradictory but
readable file is `unsupported`; unreadable/corrupt HDF5 is `decode_error`.

The DQF attributes follow the CF flag-mask convention. Consequently:

- value `0`: neither published problem bit is set;
- bit `1`: NOAA marks the retrieval bad quality;
- bit `2`: NOAA marks degradation due to viewing zenith angle or the
  latitude threshold;
- value `3`: both bits are set;
- value `-1`: missing DQF and never a valid/optimal retrieval.

The candidate decoder retains the raw DQF value and separate masks for
missing, bad-quality and degraded. It must not collapse these into a made-up
confidence score. A later admission policy may decide whether degraded pixels
are usable for a particular purpose, but the decoder itself may not silently
discard or promote them.

## Grid orientation evidence

`Rows` and `Columns` are NetCDF dimension scales rather than coordinate
variables: all their stored values are zero. They cannot be used as latitude
or longitude arrays. Coordinates must be reconstructed from the certified
regular-grid profile, and the north/south row direction required an
independent check.

The local orientation audit uses two NOAA-published artifacts for the same
valid time, 21 February 2025 15:20 UTC:

- official STAR global visualization
  `202502211520SC.png`, 151,003 bytes, SHA-256
  `90b8e1c7cb1e41db4e02dd47ebfae92d61cdae458887c25d1c2d5e0330b61c64`;
- exact GLB-5 NetCDF
  `RRQPE-INST-GLB-5_v1r1_blend_s202502211520000_e202502211529599_c202502211540495.nc`,
  3,883,072 bytes, SHA-256
  `a15d31a9cdc88aae8a9ee794af35fc792113c6049f75d5b8740b2239b5c03de7`.

The files are retained only as local `.analysis/noaa-orientation-audit`
evidence, not as distributable repository fixtures. The official image labels
70 N at its top and 60 S at its bottom. Comparing its non-grey precipitation
pixels with the exact RRQPE positive-value mask gives:

| Candidate row mapping | Matthews/phi correlation | Jaccard overlap |
|---|---:|---:|
| row 0 = 70 N, rows north-to-south | 0.61949 | 0.46092 |
| vertically reversed | 0.00939 | 0.02047 |

Map coastlines, antialiasing and image resampling prevent a perfect
pixel-for-pixel score, but the two-order-of-magnitude separation rejects the
reversed candidate. The certified reconstruction is therefore:

```text
latitude(row)  = 70.00 - 0.02 * row       (row 0..6500)
longitude(col) = -180.00 + 0.02 * column  (col 0..17999)
```

A decoder must revalidate the exact domain, resolution and shape before using
this profile. Any producer change is `unsupported`, never an opportunity to
guess a new orientation.

## Implemented bounded decoder behaviour

The global raster contains 117,018,000 pixels. A tracker job must never decode
or publish that complete array merely to inspect a local horizon. The decoder
must accept a prevalidated geographic region, derive a conservative integer
window from the certified regular grid and enforce hard maxima for rows,
columns, pixel count, decoded bytes and artifact bytes before the first raster
read. Dateline-wrapping regions require two explicit source windows and one
unambiguous output layout; a first implementation may instead fail closed on
dateline crossing.

For every selected pixel it must preserve at least:

1. signed int16 `rrqpe_raw` including the fill code;
2. float64 `precipitation_rate_mm_h`, populated only where RRQPE is not fill;
3. `rate_valid_mask` and `rate_missing_mask`;
4. signed int8 `dqf_raw` including `-1`;
5. `dqf_missing_mask`, `dqf_bad_quality_mask` and
   `dqf_degraded_mask`;
6. a separate `quality_optimal_mask`, true only for non-missing DQF value 0.

The physical conversion is exactly `0.1 * rrqpe_raw` for non-fill values.
Negative non-fill rainfall codes, values outside the int16/product contract,
unknown DQF bits or inconsistent RRQPE/DQF missingness must fail closed. The
decoder may not infer a ten-minute accumulation: ERR is an instantaneous
`mm/h` estimate whose source scan spans the declared start/end interval.

## Evidence and admission boundary

A completed candidate decode follows the same order already used by the DWD
and KNMI candidates:

1. verify schema-3 capture, both member receipts, hashes and provenance;
2. reconstruct and validate the exact S3 key/capture identity;
3. validate the full NetCDF metadata profile without reading the global
   raster;
4. validate the requested region and resource bounds;
5. read only the bounded RRQPE and DQF windows;
6. construct deterministic arrays and masks;
7. publish and reread a content-addressed immutable candidate artifact;
8. only then append `decoded_complete` to `CanonicalEvidenceStore` v2 using
   `non_operational_candidate_artifact`.

Known structural/semantic rejection may append only a payload-free
`unsupported`, `incomplete` or `decode_error` receipt. It must contain no
quantity, unit, coverage, grid or artifact. Raw-integrity failure may not
produce canonical evidence at all.

Even `decoded_complete` remains non-operational. Required later gates are:
hard subprocess CPU/RSS/deadline isolation, resource/thermal soak, temporal
high-watermark handling, multi-event replay,
comparison against radar and independent ground observations, regional bias
and false-alarm analysis, and explicit admission policy. No decoder result may
be wired to the HA integration before those gates and the snapshot-only
cutover are complete.

## Local implementation and exact-byte replay evidence

`storm_tracker_worker/noaa_err_quantitative.py` now implements this profile as
an isolated decoder-v3 candidate while retaining artifact schema v2. It accepts
one non-wrapping region only, caps each axis
at 1024 pixels and the total at 1,048,576 pixels, verifies the complete HDF5
object/filter/storage contract before the regional read, and requires every
selected RRQPE and DQF chunk to be physically allocated with no skipped
filter. Its deterministic artifact has a 32 MiB object limit, a 24 MiB decoded
array limit, a hard 512 MiB shared-store ceiling, 65,536-object ceiling and a
64-object/64 MiB staging ceiling. Object and staging filenames are closed
schemas. Capacity check and publication are serialized across threads and
processes; the Linux/HA target fsyncs the object-directory entry after the
atomic hard link. Publication is content-addressed, immutable, re-read and
semantically checked before canonical evidence is committed.

Every decode has one absolute monotonic 60-second cooperative deadline shared
by raw validation, metadata/chunk loops, bounded reads, artifact construction,
publication, reread and the pre-commit boundary. Checks around HDF5 and zlib C
calls cannot interrupt a C call already in progress and byte limits are not an
OS-enforced RSS limit. Therefore decoder v3 is still blocked from admission
until the generic worker subprocess runner provides a parent-enforced hard
deadline plus CPU/RSS isolation and those limits pass NUC thermal soak. The
Windows development fallback cannot fsync a directory and is not crash-proof;
the deployment target is Linux/HA.

Complete native-capture retries use `load_capture_bundle_bytes`: the capture
catalog and manifest are validated and each listing/NetCDF member is read and
SHA-verified exactly once. Only a deterministic readable-profile rejection is
cached, and only for the exact tuple of content SHA, S3 key, ETag and inspector
version. Corruption, HDF5 I/O, timeouts and other transient failures are never
cached.

The exact live capture recorded above was decoded locally for the region
48..53 N, 1..8 E. The resulting window was 251 by 351 pixels. It produced:

- outcome `decoded_complete`;
- 995 positive precipitation-rate pixels;
- maximum instantaneous rate 2.8 mm/h;
- DQF counts `{0: 88101}` (all selected pixels had neither published issue
  bit set);
- immutable artifact-v2/decoder-v3 size 7,985 bytes;
- artifact SHA-256
  `fffc1b537a5cd8fa7f92dd45df5282a0ce639c734b11ca54a68fe4d6bbf748b0`;
- decoder config SHA-256
  `a0376c638380587b402a84f0b44c6ac12b758d496e54ff679648c7bb5fafd964`.

This is decode evidence, not weather-source admission. It neither proves ERR
regional skill nor grants ground confirmation. The focused suite additionally
checks exact float32 grid drift, missing input files, live dimension-scale and
axis references, semantic-versus-corruption typing, missing or unallocated
chunks, per-chunk filter bypass, raw and derived-object corruption,
semantically changed-but-rehashed artifact headers, non-standard JSON, exact
retry authority count and deterministic-rejection caching, thread and true
process capacity pressure, staging/object limits, cooperative expiry, inert
import and absence from registry/service wiring.

Artifact v2 embeds the complete schema-3 capture manifest, both member content
and receipt digests, exact raw receipt identifiers, source ETag/key and the
raw-store capture receipt. The reader independently reconstructs the manifest
digest, capture ID, capture-receipt digest, S3-key times and upstream capture
identity. The decoder then compares the reread artifact byte-for-byte at the
metadata level with the already verified raw capture before committing
canonical evidence. A content hash alone is integrity, not authenticity; the
schema-3 raw store and its canonical evidence reference remain the provenance
trust anchor.

The combined focused native-capture and quantitative suites have 97 passing
tests: 50 native-capture tests and 47 quantitative-decoder tests. This is local
software evidence only, not regional meteorological validation or admission.
