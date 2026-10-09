# Multi-provider replay matrix

## Status and proof boundary

This is an offline, worker-only validation artefact. It does not import or
wire the worker service, Home Assistant, MQTT, acquisition scheduling, or
operational snapshot publication. Every sequence and probe has:

- `meteorological_admission_granted = false`;
- `motion_claim_generated = false`;
- `operational_snapshot_claim_generated = false`.

The matrix proves bounded decode, grid, accounting, translation, CellTrack,
science-shadow, restart, and duplicate behaviour over the local material
listed below. It does **not** prove precipitation quantity, rain rate,
provider semantics, independent cell identity, motion truth, or NUC
production admission.

Implementation:

- `storm_tracker_worker/multi_provider_replay_matrix.py`
- `scripts/run_multi_provider_replay_matrix.py`
- `tests/test_worker_multi_provider_replay_matrix.py`

The final evidence is:

- report: `.analysis/multi_provider_replay_matrix_20260808_v2/report.json`
- report contract: `stv3-multi-provider-replay-matrix-v1`
- internal payload SHA-256:
  `07e5f625993a53c1dff887613e94f8cbafc832c20bc8e0198fd3f466b78b9179`
- complete report-file SHA-256:
  `7f408e1855e7ea18946af74bdf9e04d6764d80fbdfc4a1a253d01a890dfe1857`
- canonical JSON: verified byte-exact
- report size: 14,314 bytes
- evidence directory: 16 files, 15,893,711 bytes

The prior `v1` directory was not overwritten or removed. The final run used
a new `v2` directory.

## Multi-frame replay path

Each categorical frame follows the same inert chain:

1. bounded local-source inventory and content manifest;
2. provider-specific structural load;
3. bounded analysis-grid transform;
4. hierarchical detection;
5. science-shadow composition with exact source, grid, detector, transform,
   semantic, and pipeline fingerprints;
6. shared-system translation;
7. central advected-footprint evidence;
8. CellTrack v4 association and multi-hypothesis identity;
9. durable science-shadow receipt and checkpoint;
10. midpoint reconstruction from the durable store;
11. exact retry of the final `run_id`, which must return a duplicate without
    evidence or transition arrays;
12. final durable recovery and before/after source-manifest comparison.

No speed, distance, or IoU gate removes candidate pairs. For every transition,
the recorded candidate count must equal the expected complete cross-product.
Every candidate has exactly one association-evidence SHA and is accounted as
either `advected` or `unadvected_fallback`.

## KMI public-composite result

Source:
`.analysis/kmi_public_radar_composite_20260730_sequence_20`

The exact public WCS GeoTIFF sequence contains 20 frames at a strict 300-second
cadence. The existing WCS/GeoTIFF audit validates the container and grid of
every frame. The capture receipt explicitly keeps all semantic gates closed:
the public product is not asserted to be native RADQPE, and quantity, unit,
nodata semantics, and operational cutover are unverified.

For this replay only, finite raw values greater than zero become one derived
structural category. They are not called rain, dry evidence, or mm/h. Across
the 20 frames, 1,125,466 positive source cells entered this structural path.

Final result:

| Field | Result |
| --- | ---: |
| Frames processed/advanced | 20/20 |
| Cadence | 300 s |
| Timestamp span | 1785410100–1785415800 |
| Durable revision after recovery | 20 |
| Final tracks | 13 |
| Candidate/expected pairs | 419/419 |
| Advected pairs | 146 |
| Explicit fallback pairs | 273 |
| Unique association-evidence SHAs | 419 |
| Translation-available frames | 19 |
| Restart / exact duplicate retry | 1 / 1 |
| Wall / process CPU | 14.387 s / 14.250 s |

Source manifest before and after:
`0c6fd67f1f042a9858729f3ef4a37429ce5bbeaa307e20e421aa0810dd08e745`.
The source tree was unchanged.

## Houston/RainViewer result

Source: `.analysis/houston_motion_replay.json`

All 24 frames are retained. The file contains a geometry/cadence
discontinuity, so the loader fail-closes into three contiguous cohorts of 12,
2, and 10 frames. Every cohort has a strict 600-second cadence and traverses
the same translation, advected-footprint, CellTrack v4, and science-shadow
path as KMI.

The source is a previously derived normalized categorical cache. It is not
byte-bound to the original RainViewer tiles, has no physical precipitation
unit, and cannot support meteorological admission or a motion-truth claim.

| Cohort | Frames | Candidate/expected | Advected | Fallback | Translation frames | Restart/duplicate | Wall / CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 12 | 2835/2835 | 391 | 2444 | 11 | 1/1 | 56.798 / 56.297 s |
| 2 | 2 | 208/208 | 48 | 160 | 1 | 1/1 | 1.703 / 1.703 s |
| 3 | 10 | 2964/2964 | 483 | 2481 | 9 | 1/1 | 50.740 / 50.313 s |

Across Houston, 24/24 frames and 6,007/6,007 candidate pairs were accounted;
922 pairs used advected evidence and 5,085 used an explicit unadvected
fallback. No pair was silently dropped.

Source manifest before and after:
`3f54c4a31d84a2d567cc28eb4e1442a74dcd10985d6c3c82090cad02abab3354`.
The source file was unchanged.

## Single-frame and metadata probes

Single frames never produce motion. Raw bytes are copied into a bounded work
directory before a store is initialized, so the final probe cannot mutate the
source fixture.

| Provider | Retained evidence | Outcome | Exact accounting | Boundary |
| --- | --- | --- | --- | --- |
| NOAA ERR | exact raw capture | `decoded_complete` | 88,101/88,101 pixels; 2/2 members; exact retry | candidate decoder only; one frame |
| OPERA DBZH | exact raw capture | `unsupported:raw_provenance` | 1/1 member; native grid 4400×3800 | retained capture predates current provenance certificate; quantitative pixel accounting withheld |
| KNMI RTCOR | authority JSON only | `withheld` | authority grid 765×700 | exact raw HDF5 not retained |
| RainViewer | exact raw capture | `withheld` | 19/19 manifest/tile members | structural PNG/manifest audit only; no certified quantitative decode |
| DPC SRI | metadata contract | `withheld` | no raster | raw payload absent |
| DWD RV | metadata contract | `withheld` | no raster in this matrix | no motion claim from this single metadata golden |
| H SAF H40B | metadata contract | `withheld` | no raster | raw payload absent |
| Met Office UK | metadata contract | `withheld` | no raster | raw payload absent |
| Météo-France DPRadar | metadata contract | `withheld` | no raster | raw payload absent |

Every source manifest is identical before and after its final probe. NOAA and
OPERA also perform an exact durable duplicate retry. OPERA's rejection is a
successful fail-closed result, not a decoder success: the retained capture
cannot be upgraded by inventing a missing provenance certificate.

During early development, before the copy-before-initialize boundary was
added, the local NOAA, OPERA, and RainViewer raw fixture stores were opened by
`RawArtifactStore.initialize()`. That added/currentized store profile state and
changed the fixture database/profile bytes at that earlier point. No safe
pre-change manifest exists, so those bytes were not deleted or guessed back.
The hashes in the final `v2` report bind the resulting current fixture state;
the final run itself proves that state remained byte-identical before/after.

## Resource evidence

The full command completed in 132.9 seconds on the development Windows host.
Sequence-local wall and process-CPU times are recorded above. RSS values were:

- KMI: 224,505,856 before; 232,177,664 after; process-lifetime peak
  244,789,248 bytes;
- Houston cohort 1: 232,452,096 before; 239,927,296 after; lifetime peak
  245,309,440 bytes;
- Houston cohort 2: 239,927,296 before; 239,685,632 after; lifetime peak
  246,272,000 bytes;
- Houston cohort 3: 239,685,632 before; 241,086,464 after; lifetime peak
  247,595,008 bytes.

The peak is a process-lifetime Windows working-set peak and is therefore
monotonic across cohorts, not an isolated per-cohort peak. This matrix runs
in-process and does not exercise the hard process resource envelope. These
figures are diagnostic evidence only and do not grant HA/Z2M or NUC admission.

## Reproduction

From the repository root, with the workspace dependency path available:

```powershell
$env:PYTHONPATH=(Resolve-Path '.codex-runtime-deps').Path
python scripts/run_multi_provider_replay_matrix.py `
  --output-directory .analysis/multi_provider_replay_matrix_<new-id> `
  --maximum-analysis-pixels 1500
```

The output directory must be new or empty. The script has no network client
and no service, HA, or MQTT import.

Focused tests:

```powershell
$env:PYTHONPATH=(Resolve-Path '.codex-runtime-deps').Path
python -m pytest -q -W error tests/test_worker_multi_provider_replay_matrix.py
```

## Remaining admission blockers

Production admission remains closed until all of the following exist and are
independently validated:

- hard process isolation and enforceable CPU/RSS/wall/output limits for this
  exact composition;
- certified provider quantity, unit, nodata, timing, grid, licence, and
  transformation semantics;
- exact raw RainViewer bytes bound to the Houston normalized replay;
- current valid raw provenance for the OPERA quantitative decoder;
- a retained exact KNMI HDF5 golden and complete decoder/accounting replay;
- raw raster goldens for the providers represented only by metadata;
- independent identity and displacement truth suitable for scoring tracking;
- calibrated thresholds and uncertainty validated across enough independent
  weather regimes;
- worker-first lifecycle, retention, privacy, and HA/Z2M resource-admission
  evidence for the final production composition.

Until those gates close, this matrix is structural/categorical evidence only.
