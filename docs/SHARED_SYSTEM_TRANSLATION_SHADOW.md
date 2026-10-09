# Assignment-independent shared system translation (shadow)

Status: pure, deterministic, bounded shadow evidence. It is not imported by
the worker service, Home Assistant, MQTT, `CellTrackEngine`, association code,
or WeatherSystem grouping. It grants no meteorological admission and publishes
no operational snapshot.

## Purpose

`shared_system_translation.py` estimates one grid-aligned translation between
two binary rain-shield fields before any cell-to-cell assignment. Every
rain-shield footprint in a frame contributes to one exact binary union. The
primitive therefore measures shared raster translation independently of which
individual detection is later associated, split, merged, born, or ended.

This is descriptive motion evidence. It is not independent speed truth and it
does not prove that all cells share one physical velocity.

## Public API

- `BinaryRainShieldFootprintFrame`: immutable frame ID, time, `AnalysisGrid`,
  and disjoint rain-shield footprints. Pixel and footprint order is
  canonicalized. Duplicate, overlapping, non-integer, or out-of-grid pixels
  fail closed.
- `SharedSystemTranslationConfig`: caller-owned resource limits plus a
  low-overlap uncertainty threshold. There is deliberately no speed or
  distance threshold.
- `SharedSystemTranslationAccounting`: exhaustive input pixels and bounded
  work accounting.
- `BinaryShiftTranslationCandidate`: selected or runner-up integer shift,
  metric displacement/speed, full-support similarity, and non-circular pixel
  accounting.
- `SharedSystemTranslationResult`: checksummed result with a selected
  candidate when evidence permits, peak uniqueness, runner-up, uncertainty,
  and explicit admission/snapshot/gate flags fixed to false.
- `estimate_shared_system_translation(previous, current, config=None)`:
  stateless pure estimator.

Result status is one of:

- `resolved_unique_peak`: a unique maximum with no configured uncertainty;
- `estimated_with_uncertainty`: the bounded point estimate remains visible,
  but overlap is low, the peak is tied, or the peak lies on the complete
  search boundary;
- `indeterminate`: support is absent or the complete calculation cannot fit
  the caller-owned capacity. No partial peak is exposed.

## Complete non-circular search

For height `h` and width `w`, the estimator considers exactly
`(2h-1)(2w-1)` shifts:

- row shift `-(h-1)..h-1`;
- column shift `-(w-1)..w-1`.

It forms an exact integer overlap histogram from every ordered
previous/current support-pixel pair. A shift never wraps around an edge. The
selected score is the maximum overlap count; because both support totals are
constant, this has the same ordering as full-support Jaccard or Dice.

Equal peaks are not broken meteorologically. The result reports the complete
tie count and uses only a deterministic receipt representative: least metric
distance, then row shift, then column shift. The runner-up remains present and
the margin is zero for a tie.

## Exact accounting

For every result:

- foreground plus background equals every frame pixel;
- pixel references equal the exact union because footprints must be disjoint;
- expected pair contributions equal `|previous support| * |current support|`;
- `overlap_histogram_mass` equals the number of evaluated pair contributions;
- a complete search evaluates the entire shift domain and every expected pair.

For a candidate, the comparison window is the geometrical intersection of the
grid and shifted grid. The result separately accounts previous support shifted
inside/outside, current support inside/outside that window, overlap,
previous-only, current-only, both-dry, and full-support union. Edge clipping
therefore cannot become circular overlap or a falsely perfect full-support
Jaccard score.

## Capacity semantics

Defaults are:

- 250,000 grid pixels per frame;
- 250,000 support pixels per frame;
- 2,000,000 ordered pair contributions;
- 1,000,000 shifts in the complete domain;
- 100,000 distinct nonzero-overlap shifts.

Hard module ceilings prevent a caller from silently disabling the bounds.
Exceeding any limit returns `indeterminate` with exact work accounting. It
never shrinks the displacement window, samples pixels, clips a velocity, or
returns a partial best candidate.

## DWD Germany ROI feasibility measurement

Read-only measurement against sealed 36-frame manifest
`aaa9045acb6407c3ca714563c6293f5a66f9dd284d1fb00b508be4d1d991e7dc`
gave:

- grid 350 x 425 = 148,750 pixels;
- complete shift domain 699 x 849 = 593,451 candidates;
- 6..27 shield objects per frame;
- 70..240 union support pixels per frame;
- zero overlapping shield pixel references in all 36 frames;
- 5,250..56,640 ordered support pairs over the 35 adjacent intervals;
- at most 7,553 distinct nonzero-overlap shifts in one interval.

That observed sequence fits every default preflight cap. The module remains
unbound from the tracker and the sequence provides no independent motion truth.

## Sealed 35-interval DWD replay

The separate closed replay module
`science_shadow_dwd_roi_shared_translation.py` reopened the exact immutable
36-frame sequence and the previously frozen masked full-domain normalized
cross-correlation reference. It verified the source before and after replay and
produced 35 adjacent interval results:

- 35/35 `resolved_unique_peak`;
- 35/35 shift `(row=0, column=+1)`, or 2 km east per five minutes;
- 35/35 descriptive interval speed 24 km/h;
- shield-overlap fraction 0.613333..0.917391;
- no low-overlap, boundary, or tied-peak uncertainty under the fixed untuned
  0.25 shadow threshold;
- 1,146,804/1,146,804 ordered pixelpairs accounted;
- 20,770,785/20,770,785 full-domain shift candidates evaluated.

The frozen rate-raster reference selected `(0,+1)` in 31/35 intervals. Its
four other shifts were the already documented very-low-overlap intervals
29->30, 30->31, 33->34, and 34->35. The shared binary footprint stayed at
`(0,+1)` in those intervals. This is cross-method robustness evidence only:
the rate-reference score/valid-mask overlap and binary shield overlap have
different denominators and are never subtracted or treated as accuracy.

Artifacts:

- `.analysis/dwd_rv_germany_roi_2km_shared_system_translation_20260808_evidence.json`
  - SHA-256 `a347e44239ddab46f0e6b958722ce805058b73c5295affb851ab9e7e4b4b6beb`
- `.analysis/dwd_rv_germany_roi_2km_shared_system_translation_20260808_evidence.md`
  - SHA-256 `b6ff66c1e06162608d3099e0f1a0c99036abc64190bc1d11c15eb1c37b60c491`
- deterministic scientific payload SHA-256
  `a2f18b1a7f65ecbff8868de31e26f381131ad492c337cf0d8211aed9ca41ec7f`

One parent-process host observation took 22.7423 s total; per interval the
median was 0.630657 s, p95 0.905738 s, and maximum 0.938024 s. These timings
are preserved in the JSON but deliberately excluded from the deterministic
scientific payload hash. They are not NUC/live admission evidence.

## Open blockers before any integration

1. Existing `SystemTranslationEvidence` cannot represent a usable point shift
   together with uncertainty: its current contract rejects uncertainty reasons
   when evidence is available. Silent adaptation would discard scientific
   uncertainty, so no adapter is provided.
2. Binary overlap measures only rain-shield support. It does not use intensity,
   nodata/undetect semantics, subpixel phase, deformation covariance, or an
   independently verified velocity.
3. Full-domain pair enumeration is deliberately bounded. A denser future event
   can become explicitly indeterminate even when this sparse DWD episode fits.
4. A production deadline or process-kill guarantee belongs around a future
   isolated caller; this pure function contains no process, thread, network, or
   state machinery.
5. Calibration against held-out, independently observed motion is required
   before uncertainty thresholds or any operational weighting can be admitted.
