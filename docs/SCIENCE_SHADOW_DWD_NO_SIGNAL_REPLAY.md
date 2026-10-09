# DWD RV immutable no-signal science replay

## Status and scope

This is an offline, worker-only, fail-closed science-shadow experiment. It is
not connected to the worker service, MQTT, Home Assistant, admission, forecast
publication or operational snapshots. It does not rebind WeatherSystems.

The only admitted input is the immutable 36-frame export with these identities:

- sequence manifest `e94b8f14393fb0e2076fd54ff7b0a2c0133416cd1cde87bb18acd01df5eda48a`;
- sequence ID `0fc414def8b41ddd27c433a73875d625ea40f5b485a2d330c6edd654c3848dc2`;
- source manifest `20335e6c50b35d57dd111bea7e3670f515e933da5239ba39e74ccebc41030852`;
- upstream process-metrics manifest `7e501592a64faf9bece7af064a425a2d7a3f96719ae90847f7b9753a9a308b60`;
- target-grid SHA-256 `95c7c9a3e805bdad47474b803b49332d891b60f2dee270bb1706bf8ae4dbd364`.

Only the 36 `+000` observations at exact 300-second cadence are tracking
inputs. Forecast members are source-inventory evidence only and are never
opened by the science factory.

## Closed read-only authority

`storm_tracker_worker.science_shadow_dwd_replay` implements the closed factory.
It authorizes exactly:

1. the immutable export directory as `read_directory`;
2. the exact hashed sequence manifest as `read_file`;
3. one separate receipt/checkpoint database as `transactional_file`.

The factory requires the exact three-manifest and 36-object inventory. It
rejects extra, missing, renamed, symlinked, noncanonical, oversized or
checksum-mismatched entries. Every child revalidates source, sequence, metrics
and all evidence objects before opening the selected object. No network API,
raw archive, provider runtime or acquisition consumer is invoked.

For every frame the factory rechecks:

- provider `dwd_radolan`, product `dwd_open_data_composite_rv_capture`;
- physical quantity `interval_mean_precipitation_rate`, unit `mm/h`;
- lead zero, timestamp order and exact 300-second cadence;
- DWD accumulation-to-interval-mean-rate semantics certificate;
- 316 x 316 metric grid and grid SHA-256;
- transform, detector and pipeline fingerprints;
- canonical run, candidate, normalized-frame, completion-event and output lineage;
- exact completion-to-process-metrics binding;
- finite values are nonnegative and finite;
- nodata, undetect, mixed and outside-support null accounting is lossless;
- null pixels remain missing and are never converted to zero/dry;
- a fresh hierarchical detector replay exactly equals the sealed detection frame.

The acquisition evidence source contains a different transform-certificate hash
per frame. That hash is provenance, not provider identity. Before tracking, a
checksummed derived adapter replaces only the tracking source label with the
stable provider/product identity
`canonical:dwd_radolan/dwd_open_data_composite_rv_capture`. The original source,
transform certificate, transform receipt, analysis lineage and evidence hash
remain bound into new derived lineage hashes. Values, grid, units and masks do
not change. Without this distinction the tracker correctly but unhelpfully
classifies every frame as a provider handoff.

## Preflight result: no observable cells

All 36 objects and 18,299,803 evidence bytes validated. Per frame:

- finite detector support: 11..534 of 99,856 pixels;
- support fraction: 0.011016%..0.534770%, mean 0.214976%;
- total finite samples: 7,728;
- maximum rain rate: 0.084..0.300 mm/h;
- sealed and replayed detection objects: exactly zero in all frames.

The certified DWD shield definition is 1.0 mm/h. The Belgium crop therefore
contains no observable cell for either identity algorithm. This is valid
no-signal and sparse-coverage robustness evidence, but it cannot measure
identity continuity, fragmentation, jumps or speed.

## Equal bounded v4.1/v5 probe

The full 36-object input is preflighted for every child. To avoid a scientifically
pointless dual 36-child run, frames 0, 1, 17 and 35 were then processed under
each identity policy through `ScienceShadowIsolatedRunner`. The durable
checkpoint interval was two frames; the runner was reconstructed before the
third probe and an earlier canonical run was retried afterward.

Both modes produced:

- four advanced revisions and an exact durable duplicate;
- zero candidate edges, tracks, births, deaths and lifecycle events;
- zero WeatherSystems, jump samples and speed hypotheses;
- identical no-signal accounting and no leaked child process.

Tracking-probe resource evidence (p50 / nearest-rank p95 / max):

| Mode | Parent wall seconds | Child CPU seconds | Peak child RSS bytes |
|---|---:|---:|---:|
| v4.1 per-transition | 3.832 / 4.093 / 4.093 | 3.508 / 3.828 / 3.828 | 85,334,016 / 86,450,176 / 86,450,176 |
| v5 multi-scan | 3.915 / 4.487 / 4.487 | 3.633 / 4.141 / 4.141 | 85,786,624 / 86,339,584 / 86,339,584 |

These are four-frame tracking-probe distributions, not full-cadence production
capacity evidence. The separate upstream 36-frame extraction metrics remain in
the immutable metrics manifest.

Policy hashes:

- v4.1 `e5b7045e6023be1e9863474d89dca2a68487512b3327f148e0dcae1f82012192`;
- v5 `4377b539f4f519742737aab6286af89d80f1482ad73913dc5d6221c146ac5374`.

No hard speed or distance cutoff is introduced. Both policies use the same
physical detector and the same uncalibrated soft association costs; v5 differs
only by its bounded multi-scan memory. Because no cell exists, neither policy
can be promoted, rejected or compared for identity skill on this crop.

## Evidence and tests

- JSON evidence: `.analysis/dwd_rv_science_shadow_no_signal_20260808_evidence.json`,
  SHA-256 `2694c8826e0a90cf4ef0c0abeaea9c17ff063b43c217d6c8a5f850cad8cd981f`.
- Human report: `.analysis/dwd_rv_science_shadow_no_signal_20260808_report.md`,
  SHA-256 `ee688e42c6fe86a11393138897f82b15b53f5d555d2f8102c489fbfe55a93285`.
- Evidence-record ordered-set SHA-256
  `996448795237b7f5b7daf7107dad2beb77319725af86d66619572bad76d55e7b`.
- Focused suite: `7 passed` with `-W error`, including closed inventory,
  read-only behavior, manifest substitution, stable source derivation,
  v4.1/v5 restart and duplicate determinism, Windows spawn, stale authority,
  authority tamper and child cleanup. An independent root run also passed 7/7.

## Remaining blockers and next bounded experiment

Admission stays false because independent cell-identity and speed truth are
absent, the Belgium crop has no cells at the certified threshold, finite support
is sparse, and the association/posterior weights are not externally calibrated.

The same raw `+000` frames do contain substantial precipitation elsewhere in
the DWD domain. A separate follow-up may create a new, independently sealed
read-only export on the predeclared union-support ROI: source rows 150..999 and
columns 150..849, corresponding to origin `(149500, -149500)`, width 700 and
height 850 at 1 km. That ROI was chosen before identity tracking from the union
of pixels at or above 1.0 mm/h, rounded outward to 50 km edges. The follow-up
must preserve every connected component and all split/merge alternatives,
must not auto-merge WeatherSystems, and must remain without a speed gate or
accuracy/admission claim.
