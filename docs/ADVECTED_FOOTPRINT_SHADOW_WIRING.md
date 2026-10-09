# Advected-footprint shadow wiring evidence

Status: worker-only, candidate shadow. This is not live-wired and grants no
meteorological or operational admission.

## One source of association overlap truth

`cell_track_engine.py` now estimates assignment-independent binary
rain-shield translation contexts before any identity is selected. There is one
whole-field context plus one context for every previous `WeatherSystem`.
Every context is bound to the exact `AnalysisGrid`, source frame IDs,
timestamps, binary-frame fingerprints, translation-result SHA-256 and complete
search accounting.

For every previous/current detection pair, the engine builds exactly one
immutable `AdvectedFootprintEvidence` receipt. The same object/receipt SHA is
then consumed by:

1. local-deformation residuals and uncertainty;
2. the main assignment edge's footprint-IoU component;
3. derived split/merge morphology alternatives;
4. multi-scan edge ranking, path-local deformation and split/merge
   alternatives.

No layer recomputes translated IoU. There is no speed, distance or IoU reject.
The complete candidate cross product remains present until a caller-owned
compute cap makes the entire transition explicitly indeterminate.

## Temporal and ambiguity handling

A shift is applied only for one unique, complete peak bound to the exact grid,
frames and elapsed interval. Ambiguous, incomplete or indeterminate translation
keeps the pair as `unadvected_fallback` with exact reason codes. A detection
older than the immediately previous frame additionally receives
`previous_detection_outside_translation_time_window`; even if another active
system supplies a valid one-frame vector, the requested vector is recorded but
the applied shift is exactly zero. A one-frame vector is therefore never
extrapolated through a gap cohort.

`estimated_with_uncertainty` may be used only when its peak is still unique and
the complete search is proven. Its uncertainty reasons remain in the nested
receipt, while local-deformation covariance uses the configured conservative
ceiling.

## Hash and restart boundary

The following IDs bind canonical binary-float encodings of full evidence and
costs:

- local candidate: feature vector, advected receipt SHA, translation
  uncertainty and local residual/covariance, followed by the final edge-cost
  receipt;
- identity hypothesis and snapshot: candidate IDs, cost receipts, solution
  costs/weights and policy fingerprint;
- lineage alternative: every contributing advected receipt SHA and exact cost;
- multi-scan assignment problem/solution, step, branch, posterior and durable
  commit-chain anchor: full edge receipts, matched advected SHAs, paths, costs,
  weights and pruning/search accounting.

Version boundary:

- `stv3-shared-translation-advected-footprint-evidence-v2`
- `stv3-shadow-cell-track-engine-v4`
- `stv3-shadow-multi-hypothesis-identity-v3`
- `stv3-shadow-multi-scan-identity-v2`

The science state codec admits the nested receipt type but still requires the
exact closed dataclass field set. The tracking-policy fingerprint includes the
new algorithms and aggregate translation caps. Old runtime profiles are
rejected by store identity; an old v3 CellTrack checkpoint also fails dataclass
validation instead of being silently upgraded. Replay is required.

## Resource accounting

Before translation work starts, the engine checks the complete number of
translation contexts, full integer-shift candidates across all contexts,
support-pixel pair contributions and advected detection pairs. The estimator's
own grid/support/nonzero-shift hard caps remain in force. Any capacity outcome
returns the old immutable state and an explicit reason; it never substitutes a
partial context list or candidate subset.

## Deterministic proof coverage

Focused tests cover:

- raw IoU 0 becoming advected IoU 1;
- two independent systems using two different translation-result SHAs/vectors;
- a gap pair forced to zero shift while another system has a usable vector;
- ambiguous translation falling back without a centroid pseudo-vector;
- extreme 18,000,000 km/h translation retained with every gate flag false;
- main and multi-scan split/merge alternatives remaining `selected=False` and
  carrying the same pair receipts;
- complete cross-products, clipping, anisotropic negative-y shifts and
  deterministic receipt hashes;
- feature/hash drift propagation through candidate, snapshot, multi-scan
  branch/posterior IDs;
- state-codec restart equality and old-version rejection;
- translation-context and assignment/multi-scan capacity failures preserving
  the exact previous state.

## Remaining admission blockers

This shadow change is not a meteorological validation result. Operational
admission remains closed pending held-out provider replays, calibrated
translation/association uncertainty, quantified split/merge and gap error
rates, cross-provider geographic/time alignment evidence, and NUC resource
evidence under production-size grids and system counts. No service, Home
Assistant, MQTT, dashboard, snapshot-publication or live scheduler wiring is
part of this change.
