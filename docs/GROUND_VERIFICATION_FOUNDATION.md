# Shadow ground verification foundation

Status: pure local/shadow implementation for `P1-F04`. It is not imported by
Home Assistant, the worker service, the ground-fusion runtime, package exports,
or any deployment path.

Implementation:

- `custom_components/storm_tracker_v3/engine/ground_verification.py`
- `storm_tracker_worker/tracking_core/engine/ground_verification.py`

Those two files are byte-identical. The immutable algorithm identifier is
`stv3-event-held-out-ground-verification-v1`.

## Scientific boundary

The verifier does not create labels, derive truth from the candidate engine,
convert missing evidence to dry weather, select a wet/dry threshold, fabricate
probabilities, calibrate probabilities, or decide whether a release passes.
It only compares a categorical candidate record with an already existing
`WetDryLabel` in an `ExternalTruthContext`-compatible object.

Candidate output has four distinct states:

- `wet`;
- `dry`;
- `unknown`;
- `unavailable`.

Only `wet` and `dry` enter the 2x2 confusion counts. `unknown` and
`unavailable` remain separately counted indeterminate outputs. They also
reduce explicitly reported decision coverage. Neither is ever interpreted as
dry.

## Typed case admission

`GroundWetDryCandidate` stores:

- immutable candidate and decision IDs;
- evaluation stratum and candidate algorithm version;
- event and frame ID;
- provider-independent physical-site ID;
- exact observation interval;
- provider and capability ID;
- whether that candidate capability can observe precipitation absence;
- categorical candidate state;
- optional candidate track ID.

`admit_ground_verification_case()` accepts one candidate, one caller-selected
event-held-out partition, one external truth context and one exact label ID.
The pair is admitted only if all of the following hold:

1. the event and frame exist in the immutable split;
2. both belong to the caller-selected partition;
3. the frame belongs to the candidate event;
4. the requested label exists and is a `wet_dry` label;
5. label state is `confirmed` and wet/dry truth is not `unknown`;
6. its source exists and declares independently eligible external truth;
7. label event and frame exactly equal candidate event and frame;
8. external physical-site ID exactly equals candidate physical-site ID;
9. interval start and end exactly equal the candidate interval;
10. dry truth comes from a source capable of detecting absence;
11. a candidate dry decision comes from a candidate capability capable of
    observing absence.

No geographic-nearness, timestamp tolerance, station-name similarity or
fallback matching is used. A data-quality failure produces a typed exclusion,
not a synthetic case. Malformed API inputs raise immediately.

Every exclusion reason is retained:

- event/frame absent;
- partition mismatch;
- event/frame mismatch;
- label absent or wrong kind;
- label ambiguous/not confirmed;
- truth unknown;
- source absent or not independent;
- label event/frame mismatch;
- physical-site mismatch;
- exact interval mismatch;
- invalid non-dry-capable truth/candidate dry statement.

The current `EventSplit` stores event/frame identities and partitions but not
the original `EventFrame.valid_time_utc`. The verifier can therefore prove
exact candidate-to-label interval equality, but it cannot independently
reconstruct the original frame valid time from the split. Persisting and
verifying that timestamp remains an external evidence-contract gate.

## Event-weighted confusion metrics

For each admitted determinate case:

| External truth | Candidate wet | Candidate dry |
| --- | ---: | ---: |
| wet | true positive | false negative |
| dry | false positive | true negative |

Raw integer counts remain in the report. Conditional rates are calculated
inside each meteorological event first and then averaged with equal event
weight.

For every event `e` with at least one determinate wet-truth case:

```text
sensitivity_e = TP_e / (TP_e + FN_e)
```

Overall event-weighted sensitivity is:

```text
sensitivity = mean_e(sensitivity_e)
```

Specificity is calculated analogously over events containing at least one
determinate dry-truth case:

```text
specificity_e = TN_e / (TN_e + FP_e)
specificity   = mean_e(specificity_e)
```

The report also contains event-weighted:

- overall determinate-decision coverage;
- wet-truth decision coverage;
- dry-truth decision coverage;
- false-negative rate (`1 - sensitivity`) when sensitivity exists;
- false-positive rate (`1 - specificity`) when specificity exists.

If a stratum has no eligible wet denominator, sensitivity is `None`. If it has
no eligible dry denominator, specificity is `None`. Zero evidence is never
reported as zero error or perfect skill. There is no hidden minimum sample
size, confidence interval, bootstrap setting, threshold, or release margin.

## Provider and capability strata

The report provides:

- one overall candidate-record summary;
- one summary per provider;
- one summary per exact `(provider, capability,
  candidate_can_observe_absence)` combination.

A provider/capability stratum is present only when it has at least one admitted
case. Unsupported conditional metrics remain `None`, while raw case/event and
indeterminate counts remain visible.

The overall record-level result can contain correlated rows when multiple
providers observe the same physical site. It must not be presented as an
independent-site confidence interval. The exact reuse audit below exposes this
condition. A future site-clustered uncertainty analysis requires a
preregistered resampling design and is not fabricated here.

## Exact-label duplicate-site and cross-track audit

Audit counters use admitted cases only. Consequently every counted group has:

- an independently eligible external source;
- a confirmed wet/dry state;
- exact event, frame, site and interval agreement.

Within one candidate `decision_id`, cases sharing that exact external label
and physical site form a duplicate-site group. The report counts:

- number of duplicate-site groups;
- extra candidate rows beyond the first;
- groups spanning more than one provider;
- groups containing conflicting determinate wet and dry candidate states.

Across decisions, an exact label assigned to more than one non-null candidate
track ID forms a cross-track overlap group. The report counts overlap groups,
extra distinct track assignments and admitted cases missing track metadata.
Grouping uses the exact event/frame/site/interval truth unit, not label ID, so
two independent labels for the same unit cannot hide reuse. If independently
confirmed labels disagree on wet versus dry for one exact unit, report
generation fails closed; contradictory truth is not averaged into skill.

These are overlap/audit counters, not causal correctness labels. The current
`WetDryLabel` says that a physical site was wet or dry in an interval. It does
not say which cell, track or storm caused that state. Therefore:

```text
cross_storm_correctness_evaluable = false
cross_storm_correctness_external_gate =
  requires_independent_causal_cell_or_track_identity_labels
```

The verifier never calls a cross-track overlap a false cross-storm
confirmation and never claims that the reuse was correct. That requires
independently labelled causal cell/track identity.

## Explicit remaining external gates

This foundation cannot honestly complete the following work from the current
wet/dry label contract alone:

- independently labelled causal cell/track/storm attribution;
- cross-storm false-confirmation rate;
- leave-one-physical-site-out validation without a preregistered site split;
- station-distance/representativeness verification without immutable site
  geometry and an explicit spatial sampling design;
- virga/radar-ground disagreement classification without independent
  precipitation aloft/surface labels;
- probability calibration because the categorical candidate contract stores
  no calibrated probability;
- confidence intervals, release thresholds and practical margins without a
  preregistered statistical protocol and independent event corpus;
- original frame-valid-time equality until that time is retained in the split
  evidence contract;
- production claims until real held-out events and labels are ingested and the
  shadow report is run.

## Tests

`tests/test_ground_verification.py` covers:

- exact HA/worker mirror equality;
- exact successful admission;
- site/time mismatch exclusions;
- ambiguous/unknown truth remaining non-dry;
- independent-source enforcement;
- dry-capability enforcement;
- partition and event/frame fail-closed behavior;
- unequal frame counts with equal event weighting;
- separate unknown/unavailable counts and coverage;
- provider/capability strata with unsupported rates left `None`;
- explicit exclusion accounting;
- duplicate-site and cross-track overlap auditing;
- exact-unit grouping across distinct label IDs and fail-closed contradictory
  external truth;
- rejection of duplicate candidate IDs and mixed algorithms.

The focused suite is intended to run with warnings promoted to errors:

```text
python -m pytest tests/test_ground_verification.py -q -W error
```
