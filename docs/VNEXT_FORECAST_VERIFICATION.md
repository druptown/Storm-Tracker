# vNext forecast verification

Status: pure local/shadow verification foundation. It is not imported by the
Home Assistant integration, legacy nowcast, tracking-core package export, or
worker service.

Implementation:

- `custom_components/storm_tracker_v3/engine/forecast_verification.py`
- `storm_tracker_worker/tracking_core/engine/forecast_verification.py`

Those files are byte-identical. The implementation has no pySTEPS runtime
dependency. The official pySTEPS verification reference confirms that its
verification package covers deterministic, probabilistic, ensemble, and
spatial scores, and its FSS API defines an intensity threshold and a moving
window scale in pixels:

- <https://pysteps.readthedocs.io/en/stable/pysteps_reference/verification.html>
- <https://pysteps.readthedocs.io/en/stable/generated/pysteps.verification.spatialscores.fss.html>

Our implementation documents its own masking, boundary, aggregation, and
bootstrap conventions because these are essential for reproducibility and are
not inferred from a library default.

## Truth and partition admission

The builders accept the current worker `ExternalTruthContext` surface without
importing or modifying `validation_corpus.py`.

A passage case is admitted only when:

- event and frame exist and belong to the caller-selected partition;
- frame belongs to the selected event;
- exactly the requested label exists;
- label kind is `PASSAGE`, state is `CONFIRMED`, and outcome is not unknown;
- label source is eligible independent external truth;
- tracker ID and evaluation window equal the forecast tracker and horizon;
- candidate forecast is `DETERMINATE`;
- candidate and explicit baseline share event, frame, partition, tracker,
  reference time, horizon, and evaluation stratum;
- candidate and baseline forecast IDs differ.

A precipitation field case additionally requires:

- a confirmed `PRECIPITATION_FIELD` label;
- independent label-source eligibility;
- exact label/truth artifact SHA-256, grid ID, valid time, variable, and unit;
- candidate and truth have identical `AnalysisGrid` geometry;
- candidate and truth valid time, variable, unit, and nodata masks match;
- at least one valid pixel;
- candidate and truth artifact IDs cannot be identical when the candidate
  supplies an artifact ID.

Missing, ambiguous, unknown, mismatched, or engine-derived truth raises an
error. It is never silently excluded, changed to dry, or assigned outcome zero.

## Event-level aggregation

All scalar scores first average forecast cases within each meteorological
`event_id`. The final point estimate is the unweighted mean of those event
means. A long event with many frames therefore cannot outweigh a short event.

Inputs in one report must share the explicit evaluation stratum, partition,
candidate algorithm and calibration version, lead, horizon, probability
evidence basis/member count/source algorithm, and baseline algorithm. FSS also
requires one exact grid geometry, variable, unit, candidate algorithm, truth
algorithm, and truth source.

Raw case counts remain visible beside independent event counts.

## Passage probability metrics

For event `e` containing cases `i`, candidate Brier score is:

```text
BS_candidate =
  mean_over_events(
    mean_over_cases_in_event((p_i - o_i)^2)
  )
```

where `o_i` is an independently confirmed binary passage outcome. The explicit
baseline is evaluated on the identical cases:

```text
BSS = 1 - BS_candidate / BS_baseline
```

There is no fallback baseline. An exactly zero baseline Brier denominator is
undefined and fails closed.

Reliability bin edges are caller-supplied, immutable, strictly increasing, and
must cover exactly `[0, 1]`. Bins are left-closed/right-open except the final
bin, which includes probability one. Every bin is returned, including empty
bins. Each bin reports:

- lower/upper edge and endpoint convention;
- raw sample count;
- independent event count;
- event-weighted forecast mean;
- event-weighted observed frequency;
- `None` means for an empty bin.

Sharpness is reported as the event-weighted forecast-probability distribution
over the same bins plus its event-weighted mean and variance. It is a
distribution/concentration property, not a skill score.

False and missed passage use an explicit caller-supplied decision threshold:

```text
predicted passage := p >= decision_threshold
```

Raw true/false/missed case counts are retained. False-passage and missed-
passage rates first calculate the conditional rate inside each relevant event
and then average events equally. Both observed passage and observed
no-passage cases are required.

ROC uses every distinct forecast probability as an empirical operating
threshold plus an explicit no-positive endpoint. True-positive and
false-positive rates are first calculated inside each event containing the
relevant truth class and then averaged equally across those events. AUC is the
trapezoidal area through those monotone operating points. It is supplementary
discrimination evidence: the implementation neither selects an operating
threshold nor turns AUC into a release decision. The current AUC is a point
estimate; an uncertainty interval for AUC remains open.

## Conditional ETA metrics

ETA verification uses only cases where independent truth confirms that passage
actually occurred. No-passage cases are counted as excluded and never receive
an artificial ETA or penalty.

Actual ETA is:

```text
actual_eta_seconds = passage_time_utc - forecast.reference_valid_at
```

Reported event-weighted metrics are:

- median ETA mean absolute error;
- median ETA signed bias (`forecast - observed`);
- pinball loss for every explicitly stored forecast quantile;
- inclusive coverage of the explicitly stored ETA interval;
- mean interval width.

Pinball loss for quantile level `q`, forecast quantile `x`, and observation `y`
is:

```text
L_q =
  q * (y - x)             when y >= x
  (q - 1) * (y - x)       when y < x
```

All cases in a report must use identical quantile levels, interval levels,
distribution basis, quantity, unit, calibration version, and source algorithm.
The caller-supplied minimum number of actual-passage events is enforced. A
positive passage outcome without a conditional ETA distribution is an error.

## Complete scalar-ensemble verification

`ScalarEnsembleVerificationCase` accepts complete scalar ensemble members and
an independently confirmed point observation. It can therefore verify local
rain intensity, ETA, or another explicitly identified scalar quantity without
reconstructing a distribution from a few quantiles. Admission requires:

- at least two finite member values with unique member IDs;
- an explicit assertion that members are exchangeable;
- one exact quantity, unit, member count, generation algorithm, calibration
  version, evaluation stratum, partition, and independent truth source per
  report;
- a confirmed independently eligible truth label and truth-artifact SHA-256;
- distinct candidate and truth artifacts when the candidate identifies its
  artifact.

For ensemble members `x_1 ... x_M` and observation `y`, empirical CRPS is
computed exactly as:

```text
CRPS = mean_i(|x_i - y|)
       - (1 / (2 M^2)) * sum_i sum_j |x_i - x_j|
```

The report also contains ensemble-mean MAE, ensemble-mean signed bias, and all
`M + 1` rank-histogram bins. A truth value tied with `k` members contributes
equal fractional mass to the `k + 1` compatible ranks; no pseudo-random
tie-breaking can change a replay. Scalar scores and rank mass are averaged
first per event and then across events. MAE, bias, and CRPS receive the same
deterministic event-bootstrap interval as the other scalar metrics.

The current live `TrackerForecast` contract stores distribution summaries, not
the complete member vector. This verifier therefore remains shadow-only until
complete members and independent local-intensity truth are durably wired. It
will fail closed rather than approximate CRPS or a rank histogram from p10,
p50, and p90.

## Masked fractions skill score

FSS configuration is entirely explicit:

- physical intensity threshold;
- threshold operator (currently `>=`);
- window rows and columns in pixels;
- minimum valid fraction per window.

Only full rectangular windows entirely inside the grid are evaluated; there
is no edge padding. Candidate and truth masks must be identical. Within an
eligible window, only valid pixels contribute:

```text
f_W = count(candidate >= threshold and valid) / count(valid)
o_W = count(truth     >= threshold and valid) / count(valid)
```

Per case:

```text
FBS_case = mean_W((f_W - o_W)^2)
REF_case = mean_W(f_W^2 + o_W^2)
```

These two components are averaged first within events and then equally across
events:

```text
FSS = 1 - mean_event(FBS_event) / mean_event(REF_event)
```

The report includes eligible and mask-excluded window counts plus the metric
window width/height derived from the exact grid resolution. No eligible
windows or an exactly zero reference denominator fails closed.

## Deterministic event bootstrap

Bootstrap configuration has no defaults. The caller must supply:

- a non-empty seed;
- number of replicates;
- lower and upper interval probabilities;
- minimum independent event count.

For each replicate, SHA-256 maps `(algorithm_version, seed, replicate, draw)`
to an index in the sorted event-ID list. It draws exactly the number of
independent events with replacement. It never draws frames/cases directly.
Input order therefore cannot change the result.

Reports store the seed SHA-256, replicate count, event count, interval levels,
point estimate, interval endpoints, and bootstrap algorithm version. BSS and
FSS use paired event resampling. If a paired bootstrap replicate has a zero
reference denominator, the interval is undefined and the run fails closed.

Bootstrap intervals describe sampling uncertainty only. They are not release
margins and the module contains no pass/fail threshold.

## Deliberately absent

This foundation does not yet provide:

- multivariate energy score, threshold-specific local-intensity Brier scores,
  SAL, or object-based spatial scores;
- an AUC confidence interval or formal comparison of correlated ROC curves;
- block bootstrap for demonstrably dependent meteorological events;
- uncertainty propagation from passage-time label uncertainty;
- observation-error-aware FSS;
- real held-out labels, preregistered strata, event sample-size decisions, or
  release margins;
- durable storage/wiring of complete ensemble members and independent scalar
  truth for the new CRPS/rank verifier;
- provider/region/lead calibration and drift monitoring;
- storage wiring, live imports, deployment, or a release claim.

Those remain real-data and scientific-validation gates.
