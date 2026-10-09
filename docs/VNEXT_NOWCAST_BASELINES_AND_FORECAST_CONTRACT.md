# vNext deterministic baselines and tracker forecast contract

Status: pure local/shadow foundation. Een kleine, additieve scheidingsadapter
is zichtbaar als technisch sensorattribuut `forecast_semantics_shadow`. De
adapter verandert de bestaande sensorstate, legacy-nowcast, automatiseringen
en worker service niet.

This implements the first auditable part of roadmap tickets `P1-N01` and
`P1-N04`. It does **not** establish meteorological skill and does not make
either ticket production-ready.

## Files and mirror rule

The following modules are byte-identical in the Home Assistant and standalone
worker trees:

- `engine/nowcast_baselines.py`;
- `engine/tracker_forecast_contract.py`.

The two roots are:

- `custom_components/storm_tracker_v3/`;
- `storm_tracker_worker/tracking_core/`.

Tests assert that de baseline nergens live wordt geïmporteerd. Het
forecastcontract wordt uitsluitend door `sensor.py` gebruikt voor de
additieve shadowadapter; integration startup, legacy `engine/nowcast.py`,
worker service en tracking-core package export importeren het niet.

## P1-N01 deterministic baseline contract

### Metric input field

`MetricRasterField` requires:

- a stable `field_id`;
- the real field-valid time as UTC Unix seconds;
- physical quantity and unit;
- an existing `AnalysisGrid`, including projected CRS, metric pixel size,
  origin, orientation, width, and height;
- immutable finite numeric raster values;
- an immutable boolean nodata mask of exactly the same dimensions.

The mask is authoritative. A masked storage value is never interpreted as
zero rain or dry evidence. Baseline output canonicalizes masked storage values
to `0.0` while retaining `valid_mask=False`; consumers must use the mask.

### Eulerian persistence

For positive lead time `dt`:

```text
R_forecast(x, t + dt) = R_observed(x, t)
```

The implementation copies quantity, unit, grid, values, and nodata support. It
records reference time, target time, exact lead seconds, source field ID,
method, and algorithm version. There is no motion or probability metadata.

### Two-frame constant-motion / Lagrangian persistence

`ConstantMotionVector` requires exactly two unique support fields and two
strictly increasing real valid times. Its reference time must equal the newest
support time. The forecast source field must be that newest support field.

`derive_two_frame_constant_motion` computes:

```text
vx = (x_current - x_previous) / (t_current - t_previous)
vy = (y_current - y_previous) / (t_current - t_previous)
```

It uses the actual interval, including irregular frame spacing. Any finite
result is preserved. There is no speed threshold, cap, clipping, smoothing, or
silent rejection.

The Lagrangian baseline applies:

```text
R_forecast(x, t + dt) = R_observed(x - u * dt, t)
```

with backward semi-Lagrangian sampling. Sampling is bilinear and
`strict_nodata`:

- every contributor with non-zero weight must be in bounds and valid;
- otherwise the target pixel is nodata;
- outside-grid areas are nodata, never extrapolated or zero-filled;
- eastward `vx` and northward `vy` follow the `AnalysisGrid` SI-axis contract.

The method is an intensity-persistence translation reference. Bilinear
resampling is not mass-conservative, and the model contains no growth, decay,
rotation, deformation, initiation, dissipation, or uncertainty model. Those
are deliberate baseline limitations, not hidden corrections.

### Baseline scientific invariants

- Only real caller timestamps determine elapsed time.
- All motion is `m s-1`; all grid coordinates and resolution are metres.
- Grid geometry and motion grid ID must match.
- Nodata never becomes dry precipitation evidence.
- No baseline emits or estimates a probability.
- Both output types are frozen dataclasses with immutable nested tuples.
- Every output carries input IDs and versioned method metadata.

## P1-N04 immutable tracker forecast contract

`TrackerForecast` describes a local result at one tracker coordinate on one
analysis grid. It records:

- forecast and tracker IDs;
- state: `DETERMINATE` or `INDETERMINATE`;
- metric tracker coordinates and grid ID;
- reference timestamp, forecast-valid timestamp, explicit lead time, and
  explicit valid horizon;
- all input track IDs and field IDs;
- forecast algorithm version and an optional real calibration version;
- passage and no-passage probability;
- probability evidence;
- ETA quantiles conditional on passage plus an explicit stored-quantile
  interval;
- local tracker-intensity quantiles conditional on passage;
- explicit indeterminate reason codes.

The contract deliberately has no system-maximum intensity field. The local
intensity quantity is attached to the tracker forecast and must be supplied by
an upstream local sampler/ensemble.

### Accepted passage probability origins

Only two evidence bases exist.

1. `ENSEMBLE_FREQUENCY`
   - requires ensemble sample size and passage-member count;
   - passage probability must equal `passage_count / sample_size`;
   - no-passage probability must be its explicit complement;
   - a raw ensemble frequency cannot claim a calibration version.
2. `EMPIRICAL_CALIBRATION`
   - requires sample size, source ID, source algorithm version, evidence ID,
     and a non-empty calibration version;
   - the forecast calibration version must match it;
   - it cannot masquerade as an ensemble member count.

There is no evidence basis for a confidence label, provider quality score,
heuristic threat score, distance score, or UI percentage. Such values cannot
be inserted through the enum-typed contract.

### Conditional distributions

ETA and local-intensity distributions require:

- an explicit ensemble-member or empirical-sample evidence basis;
- sample size, source ID, source algorithm version, and distribution ID;
- at least three strictly ordered probability levels;
- non-decreasing quantile values and an explicitly stored median;
- `conditional_on_passage=True`.

Empirical distributions require a real calibration version which must match
the forecast version. ETA uses `eta_after_reference` in seconds, cannot be
negative, and cannot extend past the stated forecast horizon. The explicit ETA
interval must point to quantiles actually present in the distribution; the
contract never interpolates an unstored interval endpoint.

When passage probability is exactly zero, ETA and intensity conditional on
passage are undefined and must be absent. Any positive passage probability
requires both conditional distributions and the ETA interval.

### Fail-closed indeterminate state

An `INDETERMINATE` record requires one or more unique reason codes, for example
`stale_input_field`, `missing_ensemble`, or `outside_empirical_horizon`.

It cannot expose partial passage numbers, ETA, or intensity products. This
prevents missing evidence from becoming `0%`, “no rain”, or a fabricated ETA.
A `DETERMINATE` record cannot carry indeterminate reasons.

## P0-F01 additive current/nearby/next/passage separation

`TrackerStatusSeparation` publiceert vier onafhankelijke feiten:

1. `current_precipitation`
   - combineert uitsluitend twee expliciete positieve bewijsbanen:
     `CurrentFootprintEvidence` en `CurrentGroundEvidence`;
   - radar-evidence scant alle losse stormcontouren uit het nieuwste
     radarframe;
   - `frame_valid_at`, `evaluated_at`, signed `frame_age_seconds`, de
     freshnesslimiet en policy-ID worden samen gevalideerd; een toekomstig
     of stale frame kan nooit current worden;
   - radar-`absent` vereist een verse, typed en volledige footprintscan met
     inputaantal en SHA-256 van de volledige scan;
   - afgeronde dashboardafstanden worden nooit als inside-polygonbewijs
     gebruikt;
   - alle gelijktijdig bevattende storm-ID's blijven behouden;
   - grond-evidence is een positive-only typed record met provider,
     physical-site-ID, bronrecord, algoritmeversie, observatie- en
     evaluatietijd, signed leeftijd, providergebonden freshnesslimiet en
     policy-ID, trackerafstand en optionele systemmatch;
   - een provider-record-ID blijft als zodanig gemarkeerd; waar die ontbreekt
     wordt expliciet `derived_station_timestamp` opgeslagen;
   - Netatmo-identiteit bewaart de volledige MAC. Afstandsgebaseerde
     deduplicatie is uitsluitend conservatief en krijgt
     `proximity_conservative_dedup`; ze claimt geen station-alias;
   - een generic `ground_precipitation_observed=True`, confidencepercentage,
     stale rij, droge rij of fysieke dubbelmeting is geen bewijs;
   - een verse lokale grondwaarneming kan actuele neerslag vaststellen wanneer
     de radarcontour nog 2,5--3,7 km naast de tracker ligt, zonder te beweren
     dat die contour de tracker bevat.
2. `nearby_precipitation`
   - is ruimtelijke context buiten de actuele trackerfootprint;
   - bevat een afzonderlijke storm-ID en afstand;
   - blijft tegelijk zichtbaar bij positieve grondregen en een radarcontour
     buiten de tracker; de radar/grond-discrepantie wordt dus niet weggepoetst;
   - wordt niet als actuele regen of passagekans geïnterpreteerd.
3. `next_threat`
   - is een afzonderlijke deterministische motioncandidate met `hit` of
     `edge`;
   - een systeem dat de tracker nu bevat, wordt niet nogmaals als volgende
     bui geselecteerd;
   - `not_detected` vereist een verse volledige typed footprintscan, een
     volledig parseerbare candidatecollectie en exacte dekking van alle
     geevalueerde storm-ID's;
   - malformed, ontbrekende, stale of resource-begrensde candidatedata blijft
     `indeterminate`; `not_detected` is geen `0%` passagekans.
4. `passage_forecast`
   - blijft `indeterminate` zolang geen geldig `TrackerForecast` beschikbaar
     is;
   - alleen het formele contract kan passage- en no-passageprobabilities
     invullen;
   - legacy confidence-, kwaliteits- en afstandsscores worden genegeerd.

De afgeleide `display_state` gebruikt uitsluitend deze shadowprioriteit:

```text
positive current radar or ground evidence
    > separate next threat
    > nearby radar precipitation
```

Daardoor blijft actuele trackerregen `current_precipitation` bij een exacte
radarfootprint of geldige lokale grondwaarneming, ook wanneer de bewegingslaag
voor dezelfde contour `mist` rapporteert. De shadow bewaart afzonderlijk
`current_precipitation.radar_footprint.state`, alle typed `ground_evidence`,
de positieve bewijsbasis en de geassocieerde storm-ID's. `mist` blijft apart
zichtbaar als `legacy_selected_relation=miss` en beschrijft alleen de
geprojecteerde toekomstige baan.

Home Assistant publiceert dit voorlopig alleen als compact genest technisch
attribuut. `native_value`, bestaande attributen, waarschuwingen en
automatiseringen blijven ongewijzigd. Een operationele omschakeling vereist
nog live HA-validatie en een formele passageforecastbron.

Alle potentieel grote collecties zijn uitvoerbegrensd. De shadow bewaart
`input_total_count`, `inspected_count`, accepted/malformed/serialized/overflow
counts, een SHA-256 over de volledige invoer en een kleine vaste set
reason-codes. Grondassessmentregels worden eveneens begrensd, waarbij verse
lokale natte onafhankelijke regels eerst worden bewaard. Begrenzing mag
positieve regen niet stil weggooien en mag nooit droog of `not_detected`
betekenen.

## Test coverage

`tests/test_nowcast_baselines_contract.py` covers:

- exact timestamp/lead arithmetic;
- irregular two-frame intervals;
- metric east/north translation;
- bilinear half-pixel translation;
- strict nodata propagation and outside-domain behavior;
- an intentionally extreme finite uncapped motion vector;
- grid, timestamp, support-ID, and immutability failures;
- exact ensemble passage/no-passage arithmetic;
- rejection of heuristic evidence and fake ensemble calibration;
- empirical calibration requirements;
- conditional ETA interval and horizon checks;
- explicit indeterminate behavior;
- exact current-frame footprintbewijs dat niet door legacy `mist` kan worden
  overschreven;
- typed positieve Netatmo/human-compatible grond-evidence die `mist` evenmin
  kan overschrijven en geen radarcontainment fabriceert;
- expliciete radar-`absent` plus ground-`present` discrepantie bij een contour
  op 3,1 km;
- fail-closed weigering van stale, droge, generieke, niet-geprovenanceerde,
  gedupliceerde of niet-canonieke grond-evidence;
- signed-age regressies voor future/stale radar- en grondrecords en weigering
  van grondproviders zonder expliciete freshnesspolicy;
- volledige Netatmo-MAC-identiteit en order-/tracker-onafhankelijke
  conservatieve nabijheidsdeduplicatie zonder aliasclaim;
- radar-`absent` en next-`not_detected` alleen na verse volledige typed scans;
- malformed en 5.000-record candidate-input blijft begrensd en
  `indeterminate`;
- 40 typed grondrecords en 5.000 lokale assessmentregels blijven begrensd,
  gehasht en behouden positieve lokale regen;
- sensoradmissie alleen uit verse onafhankelijke lokale assessmentregels,
  inclusief herberekening van de bestaande 3-km/twee-site
  representativiteitsregel;
- afzonderlijke current, nearby, next-threat en formal-passagevelden;
- weigering om afgeronde afstand `0.0` als footprintbewijs te gebruiken;
- current plus een andere naderende bui zonder identityvermenging;
- uitsluitend formeel gevalideerde probabilities in de passage-shadow;
- additieve sensorwiring zonder wijziging van `native_value`;
- byte-identical mirrors and absence of live imports.

The tests run with warnings treated as errors.

## Remaining real-data and pySTEPS gates

This foundation still lacks:

- a provider-certified adapter from normalized radar artifacts into
  `MetricRasterField` with lossless provenance;
- a real two-frame global translation estimator benchmark. The vector contract
  accepts a measured vector but does not pretend to infer one;
- held-out event replays comparing Eulerian and translation baselines at the
  same provider/region/lead/threshold strata;
- pySTEPS deterministic extrapolation and optional deterministic S-PROG;
- STEPS ensemble generation, reproducible seed policy, perturbation audit, and
  NUC multicore/runtime measurements;
- independently verified passage labels, local intensity labels, Brier score,
  reliability, sharpness, CRPS/quantile loss, ETA error, and confidence
  intervals;
- empirically justified horizons and real versioned calibration;
- storage/evidence wiring for nowcast runs and tracker forecasts;
- een producer van formele `TrackerForecast`-records voor de sensoradapter;
- de oorspronkelijke kwantitatieve grondmeetwaarde en een native
  provider-record-ID waar de provider die niet levert; absolute
  observatietijd wordt nu lossless bewaard en ontbrekende native record-ID's
  zijn expliciet als afgeleid gemarkeerd. De shadow claimt daarom voorlopig
  alleen precipitation presence, geen niet-bewaarde rain rate;
- live HA-validatie dat het extra attribuut recorder-, event-loop- en
  dashboardgrenzen niet overschrijdt;
- shadow soak, fault injection, release gates, operationele omschakeling en
  deployment.

Until those items exist, the code provides fair reference formulas and a
fail-closed schema—not evidence that forecasts are more accurate.
