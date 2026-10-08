# Oldenburg target-runtime106 assessment

The causal deployed-runtime control completed all 163 original issue times using
66 native KNMI fields in 2554.3 seconds. The new issued-interval scorer and five
boundary tests are retained in `scripts/oldenburg_target_score.py` and
`scripts/test_oldenburg_target_score.py`. They do not modify production or rerun
motion. Five tests failed before implementation and passed afterward.

Full evidence is retained at
`/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008/target-runtime106/report.txt`
and `issued-interval-metrics.json`, alongside immutable replay results. 437 old
artifacts, 69 unique used native array payloads and 161 offered motion pairs were
verified. Original predictions remain comparison baseline, not observation truth.

On identical later radar checks >=0.1 mm/h, 5–120 minutes ahead, the new issued
field intervals cover 604 wet checks versus 68 for original passages, but produce
154 wet-on-dry checks versus 26. These dependent pairs from one event do not
establish a calibrated accuracy percentage. Unknown/censored outcomes remain
unknown. All issued windows are scored; primary/held timestamps are preserved
separately, raw tracer scores remain separate.

The 00:43 issue predicts 01:47 arrival, matching the weak >0 radar leading bracket
01:45–01:50 but 8–13 minutes early against the actual >=0.1 threshold. Support
disappears around 00:59–01:21. The 01:49 issue ends at 03:47, far earlier than
the later observed >=0.1 rear bracket 05:20–05:30. Late issues approach the rear
edge. Two-hour forecasts cannot establish a four-hour-future end at early issues.

Conclusion: usable causal full-field wiring and improved wet-warning coverage,
but insufficient stability/end-time quality and increased false wet expectations.
Next work must address local motion support and early fragmented rear estimates,
then repeat a fixed benchmark and verify an independent event. No parameter sweep,
probability claim, whole scheduler/multiprovider qualification or production change
was made during this assessment.
