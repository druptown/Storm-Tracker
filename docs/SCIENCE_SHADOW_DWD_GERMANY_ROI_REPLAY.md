# DWD Germany ROI science-shadow replay

## Status

This work is offline shadow evidence only. It does not activate a service,
Home Assistant, MQTT, live acquisition, admission, forecast publication or a
WeatherSystem rebinding.

The result is deliberately asymmetric:

- v4.1 completed all 36 sealed observation frames, including durable recovery
  across the explicit 20→21 and 32→33 parent-runner boundaries and an exact
  idempotent duplicate.
- v5 did **not** complete. The parent-enforced 60-second CPU limit killed frame
  19 after 60.377112 s. Its disposition was `not_committed`; the durable head
  remained revision 18. There was no retry, cap increase or parameter tuning.

Consequently, v5 is a resource-negative challenger. It is not a tracking
result and cannot be compared with v4.1 as if both had completed the corpus.

## Immutable observations

The corpus consists of 36 official DWD RADOLAN RV `+000` observations from
2026-08-07 05:10 through 08:05 UTC at a fixed 300-second cadence. Forecast
leads are excluded.

| Binding | SHA-256 / value |
|---|---|
| Source manifest | `20335e6c50b35d57dd111bea7e3670f515e933da5239ba39e74ccebc41030852` |
| Sequence manifest | `aaa9045acb6407c3ca714563c6293f5a66f9dd284d1fb00b508be4d1d991e7dc` |
| Sequence ID | `1a4f4e14a407832c9763251779de63f9ff683407d7f8fde6c54c8b8a0ad74fc4` |
| Metrics manifest | `78430d02543bc3b87062c0e93db8a8cd72bd622f7a63e6d5e2360bdbdd524ccd` |
| Ordered evidence-record concat | `9fac976800e063fb44da4d1344f6daec1aba4ecd53ccf0d908cf55d94eab4777` |
| Ordered run-ID list | `16f439ad52481c8086afc206ec2fda9ee9365e0fa4c27cb090632ce5b09e5ebf` |
| Target grid | `cf99c2b4c08a46bde9f74f55a0b690d8496ce7b44d4a0aa379f4132023cab636` |
| ROI profile | `83d54c7d0362ae230b944dcf4523f2346d35d8bbe40fd96ec4adcf7755c50a4d` |
| Detector | `f83700f116b36dab5633c649a0b96b8cf685fddce3e575fb2bac61bfc658f6fe` |
| Pipeline | `51dab0077b75c839e26a6634aa23327ea4df25a41b98b81d60adf77b3a64cc71` |
| Transform | `486877c02d3e89bc3284eba033e941a6e44569e8995b1bc4985d2471df78c303` |

The source-aligned ROI covers source rows 150..999 and columns 150..849.
The analysis grid has origin `(149500, -149500)` metres, 2-km cells, width
350 and height 425 (148,750 pixels). Each target pixel aggregates exactly one
2×2 source block by area. Nodata, undetect and mixed support are withheld;
they are never filled, interpolated or reclassified as dry.

This 2-km transform captures the fixed geographic ROI but changes cell and
core topology. In particular, 291 of 24,820 raw source pixels at or above
1 mm/h fell into mixed target cells and were withheld. This replay is not a
lossless 1-km identity reference.

## v4.1 durable result

The v4.1 policy fingerprint is
`bc082f619713a3a1cbb71665aca1cfb087a37a583c0432dd9065afe971683bfd`.
It has no hard speed or distance rejection and retains bounded top-k evidence
per transition, but no multi-scan posterior.

The completed durable state has:

- revision 36 and state SHA
  `da84e7f0bf0e13a8f2d7cf33f44b654c35d28cbdc92d7df7e9c12292da0b5c57`;
- 57 total tracks, 20 active and 37 ended;
- all 753 detected objects represented in track samples;
- median/mean/maximum track length 14 / 13.21 / 35 observations;
- 57 births, 37 deaths, 163 gap events and 64 reacquisitions;
- 19,847 completely accounted association candidates;
- 11 derived split and 4 derived merge alternatives, but no selected
  split/merge lifecycle event in the final ledger;
- exact candidate accounting on all 36 frames;
- checkpoint revision 20 plus a contiguous 16-frame recovery tail;
- receipt-chain SHA
  `86dcbb1448f23ac7067929c493a3f0de4bfdab081faee83cd98f8262e8334119`;
- receipt-store SHA
  `8229659c0ac773b640a736e1c149b05fd8835ec022ffc4337d8723ab9cfd4de4`.

The 696 selected interval hypotheses have a median speed of 32.44 km/h,
p95 633.62 km/h and maximum 1,637.99 km/h; 128 exceed 200 km/h. These are
association-hypothesis speeds, not measured meteorological truth. Their large
tail is negative continuity evidence and prevents an accuracy or admission
claim.

The final primary WeatherSystem snapshot contains seven descriptive systems.
It remains bound to the primary per-transition ledger. It was not rebound or
promoted by this replay.

## v5 fail-closed resource result

The v5 policy fingerprint is
`e382d571cd4e7c8fa93c4f6eeec5e97e6d744edd688623686f8e84198a4c07d8`.
Its authority SHA is
`0e07410bf8b79263fdc45f473e5e8c185f246e35686d1b210953d3023e4b4ded`.

Frames 1..18 committed. On frame 19 the process result was:

- `status=resource`;
- `execution_outcome=cpu_limit_exceeded`;
- `detail_code=hard_resource_limit`;
- child exit code `3758096385`;
- elapsed 60.377112 s;
- `durable_disposition=not_committed`;
- no receipt metadata and no operational or meteorological claim.

The v5 store remained at revision 18 with SHA
`af9cd4a89baa741cef855868ed898bbd2efdb09eb73fb9d29ed958d883fa5d26`.
All child processes were gone after termination. The fixed cap was not raised,
so this is valid negative resource evidence under the tested profile, not a
reason to silently weaken isolation.

## Independent evaluation references

Both references were fixed before tracker execution and never inspect tracker
output.

The first-to-last binary support centroid at `>=1 mm/h` moved 425.60 km over
175 minutes, a descriptive aggregate reference of 145.92 km/h. It combines
multiple evolving systems, births, deaths and deformation and therefore is
not a single-cell speed truth.

Masked full-shift normalized phase correlation produced a unique peak on all
35 intervals. The dominant result in 31 intervals was one 2-km pixel east and
zero pixels north/south per five minutes (24 km/h). Four intervals selected
very large shifts, but their overlap fractions were only 0.0365..0.0459.
Their 532.89..2,647.63 km/h values are explicitly low-overlap uncertainty,
not motion truth. Missing support never contributes zero-valued dry pixels.

The independent baseline receipt SHA is
`c9f2f5447856d1d494387d8cb54a6e099bcba59882ccb86bd53bab047202741a`.

There is no scientifically valid one-to-one comparison between those aggregate
raster references and individual v4.1 identity intervals. The evidence is
descriptive and exposes uncertainty; it is not a cost-tuning target.

## Resource and admission limits

The upstream 2-km evidence conversion was separately measured under a 1-GiB
cap, but those numbers are not tracker resource measurements. The full v4.1
runner collected per-frame resource samples in memory; the later v5 terminal
failure ended the combined command before those samples were durably written.
The sealer therefore does not invent v4.1 p50/p95/max resource values.

This instrumentation gap and the v5 CPU failure are production blockers. No
NUC/HA/Z2M cadence or thermal admission follows from this desktop replay.

## Evidence artifacts

- JSON:
  `.analysis/dwd_rv_germany_roi_2km_v4_complete_v5_resource_failure_20260808_evidence.json`
  — SHA `74b0f1678f8a1d1e5270bd1c21ec7ca773e231b0b316929bc74daac2f44332fd`.
- Markdown:
  `.analysis/dwd_rv_germany_roi_2km_v4_complete_v5_resource_failure_20260808_evidence.md`
  — SHA `eace489e1b654e20095b4259c8de46ad8f0344dd965ba00783b565cbc3a83d1c`.

## Open blockers

- independent identity and speed truth are unavailable;
- no held-out calibration exists for this 2-km detector/identity profile;
- 2-km aggregation changes object and core topology;
- mixed source support is withheld and reduces observable support;
- v4.1 retains an extreme speed/jump tail;
- v5 exceeds the fixed CPU cap before completing the corpus;
- successful v4.1 per-frame resource samples were not crash-durably persisted;
- WeatherSystem rebinding remains forbidden;
- operational admission certificates remain incomplete.

