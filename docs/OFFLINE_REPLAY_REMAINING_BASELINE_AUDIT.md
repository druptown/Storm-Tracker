# Remaining-baseline executability audit

This local audit answers one narrow question: can the three roadmap baselines
that are still absent from the deterministic nowcast replay be executed
honestly from the **same immutable replay bytes** with the code and runtime
contract currently present in this repository?

The answer for the current replay contract is **no for all three**. This is a
fail-closed result, not an implementation substitute and not admission proof.

## What actually runs

The audit first calls the existing primary/repeat runner. Across the 20-frame
fixture it therefore invokes the real worker functions for all 19 adjacent
frame pairs in both executions:

- Eulerian persistence: 38 calls;
- two-frame motion derivation: 38 calls;
- constant-motion translation: 38 calls.

The three decisions below are executability preflights only. Their call count
is exactly zero and their output hashes stay `null`.

## Exact decisions

### `last_released_vnext`

The current-tree `nowcast_baselines.py` is content-addressed as related source,
but the replay pack binds neither a released executable artefact, a deployment
authority nor a preregistration source for the last released vNext. A local
candidate or similarly named archive cannot silently become release authority.

### `legacy_engine`

The real legacy `build_precipitation_status`, `Storm`, `StormEngine` and
trajectory sources are parsed, symbol-checked and content-addressed. The replay
pack contains metric raster fields, not the legacy sequence of Storm states,
target coordinates, deterministic clock contract or a preregistered
raster-to-Storm adapter. Inventing those inputs would simulate a new baseline
rather than replay the production legacy engine.

### `pysteps_deterministic_extrapolation`

The existing pySTEPS challenger source and worker requirements are audited.
The challenger exposes the stochastic STEPS and LINDA paths, not the roadmap's
official deterministic-extrapolation baseline. The replay also lacks a dense
motion contract and pySTEPS-specific preregistration. The current worker
requirements do not declare pySTEPS; a locally installed distribution, if any,
is recorded as a runtime fact but never treated as an adapter or input contract.

The masked-translation reference and pySTEPS preflight observer are therefore
not relabelled as official pySTEPS output.

## Run locally

From the repository root:

```powershell
python scripts/audit_offline_replay_remaining_baselines.py `
  --replay-pack tests/fixtures/offline_replay_nowcast_baseline_input.json
```

The command writes one canonical JSON receipt to standard output. It performs
no network call and writes no files.

## Safety and authority boundary

The receipt always keeps these values false:

- `offline_science_gate_passed`;
- `meteorological_admission`;
- `live_wiring_allowed`;
- `dashboard_publication_allowed`;
- `home_assistant_state_mutated`.

It also requires `closed_remaining_baselines=[]`, retains all three identifiers
under `unclosed_remaining_baselines`, and rejects any nonzero call count or
fabricated output SHA for them. Closing a blocker later requires a new immutable
replay contract with the missing authority/input bindings and a real
primary/repeat execution path.
