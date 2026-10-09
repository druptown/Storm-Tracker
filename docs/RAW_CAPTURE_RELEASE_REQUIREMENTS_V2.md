# RAW capture release requirements v2

## Scope

`stv3-raw-capture-release-requirements-v2` is the portable fourth commit
marker beside the unchanged `stv3-release-evidence-v1` integration/worker
archive evidence. It does not install, materialize, activate, admit
meteorological output, or prove a live Home Assistant target.

The marker is built only with `--raw-capture-requirements` and the exact
sanitized Supervisor options snapshot. The normal builder mode continues to
produce the historical three-file v1 release candidate.

## Selected target profile

The only frozen v2 profile is `cadence_only`:

- `blitzortung_enabled` is `false`;
- `event_capacity_claimed` is `false`;
- the host operational free reserve is `2147483648` bytes;
- the service persistent envelope is `17733517312` bytes;
- the retention-guard persistent envelope is `18001952768` bytes;
- the complete same-filesystem floor is `20694695936` bytes.

The portable marker contains exactly five non-path materializer arguments:
the profile switch, capture window, catalog and activation-matrix hashes, and
host reserve. The target path, target capacity declaration, and output roots
remain target-side inputs and are never smuggled into the portable evidence.

## Frozen contracts

The v2 pinset binds the activation materializer, capacity artifact, target
manifest, service config and runtime contracts, plus retention guard v2 and
its fail-closed capacity policy. It also binds every cache-filtered worker
package member by path, byte count and SHA-256. That complete membership claim
is deliberately not an import-closure or executability claim.

All portable readiness and output claims remain false: deployment, deployment
bundle completeness, live-target verification, target activation,
meteorological admission/output, dashboard output, and production enablement.
Target materialization and the live/canary checks remain separate required
steps under `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`.
