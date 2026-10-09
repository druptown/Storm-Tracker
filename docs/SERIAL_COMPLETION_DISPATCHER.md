# Serial completion dispatcher (inert candidate)

Status: deliberately **unwired**.  Nothing in service, main, Home Assistant,
MQTT, recovery, or the terminal router imports or constructs this dispatcher.
It is not a production-enable authority.

## Closed route authority

`completion_route_adapters.py` derives one canonical eleven-route catalogue
from `TERMINAL_RAW_CONSUMER_CATALOG`.  Its SHA-256 is:

`78b6d12fa4642a05d4316698dcdef06e42a442fa28b6facdda803471dd10b4a5`

The exact order is:

1. `ground.metar.global`
2. `radar.aemet.composite`
3. `radar.dpc.sri`
4. `radar.dwd.radolan_rv`
5. `radar.knmi.rtcor`
6. `radar.meteofrance.dpradar`
7. `radar.metoffice.uk_rate`
8. `satellite.goes18.rrqpe`
9. `satellite.goes19.rrqpe`
10. `satellite.hsaf.h40b`
11. `satellite.noaa.err`

The dispatcher copies that exact map into a `MappingProxyType`, pins every
adapter object/type/route/binding/lease/profile/consumer/clock identity, and
rechecks it before claim and phase invocation.  Each adapter also binds all
known mutable raw/artifact/evidence/output stores by canonical path and
`(st_dev, st_ino, st_mode, st_nlink)`.  Regular store files must have exactly
one link.  The lifecycle root/database and desired-state database receive the
same canonical, type, symlink, link-count, and runtime-witness preflight.
Those bindings are part of the route profile,
outer composition authority, dispatcher profile, owner anchor, and SQLite
manifest; they are rechecked on every tick.  Paths and witnesses never appear
in diagnostics or tick results.

An inode witness detects same-path replacement, but not an in-place valid
reset that preserves the inode.  The lifecycle and desired-state stores now
each contribute an immutable persistent instance identity to the outer
composition authority and revalidate it on every store connection.  Every
completion output store now exposes the equivalent sentinel, and every route
profile must contain at least one persistent `*.output_store` instance
authority or dispatcher construction fails closed.

## Adapter contract

| Route kind | Dispatcher call | Heavy phase scheduling |
|---|---|---|
| METAR, KNMI, Met Office | exact parent-held lease to `process_leased` | one `single` phase |
| DWD RV | exact parent-held lease to `process_leased`, then `DwdRvPipelineOutcomeAuthority` | one `single` phase |
| NOAA ERR | `begin_leased`, then `finalize_leased` on a later tick | `initial`, then `final_reproof` |
| six raw-manifest audits | `begin_leased`, then `finalize_leased` on a later tick | `initial`, then `final_reproof` |

No adapter calls a standalone `run_once`.  In particular, DWD's older
standalone `run_once` claims its own lease and accepts a caller-supplied desired
state.  It cannot prove dispatcher-wide round-robin ownership, the exact
parent-held lease, three fresh desired/lease barriers, cancellation/deadline
after final reproof, or the dispatcher's one-child permit.  The DWD adapter
therefore calls only `process_leased` and verifies the resulting durable output
through `DwdRvPipelineOutcomeAuthority` before acknowledging.

`HeavyChildPermit` is one-shot and binds exact workload, phase, tick SHA, and a
non-serializable nonce.  The dispatcher accepts a result only when
`permit.consumed == (child_invocations == 1)`.  For NOAA/raw split consumers,
the permit is consumed by an exact `before_child` callback, so recovered output
or a desired-state rejection reports zero.  Existing single-phase consumers
do not expose that callback: their value of one is deliberately a conservative
upper bound on a child-capable `process_leased` call.  Actual heavy children
can never exceed the reported value.

## Durable scheduling and crash behavior

One lifetime process claim plus an OS byte lock fences ownership.  `close()`
first requests stop and waits boundedly for tick quiescence.  A non-cooperative
child keeps the OS/process fence held; a second owner remains busy.

The bounded SQLite state contains only counters, the next route index, owner
epoch, integrity fence, and an optional
workload/event-SHA/stream-sequence/phase/expiry marker.
It never contains an event ID, lease token, payload, pending object, source
path, coordinates, or child diagnostics.  State and manifest are checksum and
schema verified under an 8 MiB physical cap.

Each tick does one of the following:

- run one retained in-memory `final_reproof` continuation;
- scan at most eleven routes in durable round-robin order and invoke at most
  one adapter phase; or
- return idle/busy/stopped/blocked.

NOAA/raw initial output is durable before a continuation marker changes from
`initial` to `final_reproof`.  The private continuation and lease token remain
memory-only.  After process loss, an unexpired lifecycle lease is not
recreated.  On reclaim, `begin_leased` reuses exact durable output with zero
initial child; a later tick performs exactly one final reproof.  A terminal
event left behind by a crash does not stall its route: the lifecycle's bounded,
checksum-verified `terminal_event_identity(stream, sequence)` proves consumed,
rejected, quarantined, superseded, or archived terminality before the old
marker is counted and cleared.  A non-terminal unexpired crash marker is never
overwritten by a different claim.

The guarded wall clock rejects process-local or restart rollback below the
last durable dispatcher transition.  Increasing observations are folded into
the existing tick/claim/continuation/finish/idle commit, avoiding one FULL
SQLite fsync for every idle probe.  On acquire/restart only, it also reads the
lifecycle store's O(1) durable monotone clock watermark.  Therefore an ACK
committed immediately before a dispatcher-state crash cannot hide a higher
wall time on restart; the lifecycle scan is not repeated on each tick.

## Terminal and failure semantics

Adapter and dispatcher result constructors enforce closed status/phase sets,
route/event pairing, terminal kind, continuation identity, and exact receipt
presence/absence.  State-commit faults after an adapter result retain the
proven workload, event SHA, phase, and conservative child count in the tick
result; they never fall back to a misleading zero/global result.

Only explicitly allowlisted cancellation, deadline, capacity, conflict,
ownership, OS, and SQLite exceptions are retryable.  Unknown exceptions,
adapter/result/permit integrity failures, `TypeError`, and programmer errors
set a durable integrity fence while retaining pending state.  Disk/system
store failures remain retryable and are never quarantined.

`PermanentEventFailure` has only the lifecycle's five event-bound reason
codes, but is not itself quarantine authority.  No current adapter supplies an
independent durable failure receipt, so the dispatcher deliberately never
calls `quarantine_leased`; a forged typed object blocks and fences instead.

NOAA/raw desired linearization is the last exact desired-state read before the
consumer's final operation.  It does not claim that the desired row was still
active at the separate lifecycle ACK commit.  Publication remains bound to
the exact desired SHA in the terminal receipt.  Their v2 output stores now
bind the persistent instance into the pending continuation and hold an exact
`BEGIN IMMEDIATE` completion-reproof lease from final output verification
through lifecycle terminalization.  Adversarial DDL/delete/recreate tests and
crash recovery close their former cross-database output/ACK blockers.

DWD performs final output verification under output-store v3's
`completion_reproof_lease`, then a fresh lease/desired barrier, then ACK while
that SQLite `BEGIN IMMEDIATE` writer fence is still held.  Output-store v3
rejects output/metadata UPDATE and DELETE, persists a store-instance SHA, and
binds it into `DwdRvPipelineOutcomeAuthority`; the adapter composition
authority also binds that identity.  A real-store adversarial test proves a
trigger-drop/delete/recreate script is locked before ACK and that restart
reuses the one durable output before exactly one lifecycle ACK.

KNMI, METAR and Met Office single-phase stores likewise migrated to persistent
v2 identities and hold their exact output writer fence through ACK.  METAR's
complete raw and physical canonical-ground factset reproof remains inside that
fence immediately before terminalization.  Consequently all eleven route
profiles now require and hash a persistent completion-output instance
authority; missing or drifted authority fails before lifecycle claim.

SQLite's writer fence does not prevent a same-UID POSIX process from renaming
a replacement database over the open output path.  The v3 path/instance
check immediately before ACK catches deterministic replacement at the final
adapter faultpoint; the context exit also prevents a silent success result.
There remains an unavoidable nanorace after the last userspace path check, so
the code cannot make the cross-file operation atomic.
Production enablement therefore also requires a systemd/container directory
authority that prevents same-UID path replacement; this is an OS policy gate,
not a claim made by the Python module.

## Machine-readable pre-wiring blockers

The following IDs are hashed into the dispatcher profile and exposed in
bounded diagnostics:

- `posix_completion_output_path_replace_os_policy_required`

Activation must remain absent while any ID is present.  This module provides
no service hook, recovery hook, handler, activation rule, HA/MQTT path, or
automatic blocker override.

## Verification scope

Focused tests cover the exact catalogue/hash, permit cross-route/cross-phase
forgery, result lies, durable state faultpoints before/after commit, two-phase
crash recovery without token serialization, eleven-route fairness, close
during initial/final/ACK boundaries, stop/cancel, clock rollback and commit
budget, state/manifest/file-cap/link/store/adaptor tamper, same-path
replacement, privacy, DWD v3 identity binding, and real lifecycle cursor
reconciliation after terminal crash.  The focused DWD integration additionally
uses the real v3 output store and exact route adapter across the DDL/ACK crash
boundary.  Existing standalone consumer APIs remain under their original
suites.
