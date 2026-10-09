# Raw-artifact source-local reclamation v1

## Status and boundary

`RawArtifactStore` remains an append-only schema-v3 store until
`initialize_reclamation_extension_v1()` is called explicitly. The additive
extension does not run from ordinary `initialize()`. It gives that physical
store instance a persistent random identity bound to the exact store profile.
Reopening the same catalog preserves the identity; another initialized store
cannot impersonate it merely by using the same path or limits.

The store implements only the `raw_artifacts` role from the frozen source
reclamation protocol. Candidate bytes and participant projections come through
the instance-held candidate authority and the shared role-view builders. Apply
and abort authority likewise come only from the instance-held physical decision
authority. There is no caller-supplied verifier callback on any prepare, apply,
resume or abort method.

Both authorities can be supplied together in the constructor, or bound once to
the same already injected `RawArtifactStore` object with
`bind_reclamation_authorities_v1(candidate_authority,
committed_decision_authority)`. This closes the create-time cycle in which the
retention candidate authority needs the existing raw object and the coordinator
in turn needs that candidate authority. The bind is an in-memory, under-lock
pair CAS: only fully unbound to one exact pair succeeds, and only the identical
two objects are idempotent. Partial input, mixed pairs, replacement authorities
and invalid types fail closed. It is valid before base initialization, between
base initialization and extension opt-in, or after extension opt-in; it never
rewrites the profile, extension metadata or persistent source-store identity.
Every operational reclamation entry point re-proves both live authority hashes
against the values pinned at bind time. The pair and both hashes are captured
under one store lock. Prepare retries validate an existing durable receipt
against that same snapshot before returning it, including after a reopen.

Every reclamation entry point also validates the complete canonical SQLite
authority produced by the store's own schema creation path. The accepted
inventory covers every base-v3 and extension-v1 table, index and trigger plus
their exact normalized DDL. A same-column table rebuild therefore cannot hide
changed primary keys, foreign keys, uniqueness, checks or `WITHOUT ROWID`, and
a same-name trigger with a different body is not accepted.

## Prepare and reference closure

Prepare pages the sealed candidate authority at the protocol cap and verifies
each archived `raw_artifacts_view_sha256` against the current capture, receipt
and content-addressed object facts. The local immutable plan records:

- every candidate capture and capture member;
- exact total and candidate capture references per receipt;
- exact total and deletable receipt references per content object;
- the shared raw candidate inventory root, child-reference root and source
  fence;
- releasable bytes and entries counted once per physically deletable object.

All referenced receipts and objects are fenced, including shared references
that are not currently delete-eligible. Otherwise a new reference could change
the exact prepared counts after the coordinator commits. Capture identity is
fenced by capture id and by provider/product plus fetch or upstream identity.
Prepare also rejects any already-live duplicate of a capture upstream identity
or of a delete-eligible receipt upstream identity; a permanent tombstone can
therefore never silently shadow a surviving duplicate.
Abort releases those prepare fences only while the held coordinator authority
proves the exact request, role, prepare receipt, store identity and fence.

## Positive apply and durable unlink journal

Raw apply is fourth in the closed five-role apply order and requires the exact
scheduler apply receipt as predecessor. Under the held COMMITTED decision it
first performs one SQLite transaction that:

1. re-proves the complete prepared reference plan;
2. persists `FILES_PENDING`, the immutable physical file identities and unlink
   intents;
3. writes permanent capture/receipt identity tombstones;
4. removes capture members, captures, zero-reference receipts and
   zero-reference object catalog rows.

Only after that transaction is durable may filesystem unlink begin. For every
intent the store first binds content hash, relative path and size one-to-one to
the durable delete-eligible object-reference rows from the same prepare. It
then proves that the pending captures, receipts and object catalog rows remain
absent and that no receipt newly references any pending content hash. One
`BEGIN IMMEDIATE` transaction and the store lock remain held for exactly that
one object through path/byte/identity proof, unlink, the strict directory
durability barrier, event insertion and event commit. Thus no cooperating
catalog writer can enter between the final reference proof and unlink.

The physical proof covers path, bytes, size, regular-file type, single-link
count, device and inode. The store keeps the no-follow file descriptor open
through unlink. On POSIX it additionally holds the exact parent directory file
descriptor and unlinks relative to that descriptor. On Windows it opens the
file with `CreateFileW`, reparse-point semantics and explicit delete sharing so
the read authority can remain live through unlink. Immediately before and
after unlink it re-proves the held device, inode, mode, size and link count plus
canonical path absence. Only link count zero after unlink can proceed to the
directory barrier and a checksum-chained `UNLINKED_DURABLE` event.

If the held descriptor exposes a hardlink alias before or after unlink, the
same SQLite writer transaction commits one immutable `FAILED_FENCED` record to
`raw_artifact_reclamation_unlink_failures` before returning an integrity error.
That record binds the request, ordinal, intent, observation phase, physical
facts, path-absence fact, clock and checksum. No event for that ordinal, durable
batch or APPLIED receipt may coexist with the fence, and every resume remains
fail-closed. A durable batch binds the full successful event prefix before the
final source-local APPLIED receipt and protocol `RawUnlinkJournalReceiptV1` are
committed.

Cooperative retries cannot resurrect tombstoned receipt or capture identities.
The pre-publication receipt fence also prevents a rejected retry from leaving
an orphan object on disk. Read/replay of an APPLIED receipt permanently
re-proves absence of each original capture and delete-eligible receipt by its
exact ID and by the provider/product plus fetch/upstream identity aliases. It
also recomputes the exact capture and receipt tombstones from the durable
prepare plan.

Object-path absence is deliberately not permanent: content-addressed bytes may
legitimately be captured again later. A journaled content SHA may reappear only
as the exact canonical base object with verified bytes, SHA, size, regular
single-link facts, at least one new non-tombstoned receipt and a complete
verified capture graph. Every receipt above that object must participate in a
complete graph, and every receipt and capture in those graphs is checked
against all permanent semantic tombstones. A bare path, orphan object, orphan
receipt, malformed graph or trigger-bypassed old identity fails replay closed.

## Crash and platform contract

Every local mutation boundary is retryable: prepare before/after commit,
catalog before/after commit, unlink before its event, event after commit,
durable batch before/after commit, final apply before/after commit, and abort
before/after commit. If power is lost after unlink but before the directory
barrier or event insert, resume observes absence, executes a new strict barrier
and records `object_was_present=false`; it never infers durability from absence
alone.

On POSIX, the default authority opens the exact directory with directory and
no-follow flags where available, verifies the opened device/inode, calls
`fsync`, then rechecks the directory identity. Windows has no built-in positive
directory-sync proof: apply fails closed unless an explicit constructor-held
directory-sync authority is supplied. This includes zero-object plans, so the
runtime cannot accidentally make platform-dependent authorization decisions.

One crash-atomic hardlink exclusion remains impossible to derive portably from
an ordinary shared filesystem: another filesystem writer can add an alias
after the final pre-unlink proof, and the process can terminate in the single
instruction interval after unlink but before post-unlink `fstat` and the
failure-fence commit. A later process sees the canonical path absent and cannot
enumerate an arbitrary alias elsewhere on the filesystem. This is not claimed
as solved. Live wiring stays disabled until an exclusive filesystem-owner and
dirfd policy removes that external writer from the authority boundary.

The exact machine-readable prewiring state is:

```json
{
  "schema": "stv3-worker-raw-reclamation-prewiring-v1",
  "live_wiring": false,
  "prewiring_blockers": [
    "exclusive_filesystem_owner_dirfd_policy_required_for_crash_atomic_hardlink_exclusion"
  ]
}
```

The `prewiring_blockers` array is also exposed verbatim as
`RAW_ARTIFACT_RECLAMATION_PREWIRING_BLOCKERS` and contains exactly one entry.

## Evidence

`tests/test_worker_raw_artifact_reclamation.py` covers create-once authority
binding across every initialization phase, same-object v3 opt-in and identity
preservation, exact shared refcounts, prepare/content/capture fences,
tombstones, idempotent replay, all local crash boundaries, directory-sync
failure recovery, same-byte inode replacement, journal/schema tamper,
same-name trigger drift, durable intent/prepare binding, restored pending
catalog references, authority-pair drift after reopen, post-apply object
republication through a new complete capture graph, permanent semantic alias
rejection, exact tombstone replay, cooperative hardlink interleaving with a
durable terminal failure fence, bare post-apply path rejection and Windows
fail-closed behavior. These tests are offline and do not deploy or access Home
Assistant.
