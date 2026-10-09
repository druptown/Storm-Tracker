# Storm Tracker — recovered version history

Recovered on **9 October 2026**. This branch is an index, not an installable release.

The original Git commits and their parent relationships were preserved, including seven older agent branches and eight previously unreferenced commits. Local source snapshots were saved on separate backup branches. `main` was not rewritten or merged with archive code.

## Where to find each version

- [Version catalog](catalog.json): every recovered archive build, its source commit, version, tag and original artifact SHA-256; original Git version markers and recovered branches are listed separately.
- [Integration source history](https://github.com/druptown/Storm-Tracker/commits/archive/integration-history-20261009): recovered integration source from retained ZIP archives.
- [Worker source history](https://github.com/druptown/Storm-Tracker/commits/archive/worker-history-20261009): recovered worker source from retained deployment archives.
- [Original main history](https://github.com/druptown/Storm-Tracker/commits/main).
- [Current source-time work](https://github.com/druptown/Storm-Tracker/tree/codex/source-time-radar).
- [Local source backup](https://github.com/druptown/Storm-Tracker/tree/codex/local-source-backup-20261009).
- [Raw-storage/replay source backup](https://github.com/druptown/Storm-Tracker/tree/codex/raw-storage-replay-backup-20261009).

Recovered archive commits each contain the component source and `history/provenance.json`. Integration files use `custom_components/storm_tracker_v3/`; worker files use `storm_tracker_worker/`. They are **component snapshots**, not necessarily complete paired application trees.

Tags identify the exact retained build rather than assuming a version number is unique:

```text
archive/integration-v<VERSION>-<ARCHIVE_SHA256_FIRST_12>
archive/worker-v<VERSION>-<ARCHIVE_SHA256_FIRST_12>
archive/git-integration-v<VERSION>-<ORIGINAL_COMMIT_FIRST_12>
```

Different archived builds carrying the same version retain separate tags. Builder-evidenced integration/worker pairs are in `catalog.json`; a pair is build provenance, **not** proof that it was deployed or passed live verification.

## Integrity and limitations

- All recovered source-file paths, bytes and executable modes were read back from Git and checked against the retained archives. Original artifact hashes and available source-tree build evidence were also checked.
- Original Git commits retain their original identities. Archive recovery commits are dated when recovered and ordered by version/source hash; this does **not** invent historical development dates or a claim that one archived variant descended from another.
- One old ZIP contained another release ZIP. Both source versions were recovered separately. Windows archive separators were normalized to repository paths, without changing source bytes.
- Worker 1.18.79 contained generated Python bytecode. Those cache files are omitted and individually listed with hashes in provenance. The source files are preserved; the imported tree is not claimed to reproduce every packaged non-source file.
- Original deployment ZIP/tar binaries remain local. GitHub contains recovered source and provenance, not a new deployment-asset publication. Private configuration, raw meteorological exports and test/simulated archive copies are not part of this recovery.
- This catalog restores **all available, validated history found in the retained Git repositories and archives**. It cannot reconstruct a version whose source was never committed or retained. Missing historical steps are not fabricated; no claim is made that every numbered build was a public release or was live.
- Recovery is not a deployment or an application-test result. NAS/NUC runtime, database and trackers were untouched.
