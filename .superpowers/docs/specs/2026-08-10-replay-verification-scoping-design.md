# Replay Verification Scoping Design

## Purpose

Scope archive replay's integrity verification to the batches a replay actually reads. Today every
invocation SHA-256 hashes every Parquet file in every committed batch of the query namespace before
filtering to the requested date range, so the cost of replaying one day grows with the size of the
whole archive.

This changes only the order and reach of verification. It does not weaken the commit protocol, the
manifest format, or the guarantees applied to data that is published.

## Motivation

Three problems follow from verifying the whole archive up front.

**Cost is unbounded.** `_committed_parquet_files` walks every numeric batch directory, hashes every
Parquet file with `_file_sha256`, and reads every file's row-group metadata. Only afterwards does it
filter file-by-file on the partition date. Replaying one day out of a year re-hashes the year. The
cost grows every day the pipeline runs, and it grows fastest exactly when an operator most wants a
fast recovery.

**Selected files are hashed twice.** `_source_signature` recomputes SHA-256 for each selected file
to build the immutable source contract, duplicating work `_committed_parquet_files` already did.

**One bad batch blocks every replay.** Any uncommitted, malformed, or inventory-mismatched batch
anywhere in the namespace raises before selection. A single damaged batch from six months ago makes
today's incident recovery impossible — the archive's primary purpose defeated by a check on data the
replay would never read.

## Chosen Guarantee

**Verify everything you publish.** Every batch a replay reads from is validated exactly as it is
today: `_SUCCESS` and `_COMMITTED` present, version-2 manifest, matching query/kind/batch identity,
exact inventory of relative path plus byte size plus SHA-256, declared row count equal to the actual
Parquet row count. Batches outside the requested range are neither read nor verified.

The invariant that survives is the one that matters: no byte reaches Kafka without full
verification. The invariant that is dropped — that a replay incidentally proves the health of the
entire archive — was never load-bearing and was actively harmful to availability.

## Design

### Batch-level date pruning

Batch directories are named by Spark batch ID and carry no date. But `RawArchiveSink.writeBatch`
writes with `partitionBy("date")`, so every committed non-empty batch directory contains
`date=YYYY-MM-DD/` children. Listing those children is a directory listing: no file opens, no reads,
no hashing.

The new order inside `_committed_parquet_files` is:

1. Enumerate numeric batch directories, rejecting non-numeric names as today.
2. For each batch, list its `date=*` children.
3. If no partition date falls in `[start_date, end_date)`, skip the batch entirely — no manifest
   parse, no inventory comparison, no hashing.
4. Otherwise run the existing validation over the batch, unchanged and complete.
5. Select in-range files from the validated batch, as today.

Because a batch is one micro-batch of a stream — 10 seconds at the default trigger — a batch that
intersects the range is almost entirely inside it. Verified bytes therefore track published bytes
closely, and the whole-batch row-count invariant is preserved for every batch that is touched. No
part of the manifest contract is relaxed.

Enumerating batch directories remains O(number of batches). That is unavoidable without a durable
date-to-batch index, which is deliberately out of scope; the change removes the O(archive bytes)
term, which is the one that makes replay unusable at scale.

### Digest reuse

`_committed_parquet_files` already computes a verified `_ArchiveFileIdentity` (path, size, SHA-256)
for each selected file. Thread those identities through to `_source_signature` so each selected file
is hashed once per invocation.

This reuse must be **value-preserving**. `_source_signature` currently hashes a JSON document whose
`files` entries hold the path relative to `archive_path`, the size, and the digest. The digests are
identical either way, so the signature value must not change. A changed signature would make every
in-flight operation fail its immutable-source check on resume.

### Compatibility

The set of selected files, `selected_rows`, `source_paths`, and `source_signature` are all
unchanged by this work. An operation started before the change must resume cleanly after it. This is
a hard requirement, not a nicety, and is covered by an explicit test.

## Accepted Losses

- **A corrupt batch outside the range no longer fails a replay.** This is the point. An operator
  recovering today's data is no longer blocked by damage to unrelated history.
- **Committed zero-row batches are skipped.** An all-invalid batch commits with an empty inventory
  and no `date=` children, so pruning skips it. It contains nothing to publish and so nothing to
  verify.
- **Replay stops functioning as an incidental archive audit.** Deleted or tampered files outside the
  selected range go unnoticed. Replay is not `fsck`. If a whole-archive integrity sweep is wanted it
  belongs in a separate audit command with its own cadence, not bolted onto the recovery path.

## Scope

Includes:

- Batch-level date pruning in `_committed_parquet_files`.
- Digest reuse between batch validation and `_source_signature`.
- Parameterizing the batch reader by manifest `kind`, so the dead-letter re-drive can consume the
  same pruned, validated reader. `kind` defaults to `valid`; behaviour for existing callers is
  unchanged.
- Tests for pruning, retained in-range validation, signature stability, and single-hashing.

Excludes:

- A durable date-to-batch index or any change to the commit protocol.
- A separate archive audit command.
- Any change to `run_replay`, the manifest schema, the cursor, or the publish loop.
- Any change to `RawArchiveSink` or the Scala side.

## Testing

- A range touching one batch among many: assert files in untouched batches are never hashed, by
  patching `_file_sha256` to fail on unexpected paths.
- Every existing corruption test is retained and retargeted so the damaged batch is **in range** —
  each must still raise the same error.
- A corrupt batch **outside** the range: replay succeeds and publishes the in-range rows.
- `source_signature` for a fixed archive equals its pre-change value (golden value), proving
  in-flight operations resume.
- Each selected file is hashed exactly once per invocation.
- A batch whose partitions straddle the range boundary is fully validated, and only its in-range
  files are selected.
- A committed zero-row batch inside the range is skipped without error.
- `kind="dead-letter"` selects dead-letter batches and rejects a `valid` manifest, and vice versa.

## Success Criteria

- Replaying one day from an archive of N days hashes only the files in batches intersecting that
  day.
- Every batch read from is validated by the unchanged, complete commit-protocol check.
- A malformed batch outside the selected range does not prevent a replay.
- `source_signature` and `selected_rows` are byte-identical to their pre-change values, so
  operations started before the change resume after it.
- The batch reader accepts a `kind` parameter and the dead-letter re-drive can build on it.
- The full `test_archive_replay.py` suite passes, plus the new cases above.

## Sequencing

This lands **before** the dead-letter re-drive. It rewrites the batch-validation loop that the
re-drive needs, and parameterizing `kind` while that loop is already open avoids writing the
date-pruning logic twice or letting two copies of the commit-protocol checks drift.

See `2026-08-10-dead-letter-redrive-design.md`.
