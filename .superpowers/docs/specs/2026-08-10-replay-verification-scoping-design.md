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
writes with `partitionBy("date")`, so a batch's `_COMMITTED` inventory lists relative paths of the
form `date=YYYY-MM-DD/part-*.parquet`. Reading that one small text file is cheap: no Parquet opens,
no hashing.

**Pruning reads the manifest, never the directory listing.** This distinction is the whole safety
argument, and the first implementation got it wrong. A manifest may declare
`date=2026-08-01/part-0.parquet` while that directory has been deleted; a listing-based prune sees
no `date=` children, concludes "empty batch", and skips it — so the replay silently publishes fewer
rows than the archive committed. Silent under-publishing is strictly worse than the cost problem
being solved here. The declared inventory still names the missing file, so a manifest-based prune
keeps the batch eligible and validation fails as it should.

The new order inside `committed_parquet_files` is:

1. Enumerate numeric batch directories, rejecting non-numeric names as today.
2. For each batch, read `_COMMITTED` and collect the `date=` prefixes its inventory declares.
3. If no declared date falls in `[start_date, end_date)`, skip the batch entirely — no inventory
   comparison, no Parquet opens, no hashing.
4. Otherwise run the existing validation over the batch, unchanged and complete.
5. Select in-range files from the validated batch, as today.

A declaration is only trusted when it is internally coherent. Any of the following keeps a batch
eligible so full validation runs and raises: a missing or unparseable `_COMMITTED`, a version other
than 2, an inventory path without a parseable `date=` prefix, a missing or negative `row_count`, or
an empty inventory that nonetheless claims rows. Only an empty inventory declaring exactly zero rows
is a trustworthy "nothing here".

Because a batch is one micro-batch of a stream — 10 seconds at the default trigger — a batch that
intersects the range is almost entirely inside it. Verified bytes therefore track published bytes
closely, and the whole-batch row-count invariant is preserved for every batch that is touched. No
part of the manifest contract is relaxed.

Enumerating batch directories remains O(number of batches). That is unavoidable without a durable
date-to-batch index, which is deliberately out of scope; the change removes the O(archive bytes)
term, which is the one that makes replay unusable at scale.

### Digest reuse

`committed_parquet_files` already computes a verified `_ArchiveFileIdentity` (path, size, SHA-256)
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

- **Data damage outside the range no longer fails a replay.** This is the point. An operator
  recovering today's data is no longer blocked by bit rot or a partial restore in unrelated history:
  a batch whose declared digests no longer match its files is skipped when out of range, and still
  rejected when in range.
- **But a broken commit *record* still blocks, wherever it is.** Because pruning depends on the
  manifest, a batch whose `_COMMITTED` is missing, unparseable, or incoherent cannot be placed in
  time, so it stays eligible and validation raises. This is a narrower blocking condition than the
  status quo, and the right one: the alternative is deciding an unreadable batch is safe to ignore.
- **Committed zero-row batches are skipped.** An all-invalid batch commits with an empty inventory
  declaring zero rows, so pruning skips it without validating. It contains nothing to publish and so
  nothing to verify.
- **Replay stops functioning as an incidental archive audit.** Deleted or tampered files outside the
  selected range go unnoticed. Replay is not `fsck`. If a whole-archive integrity sweep is wanted it
  belongs in a separate audit command with its own cadence, not bolted onto the recovery path.

## Scope

Includes:

- Batch-level date pruning in `committed_parquet_files`, driven by the commit manifest.
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
- Every existing corruption test still passes unmodified; each keeps proving the commit protocol
  is enforced for data that is read.
- Digest damage **outside** the range: replay succeeds and publishes the in-range rows. The same
  damage **inside** the range still raises.
- A batch declaring an in-range partition whose files have been deleted raises, rather than being
  pruned as empty.
- An empty inventory claiming a non-zero `row_count` is validated, not pruned.
- `source_signature` matches an independent reimplementation of its documented format, proving
  in-flight operations resume. The format is pinned rather than a golden constant, which would be
  brittle across pyarrow versions.
- Each selected file is hashed exactly once per invocation.
- A batch whose partitions straddle the range boundary is fully validated, and only its in-range
  files are selected.
- A coherent zero-row batch is skipped without validation, proven by omitting its `_SUCCESS`.
- `kind="dead-letter"` selects dead-letter batches and rejects a `valid` manifest, and vice versa.

## Success Criteria

- Replaying one day from an archive of N days hashes only the files in batches intersecting that
  day.
- Every batch read from is validated by the unchanged, complete commit-protocol check.
- Digest damage outside the selected range does not prevent a replay; a broken commit record
  still does, wherever it is.
- `source_signature` and `selected_rows` are byte-identical to their pre-change values, so
  operations started before the change resume after it.
- The batch reader accepts a `kind` parameter and the dead-letter re-drive can build on it.
- The full `test_archive_replay.py` suite passes, plus the new cases above.

## Sequencing

This lands **before** the dead-letter re-drive. It rewrites the batch-validation loop that the
re-drive needs, and parameterizing `kind` while that loop is already open avoids writing the
date-pruning logic twice or letting two copies of the commit-protocol checks drift.

See `2026-08-10-dead-letter-redrive-design.md`.
