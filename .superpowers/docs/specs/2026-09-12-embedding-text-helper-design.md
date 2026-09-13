# One embedding text contract

**Date:** 2026-09-12
**Status:** Approved design; implementation in progress on `simplify/embedding-text-helper`

## Problem and scope

Three Spark jobs produce embeddings: `AlsEmbeddingTrainingJob`, `Item2VecTrainingJob`, and
`UserEmbeddingTrainingJob`. `EmbeddingCandidateGenerationJob` consumes two of their outputs. They
share one artifact contract: a line per id, `id:` followed by space-separated floats, written to a
text path and optionally to Redis as `prefix:id` with a TTL (the key contract recorded in
[PR #20](../plans/2026-06-27-consolidation-phase2-phase5.md) and the retrieval service's
`parseVector`). The contract is implemented repeatedly:

- Three writers: ALS's `writeFactors` and `writeFactorsToRedis`; the user job's inline text write
  and `foreachPartition` Redis write in `main`; Item2Vec's single-file driver write.
- Two parsers: the user job's `readItemEmbeddings` (with an `ItemEmbedding` case class) and the
  candidate-generation job's `readEmbeddings`, which split on the first colon and on whitespace
  with slightly different edge handling.
- Three renderings of a vector column as a string: ALS's `Seq[Float]` UDF, the user job's
  `array_join(transform(...))`, Item2Vec's `mkString(" ")`.
- No spec for the user job: its mean-of-vectors aggregation and min-rating filter are untested,
  while ALS and Item2Vec have specs.

This change introduces one helper for the contract, moves ALS, the user job, and candidate
generation onto it, and adds the missing user-job spec. Item2Vec keeps its single-file driver
write, because `next_item_model.load_item_vectors` (Python) opens that path as a file, and its
existing direct `RedisWriter` call.

## Global constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, JDK 17 for sbt; add no dependencies.
- Keep every output format, path default, Redis key prefix, TTL default, environment variable,
  positional argument, and script entry point unchanged.
- Keep `RedisWriter` as the low-level writer; the helper composes it.
- Keep `trainAlsEmbeddings`, `trainItem2vec`, `topKCandidates`, and both
  `trainUserEmbeddings` overloads callable as today, except that the user job's training output
  no longer carries the pre-rendered `userEmbeddingStr` column (rendering moves to the writer).
- Malformed lines are skipped, never fail the job: a line without a colon, with an empty id, or
  with a non-numeric token is dropped, as both current parsers already do.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Leave the copies | A format change (or a parser bug) has to be found and fixed in three or four places. |
| One helper in `com.demo.util` with a Column renderer, a reader, a text writer, and a Redis writer | Selected: each job keeps its training code and drops only its copy of the plumbing. |
| Also move Item2Vec onto the DataFrame text writer | Would turn its single output file into a directory; the Python reader opens it as a file. Rejected. |
| Also unify the candidate lists' Redis writer | Different shape (a list per user, `del`/`rpush`/`expire`); not this contract. Left alone. |

## Design

### `com.demo.util.EmbeddingText`

- `vectorString(vector: Column): Column` renders a numeric array column as the space-joined
  string via `array_join(transform(vector, _.cast("string")), " ")`. Works for `array<float>`
  (ALS factors) and `array<double>` (user means).
- `read(spark, path): DataFrame` with columns `id: String`, `vector: Seq[Double]`, from a file or
  a directory of part files. A line is kept when the first colon is at index one or later and every
  whitespace-separated token after it parses as a double; otherwise it is skipped.
- `writeText(df, idCol, vectorCol, path)` writes `concat_ws(":", id, vectorString(vector))` as a
  single `value` column with `mode("overwrite").text(path)`.
- `writeRedis(df, idCol, vectorCol, host, port, keyPrefix, ttlSeconds)` renders the same string
  and calls `RedisWriter.writeWithPipeline` per partition.

### Job changes

- **ALS**: `main` calls `writeText` and `writeRedis`; `vectorToString`, `writeFactors`, and
  `writeFactorsToRedis` are removed. The spec's format test moves to the helper's spec as a
  round trip; the two training tests stay.
- **User job**: `readItemEmbeddings` and `ItemEmbedding` are removed; the path overload reads
  through `EmbeddingText.read(...).withColumnRenamed("id", "movieId")`. The DataFrame overload
  returns `userId` and `userEmbedding` only. `main` calls `writeText` and `writeRedis`; the
  cache-when-both-sinks logic stays.
- **Candidate generation**: `readEmbeddings` is removed; `main` calls `EmbeddingText.read`.
  `topKCandidates`, `cosine`, and `writeCandidatesToRedis` are untouched.
- **Item2Vec**: untouched.

### Specs

- `UserEmbeddingTrainingJobSpec` (new, written first against the current code): the user
  embedding is the mean of the item vectors of that user's ratings at or above the threshold;
  users with no qualifying rating, or whose only items have no vector, are absent.
- `EmbeddingTextSpec` (new): text round trip through `writeText` then `read` preserves ids and
  vectors; malformed lines (no colon, empty id, non-numeric token) are skipped; `vectorString`
  renders a float array.
- Existing ALS, Item2Vec, candidate-generation, and sequence-preprocessing specs stay as guards.

## Files

Paths below are relative to `recsys-pipeline/services/spark-streaming-job/src/` unless noted.

| File | Responsibility |
|---|---|
| `main/scala/com/demo/util/EmbeddingText.scala` | The contract: render, read, write text, write Redis. |
| `test/scala/com/demo/util/EmbeddingTextSpec.scala` | Round trip, malformed lines, float rendering. |
| `test/scala/com/demo/task/UserEmbeddingTrainingJobSpec.scala` | Mean, threshold, absent users. |
| `main/scala/com/demo/task/AlsEmbeddingTrainingJob.scala` | Use the helper; drop three private members. |
| `test/scala/com/demo/task/AlsEmbeddingTrainingJobSpec.scala` | Drop the moved format test. |
| `main/scala/com/demo/task/UserEmbeddingTrainingJob.scala` | Use the helper; drop the parser and case class. |
| `main/scala/com/demo/recommend/EmbeddingCandidateGenerationJob.scala` | Use the helper's reader. |
| `.superpowers/docs/plans/2026-09-12-embedding-text-helper.md` (repository root) | Reproducible implementation and verification steps. |

## Acceptance criteria

1. `UserEmbeddingTrainingJobSpec` passes on the current code before the refactor and after it.
2. `EmbeddingTextSpec` round-trips a two-row fixture exactly and skips three kinds of malformed line.
3. `grep -rn "def readEmbeddings\|def readItemEmbeddings\|def writeFactors\|vectorToString\|case class ItemEmbedding"` over `main/scala` returns nothing.
4. The `task`, `recommend`, `process`, and `util` specs pass: 10 baseline tests plus the new ones.
5. The full Spark module suite passes under JDK 17, with unrelated failures reported against a baseline.

## Risks and limits

Rendering floats through Spark's cast instead of Scala's `mkString` could in principle differ in
the last digit for `array<float>`; both use Java's `Float.toString`, and every consumer parses
with `toDouble`/`float()`, so the contract is preserved even if a digit moved. The user job's
DataFrame overload loses a column that only `main` consumed. Rollback is a revert; no artifacts
or keys change.
