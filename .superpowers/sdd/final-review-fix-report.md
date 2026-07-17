# Final review fix report

## Status

Complete. All four final-review findings are addressed without dependency, serving,
default, seed, output-schema, or disclaimer changes.

## Changes

- Fixed fixed-point filtering to remove zero-rating users even when
  `minUserRatings == 0`, including a mixed retained/emptied fixture and a 20-seed CLI
  regression.
- Precomputed an immutable popularity-ordered movie ID list once in
  `MovieLensDataset`; each initial state now only filters that ordering for its user.
  The API test locks popularity descending and natural `String` tie ordering.
- Made the greedy lexical tie test explicit with IDs `"10"` and `"2"`.
- Moved requested CSV writing ahead of console result/disclaimer output. A directory
  used as `--output` proves status 2 and empty stdout on output creation failure.

## TDD evidence

RED command:

```text
mvn test -Dtest=MovieLensDatasetTest,FiniteHorizonEnvironmentTest,EvaluationPolicyTest,MovieLensPolicyEvaluationTest -Dstyle.color=never
```

Before production changes, test compilation failed with two `cannot find symbol`
errors for the required `MovieLensDataset.movieIdsByPopularity()` API. This established
that the new ordering contract did not exist.

GREEN: the same focused command completed with 38 tests, 0 failures, 0 errors, and 0
skips.

## Repository ratings smoke

Exact command:

```text
java -cp target/classes com.demo.retrieval.evaluation.MovieLensPolicyEvaluation --ratings ../../sampledata/ratings.csv --episodes 20 --min-user-ratings 1 --min-movie-ratings 1 --bootstrap-samples 20
```

Exit 0. It printed `uniform` and `greedy` result rows (20 episodes each) and the
unchanged two-line fixed-dataset disclaimer.

## Self-review

- The popularity comparator is byte-for-byte equivalent in criteria to the former
  per-episode comparator: count descending, then natural `String` ordering.
- The cached list is immutable (`Stream.toList()`), derived after immutable count
  construction, and environment sampling/shuffle order remains unchanged.
- Empty users are removed at the source; no evaluator seed or user sampling behavior
  was patched around the defect.
- CSV schema and numeric formatting are unchanged; only the ordering of the fallible
  file write versus console rendering changed.
- No dependency or serving files changed.

## Concerns

None known. The CSV writer can still leave a partially written file if an I/O error
occurs mid-write, matching prior file semantics; the requested guarantee concerns
avoiding success-looking console output and is covered.

## Commit

The commit containing this report is reported with its SHA to the parent agent.
