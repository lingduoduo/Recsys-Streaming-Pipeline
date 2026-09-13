# GRPO surface simplification

**Date:** 2026-09-12
**Status:** Approved design; implementation in progress on `simplify/grpo-surface`

## Problem and scope

[PR #222](2026-09-12-grpo-ppo-objective-simplification-design.md) simplified the PPO objective
inside `GrpoMath` and `applyBatch`. The rest of online GRPO, designed in
[the online GRPO design](2026-08-30-online-grpo-design.md), spans three runtimes: the Scala slate
gate, config, and weight store; the Java scorer, feature packer, publisher, and event builders;
and the Python offline evaluator. Config, weight store, feature packer, publisher, and event
builders are already tight. Three places carry avoidable complexity:

- `GrpoPolicyScorer.score(movie)` and `enabled()` have no production caller; serving uses only
  `blendWeight`, `reRankOrder`, and `recordShadowSlate`. Seven tests exercise the weight-reading
  guards through `score`, which is what keeps the dead method alive. `reRankOrder` boxes its
  adjusted scores and indexes into `Double[]` and `Integer[]` to sort, then copies back to `int[]`.
- `GrpoSlates.toGroups` threads three `var` counters and a mutable buffer through nested
  if/else with `_1`/`_2`/`_3` tuple access, and repeats the drop-reason strings that
  `GateCounts.reasons` also spells out. `GateCounts.total` has no caller.
- `grpo_offline_eval.py` reaches into each row's `item_features` and parses the packed vector in
  two functions with the same four lines, re-checks version and width per row with two
  comparisons, and tells the v1 position-feature history four times (module docstring, two
  function docstrings, a comment in `main`).

This change removes those without altering gate semantics, drop-reason names, the wire format,
mode names, the blend weight, or any printed line, summary key, or error message.

## Global constraints

- Java 17 with Maven; Scala 2.12.18 with sbt under JDK 17; Python 3 with pandas; add no dependencies.
- Keep `GrpoPolicyScorer`'s public `MODE_*`, `WEIGHTS_KEY`, `ON_BLEND_WEIGHT`, `blendWeight`,
  `reRankOrder`, `recordShadowSlate`, and package-private `pairwiseConcordance` unchanged in
  signature and behavior, including the stable descending sort and its exact blend arithmetic.
- Keep `GrpoGroup`, `GateCounts` fields and `reasons`, `parseFeatureVector`, and the
  `(Seq[GrpoGroup], GateCounts)` return of `toGroups` unchanged; keep the gate order
  too-small, bad-feature-version, zero-variance.
- Keep every public function signature and every `SystemExit` message in
  `grpo_offline_eval.py`, and the printed lines and summary keys of its `main`.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Leave the code; the suites pass | No risk, but a dead public method, mutable gate bookkeeping, and four copies of one explanation remain. |
| Remove the dead method, make the gate a pure classification, share the row parse | Selected: local rewrites, each pinned by an existing suite. |
| Parse each evaluator row once in `main` and pass parsed rows to both functions | Would change the two tested signatures for a string split that is not a measured cost; rejected in favor of one shared helper called by both. |
| Route the Java scorer's Redis read through the TTL feature cache the original design named | A behavior change to serving; out of scope. |

## Design

### Java scorer

Remove `score`, `enabled`, and the `dot(double[], ServedMovie)` overload; `recordShadowSlate`
calls `dot(w, GrpoFeatures.of(served.get(i)))`. In `reRankOrder`, keep `raw`, `min`, `max`,
`range`, and the per-candidate `norm` exactly as now, store `adjusted` in a `double[]`, and produce
the order with `IntStream.range(0, n).boxed().sorted(descending by adjusted).mapToInt(...).toArray()`.
`Stream.sorted` on an ordered stream is stable, as `Arrays.sort` with a comparator was, so ties keep
index order. The `java.util.Arrays` import goes; `java.util.stream.IntStream` arrives.

The seven `score`-based tests move to the production entry points:

- mode checks assert `blendWeight()` (`0.0` for off, shadow, and an unrecognised value;
  `ON_BLEND_WEIGHT` for on) and, for off and unrecognised, that `recordShadowSlate` never touches
  Redis;
- the four weight guards (wrong version, ten-wide vector, missing hash, non-finite weight) assert
  that `recordShadowSlate` in shadow mode logs no slate line, mirroring the existing
  `aSlateWithNoUsableWeightsLogsNothing`.

The `expectedScore` helper and the unused `assertFalse` import go with them. Test count stays 29.

### Scala slate gate

`GrpoSlates` gains three reason constants (`TooSmall = "slate_too_small"`,
`BadFeatureVersion = "bad_feature_version"`, `ZeroVariance = "zero_reward_variance"`) that
`GateCounts.reasons` also uses. A private `classify(row: Row, cfg): Either[String, GrpoGroup]`
returns the first failing reason or the group, in the existing gate order. `toGroups` collects the
rows, maps them through `classify`, and builds `(kept, GateCounts)` from the partition with no
`var`. `GateCounts.total` is removed. The scaling-limitation comment on `toGroups` stays.

### Python evaluator

A private `_parse_rows(rows)` generator yields `(row, features, parsed)` for every row, where
`features` is the normalized `item_features` map and `parsed` is `parse_packed_vector(...)` or
`None`. `detect_feature_schema` and `build_scored_rows` both iterate it; the latter's version and
width check becomes one tuple comparison against `(version, dim)`. The v1 history stays in the
module docstring; the `SUPPORTED_FEATURE_VERSION` comment, the `detect_feature_schema` docstring's
second paragraph, and the comment before the flip-criterion print each become one line pointing
there.

## Files

Paths below are relative to the repository root.

| File | Responsibility |
|---|---|
| `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java` | Drop dead entry points; unboxed re-rank sort. |
| `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoPolicyScorerTest.java` | Guard tests through `blendWeight` and `recordShadowSlate`. |
| `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoSlates.scala` | Reason constants; `classify`; fold-based `toGroups`. |
| `recsys-pipeline/services/python-modeling/post-training/grpo_offline_eval.py` | Shared row parse; single-tuple schema check; one telling of the v1 history. |
| `.superpowers/docs/plans/2026-09-12-grpo-surface-simplification.md` | Reproducible implementation and verification steps. |

## Acceptance criteria

1. `GrpoPolicyScorerTest` reports 29 tests, 0 failures, with no test calling `score` or `enabled`,
   and `grep -n "score(\|enabled()" GrpoPolicyScorer.java` matches nothing.
2. `testOnly com.demo.grpo.*` reports 51 tests, 0 failures, with `GrpoSlatesSpec` unchanged.
3. `test_grpo_offline_eval.py` reports 36 passed unchanged, and `main` on a fixed fixture prints
   the same stdout and returns the same summary before and after, compared as JSON.
4. `grep -c "position" grpo_offline_eval.py` drops, and the phrase "served-position feature"
   appears once.
5. The full retrieval Maven suite, the full Spark module suite, and the full Python modeling suite
   pass, with unrelated failures reported against a baseline rather than claimed green.

## Risks and limits

The re-rank sort must stay stable and descending; the hand-computed blend test and the
disagreement test pin both. `classify` returns the first failing reason, which matches the old
nested order only if the checks stay in the same sequence; the five gate tests pin each reason.
The evaluator refactor is guarded by 36 tests and a parity run. Rollback is a revert; no data,
weights, or artifacts change.
