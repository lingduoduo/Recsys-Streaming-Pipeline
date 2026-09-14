# GRPO gate before collect design

## Problem and decision

`GrpoSlates.toGroups` collects every parsed slate to the driver and only then applies its three gates — slate size, feature version, reward variance. At the click-through this job runs in, the variance gate rejects almost everything it was just handed: a measured fixture of 5,000 slates with 5% engaged keeps 250. The other 4,750 are serialized to the driver, materialized as `Row` objects, and discarded.

That is the limitation the method's own comment names. It says `collect()` pulls every surviving slate's feature matrix into driver memory, that a 5,000-slate batch of ten nine-feature items is a few MB so the driver holds it comfortably, and that "the ceiling is the driver's heap, not the cluster's, and raising the trigger size is the operation that hits it." The gates are what decide *surviving*, and they run after the collect rather than before it.

Evaluate the gates in Spark and collect only the slates that pass. At 5% engagement that is 250 rows instead of 5,000 — a twentyfold reduction in what crosses to the driver, and the reduction grows as click-through falls, which is the direction that makes the ceiling worse.

This is deliberately not the `treeAggregate` migration the original GRPO design called for. That would keep per-slate gradient contributions in Spark entirely, and the existing comment enumerates what it costs: gate counts onto Spark accumulators because they only settle after an action, `applyBatch` turning from a pure `Array` function into one that broadcasts weights and launches a job per inner epoch, and rewrites of both `GrpoSlatesSpec` and `GrpoPolicyStreamingJobSpec`, which today test the learning rule with no Spark session at all. Gating before the collect removes the volume without touching any of that: `toGroups` still returns a `Seq[GrpoGroup]`, `applyBatch` stays pure, and both specs keep their current shape.

## What this does and does not claim

It claims a reduction in rows and bytes reaching the driver, which is deterministic and countable: the number of collected rows equals the number of kept slates instead of the number of parsed slates.

It does not claim a latency improvement, because that was measured and the measurement failed. A spike established robustly that the parse-and-collect path dominates a micro-batch — 200 to 750 ms against `applyBatch`'s 0.2 to 24 ms, one to two orders apart, a gap far larger than the noise. But splitting that path into its `from_json`, collect, and driver-classify components produced a negative component at five repetitions, meaning the noise exceeded the effect being measured. The saving in wall-clock time is therefore unquantified here, and nothing in this design or its delivery should assert one. The justification is driver memory and the heap ceiling.

## Global constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17; no new dependencies.
- Preserve `toGroups(slates: DataFrame, cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts)` exactly — same signature, same return type.
- Preserve the gate precedence. A slate failing more than one gate is counted under the first it fails, in the order size, then feature version, then reward variance. `GateCounts` must be identical to today's for every input.
- Preserve `GrpoGroup`'s contents and the order of the returned `Seq`, which must remain the slates' order in the input DataFrame.
- Preserve `parseFeatureVector`, `TooSmall`, `BadFeatureVersion`, `ZeroVariance` and `GateCounts` as they are, including `parseFeatureVector`'s rejection of an unparseable vector and of a width that is not `dim`.
- One definition of the gates. The Spark-side and driver-side paths must call the same code, so they cannot drift.
- `applyBatch` stays a pure function over `Array`s and `toGroups` keeps returning a strict `Seq`; neither may require a Spark session to test.

## Implementation

Extract the gate decision into `dropReason(items: Seq[Row], featureVersion: String, dim: Int): Option[String]`, which returns the first gate a slate fails or `None`. It takes the two primitives the gates actually read rather than a `GrpoJobConfig`, so the UDF closes over a `String` and an `Int` and the Redis host, port and hyperparameters never reach an executor. `classify` is replaced by `buildGroup`, which constructs the `GrpoGroup` for a slate already known to pass, and both it and `dropReason` share one feature-parsing helper, so no second definition of what makes a slate valid can appear.

`toGroups` registers `dropReason` as a UDF over the `items` column, projects `slate_id`, `items` and the reason, and reuses that frame twice: once as `groupBy(reason).count()`, which returns at most four small rows and moves no slate data to the driver, and once as `filter(reason.isNull)`, whose `collect` carries only surviving slates. The tagged frame is cached before both actions so the parse and the gate evaluation are not repeated, and unpersisted before returning.

`GateCounts` is assembled from the aggregated counts rather than from driver-side classification. The surviving rows still go through `classify`, whose gates all pass by construction, which keeps group construction on exactly one code path.

`toGroups` reads `cfg.featureVersion` and `cfg.dim` into locals before building the UDF, which is what keeps the closure down to two primitives; capturing `cfg` inside the lambda would ship the whole config.

## Validation and acceptance

1. For every fixture in the existing `GrpoSlatesSpec`, `toGroups` returns the same groups in the same order and the same `GateCounts` as before. Compared against a frozen copy of the pre-change implementation, not against hand-written expectations.
2. Gate precedence is preserved where gates overlap: a one-item slate with a bad feature vector counts as `TooSmall`, not `BadFeatureVersion`; a two-item slate with a bad vector and zero variance counts as `BadFeatureVersion`, not `ZeroVariance`.
3. The number of rows reaching the driver equals the number of kept slates. Asserted by counting, using a fixture where most slates fail the variance gate, so the claim this design rests on is tested rather than assumed.
4. A slate with an empty `items` array is handled as today, counted under `TooSmall`. A slate whose `items` is **null** is a deliberate behavior change and is asserted as such, not as equivalence: the old path did `row.getSeq[Row](1).size`, and `getSeq` returns null for a null field, so it threw `NullPointerException`. `items` is nullable in `SlateSchema` and `from_json` yields null for a missing field, so a malformed slate off Kafka could reach it. Verified both ways — the frozen oracle throws, the new path drops the slate as `TooSmall`.
5. The existing `GrpoSlatesSpec`, `GrpoPolicyStreamingJobSpec`, `GrpoMathSpec` and `GrpoJobConfigSpec` pass unchanged, and `applyBatch` still needs no Spark session.
6. The Spark module suite passes under JDK 17 and `git diff --check` is clean.

## Limits

A UDF is opaque to the Catalyst optimizer, so the gate cannot be pushed into a scan or combined with other predicates; it runs once per row as a black box. That is acceptable because the alternative — reimplementing `parseFeatureVector`'s version prefix, width and numeric-parse semantics as SQL expressions — would put a second definition of the gates in the codebase, which the constraints forbid for exactly the reason that it would silently drift.

The tagged frame is cached, so this trades driver memory for executor memory across two actions. On a 5,000-slate batch the cached frame is the same few MB the driver used to hold, now spread across executors. Raising the trigger size therefore still has a ceiling; it is the cluster's rather than the driver's, which is the point, but it is not unbounded.

One behavior genuinely improves rather than being preserved, which is why the acceptance list treats it separately. A slate arriving with no `items` array at all used to crash the micro-batch with a `NullPointerException` from calling `.size` on the null `getSeq` returned; it is now gated as `TooSmall` like any other slate too small to form a group. This was found while reviewing the change rather than designed in, and it is an improvement — a single malformed slate should not fail a batch — but it means the "identical for every input" constraint above holds for every input the old path survived, not literally every input.

Row order is preserved because `filter` does not reorder within partitions and the collect concatenates partitions in order, which is the same property the current `collect` relies on. No shuffle is introduced on the filtered path; the `groupBy` for counts shuffles at most four keys.

## Delivery

Publish this spec and its implementation plan on `perf/grpo-gate-before-collect` in a draft PR against `master`, then add the reviewed code, tests and evidence to the same PR and mark it ready.
