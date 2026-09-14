# Optimize SFT training design

## Problem and scope

`CtrRankingModelTrainingJob` is the repository's offline supervised fine-tuning stage. Its date-selection query reads eligible rows to discover the latest observed dates, then `run` scans the validation split again solely to count rows. Evaluation subsequently reads those rows for inference.

Count rows during date discovery and reuse the holdout total in `run`. This removes the standalone validation-count action. The existing compact training and evaluation caches remain sufficient; this change introduces no cache or dependency.

## Contract

- Use Scala 2.12.18 and Spark 3.5.1 with no new dependencies.
- Preserve the latest `max(1, holdoutDays)` observed date strings as the holdout; gaps between dates do not change selection.
- Exclude null dates from both splits and from the holdout count.
- Preserve `splitByDate(DataFrame, Int): (DataFrame, DataFrame)` for existing callers.
- Keep row counts as `Long` until the existing metrics conversion to `Double`.
- Preserve input eligibility filters, feature assembly, labels, model hyperparameters, metric names, model serialization, and cache ownership.
- Collect at most `max(1, holdoutDays)` date/count records to the driver.

## Design

Add package-visible `splitByDateWithCount(df: DataFrame, holdoutDays: Int): (DataFrame, DataFrame, Long)`. Select `date` cast to string, exclude nulls, group by date and count, order descending, limit to the requested holdout, and collect. Sum the collected counts as `Long`. Build both lazy split DataFrames with the existing cast-and-membership expressions.

The public `splitByDate` delegates to this helper and returns the two DataFrames. `run` consumes all three results and removes `validRaw.count()`. Training still materializes its compact cache through its existing count and rejects empty train/validation splits before fitting.

## Alternatives and tradeoffs

Keeping the separate count is simple but repeats input work. Caching raw validation data avoids rereading storage at the cost of retaining a wide frame. Grouped date counts reuse the existing date-discovery aggregation and add one count per date without a new cache, so they are the selected approach.

Grouping adds a `Long` aggregate per observed date and count values to the existing shuffle. The speedup depends on partition pruning, storage, and holdout size; this design claims removal of an action, not a measured end-to-end speedup or lower peak memory. As before, the batch assumes stable input during a run.

## Validation and acceptance

1. An accumulator-backed five-row input (including one null date) is processed five times total during split/count preparation, rather than ten with the separate-count baseline. Latest two dates contain three rows; the old date contains one.
2. Empty and all-undated inputs yield zero validation rows. A nonpositive holdout selects one date; an oversized holdout counts all dated rows.
3. Existing tests retain temporal split semantics, metric parity, cache cleanup, model/metrics writes, and logistic-regression and GBT coverage.
4. Run the SFT suite and the Spark module suite; record actual outcomes and any limitations in the PR.
5. Update the architecture documentation to explain the combined date/count query.

## Delivery

Include this spec, its implementation plan, Scala implementation, regression tests, and architecture documentation in a PR against `master`. The initial implementation and focused red/green checks preceded this written spec in the same session; these documents formalize that work and guide its review and delivery.
