# OPE Bootstrap Confidence Intervals — Design

## Goal

Add reproducible 95% confidence intervals to the bandit Direct Method off-policy evaluation report so policy values and lift are not presented as unsupported point estimates.

## Scope

The change stays within the existing Python OPE report and its tests. It does not change serving behavior, replay schemas, policy definitions, runtime dependencies, or the reward-model implementation.

The intervals quantify event-sampling uncertainty conditional on the fitted reward model. They do not quantify uncertainty introduced by fitting that model. The report must state this limitation alongside its output.

## Approach

Use an event-level nonparametric percentile bootstrap:

1. Fit the reward model once using the existing deterministic train/test split.
2. Compute the existing policy point estimates over all eligible replay events.
3. Draw complete replay events with replacement, preserving each event's candidate slate and logged outcome.
4. Reevaluate every policy against the fixed fitted reward model for each resample.
5. Use the 2.5th and 97.5th percentiles as the 95% interval endpoints.

The default is 1,000 resamples. A deterministic pseudo-random generator makes repeated runs with the same input and seed reproducible.

This conditional bootstrap is preferred over analytic intervals because lift is a ratio and target-policy values are model predictions. Re-fitting the reward model inside every resample would capture more uncertainty but is deferred because of its substantially higher runtime and implementation complexity.

## Interface

Add two CLI options to `ope_eval_report.py`:

- `--bootstrap-samples`: number of resamples, default `1000`; `0` disables interval computation.
- `--bootstrap-seed`: deterministic random seed with a documented fixed default.

Add four fields to every result row and CSV:

- `value_ci_low`
- `value_ci_high`
- `lift_ci_low`
- `lift_ci_high`

Console output prints the value and lift intervals beside each policy's existing point estimates. The calibration header remains prominent.

## Computation Boundaries

The bootstrap implementation will be a pure function that accepts events, the already-fitted reward model, the point-estimate rows, sample count, and seed. It returns rows enriched with interval fields without mutating the input rows.

Each resample evaluates all policies on the same sampled event indices, preserving paired comparisons between a target policy and the logging baseline. This pairing is important for lift uncertainty.

Policy point estimates remain those produced by the existing full-data `evaluate` call. Bootstrap means do not replace them.

## Edge Cases

- `bootstrap_samples < 0`: reject with a clear argument error.
- `bootstrap_samples == 0`: retain point estimates and emit unavailable interval fields.
- Empty eligible event input: preserve the existing clear exit before model fitting or bootstrap.
- One eligible event: return a degenerate interval at the resampled estimate.
- Zero logging reward: value intervals remain available, but lift point estimates and intervals are unavailable because the ratio is undefined. Existing misleading zero lift behavior will be corrected for this case.
- A bootstrap sample with zero logging reward: exclude its lift statistic for percentile calculation while retaining its policy-value statistic. If no finite lift replicates remain, report the lift interval as unavailable.

Unavailable interval values are represented consistently as `None` in Python, blank cells in CSV, and `N/A` in console output.

## Testing

Extend the focused Python suite with tests that verify:

- identical events, sample count, and seed produce identical intervals;
- each ordinary point estimate falls within its interval on a stable synthetic dataset;
- logging lift and its interval are exactly zero when logging reward is positive;
- one-event input produces valid degenerate intervals;
- zero logging reward produces unavailable lift and lift intervals without hiding value uncertainty;
- zero bootstrap samples produces unavailable intervals while preserving point estimates;
- negative sample counts fail clearly;
- CSV output contains the four new columns;
- the existing OPE, logistic, replay-buffer, and replay-export tests remain green.

## Acceptance Criteria

The implementation is complete when the CLI produces deterministic conditional 95% intervals in console and CSV output, all edge cases above have automated coverage, the focused evaluation suite passes, and the report clearly states that reward-model uncertainty is excluded.
