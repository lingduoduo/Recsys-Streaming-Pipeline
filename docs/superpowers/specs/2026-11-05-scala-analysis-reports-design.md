# Scala analysis reports

The user prefers Scala over Python for the analysis report and confirmed conversion of its generators. Port recall evaluation, ranking evaluation, off-policy evaluation, and the React dashboard JSON exporter into the existing Scala project. Update `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md` to use the resulting Scala entry points.

## Implementation decisions

Use the existing Scala 2.12.18 / Spark 3.5.1 project under `recsys-pipeline/services/spark-streaming-job`, its Jedis dependency, and established report job conventions. This is an implementation choice based on the repository. Add real Scala implementations; do not invoke Python from Scala.

Retain current Python implementations during migration so existing consumers continue working. Port shared functionality needed by the four new entry points, including dashboard measurements and metadata enrichment. Do not convert unrelated event producers or post-training algorithms merely because they are mentioned in the report. Document those Python dependencies honestly.

## Compatibility

- Recall: preserve BM25, cosine, reciprocal-rank fusion, leave-one-out protocol, user weighting, deterministic ties, CSV fields, and output location.
- Ranking: preserve popularity, negative-position and embedding signals, average-rank AUC ties, population z-score sigmoid calibration, logloss, coverage, and CSV schema.
- Off-policy: preserve Direct Method reward estimation, candidate feature extraction, exclusion of policy-only predictions from reward features, policies, calibration, event bootstrap intervals, input selection, and output fields. Equivalent numerical results are required within declared tolerances; differences in random generators must be documented and tested statistically or against controlled resamples.
- Dashboard: preserve the JSON schema consumed by the frontend, including measurement schema version, section status and missing-data semantics, keyword grids, truncation rules, and aggregate-only diversity rows. Non-finite numbers serialize as null. Reject empty training samples.
- Support existing report input options and Redis configuration. A new invocation may use the project's established Spark launcher, with explicit mappings for CLI options if needed.
- Keep the frontend and current committed dashboard snapshot unchanged unless a verified fixture exposes a compatibility issue.

## Verification

Use small deterministic fixtures to compare Scala outputs with existing Python calculations. Cover recall holdout exclusion and ties; ranking ties and single-class data; off-policy feature leakage, missing candidates, constant labels and reproducibility; dashboard schema, unavailable sources, empty input and strict JSON. Compile and run focused Scala tests, existing relevant contract tests, and an end-to-end export from fixture Parquet with a controlled Redis catalog. Check updated documentation commands against implemented entry points.

## Delivery

Deliver the Scala source, compatibility tests, and runnable report documentation. Report any numerical or environmental limitations explicitly. No deployment, removal of Python services, or unrelated architecture changes are included.
