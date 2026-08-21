# Analysis Reports

This is the focused reference for choosing, generating, and interpreting offline analysis
artifacts. For the only canonical end-to-end setup, movie-category simulation, React snapshot,
and teardown sequence, follow the [pipeline README](../../README.md#canonical-finite-local-workflow).
The commands here assume that workflow has already reached its literal `==> done` signal.

## Choose the artifact

| Desired artifact | Generator | Primary output |
|---|---|---|
| Keyword and subkeyword CSVs | `KeywordAnalysisReportJob` | `<input>/../report-keywords/` |
| Query-intent CSVs | `QueryAnalysisReportJob` | `<input>/../report-queries/` |
| Relevance CSVs | `RelevanceAnalysisReportJob` | `<input>/../report-relevance/` |
| Recall evaluation CSV | `recall_eval_report.py` | `<input>/../report-recall-eval/recall_eval.csv` |
| Ranking evaluation CSV | `ranking_eval_report.py` | `<input>/../report-ranking-eval/ranking_eval.csv` |
| Off-policy evaluation CSV | `ope_eval_report.py --output <file>` | the exact `--output` path |
| Standalone self-contained HTML | `analysis_dashboard_report.py` | `<input>/../report-dashboard/index.html` |
| React dashboard snapshot | `frontend/export_dashboard_json.py` | `frontend/data/dashboard.json` (from `recsys-pipeline/`) |

The standalone HTML can be opened directly. The React app renders a static JSON snapshot and does
not query Spark or Redis in the browser.

## Shared definitions

- A **query** is a recommended impression's genre-combination intent,
  `concat_ws(" ", genres)`.
- Engagement label `0.0`, `1.0`, or `2.0` means impression-only, clicked, or ordered.
- **CTR** is clicks divided by impressions; **CVR** is orders divided by impressions.
- **Recall@k** is the mean fraction of held-out relevant items recovered in the top `k`;
  **hitrate@k** is the fraction of evaluated users with at least one hit.
- Ranking **ROC-AUC** measures ordering quality; **logloss** is computed after per-signal
  z-score/sigmoid calibration; **coverage** is the fraction of impressions a signal can score.
- Off-policy **value** is a Direct-Method reward estimate. Its 95% event-bootstrap intervals are
  conditional on the fitted reward model and do not include model-fit uncertainty.

## Prerequisites and inputs

Run the setup block from the repository root:

```bash
cd recsys-pipeline
python -m pip install -r services/python-modeling/requirements.txt
python -m pip install pandas pyarrow numpy redis
(cd services/spark-streaming-job && sbt assembly)
test -s services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

All engagement reports need a populated `training_samples` Parquet directory, not merely an
existing directory. The examples use:

```text
/tmp/spark-recsys/movie-category-sim/training-samples
```

Keep Redis running after the movie-category simulation:

- `movie:*:features` supplies the movie title/genre corpus and fills missing Python-report
  metadata.
- `i2vEmb:*` and `uEmb:*` enable embedding recall and ranking.
- `global:item_popularity` enables the ranking popularity signal.
- `replay:recommendations` supplies feedback-completed events for off-policy evaluation.

The three Scala CSV jobs use the Parquet `genres` column directly and do not enrich it from Redis.
The Python loader can enrich rows whose `genres` array is empty from
`movie:{item_id}:features`. Without those hashes, keyword/query values can become `unknown`,
category tables can be empty, and recall is unavailable. An empty Parquet input is rejected.

## Spark keyword, query, and relevance reports

Run this focused block from the repository root after the Spark assembly exists:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples

SPARK_MAIN_CLASS=com.demo.report.KeywordAnalysisReportJob \
  KEYWORD_ANALYSIS_INPUT_PATH="$IN" ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.report.QueryAnalysisReportJob \
  QUERY_ANALYSIS_INPUT_PATH="$IN" ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.report.RelevanceAnalysisReportJob \
  RELEVANCE_ANALYSIS_INPUT_PATH="$IN" ./run-streaming-job.sh
```

Outputs:

- `report-keywords/by_keyword`, `by_subkeyword`, and
  `top_keywords_l1` / `top_keywords_l2` / `top_keywords_l3`;
- `report-queries/top_queries` and `by_query_length`;
- `report-relevance/by_state`, `by_query`, and `by_genre`.

Each named output is a Spark CSV directory containing a header-bearing part file.

## Recall evaluation

`recall_eval_report.py` compares lexical BM25, embedding cosine similarity, and reciprocal-rank
fusion under a leave-one-out protocol. A user needs at least two distinct clicked items
that exist in the Redis movie corpus to be evaluated. BM25 needs `movie:*:features`; embedding and
hybrid results additionally need `i2vEmb:*`.

Run from the repository root:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples
REDIS_HOST=localhost python services/python-modeling/recall_eval_report.py \
  --input "$IN" \
  --ks 5,10,20
```

The command prints metrics per method and `k`, then writes
`$IN/../report-recall-eval/recall_eval.csv` with `method`, `k`, `recall_at_k`,
`hitrate_at_k`, `users_evaluated`, and `instances`. Use `--outdir` to override the directory.
If Redis has no movie corpus, the script reports that it cannot evaluate and exits without a CSV.

## Ranking evaluation

`ranking_eval_report.py` compares global popularity, negative impression position, and
`dot(uEmb[user], i2vEmb[item])` against the click label. Position needs only Parquet input;
popularity and embedding coverage depend on their Redis keys.

Run from the repository root:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples
REDIS_HOST=localhost python services/python-modeling/ranking_eval_report.py \
  --input "$IN"
```

The output is `$IN/../report-ranking-eval/ranking_eval.csv` with `signal`, `n`, `positives`,
`coverage`, `auc`, and `logloss`. Use `--outdir` to override the directory. AUC is empty when a
signal has only one observed class; embedding metrics are empty when no rows have both vectors.

## Off-policy evaluation

`ope_eval_report.py` fits a dependency-light logistic reward model to logged taken-action features,
then re-picks every event under logging, popularity, CTR, deterministic random, and available
`model:*` policies. It requires feedback-completed events with an observed reward. Those events
are normally written to `replay:recommendations` by later `POST /feedback` calls.

`post-training/post_train_q.py` fits offline Q functions on the same replay buffer and injects
their per-candidate predictions as `tabQ` and `fqiQ`, which the evaluator discovers automatically
as `model:tabQ` and `model:fqiQ`. Those two keys are policy scores, not observations, so
`feature_names()` excludes them from the reward model's schema (`POLICY_ONLY_PRED_KEYS`) while
`policy_names()` keeps them: an estimator fit on the Q values it is then asked to grade would
report lift for a policy that learned nothing.

Caveats when reading those rows. The Direct Method estimator is single-step, so it cannot credit
the long-horizon value that Q-learning exists to capture — the held-out mean `|TD error|` printed
by the trainer is the complementary check. That residual is reported per algorithm and is not a
head-to-head: the two arms fit different state representations. The tabular residual in particular
must be read against the held-out Q-table coverage printed beside it, because an unseen
`(state, action)` scores 0.0 and so flatters a sparse table. And the fitted Q is trained on the
non-held-out split while policy values are computed over all events, the same footing as the
reward model itself.

`post-training/post_train_dpo.py` adds `model:dpoScore` from the same replay dump, fit on
preference pairs drawn from the slate log. Read its held-out pairwise accuracy against the
reference accuracy printed beside it: the reference is the logged `predictionScore`, so a policy
that fails to beat it has learned nothing the logging policy did not already know. Check the
reported join yield first — pairs whose replay row is missing are dropped, and a low yield means
the accuracy figures describe a small and possibly unrepresentative subset of the slate log.

Run the Redis-backed evaluation from the repository root:

```bash
cd recsys-pipeline
REDIS_HOST=localhost python services/python-modeling/ope_eval_report.py \
  --key replay:recommendations \
  --output /tmp/spark-recsys/movie-category-sim/ope_eval.csv
```

The CSV includes policy value, lift versus logging, event count, reward-estimator AUC/MSE, and 95%
value/lift interval bounds. `--bootstrap-samples 0` disables intervals; `--bootstrap-seed`
controls deterministic resampling. To evaluate an exported replay dataset instead of live Redis,
replace `--key ...` with `--parquet /path/to/replay.parquet`. The script exits without output when
there are no events whose `reward` is present.

## Consolidated standalone HTML

`analysis_dashboard_report.py` recomputes relevance, keyword, query, recall, ranking, and live
off-policy sections in one HTML file. It also renders the Java MovieLens policy-evaluation CSV
when `--mdp-csv` exists. Missing Redis replay/embedding data or a missing MDP CSV becomes an
explicit N/A card rather than a fabricated metric.

Run from the repository root:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples
REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py \
  --input "$IN" \
  --ks 5,10,20 \
  --mdp-csv "$IN/../mdp_eval.csv"
```

The output is `$IN/../report-dashboard/index.html`; use `--outdir` to override it. The default MDP
input is already `$IN/../mdp_eval.csv`, so `--mdp-csv` is optional when the file is there.

## React snapshot

The canonical workflow from `recsys-pipeline/` owns snapshot generation so there is only one
end-to-end path. Its export command uses the movie-category input and validates a positive row
count plus a populated Keyword Gap L1 table. The output is
`frontend/data/dashboard.json`. After regenerating it, hard-refresh `http://localhost:3000` or
restart the development server if it still serves the old snapshot.

If validation reports `unknown` values or zero category rows, confirm that the simulation reached
`==> done`, Parquet files exist under the movie-category input, and Redis still holds
`movie:*:features`.
