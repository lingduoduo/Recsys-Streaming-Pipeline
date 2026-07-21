# Analysis Reports

Offline reports over the engagement data (`training_samples` Parquet, joined with movie
genres/embeddings from Redis where needed). The first three are PySpark (run via
`"$SPARK_HOME/bin/spark-submit"`); the two retrieval-eval reports are self-contained pandas/Python
(hand-rolled metrics, run with plain `python`). All write CSVs under `<input>/../report-*`.

| Report | Script | What it shows | Outputs |
|--------|--------|---------------|---------|
| **Keyword / SubKeyword Distributions** | `KeywordAnalysisReportJob` (Scala) | Distribution of keyword (1st genre) & subkeyword (2nd genre) for movies vs queries; per-category (l1/l2/l3) top keywords | `by_keyword`, `by_subkeyword`, `top_keywords_l1/l2/l3` |
| **Query Analysis** | `QueryAnalysisReportJob` (Scala) | Most-common queries (genre-combo intent); short (≤10 chars) vs long (>10) query engagement | `top_queries`, `by_query_length` |
| **Relevance Analysis** | `RelevanceAnalysisReportJob` (Scala) | Relevance-state (impression/click/order) distribution + mean score by query and by movie genre | `by_state`, `by_query`, `by_genre` |
| **Recall-Task Performance** | `recall_eval_report.py` | BM25 vs embedding vs hybrid (RRF) retrieval, leave-one-out per user | `recall_eval.csv` (recall@k, hitrate@k) |
| **Ranking Performance** | `ranking_eval_report.py` | logloss + ROC-AUC of ranking signals (popularity / position / embedding) vs the click label | `ranking_eval.csv` |
| **Off-policy evaluation** | `ope_eval_report.py` | Direct-Method policy value + lift-vs-logging with 95% bootstrap CIs, over the Redis replay buffer | `ope_eval.csv` |
| **Consolidated Dashboard** | `analysis_dashboard_report.py` | All five analyses + the off-policy card (live) and MDP card (from `mdp_eval.csv`) as one self-contained HTML page | `report-dashboard/index.html` |

Definitions shared across reports: a **query** = a recommended impression's genre-combo intent
(`concat_ws(" ", genres)`); engagement label `0.0/1.0/2.0` = impression-only / clicked / ordered;
**CTR** = clicks/impressions, **CVR** = orders/impressions.

```bash
IN=/tmp/spark-recsys/movie-category-sim/training-samples   # any sim's training_samples Parquet

# Pure-Spark Scala jobs (build once: cd services/spark-streaming-job && sbt assembly)
SPARK_MAIN_CLASS=com.demo.report.KeywordAnalysisReportJob   KEYWORD_ANALYSIS_INPUT_PATH="$IN"   ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.report.QueryAnalysisReportJob     QUERY_ANALYSIS_INPUT_PATH="$IN"     ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.report.RelevanceAnalysisReportJob RELEVANCE_ANALYSIS_INPUT_PATH="$IN" ./run-streaming-job.sh

# Retrieval-eval reports (plain Python; need movie:{id}:features, i2vEmb/uEmb in Redis)
REDIS_HOST=localhost python services/python-modeling/recall_eval_report.py  --input "$IN"
REDIS_HOST=localhost python services/python-modeling/ranking_eval_report.py --input "$IN"

# Consolidated HTML dashboard (plain python; recall/ranking sections need Redis corpus)
REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py --input "$IN"
```

See `docs/specs/` and `docs/plans/` for each report's full spec/plan.
