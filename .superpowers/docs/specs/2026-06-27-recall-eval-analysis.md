# Spec: Recall-Task Evaluation (BM25 vs embedding vs hybrid)

## Objective

Compare three retrieval methods — **lexical (BM25)**, **embedding (cosine)**, and **hybrid
(Reciprocal Rank Fusion)** — on the engagement data, using **recall@k** and **hitrate@k**.

Delivered as a self-contained pandas/Python job (`recall_eval_report.py`).

## Protocol (chosen scope)

- **Query unit:** a user; **relevant** = the movies the user clicked/ordered (from `training_samples`).
- **Leave-one-out:** for each clicked item `h`, build the query from the user's **other** clicks
  and check whether `h` is retrieved in the top-k from the movie corpus. Users with < 2 clicks are skipped.
- **Query representation (derived from clicks):** BM25 query = the other clicks' genres + titles;
  embedding query = mean of their `i2vEmb` vectors.
- **Candidates:** the full movie corpus minus the query's (seen) items; `h` remains a candidate.

## Methods

| Method | Ranking |
|--------|---------|
| `bm25` | Okapi BM25 (`k1=1.5`, `b=0.75`) of query terms over each movie's title+genres document |
| `embedding` | cosine(query vector, item `i2vEmb`) |
| `hybrid` | Reciprocal Rank Fusion (`1/(60+rank)`) of the BM25 and embedding rankings |

## Metrics (per method × k, averaged over evaluated users)

- **recall@k** = mean over users of (held-out clicks recovered in top-k) / (#clicks)
- **hitrate@k** = mean over users of `1[≥1 held-out click recovered in top-k]`

(`k ∈ {5,10,20}` by default; under LOO these differ — recall is the per-user recovery fraction,
hitrate is per-user ≥1 coverage.)

## Inputs / outputs

- **Input:** `training_samples` Parquet (`user_id`, `item_id`, `clicked`/`label`); movie corpus
  (`title`+`genres`) and item embeddings from Redis `movie:{id}:features` and `i2vEmb:{id}`.
- **Output:** `recall_eval.csv` — `method`, `k`, `recall_at_k`, `hitrate_at_k`, `users_evaluated`, `instances`.

## Design notes

- Pure functions: `tokenize`, `build_bm25`/`bm25_score`, `cosine`, `mean_vec`, `rank_topk`, `rrf`,
  `evaluate` — unit-tested in-process (no Spark/Redis). BM25 is hand-rolled (no new dependency).
- Single-node pandas/Python (the catalog is small; retrieval is per-user×fold over the corpus).
  Run with plain `python` (no spark-submit). Embedding/hybrid degrade gracefully if `i2vEmb` is absent.

## Boundaries

- **In:** the report. Additive — reads existing Parquet/Redis, writes a CSV.
- **Out:** ANN/index serving; user-profile (uEmb) query representation; NDCG/MRR (recall/hitrate only);
  a Spark/streaming variant.

## Success criteria (testable)

- [ ] `evaluate` runs LOO per user and returns recall@k/hitrate@k per method, distinct where expected.
- [ ] BM25 ranks token-overlapping docs higher; cosine ranks nearest vectors; RRF fuses both.
- [ ] Users with < 2 clicks are skipped. (All unit-tested with hand-computed expectations.)
