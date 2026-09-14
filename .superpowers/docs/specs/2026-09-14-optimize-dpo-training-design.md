# Optimize DPO training design

## Problem and decision

The DPO arm's cost is not in fitting. `dpo.fit` takes about 126 ms for 5,000 pairs over its 200 default epochs, dominated by `run_backward`, and it already hoists the reference tensors out of the epoch loop. The cost is upstream, in `slate_pairs.build_pairs`, and most of it is discarded.

`replay_index` calls `ope_eval_report.candidate_features` for every candidate of every replay event, eagerly. A preference pair only exists where a slate carried both an engagement and a non-engaged item. At the low click-through this pipeline runs in — the same assumption the GRPO zero-variance gate is built around — most slates carry no engagement, so most candidates never appear in a pair and their feature vectors are built for nothing.

Hold the candidate dict in the index and extract its features on first use, memoized. Pairs, diagnostics and drop counts come out identical; the work simply follows the pairs that exist.

Three smaller repeats in the same function are fixed in the same pass, because they are the same mistake at smaller scale: `is_chosen` is evaluated twice per slate item by a comprehension per side; `indexed_request_ids` makes a second full pass over every index key to recover a request-id set the indexing loop could have collected; and the `str(item_id)` keys are rebuilt inside the chosen-by-rejected cross product rather than once per item.

`dpo.fit` is deliberately out of scope. Its only lever is fusing the two per-epoch forward passes into one, worth perhaps 10 to 15% of 126 ms, and a fused matmul over 2n rows can select different BLAS blocking than two n-row calls. Over 200 Adam epochs any bit-level difference compounds into different weights and a different reported pairwise accuracy, so the arm would need re-baselining for a small gain.

## Global constraints

- Python 3 with numpy and torch as already pinned; no new dependencies.
- Preserve `build_pairs(slates, events, names=None)` and its `(pairs, dropped, JoinDiagnostics)` return shape. `PreferencePair`, `JoinDiagnostics` and `REFERENCE_PRED_KEY` keep their fields and values.
- Preserve `is_chosen(item)`, which is called directly by tests.
- Produce pairs in the same order, with the same field values, as today. `dropped`, `n_slate_request_ids`, `n_slate_request_ids_matched` and `n_missing_reference_sides` must be identical for every input.
- Preserve the reference semantics exactly: an absent or null `modelPredictions`, or an absent `predictionScore`, reads as 0.0 and counts toward `n_missing_reference_sides`.
- `replay_index` may change shape: it has no caller outside this module and no test of its own.
- Extract each used candidate's features at most once, as the eager index did. The memo must not turn reuse across pairs into repeated extraction.

## Implementation

`replay_index(events)` drops its `names` parameter and returns `(index, request_ids)`, where `index` maps `(requestId, item)` to the candidate dict itself and `request_ids` is every indexed requestId collected during the loop.

`build_pairs` keeps its signature. It resolves a key to `(features, reference, has_reference)` through a memo dict populated on first use, so a candidate appearing on many pairs is extracted once and a candidate appearing on none is never extracted. The reference read moves alongside the feature extraction, since it is needed exactly when a pair is built.

Within each slate, one pass over `items` partitions into chosen and rejected while evaluating `is_chosen` once per item, and each side's `(requestId, item)` key is built once as the item is partitioned rather than inside the cross product.

## Validation and acceptance

1. For a fixture where pairs exist, `build_pairs` returns pairs equal to today's — same order, same `request_id`, `user`, `chosen_item`, `rejected_item`, both feature lists, both reference floats — and the same `dropped` and all three `JoinDiagnostics` fields. Compared against the pre-change implementation, not against hand-written expectations.
2. The same equality holds for: a total join failure (no slate id in the replay), a slate with no engagement, a slate with no unengaged item, a slate whose items are absent from the replay on one side only, a candidate with null `modelPredictions`, a candidate missing `predictionScore`, and a Parquet-round-tripped slate whose nested items arrive as ndarrays.
3. `candidate_features` is called once per distinct used key and never for an unused one. Asserted by counting calls, which is available here because `build_pairs` reaches it through the `ope_eval_report` module attribute.
4. `is_chosen` is called once per slate item.
5. The existing DPO suite passes unchanged, including `post_train_dpo.main` end to end and the `dpoScore` registration checks.
6. Benchmark evidence records `replay_index` and `build_pairs` before and after at 100%, 30% and 5% engaged slates, plus the zero-join case.

## Measured baseline and limits

Fixture of 5,000 replay events at ten candidates each, five feature names, slates matching one-to-one, median of five runs:

| engaged slates | pairs built | `replay_index` | `build_pairs` | index share | feature extraction discarded |
|---|---|---|---|---|---|
| 100% | 45,000 | 99.0 ms | 172.6 ms | 57% | 0% |
| 30% | 13,500 | 97.2 ms | 141.0 ms | 69% | 70% |
| 5% | 2,250 | 94.0 ms | 107.1 ms | 88% | 95% |
| join fails | 0 | 84.0 ms | 95.8 ms | 88% | 100% |

The zero-join row is not hypothetical: `post_train_dpo`'s own docstring records that slates built without `RECSYS_GRPO_EMIT_EVENTS=true`, which is the default, mint their own request ids and can never join.

Limits. The gain is proportional to how much of the index goes unused, so it is largest at low click-through and approximately nil at full engagement, where a small memo overhead may make the change marginally slower. These are single-process timings on one machine with a synthetic fixture; real slate and candidate shapes vary. This addresses the join, not the end-to-end script, which also pays Parquet or Redis load, `dpo.fit`, `score_events` over every candidate, and a Parquet write. No claim is made about total runtime.

## Delivery

Publish this spec and its implementation plan on `optimize/dpo-training` in a draft PR against `master`, then add the reviewed code, tests and benchmark evidence to the same PR and mark it ready, as the SFT and PPO optimizations were delivered.
