# Make the dashboard's off-policy section reachable and honest design

## Problem and decision

The dashboard's off-policy evaluation section grades fifteen policies, every one of them a score
field belonging to the retrieval service that left this repository in PR #242 --
`model:banditScore`, `model:deepLearningScore`, `model:qValue`, `model:contentScore` and so on. None
of the four post-training arms appears, although all four exist and PRs #221-#227 built them.

Three separate facts produce that, and only the third is a stale snapshot.

**The section's input has no producer in this repository.** `compute_ope` reads
`replay:recommendations` from Redis. Nothing here writes that key: no `.scala` file mentions it, and
every Python reference is a reader (`ope_support.load_from_redis`, `replay_export.py`,
`post_train_dpo.py`, `post_train_q.py`, `compute_ope` itself). The departed service wrote it, from
its `/feedback` handler. On a fresh checkout the section is therefore N/A on every run, and its
N/A text -- "no replay-buffer events with reward in Redis" -- reads as an empty buffer rather than
an absent writer.

`recsys-pipeline/README.md:314` states the opposite: "The `replay:recommendations` Redis list is
populated by `ExperienceCollectorStreamingJob`." That job writes a Kafka topic and an optional
Parquet slate sink. It performs no Redis write at all. This is the same defect class as the
`user:{id}:served_history` claim corrected in #247 -- a document crediting this repository with a
writer it does not contain.

**The post-training arms are structurally unreachable from the dashboard.** They are not missing
from the code. `ope_eval_report.POLICY_ONLY_PRED_KEYS` is `("tabQ", "fqiQ", "dpoScore",
"grpoScore")`, and `policy_names` admits every numeric `modelPredictions` key as a `model:*` policy,
so an event carrying `dpoScore` produces a `model:dpoScore` row with no further work.
`post_train_dpo.score_events` writes `dpoScore` into every candidate in place, and
`post_train_q.py` writes `tabQ` and `fqiQ`; both persist the result through `--output-parquet`,
documented as being "for ope_eval_report.py --parquet". `compute_ope` accepts no Parquet path. The
scored replay exists on disk and the dashboard cannot read it.

**The committed snapshot is pre-split.** `frontend/data/dashboard.json` holds an `ope` block from a
run made while the service still lived here, which is why fifteen serving-side policies appear at
all rather than nothing.

Fix the reachability and the honesty. Give `compute_ope` the Parquet input its sibling already has,
thread it through the exporter, state in the payload which source the rows came from, correct the
N/A text, and correct the false writer claim.

## What this does and does not claim

It claims that after this change a reader who has run `post_train_dpo.py --output-parquet <file>` or
`post_train_q.py --output-parquet <file>` can pass that file to the exporter and see
`model:dpoScore`, `model:tabQ` and `model:fqiQ` in the dashboard, because `policy_names` already
admits them and only the input path was missing. It claims the section states which source produced
its rows, and that its N/A text names the actual reason.

It does not claim the section becomes populated by default. Nothing in this repository writes
`replay:recommendations`, and no simulation runs post-training -- `grep post_train scripts/*.sh` is
empty. On a default run the section is N/A, as it is today, but for a stated reason instead of a
misleading one. Wiring post-training into a simulation was considered and rejected as a separate
decision about run time, not a documentation or plumbing fix.

It does not claim the fifteen serving-side policies are wrong to show. When the serving path is
running and writing the buffer, those rows are what the serving path actually scored, and they are
the correct content for a Redis-sourced run. What was wrong was the absence of any way to see the
other four, and the absence of any indication of which world a reader was looking at.

It does not address the circularity recorded for this estimator: every `model:*` policy is graded by
a reward model fit on features that include the taken action's own score, except the four
`POLICY_ONLY_PRED_KEYS`, which `feature_names` excludes for exactly that reason. Adding a Parquet
input does not change that, and the four arms this change surfaces are precisely the ones already
protected from it.

It makes no claim about `frontend/data/dashboard.json`, which is **not** regenerated. That snapshot
is not byte-reproducible, and the new `source` key will be absent from it until someone regenerates
it, so every consumer must treat `source` as optional.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Four commits, reviewable independently and in this order: the data path, the payload and its
  renderer, the false claim and its guard, the documentation.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `compute_ope`'s existing five parameters keep their names, order and defaults. `parquet` is added
  last, defaulting to `None`, so every existing call site and test is unaffected.
- `build()` in `export_dashboard_json.py` gains `ope_parquet` as its **last** parameter, after
  `config`. Its only caller passes all six arguments positionally -- `build(args.input, host, port,
  args.experiences, args.live_metrics, {...})` at line 147 -- so inserting the new parameter next to
  the other inputs would land the config dict in `ope_parquet`. Reading position rather than
  assuming keyword use is the whole reason this constraint exists.
- `MEASUREMENT_SCHEMA_VERSION` stays `"2.0"`. The `ope` block is a diagnostic section, not one of the
  seven measurement sections, and `validate_measurements.mjs` neither lists `ope` in
  `DIAGNOSTIC_ROWS` nor rejects unknown keys -- so adding `source` needs no validator change and the
  unregenerated snapshot stays valid.
- `frontend/components/sections.jsx` is touched only inside `OpeSection`: the `NaCard` reason and one
  fine-print line. No other section, no layout, no CSS.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.

## Implementation

### Commit 1 -- the data path

Add `parquet: str | None = None` as the last parameter of `compute_ope` in
`services/python-modeling/analysis_dashboard_report.py`. When it is set, load events with
`ope_support.load_from_parquet(parquet)`; otherwise keep the existing Redis path unchanged, including
its `except Exception: return None` for an unreachable Redis. A Parquet path that does not exist is a
caller error rather than a missing optional input, so it is not swallowed: the loader's own exception
propagates, which is how `ope_eval_report.py --parquet` already behaves.

The reward-model fit, the bootstrap and the row shape are untouched. This is the whole mechanism by
which the post-training arms appear: they were already admitted by `policy_names`.

### Commit 2 -- provenance and the two strings

Add `"source"` to the dict `compute_ope` returns: `f"parquet:{parquet}"` when the Parquet path was
used, `f"redis:{key}"` otherwise.

Thread the input through the exporter: `build(..., ope_parquet: str | None = None, ...)` passes it to
`dash.compute_ope(host, port, parquet=ope_parquet)`, and `--ope-parquet` is added to `parse_args`
with help text naming `post_train_dpo.py` / `post_train_q.py --output-parquet` as its producer.

In `OpeSection`, replace the `NaCard` reason. The current text -- "no replay-buffer events with
reward in Redis" -- describes an empty buffer. The replacement states that this repository writes no
`replay:recommendations`, that the serving path does, and that `--ope-parquet` is the offline route.
Add the source to the existing fine-print line, tolerating its absence with a fallback, because the
committed snapshot predates the key.

### Commit 3 -- the false claim and its guard

Correct `recsys-pipeline/README.md:314` to say what writes the buffer: the serving path, in
`lingduoduo/Recsys-Backend-Service`, and that `ExperienceCollectorStreamingJob` writes the
`training_experiences` Kafka topic plus an optional Parquet slate sink instead.

Add a guard in the family of `test_retrieval_service_extracted.py`: no live documentation may claim
`ExperienceCollectorStreamingJob` populates `replay:recommendations`. The assertion pairs the job
name with the key within a single line, and names the file and line when it fires. A prose-level
check is the only kind available -- the claim is about a Redis key a Scala job does not write, and
nothing compiles a sentence.

### Commit 4 -- the documentation

`frontend/README.md`: document `--ope-parquet` in the refresh section beside `--experiences` and
`--live-metrics`, and say what the off-policy section shows from each source.

`docs/recommendation_architecture/Analysis_Report.md`: the off-policy passage gains the two-source
distinction -- Redis for what the serving path scored, the post-training Parquet for the four arms.

`recsys-pipeline/README.md`: the exporter's flag list gains `--ope-parquet`.

## Validation and acceptance

1. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline at this branch point is
   563 passed, 2 skipped. This change adds five tests and deletes none, so the expected result is
   568 passed, 2 skipped, zero failures.
2. A `compute_ope` call with a Parquet fixture whose candidates carry `dpoScore` returns rows
   including `model:dpoScore`, and `source == "parquet:<path>"`.
3. A `compute_ope` call with both a Parquet path and a reachable Redis uses the Parquet -- proven by
   the policy list, not by mocking.
4. `compute_ope(host, port)` with no Parquet and no Redis still returns `None`, unchanged.
5. `cd frontend && python3 export_dashboard_json.py --help` lists `--ope-parquet`.
6. `cd frontend && npm run validate:data` passes against the unregenerated snapshot.
7. `grep -rn 'no replay-buffer events with reward in Redis'` over live trees returns nothing.
8. `git diff --name-only origin/master` lists no `frontend/data/dashboard.json`.
9. `git diff --check` clean.

## Limits

The section is still N/A on a default run, and this change does not alter that. It converts an
unreachable surface into a reachable one and a misleading N/A into an accurate one. Someone must run
post-training and pass the Parquet, or run the serving path so it writes the buffer.

`source` is a string the payload carries, not a guarantee about the rows. It says which input the
events came from; it does not say when they were collected, which model version scored them, or
whether the Parquet's scores are current. A reader who passes a month-old Parquet gets a month-old
answer labelled `parquet:<path>`, and that label is the whole of the provenance on offer.

The guard in commit 3 is a string pairing on a single line. Prose that splits the job name and the
key across two sentences would pass it while saying the same false thing. It catches the sentence
that exists today and its close variants, which is what a prose-level guard can do.

Nothing here makes the four arms comparable to the eleven serving-side scores in one table. They are
never present in the same events: the serving scores come from the buffer the service writes, the
arm scores from a Parquet that post-training writes from that same buffer. A run shows one world or
the other, which is why `source` exists rather than a merged view.
