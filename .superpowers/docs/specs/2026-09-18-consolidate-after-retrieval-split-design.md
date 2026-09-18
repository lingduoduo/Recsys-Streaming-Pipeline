# Consolidate after the retrieval-service split design

## Problem and decision

PR #241 made the Java retrieval service portable, #242 deleted it from this repository, and #243
corrected the five documents that still described it as living here. All three were about
*references*. This change is about what the split left *working incorrectly*, and what it left
unreachable.

Three things, in descending order of harm.

**The simulation can no longer measure the service, and says the wrong reason.**
`recsys-pipeline/scripts/run-movie-category-sim.sh` reaches the service at three bare paths --
`$SERVICE_URL/metrics`, `$SERVICE_URL/recommend/$user`, `$SERVICE_URL/feedback`. In
`lingduoduo/Recsys-Backend-Service` those endpoints are now served by
`com.recsys.api.rest.retrieval.RetrievalRecommendationController`, which is annotated
`@RequestMapping("/api/v1/retrieval")`. Every one of the three moved. The burst is gated on
`curl -sf "$SERVICE_URL/metrics"`, which 404s against the current backend, so the sim takes the
else branch and prints `no service answering at $SERVICE_URL -- latency stays N/A` even when the
backend is running and healthy. The latency, freshness and filter-decision cards are unreachable,
and the diagnosis printed is false: the service *is* answering, at a different path.

Note what does not happen, because it constrains the fix. The backend also has
`HealthController`, mapped at `/health`, with its own `/metrics`. That is `/health/metrics`, not
`/metrics`, so the probe does not accidentally capture the wrong payload into
`live-metrics.json`. The failure is a clean 404, not a silent mismeasurement. Had `HealthController`
been mapped at the root, this would have been a data-corruption bug rather than a dead-branch bug.

**The MDP policy-evaluation card is unreachable by construction.** The evaluator that wrote
`mdp_eval.csv` (`MovieLensPolicyEvaluation`) left with the service. Nothing in this checkout can
produce that file. Yet the plumbing that consumes it survives in six places: the sim defines
`MDP_CSV` and passes `--mdp-csv` twice, `analysis_dashboard_report.py` carries `compute_mdp` and
`_mdp_section` and an N/A fallback, `frontend/export_dashboard_json.py` carries a positional
`mdp_csv` parameter and its own `--mdp-csv` and a path-inference comment explaining why the flag
must be passed to two different programs, `frontend/components/sections.jsx` renders `MdpSection`,
`frontend/app/page.jsx` mounts it, and `frontend/data/dashboard.json` still carries a populated
`mdp` block from a run made before the extraction. Four test sites assert this machinery,
including one in `test_service_scripts.py` that asserts the dead variable is still present.

**The repository has two spec homes.** `.gitignore` establishes `.superpowers/docs/` as the
versioned location for specs and plans -- `/.superpowers/*` is ignored and `!/.superpowers/docs/`
is negated back in. Twenty design documents live there. But PR #241 wrote its spec and plan to a
second tree, `docs/superpowers/`, which is the only content under the root `docs/` directory.
`test_retrieval_service_extracted.py` then had to list `docs/superpowers/` alongside
`.superpowers/` in `HISTORICAL_PREFIXES` so its grep would skip both.

Fix all three. Correct the service paths and make route drift report differently from service
absence; delete the MDP chain end to end; move the stray spec tree into the versioned one.

## What this does and does not claim

It claims that after this change, a reader running `run-movie-category-sim.sh` against a healthy
`Recsys-Backend-Service` gets the latency, freshness and filter-decision cards populated rather
than N/A, and that a reader running it against a backend whose retrieval routes have moved *again*
is told that specifically, rather than being told no service is answering.

It does not claim the route contract is verified ahead of time. Nothing in this repository can
know where the backend serves its routes without either a network call to a running instance or a
checked-in copy of the backend's route table -- and a checked-in copy is one more artifact that
drifts, detected by nothing, which is the defect being fixed rather than a fix for it. The
detection this change adds is run-time and loud: when the sim runs, drift is named. When the sim
does not run, drift is invisible, exactly as today.

It does not claim the MDP capability is preserved anywhere in this repository. It is deleted, not
relocated. A reader who wants MDP policy evaluation runs it in `Recsys-Backend-Service` and reads
its output there; this repository's dashboard no longer has a card for it, N/A or otherwise.

It makes no claim about the numbers in `frontend/data/dashboard.json` beyond the removal of one
key. That snapshot is not byte-reproducible -- two freshness age fields shift on every
regeneration -- so it is edited surgically rather than regenerated, and every other value in it is
left exactly as committed.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Three commits, reviewable independently and in this order: contract, orphans, documentation.
  The documentation pass is last because it must describe what the first two landed.
- `frontend/data/dashboard.json` is edited by deleting the single `mdp` key. It is not
  regenerated, and no other key in it changes.
- The seven measurement sections -- relevance, satisfaction, freshness, diversity, fairness,
  safety, latency -- are untouched. `mdp` is a *diagnostic* section, not one of the seven, so
  `frontend/validate_measurements.mjs` (whose `SECTIONS` and `DIAGNOSTIC_ROWS` never name `mdp`)
  needs no change and every "seven measurement sections" claim in the documentation stays true.
- The `SERVICE BURST` block keeps the properties two existing tests assert: it contains `|| true`
  or `continue`, and contains no `set -e`. A missing or moved service must never fail the run.
- `test_retrieval_service_extracted.py` keeps scanning for `java-retrieval-service`. Only its
  `docs/superpowers/` exemption is removed, because the path it exempts ceases to exist.
- The heading `## Retrieval Service Configuration` in `recsys-pipeline/README.md` keeps its exact
  text; `3_Cold_Start.md:45` and `6_Predicting_Scoring.md:75` link to its anchor.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten, and
  `.py`/`.scala` provenance comments naming Java classes that produced fixtures are left alone.

## Implementation

### Commit 1 -- the route contract

In `run-movie-category-sim.sh`, add `RETRIEVAL_BASE="${RETRIEVAL_BASE:-/api/v1/retrieval}"` beside
the existing `SERVICE_PORT` and `SERVICE_URL` definitions, and route all three calls through
`$SERVICE_URL$RETRIEVAL_BASE`. `SERVICE_URL` keeps meaning the origin, so an operator pointing at a
differently-prefixed deployment overrides `RETRIEVAL_BASE` alone.

Replace the two-outcome gate with three. Probe `$SERVICE_URL$RETRIEVAL_BASE/metrics` first; on
success, run the burst unchanged. On failure, probe `$SERVICE_URL/health/live` -- the backend's
liveness endpoint, which is stable across route reorganisations because it belongs to
`HealthController` rather than to the retrieval controller. If liveness answers, print a drift
message naming `$RETRIEVAL_BASE` and the fact that the service is up; if it does not, print the
existing absence message. Both failure paths leave the cards N/A and neither fails the run.

Pin the shape in `test_service_scripts.py`: assert the three retrieval calls all derive from a
single `$RETRIEVAL_BASE` variable rather than being spelled out independently, and assert the
drift branch exists and is distinct from the absence branch. This is a repo-local structural
assertion -- see Limits.

### Commit 2 -- the orphans

Delete the MDP chain, in dependency order so nothing references a removed symbol at any point:

- `frontend/app/page.jsx`: the `MdpSection` import and its `<MdpSection data={data.mdp} />` use.
- `frontend/components/sections.jsx`: `MdpSection`.
- `frontend/data/dashboard.json`: the `mdp` key.
- `frontend/export_dashboard_json.py`: the `mdp_csv` parameter of `build()`, the `mdp =
  dash.compute_mdp(...)` call, the `"mdp"` entry in the returned dict, the `--mdp-csv` argument,
  and the path-inference block and comment in `main()`. `build()` loses a positional parameter;
  its only caller is `main()` in the same file.
- `services/python-modeling/analysis_dashboard_report.py`: `compute_mdp`, `_mdp_section`, the
  `--mdp-csv` argument, and the three lines in `main()` that infer the path, compute the card and
  append it or its N/A fallback.
- `scripts/run-movie-category-sim.sh`: the whole MDP banner block, `--mdp-csv "$MDP_CSV"` in the
  analysis-report invocation, and the `export_args` line that conditionally adds it.
- Tests: `test_compute_mdp_reads_csv_and_missing_is_none` and the MDP half of
  `test_ope_and_mdp_section_renderers` in `test_analysis_dashboard.py`; the `--mdp-csv` fixture
  and the two MDP assertions in the end-to-end report test in the same file; `"mdp"` in the
  diagnostic-set assertion in `test_dashboard_measurement_contract.py`; and the
  `MDP_CSV="$SIM_ROOT/mdp_eval.csv"` assertion in `test_service_scripts.py`.

Then `git mv` the two files under `docs/superpowers/` into `.superpowers/docs/specs/` and
`.superpowers/docs/plans/`, remove the emptied root `docs/` directory, and drop `docs/superpowers/`
from `HISTORICAL_PREFIXES` in `test_retrieval_service_extracted.py` along with the sentence in its
module docstring that names the tree.

### Commit 3 -- the documentation

Two jobs, both prose.

Remove the MDP text the second commit orphans: `recsys-pipeline/README.md` at the `--mdp-csv`
invocation example and the surrounding sentences about MDP cards rendering N/A, the MDP paragraph
in the two-evaluator discussion, and the diagnostic list that names MDP;
`docs/recommendation_architecture/Analysis_Report.md` at the `--mdp-csv` example and the two
sentences about default MDP input paths; `frontend/README.md` at the six sites that describe the
flag, its default, and the card.

State the two-repository boundary once. The root `README.md` -- 62 lines, an orientation page --
becomes the canonical statement of what lives here and what lives in
`lingduoduo/Recsys-Backend-Service`, including the `/api/v1/retrieval` prefix the first commit
introduced and the `RETRIEVAL_BASE` override. The other live files that currently re-explain the
split link to that section instead of restating it, with one exception: `recsys-pipeline/README.md`
Quick Start Step 2 keeps its concrete clone-and-run instructions, because a reader following
numbered steps should not have to leave the page.

## Validation and acceptance

1. `bash -n recsys-pipeline/scripts/run-movie-category-sim.sh` is clean.
2. The sim's three retrieval calls all contain `$RETRIEVAL_BASE`, and no bare `$SERVICE_URL/metrics`,
   `$SERVICE_URL/recommend` or `$SERVICE_URL/feedback` remains.
3. `grep -rn 'mdp' --include='*.py' --include='*.sh' --include='*.jsx' --include='*.mjs'` over
   `scripts/`, `services/`, `frontend/` and `integration-tests/` returns nothing, case-insensitive.
4. `frontend/data/dashboard.json` parses, has no `mdp` key, and every other top-level key is
   byte-identical to the committed version -- verified by diffing the JSON with the `mdp` key
   removed from the original, not by regenerating.
5. `cd recsys-pipeline/frontend && npm run validate:data && npm run build` both succeed.
6. `git ls-files docs/` is empty and `git ls-files .superpowers/docs/ | wc -l` has grown by two.
7. `python3 -m pytest -q` from `recsys-pipeline`. The baseline is 558 passed, 1 skipped; this
   change *deletes* tests, so the expected result is a lower count, and the plan records the exact
   number. Asserting "unchanged" would be the wrong gate. What must hold is zero failures and that
   the drop equals the number of tests deliberately removed.
8. `grep -rn 'java-retrieval-service'` returns only `.superpowers/`, `.planning/` and `.py`/`.scala`
   provenance comments -- and no longer any `docs/superpowers/` path, that tree having moved.
9. `git diff --check` clean.

## Limits

The route-drift detection added by the first commit is run-time, not build-time. It fires when
someone runs the simulation against a live backend. It does not fire in CI, it does not fire on a
pull request in either repository, and it cannot fire at all if the backend moves its routes while
nobody runs the sim. The alternative -- committing the backend's route table here and testing
against it -- was rejected because that artifact would itself drift undetected, reproducing the
defect one level up. The honest position is that the sim now reports drift accurately when it
observes it, and that observing it still requires running the sim.

The structural test on the sim script asserts that the three calls share one variable. It catches a
future edit that hardcodes a fourth path; it does not catch `RETRIEVAL_BASE`'s default being wrong.
Only a live probe can catch that, and a live probe cannot run in CI without a backend to probe.

Deleting the MDP card is irreversible in the sense that restoring it means rewriting the renderer,
the exporter entry and the two CLI flags, not reverting a feature toggle. That is the intended
trade: the capability genuinely left this repository, and a permanently-N/A card that no code path
can populate is worse documentation than no card, because it implies the measurement is merely
missing rather than relocated.

The documentation commit consolidates the boundary statement into the root README and links to it.
Anyone reading only `recsys-pipeline/README.md` -- which is 1117 lines and the file most readers
actually open -- now follows a link for the full boundary rather than reading it in place. Step 2's
concrete instructions are exempted precisely because that trade is wrong inside a numbered
walkthrough, but the trade is still being made everywhere else.
