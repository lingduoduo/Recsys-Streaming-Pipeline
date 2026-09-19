# Drop the HTML dashboard renderer design

## Problem and decision

The dashboard has two renderers of the same computations.

`services/python-modeling/analysis_dashboard_report.py` is 742 lines. Lines 1-493 compute: sample
and slate loading, six `compute_*` functions, `measurement_config`, `build_measurement_dashboard`,
and the `_live_*` mappers that fold a running backend's `/metrics` payload into the offline column
names. Lines 494-742 render: `_esc`, `svg_bar`, `svg_line`, `html_table`, `section`, `na_card`,
`render_html`, five `_*_section` builders, `_ci`, `_ope_section`, `main`, and the `__main__` guard.
That second half writes a self-contained `index.html` covering six sections -- relevance, keyword,
query, recall, ranking, off-policy evaluation.

`frontend/export_dashboard_json.py` imports the first half and writes `frontend/data/dashboard.json`,
which the Next.js app renders as thirteen sections: the seven measurement sections from
`build_measurement_dashboard` (relevance, satisfaction, freshness, diversity, fairness, safety,
latency) plus six diagnostic ones. Those six are the HTML report's six, with one name change worth
stating because it is easy to misread: the HTML section built from `compute_relevance` is titled
"Engagement funnel" and lands under the `engagement` key, while the React `relevance` section comes
from `build_measurement_dashboard` and has no HTML counterpart. The HTML report is a strict subset.
`frontend/README.md:30` already says so: the diagnostic sections are listed as ones
"`analysis_dashboard_report.py`'s HTML report does not render".

Section by section, what each HTML renderer reads against what the exporter publishes:

| HTML section | Reads | Exporter key | Published |
|---|---|---|---|
| Engagement funnel | `headline`, `funnel`, `by_query.head(10)`, `by_genre.head(10)` | `engagement` | same, plus `ctr` and `cvr` |
| Keyword gap | `headline`, `by_keyword.head(10)`, `tops[l1,l2,l3].head(10)` | `keyword` | `head(50)`, `head(50)`, tops `head(10)` |
| Query intent | `headline`, `top_queries.head(10)`, all of `by_length` | `query` | identical |
| Recall | `headline`, `rows` | `recall` | identical |
| Ranking | `headline`, `rows` | `ranking` | identical |
| Off-policy evaluation | `headline`, `rows`, `calibration` | `ope` | identical |

Every field the HTML renders is published, at equal or greater row counts -- `keyword` carries fifty
rows where the HTML showed ten.

`run-movie-category-sim.sh` runs both on every run -- the HTML report at lines 255-258, the JSON
exporter at 261-268 -- and then its closing banner advertises the smaller artifact:
`dashboard at $SIM_ROOT/report-dashboard/index.html`.

The compute half is not duplicated, and that is worth stating because it bounds the change.
`compute_ope` delegates to `ope_eval_report.fit_reward_model` / `evaluate` /
`bootstrap_intervals` rather than reimplementing off-policy evaluation; `compute_recall` and
`compute_ranking` reuse `recall_eval_report` and `ranking_eval_report`. The duplication is entirely
in rendering.

Delete the renderer. `analysis_dashboard_report.py` becomes what its only consumer already treats it
as -- a compute library -- and the React dashboard becomes the single surface.

## What this does and does not claim

It claims the six sections the HTML report renders are all present in the React dashboard, so no
measurement or diagnostic is lost. It claims every symbol deleted is referenced only from inside the
deleted range, verified symbol by symbol rather than by reading the file top to bottom.

It does not claim the change is free. The HTML report's one property the React dashboard cannot match
is that it needs no Node: a single file, openable from disk. There is no static export configured
(`frontend/package.json` has `next dev`, `next build`, `next start`, and no `output: "export"`), so
after this change the only way to *see* the dashboard is `npm run dev` or `npm run build && npm run
start`. The root README currently lists Node as "(frontend dashboard, optional)". That claim stops
being true, and this change corrects it rather than leaving it to drift. Python alone still produces
`dashboard.json`; only viewing needs Node.

It does not claim the OPE section is correct, and does not touch it. The committed snapshot's fifteen
policies are all serving-side score fields captured before the retrieval service left this repository
-- `model:banditScore`, `model:deepLearningScore`, `model:qValue` and so on -- and none of the
post-training arms appear. `ope_eval_report.policy_names` discovers policies from the events, so the
section will show whatever the replay buffer holds. Making it reflect post-training is separate work
and is not in this change.

It makes no claim about `frontend/data/dashboard.json`, which is not regenerated. That snapshot is not
byte-reproducible -- two freshness age fields shift on every regeneration -- and nothing in this
change alters what the exporter writes.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Four commits, reviewable independently and in this order: the deletion with its tests, the
  simulation, the documentation, the guard. Documentation comes after the simulation because it
  describes the banner the simulation change produces.
- `frontend/` is not touched apart from `frontend/README.md`. No `.jsx`, no `.mjs`, no
  `package.json`, no `next.config.js`, and `frontend/data/dashboard.json` is not regenerated.
- `build_measurement_dashboard`, `measurement_config`, `MEASUREMENT_SCHEMA_VERSION` and every
  `compute_*` function keep their exact names and signatures. `export_dashboard_json.py` imports them
  and is not edited.
- The seven measurement sections and `frontend/validate_measurements.mjs` are untouched. The schema
  stays `"2.0"` on both sides.
- `import os` stays (used at line 57). `import argparse` goes: after `main` is deleted nothing uses
  it. `pandas`, `numpy` and `redis` are imported lazily inside functions, so the top-level import
  block is only those two lines.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.

## Implementation

### Commit 1 -- the renderer and its tests

Delete lines 494-742 of `analysis_dashboard_report.py`, from `def _esc` through the `__main__`
guard, and `import argparse` from the top. Rewrite the module docstring: it currently says
"Consolidated analysis dashboard -- keyword / query / relevance / recall / ranking as one HTML page"
and "writes a single self-contained index.html", both of which describe the deleted half. The new
docstring says what the module is -- the compute layer behind `frontend/data/dashboard.json` -- and
names `export_dashboard_json.py` as its consumer.

Delete the five tests in `integration-tests/python_modeling/test_analysis_dashboard.py` that exercise
the renderer: `test_renderers_emit_svg_and_tables`, `test_render_html_uses_modern_product_analytics_structure`,
`test_render_html_embeds_responsive_visual_system`, `test_ope_section_renderer`, and
`test_main_writes_recall_na_and_position_ranking_without_redis`. The remaining 22 tests in that file
exercise compute functions and must pass unchanged, as must all 15 in
`test_dashboard_measurement_contract.py`.

### Commit 2 -- the simulation

Remove the `ANALYSIS DASHBOARD` step: the banner echo, the `REDIS_HOST=... python
services/python-modeling/analysis_dashboard_report.py --input "$OUT_DIR"` invocation, and its
`grep -vE` filter. The `REACT DASHBOARD SNAPSHOT` step directly below is unchanged.

Rewrite the closing banner. It currently names the deleted artifact; it now points at
`frontend/data/dashboard.json` and says `npm run dev` renders it. The CSV half of that line is
unchanged.

No test asserts the deleted step -- `test_movie_category_sim.py` and `test_service_scripts.py` were
both checked and neither mentions `analysis_dashboard_report`, `ANALYSIS DASHBOARD` or
`report-dashboard`. The structural assertions those files do make (the `SERVICE BURST` properties,
`--experiences`, `--live-metrics`) are about other parts of the script and keep passing.

### Commit 3 -- the documentation

Nine sites in four files.

`frontend/README.md`: the description at line 6 that credits the Python HTML report as a source, and
the two sites at 28-30 where the diagnostic sections are introduced by saying the HTML report does
not render them -- a distinction that stops existing.

`recsys-pipeline/README.md`: the sample simulation output at line 828, which quotes the old banner
verbatim; the standalone-report passage at 937-948, which tells a reader to run the deleted script
and states where it writes; and the troubleshooting table row that names the HTML artifact.

`docs/recommendation_architecture/Analysis_Report.md`: the table row at 18 offering "Standalone
self-contained HTML", and the two sites at 191 and 200 describing and invoking the script.

Root `README.md`: line 85, `Node.js 18+ / npm (frontend dashboard, optional)`. Viewing the dashboard
now requires it. The line says so, and says what still does not need it -- the pipeline and the JSON
export.

### Commit 4 -- the guard

Add `integration-tests/python_modeling/test_dashboard_single_renderer.py`, following the conventions
of `test_retrieval_service_extracted.py`: a module docstring stating what was removed and why, and
assertions that name the offending site when they fail.

Two tests. The first imports `analysis_dashboard_report` and asserts it exposes none of the deleted
rendering symbols -- `render_html`, `svg_bar`, `svg_line`, `html_table`, `na_card`, `section`, `main`
-- so a second renderer cannot reappear in the module that used to hold one. The second reads
`scripts/run-movie-category-sim.sh` and asserts it invokes `export_dashboard_json.py` and does not
invoke `analysis_dashboard_report.py`, so the simulation cannot go back to producing two artifacts.

The `_esc` and `_ci` helpers are not named in the first assertion. Both are generic enough that a
future compute function could legitimately want them, and a guard that forbids a two-line string
escaper is a guard that gets deleted rather than obeyed.

## Validation and acceptance

1. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline at this branch point is
   565 passed, 2 skipped. This change deletes five tests and adds two, so the expected result is
   562 passed, 2 skipped, with zero failures. Asserting "unchanged" would be the wrong gate.
2. `python3 -c "import analysis_dashboard_report"` from `services/python-modeling` succeeds -- the
   module still imports with `argparse` gone.
3. `cd frontend && python3 export_dashboard_json.py --help` succeeds, proving the import surface the
   exporter depends on survived.
4. `cd frontend && npm run validate:data` passes against the unregenerated snapshot.
5. `bash -n scripts/run-movie-category-sim.sh` is clean.
6. `grep -rn 'report-dashboard\|render_html\|svg_bar' --include='*.py' --include='*.sh'
   --include='*.md'` over live trees returns only the new guard test.
7. `wc -l services/python-modeling/analysis_dashboard_report.py` reports 493 or fewer.
8. `git diff --check` clean.

## Limits

The guard pins symbol names, not behaviour. Nothing stops someone adding `def emit_html()` to a new
module and wiring it into the simulation; the second test catches only the specific case of
`run-movie-category-sim.sh` calling `analysis_dashboard_report.py` again. A general "one renderer"
property is not expressible as a test, because "renderer" is not a thing the code declares.

Deleting the HTML report is irreversible in the sense that restoring it means rewriting 249 lines of
SVG and table generation, not reverting a flag. That is the intended trade: it rendered six of
thirteen sections, and a reader who opened it got a partial view of the run while believing they had
the dashboard -- which the simulation's own closing banner encouraged.

The change removes the only Node-free way to view results, and the honest position is that this
repository now needs npm to show anything visual. The alternatives were considered and rejected: a
static export preserves the property by adding build machinery to replace the machinery being
removed, and a terminal summary is new code in a change whose purpose is deletion.

Nothing here addresses the OPE section's contents. It still grades fifteen score fields belonging to
a service that no longer lives here and shows no post-training arm, and after this change it does so
in one surface instead of two. That is a narrower problem than it was, and still a problem.
