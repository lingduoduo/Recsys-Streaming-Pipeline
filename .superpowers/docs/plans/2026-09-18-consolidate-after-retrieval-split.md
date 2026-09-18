# Consolidate After the Retrieval Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the simulation's retrieval-service routes so they reach the backend's
`/api/v1/retrieval` prefix and report route drift distinctly from service absence, delete the
MDP policy-evaluation chain that no code path in this repository can populate, and move the stray
`docs/superpowers/` spec tree into the versioned `.superpowers/docs/` home.

**Architecture:** Three independent commits in order. The first is the only behavioral change: one
new shell variable, three call sites routed through it, and a two-outcome probe becoming three. The
second is pure deletion across six files plus four test sites, ordered so no step leaves a
reference to a removed symbol. The third is prose only.

**Tech Stack:** Bash, Python 3 (pytest), Next.js/React (JSX), Node (ESM validator).

**Spec:** `.superpowers/docs/specs/2026-09-18-consolidate-after-retrieval-split-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Three commits in this order: contract, orphans, documentation.
- `recsys-pipeline/frontend/data/dashboard.json` is edited by deleting the single `mdp` key. It is
  NOT regenerated — the snapshot is not byte-reproducible, two freshness age fields shift on every
  run. Every other key stays byte-identical.
- The seven measurement sections — relevance, satisfaction, freshness, diversity, fairness, safety,
  latency — are untouched. `frontend/validate_measurements.mjs` needs no change; its `SECTIONS` and
  `DIAGNOSTIC_ROWS` never name `mdp`. Every "seven measurement sections" claim in the docs stays true.
- The `SERVICE BURST` block must keep containing `|| true` or `continue`, and must not contain
  `set -e`. Two existing tests split the script on that marker and assert exactly this.
- `recsys-pipeline/README.md`'s heading `## Retrieval Service Configuration` keeps its exact text;
  `3_Cold_Start.md:45` and `6_Predicting_Scoring.md:75` link to its anchor.
- `.superpowers/docs/**` and `.planning/**` are historical records and are not rewritten. `.py` and
  `.scala` provenance comments naming Java classes that produced fixtures are left alone.
- Baseline before any change: `python3 -m pytest -q` from `recsys-pipeline` gives
  **558 passed, 1 skipped**. Expected after all three tasks: **559 passed, 1 skipped** (Task 1 adds
  two tests, Task 2 removes one).
- All pytest commands run from the `recsys-pipeline` directory. All npm commands run from
  `recsys-pipeline/frontend`.

---

### Task 1: Route the simulation at the backend's versioned retrieval prefix

The backend serves the three endpoints the sim uses from
`com.recsys.api.rest.retrieval.RetrievalRecommendationController`, annotated
`@RequestMapping("/api/v1/retrieval")`. The sim calls them at bare paths, so the gate 404s and the
sim reports absence. `HealthController` is mapped at `/health`, so `/health/live` is a liveness
probe that survives retrieval-route reorganisation and lets the sim tell "up but moved" from "down".

**Files:**
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh:14-15` (add the variable) and `:203-234` (the SERVICE BURST block)
- Test: `recsys-pipeline/integration-tests/test_service_scripts.py` (add two tests after `test_movie_category_sim_probes_a_running_service_instead_of_building_one`)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: the shell variable `RETRIEVAL_BASE`, defaulting to `/api/v1/retrieval`, overridable from
  the environment. Task 3 documents it in the root README. No Python or JS symbol changes.

- [ ] **Step 1: Write the two failing tests**

Append to `recsys-pipeline/integration-tests/test_service_scripts.py`:

```python
def test_movie_category_sim_routes_every_retrieval_call_through_one_base() -> None:
    """The backend serves these under /api/v1/retrieval; one variable must carry the prefix."""
    script = SIM_SCRIPT.read_text(encoding="utf-8")

    assert 'RETRIEVAL_BASE="${RETRIEVAL_BASE:-/api/v1/retrieval}"' in script
    # all three retrieval calls derive from the same base
    for path in ("/metrics", "/recommend/", "/feedback"):
        assert f'$SERVICE_URL$RETRIEVAL_BASE{path}' in script
    # and none of them is spelled against the bare origin
    for path in ("/metrics", "/recommend/", "/feedback"):
        assert f'"$SERVICE_URL{path}' not in script


def test_movie_category_sim_reports_moved_routes_differently_from_a_missing_service() -> None:
    """A backend that is up with relocated routes must not be reported as absent."""
    script = SIM_SCRIPT.read_text(encoding="utf-8")
    burst = script.split("SERVICE BURST")[1]

    # liveness is probed separately from the retrieval metrics endpoint
    assert '$SERVICE_URL/health/live' in burst
    # the two diagnoses are distinct strings, and drift names the base it tried
    assert "retrieval routes are not at $RETRIEVAL_BASE" in burst
    assert "no service answering at $SERVICE_URL" in burst
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_service_scripts.py -k "one_base or moved_routes" -v`
Expected: both FAIL on the first assertion — `RETRIEVAL_BASE` is not in the script.

- [ ] **Step 3: Add the base variable**

In `recsys-pipeline/scripts/run-movie-category-sim.sh`, after the `SERVICE_URL` line (currently
line 15), insert:

```bash
# The backend serves retrieval under a versioned prefix (RetrievalRecommendationController is
# @RequestMapping("/api/v1/retrieval") in lingduoduo/Recsys-Backend-Service). SERVICE_URL stays the
# origin so a differently-prefixed deployment overrides RETRIEVAL_BASE alone.
RETRIEVAL_BASE="${RETRIEVAL_BASE:-/api/v1/retrieval}"
```

- [ ] **Step 4: Route the three calls and split the gate into three outcomes**

Replace the SERVICE BURST block's comment, `if` line, the two inner `curl` URLs, the closing
`curl` and the `else` branch. The body of the burst loop is otherwise unchanged. The result:

```bash
echo "==> SERVICE BURST (real /metrics latency, freshness, and filter decisions)"
# The retrieval service lives in lingduoduo/Recsys-Backend-Service. The sim measures whatever is
# already listening on $SERVICE_URL$RETRIEVAL_BASE rather than building one from this checkout, and
# neither a missing service nor a moved route may ever fail the run -- both only cost the cards.

if curl -sf "$SERVICE_URL$RETRIEVAL_BASE/metrics" >/dev/null 2>&1; then
  for i in $(seq 1 "$BURST_REQUESTS"); do
    user="user_$(( (i % 10) + 1 ))"
```

the `item=` line becomes:

```bash
    item="$(curl -sf "$SERVICE_URL$RETRIEVAL_BASE/recommend/$user?limit=6" \
```

the feedback POST becomes:

```bash
      curl -sf -X POST "$SERVICE_URL$RETRIEVAL_BASE/feedback" \
```

the capture becomes:

```bash
  curl -sf "$SERVICE_URL$RETRIEVAL_BASE/metrics" > "$LIVE_METRICS" 2>/dev/null || true
  echo "   captured $(wc -c < "$LIVE_METRICS" 2>/dev/null || echo 0) bytes of live metrics"
elif curl -sf "$SERVICE_URL/health/live" >/dev/null 2>&1; then
  # Liveness belongs to HealthController (@RequestMapping("/health")), so it survives a retrieval
  # route reorganisation. Reaching here means the service is up and the prefix moved.
  echo "   service is up at $SERVICE_URL but retrieval routes are not at $RETRIEVAL_BASE"
  echo "   — contract drift; set RETRIEVAL_BASE to the current prefix. Latency stays N/A."
else
  echo "   no service answering at $SERVICE_URL — latency stays N/A"
fi
```

- [ ] **Step 5: Run the new tests, the two pre-existing SERVICE BURST tests, and the syntax check**

Run: `bash -n recsys-pipeline/scripts/run-movie-category-sim.sh`
Expected: no output, exit 0.

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_service_scripts.py -v`
Expected: all PASS, including `test_movie_category_sim_never_fails_on_a_missing_live_service`
(the block still has `|| true` and no `set -e`) and
`test_movie_category_sim_probes_a_running_service_instead_of_building_one` (the
`SERVICE_URL="${SERVICE_URL:-http://localhost:$SERVICE_PORT}"` line is untouched).

- [ ] **Step 6: Run the whole suite**

Run: `cd recsys-pipeline && python3 -m pytest -q`
Expected: **560 passed, 1 skipped** — the 558 baseline plus this task's two new tests.

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/scripts/run-movie-category-sim.sh recsys-pipeline/integration-tests/test_service_scripts.py
git commit -m "fix(sim): reach the backend's versioned retrieval routes and name route drift

The backend serves /metrics, /recommend and /feedback under /api/v1/retrieval
(RetrievalRecommendationController). The sim called all three at the bare origin, so
its gate 404d and it reported 'no service answering' against a healthy backend, leaving
the latency, freshness and filter cards permanently N/A.

Route the three calls through one RETRIEVAL_BASE variable, and probe /health/live when
the retrieval metrics endpoint misses, so a service that is up with moved routes is
reported as drift rather than as absent.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Delete the MDP chain and move the stray spec tree

`MovieLensPolicyEvaluation`, which wrote `mdp_eval.csv`, left with the service. Nothing here can
produce that file, so every consumer of it is unreachable. Delete in dependency order — consumers
before producers — so no intermediate step references a removed symbol.

`docs/superpowers/` is the only content under the root `docs/` directory and holds PR #241's spec
and plan. `.gitignore` designates `.superpowers/docs/` as the versioned home (`/.superpowers/*`
ignored, `!/.superpowers/docs/` negated back in), where twenty other design documents live.

**Files:**
- Modify: `recsys-pipeline/frontend/app/page.jsx:16,50`
- Modify: `recsys-pipeline/frontend/components/sections.jsx:628-657` (delete; `MdpSection` is the last function in the file)
- Modify: `recsys-pipeline/frontend/data/dashboard.json` (delete the `mdp` key only)
- Modify: `recsys-pipeline/frontend/export_dashboard_json.py:74,112,125,133-134,151-156`
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py:288-300,716-725,735-736,762-765`
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh:246-251,257,263`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py:271-284,286-313,333-352`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py:200`
- Modify: `recsys-pipeline/integration-tests/test_service_scripts.py:406-407`
- Modify: `recsys-pipeline/integration-tests/test_retrieval_service_extracted.py` (docstring and `HISTORICAL_PREFIXES`)
- Move: `docs/superpowers/specs/2026-09-16-standalone-retrieval-design.md` → `.superpowers/docs/specs/`
- Move: `docs/superpowers/plans/2026-09-16-standalone-retrieval.md` → `.superpowers/docs/plans/`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `export_dashboard_json.build()` loses its third positional parameter and becomes
  `build(input_dir: str, host: str, port: int, experiences: str | None = None,
  live_metrics: str | None = None, config: dict | None = None) -> dict`. Its only caller is
  `main()` in the same file. `analysis_dashboard_report` loses the module-level names
  `compute_mdp` and `_mdp_section`. Task 3 consumes neither.

- [ ] **Step 1: Delete the tests that assert the MDP machinery, and run to watch them go**

In `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`, delete the whole
of `test_compute_mdp_reads_csv_and_missing_is_none` (lines 271-284). Rename
`test_ope_and_mdp_section_renderers` to `test_ope_section_renderer` and delete from its body the
`mdp = {...}` dict, the `html = dash._mdp_section(mdp)` call and the two assertions that follow it
(the `"MDP policy evaluation" in html` and `"[-0.626, -0.267]" in html and "mdp_eval.csv" in html`
lines). In the end-to-end report test, delete the `mdp_csv = tmp_path / "mdp_eval.csv"` assignment
and its `write_text` call, drop `"--mdp-csv", str(mdp_csv)` from the `subprocess.run` argument
list, and replace the trailing comment and assertion:

```python
    # OPE has no Redis buffer, so its card is N/A.
    assert "Not measured — no replay-buffer events with reward in Redis" in page
```

In `recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py:200`:

```python
    assert set(output) >= {"engagement", "keyword", "query", "recall", "ranking", "ope"}
```

In `recsys-pipeline/integration-tests/test_service_scripts.py`, delete the comment
`# the MDP card still has a path to render "Not measured"` and the assertion
`assert 'MDP_CSV="$SIM_ROOT/mdp_eval.csv"' in script`.

Run: `cd recsys-pipeline && python3 -m pytest integration-tests -q`
Expected: **559 passed, 1 skipped** — one test fewer than Task 1 left, and no failures, because the
production code still exports everything the remaining tests touch.

- [ ] **Step 2: Delete the React card**

In `recsys-pipeline/frontend/app/page.jsx`, remove `  MdpSection,` from the import list and
`        <MdpSection data={data.mdp} />` from the grid.

In `recsys-pipeline/frontend/components/sections.jsx`, delete lines 628-657 — the entire
`export function MdpSection({ data }) { ... }`, which is the last function in the file. Leave the
file ending after the preceding function's closing brace.

- [ ] **Step 3: Delete the `mdp` key from the committed snapshot**

Do NOT regenerate the file. Delete the key in place, preserving the formatting of every other key:

```bash
cd recsys-pipeline/frontend && python3 - <<'PY'
import json, pathlib
p = pathlib.Path("data/dashboard.json")
d = json.loads(p.read_text())
assert "mdp" in d, "mdp key already absent"
del d["mdp"]
p.write_text(json.dumps(d, indent=2) + "\n")
PY
```

Then confirm nothing else moved:

```bash
cd recsys-pipeline && git show HEAD:recsys-pipeline/frontend/data/dashboard.json \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); d.pop("mdp"); print(json.dumps(d, indent=2))' \
  > /tmp/expected-dashboard.json
diff <(python3 -c 'import json;print(json.dumps(json.load(open("frontend/data/dashboard.json")),indent=2))') /tmp/expected-dashboard.json
```

Expected: `diff` prints nothing.

- [ ] **Step 4: Delete the exporter plumbing**

In `recsys-pipeline/frontend/export_dashboard_json.py`:
- change the `build` signature to `def build(input_dir: str, host: str, port: int,` /
  `          experiences: str | None = None, live_metrics: str | None = None,` /
  `          config: dict | None = None) -> dict:`
- delete `    mdp = dash.compute_mdp(mdp_csv)`
- delete the `"mdp": {...} if mdp else None,` entry from the returned dict
- delete the `--mdp-csv` `add_argument` call and its `help` line
- in `main()`, delete the three-line comment beginning `# Match analysis_dashboard_report.py` and
  the `mdp_csv = args.mdp_csv or ...` line, and drop `mdp_csv` from the `build(...)` call so it
  reads `data = build(args.input, host, port, args.experiences, args.live_metrics, {`

- [ ] **Step 5: Delete the HTML report plumbing**

In `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`:
- delete `compute_mdp` in its entirety (lines 288-300)
- delete `_mdp_section` in its entirety (lines 716-725)
- delete the `--mdp-csv` `add_argument` call and its `help` line
- delete the four lines in `main()` that read `mdp_csv = args.mdp_csv or ...`,
  `mdp = compute_mdp(mdp_csv)`, and the `sections.append(_mdp_section(mdp) if mdp` /
  `else na_card(...))` pair

- [ ] **Step 6: Delete the simulation's MDP block**

In `recsys-pipeline/scripts/run-movie-category-sim.sh`, delete the entire MDP stanza — the blank
line, the `echo "==> MDP POLICY EVALUATION ..."` banner, the three-line comment, the
`MDP_CSV="$SIM_ROOT/mdp_eval.csv"` assignment and the `echo "   evaluator now lives ..."` line.
In the analysis-dashboard invocation, drop the `--mdp-csv "$MDP_CSV"` continuation so it reads:

```bash
REDIS_HOST=localhost REDIS_PORT=6379 \
  python services/python-modeling/analysis_dashboard_report.py --input "$OUT_DIR" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"
```

Delete the line `[[ -s "$MDP_CSV" ]] && export_args+=(--mdp-csv "$MDP_CSV")`.

- [ ] **Step 7: Move the stray spec tree and drop its guard exemption**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git mv docs/superpowers/specs/2026-09-16-standalone-retrieval-design.md .superpowers/docs/specs/
git mv docs/superpowers/plans/2026-09-16-standalone-retrieval.md .superpowers/docs/plans/
rmdir docs/superpowers/specs docs/superpowers/plans docs/superpowers docs
```

In `recsys-pipeline/integration-tests/test_retrieval_service_extracted.py`, change:

```python
HISTORICAL_PREFIXES = (".superpowers/", ".planning/")
```

and in the module docstring replace `Dated design records under .superpowers/docs, docs/superpowers
and .planning are history` with `Dated design records under .superpowers/docs and .planning are
history`.

- [ ] **Step 8: Verify every trace is gone**

Run: `bash -n recsys-pipeline/scripts/run-movie-category-sim.sh`
Expected: no output.

Run: `cd recsys-pipeline && grep -rni 'mdp' scripts services frontend integration-tests --include='*.py' --include='*.sh' --include='*.jsx' --include='*.mjs' --include='*.json' | grep -v __pycache__ | grep -v node_modules`
Expected: no output.

Run: `cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && git ls-files docs/ | wc -l && git ls-files .superpowers/docs/ | wc -l`
Expected: `0` then a count two higher than before the move.

- [ ] **Step 9: Run the full suite and the frontend gates**

Run: `cd recsys-pipeline && python3 -m pytest -q`
Expected: **559 passed, 1 skipped**, zero failures.

Run: `cd recsys-pipeline/frontend && npm run validate:data`
Expected: `dashboard.json valid: 7 measurement sections, schema 2.0`.

Run: `cd recsys-pipeline/frontend && npm run build`
Expected: a successful Next.js build with no unresolved-import error for `MdpSection`.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "chore: delete the unreachable MDP chain and unify the spec tree

MovieLensPolicyEvaluation left with the service, so nothing in this checkout can
write mdp_eval.csv. Remove every consumer of it: the sim's MDP_CSV stanza and its
two --mdp-csv arguments, compute_mdp and _mdp_section in the HTML report, the
mdp_csv plumbing in the JSON exporter, MdpSection and its mount, and the stale mdp
block in the committed snapshot. A card no code path can populate reads as a missing
measurement rather than a relocated one.

Also move PR #241's spec and plan out of docs/superpowers/ into .superpowers/docs/,
which .gitignore already designates as the versioned home, and drop the now-dead
docs/superpowers/ exemption from the extraction guard.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: State the two-repository boundary once and sweep the orphaned prose

Task 2 leaves MDP text in three documents describing a flag that no longer exists. Separately, nine
live files each re-explain where the retrieval service lives; the root `README.md` — 62 lines, an
orientation page — is the right single home for that, and Task 1 introduced `RETRIEVAL_BASE`, which
is currently documented nowhere.

**Files:**
- Modify: `README.md` (the boundary section; add `RETRIEVAL_BASE`)
- Modify: `recsys-pipeline/README.md` (MDP prose at the `--mdp-csv` example, the N/A sentences, the two-evaluator paragraph, and the diagnostic list)
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md:193,204,207-208`
- Modify: `recsys-pipeline/frontend/README.md:6,47,50-51,56,61,71`

**Interfaces:**
- Consumes: `RETRIEVAL_BASE` and its `/api/v1/retrieval` default from Task 1; the absence of
  `--mdp-csv` from both entry points from Task 2.
- Produces: nothing code-level.

- [ ] **Step 1: Sweep the orphaned MDP prose**

In `recsys-pipeline/README.md`: remove `  --mdp-csv "$IN/../mdp_eval.csv"` from the invocation
example; in the sentence at the "off-policy and MDP cards render N/A" line, drop the MDP clause and
keep the off-policy one; drop the MDP paragraph from the two-evaluator discussion, leaving the
reward-model evaluator and the sentence that neither is a claim of A/B lift; and remove `MDP` from
the `engagement/keyword/query/recall/ranking/OPE/MDP` diagnostic list.

In `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md`: remove the
`--mdp-csv "$IN/../mdp_eval.csv"` example line, the `when --mdp-csv exists` clause and the
`Missing ... a missing MDP CSV` phrase from the N/A sentence, and the two sentences about the
default MDP input path.

In `recsys-pipeline/frontend/README.md`: remove `MDP` from the diagnostics list on line 6 and from
the diagnostic-sections parenthetical near line 71, and delete the whole `--mdp-csv` block — the
example line, the two sentences about its default, and the paragraph beginning
`run-movie-category-sim.sh no longer produces this file itself`.

- [ ] **Step 2: Verify no MDP prose survives**

Run: `cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && grep -rni 'mdp' --include='*.md' . | grep -v '^\./\.superpowers/' | grep -v '^\./\.planning/' | grep -v node_modules`
Expected: no output.

- [ ] **Step 3: Write the canonical boundary section in the root README**

Add to `README.md` a section — heading `## Repository boundary` — that states in one place: this
repository holds the streaming pipeline (Spark jobs, Python modeling, simulations, dashboard); the
retrieval service holds the serving path and lives in `lingduoduo/Recsys-Backend-Service`; the
service exposes its retrieval endpoints under `/api/v1/retrieval`, which the simulation reaches via
`SERVICE_URL` (origin) and `RETRIEVAL_BASE` (prefix, default `/api/v1/retrieval`), either of which
can be overridden from the environment; and that Maven is a prerequisite of that repository, not of
this checkout.

- [ ] **Step 4: Point the restating documents at it**

In `recsys-pipeline/README.md`, `recsys-pipeline/docs/recommendation_architecture/API.md`,
`recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`,
`recsys-pipeline/docs/recommendation_flows/7_Shuffling.md` and `recsys-pipeline/frontend/README.md`,
replace each in-place restatement of where the service lives with a link to the new section, as
`[repository boundary](../README.md#repository-boundary)` at the correct relative depth for each
file. Count the directory levels per file; `API.md` sits two levels below `recsys-pipeline/`, so it
needs `../../../README.md#repository-boundary` to reach the repository root.

**Exception:** `recsys-pipeline/README.md` Quick Start Step 2 keeps its concrete clone-and-run
instructions verbatim. A reader following numbered steps must not have to leave the page.

- [ ] **Step 5: Verify the links resolve and the guarded anchors survive**

Run: `cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && python3 - <<'PY'
import pathlib, re
root = pathlib.Path(".")
bad = []
for md in root.rglob("*.md"):
    if any(p in md.parts for p in (".superpowers", ".planning", "node_modules", ".git")):
        continue
    for target, anchor in re.findall(r"\]\((\.\./[^)#]*\.md)#([a-z0-9-]+)\)", md.read_text()):
        dest = (md.parent / target).resolve()
        if not dest.exists():
            bad.append(f"{md}: missing file {target}")
            continue
        heads = [re.sub(r"[^a-z0-9 -]", "", h.lower()).replace(" ", "-")
                 for h in re.findall(r"^#+ (.+)$", dest.read_text(), re.M)]
        if anchor not in heads:
            bad.append(f"{md}: missing anchor #{anchor} in {target}")
print("\n".join(bad) or "all relative anchors resolve")
PY`
Expected: `all relative anchors resolve`.

Run: `cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && grep -n '^## Retrieval Service Configuration$' recsys-pipeline/README.md`
Expected: one match — the heading two flow documents link to is unchanged.

- [ ] **Step 6: Run the full suite**

Several tests assert README content, so a reword can break a test in a file this task never opens.

Run: `cd recsys-pipeline && python3 -m pytest -q`
Expected: **559 passed, 1 skipped**.

Run: `cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && git diff --check`
Expected: no output.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "docs: state the repository boundary once and drop the orphaned MDP prose

Nine live files each re-explained where the retrieval service lives. Put that in the
root README, including the /api/v1/retrieval prefix and the SERVICE_URL/RETRIEVAL_BASE
overrides, and link to it from the rest -- except Quick Start Step 2, whose reader is
mid-walkthrough and should not have to leave the page.

Also remove the --mdp-csv documentation the previous commit orphaned.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] `cd recsys-pipeline && python3 -m pytest -q` → **559 passed, 1 skipped**
- [ ] `cd recsys-pipeline/frontend && npm run validate:data && npm run build` → both succeed
- [ ] `bash -n recsys-pipeline/scripts/run-movie-category-sim.sh` → clean
- [ ] `git ls-files docs/` → empty
- [ ] `grep -rni 'mdp' --include='*.py' --include='*.sh' --include='*.jsx' --include='*.md' --include='*.json' .` outside `.superpowers/`, `.planning/`, `node_modules/` and `__pycache__/` → no output
- [ ] `grep -rn 'java-retrieval-service' .` → only `.superpowers/`, `.planning/`, and `.py`/`.scala` provenance comments
- [ ] `git diff --check` → clean
- [ ] Three commits on the branch, in order: contract, orphans, documentation
