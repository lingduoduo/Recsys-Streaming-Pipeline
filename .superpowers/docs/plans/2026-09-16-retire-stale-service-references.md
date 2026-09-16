# Retire Stale Service References Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the five live references that still describe the retrieval service as part of this repository, none of which the excision's grep-based guard can detect.

**Architecture:** Five independent single-line documentation edits plus one shell-script banner. No logic, test, workflow or dependency change. The Python suite is the gate, because several tests assert README content.

**Tech Stack:** Markdown, Bash, Python 3.12 with pytest.

**Spec:** [Retire stale service references design](../specs/2026-09-16-retire-stale-service-references-design.md)

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Documentation and one shell-script `echo` only. No test, source, workflow or dependency file changes.
- The heading `## Retrieval Service Configuration` in `recsys-pipeline/README.md` keeps its exact text: `docs/recommendation_flows/3_Cold_Start.md:45` and `6_Predicting_Scoring.md:75` link to its anchor.
- The `SERVICE BURST` block of `run-movie-category-sim.sh` is not touched. Two existing tests split the script on that marker and assert the block contains `|| true` or `continue` and no `set -e`.
- Behavioral statements that remain true stay as written — in particular `7_Shuffling.md`'s claim that `TopKScoreSelector` does not read the randomization property.
- Historical records under `.superpowers/docs/**`, `docs/superpowers/**` and `.planning/**` are not rewritten; `.py`/`.scala` provenance comments are left alone.
- The Python suite must remain at **558 passed, 1 skipped**.

## Environment

All pytest commands run from `recsys-pipeline`, which holds the `pytest.ini` whose `testpaths = integration-tests` makes a bare `pytest` collect the right tree:

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q
```

Branch: `docs/retire-stale-service-references`, already created off `master` at `3f6e972`.

## File Structure

- Modify: `README.md:57` — one bullet.
- Modify: `recsys-pipeline/README.md:109` — one prerequisites line.
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh:246` — one `echo`.
- Modify: `recsys-pipeline/docs/recommendation_architecture/API.md:9` — one link.
- Modify: `recsys-pipeline/docs/recommendation_flows/7_Shuffling.md:15` — one clause.

No file is created or deleted.

---

### Task 1: Correct all five stale references

**Files:** the five listed above.

**Interfaces:**
- Consumes: nothing.
- Produces: nothing later tasks depend on. Task 2 only delivers.

These five are batched as one task deliberately: they are the same shape (a single stale claim per file), they share one cause and one acceptance gate, and a reviewer would judge them as a unit.

- [ ] **Step 1: Record the baseline**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
```

Expected: `558 passed, 1 skipped`. If it differs, stop — every later assertion is relative to this.

- [ ] **Step 2: Requalify the root README's Maven prerequisite**

`README.md:57`, replace exactly:

```
- Maven 3.8+ (retrieval service)
```

with:

```
- Maven 3.8+ (only to build the separate retrieval service, not this repository)
```

- [ ] **Step 3: Move Maven out of the pipeline README's prerequisites**

`recsys-pipeline/README.md:109`, replace exactly:

```
**Prerequisites:** Java 17, Apache Spark 3.5.x (Scala 2.12), sbt, Maven 3.8+, Docker Compose, Python 3.
```

with:

```
**Prerequisites:** Java 17, Apache Spark 3.5.x (Scala 2.12), sbt, Docker Compose, Python 3. Step 2 additionally needs Maven 3.8+, which belongs to the separate retrieval-service repository rather than to this checkout.
```

Nothing here invokes Maven — PR #242 deleted the only `pom.xml` — but Step 2 tells the reader to clone and run a Spring Boot/Maven project, so the tool is named where it is actually needed.

- [ ] **Step 4: Make the MDP banner agree with the comment beneath it**

`recsys-pipeline/scripts/run-movie-category-sim.sh:246`, replace exactly:

```bash
echo "==> MDP POLICY EVALUATION (uniform vs greedy over the generated ratings)"
```

with:

```bash
echo "==> MDP POLICY EVALUATION (not measured — evaluator lives in Recsys-Backend-Service)"
```

The three lines beneath it already explain that the card is always unmeasured from a pipeline-only checkout; the banner now says the same thing instead of announcing an evaluation that cannot run. Do not touch `MDP_CSV` on the next line — a downstream call passes `--mdp-csv` unconditionally.

- [ ] **Step 5: Fix the API.md link's path and anchor**

`recsys-pipeline/docs/recommendation_architecture/API.md:9`, replace exactly:

```
[retrieval-service workflow](../../../README.md#3-experiment-pipeline--retrieval-service-8080)
```

with:

```
[retrieval-service workflow](../../README.md#optional-reference-experiment-pipeline--retrieval-service-8080)
```

Two defects in one line. `../../../` resolved to the repository root instead of `recsys-pipeline/` — a bug that predates the extraction — and the anchor named a section that no longer exists. The new anchor is the GitHub slug of `## Optional reference: experiment pipeline — retrieval service :8080` at `recsys-pipeline/README.md:978`. The double hyphen is correct and not a typo: GitHub's slugger drops the em dash but converts each surrounding space to its own hyphen. The original link had `pipeline--retrieval` for the same reason.

- [ ] **Step 6: Qualify the application.yml reference**

`recsys-pipeline/docs/recommendation_flows/7_Shuffling.md:15`, replace exactly:

```
(`RECSYS_RANDOMIZATION_POOL`, default `5`) is declared in `application.yml`. In the current serving
```

with:

```
(`RECSYS_RANDOMIZATION_POOL`, default `5`) is declared in the retrieval service's own
`application.yml`, now in
[lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service). In the current serving
```

This matches the qualification `recsys-pipeline/README.md:334` already uses for the same file. The rest of the sentence — that `TopKScoreSelector` does not read the property — is still true and must not change.

- [ ] **Step 7: Verify all five strings are gone**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
grep -rn 'Maven 3.8+ (retrieval service)' README.md; \
grep -rn 'sbt, Maven 3.8+' recsys-pipeline/README.md; \
grep -rn 'MDP POLICY EVALUATION (uniform vs greedy' recsys-pipeline/scripts/run-movie-category-sim.sh; \
grep -rn '\.\./\.\./\.\./README.md#3-experiment' recsys-pipeline/docs/recommendation_architecture/API.md; \
grep -rn 'is declared in `application.yml`' recsys-pipeline/docs/recommendation_flows/7_Shuffling.md; \
echo "--- above should be empty ---"
```

Expected: no matches before the marker line.

- [ ] **Step 8: Verify the corrected link resolves**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/docs/recommendation_architecture
ls ../../README.md
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
python3 - <<'PY'
import re
lines = open('recsys-pipeline/README.md', encoding='utf-8').read().split('\n')
def gh_slug(t):
    s = re.sub(r'[^\w\s-]', '', t.lower()).strip()
    return re.sub(r'\s', '-', s)   # NOT \s+ — each space becomes its own hyphen
want = 'optional-reference-experiment-pipeline--retrieval-service-8080'
hits = [i+1 for i, l in enumerate(lines) if l.startswith('#') and gh_slug(l.lstrip('#').strip()) == want]
print("heading lines matching the anchor:", hits)
assert hits, "anchor does not resolve to any heading"
PY
```

Expected: `../../README.md` exists and the anchor matches the heading at line 978. Use `\s`, not `\s+` — a collapsing regex reports a single hyphen and will make you 'fix' a correct link.

- [ ] **Step 9: Verify the shell script and the untouched invariant**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
bash -n recsys-pipeline/scripts/run-movie-category-sim.sh && echo "syntax ok"
git diff -U0 recsys-pipeline/scripts/run-movie-category-sim.sh | grep -c '^[+-]echo'
```

Expected: `syntax ok`, and exactly 2 changed `echo` lines (the one removed and the one added) — proving nothing else in the script moved. The `SERVICE BURST` block must appear nowhere in the diff.

- [ ] **Step 10: Run the full suite — the real gate**

```bash
cd recsys-pipeline && python3 -m pytest -q 2>&1 | tail -2
```

Expected: `558 passed, 1 skipped`, unchanged from Step 1. Several tests assert README content, so a reword can break a test in a file this change never opened. If one fails, fix the wording rather than the test — unless the move genuinely made the assertion false, in which case stop and report it.

- [ ] **Step 11: Confirm nothing out of scope changed**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git diff --name-only
git diff --check
```

Expected: exactly the five files, and no whitespace errors. No `.py`, `.scala`, `.yml`, `pom.xml` or dependency file may appear.

- [ ] **Step 12: Commit**

```bash
git add README.md recsys-pipeline/README.md \
  recsys-pipeline/scripts/run-movie-category-sim.sh \
  recsys-pipeline/docs/recommendation_architecture/API.md \
  recsys-pipeline/docs/recommendation_flows/7_Shuffling.md
git commit -m "docs: retire the five stale retrieval-service references

PR #242 rewrote every document that named the service's path, but these
five describe it without naming it, so the hygiene test's grep could not
see them. Each told a reader something false: two prerequisite lines
demanded Maven for a checkout with no pom.xml, the sim banner announced an
evaluation whose evaluator had left, API.md's link had both a wrong
relative depth and a dead anchor, and 7_Shuffling.md pointed at an
application.yml that is now in another repository.

The Maven lines are requalified rather than deleted: Quick Start Step 2
still tells the reader to clone and run the backend repo, which genuinely
is a Spring Boot/Maven project.

The API.md relative-path bug predates the extraction; it is fixed here
because the same line needed its anchor corrected anyway."
```

---

### Task 2: Open the pull request

**Files:** none.

**Interfaces:**
- Consumes: Task 1's commit.

- [ ] **Step 1: Commit the spec and plan**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git add .superpowers/docs/specs/2026-09-16-retire-stale-service-references-design.md \
  .superpowers/docs/plans/2026-09-16-retire-stale-service-references.md
git commit -m "docs: specify and plan retiring the stale service references"
```

- [ ] **Step 2: Push and open the PR against master**

```bash
git push -u origin docs/retire-stale-service-references
gh pr create --base master \
  --title "Retire the five stale retrieval-service references" \
  --body "<see spec; include the baseline and post-change test counts>"
```

- [ ] **Step 3: Confirm CI**

```bash
gh pr checks --watch
```

Expected: `python` and `scala` both pass. The `scala` job takes 2-4 minutes on CI and this change touches no Scala file, so its result must equal master's.

---

## Self-Review

**Spec coverage.** The spec names five references and one delivery step. Steps 2 and 3 cover the two Maven prerequisites, Step 4 the MDP banner, Step 5 the API.md path and anchor, Step 6 the `7_Shuffling.md` qualification. Spec acceptance criterion 1 → Step 7; criterion 2 → Step 8; criterion 3 → Step 9; criterion 4 → Steps 1 and 10; criterion 5 → unchanged by this work and re-asserted by Step 11's scope check; criterion 6 → Step 11.

**Placeholder scan.** No "TBD" or "similar to Task N". Every edit carries its exact before and after text. The one gap is Task 2 Step 2's PR body, which is deliberately left to the author because it summarises results that do not exist until Task 1 runs; the spec and the commit message supply its content.

**Type consistency.** No code interfaces exist in this plan. The two textual values that must agree across steps are the anchor `optional-reference-experiment-pipeline--retrieval-service-8080`, which appears identically in Step 5's replacement and Step 8's assertion, and the count `558 passed, 1 skipped`, identical in Steps 1 and 10.

**A trap worth restating.** Step 8's anchor check must use `re.sub(r'\s', '-', …)`, not `\s+`. The collapsing form derives a single hyphen for the em-dash heading, contradicts the correct link, and would lead an implementer to "fix" Step 5 into a broken state. This exact mistake was made once while writing the spec and caught only by cross-checking against the original link, which carried the same double hyphen.
