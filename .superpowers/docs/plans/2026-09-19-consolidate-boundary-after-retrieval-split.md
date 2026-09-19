# Consolidate the repository boundary after the retrieval split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct every broken link, stale endpoint path and false ownership claim the retrieval-service split left in this repository's live documentation, and add the guard that makes all three regressions fail CI instead of shipping.

**Architecture:** Documentation-only, plus one new test file. Each task writes a guard assertion first, watches it fail against the defect the audit found, then fixes the documentation until it passes. The guard lives in `integration-tests/test_doc_links.py` and grows one test per defect class: unresolvable links, unprefixed endpoints, unmarked flow documents.

**Tech Stack:** Python 3 / pytest (the repository's `pytest.ini` sets `testpaths = integration-tests`). Markdown. No Scala, no JavaScript, no shell.

**Spec:** `.superpowers/docs/specs/2026-09-19-consolidate-boundary-after-retrieval-split-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Documentation and one new test file only. No `.scala`, no `.py` under `services/`, no `.jsx`, no `.json`, no workflow, no script. The frontend is not rebuilt because nothing it reads changes.
- The two aligned ASCII blocks in `recsys-pipeline/README.md` (Experiment Pipeline, lines 76-92; architecture, lines 696-700) keep their arrow alignment. They get a prefix note above the fence; the paths inside them are not expanded.
- These headings keep their exact text, because inbound links depend on their anchors: `## Retrieval Service Configuration` and `## Canonical finite local workflow` in `recsys-pipeline/README.md`, `## Repository boundary` in the root `README.md`, and all ten `## GET|POST /api/v1/retrieval/...` headings in `docs/recommendation_architecture/API.md`.
- `## Repository boundary` in the root README stays the canonical statement. The flow documents link to it; none of them restates it.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten. `.py`/`.scala` provenance comments naming Java classes are left alone.
- Measured baseline at this branch point: **561 passed, 2 skipped**. This plan adds four tests and deletes none, so the final expected result is **565 passed, 2 skipped**.

## Deviation from the spec

The spec ordered three commits as corrections, boundary notes, guard — guard last "because it must pass against the first two". This plan puts each guard assertion *before* the fix it covers, so every correction is proved by a test that failed first. That is what `.claude/CLAUDE.md` §4 requires ("Fix the bug" → "Write a test that reproduces it, then make it pass"), and it is what the spec's own acceptance items 1 and 2 measure. The commit count rises from three to four: the Maven deletion becomes its own commit because it is the only change to the root README's Requirements list and shares no test with anything else.

## File Structure

| File | Responsibility |
|---|---|
| Create: `recsys-pipeline/integration-tests/test_doc_links.py` | All three guards. Scans live markdown only; follows the exclusion conventions of `test_retrieval_service_extracted.py`. |
| Modify: `recsys-pipeline/README.md` | Two API anchors (238, 243); one prose path (231); prefix notes above two ASCII fences. |
| Modify: `recsys-pipeline/docs/recommendation_flows/1..9_*.md` | Startup-link retarget (all nine); prose prefixes (6, 8, 9); boundary block (all nine); the false writer claim (1). |
| Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md` | One link depth fix (473). |
| Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md` | One prose prefix (140). |
| Modify: `README.md` (root) | Delete the Maven line from Requirements (81). |

---

### Task 1: The link-resolution guard, and the twelve anchors it catches

**Files:**
- Create: `recsys-pipeline/integration-tests/test_doc_links.py`
- Modify: `recsys-pipeline/README.md:238,243`
- Modify: `recsys-pipeline/docs/recommendation_flows/1_Query_Hydration.md:7` through `8_Store_Context.md:7` (eight files, identical line)
- Modify: `recsys-pipeline/docs/recommendation_flows/9_Track_Metrics.md:8`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md:473`

**Interfaces:**
- Produces: `live_markdown()` yielding `(relative_path, text)` pairs, `slugs(path)` returning the GitHub-style anchor set of a markdown file, and the module constants `GIT_ROOT`, `HISTORICAL_PREFIXES`, `SKIP_DIRS`. Tasks 2 and 3 add tests to this same file and reuse all of them.

- [ ] **Step 1: Write the failing test**

Create `recsys-pipeline/integration-tests/test_doc_links.py`:

```python
"""Every relative link in live documentation resolves, every documented retrieval
endpoint carries the prefix the backend serves it at, and every flow document says
which repository its stage runs in.

PR #244 rewrote the ten headings in docs/recommendation_architecture/API.md to carry
/api/v1/retrieval and left behind the two anchors that pointed at them. The same
commit's message claimed it had "corrected every stale endpoint path" while twenty
bare paths survived it. Both regressions are silent: nothing compiles a markdown
link, and nothing calls a path written in prose. These tests are the check that was
missing.

Two categories are deliberately not scanned. Dated design records under
.superpowers/docs and .planning are history -- they describe the repository as it was
and must not be rewritten. And http/mailto targets are not fetched: that would make
the suite depend on the network and on another repository's default branch, so a link
to a renamed file in Recsys-Backend-Service still breaks here silently.
"""

import re
from pathlib import Path

GIT_ROOT = Path(__file__).resolve().parents[2]

HISTORICAL_PREFIXES = (".superpowers/", ".planning/")
SKIP_DIRS = {
    ".git", "target", "node_modules", ".next", "__pycache__",
    ".worktrees", ".pytest_cache", ".venv", "venv",
}

LINK = re.compile(r"\]\(([^)\s]+)\)")
HEADING = re.compile(r"^#+\s+(.*)")


def live_markdown():
    """Every markdown file describing the repository as it is now."""
    for path in sorted(GIT_ROOT.rglob("*.md")):
        relative = path.relative_to(GIT_ROOT)
        if relative.as_posix().startswith(HISTORICAL_PREFIXES):
            continue
        if set(relative.parts) & SKIP_DIRS:
            continue
        yield relative, path.read_text(encoding="utf-8")


def slugs(path):
    """The anchors GitHub generates for a markdown file's headings.

    Lowercase, drop backticks/asterisks/underscores/brackets/parentheses, drop every
    remaining character outside [a-z0-9 -], then spaces to hyphens. A `#` comment
    inside a fenced block reads as a heading here, which can only add an anchor that
    nothing links to -- it cannot mask a broken link.
    """
    found = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = HEADING.match(line)
        if not match:
            continue
        text = match.group(1).strip().lower()
        text = re.sub(r"[`*_\[\]()]", "", text)
        text = re.sub(r"[^a-z0-9 -]", "", text)
        found.add(text.replace(" ", "-"))
    return found


def test_every_relative_documentation_link_resolves():
    broken = []
    for relative, text in live_markdown():
        for match in LINK.finditer(text):
            target = match.group(1)
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            path_part, _, fragment = target.partition("#")
            source = GIT_ROOT / relative
            resolved = (source.parent / path_part).resolve() if path_part else source
            line = text[: match.start()].count("\n") + 1
            if not resolved.exists():
                broken.append(f"{relative}:{line} -> {target} (no such file)")
                continue
            if fragment and resolved.suffix == ".md" and fragment.lower() not in slugs(resolved):
                broken.append(f"{relative}:{line} -> {target} (no such heading)")
    assert not broken, "broken documentation links:\n" + "\n".join(broken)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_doc_links.py -q`
Expected: FAIL, listing exactly 12 broken links — `recsys-pipeline/README.md:238` and `:243` (no such heading), the eight `#recsys-pipeline` links in flow docs 1-8, `9_Track_Metrics.md:8`, and `Data_Pipeline.md:473`.

- [ ] **Step 3: Retarget the two API anchors**

The headings these describe were renamed by #244; the anchors move to the headings, not the reverse.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("README.md")
t = p.read_text()
assert t.count("API.md#get-usersuserprofile") == 1
assert t.count("API.md#get-actuatorprofile-audit") == 1
t = t.replace("API.md#get-usersuserprofile", "API.md#get-apiv1retrievalusersuserprofile")
t = t.replace("API.md#get-actuatorprofile-audit", "API.md#get-apiv1retrievalprofile-audit")
p.write_text(t)
PY
```

- [ ] **Step 4: Retarget the ten startup links**

All ten describe "the complete local startup sequence", which is `## Canonical finite local workflow` at `recsys-pipeline/README.md:744` — two directories up from the flow and architecture folders, not three. The link text stops saying "root" because the target is not the root README.

```bash
cd recsys-pipeline/docs
python3 - <<'PY'
from pathlib import Path
new = "[canonical finite local workflow](../../README.md#canonical-finite-local-workflow)"
changed = 0
for path in sorted(Path("recommendation_flows").glob("*.md")):
    t = path.read_text()
    for old in (
        "[root quick start](../../../README.md#recsys-pipeline)",
        "[root quick start](../../../README.md#canonical-finite-local-workflow)",
    ):
        if old in t:
            t = t.replace(old, new)
            changed += 1
    path.write_text(t)
p = Path("recommendation_architecture/Data_Pipeline.md")
t = p.read_text()
old = "[finite local workflow](../../../README.md#canonical-finite-local-workflow)"
assert t.count(old) == 1
p.write_text(t.replace(old, "[finite local workflow](../../README.md#canonical-finite-local-workflow)"))
print("flow links retargeted:", changed)
PY
```

Expected output: `flow links retargeted: 9`

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_doc_links.py -q`
Expected: PASS (1 passed)

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/integration-tests/test_doc_links.py recsys-pipeline/README.md \
        recsys-pipeline/docs/recommendation_flows recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
git commit -m "$(cat <<'MSG'
test: resolve every documentation link, and fix the twelve that did not

Two anchors pointed at API.md headings that #244 renamed in the same commit
that updated the prose around them. The other ten pointed at #recsys-pipeline
and #canonical-finite-local-workflow on the root README; the first has never
existed and the second is one directory down. All ten described the same
target, which does exist.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: The prefix guard, and the twenty stale paths it catches

**Files:**
- Modify: `recsys-pipeline/integration-tests/test_doc_links.py` (append)
- Modify: `recsys-pipeline/docs/recommendation_flows/6_Predicting_Scoring.md:17,26,28`
- Modify: `recsys-pipeline/docs/recommendation_flows/8_Store_Context.md:16,25`
- Modify: `recsys-pipeline/docs/recommendation_flows/9_Track_Metrics.md:11,12,21,28,29,32,67,71`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md:140`
- Modify: `recsys-pipeline/README.md:231` and the fences above lines 78 and 697

**Interfaces:**
- Consumes: `live_markdown()` and `GIT_ROOT` from Task 1.
- Produces: the module constants `PREFIX` (`"/api/v1/retrieval"`) and `PREFIX_NOTE` (`"Paths are relative to the service prefix"`). Task 3 does not use them.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/test_doc_links.py`:

```python
PREFIX = "/api/v1/retrieval"

# The two aligned ASCII blocks in recsys-pipeline/README.md carry this note above the
# fence instead of the prefix inline: expanding the paths inside them would shift
# every arrow on the line out of alignment.
PREFIX_NOTE = "Paths are relative to the service prefix"

ENDPOINTS = "recommend|feedback|metrics|predict|embedding|users"
BARE_ENDPOINT = re.compile(rf"(?:(?:GET|POST)\s+|localhost:8080)/(?:{ENDPOINTS})\b")


def noted_fence_lines(text):
    """Line numbers inside a fenced block whose note above the fence declares the prefix."""
    lines = text.splitlines()
    noted, inside, declared = set(), False, False
    for index, line in enumerate(lines):
        if line.startswith("```"):
            if inside:
                inside = False
            else:
                inside = True
                declared = any(PREFIX_NOTE in before for before in lines[max(0, index - 3): index])
            continue
        if inside and declared:
            noted.add(index + 1)
    return noted


def test_documented_retrieval_endpoints_carry_the_service_prefix():
    bare = []
    for relative, text in live_markdown():
        noted = noted_fence_lines(text)
        for match in BARE_ENDPOINT.finditer(text):
            line = text[: match.start()].count("\n") + 1
            if line in noted:
                continue
            bare.append(f"{relative}:{line} -> {match.group(0)}")
    assert not bare, (
        f"retrieval endpoints documented without the {PREFIX} prefix:\n" + "\n".join(bare)
    )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_doc_links.py -q`
Expected: FAIL, listing 20 bare paths — 4 in `6_Predicting_Scoring.md`, 2 in `8_Store_Context.md`, 8 in `9_Track_Metrics.md`, 1 in `Analysis_Report.md`, and 5 in `recsys-pipeline/README.md` (lines 79, 83, 89, 231, 698).

- [ ] **Step 3: Prefix the sixteen prose paths**

Every prose occurrence is inside backticks; the four inside the ASCII fences are not. A lookbehind on the backtick is what keeps this pass out of the diagrams.

```bash
cd recsys-pipeline
python3 - <<'PY'
import re
from pathlib import Path
pattern = re.compile(r"(?<=`)(GET|POST) /(recommend|feedback|metrics|predict|embedding|users)")
targets = [
    "docs/recommendation_flows/6_Predicting_Scoring.md",
    "docs/recommendation_flows/8_Store_Context.md",
    "docs/recommendation_flows/9_Track_Metrics.md",
    "docs/recommendation_architecture/Analysis_Report.md",
    "README.md",
]
total = 0
for name in targets:
    path = Path(name)
    text = path.read_text()
    text, count = pattern.subn(r"\1 /api/v1/retrieval/\2", text)
    path.write_text(text)
    print(f"{name}: {count}")
    total += count
print("total:", total)
PY
```

Expected output: `6_Predicting_Scoring.md: 4`, `8_Store_Context.md: 2`, `9_Track_Metrics.md: 8`, `Analysis_Report.md: 1`, `README.md: 1`, `total: 16`.

Note this also rewrites the heading `## \`GET /metrics\`` at `9_Track_Metrics.md:32` to `## \`GET /api/v1/retrieval/metrics\``. That is intended and safe: no file links to `#get-metrics` (verified by grep), and from this commit forward Task 1's test would catch it if one did.

- [ ] **Step 4: Note the prefix above the two ASCII fences**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
note = "Paths are relative to the service prefix `/api/v1/retrieval`.\n\n"
p = Path("README.md")
lines = p.read_text().splitlines(keepends=True)
# Insert above the two ```text fences that contain bare endpoint paths, bottom-up so
# the first insertion does not shift the second target.
for marker in ("GET /recommend/{user} ──►", "Java Retrieval Service (Spring Boot :8080)"):
    index = next(i for i, line in enumerate(lines) if marker in line)
    fence = max(i for i in range(index) if lines[i].startswith("```"))
    lines.insert(fence, note)
p.write_text("".join(lines))
PY
sed -n '76,82p' README.md
```

Expected: the note appears immediately above the ```` ```text ```` fence, with the diagram's arrows unchanged.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_doc_links.py -q`
Expected: PASS (2 passed)

Then confirm the diagrams did not move:
Run: `git diff --stat recsys-pipeline/README.md` and `git diff recsys-pipeline/README.md | grep -c '^[-+].*──►'`
Expected: `0` — no arrow line is added or removed.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/integration-tests/test_doc_links.py recsys-pipeline/README.md \
        recsys-pipeline/docs/recommendation_flows recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md
git commit -m "$(cat <<'MSG'
test: require the service prefix on documented endpoints, and add the twenty missing

#244's message says it corrected every stale endpoint path. Twenty survived
it, so a reader copying curl localhost:8080/recommend/user_1 out of any of
these files got a 404 from the current backend. Sixteen are prose and take
the prefix inline; four are inside aligned ASCII diagrams and take a note
above the fence, because expanding them would shift every arrow.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: The boundary guard, the false claim, and the nine notes

**Files:**
- Modify: `recsys-pipeline/integration-tests/test_doc_links.py` (append)
- Modify: all nine `recsys-pipeline/docs/recommendation_flows/*.md`

**Interfaces:**
- Consumes: `GIT_ROOT` from Task 1.
- Produces: nothing later tasks use.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/test_doc_links.py`:

```python
BOUNDARY_SERVICE = "Recsys-Backend-Service"
BOUNDARY_ANCHOR = "README.md#repository-boundary"

FLOWS = GIT_ROOT / "recsys-pipeline" / "docs" / "recommendation_flows"


def test_every_flow_document_marks_the_repository_boundary():
    """Eight of the nine stages run entirely in the other repository.

    A reader who opens one of these mid-narrative must be told that before they
    read a class name, because this checkout has no Java to check it against.
    """
    unmarked = []
    for path in sorted(FLOWS.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        if BOUNDARY_SERVICE not in text or BOUNDARY_ANCHOR not in text:
            unmarked.append(path.name)
    assert not unmarked, "flow documents that do not mark the boundary: " + ", ".join(unmarked)


def test_no_document_claims_an_in_repo_serving_side_effect_writer():
    """served_history and impressions left with the service.

    Neither key has a writer, a reader or a test in this checkout; they appear
    only in documentation. Saying otherwise claims a capability this repository
    does not have.
    """
    offenders = [
        f"{relative}" for relative, text in live_markdown() if "in-repo serving side-effect" in text
    ]
    assert not offenders, "documents claiming an in-repo serving writer: " + ", ".join(offenders)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_doc_links.py -q`
Expected: FAIL twice — the boundary test naming the eight documents other than `7_Shuffling.md`, and the writer test naming `recsys-pipeline/docs/recommendation_flows/1_Query_Hydration.md`.

- [ ] **Step 3: Correct the false claim**

`1_Query_Hydration.md` currently carries two adjacent bullets: one asserting an in-repo writer for `served_history` and `impressions`, one listing the keys the service reads that this repository does not write. The first is false, so the keys move into the second.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("docs/recommendation_flows/1_Query_Hydration.md")
t = p.read_text()
old = """- The in-repo serving side-effect writer maintains `user:{id}:served_history` and
  `user:{id}:impressions` after a request selects at least one item.
- The retrieval service reads `user:{id}:recent` and `user:{id}:rated` lists plus
  `user:{id}:request_history`, `user:{id}:bloom_filter`, and `user:{id}:cached_movies` hashes, but
  this repository contains no writer for those keys."""
new = """- The retrieval service maintains `user:{id}:served_history` and `user:{id}:impressions` itself,
  after a request selects at least one item. The writer left with the service: neither key has a
  writer, a reader, or a test in this checkout, and both appear here only as documentation.
- The retrieval service reads `user:{id}:recent` and `user:{id}:rated` lists plus
  `user:{id}:request_history`, `user:{id}:bloom_filter`, and `user:{id}:cached_movies` hashes, but
  this repository contains no writer for those keys."""
assert t.count(old) == 1, "the two bullets do not match verbatim -- re-read lines 26-34"
p.write_text(t.replace(old, new))
PY
```

- [ ] **Step 4: Add the boundary block to all nine documents**

The block goes directly under the `**References:**` line, which every one of the nine has.

```bash
cd recsys-pipeline/docs/recommendation_flows
python3 - <<'PY'
from pathlib import Path
block = """> **Where this runs:** this stage executes in the retrieval service, now in
> [lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service) — see
> the [repository boundary](../../../README.md#repository-boundary). The Redis key contract below is
> owned by this repository and is authoritative. The class and bean names are as of the split and
> may have been renamed there; nothing in either repository detects that.
"""
for path in sorted(Path(".").glob("*.md")):
    lines = path.read_text().splitlines(keepends=True)
    index = next(i for i, line in enumerate(lines) if line.startswith("**References:**"))
    lines.insert(index + 2, block + "\n")
    path.write_text("".join(lines))
    print("marked", path.name)
PY
```

Expected: nine `marked ...` lines.

- [ ] **Step 5: Strip the clause the block now duplicates in 7_Shuffling.md**

`7_Shuffling.md` already said where the property lives. The property-specific half is unique to that document and stays; only the repository-and-boundary clause goes, because the block above now carries it.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("docs/recommendation_flows/7_Shuffling.md")
t = p.read_text()
old = """`recsys.candidate-generation.top-n-randomization-pool`
(`RECSYS_RANDOMIZATION_POOL`, default `5`) is declared in the retrieval service's own
`application.yml`, now in
[lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service) (see the
[repository boundary](../../../README.md#repository-boundary)). In the current serving"""
new = """`recsys.candidate-generation.top-n-randomization-pool`
(`RECSYS_RANDOMIZATION_POOL`, default `5`) is declared in the retrieval service's own
`application.yml`. In the current serving"""
assert t.count(old) == 1
p.write_text(t.replace(old, new))
PY
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_doc_links.py -q`
Expected: PASS (4 passed). The link test passing here also proves the nine new `#repository-boundary` links resolve at their depth.

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/integration-tests/test_doc_links.py recsys-pipeline/docs/recommendation_flows
git commit -m "$(cat <<'MSG'
docs: mark the boundary in every flow document, and drop one false claim

1_Query_Hydration.md said an in-repo writer maintains served_history and
impressions. That writer left with the service: grep finds both keys in
documentation only, with no writer, reader or test in this checkout.

The nine flow documents narrate a request path that now runs entirely in the
other repository, and named thirty classes this checkout cannot compile. Each
now opens with the treatment 7_Shuffling.md already carried: where the stage
runs, that the Redis key contract here is the authoritative part, and that the
class names are as of the split.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 4: Drop the other repository's build tool from Requirements

**Files:**
- Modify: `README.md:81`

**Interfaces:**
- Consumes: nothing. Produces: nothing.

- [ ] **Step 1: Confirm the fact is already stated canonically**

Run: `grep -n 'Maven' README.md`
Expected: line 16 (the boundary table's "Builds with" row), line 30 ("Maven is a prerequisite of that repository, not of this checkout. Nothing here has a `pom.xml`"), and line 81 (the Requirements entry). Line 30 is what makes line 81 redundant rather than merely stale.

- [ ] **Step 2: Delete the Requirements entry**

```bash
python3 - <<'PY'
from pathlib import Path
p = Path("README.md")
t = p.read_text()
old = "- Maven 3.8+ (only to build the separate retrieval service, not this repository)\n"
assert t.count(old) == 1
p.write_text(t.replace(old, ""))
PY
grep -n 'Maven' README.md
```

Expected: lines 16 and 30 only.

- [ ] **Step 3: Verify nothing else claimed it**

Run: `git ls-files '*.xml' | grep -c pom || true` and `git ls-files '*.java' | wc -l`
Expected: `0` and `0`. The Requirements list was the last place this repository asked for a tool it cannot use.

- [ ] **Step 4: Run the full suite**

Run: `cd recsys-pipeline && python3 -m pytest -q`
Expected: `565 passed, 2 skipped` — the 561/2 baseline plus this plan's four new tests. Zero failures.

- [ ] **Step 5: Confirm the diff touched only documentation and one test**

Run: `git diff --stat origin/master -- . | tail -3` and `git diff --name-only origin/master | grep -vE '\.md$|test_doc_links\.py$'`
Expected: the second command prints nothing. No `.scala`, `.jsx`, `.json`, `.sh` or `.yml` file changed, so no build or frontend verification is required.

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "$(cat <<'MSG'
docs: stop listing Maven as a requirement of this repository

The boundary section fifty lines above already says Maven is a prerequisite
of the other repository and that nothing here has a pom.xml. Both remain
true: this checkout has no pom.xml and no .java file.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

## Self-Review

**Spec coverage.** Anchors → Task 1. Stale paths and the ASCII-block exception → Task 2. The false claim → Task 3 steps 3 and its guard. The nine boundary notes and the class-name caveat → Task 3 steps 4-5. The Maven line → Task 4. The guard itself → distributed across Tasks 1-3, which is the plan's recorded deviation. Spec acceptance items 1-8 map to Task 1 step 5, Task 2 step 5, Task 3 steps 2 and 6, Task 3 step 6, Task 4 step 2, Task 4 step 4, Task 4 step 5, and a final `git diff --check` in the PR step.

**Placeholders.** None: every step carries the exact script or command it needs, and every `replace` asserts its match count before writing so a drifted line fails loudly instead of silently doing nothing.

**Type consistency.** `live_markdown()`, `slugs()`, `noted_fence_lines()`, `GIT_ROOT`, `PREFIX`, `PREFIX_NOTE`, `BOUNDARY_SERVICE`, `BOUNDARY_ANCHOR` and `FLOWS` are each defined once, in the task that first uses them, and referenced under the same names afterward. `ENDPOINTS` is shared by one regex only.

**One risk worth stating.** Task 2 step 3 rewrites a heading (`## GET /metrics`). That is the exact move that caused two of Task 1's twelve failures. It is safe here only because no file links to that anchor today — and from Task 1 onward, the link test is what proves it stays safe.
