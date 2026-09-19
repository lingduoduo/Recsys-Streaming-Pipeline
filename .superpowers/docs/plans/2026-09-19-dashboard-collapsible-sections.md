# Collapse every dashboard section to its headline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every dashboard section a native `<details>` disclosure whose collapsed row carries the section's headline number, so a route reads as a one-screen outline that still shows every key figure.

**Architecture:** One component changes — `Section` in `components/ui.jsx` — and all thirteen sections inherit it, because `measurements.jsx`, `diagnostics.jsx` and `keyword-report.jsx` all render cards through it. Native `<details>`/`<summary>` supplies the open state, the keyboard handling and the screen-reader semantics, so no section becomes a client component.

**Tech Stack:** React 19 server components, native HTML disclosure elements, plain CSS in `app/globals.css`, one `useEffect` in the existing `nav.jsx` client component. Tests are Python scanning JSX and CSS sources, plus `npm run build` and a live dev server fetch.

**Spec:** `.superpowers/docs/specs/2026-09-19-dashboard-collapsible-sections-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `validate_measurements.mjs`, `export_dashboard_json.py` and every Python compute function are untouched.
- `Section` keeps its exact prop list — `title`, `headline`, `description`, `actions`, `id`, `children` — and every call site is unchanged. The change is entirely inside its body.
- `NaCard`, `MetricTile`, `MetricGrid`, `MetricCard`, `ChartGrid`, `BarChart`, `GroupedBarChart` and `DataTable` keep their current markup.
- Section `id`s keep their current values: the scorecard's tile hrefs and `test_dashboard_routes.py` depend on them.
- The six contract tests in `test_dashboard_measurement_contract.py` must keep passing **unmodified** — they parse call sites, which this change does not touch.
- `.report-card` keeps its class name.
- Measured baseline at this branch point: **574 passed, 2 skipped**. This plan adds five tests — one in Task 1, three in Task 2, one in Task 3 — so the final expected result is **579 passed, 2 skipped**. The spec says four and 578; counting the tasks rather than trusting that summary line is what caught it.

## Pre-validated facts

Measured before this plan was written — do not re-derive:

- The palette defined on `:root` is `--canvas`, `--surface`, `--ink`, `--muted`, `--line`, `--indigo`, `--indigo-soft`, `--amber`, `--amber-soft`, `--shadow` and three `--series-*` entries. **There is no `--border`.**
- The nav CSS added by #255 uses `var(--border)` twice — in `.nav`'s `border-bottom` and `.nav a:hover`'s `border-color`. Both resolve to nothing, so the nav has no separator rule and no hover border. This is a live defect on `master`.
- `Section` is the only card renderer: `measurements.jsx` calls it once (inside `MeasurementSection`), `diagnostics.jsx` five times, `keyword-report.jsx` once. `NaCard` is separate and stays flat.
- `.section-heading h2` is `margin: 0 0 10px; font-size: 1.3rem; letter-spacing: -0.02em;` — the summary must carry equivalent rules once `.section-heading` no longer wraps the title.
- There are two client components: `nav.jsx` and `keyword-report.jsx`.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `frontend/components/ui.jsx` | `Section` becomes a `<details>` disclosure. |
| Modify: `frontend/app/globals.css` | `.section-summary` rules and its marker; fix the two `var(--border)` uses. |
| Modify: `frontend/components/nav.jsx` | Open the `<details>` named by `location.hash`. |
| Modify: `frontend/integration-tests/python_modeling/test_dashboard_routes.py` | Four new assertions. |
| Modify: `frontend/README.md` | One paragraph on the collapsed default. |

---

### Task 1: Fix the undefined CSS variable, and guard against another

**Files:**
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py` (append)
- Modify: `recsys-pipeline/frontend/app/globals.css`

**Interfaces:**
- Consumes: `FRONTEND` from the existing module.
- Produces: nothing later tasks use.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`:

```python
def test_every_css_variable_used_is_defined():
    """An undefined custom property is silently dropped, and nothing catches it.

    `border-bottom: 1px solid var(--border)` with no --border declared computes to no
    border at all: the stylesheet still parses, `npm run build` still succeeds, and the
    rule just does not apply. #255 shipped exactly that twice in the nav.
    """
    css = (FRONTEND / "app" / "globals.css").read_text(encoding="utf-8")
    declared = set(re.findall(r"^\s*(--[\w-]+)\s*:", css, re.M))
    used = set(re.findall(r"var\((--[\w-]+)", css))
    undefined = sorted(used - declared)
    assert not undefined, (
        "these custom properties are used but never declared, so every rule using them "
        f"is silently dropped: {', '.join(undefined)}"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py::test_every_css_variable_used_is_defined -q 2>&1 | grep -E 'silently dropped|passed|failed'`
Expected: FAIL naming `--border`.

- [ ] **Step 3: Use the variable that exists**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("app/globals.css")
t = p.read_text()
assert t.count("var(--border)") == 2, f"expected 2 uses, found {t.count('var(--border)')}"
p.write_text(t.replace("var(--border)", "var(--line)"))
print("--border -> --line in both nav rules")
PY
grep -n 'var(--line)' app/globals.css | tail -3
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -1`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py \
        recsys-pipeline/frontend/app/globals.css
git commit -m "$(cat <<'MSG'
fix(dashboard): the nav referenced a CSS variable that does not exist

#255 styled the route nav with var(--border). The palette declares --line;
there is no --border, so both rules -- the nav's bottom separator and the hover
border on each link -- computed to nothing and were dropped.

An undefined custom property is invisible to every gate this repository has:
the stylesheet parses, npm run build succeeds, and the rule simply does not
apply. The new assertion compares every var() against the declarations on
:root.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: Section becomes a disclosure

**Files:**
- Modify: `recsys-pipeline/frontend/components/ui.jsx` (`Section`, lines 12-30)
- Modify: `recsys-pipeline/frontend/app/globals.css`

**Interfaces:**
- Consumes: nothing.
- Produces: `<details className="report-card" id={id}>` with `<summary className="section-summary">` and `<div className="section-body">`. Tasks 3 and 4 depend on that structure.

- [ ] **Step 1: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`:

```python
def test_sections_are_collapsible_disclosures():
    """Thirteen full-height sections stacked in one column is what this replaced.

    Every card renders through Section, so one component decides it for all of them.
    """
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    section = re.search(r"export function Section\(\{(.*?)\n\}", ui, re.S)
    assert section, "ui.jsx must export a Section component"
    body = section.group(1)
    assert "<details" in body and "<summary" in body, (
        "Section must render a <details>/<summary> disclosure; a plain <section> is the "
        "stacked layout this replaced"
    )


def test_a_collapsed_section_still_shows_its_headline():
    """Collapsing without the headline turns the page into a contents list.

    Every section computes a headline -- "CTR 4.2%", "p95 42 ms" -- and the collapsed
    row is where it has to appear for the closed page to still read as a report.
    """
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    summary = re.search(r"<summary[^>]*>(.*?)</summary>", ui, re.S)
    assert summary, "Section's disclosure must have a summary"
    assert "headline" in summary.group(1), (
        "the summary must render {headline}; without it a collapsed section shows only "
        "its title"
    )


def test_na_cards_are_not_disclosures():
    """A section with no measurement has nothing to expand into."""
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    na = re.search(r"export function NaCard\(\{(.*?)\n\}", ui, re.S)
    assert na, "ui.jsx must export a NaCard component"
    assert "<details" not in na.group(1), (
        "NaCard must stay a flat card: opening it would reveal nothing"
    )
```

- [ ] **Step 2: Run the tests to verify two fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -4`
Expected: 2 failed (`test_sections_are_collapsible_disclosures`, `test_a_collapsed_section_still_shows_its_headline`), 6 passed — `test_na_cards_are_not_disclosures` already holds.

- [ ] **Step 3: Rewrite Section**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/ui.jsx")
t = p.read_text()
old = '''export function Section({ title, headline, description, actions, id, children }) {
  return (
    <section className="report-card" id={id}>
      {/* The flex row is its own element: `.section-heading` is shared with NaCard,
          whose heading is a plain h2 + paragraph that must keep stacking. */}
      <div className="section-heading">
        <div className="section-heading-row">
          <div className="section-heading-main">
            <h2>{title}</h2>
            {headline ? <p className="insight">{headline}</p> : null}
            {description ? <p className="section-description">{description}</p> : null}
          </div>
          {actions ? <div className="section-actions">{actions}</div> : null}
        </div>
      </div>
      <div className="section-body">{children}</div>
    </section>
  );
}'''
new = '''export function Section({ title, headline, description, actions, id, children }) {
  return (
    // A native disclosure: the browser owns the open state, the keyboard handling and the
    // screen-reader semantics, so no section has to become a client component. Collapsed by
    // default -- the summary carries the headline, so a closed page still reads as a report
    // rather than a table of contents.
    <details className="report-card" id={id}>
      <summary className="section-summary">
        <div className="section-summary-main">
          <h2>{title}</h2>
          {headline ? <p className="insight">{headline}</p> : null}
        </div>
      </summary>
      <div className="section-body">
        {description ? <p className="section-description">{description}</p> : null}
        {actions ? <div className="section-actions">{actions}</div> : null}
        {children}
      </div>
    </details>
  );
}'''
assert t.count(old) == 1, "Section's body does not match verbatim"
p.write_text(t.replace(old, new))
print("Section is now a disclosure")
PY
```

- [ ] **Step 4: Style the summary**

`.section-heading` no longer wraps the title, so its `h2` rules are restated for the summary. The
default marker is replaced by a triangle that rotates when open, which renders the same across
browsers.

```bash
cd recsys-pipeline/frontend
cat >> app/globals.css <<'CSS'

/* The collapsed row of a section. It carries the title and that section's headline number,
   so a closed page is still a readable report. */
.section-summary {
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 20px 28px;
  cursor: pointer;
  list-style: none;
}

/* Replace the default disclosure marker with one that renders identically everywhere. */
.section-summary::-webkit-details-marker {
  display: none;
}

.section-summary::before {
  content: "";
  flex: none;
  width: 0;
  height: 0;
  border-left: 7px solid var(--muted);
  border-top: 5px solid transparent;
  border-bottom: 5px solid transparent;
  transition: transform 120ms ease;
}

details[open] > .section-summary::before {
  transform: rotate(90deg);
}

details[open] > .section-summary {
  border-bottom: 1px solid var(--line);
}

.section-summary-main {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 8px 14px;
  min-width: 0;
}

.section-summary h2 {
  margin: 0;
  font-size: 1.3rem;
  letter-spacing: -0.02em;
}
CSS
```

- [ ] **Step 5: Run the tests and build**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py integration-tests/python_modeling/test_dashboard_measurement_contract.py -q 2>&1 | tail -2`
Expected: 23 passed — the eight route assertions and the fifteen contract tests, the latter unmodified.

Then, with **no dev server running** (a build clobbers the shared `.next`):
Run: `cd recsys-pipeline/frontend && npm run build 2>&1 | tail -8`
Expected: successful build listing `/`, `/online`, `/offline`.

- [ ] **Step 6: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/ui.jsx recsys-pipeline/frontend/app/globals.css \
        recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py
git commit -m "$(cat <<'MSG'
feat(dashboard): collapse every section to a row carrying its headline

/online stacked nine full-height sections. Each one now renders as a native
<details> whose summary holds the title and that section's headline number, so
the route reads as a one-screen outline that still shows every key figure and
opens on a click.

Native disclosure means the browser owns the open state, the keyboard handling
and the screen-reader semantics: no section becomes a client component and
there is no state to manage. NaCard stays flat -- a section with no measurement
has nothing to expand into.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: A targeted section opens

**Files:**
- Modify: `recsys-pipeline/frontend/components/nav.jsx`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py` (append)

**Interfaces:**
- Consumes: the `<details id=…>` structure from Task 2.
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`:

```python
def test_the_hash_target_is_opened():
    """A scorecard tile links to /online#satisfaction.

    With sections collapsed, that anchor would otherwise land a reader on a closed row.
    Nav already runs on every route and is already a client component, so it is where
    the eight lines of DOM work belong rather than in a second client boundary.
    """
    nav = (FRONTEND / "components" / "nav.jsx").read_text(encoding="utf-8")
    assert "hashchange" in nav, "Nav must react to hashchange, not only to the first load"
    assert re.search(r"\.open\s*=\s*true", nav), (
        "Nav must set open on the <details> the hash names, or a tile click lands on a "
        "collapsed section"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py::test_the_hash_target_is_opened -q 2>&1 | grep -E 'hashchange|passed|failed'`
Expected: FAIL on the `hashchange` assertion.

- [ ] **Step 3: Open the target in Nav**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/nav.jsx")
t = p.read_text()

old_import = 'import { usePathname } from "next/navigation";'
new_import = 'import { useEffect } from "react";\nimport { usePathname } from "next/navigation";'
assert t.count(old_import) == 1
t = t.replace(old_import, new_import)

old_body = "  const pathname = usePathname();\n"
new_body = '''  const pathname = usePathname();

  // Sections are collapsed by default, so a link to #satisfaction would land on a closed
  // row. Open whatever the hash names, on arrival and on every later hash change.
  useEffect(() => {
    const openTarget = () => {
      const target = document.getElementById(decodeURIComponent(window.location.hash.slice(1)));
      if (target instanceof HTMLDetailsElement) target.open = true;
    };
    openTarget();
    window.addEventListener("hashchange", openTarget);
    return () => window.removeEventListener("hashchange", openTarget);
  }, [pathname]);
'''
assert t.count(old_body) == 1
p.write_text(t.replace(old_body, new_body))
print("Nav opens the hash target")
PY
```

- [ ] **Step 4: Run the test and rebuild**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -1`
Expected: 9 passed.

Run: `cd recsys-pipeline/frontend && npm run build 2>&1 | tail -5`
Expected: successful build.

- [ ] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/nav.jsx \
        recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py
git commit -m "$(cat <<'MSG'
feat(dashboard): open the section a scorecard tile points at

The tiles link to /online#satisfaction and friends. With sections collapsed
that anchor scrolls to a closed row, so a tile click stopped showing the detail
it used to.

Nav opens whatever the hash names, on arrival and on hashchange. It lives there
because Nav already renders on every route and is already a client component
for its active-link state; a second client boundary for eight lines of DOM work
would cost a bundle entry for nothing.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 4: Verify against the rendered pages, and document

**Files:**
- Modify: `recsys-pipeline/frontend/README.md`

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

- [ ] **Step 1: Count disclosures on the rendered routes**

A build was run last, so `.next` holds a production build; start the dev server fresh.

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
rm -rf .next
(npm run dev >/tmp/dev.log 2>&1 &)
until curl -sf -o /dev/null http://localhost:3000/online 2>/dev/null; do sleep 2; done
echo -n "/online <details> count: "; curl -s http://localhost:3000/online | grep -o '<details' | wc -l | tr -d ' '
echo -n "/offline <details> count: "; curl -s http://localhost:3000/offline | grep -o '<details' | wc -l | tr -d ' '
echo "ids on /online:"; curl -s http://localhost:3000/online | grep -oE 'id="[a-z-]+"' | sort -u | tr '\n' ' '
```

Expected: 9 on `/online` (one per section; `keyword` and the six measurement sections and the two
other diagnostics), 4 on `/offline`, and the same ids as before the change.

Note: if a section is currently rendering as `NaCard` because its data is unavailable, it is a
`<section>` and not counted. Check the count against how many of that route's sections are
`"available"` in the snapshot before treating a lower number as a failure.

- [ ] **Step 2: Document the collapsed default**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("README.md")
t = p.read_text()
anchor = """`components/groups.js` maps each section to its route; moving one is a one-line change there."""
addition = """`components/groups.js` maps each section to its route; moving one is a one-line change there.

Sections are collapsed by default. Each one's row carries its headline number — CTR, p95, fresh
share — so a route reads as an outline that still shows every key figure, and a click opens the
charts and tables. Clicking a scorecard tile opens the section it points at. The open state is not
remembered: every visit starts collapsed."""
assert t.count(anchor) == 1
p.write_text(t.replace(anchor, addition))
print("README documents the collapsed default")
PY
```

- [ ] **Step 3: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
(cd frontend && npm run validate:data)
git -C .. diff --name-only origin/master | grep -x 'recsys-pipeline/frontend/data/dashboard.json' && echo "FAIL: snapshot changed" || echo "snapshot untouched"
git -C .. diff --check && echo "diff --check clean"
```

Expected: `579 passed, 2 skipped`; `dashboard.json valid: 7 measurement sections, schema 2.0`;
`snapshot untouched`; `diff --check clean`.

- [ ] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/README.md
git commit -m "$(cat <<'MSG'
docs: describe the collapsed default and what opens a section

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

## Self-Review

**Spec coverage.** The disclosure → Task 2. The anchor behaviour → Task 3. The guard → Tasks 1-3, each assertion landing with the change it covers. The documentation → Task 4. The spec's constraint that the six contract tests pass unmodified → verified in Task 2 step 5. Spec acceptance items 1-8 map to Task 2 step 5 and Task 3 step 4 (item 1), Task 4 step 3 (items 2, 6, 7, 8), Task 2 step 5 (items 3, 4), and Task 4 step 1 (item 5).

**Placeholders.** None. Every replacement asserts its match count, and the CSS variable inventory and the `.section-heading h2` rules were measured before writing — see Pre-validated facts.

**Type consistency.** `Section`'s props are unchanged, so no call site moves. `.section-summary`, `.section-summary-main` and `.section-body` are the class names used in both Task 2's JSX and Task 2's CSS. `FRONTEND` is the existing module constant the new assertions reuse.

**One scope note.** Task 1 fixes a defect that #255 shipped and this change did not cause. It is included because it is two characters in the same stylesheet this plan already edits, and because the guard that catches it belongs with the other CSS-aware assertions. Splitting it into its own pull request would mean two reviews of one file.
