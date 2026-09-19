# A sidebar and a page per section — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Replace three multi-section routes with a persistent grouped sidebar and thirteen pages, each showing exactly one section under an eyebrow, a title and a description.

**Architecture:** One dynamic route, `app/[group]/[section]/page.jsx`, generates all thirteen pages from a catalogue. The catalogue splits in two so the sidebar stays a small client bundle: `groups.js` is pure data and imports nothing, `section-registry.jsx` maps keys to components and is imported only by the server page. `Section` reverts to a flat card, because a page holding one section has nothing to collapse.

**Tech Stack:** Next.js 15.1.6 app router with `generateStaticParams` and `notFound()`, React 19 server components, one client component for the sidebar, plain CSS. Tests are Python scanning JSX, plus `npm run build` and live dev-server fetches.

**Spec:** `.superpowers/docs/specs/2026-09-19-dashboard-sidebar-section-pages-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `validate_measurements.mjs`, `export_dashboard_json.py` and every Python compute function are untouched.
- The six contract tests in `test_dashboard_measurement_contract.py` must keep passing **unmodified**.
- Every section component keeps its exact exported name and its `data` prop.
- Section `id`s keep their current values.
- `SECTION_ROUTE` keeps its name and key set; only its values change.
- `components/groups.js` imports nothing.
- Every `var()` in `globals.css` resolves to a `:root` declaration or an inline JSX style — the #256 guard.
- Measured baseline at this branch point: **580 passed, 2 skipped**. This change **replaces** assertions as well as adding them: `test_dashboard_routes.py` is rewritten from 10 tests to 11, so the expected final result is **581 passed, 2 skipped**.

## Execution record

Executed 2026-09-19 on `feat/dashboard-sidebar-section-pages`, four commits, all steps checked.
Final suite: **581 passed, 2 skipped**, from a 580/2 baseline, exactly as predicted.

Two deviations:

1. **Tasks 2 and 3 had to land as one commit.** The plan put a build gate at the end of Task 2,
   which could never have passed: Task 1 removed `ROUTES` from `groups.js`, and `nav.jsx` -- which
   Task 3 deletes -- still imported it, so the tree did not build in between. The route change and
   the nav replacement are mutually dependent. This is the same ordering flaw the previous plan
   caught and fixed with a temporary barrel; here it was not caught until the build ran.
2. **The purity assertion matched its own documentation.** `assert "import" not in groups` fired on
   the word "imported" in groups.js's header comment. It now matches `^import\b` statements.

One environment trap, twice now: an orphaned dev server still held port 3000, so a newly started one
took 3001 while every `curl localhost:3000` checked the stale build. Kill by pattern
(`pkill -f 'next dev'`) and confirm the port is free before trusting any rendered-page check.

## Pre-validated facts

Measured before this plan was written:

- `test_dashboard_routes.py` currently holds ten tests. Four describe designs this change removes — `test_every_section_is_mounted_on_the_page_its_route_names` and `test_no_section_is_mounted_on_two_pages` (no `page.jsx` will name a component), `test_sections_are_collapsible_disclosures` and `test_a_collapsed_section_still_shows_its_headline` (the disclosure is reverted), plus `test_the_hash_target_is_opened` (nothing to open). The file is rewritten rather than patched.
- `groups.js` exports `ROUTES` (3 entries) and `SECTION_ROUTE` (13 keys, values `"/online"` or `"/offline"`).
- `nav.jsx` is a client component with the active-link state and the hash-opening effect from #256.
- The 13 section keys and their components: `query`→`QuerySection`, `keyword`→`KeywordSection`, `engagement`→`EngagementSection`, `satisfaction`→`SatisfactionSection`, `freshness`→`FreshnessSection`, `diversity`→`DiversitySection`, `fairness`→`FairnessSection`, `safety`→`SafetySection`, `latency`→`LatencySection`, `recall`→`RecallSection`, `ranking`→`RankingSection`, `relevance`→`RelevanceSection`, `ope`→`OpeSection`.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `frontend/components/groups.js` | `GROUPS`, `SECTIONS`, `SECTION_ROUTE`. Pure data, no imports. |
| Create: `frontend/components/section-registry.jsx` | Key → component. Imported only by the server page. |
| Create: `frontend/app/[group]/[section]/page.jsx` | The one page that renders all thirteen. |
| Delete: `frontend/app/online/page.jsx`, `frontend/app/offline/page.jsx` | Their routes are group prefixes now. |
| Rename: `frontend/components/nav.jsx` → `frontend/components/sidebar.jsx` | Grouped sidebar, active state, no hash effect. |
| Modify: `frontend/app/layout.jsx` | Two-column shell. |
| Modify: `frontend/components/ui.jsx` | `Section` back to a flat card, no title. |
| Modify: `frontend/app/globals.css` | Sidebar, page header, two-column grid, flat card; drop `.nav` and `.section-summary`. |
| Modify: `frontend/components/scorecard.jsx` | Tile hrefs lose the fragment. |
| Rewrite: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py` | Guards for the new shape. |
| Modify: `frontend/README.md` | Routes section. |

---

### Task 1: The catalogue

**Files:**
- Modify: `recsys-pipeline/frontend/components/groups.js`
- Modify: `recsys-pipeline/frontend/components/scorecard.jsx`

**Interfaces:**
- Produces: `GROUPS` — `[{key, label}]` for `online` and `offline`; `SECTIONS` — `{[key]: {group, label, description}}` for all thirteen; `SECTION_ROUTE` — `{[key]: "/<group>/<key>"}`. Tasks 2, 3 and 5 import these by name.

- [x] **Step 1: Rewrite the catalogue**

```bash
cd recsys-pipeline/frontend
cat > components/groups.js <<'JS'
// The dashboard's catalogue: which sections exist, what they are called, and where each
// one lives. Pure data with no imports, because the sidebar is a client component and
// must not pull chart and table code into the browser bundle -- section-registry.jsx
// holds the component map and is imported only by the server page.
export const GROUPS = [
  { key: "online", label: "Online prediction" },
  { key: "offline", label: "Offline prediction" },
];

// Online sections describe what the serving path actually did; offline sections are models
// re-scored afterwards. Only latency is purely live telemetry -- satisfaction, freshness
// and safety are offline rows with live ones merged in, and intents and the off-policy arms
// are computed from logged Parquet.
export const SECTIONS = {
  query: {
    group: "online", label: "Intents",
    description: "What users asked for, and how query length moves click-through.",
  },
  keyword: {
    group: "online", label: "Keyword gap",
    description: "Where catalog supply diverges from query demand.",
  },
  engagement: {
    group: "online", label: "Engagement funnel",
    description: "Impression to click to order, broken down by query and genre.",
  },
  satisfaction: {
    group: "online", label: "Satisfaction",
    description: "Observed satisfaction of the slates that were served.",
  },
  freshness: {
    group: "online", label: "Freshness",
    description: "How recent the served catalog is, against the freshness window.",
  },
  diversity: {
    group: "online", label: "Diversity",
    description: "Genre spread across served slates.",
  },
  fairness: {
    group: "online", label: "Fairness",
    description: "Outcome parity across the supported demographic groups.",
  },
  safety: {
    group: "online", label: "Safety",
    description: "Candidate safety policy accounting and unsafe exposure.",
  },
  latency: {
    group: "online", label: "Latency",
    description: "Live request and stage latency reported by the backend.",
  },
  recall: {
    group: "offline", label: "Candidate recall",
    description: "recall@k and hit rate for each retrieval method.",
  },
  ranking: {
    group: "offline", label: "Ranking quality",
    description: "AUC and log loss for each scoring signal.",
  },
  relevance: {
    group: "offline", label: "Relevance",
    description: "Graded relevance — NDCG and MRR — across labeled slates.",
  },
  ope: {
    group: "offline", label: "Off-policy evaluation",
    description: "Estimated value and lift of each candidate policy on logged events.",
  },
};

// Derived so a section cannot be listed in one place and routed to another.
export const SECTION_ROUTE = Object.fromEntries(
  Object.entries(SECTIONS).map(([key, { group }]) => [key, `/${group}/${key}`]),
);
JS
node -e "import('./components/groups.js').then(m => {
  console.log('groups:', m.GROUPS.length, '| sections:', Object.keys(m.SECTIONS).length);
  console.log('sample route:', m.SECTION_ROUTE.satisfaction, m.SECTION_ROUTE.ope);
})" 2>/dev/null
```

Expected: `groups: 2 | sections: 13` and `sample route: /online/satisfaction /offline/ope`.

- [x] **Step 2: Drop the fragment from the tile hrefs**

`SECTION_ROUTE[key]` is a page now, so appending `#key` would add a redundant hash.

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/scorecard.jsx")
t = p.read_text()
old = "            href={`${SECTION_ROUTE[key]}#${key}`}"
new = "            href={SECTION_ROUTE[key]}"
assert t.count(old) == 1, "the scorecard's href line does not match verbatim"
p.write_text(t.replace(old, new))
print("tile hrefs are page links")
PY
grep -n 'SECTION_ROUTE' components/scorecard.jsx
```

- [x] **Step 3: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/groups.js recsys-pipeline/frontend/components/scorecard.jsx
git commit -m "$(cat <<'MSG'
refactor(dashboard): make groups.js the catalogue of sections

Each section gains the label the sidebar and the page title show, and the
description that sits beneath the title. SECTION_ROUTE is derived from that
rather than written separately, so a section cannot be listed under one group
and routed to another.

The scorecard's tiles link to pages now, so they drop the fragment.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: One route for thirteen pages

**Files:**
- Create: `recsys-pipeline/frontend/components/section-registry.jsx`
- Create: `recsys-pipeline/frontend/app/[group]/[section]/page.jsx`
- Delete: `recsys-pipeline/frontend/app/online/page.jsx`, `recsys-pipeline/frontend/app/offline/page.jsx`

**Interfaces:**
- Consumes: `GROUPS`, `SECTIONS` from `./groups`.
- Produces: `SECTION_COMPONENTS` — `{[key]: Component}` — from `./section-registry`. Task 5's guard reads it.

- [x] **Step 1: Write the registry**

```bash
cd recsys-pipeline/frontend
cat > components/section-registry.jsx <<'JSX'
import {
  RelevanceSection, SatisfactionSection, FreshnessSection, DiversitySection,
  FairnessSection, SafetySection, LatencySection,
} from "./measurements";
import {
  EngagementSection, QuerySection, RecallSection, RankingSection, OpeSection,
} from "./diagnostics";
import { KeywordSection } from "./keyword-report";

// Key -> the component that renders it. Kept apart from groups.js on purpose: the sidebar
// imports groups.js and is a client component, so pulling this map -- and through it every
// chart and table -- into that bundle would be a needless cost.
export const SECTION_COMPONENTS = {
  query: QuerySection,
  keyword: KeywordSection,
  engagement: EngagementSection,
  satisfaction: SatisfactionSection,
  freshness: FreshnessSection,
  diversity: DiversitySection,
  fairness: FairnessSection,
  safety: SafetySection,
  latency: LatencySection,
  recall: RecallSection,
  ranking: RankingSection,
  relevance: RelevanceSection,
  ope: OpeSection,
};
JSX
```

- [x] **Step 2: Write the dynamic page**

```bash
cd recsys-pipeline/frontend
mkdir -p "app/[group]/[section]"
cat > "app/[group]/[section]/page.jsx" <<'JSX'
import { notFound } from "next/navigation";
import data from "../../../data/dashboard.json";
import { GROUPS, SECTIONS } from "../../../components/groups";
import { SECTION_COMPONENTS } from "../../../components/section-registry";

// Every valid group/section pair, so all thirteen prerender as static content.
export function generateStaticParams() {
  return Object.entries(SECTIONS).map(([section, { group }]) => ({ group, section }));
}

export default async function Page({ params }) {
  const { group, section } = await params;
  const spec = SECTIONS[section];
  if (!spec || spec.group !== group) notFound();

  const Component = SECTION_COMPONENTS[section];
  const groupLabel = GROUPS.find((entry) => entry.key === group)?.label ?? group;

  return (
    <article className="section-page">
      <header className="page-header">
        <span className="eyebrow">{groupLabel}</span>
        <h1>{spec.label}</h1>
        <p className="page-description">{spec.description}</p>
      </header>
      <Component data={data[section]} />
    </article>
  );
}
JSX
rm app/online/page.jsx app/offline/page.jsx
rmdir app/online app/offline
find app -name 'page.jsx' | sort
```

Expected: `app/[group]/[section]/page.jsx` and `app/page.jsx`.

- [x] **Step 3: Build and confirm fourteen routes**

With **no dev server running** — a build clobbers the shared `.next`.

Run: `cd recsys-pipeline/frontend && npm run build 2>&1 | tail -24`
Expected: a route table listing `/` and thirteen entries under `/online/…` and `/offline/…`, all
marked as static.

- [x] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add -A recsys-pipeline/frontend/app recsys-pipeline/frontend/components/section-registry.jsx
git commit -m "$(cat <<'MSG'
feat(dashboard): one page per section, generated from the catalogue

app/[group]/[section]/page.jsx resolves the section from the URL and renders
it under an eyebrow, a title and a description. generateStaticParams
enumerates all thirteen pairs, so every page prerenders exactly as the three
routes did. An unknown pair is a 404 rather than an empty page.

Thirteen near-identical page files would be thirteen places to forget
something.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: The sidebar

**Files:**
- Create: `recsys-pipeline/frontend/components/sidebar.jsx`
- Delete: `recsys-pipeline/frontend/components/nav.jsx`
- Modify: `recsys-pipeline/frontend/app/layout.jsx`
- Modify: `recsys-pipeline/frontend/app/globals.css`

**Interfaces:**
- Consumes: `GROUPS`, `SECTIONS`, `SECTION_ROUTE` from `./groups`.
- Produces: `Sidebar`, used only by `app/layout.jsx`.

- [x] **Step 1: Write the sidebar**

```bash
cd recsys-pipeline/frontend
cat > components/sidebar.jsx <<'JSX'
"use client";

import { usePathname } from "next/navigation";
import { GROUPS, SECTIONS, SECTION_ROUTE } from "./groups";

// A client component because it marks the active link from the current path. It imports
// groups.js only -- pure data -- so no chart or table code reaches this bundle. Every page
// and every section stays a server component; keyword-report.jsx is the other client
// component, for its Top-K selector.
export function Sidebar() {
  const pathname = usePathname();

  const link = (href, label) => (
    <a key={href} href={href}
       className={pathname === href ? "sidebar-link sidebar-active" : "sidebar-link"}
       aria-current={pathname === href ? "page" : undefined}>
      {label}
    </a>
  );

  return (
    <nav className="sidebar" aria-label="Dashboard sections">
      <div className="sidebar-brand">
        <span className="sidebar-mark">RC</span>
        <span>
          <strong>Recsys Analysis</strong>
          <em>Streaming pipeline</em>
        </span>
      </div>
      <div className="sidebar-group">{link("/", "Overview")}</div>
      {GROUPS.map(({ key, label }) => (
        <div className="sidebar-group" key={key}>
          <span className="sidebar-label">{label}</span>
          {Object.entries(SECTIONS)
            .filter(([, spec]) => spec.group === key)
            .map(([section, spec]) => link(SECTION_ROUTE[section], spec.label))}
        </div>
      ))}
    </nav>
  );
}
JSX
rm components/nav.jsx
```

- [x] **Step 2: Make the layout two columns**

```bash
cd recsys-pipeline/frontend
cat > app/layout.jsx <<'JSX'
import "./globals.css";
import data from "../data/dashboard.json";
import { Sidebar } from "../components/sidebar";

export const metadata = {
  title: "Recsys Analysis Dashboard",
  description: "Engagement, intent, retrieval, ranking, and offline policy evaluation.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <div className="app-shell">
          <Sidebar />
          <main className="app-main">
            {children}
            <footer className="app-footer">
              {data.rows.toLocaleString()} rows from <code>{data.input}</code>
            </footer>
          </main>
        </div>
      </body>
    </html>
  );
}
JSX
```

- [x] **Step 3: Replace the nav CSS with the shell CSS**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
import re
from pathlib import Path
p = Path("app/globals.css")
t = p.read_text()
# The .nav rules described a top pill bar that no longer exists.
start = t.index("/* Route nav in the shared layout.")
end = t.index("/* Groups the sections within a route.")
t = t[:start] + t[end:]
p.write_text(t)
print("nav rules removed")
PY
cat >> app/globals.css <<'CSS'

/* Two columns: a persistent sidebar of destinations, and one section beside it. */
.app-shell {
  display: grid;
  grid-template-columns: 248px minmax(0, 1fr);
  min-height: 100vh;
}

.sidebar {
  display: flex;
  flex-direction: column;
  gap: 22px;
  padding: 24px 18px;
  border-right: 1px solid var(--line);
  background: var(--surface);
}

.sidebar-brand {
  display: flex;
  align-items: center;
  gap: 11px;
  padding: 0 10px 4px;
}

.sidebar-mark {
  display: grid;
  place-items: center;
  width: 34px;
  height: 34px;
  flex: none;
  border-radius: 10px;
  background: var(--indigo);
  color: #ffffff;
  font-size: 0.78rem;
  font-weight: 700;
}

.sidebar-brand strong {
  display: block;
  font-size: 0.92rem;
  letter-spacing: -0.01em;
}

.sidebar-brand em {
  display: block;
  font-size: 0.74rem;
  font-style: normal;
  color: var(--muted);
}

.sidebar-group {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.sidebar-label {
  padding: 0 10px 6px;
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.09em;
  text-transform: uppercase;
  color: var(--muted);
}

.sidebar-link {
  padding: 7px 10px;
  border-radius: 8px;
  font-size: 0.86rem;
  color: var(--muted);
  text-decoration: none;
}

.sidebar-link:hover {
  color: var(--ink);
  background: var(--canvas);
}

.sidebar-link.sidebar-active {
  color: var(--ink);
  background: var(--indigo-soft);
  font-weight: 600;
}

.app-main {
  display: flex;
  flex-direction: column;
  gap: 24px;
  padding: 40px 44px 32px;
  min-width: 0;
}

.app-footer {
  margin-top: auto;
  padding-top: 18px;
  border-top: 1px solid var(--line);
  font-size: 0.78rem;
  color: var(--muted);
}

/* The header above the one section a page shows. */
.page-header h1 {
  margin: 6px 0 8px;
  font-size: 2rem;
  letter-spacing: -0.03em;
}

.page-description {
  margin: 0;
  max-width: 62ch;
  color: var(--muted);
}

.section-page {
  display: grid;
  gap: 22px;
}

/* A 248px column leaves nothing for a table on a phone, so the sidebar becomes a strip. */
@media (max-width: 860px) {
  .app-shell {
    grid-template-columns: minmax(0, 1fr);
  }

  .sidebar {
    flex-direction: row;
    flex-wrap: wrap;
    align-items: center;
    gap: 10px;
    border-right: none;
    border-bottom: 1px solid var(--line);
  }

  .sidebar-group {
    flex-direction: row;
    flex-wrap: wrap;
    align-items: center;
  }

  .app-main {
    padding: 24px 18px;
  }
}
CSS
```

- [x] **Step 4: Build**

Run: `cd recsys-pipeline/frontend && npm run build 2>&1 | tail -6`
Expected: successful build.

- [x] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add -A recsys-pipeline/frontend/components recsys-pipeline/frontend/app
git commit -m "$(cat <<'MSG'
feat(dashboard): a persistent grouped sidebar instead of a top nav

Every destination is listed and labelled by group, so the structure is visible
from any page rather than implied by scroll order. The layout becomes two
columns; the row count and input path move to a footer under the content.

The sidebar imports groups.js only. That file is pure data on purpose: pulling
the component registry into a client bundle would drag every chart and table
with it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 4: The flat card

**Files:**
- Modify: `recsys-pipeline/frontend/components/ui.jsx`
- Modify: `recsys-pipeline/frontend/app/globals.css`

**Interfaces:**
- Consumes: nothing.
- Produces: `Section` rendering `<section className="report-card">` with a `.section-heading` and `.section-body`, and no title.

- [x] **Step 1: Revert the disclosure**

A page holding one section has nothing to collapse, and the page header already carries the title.

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/ui.jsx")
t = p.read_text()
old = '''    // A native disclosure: the browser owns the open state, the keyboard handling and the
    // screen-reader semantics, so no section has to become a client component. Collapsed by
    // default -- the summary carries the headline, so a closed page still reads as a report
    // rather than a table of contents.
    <details className="report-card" id={id}>
      <summary className="section-summary">
        <div className="section-summary-main">
          <h2>{title}</h2>
          {headline ? <p className="insight">{headline}</p> : null}
          {metric ? <p className="section-metric">{metric}</p> : null}
        </div>
      </summary>
      <div className="section-body">
        {description ? <p className="section-description">{description}</p> : null}
        {actions ? <div className="section-actions">{actions}</div> : null}
        {children}
      </div>
    </details>'''
new = '''    // One section per page, so there is nothing to collapse: #256 made this a disclosure
    // when nine sections shared a route, and that is the wrong shape here. `title` stays in
    // the signature because six contract tests parse it out of the call sites, but the page
    // header renders it -- showing it here as well would print every title twice.
    <section className="report-card" id={id}>
      <div className="section-heading">
        {headline ? <p className="insight">{headline}</p> : null}
        {metric ? <p className="section-metric">{metric}</p> : null}
        {description ? <p className="section-description">{description}</p> : null}
        {actions ? <div className="section-actions">{actions}</div> : null}
      </div>
      <div className="section-body">{children}</div>
    </section>'''
assert t.count(old) == 1, "Section's disclosure body does not match verbatim"
p.write_text(t.replace(old, new))
print("Section is a flat card again")
PY
```

- [x] **Step 2: Replace the summary CSS**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("app/globals.css")
t = p.read_text()
start = t.index("/* The collapsed row of a section.")
end = t.index("/* The figure in a collapsed measurement row.")
t = t[:start] + t[end:]
t = t.replace("/* The figure in a collapsed measurement row. Those seven sections carry prose headlines\n   from the calculator, so the number comes from the same spec the scorecard tiles use. */",
              "/* The figure in a section's heading. The seven measurement sections carry prose headlines\n   from the calculator, so the number comes from the same spec the scorecard tiles use. */")
p.write_text(t)
print("summary rules removed")
PY
cat >> app/globals.css <<'CSS'

/* A section's heading now carries only its headline, figure and description: the page
   header above the card owns the title. */
.section-heading {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 10px 14px;
}
CSS
grep -c 'section-summary' app/globals.css
```

Expected: `0`.

- [x] **Step 3: Build and commit**

Run: `cd recsys-pipeline/frontend && npm run build 2>&1 | tail -5`
Expected: successful build.

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/ui.jsx recsys-pipeline/frontend/app/globals.css
git commit -m "$(cat <<'MSG'
refactor(dashboard): sections are flat cards again

#256 made Section a <details> because nine of them shared a route. With one
section per page that is the wrong shape -- the page would open as a single
collapsed row and seeing anything would take a click.

Section also stops rendering `title`: the page header owns it, and printing it
in both places would show every title twice. The prop stays in the signature
because six contract tests parse it out of the call sites.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 5: The guards and the documentation

**Files:**
- Rewrite: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`
- Modify: `recsys-pipeline/frontend/README.md`

**Interfaces:**
- Consumes: everything above.

- [x] **Step 1: Rewrite the guard module**

Four of the ten assertions describe designs this change removed. Replacing the file states that,
rather than leaving assertions that pass for the wrong reason.

```bash
cd recsys-pipeline
cat > integration-tests/python_modeling/test_dashboard_routes.py <<'PY'
"""The dashboard's catalogue, its routes and its chrome agree with each other.

Thirteen sections, one per page, listed in a sidebar. components/groups.js is the
catalogue and the single source of truth; these tests hold everything else to it.

Four assertions from earlier iterations are gone because the designs they guarded are:
sections are no longer mounted in page files (one dynamic route renders them all), and
they are no longer collapsible (#256 made them disclosures when nine shared a route,
which is the wrong shape for one section per page).
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FRONTEND = REPO / "frontend"
GROUPS = FRONTEND / "components" / "groups.js"
REGISTRY = FRONTEND / "components" / "section-registry.jsx"
SIDEBAR = FRONTEND / "components" / "sidebar.jsx"
PAGE = FRONTEND / "app" / "[group]" / "[section]" / "page.jsx"

SECTION_KEYS = {
    "query", "keyword", "engagement", "satisfaction", "freshness", "diversity",
    "fairness", "safety", "latency", "recall", "ranking", "relevance", "ope",
}


def _catalogue():
    """The SECTIONS map parsed out of groups.js: key -> group."""
    source = GROUPS.read_text(encoding="utf-8")
    body = re.search(r"export const SECTIONS = \{(.*?)\n\};", source, re.S)
    assert body, "groups.js must export a SECTIONS map"
    return dict(re.findall(r"(\w+):\s*\{\s*\n?\s*group:\s*\"(\w+)\"", body.group(1)))


def test_the_catalogue_lists_every_section():
    assert set(_catalogue()) == SECTION_KEYS


def test_every_section_has_a_component_and_every_component_a_section():
    """No page file names a component now, so the registry is where a section goes missing."""
    registry = REGISTRY.read_text(encoding="utf-8")
    mapped = set(re.findall(r"^\s+(\w+):\s*\w+Section,", registry, re.M))
    catalogue = set(_catalogue())
    assert mapped == catalogue, (
        f"registry and catalogue disagree: only in registry {sorted(mapped - catalogue)}, "
        f"only in catalogue {sorted(catalogue - mapped)}"
    )


def test_every_section_prerenders():
    """generateStaticParams enumerates the catalogue, so all thirteen are built."""
    page = PAGE.read_text(encoding="utf-8")
    assert "generateStaticParams" in page, "every sidebar link must prerender"
    assert "Object.entries(SECTIONS)" in page, (
        "the params must come from the catalogue, not a second hand-written list"
    )


def test_an_unknown_section_is_a_404():
    page = PAGE.read_text(encoding="utf-8")
    assert "notFound()" in page, "an unknown group/section pair must 404, not render empty"


def test_the_sidebar_lists_every_section_once():
    sidebar = SIDEBAR.read_text(encoding="utf-8")
    assert "SECTION_ROUTE[section]" in sidebar, (
        "sidebar links must come from SECTION_ROUTE so they cannot drift from the catalogue"
    )
    assert "GROUPS.map" in sidebar, "the sidebar must render one labelled block per group"


def test_the_sidebar_does_not_import_the_component_registry():
    """It is a client component: importing the registry would pull every chart into the bundle."""
    sidebar = SIDEBAR.read_text(encoding="utf-8")
    assert "section-registry" not in sidebar
    groups = GROUPS.read_text(encoding="utf-8")
    assert "import" not in groups, "groups.js must stay pure data with no imports"


def test_the_page_header_names_the_section():
    page = PAGE.read_text(encoding="utf-8")
    for fragment in ("{groupLabel}", "{spec.label}", "{spec.description}"):
        assert fragment in page, f"the page header must render {fragment}"


def test_sections_are_flat_cards():
    """#256 made these disclosures for a nine-section route. One per page has nothing to collapse."""
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    section = re.search(r"export function Section\(\{(.*?)\n\}", ui, re.S)
    assert section, "ui.jsx must export a Section component"
    assert "<details" not in section.group(1), (
        "Section must be a flat card: with one section per page a disclosure opens as a "
        "single collapsed row"
    )


def test_a_section_card_does_not_repeat_the_page_title():
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    section = re.search(r"export function Section\(\{(.*?)\n\}", ui, re.S)
    assert "{title}" not in section.group(1), (
        "the page header renders the title; rendering it here too prints it twice"
    )


def test_measurement_sections_still_carry_their_figure():
    """The seven measurement sections have prose headlines, so the number comes from HEADLINES."""
    measurements = (FRONTEND / "components" / "measurements.jsx").read_text(encoding="utf-8")
    assert "headlineValue" in measurements and re.search(r"metric=\{metric\}", measurements), (
        "MeasurementSection must pass the scorecard's figure to Section as `metric`"
    )


def test_every_css_variable_used_is_defined():
    """An undefined custom property is silently dropped, and nothing catches it.

    `border-bottom: 1px solid var(--border)` with no --border declared computes to no
    border at all: the stylesheet still parses, `npm run build` still succeeds, and the
    rule just does not apply. #255 shipped exactly that twice in the nav.
    """
    css = (FRONTEND / "app" / "globals.css").read_text(encoding="utf-8")
    declared = set(re.findall(r"^\s*(--[\w-]+)\s*:", css, re.M))
    # A property can also be declared inline from JSX -- keyword-report.jsx sets
    # {"--token-score": t} per token -- so the stylesheet alone is not the full picture.
    for path in sorted((FRONTEND / "components").glob("*.jsx")):
        declared.update(re.findall(r"\"(--[\w-]+)\"\s*:", path.read_text(encoding="utf-8")))
    used = set(re.findall(r"var\((--[\w-]+)", css))
    undefined = sorted(used - declared)
    assert not undefined, (
        "these custom properties are used but never declared, so every rule using them "
        f"is silently dropped: {', '.join(undefined)}"
    )
PY
python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -2
```

Expected: 11 passed.

- [x] **Step 2: Verify the rendered pages**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
rm -rf .next
(npm run dev >/tmp/dev.log 2>&1 &)
until curl -sf -o /dev/null http://localhost:3000/ 2>/dev/null; do sleep 2; done
for route in /online/satisfaction /online/latency /offline/ope /offline/relevance; do
  printf '%-24s HTTP %s  cards=%s\n' "$route" \
    "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:3000$route)" \
    "$(curl -s http://localhost:3000$route | grep -o 'report-card' | wc -l | tr -d ' ')"
done
printf '%-24s HTTP %s\n' "/online/nonsense" "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:3000/online/nonsense)"
```

Expected: 200 with one `report-card` each, and 404 for the unknown section.

- [x] **Step 3: Update the README**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
import re
from pathlib import Path
p = Path("README.md")
t = p.read_text()
start = t.index("## Routes\n")
end = t.index("## Run\n")
new = """## Routes

A sidebar lists every destination, grouped; each page shows exactly one section under an eyebrow,
a title and a description.

| Route | Sidebar group | Shows |
|---|---|---|
| `/` | — | The scorecard, seven tiles, each linking to that section's page |
| `/online/<section>` | Online prediction | What the serving path did: intents, keyword gap, engagement, satisfaction, freshness, diversity, fairness, safety, latency |
| `/offline/<section>` | Offline prediction | Models re-scored afterwards: recall, ranking, relevance, off-policy evaluation |

All thirteen come from one file, `app/[group]/[section]/page.jsx`, with `generateStaticParams`
enumerating the catalogue in `components/groups.js`. That catalogue is pure data with no imports,
because the sidebar is a client component; `components/section-registry.jsx` holds the key-to-
component map and is imported only by the server page.

Only `latency` is purely live telemetry — satisfaction, freshness and safety are offline rows with
live ones merged in when a backend was running.

"""
p.write_text(t[:start] + new + t[end:])
print("README routes section rewritten")
PY
```

- [x] **Step 4: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
python3 -m pytest integration-tests/python_modeling/test_dashboard_measurement_contract.py -q 2>&1 | tail -1
(cd frontend && npm run validate:data)
git -C .. diff --name-only origin/master | grep -x 'recsys-pipeline/frontend/data/dashboard.json' && echo "FAIL: snapshot changed" || echo "snapshot untouched"
git -C .. diff --check && echo "diff --check clean"
```

Expected: `581 passed, 2 skipped`; `15 passed`; `dashboard.json valid: 7 measurement sections, schema 2.0`; `snapshot untouched`; `diff --check clean`.

- [x] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py \
        recsys-pipeline/frontend/README.md
git commit -m "$(cat <<'MSG'
test: guard the catalogue, the sidebar and the page header

Four assertions from #255 and #256 described designs this change removes --
sections mounted in page files, and sections that collapse. Replacing the
module says so, rather than leaving assertions that pass for the wrong reason.

Eleven now: the catalogue lists thirteen, the registry and catalogue agree,
every section prerenders, an unknown pair 404s, the sidebar links come from
SECTION_ROUTE and it never imports the component registry, the header names the
section, cards are flat and do not repeat the title, measurement figures
survive, and every CSS variable resolves.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

## Self-Review

**Spec coverage.** The catalogue → Task 1. The dynamic route and registry → Task 2. The sidebar and two-column layout → Task 3. The flat card and the title move → Task 4. The guards and README → Task 5. Spec acceptance items 1-8 map to Task 2 step 3 (item 1), Task 5 step 2 (items 2, 6), Task 5 step 4 (items 3, 4, 5, 7, 8).

**Placeholders.** None. Every replacement asserts its match count; the CSS excisions slice between literal comment markers that exist in the file today.

**Type consistency.** `GROUPS`, `SECTIONS`, `SECTION_ROUTE` are defined in Task 1 and imported by name in Tasks 2, 3 and 5. `SECTION_COMPONENTS` is defined in Task 2 and read by Task 5's guard. `Sidebar` is defined in Task 3 and used only by `layout.jsx`. The thirteen keys are identical in Task 1's catalogue, Task 2's registry and Task 5's `SECTION_KEYS`.

**Two risks worth stating.** Next 15 made `params` a promise in async server components; Task 2's page awaits it, and a build error naming `params` means that assumption is wrong for this version — check before changing anything else. And Task 3 and Task 4 both excise CSS by slicing between comment markers; if either marker has drifted the slice will raise rather than silently cut the wrong range, which is the intent.
