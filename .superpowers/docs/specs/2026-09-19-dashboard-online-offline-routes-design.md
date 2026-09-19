# Split the dashboard into online and offline prediction analysis design

## Problem and decision

`frontend/app/page.jsx` mounts thirteen sections in one column: a seven-tile scorecard followed by
relevance, satisfaction, freshness, diversity, fairness, safety, latency, engagement, keyword, query,
recall, ranking and off-policy evaluation. Reading any one of them means scrolling past the rest, and
there is no way to send someone to a particular one -- the scorecard's tiles anchor within the same
page, so every link is `#relevance`, `#safety`, and so on.

Split it along the axis that distinguishes what the numbers mean: **did this number describe what the
serving path actually did, or what a model would have done if re-scored afterwards.**

That line is not the same as the data's provenance, and the difference matters enough to state.
Only `latency` comes purely from live telemetry -- `_live_latency` reading the backend's `/metrics`.
`satisfaction`, `freshness` and `safety` are computed offline from Parquet with live rows merged in by
`_merge_live_row` when a backend was running. Everything else, including query intents and the
off-policy arms, is computed from logged Parquet and Redis. So "online" here means *analysis of the
served traffic*, not *data that arrived live*.

Three routes:

| Route | Contents |
|---|---|
| `/` | The scorecard, seven tiles, each linking into the page that holds its section |
| `/online` | Intents, engagement, satisfaction, freshness, safety, latency, diversity, fairness, keyword |
| `/offline` | Recall, ranking, off-policy evaluation, relevance |

Nine sections on `/online` is still a long page, so it carries three subheadings -- demand, served
quality, serving health -- and `/offline` carries two: retrieval and ranking, then policy. Subheadings
inside a route are cheaper than more routes and keep related sections adjacent.

## What this does and does not claim

It claims every one of the thirteen sections is mounted on exactly one page after this change, and
that a test fails if one is mounted twice or not at all. Moving components between files is how a
section gets silently dropped, and nothing in the repository would currently notice.

It claims the scorecard's tiles still reach their sections. They currently use bare `#key` anchors,
which resolve within one page; with the sections spread over two routes, six tiles must point at
`/online#key` and one -- relevance -- at `/offline#relevance`. Those hrefs are derived from a single
map rather than written per tile, so a section moving pages cannot leave a tile pointing at the wrong
one.

It does not claim the data contract changes. `frontend/data/dashboard.json` is not regenerated and not
edited, `validate_measurements.mjs` is untouched, and the schema stays `"2.0"`. The validator checks
the JSON's seven measurement sections, which is unaffected by which page renders them -- and note that
this change does put one of those seven, relevance, on a different page from the other six. The
"seven measurement sections" contract is about the payload, not the layout.

It does not claim the visual design changes. Every section renders exactly the components it renders
today, with the same columns, charts and fine print. What changes is which page each one lives on, plus
a nav and two levels of heading.

It does not restyle `globals.css` beyond adding nav and subheading rules. The existing design system --
`page-shell`, `report-grid`, `report-card`, `scorecard`, `metric-tile` -- is reused as-is.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Five commits, reviewable independently and in this order: the shared mapping and the component
  split, the routes and nav, the scorecard hrefs, the contract tests, the documentation.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `validate_measurements.mjs`, `export_dashboard_json.py` and every Python compute function are
  untouched. This change is JSX, CSS, one JS module, and tests.
- `components/ui.jsx` keeps every export and signature it has, including `MetricTile`'s `href` prop.
- `components/keyword-report.jsx` is not split or moved; `/online` imports `KeywordSection` from it.
- Every section component keeps its exact exported name, so the guard test and the contract tests can
  find it by name: `RelevanceSection`, `SatisfactionSection`, `FreshnessSection`, `DiversitySection`,
  `FairnessSection`, `SafetySection`, `LatencySection`, `EngagementSection`, `QuerySection`,
  `RecallSection`, `RankingSection`, `OpeSection`, `KeywordSection`, `Scorecard`.
- `HEADLINES`, `TITLES` and `LOW_COVERAGE` keep their exact names and shapes: six of the contract
  tests parse them out of the component sources by name.
- Only `components/nav.jsx` is a client component. The pages and every section stay server components.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.

## Implementation

### Commit 1 -- the shared mapping and the component split

Create `components/groups.js`, the single source of truth for where each section lives:

```js
export const ROUTES = [
  { href: "/", label: "Overview" },
  { href: "/online", label: "Online prediction analysis" },
  { href: "/offline", label: "Offline prediction analysis" },
];

// Which route renders each section. The scorecard reads this to build its tile hrefs, the nav
// reads the labels, and the guard test reads it to check every section is mounted exactly once.
export const SECTION_ROUTE = {
  satisfaction: "/online", freshness: "/online", safety: "/online", latency: "/online",
  diversity: "/online", fairness: "/online", query: "/online", keyword: "/online",
  engagement: "/online",
  relevance: "/offline", recall: "/offline", ranking: "/offline", ope: "/offline",
};
```

Split `components/sections.jsx`, which is 626 lines and of which each route needs a part:

- `components/format.js` -- `num`, `pct`, `share`, `ci`, `count`, `rankBy`, `maxByField`,
  `COUNT_COLUMNS`, `RATE_COLUMNS`. Pure formatting, no JSX.
- `components/scorecard.jsx` -- `HEADLINES`, `TITLES`, `LOW_COVERAGE`, `headlineRow`,
  `headlineFieldPublished`, `headlineValue`, `Scorecard`.
- `components/measurements.jsx` -- `MeasurementSection` and the seven sections built from it
  (lines 149-463 today).
- `components/diagnostics.jsx` -- `EngagementSection`, `QuerySection`, `RecallSection`,
  `RankingSection`, `OpeSection` (lines 464-626 today).

`components/sections.jsx` is deleted. Nothing imports it after this commit except the contract tests,
which commit 4 updates.

### Commit 2 -- the routes and the nav

`app/layout.jsx` becomes the shared shell: it imports the snapshot, renders the hero with the row
count and input path that `page.jsx` renders today, renders `<Nav />`, and renders `{children}`.

`components/nav.jsx` is a client component -- `"use client"` plus `usePathname` -- rendering one link
per entry in `ROUTES` with `aria-current="page"` and a `nav-active` class on the match. It is the only
client component in the app.

Three pages, each importing only what it renders:

- `app/page.jsx` -- `<Scorecard data={data} />` and nothing else.
- `app/online/page.jsx` -- three subheadings. Demand: `QuerySection`, `KeywordSection`,
  `EngagementSection`. Served quality: `SatisfactionSection`, `FreshnessSection`, `DiversitySection`,
  `FairnessSection`, `SafetySection`. Serving health: `LatencySection`.
- `app/offline/page.jsx` -- two subheadings. Retrieval and ranking: `RecallSection`, `RankingSection`,
  `RelevanceSection`. Policy: `OpeSection`.

`globals.css` gains `.nav`, `.nav a`, `.nav a.nav-active` and `.group-heading` rules, reusing the
existing colour variables. The `.hero` rules already exist and are reused unchanged.

### Commit 3 -- the scorecard's cross-page links

`Scorecard` builds each tile's href as `` `${SECTION_ROUTE[key]}#${key}` `` instead of `` `#${key}` ``.
Six resolve to `/online#...` and relevance to `/offline#relevance`.

The section ids those anchors target already exist: `MeasurementSection` passes
`id={title.toLowerCase()}` to `Section`, which renders it. No id changes.

### Commit 4 -- the contract tests

`test_dashboard_measurement_contract.py` reads `components/sections.jsx` as text in six places to
check that React's declared columns match what the exporter publishes. That file no longer exists.

Rather than repointing each site at whichever new file holds its assertion, add one module-level
helper that concatenates every `components/*.jsx` and have all six read that. The assertion each test
makes is "the dashboard's components declare this column", not "this one file does", so the helper
states the contract more accurately than the path did -- and survives the next split.

### Commit 5 -- the guard and the documentation

Add `integration-tests/python_modeling/test_dashboard_routes.py`:

- Every key in `SECTION_ROUTE` is mounted on the page its route names, found by scanning
  `app/**/page.jsx` for the component's exported name.
- Every section component exported by `measurements.jsx`, `diagnostics.jsx` and `keyword-report.jsx`
  appears in `SECTION_ROUTE` -- so adding a section without placing it fails.
- No section is mounted on two pages.
- `Scorecard` derives its hrefs from `SECTION_ROUTE` rather than hardcoding `#`-only anchors.

Update `frontend/README.md`: the structure listing names `sections.jsx`, which is gone, and the
"Run" section describes a single page. Both become the three-route layout with the online/offline
split stated.

## Validation and acceptance

1. `cd frontend && npm run build` succeeds, with `/`, `/online` and `/offline` in its route listing.
2. `cd frontend && npm run validate:data` passes unchanged against the unregenerated snapshot.
3. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline at this branch point is
   570 passed, 2 skipped. This change adds four tests, so the expected result is 574 passed,
   2 skipped, zero failures.
4. The six updated contract tests pass without changing what they assert -- only where they read it.
5. `grep -rn 'sections\.jsx'` over live trees returns nothing.
6. Each of the thirteen sections appears in exactly one `app/**/page.jsx`, verified by the guard.
7. `git diff --name-only origin/master` lists no `frontend/data/dashboard.json`.
8. `git diff --check` clean.

## Limits

The split is a judgement about meaning, not a property of the data. `relevance` sits on the offline
page because NDCG and MRR re-score a served slate against realized labels, while `satisfaction` sits
on the online page because CTR counts what happened -- both read the same rows from the same run. A
reader who disagrees with that placement changes one line in `SECTION_ROUTE`, which is the reason the
mapping is a module rather than thirteen hardcoded hrefs.

Nine sections on `/online` is better than thirteen on `/` and is still a long page. The subheadings
group them; they do not shorten it. Splitting further would mean a route per subgroup, which trades
scrolling for clicking and was not what was asked for.

The guard matches component names in page sources as text. It catches a section that is mounted twice
or not at all. It cannot catch a section mounted inside a conditional that never renders, or one
rendered with the wrong `data` prop -- `<RecallSection data={data.ranking} />` passes every assertion
here. Only a rendering test would catch that, and the repository has no JSX test harness.

Concatenating the component sources for the contract tests introduces a boundary the single-file read
did not have. Those tests use regexes like `title="Relevance"([\s\S]*?)\n    />`, which could in
principle match across a join if one file ended mid-pattern. In practice each section's opening tag and
its closing `/>` sit in the same file, so every match stays local -- but a future split that separates
a component's tag from its props would break the assumption silently rather than loudly. The files are
joined with newlines to keep line-anchored patterns honest.

`npm run build` proves the pages compile and prerender. It does not prove they look right. The visual
result is unverified by anything automatic, which is the standing situation for this frontend.
