# Collapse every dashboard section to its headline design

## Problem and decision

PR #255 split the dashboard into three routes, which shortened `/` and `/offline` but left
`/online` stacking nine full-height sections top to bottom. Each section renders a heading, a
sample-size line, a metric grid, one or two charts and a table, so nine of them is several screens
of scrolling to reach the last one, and the page never shows what it contains -- the structure is
implied by scroll order.

Make every section collapsible, collapsed by default, with its headline visible in the collapsed
row. A route then reads as an outline: one row per section carrying that section's key number, with
the group headings from #255 still separating them. Nine rows fit on one screen, and expanding a
section is a click.

The collapsed row is not empty. Every section already computes a `headline` string -- "CTR 4.2%",
"best 'model:qValue' value 0.42 vs logging 0.31 · est AUC 0.71" -- which today renders inside the
card as `.insight`. Moving it into the summary means the collapsed page is itself a readable
report, not a table of contents that hides the numbers.

Use native `<details>` and `<summary>`. The open/closed state, the disclosure triangle, keyboard
operation and screen-reader semantics all come from the browser, so no section needs to become a
client component and no state has to be managed.

## What this does and does not claim

It claims every section that renders through `Section` becomes collapsible in one change. All
thirteen do: `measurements.jsx` routes its seven through `MeasurementSection`, `diagnostics.jsx`
calls `Section` five times, and `keyword-report.jsx` calls it once. Nothing else renders a card.

It claims the scorecard's tiles keep working. They link to `/online#satisfaction` and friends, and
a collapsed `<details>` with that id would otherwise leave a reader on a closed row. `Nav` -- which
is already a client component for its active-link state -- gains a small effect that opens the
`<details>` matching `location.hash` on mount and on `hashchange`. The pages and all thirteen
sections stay server components.

It does not claim `NaCard` changes. A section with no measurement renders a title and a reason;
there is nothing to expand, and making it a disclosure that opens to nothing would be worse than
leaving it flat.

It does not change what any section computes or displays once open. Same columns, same charts, same
fine print, same ids. What moves is the headline, from inside the card to its summary row.

It does not claim the dashboard has only one client component. It has two: `nav.jsx`, and
`keyword-report.jsx`, whose Top-K selector uses `useState`. The design document for #255 said
"only components/nav.jsx is a client component", which was wrong and is corrected here.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Four commits, reviewable independently and in this order: the disclosure, the anchor behaviour,
  the guard, the documentation.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `validate_measurements.mjs`, `export_dashboard_json.py` and every Python compute function are
  untouched.
- `Section` keeps its exact prop list -- `title`, `headline`, `description`, `actions`, `id`,
  `children` -- and every call site is unchanged. The change is entirely inside its body.
- `NaCard`, `MetricTile`, `MetricGrid`, `MetricCard`, `ChartGrid`, `BarChart`, `GroupedBarChart`
  and `DataTable` keep their current markup.
- Section `id`s keep their current values, because the scorecard's tile hrefs and
  `test_dashboard_routes.py` depend on them.
- The six contract tests in `test_dashboard_measurement_contract.py` parse component *call sites*
  -- `title="Relevance"` followed by `columns={[...]}` -- which this change does not touch. They
  must keep passing without modification.
- `.report-card` keeps its class name; the summary and body get new classes rather than replacing it.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.

## Implementation

### Commit 1 -- the disclosure

Rewrite `Section` in `components/ui.jsx`. It currently renders:

```
<section className="report-card" id={id}>
  <div className="section-heading"> … h2, headline, description, actions … </div>
  <div className="section-body">{children}</div>
</section>
```

It becomes a `<details className="report-card" id={id}>` whose `<summary className="section-summary">`
holds the `h2` and the headline, with `description` and `actions` moving into the body where they
belong once opened. The body keeps `.section-body`.

`globals.css` gains rules for `.section-summary` -- flex row, pointer cursor, the headline in the
muted colour to the right of the title -- and hides the default marker in favour of a rotating
triangle drawn with `summary::before`, so the control reads the same in both browsers' default
styles. `.report-card` keeps its padding and border; `details > summary` gets the padding instead of
`.section-heading`, which no longer wraps the title.

### Commit 2 -- the anchor behaviour

`Nav` gains an effect: read `location.hash`, find `document.getElementById(hash.slice(1))`, and if
it is a `<details>`, set `open`. Run it on mount and on `hashchange`. A tile click then scrolls to
an open section exactly as it did before #255 collapsed anything.

This lives in `Nav` rather than a new component because `Nav` is already a client component and is
rendered on every route by the shared layout. Adding a second client boundary to do eight lines of
DOM work would cost a bundle entry for no benefit.

### Commit 3 -- the guard

Extend `integration-tests/python_modeling/test_dashboard_routes.py`:

- `Section` renders a `<details>` with a `<summary>`, so sections are collapsible. A regression to
  `<section>` would silently restore the stacked page.
- The summary carries `{headline}`, so a collapsed row still shows its number. Collapsing without
  the headline would turn the page into a contents list.
- `NaCard` still renders a plain `<section>` -- it is deliberately not a disclosure.
- `Nav` opens the `<details>` named by the hash, so the scorecard's tiles keep working.

These are text assertions over the component sources, which is what this repository can check
without a JSX test harness. The limits section says what that does not cover.

### Commit 4 -- the documentation

`frontend/README.md` gains a paragraph under Routes: sections are collapsed by default and show
their headline; a scorecard tile opens the one it targets. The structure listing is unchanged, since
no files are added or removed.

## Validation and acceptance

1. `cd frontend && npm run build` succeeds for all three routes.
2. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline at this branch point is
   574 passed, 2 skipped. This change adds four tests, so the expected result is 578 passed,
   2 skipped, zero failures.
3. The six contract tests in `test_dashboard_measurement_contract.py` pass **unmodified**.
4. `test_dashboard_routes.py`'s existing four assertions pass unmodified: ids and mounting are
   unchanged.
5. Against a running dev server, `/online` returns nine `<details` elements and `/offline` four,
   and every one carries the id its route map names.
6. `cd frontend && npm run validate:data` passes against the unregenerated snapshot.
7. `git diff --name-only origin/master` lists no `frontend/data/dashboard.json`.
8. `git diff --check` clean.

## Limits

The guard asserts that `Section` renders a disclosure and that the summary interpolates `headline`.
It cannot assert that the result is *usable*: a summary whose headline is an empty string, or a
`details` whose body throws, passes every check here. The repository has no JSX rendering harness,
so "it looks right" remains a human judgement, verified this time by fetching the rendered pages
from a dev server and counting elements.

Collapsed-by-default means the first thing a reader sees on `/online` is nine numbers and no charts.
That is the point, and it is a trade: someone who wants every chart at once now clicks nine times.
No "expand all" control is included, because the scorecard already exists for the overview case and
a second global control is speculative until someone wants it.

The open state is not remembered. Navigating away and back re-collapses everything, because
`<details>` has no persistence and adding it would mean either `localStorage` -- which this
dashboard uses nowhere -- or a state parameter in the URL. Both are more machinery than the problem
justifies today.

`keyword-report.jsx` carries its own copy of the `num` formatter that `format.js` now exports, left
over from before the split in #255. It is untouched here: deduplicating it is unrelated to
collapsing sections, and this change does not make it worse.
