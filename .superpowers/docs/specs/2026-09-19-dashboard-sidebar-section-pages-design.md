# A sidebar and a page per section design

## Problem and decision

Three iterations have not solved the complaint. #255 split thirteen sections across three routes,
which left `/online` holding nine. #256 collapsed each section to a headline row, which made that
page short but turned reading any section into a click, and left the route still holding nine
things. The complaint each time was the same: a page that is a pile of sections.

The reference design settles it. A persistent left sidebar lists every destination, grouped and
labelled; the main area shows exactly one thing, introduced by an eyebrow, a title and a line of
description. Nothing stacks and nothing collapses, because each page holds one section.

Thirteen sections become thirteen pages under two groups, plus the overview:

| Route | Sidebar group | Shows |
|---|---|---|
| `/` | — | The scorecard |
| `/online/query` … `/online/latency` | Online prediction | One section each: intents, keyword, engagement, satisfaction, freshness, diversity, fairness, safety, latency |
| `/offline/recall` … `/offline/ope` | Offline prediction | One section each: recall, ranking, relevance, off-policy |

## What this reverses from #256, and why

#256 made `Section` a `<details>` disclosure whose summary carried the section's figure. On a page
holding one section that is strictly worse: the page would open as a single collapsed row, and
seeing anything at all would take a click. The disclosure is reverted to a flat card.

Two things #256 added are kept. The `--border` fix and its guard stay, because that defect was real
and unrelated to layout. The work that gave the seven measurement sections a figure of their own --
`HEADLINES`, `headlineFieldPublished` and `headlineValue` exported from `scorecard.jsx` and consumed
by `MeasurementSection` -- also stays, but the figure moves from a summary row into the page header,
where it sits beside the title as the reference design's subtitle does.

`Nav`'s hash-opening effect goes with the disclosure: there is nothing to open, and a tile now links
to a page rather than to a fragment.

Reverting one's own merged work two commits later is worth stating rather than quietly undoing. The
disclosure was the right answer to "nine sections on one page" and the wrong answer to "one section
per page", and the second framing is the one the reference design uses.

## What this does and does not claim

It claims thirteen pages are generated from one file. `app/[group]/[section]/page.jsx` resolves the
section from the URL and renders it, with `generateStaticParams` enumerating every valid pair from
the registry, so all thirteen prerender as static content exactly as the three routes do today.
Thirteen near-identical page files would be thirteen places to forget something.

It claims the sidebar stays a small client bundle. It needs labels and hrefs, not components, so the
catalogue splits in two: `components/groups.js` holds pure data -- group labels, section labels,
descriptions, routes -- and is what the sidebar imports; `components/section-registry.jsx` maps each
key to its component and is imported only by the server page. A sidebar that imported the registry
would pull every chart and table into the client bundle.

It claims the page header owns the section's title. `Section` keeps `title` in its signature, because
six contract tests parse `title="Relevance"` and similar out of the call sites, but stops rendering
it: the header above the card shows the eyebrow, the title and the description, and the card shows
the headline, the figure and the content. Without that change the title would appear twice on every
page.

It does not claim the sections themselves change. Same computations, same columns, same charts, same
ids, same `data` props. What changes is the chrome around them and how many share a page.

It does not add sorting, filtering or any other affordance visible in the reference design. Those are
features, not structure, and this change is about structure.

It makes no claim about `frontend/data/dashboard.json`, which is not regenerated and not edited.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Five commits, reviewable independently and in this order: the catalogue, the dynamic route, the
  sidebar and layout, the flat card, the guards and documentation.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `validate_measurements.mjs`, `export_dashboard_json.py` and every Python compute function are
  untouched.
- The six contract tests in `test_dashboard_measurement_contract.py` must keep passing
  **unmodified**: they parse component call sites, which do not change.
- Every section component keeps its exact exported name and its `data` prop.
- Section `id`s keep their current values.
- `SECTION_ROUTE` keeps its name and its key set, because `scorecard.jsx` and the guard tests read
  it; its values change from `"/online"` to `"/online/satisfaction"` and so on.
- `components/groups.js` imports nothing. It is the only module the sidebar needs.
- The CSS custom-property guard added in #256 keeps passing: every `var()` in `globals.css` resolves
  to a declaration in `:root` or an inline JSX style.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.

## Implementation

### Commit 1 -- the catalogue

`components/groups.js` becomes the full catalogue and still imports nothing:

- `GROUPS` -- an ordered list of `{ key, label }`: `online` → "Online prediction", `offline` →
  "Offline prediction".
- `SECTIONS` -- each section key mapped to `{ group, label, description }`. The label is what the
  sidebar and the page title show ("Intents", "Satisfaction"); the description is the line beneath
  the title.
- `SECTION_ROUTE` -- derived from `SECTIONS`, mapping each key to `/${group}/${key}`.

`scorecard.jsx` drops the fragment from its tile hrefs: `SECTION_ROUTE[key]` is now a page, so
`` `${SECTION_ROUTE[key]}#${key}` `` would append a redundant hash.

### Commit 2 -- the dynamic route

`components/section-registry.jsx` maps each key to its component, importing from `measurements.jsx`,
`diagnostics.jsx` and `keyword-report.jsx`.

`app/[group]/[section]/page.jsx` looks the key up, renders `notFound()` when the pair is unknown,
and otherwise renders the header -- eyebrow from the group's label, `h1` from the section's label,
description beneath -- followed by the section component with `data={snapshot[key]}`.
`generateStaticParams` returns every `{ group, section }` pair from `SECTIONS`.

`app/online/page.jsx` and `app/offline/page.jsx` are deleted: their routes are now group prefixes,
not pages.

### Commit 3 -- the sidebar

`components/nav.jsx` becomes `components/sidebar.jsx`: a brand block, then `Overview`, then one
labelled group per `GROUPS` entry with its sections beneath. It stays a client component for the
active-link state and drops the hash-opening effect.

`app/layout.jsx` becomes a two-column shell -- sidebar beside a main column -- replacing the
centred single column. The hero moves into the sidebar's brand block, which is where the reference
design puts it.

`globals.css` gains the sidebar, the page header and the two-column grid, and loses the `.nav`
rules. At the narrow breakpoint the sidebar becomes a horizontal strip above the content, because a
fixed 240px column on a phone leaves nothing for the table.

### Commit 4 -- the flat card

`Section` reverts from `<details>` to `<section className="report-card">` and stops rendering
`title`. It renders the headline and the figure in a heading block, then the body. The
`.section-summary` rules are replaced by heading rules; the disclosure marker rules go.

### Commit 5 -- the guards and the documentation

`test_dashboard_routes.py` changes shape with the routes it guards. Four assertions from #255 and
#256 no longer describe the design and are replaced rather than deleted silently:

- every key in `SECTIONS` resolves to a component in the registry, and every registry entry appears
  in `SECTIONS` -- the mounting check, now that no `page.jsx` names a component;
- `generateStaticParams` enumerates all thirteen, so every sidebar link prerenders;
- the sidebar links to every section exactly once;
- the page header renders the section's label and description, and the measurement figure;
- `Section` is a flat card again, with the reason recorded;
- the scorecard's tile hrefs come from `SECTION_ROUTE` and carry no fragment.

The CSS variable guard and the four assertions that still hold are kept unchanged.

`frontend/README.md`'s Routes section is rewritten for the sidebar and the page-per-section layout.

## Validation and acceptance

1. `cd frontend && npm run build` lists fourteen prerendered routes: `/` plus thirteen
   `/online/*` and `/offline/*`.
2. Against a running dev server, each of the thirteen returns 200 and contains exactly one
   `.report-card`, and `/online/nonsense` returns 404.
3. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline at this branch point is
   580 passed, 2 skipped. The plan records the expected count, which is not simply higher: this
   change replaces assertions as well as adding them.
4. The six contract tests pass **unmodified**.
5. `cd frontend && npm run validate:data` passes against the unregenerated snapshot.
6. Every section id is still present on the page that renders it.
7. `git diff --name-only origin/master` lists no `frontend/data/dashboard.json`.
8. `git diff --check` clean.

## Limits

Thirteen pages means thirteen clicks to see everything, where the single page needed one scroll.
That is the trade the reference design makes, and it is only the right trade if sections are read one
at a time rather than compared side by side. The scorecard remains the one place that shows seven
measurements together.

`generateStaticParams` makes every page static at build time, which is correct for a committed
snapshot and would need revisiting if the dashboard ever read live data per request.

The guards check the registry, the sidebar and the header as text. They cannot check that a page
*looks* like the reference design, that the sidebar is legible at a narrow width, or that the header
reads well. Those remain human judgements, made against a running dev server.

This is the third structural change to the same surface in one session. The sections underneath have
not changed at all, and each iteration has been chrome: three routes, then disclosures, now a sidebar
and a page each. Whatever the next complaint is, it is worth asking whether it is about structure or
about what the sections say, because structure has now been tried three ways.
