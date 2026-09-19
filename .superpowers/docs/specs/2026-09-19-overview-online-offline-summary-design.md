# An overview covering both groups design

## Problem and decision

The overview shows seven tiles, one per measurement section. Six of those sections are online and one
— relevance — is offline, so the page reads as a summary of the online half with one offline figure
mixed in, and the six diagnostic sections are absent entirely. A reader who wants to know how
retrieval, ranking or off-policy evaluation is doing has to visit three pages to find out.

Show all thirteen, grouped the way the sidebar groups them: **Online prediction** with nine tiles,
**Offline prediction** with four, in the order `components/groups.js` already defines.

## Where the six new figures come from

The seven measurement sections share one envelope — `status`, `sampleSize`, `coverage`, `rows` — which
is why one `HEADLINES` spec covers them all. The six diagnostics do not share a shape, so each needs
its own rule. All six figures exist in the snapshot today:

| Section | Figure | Source | Support |
|---|---|---|---|
| Engagement funnel | CTR | `ctr` scalar | `funnel.impression` |
| Intents | average query length | `average_query_length` scalar | rows in `by_length` |
| Keyword gap | largest divergence | max over `by_keyword` | rows in `by_keyword` |
| Candidate recall | best recall@k | max over `rows` | `users_evaluated` of that row |
| Ranking quality | best AUC | max over `rows` | `n` of that row |
| Off-policy evaluation | best lift vs logging | max over `rows` | `n_events` of that row |

Two are scalars read directly; four are a maximum over a named row set. The row set differs by section
— `rows` for three of them, `by_keyword` for one — so the spec names the path rather than assuming it.

## What this does and does not claim

It claims the overview covers every section. A guard asserts that each key in `SECTIONS` has a tile,
so adding a fourteenth section later cannot silently skip the summary.

It claims the tiles stay honest about absence. A diagnostic section serialises as `null` when its
input was missing, and its tile then reads `N/A` with the reason, exactly as an unavailable
measurement does today. Nothing computes a figure from a missing input.

It does not claim the off-policy tile will read usefully. With the committed snapshot its only policy
is `logging`, compared against itself, so the tile shows `best lift +0.0%`. That is the data being
truthful — the section is reachable but unpopulated, as documented in #258 — and it will mean
something once a backend-fed replay buffer exists. Suppressing the tile because its current value is
dull would hide the same fact the section already states.

It does not change any section page, any computation, or the exporter. The figures are read from the
snapshot the dashboard already ships.

It does not add a group-level roll-up — no "7 of 9 online sections healthy" line. That would need a
definition of healthy that this repository does not have, and inventing one for a summary line is the
kind of fabricated measure the dashboard avoids elsewhere.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Three commits: the diagnostic spec, the grouped overview, the guard.
- Only `frontend/components/scorecard.jsx`, `frontend/app/page.jsx` and the guard test change.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `HEADLINES`, `TITLES` and `LOW_COVERAGE` keep their exact names and shapes: six contract tests parse
  them out of the component sources by name.
- `MetricTile` keeps its current props and markup.
- Every section keeps its `SECTION_ROUTE` link, so a tile still opens that section's page.
- The overview stays a server component.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.

## Implementation

### Commit 1 -- the diagnostic spec

`scorecard.jsx` gains `DIAGNOSTICS`, a map from section key to `{ label, format }` plus either
`scalar` — a top-level numeric field — or `rows` and `field`, naming a row set to take the maximum of.
A `support` key names the field carrying the tile's `n=`, read from the same row for a row-based spec.

A `diagnosticTile(section, spec)` helper returns `{ value, sampleSize }` or `null` when the section is
absent or the field never appears, so a missing input produces an N/A tile rather than a zero.

### Commit 2 -- the grouped overview

`Scorecard` renders one block per entry in `GROUPS`, each with the group's label and the tiles for the
sections `SECTIONS` assigns to it, in that file's order. A section is rendered from `HEADLINES` when it
has an entry there and from `DIAGNOSTICS` otherwise.

`app/page.jsx`'s header stops saying "Seven measurement envelopes at a glance" -- it is thirteen
sections across two groups now.

`globals.css` gains a heading rule for the group label above each tile block, reusing the existing
`.sidebar-label` treatment rather than inventing a second small-caps style.

### Commit 3 -- the guard

`test_dashboard_routes.py` gains an assertion that every key in `SECTIONS` appears in either
`HEADLINES` or `DIAGNOSTICS`, parsed out of `scorecard.jsx`. A section added to the catalogue without a
tile fails by name.

## Validation and acceptance

1. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline is 581 passed, 2 skipped; this
   adds one test, so the expected result is 582 passed, 2 skipped.
2. The six contract tests pass **unmodified**.
3. `cd frontend && npm run build` succeeds.
4. Against a running dev server, `/` renders thirteen tiles under two group headings, and each tile's
   href matches `SECTION_ROUTE` for its section.
5. The off-policy tile renders rather than being omitted, showing its current value.
6. `cd frontend && npm run validate:data` passes against the unregenerated snapshot.
7. `git diff --name-only origin/master` lists no `frontend/data/dashboard.json`.
8. `git diff --check` clean.

## Limits

Six of the thirteen figures are a maximum over a row set, and a maximum is a summary that hides its
distribution. "Best AUC 0.560" says nothing about the two signals that scored lower, and "best
recall@k" collapses three methods at three values of k into one number. The tile links to the section
that shows the rest, which is the whole reason a tile is allowed to be reductive.

The support counts are not comparable across tiles. A measurement tile's `n` is the sample the envelope
was computed from; the recall tile's is users evaluated; the off-policy tile's is replay events. They
share a visual treatment and mean different things, which the section pages disambiguate and the tiles
do not.

Nothing tests that a figure is *correct*, only that it renders. The guard checks coverage — every
section has a tile — not arithmetic. A wrong field name in a spec would produce an N/A tile, which is
visible, but a right-shaped wrong field would produce a plausible wrong number silently.
