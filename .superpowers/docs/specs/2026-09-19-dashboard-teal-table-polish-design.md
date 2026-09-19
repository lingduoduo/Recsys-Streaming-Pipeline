# Teal accent and denser tables design

## Problem and decision

The dashboard's structure now matches the reference design shared in this session -- a grouped
sidebar, a page per section, an eyebrow and title above one card. Its visual treatment does not.
The reference uses a teal accent, small-caps column headers, right-aligned tabular numerics, an
em-dash for a missing cell, and rows tight enough to show roughly eighteen at a glance. This
dashboard uses an indigo accent, sentence-case headers, left-aligned numbers, the string `N/A`, and
comfortable row spacing.

Adopt the treatment. Nothing structural changes: no route moves, no section changes what it
computes or displays, and no new interactivity appears.

Two things the reference shows that this change does **not** adopt, because they were considered and
declined: sortable column headers, which would make `DataTable` a client component when every
section is server-rendered today; and a table-first layout that demotes the charts, which would bury
content the sections were built around.

## What this does and does not claim

It claims the accent rename is complete. `--indigo` and `--indigo-soft` become `--accent` and
`--accent-soft`, and the CSS custom-property guard added in #256 fails the build's test suite if any
`var(--indigo…)` survives -- so "complete" is checked rather than asserted.

It claims the chart palette is deliberately untouched. `--series-0` through `--series-2` are a
designed categorical set whose job is to distinguish series from one another, not to match the
accent. The reference design contains no charts and so offers no guidance; changing them would be a
separate decision made without evidence.

It claims only table cells become em-dashes. `num()` in `format.js` keeps returning the string
`"N/A"`, because metric cards and `NaCard` state absence deliberately -- this repository's rule is
that nothing invents a value for a missing measurement, and an explicit `N/A` on a headline card
says that more clearly than a dash. Inside a dense table the dash reads better and means the same.

It does not claim the dashboard will look like the reference. The reference is a different product
with different content: its page is one table of thirty-six models, where these pages carry metric
tiles and charts above their tables. This change borrows the treatment, not the content.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Three commits: the accent, the tables, the dead-CSS cleanup.
- Only `frontend/app/globals.css` and `frontend/components/ui.jsx` change. No route, no section
  component, no Python.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- Every section stays a server component. `DataTable` gains no interactivity and no `"use client"`.
- `num()`, `pct()`, `share()`, `ci()` and `count()` in `format.js` are unchanged.
- The `--series-*` chart palette is unchanged.
- The CSS custom-property guard keeps passing: every `var()` resolves to a `:root` declaration or an
  inline JSX style.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.

## Implementation

### Commit 1 -- the accent

In `globals.css`, rename `--indigo` to `--accent` with the value `#0d9488`, and `--indigo-soft` to
`--accent-soft` with `#f0fdfa`. Update the seven `var(--indigo…)` sites. Change `.insight`'s
hardcoded `color: #3730a3` -- indigo ink chosen to sit on the indigo-soft background -- to `#115e59`,
the teal equivalent.

`--series-0` keeps its `#4f46e5`: it belongs to the chart palette, not the accent.

### Commit 2 -- the tables

In `globals.css`: `th` becomes uppercase with letter-spacing in `var(--muted)` at a smaller size;
`td` and `th` padding tightens; a `.num` class right-aligns a cell and applies
`font-variant-numeric: tabular-nums` so digits align down a column.

In `ui.jsx`, `DataTable` marks a cell `.num` when its raw value is a number, and renders `—` rather
than `N/A` when the value is null, undefined or an empty string. Column headers keep their existing
`replaceAll("_", " ")` treatment; the small caps come from CSS, so the header text a contract test
would read is unchanged.

### Commit 3 -- the dead CSS

`.hero`, `.hero h1`, `.hero p` and `.report-badge` have had no markup since #255 moved the hero into
the shared layout and #257 replaced that layout with the sidebar. They are removed. `.eyebrow` is
kept: the page header still uses it.

## Validation and acceptance

1. `python3 -m pytest -q` from `recsys-pipeline` gives 581 passed, 2 skipped -- unchanged, because
   this is CSS and one display string.
2. The CSS custom-property guard passes, proving no `var(--indigo…)` survives the rename.
3. `grep -c 'var(--indigo' frontend/app/globals.css` returns 0; `grep -c 'series-0' ` still finds the
   chart palette.
4. `grep -rn 'hero\|report-badge' frontend/app/globals.css` returns nothing.
5. `cd frontend && npm run build` succeeds for all fourteen routes.
6. Against a running dev server, a section page's rendered HTML contains `#0d9488` or the
   `--accent` variable in use, at least one `class="num"` cell, and at least one `—`.
7. `cd frontend && npm run validate:data` passes against the unregenerated snapshot.
8. `git diff --check` clean, and `git diff --name-only origin/master` lists only the two frontend
   files plus this spec and its plan.

## Limits

Colour is not tested beyond the variable's existence. Nothing checks contrast, and the teal ink on
teal-soft (`#115e59` on `#f0fdfa`) was chosen by analogy with the indigo pair it replaces rather than
measured against a contrast ratio. If it reads poorly, it is a one-line change.

The `.num` alignment keys off `typeof value === "number"` in the raw row, not off the column. A
column whose values arrive as numeric strings will not right-align, and a column mixing numbers with
`null` will right-align only some cells -- though since a null renders as an em-dash, which is
narrow, the effect is slight.

Row density is set by padding alone. On a narrow viewport the table still scrolls horizontally inside
`.table-shell`, exactly as before; this change makes more rows visible, not more columns.
