# Group the sections by the question they answer design

## Problem and decision

The sidebar groups thirteen sections as "Online prediction" and "Offline prediction". Three of the
nine under Online are not predictions at all: **Intents** is what users searched for, **Keyword gap**
is catalog supply against query demand, and **Engagement funnel** is observed impression → click →
order. None involves a model predicting anything. **Latency** is not a prediction either; it is
serving health.

The axis was chosen in #255 as "what the serving path did versus what a model would have done
re-scored", which is defensible for the ten sections that are about serving or models. The other
three were stretched to fit, and the group name then claimed something false about them.

Group by the question a reader arrives with instead:

| Group | Route prefix | Sections |
|---|---|---|
| Demand & content | `/demand` | Intents, Keyword gap |
| Serving & outcomes | `/serving` | Engagement funnel, Satisfaction, Freshness, Diversity, Fairness, Safety, Latency |
| Model evaluation | `/models` | Candidate recall, Ranking quality, Relevance, Off-policy evaluation |

No group asserts that its members are predictions. "Demand & content" says where the questions come
from, "Serving & outcomes" covers what was shown and what followed, and "Model evaluation" is the
only group about models scoring anything.

## What this does and does not claim

It claims the change is confined to the catalogue and one documentation table. `components/groups.js`
is the single source of truth introduced in #257: the sidebar renders `GROUPS`, the dynamic route
enumerates `SECTIONS`, `SECTION_ROUTE` is derived, the overview groups by the same map, and each page
header reads the group's label. Changing the catalogue moves all of it. Grepping for the old names
finds them in `groups.js` and `frontend/README.md` and nowhere else.

It claims the guard tests need no edit. They parse `SECTIONS` for group membership generically rather
than naming any group, so they hold across the rename — which is the property the #257 design was
after and this change tests for the first time.

It does not claim the old axis was wrong for everything it covered. Satisfaction, freshness,
diversity, fairness and safety really are properties of what was served, and recall, ranking,
relevance and off-policy really are models re-scored. Those ten keep their relative grouping; the
change is that the three descriptive sections stop being filed under a label that calls them
predictions, and that the group names stop over-claiming.

It does not change any URL a reader has bookmarked in a way that redirects. `/online/query` becomes
`/demand/query` and the old path 404s. Nothing outside this repository links to these routes, and the
dynamic route already returns `notFound()` for an unknown pair, so the failure is loud rather than a
blank page.

It does not change what any section computes, displays, or is called. Only which group it sits in.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Two commits: the catalogue, then the documentation.
- Only `frontend/components/groups.js` and `frontend/README.md` change.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- Every section keeps its key, its label, its description and its component.
- `GROUPS`, `SECTIONS` and `SECTION_ROUTE` keep their exported names and shapes; `SECTION_ROUTE` stays
  derived from `SECTIONS` rather than written out.
- `groups.js` keeps importing nothing — the sidebar is a client component.
- No test file is edited. If a guard needs changing, that is a finding about the guard, not a step in
  this change.

## Implementation

### Commit 1 -- the catalogue

`GROUPS` becomes three entries: `demand` → "Demand & content", `serving` → "Serving & outcomes",
`models` → "Model evaluation". Each section's `group` is reassigned per the table above.

The file's header comment currently explains the online/offline distinction and the live-telemetry
caveat that went with it. It is rewritten to explain the new axis, and keeps the caveat where it
still belongs: latency is the only section sourced purely from live telemetry, and satisfaction,
freshness and safety merge live rows into offline ones when a backend is running. That fact did not
stop being true; it stopped being the organising principle.

Section order within `SECTIONS` follows the new groups, so the sidebar and the overview read in the
same order as the table above.

### Commit 2 -- the documentation

`frontend/README.md`'s Routes table lists `/online/<section>` and `/offline/<section>` with the old
group names. It becomes the three routes with their memberships, and the sentence about latency being
the only purely live section moves with the caveat it explains.

## Validation and acceptance

1. `python3 -m pytest -q` from `recsys-pipeline` gives 583 passed, 2 skipped — unchanged, and with no
   test edited. A failure here is a guard that hardcoded a group name, which would be worth fixing on
   its own terms rather than by editing the test to match.
2. `cd frontend && npm run build` prerenders thirteen section pages under `/demand`, `/serving` and
   `/models`.
3. Against a running dev server, the sidebar shows three labelled groups totalling thirteen links,
   `/demand/query` returns 200, and `/online/query` returns 404.
4. The overview shows three tile blocks summing to thirteen tiles.
5. `grep -rn '/online\|/offline' frontend/` returns nothing outside `.next`.
6. `cd frontend && npm run validate:data` passes against the unregenerated snapshot.
7. `git diff --name-only origin/master` lists exactly two frontend files.
8. `git diff --check` clean.

## Limits

"Serving & outcomes" carries seven of the thirteen sections, so it is doing more work than the other
two. Splitting it into what was served (freshness, diversity, safety, latency) and what happened
(engagement, satisfaction, fairness) was considered and declined: four groups for thirteen items makes
the sidebar mostly headings, and the split is not obvious for fairness, which is an outcome measured
across a serving property.

Grouping by question is a judgement about how a reader thinks, and this is the second such judgement
for the same thirteen sections. If it is wrong again, the cost is one file — which is the point of
the catalogue, and worth more than getting the grouping right first time.

The old routes 404 rather than redirecting. Next.js can redirect from `next.config.js`, but a redirect
for URLs that existed for part of one afternoon is machinery for nobody.
