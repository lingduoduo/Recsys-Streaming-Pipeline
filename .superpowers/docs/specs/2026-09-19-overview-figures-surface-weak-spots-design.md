# Overview figures that surface weak spots design

## Problem and decision

#260 gave each diagnostic section an overview tile. Four of those figures are a maximum over a row
set, and a maximum flatters: it reports the best-performing member and says nothing about the
weakest. On a scorecard whose job is to say where to look, that is the wrong summary.

Reviewing the four against the data shows the fix is not "use the minimum". Two of them would become
*more* misleading that way, one is already wrong for a different reason, and one is correct as it
stands.

| Section | Now | Becomes | Why |
|---|---|---|---|
| Ranking quality | best AUC `0.560` | **worst AUC `0.506`** | AUCs are 0.5058, 0.5319, 0.5595 — a near-random signal is the finding; the best hides it |
| Keyword gap | largest divergence `0.0137` | **largest \|divergence\| `-0.0182`** | divergence is signed, spanning −0.0182 to +0.0137; taking the maximum reports the smaller mismatch |
| Candidate recall | best recall@k `0.070` | **best recall@10 `0.036`** | rows are method × k, and recall rises with k by construction, so the maximum reports k=20 rather than retrieval quality |
| Off-policy | best lift `0.0%` | unchanged | "is any policy better than logging?" is the question the section exists to answer |

## What this does and does not claim

It claims the recall figure stops being a function of `k`. With rows at k ∈ {5, 10, 20} for three
methods, `bm25` alone runs 0.0177 → 0.0351 → 0.0692; any extremum over the unfiltered set is chosen
by its `k`, not by its method. Fixing k at 10 makes the tile a comparison between methods, which is
what a reader assumes they are seeing. Ten is conventional and arbitrary; twenty would be equally
defensible and is a one-line change.

It claims the keyword figure was wrong, not merely flattering. `divergence` is signed — negative is
under-supply, positive over-supply — so the largest mismatch in the committed snapshot is −0.0182
while the tile reports +0.0137. Selecting by magnitude fixes that and keeps the sign in the displayed
value, so a reader can still tell which direction the gap runs.

It does not claim "worst" is universally better than "best". Off-policy keeps its maximum because
the section's purpose is finding a policy that beats logging; the worst candidate answers nothing.
The choice is per-section and is recorded per-section.

It does not change any section page, the exporter, or any computation. These are the four extremum
rules the overview tiles use.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Two commits: the selection helper, then the four specs and their labels.
- Only `frontend/components/format.js`, `frontend/components/scorecard.jsx` and the guard test change.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `HEADLINES` and `LOW_COVERAGE` keep their exact names and shapes — the contract tests parse
  `const HEADLINES = {` and the string `<= LOW_COVERAGE`.
- `maxByField` keeps its exported name and behaviour: `HEADLINES.fairness` uses it through
  `headlineRow`, and changing it would move the fairness tile too.
- `DIAGNOSTICS` keeps its name — the #260 guard parses it to check every section has a tile.
- Every tile keeps its `SECTION_ROUTE` link and its `support` count.
- The overview stays a server component.

## Implementation

### Commit 1 -- one selection helper

`format.js` gains `pickByField(rows, field, select = "max")`, returning the row whose `field` is
extremal under `select`: `"max"`, `"min"`, or `"abs"` for largest magnitude. Rows whose field is
null or undefined are skipped, as `maxByField` already skips them — treating a missing value as zero
would let "worst AUC" name a signal that was never scored.

`maxByField` becomes `pickByField(rows, field, "max")`, keeping its name and its docstring about
fairness so `headlineRow` is untouched.

### Commit 2 -- the four rules

`DIAGNOSTICS` entries gain two optional keys: `select`, passed to `pickByField`, and `where`, a
predicate narrowing the rows before selection. `diagnosticTile` applies `where` then `pickByField`.

- `ranking` gains `select: "min"` and the label `worst AUC`.
- `keyword` gains `select: "abs"` and the label `largest gap`.
- `recall` gains `where: (row) => row.k === 10` and the label `best recall@10`.
- `ope` is unchanged, defaulting to `"max"`.

The guard from #260 gains an assertion that every `DIAGNOSTICS` entry naming `rows` also states
which extremum it takes, so a future row-based tile cannot silently default to the flattering one.

## Validation and acceptance

1. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline is 582 passed, 2 skipped;
   this adds one test, so the expected result is 583 passed, 2 skipped.
2. The contract tests pass **unmodified**.
3. Against a running dev server, the overview shows `worst AUC 0.506`, `largest gap -0.018`,
   `best recall@10 0.036`, and `best lift 0.0%`.
4. The fairness tile still reads `largest CTR gap 0.049` — proof `maxByField` behaved identically
   through the refactor.
5. `cd frontend && npm run build` succeeds; `npm run validate:data` passes.
6. `git diff --name-only origin/master` lists no `frontend/data/dashboard.json`.
7. `git diff --check` clean.

## Limits

`k = 10` is a choice, not a derivation. A reader who cares about k=20 sees a different ordering of
methods only if the methods cross over between k values, which they do not in this snapshot —
`hybrid` leads at every k. The tile would mislead only if that stopped being true and nobody
revisited the constant.

"Worst AUC" names the weakest signal but not how many signals are weak. Three signals with AUC near
0.5 and one signal near 0.5 produce the same tile. The section page shows the distribution; the tile
points at it.

Selecting by magnitude and displaying the signed value means the keyword tile can show a negative
number under a label that says "largest". That is deliberate — the sign carries which way the gap
runs — but it does read oddly, and the alternative of showing `0.018` would hide the direction.
