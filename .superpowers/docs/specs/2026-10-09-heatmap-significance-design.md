# Keyword heatmaps: colour only what differs from the grid — Design

## The gap

The three keyword heatmaps (category × keyword, topic × keyword, decade × keyword) shade every
cell by its CTR on one shared ramp (#267). The ramp spans the pooled 5th–95th percentile, CTR
0.116–0.190 on the last measurement — about 7 points. A cell at the 181-impression minimum has a
standard error near 2.7 points, so chance alone can move its shade across most of the ramp. The
impression count that would reveal this is only in the hover title.

Measured on the shipped snapshot (each cell's CTR against its own grid's pooled CTR, 0.1514):

| grid | cells | \|z\| ≥ 2 | expected by chance (4.55%) |
|---|---|---|---|
| category × keyword | 102 | 30 (29%) | ~5 |
| topic × keyword | 237 | 45 (19%) | ~11 |
| decade × keyword | 90 | 18 (20%) | ~4 |

Between 71% and 81% of cells are statistically consistent with their grid's rate, yet they are
painted across the full ramp. The figure mostly shows sampling noise.

## Design

### Python — `cross_tab` publishes z

`compute_keyword`'s `cross_tab(level, row_name)` adds `z` to every cell:

```
p0 = Σ query_clicks / Σ movie_impressions   over the whole grid
z  = (ctr − p0) / sqrt(p0 · (1 − p0) / movie_impressions)
```

rounded to 2 dp; `None` when `p0` is 0 or 1 (no variance) or a cell has no impressions. `p0` is
per grid, because each grid is read on its own. The exporter already ships every cell field, so it
needs no change.

### Frontend — the gate lives in `heat-domain.mjs`

A new pure export, so it is tested by running it (`test_heat_domain.py` already runs this module
in node):

```js
export const SIGNAL_Z = 2;
/** "signal" | "noise" | "unknown" -- unknown is a snapshot that predates z. */
export function heatSignal(z) { ... }
```

- `|z| ≥ 2` → `"signal"`: the cell keeps its ramp shade, unchanged from today.
- `|z| < 2` → `"noise"`: the cell gets a new `heat-noise` class — the plain `--surface`
  background (amended: `--accent-soft` is where the ramp starts, so it hid significantly low cells), CTR text still printed — so it reads as "consistent with this grid's rate".
- `z` absent or not a finite number → `"unknown"`: shaded as today. A snapshot exported before
  this change keeps its old look instead of turning entirely grey, and nothing claims a test that
  was not run.

The cell title gains `· z = 1.3`. The forced-diagonal outline and the hatched never-served
cells are untouched. The shared colour domain is unchanged (still pooled over every cell), so a
signal cell's shade means what it meant before.

Each grid's caption reports its count, e.g. "45 of 237 cells differ from this grid's CTR
(|z| ≥ 2); about 11 would by chance alone." The section's shared fine print explains the rule
once, including the multiple-comparisons caveat: at this threshold roughly 1 in 22 cells is
coloured by chance, which the per-grid expected count states rather than corrects for.

## Amended after the final review

Two findings changed the design; both were measured on the surviving run before acting.

**The binomial SE is wrong here.** A cell is a handful of movies (median 4) repeated across many
impressions, so impressions are not independent. Shuffling genres across items (no genre effect
at all), the binomial z still flagged **~11 of 102** category cells at |z| ≥ 2 (range 6–17) — the
"about 5 by chance" caption was off by more than 2×. `z` now uses an **item-clustered (sandwich)
SE** over per-movie residuals, `var = Σ_i (clicks_i − p0·n_i)² / n² · m/(m−1)`. Under the same
shuffle it flags **~0.8 of 102** (range 0–2), so the nominal caption errs conservative. A
one-movie cell has no between-item variance: `z` is `null` and the cell is drawn neutral; the
caption counts them as untestable. A *missing* `z` still means a pre-z snapshot (old shading).

Real counts with the clustered SE: category 13 of 95 testable (6 above, 7 below; 7 untestable),
topic 9 of 135 (102 untestable — most primary-genre × keyword pairs are one film), decade 4 of 87
(chance level). The measurement table above is the binomial view and is kept as the motivation.

**A one-hue ramp hides significant lows.** Its floor is nearly white, so the strongest low cells
read as noise. `heatSignal` returns `high` / `low` / `noise` / `unknown`; low cells take the second
series hue (`--series-1`), deeper as CTR falls; noise cells sit on plain `--surface`.

## Non-goals

- No multiple-comparison correction (Bonferroni/FDR). The expected-by-chance count is stated
  instead; a correction would hide most of the real signal at these cell sizes.
- No shrinkage (empirical Bayes). It would make a cell's shade differ from its printed CTR,
  breaking #267's "equal colour steps are equal CTR steps".
- No per-row baselines. One p0 per grid matches the one shared ramp.

## Testing

- Python (`test_analysis_dashboard.py`): `z` on a hand-computed grid; `None` when the grid's p0
  is 0.
- Node-backed (`test_heat_domain.py`): `heatSignal` for 2.0 (signal, boundary inclusive), −2.5
  (signal), 1.99 (noise), `null` / `undefined` / `NaN` (unknown).
- Build: on the current snapshot (no `z`) the page shows zero `heat-noise` cells; after a
  re-export the count of `heat-noise` cells equals the noise count computed from the snapshot.

## Snapshot

Re-export only, against the surviving run's Parquet and Redis. Expected moves: each keyword grid
cell gains `z`; freshness's two content-age fields shift with the clock. Anything else moving is
a regression.

## Acceptance

1. Every cell of `grid`, `topic_grid`, `decade_grid` carries `z` as defined.
2. `heatSignal` classifies as above, verified in node.
3. On the re-exported snapshot, noise cells render neutral, signal cells keep their shade, and
   each grid's caption gives its signal count and the chance expectation.
4. Full suite green; no new npm dependency.
