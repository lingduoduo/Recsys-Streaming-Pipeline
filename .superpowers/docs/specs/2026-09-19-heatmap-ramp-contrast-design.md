# Heatmap ramp contrast — Design

## Goal

Improve the heatmaps' contrast **without changing any underlying value**. The numbers printed in
the cells, the titles, and every exported figure stay exactly as they are. Only the mapping from
CTR to colour changes.

Deliberately out of scope: metric semantics, the `grid`/`topic_grid` data, and the snapshot. This
PR changes no data file.

## The problem, measured

Heat is currently `ctr / max(ctr)` — zero-anchored and scaled by the grid's own maximum. Real CTR
occupies a narrow band, so the cells land in the top half of the ramp:

| grid | deciles used | worst crowding | rendered range |
|---|---|---|---|
| category (102 cells) | 6/10 | 32.4% in one decile | 0.48 – 1.00 |
| topic (237 cells) | 7/10 | 32.1% in one decile | 0.40 – 1.00 |

The `Drama&Romance` row of the category grid renders as a uniform shade across all eighteen genres.

## Candidates considered

| mapping | deciles | crowding | what colour encodes |
|---|---|---|---|
| current `ctr/max` | 6/10 | 32.4% | magnitude, compressed |
| min–max | 10/10 | 21.6% | magnitude; anchored on two single cells |
| **robust p5–p95, clipped** | **10/10** | **14.7%** | **magnitude, linear within the domain** |
| quantile (rank) | 10/10 | 10.8% | rank, *not* magnitude |

**Quantile is rejected despite scoring best.** With 102 cells, two cells differing by 0.01 percentage
points would receive visibly different shades: it manufactures apparent structure out of noise. It
also defeats the stated goal, since colour would stop tracking the values at all.

**Min–max is rejected** because both anchors are single cells, so one outlier re-scales the whole
figure between exports.

## Decision

A **robust p5–p95 domain, shared across both grids**, clipped at the ends.

Shared rather than per-grid because the two heatmaps sit in the same section and are read against
each other. Today they are *not* comparable — each scales by its own maximum — so a shared domain
fixes an existing defect rather than introducing a constraint.

```
domain = [p5, p95] over the pooled ctr values of grid + topic_grid
t      = clamp((ctr - lo) / (hi - lo), 0, 1)
```

Linear interpolation for the percentile, matching the usual definition.

### What this costs, stated plainly

- **The extreme 10% saturate.** A cell at the ceiling and a cell near it look alike. This is the
  price of the contrast gain and it is bounded: at most 5% of cells at each end.
- **The domain is data-dependent**, so two snapshots are still not comparable by eye. The current
  mapping is data-dependent too (it divides by the grid's max), so nothing is lost. The only option
  that would buy cross-snapshot comparability is a hard-coded domain, rejected because a future run
  outside the range would saturate wholesale and stop discriminating silently.

Because the endpoints are no longer implicit, **the legend states them**: the reader is told the
colour spans a specific CTR range and that cells outside it saturate.

## Structure

The mapping moves out of the component into `components/heat-domain.mjs`, a pure module with no
React import:

| export | contract |
|---|---|
| `percentile(sorted, p)` | linear-interpolated percentile of an ascending array |
| `heatDomain(groups, loPct = 5, hiPct = 95)` | pools `ctr` across row-arrays, returns `[lo, hi]` |
| `heatScore(ctr, [lo, hi])` | clamped position in the domain, `0` when `hi <= lo` |

`.mjs` so that `node` can import it directly — the extension `validate_measurements.mjs` already
establishes. That makes the arithmetic testable for real, rather than by scanning JSX text, which is
the only reason the module exists; nothing else is extracted.

Unchanged: a pair never served is hatched, the forced diagonal is outlined, the ramp is capped at
70% of `--accent` for contrast, and each cell still prints its own CTR.

## Acceptance

1. Category grid, rendered: 10/10 deciles occupied, worst crowding ≤ 20% (from 6/10 and 32.4%).
2. Topic grid, rendered: still 10/10 deciles, crowding no worse than now — the regression check.
3. `heatScore` clamps: a value below `lo` gives exactly 0, above `hi` exactly 1.
4. Both heatmaps receive the *same* domain object.
5. The legend states the domain's endpoints.
6. `dashboard.json` is byte-identical to master's.
7. Full suite green, `validate:data` passes, build clean.
