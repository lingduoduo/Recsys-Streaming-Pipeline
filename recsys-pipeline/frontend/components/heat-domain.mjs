// How a CTR becomes a shade.
//
// The obvious mapping, ctr / max(ctr), is zero-anchored, and real CTR occupies a narrow band
// well above zero -- so every cell lands in the top half of the ramp and the figure reads as
// uniformly warm. Measured on the shipped snapshot: the category grid used 6 of 10 deciles
// with a third of its cells in one of them.
//
// So the domain is the 5th-95th percentile of the values actually present, shared across every
// grid drawn together, with the ends clipped. Colour stays linear in CTR -- equal steps of
// colour are equal steps of CTR -- which a rank/quantile mapping would not preserve: that would
// give two cells differing by 0.01pp visibly different shades.

/** Linear-interpolated percentile of an ASCENDING array. */
export function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const k = ((sorted.length - 1) * p) / 100;
  const lo = Math.floor(k);
  const hi = Math.min(lo + 1, sorted.length - 1);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (k - lo);
}

/**
 * Pool the ctr values of every row-array and return [lo, hi].
 * Shared across grids on purpose: two heatmaps in one section get read against each other,
 * and a per-grid domain would give the same shade two different meanings.
 */
export function heatDomain(groups, loPct = 5, hiPct = 95) {
  const values = [];
  for (const rows of groups) {
    for (const r of rows ?? []) {
      if (typeof r?.ctr === "number") values.push(r.ctr);
    }
  }
  if (!values.length) return [0, 0];
  values.sort((a, b) => a - b);
  return [percentile(values, loPct), percentile(values, hiPct)];
}

/** Position of ctr within the domain, clamped. Cells outside it saturate. */
export function heatScore(ctr, [lo, hi]) {
  if (typeof ctr !== "number" || !(hi > lo)) return 0;
  return Math.min(1, Math.max(0, (ctr - lo) / (hi - lo)));
}
