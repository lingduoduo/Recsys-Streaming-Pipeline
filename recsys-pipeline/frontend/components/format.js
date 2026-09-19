// Formatting and ranking helpers shared by every dashboard section. No JSX, no data access:
// every function here turns a value into a string or orders rows.

export const num = (v, d = 4) => (v === null || v === undefined ? "N/A" : (Math.round(v * 10 ** d) / 10 ** d).toString());
export const pct = (v) => (v === null || v === undefined ? "N/A" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`);
export const share = (v) => (v === null || v === undefined ? "N/A" : `${(v * 100).toFixed(1)}%`);
export const ci = (lo, hi, asPct = false) =>
  lo === null || lo === undefined || hi === null || hi === undefined
    ? "N/A"
    : asPct
      ? `[${pct(lo)}, ${pct(hi)}]`
      : `[${num(lo)}, ${num(hi)}]`;

export const count = (v) => (v === null || v === undefined ? "N/A" : Number(v).toLocaleString());

// Rank by a field, dropping rows that have no value for it. Treating a missing
// value as zero would let "best AUC" name a signal that was never scored.
export const rankBy = (rows, field, direction = "desc") =>
  (rows ?? [])
    .filter((r) => r?.[field] !== null && r?.[field] !== undefined)
    .sort((a, b) => (direction === "desc" ? b[field] - a[field] : a[field] - b[field]));

export const COUNT_COLUMNS = {
  impressions: count, clicks: count, orders: count, queries: count,
  movie_impressions: count, query_clicks: count, query_orders: count,
  users_evaluated: count, instances: count, n: count, positives: count,
  episodes: count,
};
export const RATE_COLUMNS = { ctr: share, cvr: share, coverage: share, positive_rate: share };

// The extremal row by one field. `select` is "max", "min", or "abs" for the largest magnitude,
// which is what a signed field like keyword divergence needs: −0.0182 is a bigger gap than
// +0.0137, and taking the maximum would report the smaller one.
//
// A row whose field is null or undefined is skipped rather than treated as zero. Treating it as
// zero would let "worst AUC" name a signal that was never scored.
export function pickByField(rows, field, select = "max") {
  const rank = select === "abs" ? (value) => Math.abs(value) : (value) => value;
  const better = select === "min" ? (a, b) => a < b : (a, b) => a > b;
  return (rows ?? []).reduce((best, row) => {
    const value = row?.[field];
    if (value === null || value === undefined) return best;
    return best === null || better(rank(value), rank(best[field])) ? row : best;
  }, null);
}

// The row a section's headline is read from, when it is not a fixed index. Fairness emits one
// row per demographic dimension in DEFAULT_DIMENSIONS order, not in gap order, so rows[0] is
// whichever dimension sorts first — never "the largest gap" the tile claims to show.
export function maxByField(rows, field) {
  return pickByField(rows, field, "max");
}
