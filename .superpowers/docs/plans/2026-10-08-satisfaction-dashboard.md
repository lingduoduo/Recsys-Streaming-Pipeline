# Satisfaction Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The Satisfaction section reports who and what its averages cover, how CTR / order rate / rating / active users move over the run, and the shape of the rating evidence.

**Architecture:** `compute_satisfaction` gains three summary fields and three envelope keys (`series`, `series_bucket_seconds`, `rating_distribution`); the exporter already passes the envelope through. The frontend gets one new plain-SVG `LineChart` and a reworked `SatisfactionSection`.

**Tech Stack:** Python 3.12 + pandas (pytest); Next 15 / React 19, no chart library.

**Spec:** `.superpowers/docs/specs/2026-10-08-satisfaction-dashboard-design.md`

## Global Constraints

- `impression_ts` is epoch **seconds**.
- 24 equal-width buckets over the observed `[min, max]`; the max lands in bucket 23.
- A missing measurement is `None`/`null`, never 0 (counts of an empty bucket are 0; rates are `None`).
- Rating bins are 0.5 wide, from the lowest observed rating's bin through 4.5; 5.0 falls in 4.5.
- No new npm dependency.
- Every commit runs the branch guard: `test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }`
- Python tests run from `recsys-pipeline/`: `python3 -m pytest -q`. Baseline on master: `611 passed, 2 skipped`.

## Review Focus

1. **Non-numeric or null `impression_ts` mixed with valid ones** — those rows are excluded from the series only, not from the summary row. Pinned in Task 2.
2. **Float bucket edges** — `(hi - lo) / ((hi - lo) / 24)` can evaluate to 24.0; the clip keeps it in bucket 23. Pinned in Task 2 by a timestamp at exactly `hi`.
3. **Offline section unavailable but live row present** — `_merge_live_row` builds a fresh envelope without `series`; the UI must read `data.series ?? []`. Pinned in Task 5 by the fallback branch rendering on the current snapshot.
4. **A run with no ratings at all** — `rating_distribution == []`, every bucket `mean_rating: None`; the distribution chart is not drawn. Pinned in Task 3.
5. **The regex contract tests on the component source** — `test_satisfaction_coverage_chart_omits_the_series_that_cannot_show_coverage` parses `chart={…\n      }}` and `const fields = [`; `test_dashboard_columns_match_the_published_measurement_keys` checks every requested column is published. Task 5 runs both.

---

### Task 1: Population fields on the summary row

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/quality_measurements.py` (`compute_satisfaction`, ~line 78)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`

**Interfaces:**
- Produces: summary row keys `users: int | None`, `items: int | None`, `rated_samples: int`; helper `_distinct(samples, name) -> int | None`.

- [ ] **Step 1: Write the failing tests**

Append to `test_quality_measurements.py`:

```python
def _timed_samples():
    # Four samples over a 24-second span: buckets 0, 0, 5 and 23 (bucket 10 stays empty).
    return pd.DataFrame([
        {"impression_ts": 100, "user_id": "u1", "item_id": "i1", "clicked": 1, "ordered": 1, "rating": 4.0},
        {"impression_ts": 100, "user_id": "u2", "item_id": "i2", "clicked": 0, "ordered": 0, "rating": None},
        {"impression_ts": 105, "user_id": "u1", "item_id": "i2", "clicked": 1, "ordered": 0, "rating": None},
        {"impression_ts": 124, "user_id": "u3", "item_id": "i3", "clicked": 0, "ordered": 1, "rating": 5.0},
    ])


def test_satisfaction_counts_the_population_behind_its_averages():
    row = compute_satisfaction(_timed_samples())["rows"][0]

    assert (row["users"], row["items"], row["rated_samples"]) == (3, 3, 2)


def test_satisfaction_population_is_none_not_zero_without_the_columns():
    row = compute_satisfaction(pd.DataFrame([{"clicked": 1}]))["rows"][0]

    assert row["users"] is None and row["items"] is None
    assert row["rated_samples"] == 0
```

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py -k population`
Expected: FAIL with `KeyError: 'users'`

- [ ] **Step 3: Implement**

In `compute_satisfaction`, add to `row` after `"completion_coverage"`:

```python
        "users": _distinct(samples, "user_id"),
        "items": _distinct(samples, "item_id"),
        "rated_samples": len(ratings),
```

Add beside `_observed_column`:

```python
def _distinct(samples: pd.DataFrame, name: str) -> int | None:
    """Distinct observed values; None when the column is absent -- 0 users would be a claim."""
    return int(samples[name].dropna().nunique()) if name in samples else None
```

- [ ] **Step 4: Run to verify they pass**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/quality_measurements.py recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py
git commit -m "feat(satisfaction): count the users, items and ratings behind the averages"
```

### Task 2: Time series over adaptive buckets

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/quality_measurements.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`

**Interfaces:**
- Consumes: `_timed_samples()` from Task 1; `_distinct`, `_numeric_column`, `_mean`.
- Produces: envelope keys `series: list[dict]` (keys `bucket_start, impressions, users, ctr, order_rate, mean_rating, ratings`) and `series_bucket_seconds: float | None`; constant `SERIES_BUCKETS = 24`; helper `_satisfaction_series(samples) -> tuple[list[dict], float | None]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_satisfaction_series_buckets_the_observed_span():
    result = compute_satisfaction(_timed_samples())
    series = result["series"]

    assert len(series) == 24 and result["series_bucket_seconds"] == 1.0
    assert series[0] == {"bucket_start": 100.0, "impressions": 2, "users": 2, "ctr": 0.5,
                         "order_rate": 0.5, "mean_rating": 4.0, "ratings": 1}
    # A rated-free bucket has no mean rating -- not a mean of 0.
    assert series[5] == {"bucket_start": 105.0, "impressions": 1, "users": 1, "ctr": 1.0,
                         "order_rate": 0.0, "mean_rating": None, "ratings": 0}
    # Empty buckets stay, so the x-axis is evenly spaced.
    assert series[10] == {"bucket_start": 110.0, "impressions": 0, "users": 0, "ctr": None,
                          "order_rate": None, "mean_rating": None, "ratings": 0}
    # The maximum timestamp lands in the last bucket, not a 25th.
    assert series[23]["impressions"] == 1 and series[23]["mean_rating"] == 5.0


def test_satisfaction_series_skips_unparseable_timestamps_only():
    samples = pd.concat([_timed_samples(), pd.DataFrame([
        {"impression_ts": None, "user_id": "u9", "item_id": "i9", "clicked": 1, "ordered": 0},
        {"impression_ts": "not-a-time", "user_id": "u9", "item_id": "i9", "clicked": 1, "ordered": 0},
    ])], ignore_index=True)
    result = compute_satisfaction(samples)

    assert sum(b["impressions"] for b in result["series"]) == 4
    assert result["rows"][0]["users"] == 4  # the summary still counts every sample


@pytest.mark.parametrize("frame", [
    pd.DataFrame([{"clicked": 1}, {"clicked": 0}]),
    pd.DataFrame([{"clicked": 1, "impression_ts": 7}, {"clicked": 0, "impression_ts": 7}]),
])
def test_satisfaction_series_is_empty_without_a_span(frame):
    result = compute_satisfaction(frame)

    assert result["series"] == [] and result["series_bucket_seconds"] is None
```

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py -k series`
Expected: FAIL with `KeyError: 'series'`

- [ ] **Step 3: Implement**

Near the top of `quality_measurements.py`, after the imports:

```python
# The sim stamps events with wall-clock time, so a run spans minutes; a calendar bucket would be
# one point. Equal-width buckets over the observed span work for a minute or a quarter.
SERIES_BUCKETS = 24
```

Replace the final `return available(...)` of `compute_satisfaction` with:

```python
    result = available("Observed user satisfaction", [row], total, coverage)
    result["series"], result["series_bucket_seconds"] = _satisfaction_series(samples)
    return result
```

Add after `compute_satisfaction`:

```python
def _satisfaction_series(samples: pd.DataFrame) -> tuple[list[dict[str, object]], float | None]:
    """Per-bucket engagement over the observed impression_ts span (epoch seconds)."""
    if "impression_ts" not in samples:
        return [], None
    stamps = pd.to_numeric(samples["impression_ts"], errors="coerce")
    timed = samples[stamps.notna()]
    stamps = stamps[stamps.notna()]
    if stamps.empty or stamps.max() <= stamps.min():
        return [], None
    start = float(stamps.min())
    width = (float(stamps.max()) - start) / SERIES_BUCKETS
    # Float division can put the maximum at exactly SERIES_BUCKETS; it belongs to the last bucket.
    bucket = ((stamps - start) // width).clip(upper=SERIES_BUCKETS - 1).astype(int)
    series = []
    for index in range(SERIES_BUCKETS):
        part = timed[(bucket == index).to_numpy()]
        ratings = _numeric_column(part, "rating")
        series.append({
            "bucket_start": round(start + index * width, 1),
            "impressions": len(part),
            "users": _distinct(part, "user_id"),
            "ctr": _mean(_numeric_column(part, "clicked")),
            "order_rate": _mean(_numeric_column(part, "ordered")),
            "mean_rating": _mean(ratings),
            "ratings": len(ratings),
        })
    return series, round(width, 1)
```

- [ ] **Step 4: Run to verify they pass**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/quality_measurements.py recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py
git commit -m "feat(satisfaction): engagement and rating over 24 buckets of the observed span"
```

### Task 3: Rating distribution, and the live merge keeps the new keys

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/quality_measurements.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`, `recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py:203`

**Interfaces:**
- Produces: envelope key `rating_distribution: list[{"rating": float, "count": int}]`; helper `_rating_distribution(ratings: list[float]) -> list[dict]`.

- [ ] **Step 1: Write the failing tests**

In `test_quality_measurements.py`:

```python
def test_satisfaction_rating_distribution_keeps_empty_half_point_bins():
    samples = pd.DataFrame([{"clicked": 1, "rating": r} for r in (3.0, 3.2, 4.9, 5.0)])

    assert compute_satisfaction(samples)["rating_distribution"] == [
        {"rating": 3.0, "count": 2}, {"rating": 3.5, "count": 0},
        {"rating": 4.0, "count": 0}, {"rating": 4.5, "count": 2}]


def test_satisfaction_without_ratings_has_no_distribution():
    result = compute_satisfaction(pd.DataFrame([{"clicked": 1, "impression_ts": 1},
                                                {"clicked": 0, "impression_ts": 2}]))

    assert result["rating_distribution"] == []
    assert all(b["mean_rating"] is None for b in result["series"])
```

In `test_dashboard_measurement_contract.py`, directly after `assert scopes["satisfaction"] == ["offline", "live_service"]`:

```python
    # The envelope keys beside rows survive the live merge, which spreads **offline.
    assert {"series", "series_bucket_seconds", "rating_distribution"} <= set(output["satisfaction"])
```

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py -k distribution integration-tests/python_modeling/test_dashboard_measurement_contract.py -k every_measurement`
Expected: FAIL with `KeyError: 'rating_distribution'` and the subset assertion.

- [ ] **Step 3: Implement**

In `compute_satisfaction`, after the `series` line:

```python
    result["rating_distribution"] = _rating_distribution(ratings)
```

Add after `_satisfaction_series`:

```python
def _rating_distribution(ratings: list[float]) -> list[dict[str, object]]:
    """Half-point bins from the lowest observed through 4.5; a 5.0 falls in the 4.5 bin."""
    if not ratings:
        return []
    halves = Counter(min(math.floor(r * 2), 9) for r in ratings)
    return [{"rating": h / 2, "count": halves.get(h, 0)} for h in range(min(halves), 10)]
```

and `from collections import Counter` beside the existing `collections.abc` import.

- [ ] **Step 4: Run the full suite**

Run: `python3 -m pytest -q`
Expected: `619 passed, 2 skipped` (611 + 8 new test items; the parametrized test counts twice).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/quality_measurements.py recsys-pipeline/integration-tests/python_modeling/
git commit -m "feat(satisfaction): publish the rating distribution in half-point bins"
```

### Task 4: `LineChart` and a duration formatter

**Files:**
- Modify: `recsys-pipeline/frontend/components/ui.jsx` (after `GroupedBarChart`), `recsys-pipeline/frontend/components/format.js`, `recsys-pipeline/frontend/app/globals.css` (after the `.legend-swatch` rules)

**Interfaces:**
- Produces: `LineChart({ title, labels: string[], series: {name, values: (number|null)[], notes?: string[]}[], percentage?, valueFormatter?, caption? })`; `duration(seconds: number) -> string` ("45 s", "6.2 min", "3.1 h", "2.0 d").

- [ ] **Step 1: Add `duration` to `format.js`**

```js
// A span in seconds, in the largest unit that keeps it above 1.
export const duration = (s) => {
  if (s === null || s === undefined) return "N/A";
  if (s < 60) return `${Math.round(s * 10) / 10} s`;
  if (s < 3600) return `${(s / 60).toFixed(1)} min`;
  if (s < 86400) return `${(s / 3600).toFixed(1)} h`;
  return `${(s / 86400).toFixed(1)} d`;
};
```

- [ ] **Step 2: Add `LineChart` to `ui.jsx`**

```jsx
// Consecutive non-null indices, so a null breaks the line instead of dropping it to zero.
function runs(values) {
  const out = [];
  let current = [];
  values.forEach((v, i) => {
    if (finite(v) === null) {
      if (current.length) out.push(current);
      current = [];
    } else current.push(i);
  });
  if (current.length) out.push(current);
  return out;
}

// A metric over evenly spaced buckets. Plain SVG: the frontend carries no chart library. A null
// is a gap -- a bucket with no ratings has no mean rating, not one of zero. Two series at most,
// coloured by index like GroupedBarChart.
export function LineChart({ title, labels, series, percentage = false, valueFormatter, caption }) {
  const format = formatter({ percentage, valueFormatter });
  const observed = series.flatMap((s) => s.values).map(finite).filter((v) => v !== null);
  const lo = observed.length ? Math.min(...observed) : 0;
  const hi = observed.length ? Math.max(...observed) : 1;
  const W = 320, H = 120, PAD = 6;
  const x = (i) => PAD + (labels.length > 1 ? (i / (labels.length - 1)) * (W - 2 * PAD) : (W - 2 * PAD) / 2);
  const y = (v) => H - PAD - ((v - lo) / (hi - lo || 1)) * (H - 2 * PAD);
  return (
    <div className="chart-card line-chart">
      {title ? <h3>{title}</h3> : null}
      {series.length > 1 ? (
        <div className="chart-legend">
          {series.map((s, si) => (
            <span className="legend-item" key={s.name}>
              <span className={`legend-swatch series-${si}`} />
              {s.name}
            </span>
          ))}
        </div>
      ) : null}
      <div className="line-plot">
        <div className="line-axis"><span>{format(hi)}</span><span>{format(lo)}</span></div>
        <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={title}>
          {series.map((s, si) => runs(s.values).map((run) => (
            <polyline key={`${s.name}-${run[0]}`} className={`line series-${si}`}
              points={run.map((i) => `${x(i)},${y(finite(s.values[i]))}`).join(" ")} />
          )))}
          {series.map((s, si) => s.values.map((v, i) => (finite(v) === null ? null : (
            <circle key={`${s.name}-${i}`} className={`dot series-${si}`} cx={x(i)} cy={y(finite(v))} r="2.5">
              <title>{`${s.name} ${labels[i]}: ${format(finite(v))}${s.notes ? ` (${s.notes[i]})` : ""}`}</title>
            </circle>
          ))))}
        </svg>
      </div>
      {caption ? <p className="fine-print">{caption}</p> : null}
    </div>
  );
}
```

- [ ] **Step 3: Add the CSS**

```css
/* LineChart: y-axis labels in a narrow column beside a fluid SVG. */
.line-plot {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 8px;
}

.line-axis {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  font-size: 0.7rem;
  color: var(--muted);
  font-variant-numeric: tabular-nums;
}

.line-chart svg { width: 100%; height: auto; overflow: visible; }
.line-chart polyline.line { fill: none; stroke: var(--series-0); stroke-width: 2; }
.line-chart polyline.series-1 { stroke: var(--series-1); }
.line-chart circle.dot { fill: var(--series-0); }
.line-chart circle.series-1 { fill: var(--series-1); }
```

- [ ] **Step 4: Build**

Run: `cd recsys-pipeline/frontend && npm run build 2>&1 | grep -E 'Compiled|Failed|rror' | head -3`
Expected: `Compiled successfully` (nothing uses `LineChart` yet; this proves it compiles).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/frontend/components/ui.jsx recsys-pipeline/frontend/components/format.js recsys-pipeline/frontend/app/globals.css
git commit -m "feat(dashboard): a dependency-free LineChart that gaps on null"
```

### Task 5: The Satisfaction section

**Files:**
- Modify: `recsys-pipeline/frontend/components/measurements.jsx` (`MeasurementSection` chart call; `SatisfactionSection`, ~line 88)

**Interfaces:**
- Consumes: `LineChart`, `duration` (Task 4); envelope keys from Tasks 1–3.
- Produces: `MeasurementSection` calls `chart(rows, data)`.

- [ ] **Step 1: Pass the envelope to the chart callback**

In `MeasurementSection`, replace `{chart ? chart(rows) : null}` with `{chart ? chart(rows, data) : null}`. Add `LineChart` to the `./ui` import and `count, duration` to the `./format` import.

- [ ] **Step 2: Rewrite `SatisfactionSection`'s `columns`, `kpis` and `chart`**

```jsx
      columns={[
        "scope", "users", "items", "ctr", "order_rate", "mean_reward", "mean_rating",
        "rated_samples", "rating_coverage", "negative_feedback_rate", "negative_feedback_coverage",
        "mean_dwell_millis", "dwell_coverage", "mean_completion_rate", "completion_coverage",
        "feedback_events",
      ]}
      kpis={(rows) => {
        const row = rows[0] || {};
        return [
          { label: "users", value: count(row.users) },
          { label: "items", value: count(row.items) },
          { label: "CTR", value: share(row.ctr) },
          { label: "order rate", value: share(row.order_rate) },
          { label: "mean rating", value: num(row.mean_rating, 2), detail: `n = ${count(row.rated_samples)}` },
          { label: "mean dwell",
            value: row.mean_dwell_millis == null ? "N/A" : `${(row.mean_dwell_millis / 1000).toFixed(1)} s` },
        ];
      }}
```

and the chart (keep the existing comment above `const fields`):

```jsx
      chart={(rows, data) => {
        const row = rows[0] || {};
        const fields = ["rating_coverage", "dwell_coverage", "completion_coverage"];
        // Absent, not just empty, when the offline envelope was unavailable and the live merge
        // built a fresh one.
        const series = data.series ?? [];
        const width = data.series_bucket_seconds;
        const labels = series.map((b) => `+${duration(b.bucket_start - series[0].bucket_start)}`);
        const caption = `${series.length} × ${duration(width)} buckets over ${duration(width * series.length)}`;
        const ratings = data.rating_distribution ?? [];
        return (
          <ChartGrid>
            {series.length ? (
              <>
                <LineChart title="CTR and order rate over time" percentage labels={labels} caption={caption}
                  series={[{ name: "CTR", values: series.map((b) => b.ctr) },
                           { name: "order rate", values: series.map((b) => b.order_rate) }]} />
                <LineChart title="Mean rating over time" labels={labels} caption={caption}
                  valueFormatter={(v) => num(v, 2)}
                  series={[{ name: "mean rating", values: series.map((b) => b.mean_rating),
                             notes: series.map((b) => `n = ${b.ratings}`) }]} />
                <LineChart title="Active users over time" labels={labels} caption={caption}
                  valueFormatter={count}
                  series={[{ name: "users", values: series.map((b) => b.users) }]} />
              </>
            ) : <p className="na">No impression timestamps — time series unavailable.</p>}
            {ratings.length ? (
              <BarChart title="Rating distribution" valueFormatter={count}
                labels={ratings.map((r) => r.rating.toFixed(1))} values={ratings.map((r) => r.count)} />
            ) : null}
            <BarChart title="Optional signal coverage" percentage
              labels={fields.map((f) => f.replace("_coverage", ""))}
              values={fields.map((f) => row[f])} />
            <BarChart title="Engagement rates" percentage
              labels={["ctr", "order rate", "negative feedback"]}
              values={[row.ctr, row.order_rate, row.negative_feedback_rate]} />
          </ChartGrid>
        );
      }}
```

- [ ] **Step 3: Add the caveats to the section's fine print**

Append a second paragraph inside `SatisfactionSection`'s children:

```jsx
      <p className="fine-print">
        Buckets split the observed span of <code>impression_ts</code> evenly. The sim stamps events
        with wall-clock time, so on sim data the series shows drift across one run, not a calendar.
        Only orders carry a rating, and the sim derives it from completion (3 + 2 × completion).
      </p>
```

- [ ] **Step 4: Run the contract tests and the suite**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_dashboard_measurement_contract.py && python3 -m pytest -q`
Expected: contract file passes (both regex tests); suite `619 passed, 2 skipped`.

- [ ] **Step 5: Build and inspect the page against the current snapshot**

The committed snapshot predates `series`, so this exercises the fallback (Review Focus 3).

```bash
cd recsys-pipeline/frontend && npm run build 2>&1 | grep -E 'Compiled|Failed|rror' | head -3
f=.next/server/app/serving/satisfaction.html
echo -n "fallback line (want 1): "; grep -o 'time series unavailable' "$f" | wc -l | tr -d ' '
echo -n "dwell in seconds (want 1): "; grep -oE '[0-9]+\.[0-9] s<' "$f" | head -1
echo -n "KPI cards (want 6): "; grep -o 'class="metric-card"' "$f" | wc -l | tr -d ' '
```

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/frontend/components/measurements.jsx
git commit -m "feat(dashboard): satisfaction shows its population, trends and rating shape"
```

### Task 6: Regenerate the snapshot from a fresh sim run

**Files:**
- Modify: `recsys-pipeline/frontend/data/dashboard.json`

Read `project_simulation_harnesses` gotchas first: JDK 17 exported, ~25 minutes, Docker (Colima) up, read `$SIM_ROOT/parquet.log` if a drain sticks.

- [ ] **Step 1: Run the sim (background)**

```bash
cd recsys-pipeline
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
bash scripts/run-movie-category-sim.sh > /tmp/claude-sim.log 2>&1
tail -3 /tmp/claude-sim.log
```

Expected last line: `==> done. ... dashboard snapshot at recsys-pipeline/frontend/data/dashboard.json`.

- [ ] **Step 2: Verify the new keys and record the evidence**

```bash
python3 -c "
import json; s=json.load(open('frontend/data/dashboard.json'))['satisfaction']; r=s['rows'][0]
print('users',r['users'],'items',r['items'],'rated',r['rated_samples'])
print('buckets',len(s['series']),'width_s',s['series_bucket_seconds'])
print('dist',s['rating_distribution'])"
```

Record the printed values under Verification below.

- [ ] **Step 3: Build and inspect with real data**

```bash
cd frontend && npm run build 2>&1 | grep -E 'Compiled|Failed|rror' | head -3
f=.next/server/app/serving/satisfaction.html
echo -n "line charts (want 3): "; grep -o 'chart-card line-chart' "$f" | wc -l | tr -d ' '
echo -n "fallback (want 0): "; grep -c 'time series unavailable' "$f" || true
```

- [ ] **Step 4: Commit the snapshot alone, then the suite**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/data/dashboard.json
git commit -m "chore(dashboard): regenerate with the satisfaction series"
cd recsys-pipeline && python3 -m pytest -q 2>&1 | tail -1
```

## Verification

- Suite: _(record)_
- Fallback build (Task 5): _(record)_
- Sim run: users / items / rated / buckets × width / distribution: _(record)_
- Real-data build (Task 6): _(record)_
