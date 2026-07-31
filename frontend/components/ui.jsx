const round4 = (v) => (typeof v === "number" ? Math.round(v * 1e4) / 1e4 : v);

// A value that is not a finite number is missing, not zero. Charts omit its bar
// and label it N/A rather than drawing a zero-height mark that reads as measured.
const finite = (v) => (Number.isFinite(Number(v)) ? Number(v) : null);

export function Section({ title, headline, description, actions, id, children }) {
  return (
    <section className="report-card" id={id}>
      {/* The flex row is its own element: `.section-heading` is shared with NaCard,
          whose heading is a plain h2 + paragraph that must keep stacking. */}
      <div className="section-heading">
        <div className="section-heading-row">
          <div className="section-heading-main">
            <h2>{title}</h2>
            {headline ? <p className="insight">{headline}</p> : null}
            {description ? <p className="section-description">{description}</p> : null}
          </div>
          {actions ? <div className="section-actions">{actions}</div> : null}
        </div>
      </div>
      <div className="section-body">{children}</div>
    </section>
  );
}

export function NaCard({ title, reason, id }) {
  return (
    <section className="report-card status-card" id={id}>
      <div className="section-heading">
        <h2>{title}</h2>
        <p className="na">N/A — {reason}</p>
      </div>
    </section>
  );
}

export function MetricGrid({ children }) {
  return <div className="metric-grid">{children}</div>;
}

export function MetricCard({ label, value, detail }) {
  return (
    <div className="metric-card">
      <span className="card-label">{label}</span>
      <strong className="card-value">{value ?? "N/A"}</strong>
      {detail ? <span className="card-detail">{detail}</span> : null}
    </div>
  );
}

// Two-up layout for charts that read together. Named for what it lays out, so it
// cannot be confused with `.report-grid`, which is the page's section stack.
export function ChartGrid({ children }) {
  return <div className="chart-grid">{children}</div>;
}

function formatter({ percentage, valueFormatter }) {
  return (v) => {
    if (v === null) return "N/A";
    if (valueFormatter) return valueFormatter(v);
    if (percentage) return `${(v * 100).toFixed(1)}%`;
    return round4(v).toLocaleString();
  };
}

export function BarChart({ labels, values, title, horizontal = false, percentage = false, valueFormatter }) {
  const numeric = labels.map((_, i) => finite(values[i]));
  const observed = numeric.filter((v) => v !== null).map(Math.abs);
  const scale = (observed.length ? Math.max(...observed) : 0) || 1;
  const format = formatter({ percentage, valueFormatter });
  return (
    <div className="chart-card">
      {title ? <h3>{title}</h3> : null}
      <div className={horizontal ? "bar-chart horizontal" : "bar-chart"}>
        {labels.map((label, i) => {
          const v = numeric[i];
          return (
            <div className="bar-row" key={`${label}-${i}`}>
              <span className="bar-label" title={String(label)}>{label}</span>
              <div className="bar-track">
                {v === null ? null : (
                  <div
                    className={v < 0 ? "bar-fill negative" : "bar-fill"}
                    style={{ width: `${Math.max(1, (Math.abs(v) / scale) * 100)}%` }}
                    title={`${label}: ${format(v)}`}
                  />
                )}
              </div>
              <span className="bar-value">{format(v)}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// Two or three series sharing one scale, for comparisons a single series hides
// (ndcg vs mrr, fresh vs established). Series colour is fixed by index, never
// cycled; a fourth series needs a different chart, not a fourth hue.
export function GroupedBarChart({ labels, series, title, percentage = false, valueFormatter }) {
  const observed = series.flatMap((s) => s.values).map(finite).filter((v) => v !== null).map(Math.abs);
  const scale = (observed.length ? Math.max(...observed) : 0) || 1;
  const format = formatter({ percentage, valueFormatter });
  return (
    <div className="chart-card">
      {title ? <h3>{title}</h3> : null}
      <div className="chart-legend">
        {series.map((s, si) => (
          <span className="legend-item" key={s.name}>
            <span className={`legend-swatch series-${si}`} />
            {s.name}
          </span>
        ))}
      </div>
      <div className="bar-chart grouped">
        {labels.map((label, li) => (
          <div className="bar-group" key={`${label}-${li}`}>
            <span className="bar-label" title={String(label)}>{label}</span>
            <div className="bar-group-bars">
              {series.map((s, si) => {
                const v = finite(s.values[li]);
                return (
                  <div className="bar-row" key={s.name}>
                    <div className="bar-track">
                      {v === null ? null : (
                        <div
                          className={`bar-fill series-${si}`}
                          style={{ width: `${Math.max(1, (Math.abs(v) / scale) * 100)}%` }}
                          title={`${s.name} ${label}: ${format(v)}`}
                        />
                      )}
                    </div>
                    <span className="bar-value">{format(v)}</span>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function DataTable({ rows = [], columns, formatters = {}, compact = false }) {
  const cols = columns || (rows.length ? Object.keys(rows[0]) : []);
  if (!rows.length) return <p className="empty-state">No rows available.</p>;
  return (
    <div className="table-shell">
      <table className={compact ? "rpt compact" : "rpt"}>
        <thead>
          <tr>
            {cols.map((c) => (
              <th key={c}>{c.replaceAll("_", " ")}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => {
                const format = formatters[c];
                const value = format ? format(r[c], r) : formatCell(r[c]);
                return <td key={c}>{value === null || value === undefined || value === "" ? "N/A" : value}</td>;
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(v) {
  if (v === null || v === undefined) return "N/A";
  if (typeof v === "number") return String(round4(v));
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

// One scorecard tile. Status reflects DATA AVAILABILITY only — never whether the
// number is good, because no targets have been set for these measurements.
export function MetricTile({ title, value, label, sampleSize, status, reason, href }) {
  return (
    <a className={`metric-tile status-${status}`} href={href}>
      <span className="metric-title">{title}</span>
      <span className="metric-value">{value}</span>
      <span className="metric-label">{label}</span>
      <span className="metric-support">
        {status === "na" ? reason : `n=${(sampleSize ?? 0).toLocaleString()}`}
      </span>
      {/* Border color alone can't convey the low-coverage flag to screen readers or
          color-vision-deficient users, so state it as text too. */}
      {status === "low" ? <span className="sr-only">Low coverage — at or below 50%</span> : null}
    </a>
  );
}
