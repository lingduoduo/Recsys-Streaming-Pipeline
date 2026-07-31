export function Section({ title, headline, children, id }) {
  return (
    <section className="report-card" id={id}>
      <div className="section-heading">
        <h2>{title}</h2>
        {headline ? <p className="insight">{headline}</p> : null}
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

const round4 = (v) => (typeof v === "number" ? Math.round(v * 1e4) / 1e4 : v);

// Horizontal SVG bar chart — mirrors the Python dashboard's svg_bar.
export function BarChart({ labels, values, title, width = 520, barH = 22, gap = 6 }) {
  const vmax = Math.max(...values.filter((v) => Number.isFinite(v)), 0) || 1;
  const h = Math.max(labels.length, 1) * (barH + gap);
  return (
    <svg
      className="chart"
      role="img"
      viewBox={`0 -20 ${width} ${h + 24}`}
      width={width}
      fontFamily="sans-serif"
    >
      {title ? (
        <text x="0" y="-6" fontSize="13" fontWeight="bold">
          {title}
        </text>
      ) : null}
      {labels.map((lab, i) => {
        const v = values[i] ?? 0;
        const y = i * (barH + gap);
        const w = Math.max(0, Math.round((v / vmax) * (width - 160)));
        return (
          <g key={`${lab}-${i}`}>
            <title>{`${lab}: ${round4(v)}`}</title>
            <text x="0" y={y + barH - 6} fontSize="12">
              {lab}
            </text>
            <rect x="150" y={y} width={w} height={barH} rx="6" fill="#4f46e5" />
            <text x={155 + w} y={y + barH - 6} fontSize="11">
              {round4(v)}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

// Two-series horizontal bars sharing one scale — for comparisons where a single
// series would hide the relationship (ndcg vs mrr, fresh vs established).
export function GroupedBarChart({ series, labels, title, width = 520, barH = 14, gap = 6 }) {
  const colors = ["#4f46e5", "#eb6834"];
  const all = series.flatMap((s) => s.values).filter((v) => Number.isFinite(v));
  const vmax = Math.max(...all, 0) || 1;
  const groupH = series.length * (barH + 2) + gap;
  const h = Math.max(labels.length, 1) * groupH;
  return (
    <svg className="chart" role="img" viewBox={`0 -20 ${width} ${h + 24}`} width={width} fontFamily="sans-serif">
      {title ? <text x="0" y="-6" fontSize="13" fontWeight="bold">{title}</text> : null}
      {series.map((s, si) => (
        <text key={s.name} x={String(60 + si * 110)} y="-6" fontSize="11" fill={colors[si % colors.length]}>
          {s.name}
        </text>
      ))}
      {labels.map((label, li) => (
        <g key={`${label}-${li}`}>
          <text x="0" y={li * groupH + barH} fontSize="12">{label}</text>
          {series.map((s, si) => {
            const v = s.values[li] ?? 0;
            const w = Math.max(0, Math.round((v / vmax) * (width - 200)));
            return (
              <g key={s.name}>
                <title>{`${s.name} ${label}: ${v}`}</title>
                <rect x="150" y={li * groupH + si * (barH + 2)} width={w} height={barH} rx="4"
                      fill={colors[si % colors.length]} />
                <text x={155 + w} y={li * groupH + si * (barH + 2) + barH - 2} fontSize="10">{v}</text>
              </g>
            );
          })}
        </g>
      ))}
    </svg>
  );
}

// Render a list of record objects as a table over the given columns.
export function DataTable({ rows, columns }) {
  const cols = columns || (rows.length ? Object.keys(rows[0]) : []);
  return (
    <div className="table-shell">
      <table className="rpt">
        <thead>
          <tr>
            {cols.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => (
                <td key={c}>{formatCell(r[c])}</td>
              ))}
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
