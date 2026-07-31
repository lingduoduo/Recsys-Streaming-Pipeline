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


"use client";

import { useId } from "react";

export function Section({
  title,
  headline,
  description,
  actions,
  children,
}) {
  return (
    <section className="report-section">
      <div className="section-header">
        <div>
          <p className="eyebrow">Recommender analytics</p>
          <h2>{title}</h2>

          {headline ? (
            <div className="headline-chip">{headline}</div>
          ) : null}

          {description ? (
            <p className="section-description">{description}</p>
          ) : null}
        </div>

        {actions ? (
          <div className="section-actions">{actions}</div>
        ) : null}
      </div>

      <div className="section-content">{children}</div>
    </section>
  );
}

export function NaCard({ title, reason }) {
  return (
    <div className="na-card">
      <h2>{title}</h2>
      <p>{reason}</p>
    </div>
  );
}

export function MetricGrid({ children }) {
  return <div className="metric-grid">{children}</div>;
}

export function MetricCard({
  label,
  value,
  detail,
  trend,
}) {
  return (
    <div className="metric-card">
      <span className="metric-label">{label}</span>
      <strong className="metric-value">{value ?? "N/A"}</strong>

      {detail ? (
        <span className="metric-detail">{detail}</span>
      ) : null}

      {trend ? (
        <span
          className={
            trend >= 0
              ? "metric-trend positive"
              : "metric-trend negative"
          }
        >
          {trend >= 0 ? "▲" : "▼"}{" "}
          {Math.abs(trend * 100).toFixed(1)}%
        </span>
      ) : null}
    </div>
  );
}

export function ReportGrid({ children }) {
  return <div className="report-grid">{children}</div>;
}

export function Select({
  label,
  value,
  onChange,
  options,
}) {
  const id = useId();

  return (
    <label className="select-control" htmlFor={id}>
      <span>{label}</span>

      <select id={id} value={value} onChange={onChange}>
        {options.map((option) => (
          <option
            key={option.value}
            value={option.value}
          >
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export function BarChart({
  labels,
  values,
  title,
  horizontal = false,
  percentage = false,
  valueFormatter,
}) {
  const maxValue = Math.max(
    1,
    ...values.map((value) =>
      Math.abs(Number(value ?? 0))
    )
  );

  const formatValue = (value) => {
    if (valueFormatter) return valueFormatter(value);

    if (percentage) {
      return `${(Number(value ?? 0) * 100).toFixed(1)}%`;
    }

    return Number(value ?? 0).toLocaleString();
  };

  return (
    <div className="chart-card">
      <h3>{title}</h3>

      <div
        className={
          horizontal ? "bar-chart horizontal" : "bar-chart"
        }
      >
        {labels.map((label, index) => {
          const value = Number(values[index] ?? 0);
          const width = `${Math.max(
            2,
            (Math.abs(value) / maxValue) * 100
          )}%`;

          return (
            <div
              className="bar-row"
              key={`${label}-${index}`}
            >
              <span className="bar-label" title={label}>
                {label}
              </span>

              <div className="bar-track">
                <div
                  className={
                    value < 0
                      ? "bar-fill negative"
                      : "bar-fill"
                  }
                  style={{ width }}
                />
              </div>

              <span className="bar-value">
                {formatValue(value)}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function DataTable({
  rows = [],
  columns = [],
  formatters = {},
  compact = false,
}) {
  if (!rows.length) {
    return <p className="empty-state">No rows available.</p>;
  }

  return (
    <div className="table-wrapper">
      <table
        className={compact ? "data-table compact" : "data-table"}
      >
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>
                {column
                  .replaceAll("_", " ")
                  .replace(/\b\w/g, (char) =>
                    char.toUpperCase()
                  )}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={row.id ?? rowIndex}>
              {columns.map((column) => {
                const formatter = formatters[column];
                const rawValue = row[column];
                const value = formatter
                  ? formatter(rawValue, row)
                  : rawValue;

                return (
                  <td key={column}>
                    {value === null ||
                    value === undefined ||
                    value === ""
                      ? "N/A"
                      : String(value)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function TokenHeatmap({
  items = [],
  labelKey,
  scoreKey,
  selectedKey,
  onSelect,
  compact = false,
}) {
  const scores = items.map((item) =>
    Number(item[scoreKey] ?? 0)
  );

  const min = Math.min(...scores, 0);
  const max = Math.max(...scores, 1);

  const normalizedScore = (score) => {
    if (max === min) return 0.5;
    return (Number(score ?? 0) - min) / (max - min);
  };

  return (
    <div
      className={
        compact
          ? "token-heatmap compact"
          : "token-heatmap"
      }
    >
      {items.map((item, index) => {
        const label = item[labelKey];
        const score = Number(item[scoreKey] ?? 0);
        const normalized = normalizedScore(score);

        const style = {
          "--token-score": normalized,
          "--token-size": compact
            ? "1rem"
            : `${0.92 + normalized * 0.26}rem`,
        };

        const selected = selectedKey === label;

        if (onSelect) {
          return (
            <button
              key={`${label}-${index}`}
              type="button"
              className={
                selected
                  ? "token-chip selected"
                  : "token-chip"
              }
              style={style}
              onClick={() => onSelect(item)}
              title={`${label}: ${score.toFixed(4)}`}
            >
              <span>{label}</span>
              {!compact ? (
                <small>{score.toFixed(3)}</small>
              ) : null}
            </button>
          );
        }

        return (
          <span
            key={`${label}-${index}`}
            className="token-chip static"
            style={style}
            title={`${label}: ${score.toFixed(4)}`}
          >
            <span>{label}</span>
            {!compact ? (
              <small>{score.toFixed(3)}</small>
            ) : null}
          </span>
        );
      })}
    </div>
  );
}