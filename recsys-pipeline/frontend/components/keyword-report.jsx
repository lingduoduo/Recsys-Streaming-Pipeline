"use client";

import { useMemo, useState } from "react";
import { Section, NaCard, BarChart, DataTable, MetricGrid, MetricCard, ChartGrid } from "./ui";

const num = (v, d = 4) => (v === null || v === undefined ? "N/A" : (Math.round(v * 10 ** d) / 10 ** d).toString());
const share = (v) => (v === null || v === undefined ? "N/A" : `${(v * 100).toFixed(1)}%`);
const count = (v) => (v === null || v === undefined ? "N/A" : Number(v).toLocaleString());

const TOP_K_CHOICES = [10, 20, 30, 50];

function Select({ label, value, onChange, options }) {
  return (
    <label className="select-control">
      <span>{label}</span>
      <select value={value} onChange={(e) => onChange(Number(e.target.value))}>
        {options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
    </label>
  );
}

// A row axis down the side, keywords across the top, CTR as heat. A pair the run never
// served is absent from the rows and renders hatched rather than at the cold end of the
// ramp -- never served and served-but-never-clicked are different facts.
//
// markDiagonal outlines cells whose row value equals their keyword. On the topic grid those
// are forced: an item whose primary genre is Action always carries Action. The value is true,
// so the cell keeps it, but it is filled by construction rather than measured.
function RelevanceHeatmap({ rows, rowKey, rowLabel, markDiagonal = false }) {
  if (!rows?.length) {
    return (
      <p className="fine-print">
        No {rowLabel} grid in this snapshot — it predates the grid, so re-export to populate it.
      </p>
    );
  }
  const categories = [...new Set(rows.map((r) => r[rowKey]))].sort();
  const keywords = [...new Set(rows.map((r) => r.keyword))].sort();
  const cells = new Map();
  for (const r of rows) {
    if (!cells.has(r[rowKey])) cells.set(r[rowKey], new Map());
    cells.get(r[rowKey]).set(r.keyword, r);
  }
  // Heat is relative to the busiest cell in this grid, so the ramp always spans it.
  const max = Math.max(...rows.map((r) => r.ctr ?? 0), 0);

  return (
    <div className="table-shell">
      <table className="rpt compact heat-grid">
        <thead>
          <tr>
            <th scope="col">{rowLabel}</th>
            {keywords.map((k) => <th key={k} scope="col" className="num">{k}</th>)}
          </tr>
        </thead>
        <tbody>
          {categories.map((category) => (
            <tr key={category}>
              <th scope="row">{category}</th>
              {keywords.map((keyword) => {
                const cell = cells.get(category)?.get(keyword);
                if (!cell) {
                  return (
                    <td key={keyword} className="num heat-absent"
                      title={`${category} / ${keyword}: never served`} />
                  );
                }
                const t = max === 0 ? 0 : (cell.ctr ?? 0) / max;
                const forced = markDiagonal && category === keyword;
                return (
                  <td key={keyword} className={forced ? "num heat-cell heat-forced" : "num heat-cell"}
                    style={{ "--token-score": t }}
                    title={`${category} / ${keyword}: CTR ${share(cell.ctr)} over ${count(cell.movie_impressions)} impressions`}>
                    {share(cell.ctr)}
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

function TokenHeatmap({ items, labelKey, scoreKey, selectedKey, onSelect }) {
  const scores = items.map((i) => i[scoreKey]).filter((s) => s !== null && s !== undefined);
  const min = scores.length ? Math.min(...scores) : 0;
  const max = scores.length ? Math.max(...scores) : 0;
  const normalize = (s) => (s === null || s === undefined ? null : max === min ? 0.5 : (s - min) / (max - min));

  return (
    <div className="token-heatmap">
      {items.map((item, i) => {
        const label = item[labelKey];
        const score = item[scoreKey];
        const t = normalize(score);
        // A keyword with no score is rendered unfilled rather than at the cold end
        // of the ramp, which would read as "measured, and lowest".
        const style = { "--token-score": t === null ? 0 : t };
        const className = [
          "token-chip",
          t === null ? "unscored" : "",
          selectedKey === label ? "selected" : "",
        ].filter(Boolean).join(" ");

        return (
          <button key={`${label}-${i}`} type="button" className={className} style={style}
            title={`${label}: ${num(score, 4)}`} aria-pressed={selectedKey === label}
            onClick={() => onSelect(item)}>
            <span>{label}</span>
            <small>{num(score, 3)}</small>
          </button>
        );
      })}
    </div>
  );
}

function KeywordDetail({ keyword }) {
  if (!keyword) {
    return <aside className="keyword-detail-panel"><p className="empty-state">Select a keyword to inspect it.</p></aside>;
  }
  return (
    <aside className="keyword-detail-panel">
      <span className="eyebrow">Keyword detail</span>
      <h3 className="keyword-detail-title">{keyword.keyword}</h3>
      <div className="keyword-detail-metrics">
        <MetricCard label="Rank" value={`#${keyword.rank}`} />
        <MetricCard label="Relevance" value={num(keyword.mean_score, 3)} detail="mean label" />
        <MetricCard label="Impressions" value={count(keyword.movie_impressions)} />
        <MetricCard label="Clicks" value={count(keyword.query_clicks)} />
        <MetricCard label="CTR" value={share(keyword.ctr)} />
        <MetricCard label="CVR" value={share(keyword.cvr)} />
      </div>
      <h4 className="keyword-detail-subtitle">Shown versus clicked</h4>
      <BarChart title={null} percentage
        labels={["share of impressions", "share of clicks"]}
        values={[keyword.movie_share, keyword.query_share]} />
      <p className="fine-print">
        Divergence {num(keyword.divergence, 4)} is the second share minus the first: positive means
        the keyword earns a larger share of clicks than of exposure.
      </p>
    </aside>
  );
}

export function KeywordSection({ data }) {
  const available = data?.by_keyword ?? [];
  const [topK, setTopK] = useState(20);
  const [selectedKeyword, setSelectedKeyword] = useState(null);

  // Never offer a cutoff larger than the pool: "Top 50" over twelve keywords is a
  // promise the data cannot keep.
  const options = useMemo(() => {
    const fits = TOP_K_CHOICES.filter((k) => k < available.length).map((k) => ({ value: k, label: `Top ${k}` }));
    return [...fits, { value: available.length, label: `All (${available.length})` }];
  }, [available.length]);

  // The default cutoff may exceed a small pool, which would leave the select with
  // a value matching no option.
  const activeK = options.some((o) => o.value === topK) ? topK : options[options.length - 1].value;

  // Ranked by relevance, with unscored keywords last rather than sorted as zero.
  const keywords = useMemo(() => {
    const scored = available.filter((r) => r.mean_score !== null && r.mean_score !== undefined);
    const unscored = available.filter((r) => r.mean_score === null || r.mean_score === undefined);
    return [...scored.sort((a, b) => b.mean_score - a.mean_score), ...unscored]
      .slice(0, activeK)
      .map((row, i) => ({ ...row, rank: i + 1 }));
  }, [available, activeK]);

  // After the hooks, never before: hook order must not depend on the data.
  if (!available.length) {
    return <NaCard title="Keyword relevance" id="keyword" reason="no keyword rows in the snapshot" />;
  }

  const selected = keywords.find((r) => r.keyword === selectedKeyword) ?? keywords[0] ?? null;
  const best = keywords[0];
  const impressions = keywords.reduce((sum, r) => sum + Number(r.movie_impressions ?? 0), 0);
  const byDivergence = [...keywords].sort((a, b) => Math.abs(b.divergence ?? 0) - Math.abs(a.divergence ?? 0));

  return (
    <Section title="Keyword relevance" headline={data.headline} id="keyword"
      description="Catalog keywords ranked by relevance and colored by it. Select one to inspect how its exposure compares with its clicks."
      actions={<Select label="Show" value={activeK} onChange={setTopK} options={options} />}>
      <MetricGrid>
        <MetricCard label="Keywords shown" value={count(keywords.length)} detail={`of ${available.length} exported`} />
        <MetricCard label="Highest relevance" value={num(best?.mean_score, 3)} detail={best?.keyword} />
        <MetricCard label="Impressions covered" value={count(impressions)} />
        <MetricCard label="Widest divergence"
          value={num(byDivergence[0]?.divergence, 3)}
          detail={byDivergence[0]?.keyword} />
      </MetricGrid>

      <div className="keyword-report-layout">
        <div className="keyword-main-panel">
          <TokenHeatmap items={keywords} labelKey="keyword" scoreKey="mean_score"
            selectedKey={selected?.keyword} onSelect={(item) => setSelectedKeyword(item.keyword)} />
          <div className="token-legend">
            <span>lower relevance</span>
            <div className="token-legend-gradient" />
            <span>higher relevance</span>
          </div>
        </div>
        <KeywordDetail keyword={selected} />
      </div>

      <ChartGrid>
        <BarChart title="Impressions by keyword" horizontal
          labels={keywords.map((r) => r.keyword)} values={keywords.map((r) => r.movie_impressions)} />
        <BarChart title="Click-to-exposure divergence" horizontal signed
          labels={keywords.map((r) => r.keyword)} values={keywords.map((r) => r.divergence)} />
      </ChartGrid>

      <DataTable rows={keywords}
        columns={["rank", "keyword", "mean_score", "movie_impressions", "query_clicks",
                  "query_orders", "ctr", "cvr", "divergence"]}
        formatters={{
          mean_score: (v) => num(v, 4),
          movie_impressions: count, query_clicks: count, query_orders: count,
          ctr: share, cvr: share, divergence: (v) => num(v, 4),
        }} />

      <h3 className="report-subtitle">Relevance by category and keyword</h3>
      <RelevanceHeatmap rows={data.grid} rowKey="category" rowLabel="category" />

      <h3 className="report-subtitle">Relevance by topic and keyword</h3>
      <p className="fine-print">
        Topic is the item&apos;s primary genre. Outlined cells are forced: an item whose primary
        genre is Action always carries Action, so those cells are filled by construction rather
        than measured.
      </p>
      <RelevanceHeatmap rows={data.topic_grid} rowKey="topic" rowLabel="topic" markDiagonal />

      {["l1", "l2", "l3"].map((level) => {
        const rows = data.tops?.[level] ?? [];
        if (!rows.length) return null;
        return (
          <div key={level}>
            <h3 className="report-subtitle">Taxonomy level {level.toUpperCase()}</h3>
            <DataTable rows={rows} compact
              columns={[level, "keyword", "movie_impressions", "query_clicks", "ctr"]}
              formatters={{ movie_impressions: count, query_clicks: count, ctr: share }} />
          </div>
        );
      })}

      <p className="fine-print">
        Relevance is the mean label over a keyword&apos;s impressions. The pool is the {available.length}{" "}
        most-shown keywords, so this ranks relevance within them rather than across the whole catalog.
      </p>
    </Section>
  );
}
