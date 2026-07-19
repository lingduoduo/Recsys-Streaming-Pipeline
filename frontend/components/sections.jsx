import { Section, NaCard, BarChart, DataTable } from "./ui";

const num = (v, d = 4) => (v === null || v === undefined ? "N/A" : (Math.round(v * 10 ** d) / 10 ** d).toString());
const pct = (v) => (v === null || v === undefined ? "N/A" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`);
const ci = (lo, hi, asPct = false) =>
  lo === null || lo === undefined || hi === null || hi === undefined
    ? "N/A"
    : asPct
      ? `[${pct(lo)}, ${pct(hi)}]`
      : `[${num(lo)}, ${num(hi)}]`;

export function RelevanceSection({ data }) {
  if (!data) return <NaCard title="Engagement funnel" reason="no relevance data" />;
  const labels = ["impression", "click", "order"];
  return (
    <Section title="Engagement funnel" headline={data.headline}>
      <BarChart labels={labels} values={labels.map((k) => data.funnel[k])} title="Funnel" />
      <DataTable rows={data.by_query} columns={["query", "impressions", "mean_score"]} />
      <DataTable rows={data.by_genre} columns={["genre", "impressions", "mean_score"]} />
    </Section>
  );
}

export function KeywordSection({ data }) {
  if (!data) return <NaCard title="Keyword gap" reason="no keyword data" />;
  return (
    <Section title="Keyword gap" headline={data.headline}>
      <BarChart
        labels={data.by_keyword.map((r) => r.keyword)}
        values={data.by_keyword.map((r) => r.movie_impressions)}
        title="Impressions by keyword"
      />
      <DataTable rows={data.by_keyword} columns={["keyword", "movie_impressions", "query_clicks", "divergence"]} />
      {["l1", "l2", "l3"].map((lvl) => (
        <DataTable key={lvl} rows={data.tops[lvl]} columns={[lvl, "keyword", "movie_impressions", "query_clicks"]} />
      ))}
    </Section>
  );
}

export function QuerySection({ data }) {
  if (!data) return <NaCard title="Query intent" reason="no query data" />;
  return (
    <Section title="Query intent" headline={data.headline}>
      <BarChart
        labels={data.top_queries.map((r) => r.query)}
        values={data.top_queries.map((r) => r.impressions)}
        title="Top queries"
      />
      <DataTable rows={data.top_queries} columns={["query", "impressions", "clicks", "orders", "query_len", "ctr", "cvr"]} />
      <DataTable rows={data.by_length} columns={["bucket", "impressions", "clicks", "orders", "ctr", "cvr"]} />
    </Section>
  );
}

export function RecallSection({ data }) {
  if (!data) return <NaCard title="Recall" reason="no movie:*:features in Redis" />;
  return (
    <Section title="Recall" headline={data.headline}>
      <DataTable rows={data.rows} columns={["method", "k", "recall_at_k", "hitrate_at_k", "users_evaluated"]} />
    </Section>
  );
}

export function RankingSection({ data }) {
  if (!data) return <NaCard title="Ranking" reason="no popularity or i2vEmb:* signals in Redis" />;
  const scored = data.rows.filter((r) => r.auc !== null && r.auc !== undefined);
  return (
    <Section title="Ranking" headline={data.headline}>
      {scored.length ? (
        <BarChart labels={scored.map((r) => r.signal)} values={scored.map((r) => r.auc)} title="AUC by signal" />
      ) : null}
      <DataTable rows={data.rows} columns={["signal", "n", "positives", "coverage", "auc", "logloss"]} />
    </Section>
  );
}

export function OpeSection({ data }) {
  if (!data) return <NaCard title="Off-policy evaluation" reason="no replay-buffer events with reward in Redis" />;
  const disp = data.rows.map((x) => ({
    policy: x.policy,
    value: num(x.value),
    value_95ci: ci(x.value_ci_low, x.value_ci_high),
    lift_vs_logging: pct(x.lift_vs_logging),
    lift_95ci: ci(x.lift_ci_low, x.lift_ci_high, true),
    n: x.n_events,
  }));
  const cal = data.calibration;
  return (
    <Section title="Off-policy evaluation" headline={data.headline}>
      <BarChart labels={data.rows.map((r) => r.policy)} values={data.rows.map((r) => r.value)} title="Estimated policy value" />
      <DataTable rows={disp} columns={["policy", "value", "value_95ci", "lift_vs_logging", "lift_95ci", "n"]} />
      <p className="fine-print">
        Direct Method · reward estimator AUC {num(cal.auc)} MSE {num(cal.mse)} (n_test {cal.n_test}). 95%
        event-bootstrap CIs are conditional on the fixed reward model; model-fit uncertainty excluded.
      </p>
    </Section>
  );
}

export function MdpSection({ data }) {
  if (!data) return <NaCard title="MDP policy evaluation" reason="no mdp_eval.csv" />;
  const rows = data.rows.map((r) => ({ ...r, ci95: `[${num(r.ci95_low, 3)}, ${num(r.ci95_high, 3)}]` }));
  return (
    <Section title="MDP policy evaluation" headline={data.headline}>
      <DataTable rows={rows} columns={["policy", "episodes", "mean_return", "mean_steps", "standard_error", "ci95"]} />
      <p className="fine-print">
        Finite-horizon discounted return over seeded episodes; 95% bootstrap CIs quantify episode-sampling
        uncertainty for this fixed dataset.
      </p>
    </Section>
  );
}
