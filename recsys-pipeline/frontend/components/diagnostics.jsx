import {
  Section, NaCard, BarChart, DataTable, MetricGrid, MetricCard, ChartGrid,
} from "./ui";
import {
  num, pct, share, ci, count, rankBy, COUNT_COLUMNS, RATE_COLUMNS,
} from "./format";

export function EngagementSection({ data }) {
  if (!data) return <NaCard title="Engagement funnel" id="engagement" reason="no engagement data" />;
  const funnel = data.funnel || {};
  return (
    <Section title="Engagement funnel" headline={data.headline} id="engagement"
      description="Traffic from recommendation impression through click to order, split by query and by genre.">
      <MetricGrid>
        <MetricCard label="Impressions" value={count(funnel.impression)} />
        <MetricCard label="Clicks" value={count(funnel.click)} detail={`${share(data.ctr)} CTR`} />
        <MetricCard label="Orders" value={count(funnel.order)} detail={`${share(data.cvr)} CVR`} />
        <MetricCard label="Queries shown" value={count((data.by_query || []).length)} detail="top by relevance" />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Funnel"
          labels={["impression", "click", "order"]}
          values={[funnel.impression, funnel.click, funnel.order]} />
        <BarChart title="CTR by genre" percentage
          labels={(data.by_genre || []).map((r) => r.genre)}
          values={(data.by_genre || []).map((r) => r.ctr)} />
      </ChartGrid>
      <h3 className="report-subtitle">By query</h3>
      <DataTable rows={data.by_query} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS, mean_score: (v) => num(v, 4) }}
        columns={["query", "impressions", "clicks", "orders", "ctr", "cvr", "mean_score"]} />
      <h3 className="report-subtitle">By genre</h3>
      <DataTable rows={data.by_genre} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS, mean_score: (v) => num(v, 4) }}
        columns={["genre", "impressions", "clicks", "orders", "ctr", "cvr", "mean_score"]} />
      <p className="fine-print">
        CVR is orders per impression, matching the rate the exporter publishes everywhere else
        on this page — not orders per click. <code>mean_score</code> is the mean label.
      </p>
    </Section>
  );
}

export function QuerySection({ data }) {
  if (!data) return <NaCard title="Query intent" id="query" reason="no query data" />;
  const rows = data.top_queries || [];
  const byCtr = rankBy(rows, "ctr");
  const byCvr = rankBy(rows, "cvr");
  return (
    <Section title="Query intent" headline={data.headline} id="query"
      description="Query demand, engagement, conversion and intent depth.">
      <MetricGrid>
        <MetricCard label="Queries analyzed" value={count(rows.length)} />
        <MetricCard label="Highest CTR" value={share(byCtr[0]?.ctr)} detail={byCtr[0]?.query} />
        <MetricCard label="Highest CVR" value={share(byCvr[0]?.cvr)} detail={byCvr[0]?.query} />
        <MetricCard label="Mean query length" value={num(data.average_query_length, 2)} detail="characters" />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Top queries by impressions" horizontal
          labels={rows.map((r) => r.query)} values={rows.map((r) => r.impressions)} />
        <BarChart title="Top queries by CTR" horizontal percentage
          labels={byCtr.map((r) => r.query)} values={byCtr.map((r) => r.ctr)} />
      </ChartGrid>
      <DataTable rows={rows} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS }}
        columns={["query", "impressions", "clicks", "orders", "query_len", "ctr", "cvr"]} />
      <h3 className="report-subtitle">By query length</h3>
      <DataTable rows={data.by_length} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS }}
        columns={["bucket", "queries", "impressions", "clicks", "orders", "ctr", "cvr"]} />
      <p className="fine-print">
        The pool is the {(data.top_queries || []).length} most-shown queries, so the CTR and CVR
        highs above are the best within them rather than across every query observed.
      </p>
    </Section>
  );
}

export function RecallSection({ data }) {
  if (!data) return <NaCard title="Candidate recall" id="recall" reason="no movie:*:features in Redis" />;
  const rows = data.rows || [];
  const byRecall = rankBy(rows, "recall_at_k");
  const byHitRate = rankBy(rows, "hitrate_at_k");
  return (
    <Section title="Candidate recall" headline={data.headline} id="recall"
      description="Candidate-generation quality across retrieval strategies and cutoffs.">
      <MetricGrid>
        <MetricCard label="Best recall@K" value={num(byRecall[0]?.recall_at_k, 3)}
          detail={byRecall[0] ? `${byRecall[0].method}, K=${byRecall[0].k}` : undefined} />
        <MetricCard label="Best hit rate@K" value={num(byHitRate[0]?.hitrate_at_k, 3)}
          detail={byHitRate[0] ? `${byHitRate[0].method}, K=${byHitRate[0].k}` : undefined} />
        <MetricCard label="Methods" value={count(new Set(rows.map((r) => r.method)).size)} />
        <MetricCard label="Users evaluated" value={count(rankBy(rows, "users_evaluated")[0]?.users_evaluated)} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Recall@K" horizontal
          labels={rows.map((r) => `${r.method} @ ${r.k}`)} values={rows.map((r) => r.recall_at_k)} />
        <BarChart title="Hit rate@K" horizontal
          labels={rows.map((r) => `${r.method} @ ${r.k}`)} values={rows.map((r) => r.hitrate_at_k)} />
      </ChartGrid>
      <DataTable rows={rows} formatters={COUNT_COLUMNS}
        columns={["method", "k", "recall_at_k", "hitrate_at_k", "users_evaluated", "instances"]} />
    </Section>
  );
}

export function RankingSection({ data }) {
  if (!data) return <NaCard title="Ranking quality" id="ranking" reason="no popularity or i2vEmb:* signals in Redis" />;
  const rows = data.rows || [];
  const byAuc = rankBy(rows, "auc");
  const byLogloss = rankBy(rows, "logloss", "asc");
  const byCoverage = rankBy(rows, "coverage");
  return (
    <Section title="Ranking quality" headline={data.headline} id="ranking"
      description="Predictive quality and coverage of each available ranking signal.">
      <MetricGrid>
        <MetricCard label="Best AUC" value={num(byAuc[0]?.auc, 3)} detail={byAuc[0]?.signal} />
        <MetricCard label="Lowest log loss" value={num(byLogloss[0]?.logloss, 3)} detail={byLogloss[0]?.signal} />
        <MetricCard label="Best coverage" value={share(byCoverage[0]?.coverage)} detail={byCoverage[0]?.signal} />
        <MetricCard label="Signals evaluated" value={count(rows.length)} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="AUC by signal" horizontal
          labels={rows.map((r) => r.signal)} values={rows.map((r) => r.auc)} />
        <BarChart title="Coverage by signal" horizontal percentage
          labels={rows.map((r) => r.signal)} values={rows.map((r) => r.coverage)} />
      </ChartGrid>
      <DataTable rows={rows} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS }}
        columns={["signal", "n", "positives", "positive_rate", "coverage", "auc", "logloss"]} />
      <p className="fine-print">
        A signal with no scored rows reports no AUC and no positive rate rather than zero, and
        is omitted from the charts above.
      </p>
    </Section>
  );
}

export function OpeSection({ data }) {
  if (!data) return <NaCard title="Off-policy evaluation" id="ope" reason="no replay events with reward — this repository writes no replay:recommendations (the serving path does); pass --ope-parquet to read a scored replay from post-training instead" />;
  const rows = data.rows || [];
  const cal = data.calibration || {};
  const byValue = rankBy(rows, "value");
  const byLift = rankBy(rows, "lift_vs_logging");
  const disp = rows.map((x) => ({
    policy: x.policy,
    value: num(x.value),
    value_95ci: ci(x.value_ci_low, x.value_ci_high),
    lift_vs_logging: pct(x.lift_vs_logging),
    lift_95ci: ci(x.lift_ci_low, x.lift_ci_high, true),
    n: count(x.n_events),
  }));
  return (
    <Section title="Off-policy evaluation" headline={data.headline} id="ope"
      description="Estimated value and lift of each candidate policy on logged events.">
      <MetricGrid>
        <MetricCard label="Best policy" value={byValue[0]?.policy ?? "N/A"} detail={`value ${num(byValue[0]?.value, 3)}`} />
        <MetricCard label="Highest lift" value={pct(byLift[0]?.lift_vs_logging)} detail={byLift[0]?.policy} />
        <MetricCard label="Reward model AUC" value={num(cal.auc, 3)} />
        <MetricCard label="Reward model MSE" value={num(cal.mse, 4)} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Estimated policy value" horizontal
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.value)} />
        <BarChart title="Lift vs logging policy" horizontal signed valueFormatter={pct}
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.lift_vs_logging)} />
      </ChartGrid>
      <DataTable rows={disp} columns={["policy", "value", "value_95ci", "lift_vs_logging", "lift_95ci", "n"]} />
      <p className="fine-print">
        Direct Method · source {data.source ?? "unrecorded (snapshot predates source tracking)"} · reward
        estimator AUC {num(cal.auc)} MSE {num(cal.mse)} (n_test {count(cal.n_test)}). 95%
        event-bootstrap CIs are conditional on the fixed reward model; model-fit uncertainty excluded.
      </p>
    </Section>
  );
}
