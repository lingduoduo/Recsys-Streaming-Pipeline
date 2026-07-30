import { Section, NaCard, BarChart, DataTable, MetricTile } from "./ui";

const num = (v, d = 4) => (v === null || v === undefined ? "N/A" : (Math.round(v * 10 ** d) / 10 ** d).toString());
const pct = (v) => (v === null || v === undefined ? "N/A" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`);
const share = (v) => (v === null || v === undefined ? "N/A" : `${(v * 100).toFixed(1)}%`);
const ci = (lo, hi, asPct = false) =>
  lo === null || lo === undefined || hi === null || hi === undefined
    ? "N/A"
    : asPct
      ? `[${pct(lo)}, ${pct(hi)}]`
      : `[${num(lo)}, ${num(hi)}]`;

// Which single number represents each measurement on the scorecard. `field` must be a
// key the calculator actually publishes — the contract test enforces that.
const HEADLINES = {
  relevance: { rowIndex: 1, field: "ndcg_at_k", label: "NDCG@10", format: "num" },
  satisfaction: { rowIndex: 0, field: "ctr", label: "CTR", format: "pct" },
  freshness: { rowIndex: 0, field: "fresh_share", label: "fresh share", format: "pct" },
  diversity: { rowIndex: 0, field: "normalized_genre_entropy", label: "genre entropy", format: "num" },
  fairness: { rowIndex: 0, field: "ctr_max_min_gap", label: "largest CTR gap", format: "num" },
  safety: { rowIndex: 0, field: "unsafe_exposure_rate", label: "unsafe exposure", format: "pct" },
  // Endpoint rows are emitted in sorted order over the fixed {feedback, recommend}
  // allowlist, so index 1 is always /recommend.
  latency: { rowIndex: 1, field: "p95", label: "p95 /recommend", format: "ms" },
};

const TITLES = {
  relevance: "Relevance", satisfaction: "Satisfaction", freshness: "Freshness",
  diversity: "Diversity", fairness: "Fairness", safety: "Safety", latency: "Latency",
};

const LOW_COVERAGE = 0.5;

function headlineRow(section, spec) {
  return section.rows?.[spec.rowIndex] ?? section.rows?.[0];
}

// A merged live-only row (see `_merge_live_row` in analysis_dashboard_report.py) can make a
// section "available" without publishing every offline field — e.g. the live safety row has
// no `unsafe_exposure_rate`. Check the key itself, not just its value, so a merged row that
// omits the headline field is never mistaken for a published-but-null one.
function headlineFieldPublished(section, spec) {
  const row = headlineRow(section, spec);
  return !!row && Object.prototype.hasOwnProperty.call(row, spec.field);
}

function headlineValue(section, spec) {
  const value = headlineRow(section, spec)?.[spec.field];
  if (value === null || value === undefined) return "N/A";
  if (spec.format === "pct") return share(value);
  if (spec.format === "ms") return `${num(value, 1)} ms`;
  return num(value, 3);
}

export function Scorecard({ data }) {
  return (
    <section className="scorecard">
      {Object.entries(HEADLINES).map(([key, spec]) => {
        const section = data[key];
        const available = section?.status === "available";
        const published = available && headlineFieldPublished(section, spec);
        const status = !published ? "na" : (section.coverage ?? 1) < LOW_COVERAGE ? "low" : "ok";
        return (
          <MetricTile
            key={key}
            href={`#${key}`}
            title={TITLES[key]}
            value={published ? headlineValue(section, spec) : "N/A"}
            label={spec.label}
            sampleSize={section?.sampleSize}
            status={status}
            reason={published ? null : section?.warnings?.[0] || "measurement unavailable"}
          />
        );
      })}
    </section>
  );
}

// One consistent presentation for every measurement envelope: headline, the support it
// was calculated from, any warnings, then the rows. Never invents a value for N/A.
function MeasurementSection({ title, data, columns, children }) {
  if (!data || data.status !== "available") {
    return <NaCard title={title} reason={data?.warnings?.[0] || "measurement unavailable"} id={title.toLowerCase()} />;
  }
  return (
    <Section title={title} headline={data.headline} id={title.toLowerCase()}>
      <p className="fine-print">
        sample size {data.sampleSize?.toLocaleString() ?? "N/A"} · coverage {share(data.coverage)}
        {data.window ? ` · window ${data.window}` : ""}
      </p>
      {data.warnings?.length ? <p className="na">{data.warnings.join(" · ")}</p> : null}
      <DataTable rows={data.rows || []} columns={columns} />
      {children}
    </Section>
  );
}

export function RelevanceSection({ data }) {
  return (
    <MeasurementSection
      title="Relevance"
      data={data}
      columns={[
        "k", "ndcg_at_k", "mrr_at_k", "recall_at_k", "hit_rate_at_k",
        "evaluated_slate_count", "evaluated_user_count", "label_coverage",
      ]}
    />
  );
}

export function SatisfactionSection({ data }) {
  return (
    <MeasurementSection
      title="Satisfaction"
      data={data}
      columns={[
        "scope", "ctr", "order_rate", "mean_reward", "mean_rating", "rating_coverage",
        "negative_feedback_rate", "negative_feedback_coverage", "mean_dwell_millis",
        "dwell_coverage", "mean_completion_rate", "completion_coverage", "feedback_events",
      ]}
    />
  );
}

export function FreshnessSection({ data }) {
  return (
    <MeasurementSection
      title="Freshness"
      data={data}
      columns={[
        "scope", "freshness_source", "fresh_share", "freshness_coverage",
        "mean_content_age_days", "median_content_age_days", "fresh_ctr", "established_ctr",
        "fresh_mean_reward", "established_mean_reward", "exposures",
      ]}
    />
  );
}

export function DiversitySection({ data }) {
  return (
    <MeasurementSection
      title="Diversity"
      data={data}
      columns={[
        "scope", "slate_id", "unique_genres_at_k", "normalized_genre_entropy",
        "intra_list_genre_distance", "long_tail_exposure_share",
        "long_tail_popularity_cutoff", "genre_coverage", "popularity_coverage",
      ]}
    />
  );
}

export function FairnessSection({ data }) {
  return (
    <MeasurementSection
      title="Fairness"
      data={data}
      columns={[
        "dimension", "evaluated_candidates", "overall_ctr", "overall_order_rate",
        "overall_mean_reward", "overall_ndcg", "evaluated_group_count",
        "suppressed_group_count", "ctr_max_min_gap", "ctr_disparity_ratio",
        "ndcg_max_min_gap", "ndcg_disparity_ratio",
      ]}
    >
      {(data?.rows || []).map((row) =>
        row.groups?.length ? (
          <div className="measurement-groups" key={row.dimension}>
            <p className="fine-print">
              {row.dimension} groups above the support threshold ({row.suppressed_group_count} suppressed)
            </p>
            <DataTable
              rows={row.groups}
              columns={[
                "group", "support", "exposure_share", "ctr", "order_rate", "mean_reward",
                "ndcg", "ndcg_evaluated_slate_count",
              ]}
            />
          </div>
        ) : null,
      )}
      <p className="fine-print">
        Observational only: groups differ in catalog and intent, so a gap is not evidence of
        discriminatory treatment. Groups below the configured support are suppressed.
      </p>
    </MeasurementSection>
  );
}

export function SafetySection({ data }) {
  return (
    <MeasurementSection
      title="Safety"
      data={data}
      columns={[
        "scope", "policy_version", "evaluated_candidates", "filter_decisions",
        "filter_decision_rate", "reason_counts", "unknown_share",
        "unsafe_exposure_rate", "unsafe_label_coverage",
      ]}
    />
  );
}

export function LatencySection({ data }) {
  return (
    <MeasurementSection
      title="Latency"
      data={data}
      columns={["scope", "name", "unit", "p50", "p95", "p99", "count", "error_rate", "timeout_rate"]}
    >
      <p className="fine-print">
        Live service request and stage latency from the retrieval service. Stream lag
        (feedback delay, Kafka ingest lag) is measured separately in the Spark metric events.
      </p>
    </MeasurementSection>
  );
}

export function EngagementSection({ data }) {
  if (!data) return <NaCard title="Engagement funnel" reason="no engagement data" />;
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
