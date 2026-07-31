import {
  Section, NaCard, BarChart, GroupedBarChart, DataTable, MetricTile,
  MetricGrid, MetricCard, ChartGrid,
} from "./ui";

const num = (v, d = 4) => (v === null || v === undefined ? "N/A" : (Math.round(v * 10 ** d) / 10 ** d).toString());
const pct = (v) => (v === null || v === undefined ? "N/A" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`);
const share = (v) => (v === null || v === undefined ? "N/A" : `${(v * 100).toFixed(1)}%`);
const ci = (lo, hi, asPct = false) =>
  lo === null || lo === undefined || hi === null || hi === undefined
    ? "N/A"
    : asPct
      ? `[${pct(lo)}, ${pct(hi)}]`
      : `[${num(lo)}, ${num(hi)}]`;

const count = (v) => (v === null || v === undefined ? "N/A" : Number(v).toLocaleString());

// Rank by a field, dropping rows that have no value for it. Treating a missing
// value as zero would let "best AUC" name a signal that was never scored.
const rankBy = (rows, field, direction = "desc") =>
  (rows ?? [])
    .filter((r) => r?.[field] !== null && r?.[field] !== undefined)
    .sort((a, b) => (direction === "desc" ? b[field] - a[field] : a[field] - b[field]));

const COUNT_COLUMNS = {
  impressions: count, clicks: count, orders: count, queries: count,
  movie_impressions: count, query_clicks: count, query_orders: count,
  users_evaluated: count, instances: count, n: count, positives: count,
  episodes: count,
};
const RATE_COLUMNS = { ctr: share, cvr: share, coverage: share, positive_rate: share };

// The row a section's headline is read from, when it is not a fixed index. Fairness emits one
// row per demographic dimension in DEFAULT_DIMENSIONS order, not in gap order, so rows[0] is
// whichever dimension sorts first — never "the largest gap" the tile claims to show.
function maxByField(rows, field) {
  return rows.reduce(
    (best, row) =>
      row?.[field] !== null && row?.[field] !== undefined && (best === null || row[field] > best[field])
        ? row
        : best,
    null,
  );
}

// Which single number represents each measurement on the scorecard. `field` must be a
// key the calculator actually publishes — the contract test enforces that. `select: "max"`
// resolves the row by the largest value of `field` instead of by `rowIndex`, which stays as
// the fallback for a section where no row published the field.
const HEADLINES = {
  relevance: { rowIndex: 1, field: "ndcg_at_k", label: "NDCG@10", format: "num" },
  satisfaction: { rowIndex: 0, field: "ctr", label: "CTR", format: "pct" },
  freshness: { rowIndex: 0, field: "fresh_share", label: "fresh share", format: "pct" },
  diversity: { rowIndex: 0, field: "normalized_genre_entropy", label: "genre entropy", format: "num" },
  fairness: { rowIndex: 0, field: "ctr_max_min_gap", select: "max", label: "largest CTR gap", format: "num" },
  safety: { rowIndex: 0, field: "unsafe_exposure_rate", label: "unsafe exposure", format: "pct" },
  // Endpoint rows are emitted in sorted order over the fixed {feedback, recommend}
  // allowlist, so index 1 is always /recommend.
  latency: { rowIndex: 1, field: "p95", label: "p95 /recommend", format: "ms" },
};

const TITLES = {
  relevance: "Relevance", satisfaction: "Satisfaction", freshness: "Freshness",
  diversity: "Diversity", fairness: "Fairness", safety: "Safety", latency: "Latency",
};

// Coverage AT or below this is amber: half the envelope missing is not a green tile. The
// safety section is the live example — with only `unsafe_label` instrumented offline, its
// coverage lands on exactly 0.50.
const LOW_COVERAGE = 0.5;

function headlineRow(section, spec) {
  const rows = section.rows ?? [];
  const selected = spec.select === "max" ? maxByField(rows, spec.field) : null;
  return selected ?? rows[spec.rowIndex] ?? rows[0];
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
        const status = !published ? "na" : (section.coverage ?? 1) <= LOW_COVERAGE ? "low" : "ok";
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
function MeasurementSection({ title, data, columns, kpis, chart, description, children }) {
  if (!data || data.status !== "available") {
    return <NaCard title={title} id={title.toLowerCase()} reason={data?.warnings?.[0] || "measurement unavailable"} />;
  }
  const rows = data.rows || [];
  const values = kpis ? kpis(rows) : [];
  return (
    <Section title={title} headline={data.headline} description={description} id={title.toLowerCase()}>
      <p className="fine-print">
        sample size {data.sampleSize?.toLocaleString() ?? "N/A"} · coverage {share(data.coverage)}
        {data.window ? ` · window ${data.window}` : ""}
      </p>
      {data.warnings?.length ? <p className="na">{data.warnings.join(" · ")}</p> : null}
      {values.length ? (
        <MetricGrid>
          {values.map((kpi) => (
            <MetricCard key={kpi.label} label={kpi.label} value={kpi.value} detail={kpi.detail} />
          ))}
        </MetricGrid>
      ) : null}
      {chart ? chart(rows) : null}
      <DataTable rows={rows} columns={columns} />
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
        "evaluated_slate_count", "ndcg_evaluated_slate_count", "evaluated_user_count", "label_coverage",
      ]}
      kpis={(rows) => {
        const row = rows.find((r) => r.k === 10) || rows[0] || {};
        return [
          { label: "NDCG@10", value: num(row.ndcg_at_k, 3) },
          { label: "MRR@10", value: num(row.mrr_at_k, 3) },
          { label: "recall@10", value: num(row.recall_at_k, 3) },
          { label: "slates", value: (row.evaluated_slate_count ?? 0).toLocaleString() },
          // NDCG is undefined for a slate with no positive label, so those slates are dropped:
          // this, not the slate count above, is the denominator the NDCG mean is taken over.
          { label: "NDCG slates (≥1 positive)", value: (row.ndcg_evaluated_slate_count ?? 0).toLocaleString() },
        ];
      }}
      description="Ranking quality of the served slates at each cutoff, over slates that carry at least one positive label."
      chart={(rows) => (
        <ChartGrid>
          <GroupedBarChart
            title="Relevance by cutoff"
            labels={rows.map((r) => `k=${r.k}`)}
            series={[
              { name: "ndcg", values: rows.map((r) => r.ndcg_at_k) },
              { name: "mrr", values: rows.map((r) => r.mrr_at_k) },
            ]}
          />
          <BarChart
            title="Recall by cutoff"
            labels={rows.map((r) => `k=${r.k}`)}
            values={rows.map((r) => r.recall_at_k)}
          />
        </ChartGrid>
      )}
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
      kpis={(rows) => {
        const row = rows[0] || {};
        return [
          { label: "CTR", value: share(row.ctr) },
          { label: "order rate", value: share(row.order_rate) },
          { label: "mean rating", value: num(row.mean_rating, 2) },
          { label: "mean dwell", value: num(row.mean_dwell_millis, 0) },
        ];
      }}
      description="Observed engagement and the coverage of each optional feedback signal."
      chart={(rows) => {
        const row = rows[0] || {};
        // negative_feedback_coverage is the same expression as negative_feedback_rate (both count
        // the samples carrying a reason), so plotting it here would read as "not instrumented"
        // for a signal that is instrumented and simply did not fire. It stays in the table
        // beside its rate, where the two are legible together.
        const fields = ["rating_coverage", "dwell_coverage", "completion_coverage"];
        return (
          <ChartGrid>
            <BarChart title="Optional signal coverage" percentage
              labels={fields.map((f) => f.replace("_coverage", ""))}
              values={fields.map((f) => row[f])} />
            <BarChart title="Engagement rates" percentage
              labels={["ctr", "order rate", "negative feedback"]}
              values={[row.ctr, row.order_rate, row.negative_feedback_rate]} />
          </ChartGrid>
        );
      }}
    >
      <p className="fine-print">
        Negative feedback <em>coverage</em> is charted nowhere because it is the same
        expression as the negative feedback rate — a sample only carries a reason when the
        feedback fired — so a coverage bar would show an instrumented signal as
        uninstrumented. The rate itself is charted above, and both columns are in the table.
      </p>
    </MeasurementSection>
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
      kpis={(rows) => {
        const row = rows[0] || {};
        return [
          { label: "fresh share", value: share(row.fresh_share) },
          { label: "mean age (days)", value: num(row.mean_content_age_days, 1) },
          { label: "fresh CTR", value: share(row.fresh_ctr) },
          { label: "established CTR", value: share(row.established_ctr) },
        ];
      }}
      description="How much of what was shown is recent, and whether recency tracks engagement."
      chart={(rows) => {
        const row = rows[0] || {};
        return (
          <ChartGrid>
            <BarChart title="CTR by content age" percentage
              labels={["fresh", "established"]}
              values={[row.fresh_ctr, row.established_ctr]} />
            <BarChart title="Mean reward by content age"
              labels={["fresh", "established"]}
              values={[row.fresh_mean_reward, row.established_mean_reward]} />
          </ChartGrid>
        );
      }}
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
      kpis={(rows) => {
        const row = rows.find((r) => r.scope === "aggregate") || rows[0] || {};
        return [
          { label: "genre entropy", value: num(row.normalized_genre_entropy, 3) },
          { label: "unique genres", value: num(row.unique_genres_at_k, 2) },
          { label: "intra-list distance", value: num(row.intra_list_genre_distance, 3) },
          { label: "long-tail share", value: share(row.long_tail_exposure_share) },
        ];
      }}
      description="Genre spread and long-tail exposure within a slate, on a 0–1 scale."
      chart={(rows) => {
        const row = rows.find((r) => r.scope === "aggregate") || rows[0] || {};
        return (
          <BarChart title="Diversity (0–1)"
            labels={["genre entropy", "intra-list distance", "long-tail share"]}
            values={[row.normalized_genre_entropy, row.intra_list_genre_distance,
                     row.long_tail_exposure_share]} />
        );
      }}
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
      kpis={(rows) => {
        // Same row the scorecard headlines: the widest CTR gap, not the first dimension.
        const row = maxByField(rows, "ctr_max_min_gap") || rows[0] || {};
        return [
          { label: "overall CTR", value: share(row.overall_ctr) },
          { label: `largest CTR gap (${row.dimension ?? "N/A"})`, value: num(row.ctr_max_min_gap, 3) },
          { label: "groups", value: String(row.evaluated_group_count ?? 0) },
          { label: "suppressed", value: String(row.suppressed_group_count ?? 0) },
        ];
      }}
      description="Engagement and ranking quality by demographic group, for the dimension with the widest CTR gap."
      chart={(rows) => {
        const row = maxByField(rows, "ctr_max_min_gap") || rows[0] || {};
        const groups = row.groups || [];
        return groups.length ? (
          <ChartGrid>
            <BarChart title={`CTR by ${row.dimension} (overall ${num(row.overall_ctr, 3)})`}
              labels={groups.map((g) => g.group)} values={groups.map((g) => g.ctr)} percentage />
            <BarChart title={`NDCG by ${row.dimension} (overall ${num(row.overall_ndcg, 3)})`}
              labels={groups.map((g) => g.group)} values={groups.map((g) => g.ndcg)} />
          </ChartGrid>
        ) : null;
      }}
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
      kpis={(rows) => {
        const offline = rows[0] || {};
        const filtered = rows.find((r) => r.filter_decision_rate !== null && r.filter_decision_rate !== undefined) || {};
        return [
          { label: "unsafe exposure", value: share(offline.unsafe_exposure_rate) },
          { label: "label coverage", value: share(offline.unsafe_label_coverage) },
          // Not a rejection rate: a decision is recorded for allowed candidates too, and an
          // `unknown` decision means the policy could not classify the candidate at all.
          { label: "candidates with a logged decision", value: share(filtered.filter_decision_rate) },
          { label: "candidates unclassified (unknown)", value: share(filtered.unknown_share) },
          { label: "policy", value: String(offline.policy_version ?? filtered.policy_version ?? "N/A") },
        ];
      }}
      description="Policy filter decisions over evaluated candidates, and the share the policy could not classify."
      chart={(rows) => {
        // A row can carry the reason keys with every count null (nothing was logged); pick the
        // first row that actually observed decisions rather than the first row that has the keys.
        const counts = rows
          .map((r) => r.reason_counts)
          .find((c) => c && Object.values(c).some((v) => v !== null && v !== undefined)) || {};
        const entries = Object.entries(counts).filter(([, v]) => v !== null && v !== undefined);
        return entries.length ? (
          <BarChart title="Filter decisions by reason"
            labels={entries.map(([k]) => k)} values={entries.map(([, v]) => v)} />
        ) : null;
      }}
    >
      <p className="fine-print">
        A filter decision is recorded for every evaluated candidate, allowed or rejected, so the
        decision share is not a rejection rate. <code>unknown</code> means the policy had no
        catalog profile for the candidate and could not classify it — a service running against a
        catalog that does not cover the served items reports every decision as{" "}
        <code>unknown</code>.
      </p>
    </MeasurementSection>
  );
}

export function LatencySection({ data }) {
  return (
    <MeasurementSection
      title="Latency"
      data={data}
      columns={["scope", "name", "unit", "p50", "p95", "p99", "count", "error_rate", "timeout_rate"]}
      kpis={(rows) => {
        const row = rows.filter((r) => r.scope === "endpoint")[1] || rows[0] || {};
        return [
          { label: "p50", value: `${num(row.p50, 1)} ms` },
          { label: "p95", value: `${num(row.p95, 1)} ms` },
          { label: "p99", value: `${num(row.p99, 1)} ms` },
          { label: "requests", value: (row.count ?? 0).toLocaleString() },
        ];
      }}
      description="Live request and stage latency from the retrieval service."
      chart={(rows) => {
        const stages = rows.filter((r) => r.scope === "stage");
        const endpoints = rows.filter((r) => r.scope === "endpoint");
        return (
          <ChartGrid>
            {stages.length ? (
              <BarChart title="p95 by stage (ms)"
                labels={stages.map((r) => r.name)} values={stages.map((r) => r.p95)} />
            ) : null}
            {endpoints.length ? (
              <GroupedBarChart title="Percentiles by endpoint (ms)"
                labels={endpoints.map((r) => r.name)}
                series={[
                  { name: "p50", values: endpoints.map((r) => r.p50) },
                  { name: "p95", values: endpoints.map((r) => r.p95) },
                  { name: "p99", values: endpoints.map((r) => r.p99) },
                ]} />
            ) : null}
          </ChartGrid>
        );
      }}
    >
      <p className="fine-print">
        Live service request and stage latency from the retrieval service. Stream lag
        (feedback delay, Kafka ingest lag) is measured separately in the Spark metric events.
      </p>
    </MeasurementSection>
  );
}

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
        <MetricCard label="Queries" value={count((data.by_query || []).length)} />
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
  if (!data) return <NaCard title="Off-policy evaluation" id="ope" reason="no replay-buffer events with reward in Redis" />;
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
        <BarChart title="Lift vs logging policy" horizontal percentage
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.lift_vs_logging)} />
      </ChartGrid>
      <DataTable rows={disp} columns={["policy", "value", "value_95ci", "lift_vs_logging", "lift_95ci", "n"]} />
      <p className="fine-print">
        Direct Method · reward estimator AUC {num(cal.auc)} MSE {num(cal.mse)} (n_test {count(cal.n_test)}). 95%
        event-bootstrap CIs are conditional on the fixed reward model; model-fit uncertainty excluded.
      </p>
    </Section>
  );
}

export function MdpSection({ data }) {
  if (!data) return <NaCard title="MDP policy evaluation" id="mdp" reason="no mdp_eval.csv" />;
  const rows = (data.rows || []).map((r) => ({ ...r, ci95: ci(r.ci95_low, r.ci95_high) }));
  const byReturn = rankBy(rows, "mean_return");
  const bySteps = rankBy(rows, "mean_steps", "asc");
  return (
    <Section title="MDP policy evaluation" headline={data.headline} id="mdp"
      description="Finite-horizon discounted returns over seeded episodes.">
      <MetricGrid>
        <MetricCard label="Best mean return" value={num(byReturn[0]?.mean_return, 3)} detail={byReturn[0]?.policy} />
        <MetricCard label="Shortest trajectory" value={num(bySteps[0]?.mean_steps, 2)} detail={bySteps[0]?.policy} />
        <MetricCard label="Policies evaluated" value={count(rows.length)} />
        <MetricCard label="Total episodes"
          value={count(rows.reduce((sum, r) => sum + Number(r.episodes ?? 0), 0))} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Mean return by policy" horizontal
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.mean_return)} />
        <BarChart title="Mean episode length" horizontal
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.mean_steps)} />
      </ChartGrid>
      <DataTable rows={rows} formatters={COUNT_COLUMNS}
        columns={["policy", "episodes", "mean_return", "mean_steps", "standard_error", "ci95"]} />
      <p className="fine-print">
        Finite-horizon discounted return over seeded episodes; 95% bootstrap CIs quantify episode-sampling
        uncertainty for this fixed dataset.
      </p>
    </Section>
  );
}
