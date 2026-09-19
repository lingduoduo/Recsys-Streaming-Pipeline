import {
  Section, NaCard, BarChart, GroupedBarChart, DataTable,
  MetricGrid, MetricCard, ChartGrid,
} from "./ui";
import { num, share, maxByField } from "./format";

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
