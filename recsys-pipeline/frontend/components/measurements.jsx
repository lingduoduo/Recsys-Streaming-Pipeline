import {
  Section, NaCard, BarChart, GroupedBarChart, LineChart, DataTable,
  MetricGrid, MetricCard, ChartGrid,
} from "./ui";
import { num, share, count, duration, maxByField } from "./format";
import { HEADLINES, headlineFieldPublished, headlineValue } from "./scorecard";

// One consistent presentation for every measurement envelope: headline, the support it
// was calculated from, any warnings, then the rows. Never invents a value for N/A.
function MeasurementSection({ title, data, columns, kpis, chart, description, children }) {
  const key = title.toLowerCase();
  if (!data || data.status !== "available") {
    return <NaCard title={title} id={key} reason={data?.warnings?.[0] || "measurement unavailable"} />;
  }
  const rows = data.rows || [];
  const values = kpis ? kpis(rows) : [];
  // These seven sections carry prose headlines from the calculator -- "Observed user
  // satisfaction" -- so the collapsed row would show no figure. Reuse the number the
  // scorecard tile shows for the same section, so a closed section still reports something.
  const spec = HEADLINES[key];
  const metric = spec && headlineFieldPublished(data, spec)
    ? `${spec.label} ${headlineValue(data, spec)}`
    : null;
  return (
    <Section title={title} headline={data.headline} metric={metric} description={description} id={key}>
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
      {chart ? chart(rows, data) : null}
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
      description="Observed engagement and the coverage of each optional feedback signal."
      chart={(rows, data) => {
        const row = rows[0] || {};
        // negative_feedback_coverage is the same expression as negative_feedback_rate (both count
        // the samples carrying a reason), so plotting it here would read as "not instrumented"
        // for a signal that is instrumented and simply did not fire. It stays in the table
        // beside its rate, where the two are legible together.
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
            ) : <p className="na">Time series unavailable (no impression_ts span).</p>}
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
    >
      <p className="fine-print">
        Negative feedback <em>coverage</em> is charted nowhere because it is the same
        expression as the negative feedback rate — a sample only carries a reason when the
        feedback fired — so a coverage bar would show an instrumented signal as
        uninstrumented. The rate itself is charted above, and both columns are in the table.
      </p>
      <p className="fine-print">
        Buckets split the observed span of <code>impression_ts</code> evenly; whole-second stamps
        get a whole-second width, so the last bucket can cover less time and show lower counts.
        The sim stamps events
        with wall-clock time, so on sim data the series shows drift across one run, not a calendar.
        Only orders carry a rating, and the sim derives it from completion (3 + 2 × completion).
      </p>
    </MeasurementSection>
  );
}

export function FreshnessSection({ data }) {
  const bands = data?.age_bands ?? [];
  return (
    <MeasurementSection
      title="Freshness"
      data={data}
      columns={[
        "scope", "freshness_source", "fresh_share", "fresh_item_share", "fresh_exposure_lift",
        "freshness_coverage", "age_at_exposure_coverage", "mean_content_age_days",
        "median_content_age_days", "fresh_ctr", "established_ctr", "fresh_ctr_diff",
        "fresh_ctr_diff_se", "fresh_ctr_diff_z", "fresh_mean_reward", "established_mean_reward",
        "exposures",
      ]}
      kpis={(rows) => {
        const row = rows[0] || {};
        const gap = row.fresh_ctr_diff;
        return [
          { label: "fresh share", value: share(row.fresh_share),
            detail: row.fresh_item_share == null ? "of exposures"
              : `of exposures · ${share(row.fresh_item_share)} of served movies` },
          { label: "exposure lift", value: num(row.fresh_exposure_lift, 2), detail: "fresh exposure ÷ fresh supply" },
          { label: "median age at exposure",
            value: row.median_content_age_days == null ? "N/A" : `${num(row.median_content_age_days, 0)} d` },
          { label: "fresh − established CTR",
            value: gap == null ? "N/A" : `${gap >= 0 ? "+" : ""}${(gap * 100).toFixed(2)} pp`,
            detail: row.fresh_ctr_diff_z == null ? "no item ids to test" : `z = ${num(row.fresh_ctr_diff_z, 2)}` },
        ];
      }}
      description="How much of what was shown is recent, against how much recent content was served, and whether recency tracks engagement."
      chart={(rows) => {
        const row = rows[0] || {};
        const rewardObserved = (row.fresh_reward_coverage ?? 0) > 0 || (row.established_reward_coverage ?? 0) > 0;
        return (
          <ChartGrid>
            {bands.length ? (
              <>
                <GroupedBarChart title="Exposure vs supply by content age" percentage
                  labels={bands.map((b) => b.band)}
                  series={[{ name: "share of exposures", values: bands.map((b) => b.exposure_share) },
                           { name: "share of movies", values: bands.map((b) => b.item_share) }]} />
                <BarChart title="CTR by content age" percentage
                  labels={bands.map((b) => b.band)} values={bands.map((b) => b.ctr)} />
              </>
            ) : null}
            <BarChart title="CTR: fresh vs established" percentage
              labels={["fresh", "established"]}
              values={[row.fresh_ctr, row.established_ctr]} />
            {rewardObserved ? (
              <BarChart title="Mean reward by content age"
                labels={["fresh", "established"]}
                values={[row.fresh_mean_reward, row.established_mean_reward]} />
            ) : null}
          </ChartGrid>
        );
      }}
    >
      {bands.length ? (
        <DataTable rows={bands} compact
          columns={["band", "exposures", "exposure_share", "items", "item_share", "ctr", "z"]}
          formatters={{ exposures: count, exposure_share: share, items: count, item_share: share,
                        ctr: share, z: (v) => num(v, 2) }} />
      ) : null}
      <p className="fine-print">
        Content age is measured when the item was shown (impression_ts − published_at), so the
        figures do not change with the export date. Supply is the distinct movies served; a lift
        near 1 means recent movies got their share of exposure. A CTR gap within ±2 standard errors —
        movies as the unit — is noise. A band&apos;s z compares that band&apos;s movies with the overall
        rate, so movie mix (genre, say) can drive it as easily as age; across five bands, one |z| above
        2 turns up by chance in about one run in ten.
      </p>
    </MeasurementSection>
  );
}

export function DiversitySection({ data }) {
  return (
    <MeasurementSection
      title="Diversity"
      data={data}
      columns={[
        "scope", "items_served", "catalog_size", "catalog_coverage", "exposure_gini",
        "top_decile_exposure_share", "median_items_per_user", "user_repeat_rate",
        "user_repeat_rate_uniform", "unique_genres_at_k", "normalized_genre_entropy", "intra_list_genre_distance",
        "long_tail_exposure_share", "long_tail_popularity_cutoff", "genre_coverage",
        "popularity_coverage",
      ]}
      kpis={(rows) => {
        const row = rows.find((r) => r.scope === "aggregate") || rows[0] || {};
        return [
          { label: "genre entropy", value: num(row.normalized_genre_entropy, 3) },
          { label: "intra-list distance", value: num(row.intra_list_genre_distance, 3) },
          { label: "long-tail share", value: share(row.long_tail_exposure_share) },
          { label: "catalog coverage", value: share(row.catalog_coverage),
            detail: `${count(row.items_served)} of ${count(row.catalog_size)} items` },
          { label: "exposure Gini", value: num(row.exposure_gini, 3),
            detail: row.catalog_size == null ? "over served items only" : "over the whole catalog" },
          { label: "user repeat rate", value: share(row.user_repeat_rate),
            detail: row.user_repeat_rate_uniform == null
              ? "no catalog size, no baseline"
              : `uniform serving: ${share(row.user_repeat_rate_uniform)}` },
        ];
      }}
      description="Genre spread within a slate, spread across the catalog and each user's history, and long-tail exposure."
      chart={(rows, data) => {
        const histograms = data.distributions ?? {};
        const binLabel = (b) => `${b.bin_start.toFixed(1)}–${(b.bin_start + 0.1).toFixed(1)}`;
        const genres = data.genre_exposure ?? [];
        const series = data.series ?? [];
        const width = data.series_bucket_seconds;
        const labels = series.map((b) => `+${duration(b.bucket_start - series[0].bucket_start)}`);
        const caption = `${series.length} × ${duration(width)} buckets over ${duration(width * series.length)}`;
        return (
          <ChartGrid>
            {[["normalized_genre_entropy", "Per-slate genre entropy"],
              ["intra_list_genre_distance", "Per-slate intra-list distance"]].map(([key, title]) => {
              const bins = histograms[key] ?? [];
              return bins.length ? (
                <BarChart key={key} title={title} valueFormatter={count}
                  labels={bins.map(binLabel)} values={bins.map((b) => b.count)} />
              ) : null;
            })}
            {genres.length ? (
              <GroupedBarChart title="Genre share: exposures vs served items" percentage
                labels={genres.map((g) => g.genre)}
                series={[{ name: "exposures", values: genres.map((g) => g.exposure_share) },
                         { name: "served items", values: genres.map((g) => g.served_share) }]} />
            ) : null}
            {series.length ? (
              <>
                <LineChart title="Genre spread over time" labels={labels} caption={caption}
                  valueFormatter={(v) => num(v, 3)}
                  series={[{ name: "entropy", values: series.map((b) => b.normalized_genre_entropy) },
                           { name: "intra-list distance",
                             values: series.map((b) => b.intra_list_genre_distance) }]} />
                <LineChart title="Long-tail share over time" percentage labels={labels} caption={caption}
                  series={[{ name: "long-tail share",
                             values: series.map((b) => b.long_tail_exposure_share) }]} />
              </>
            ) : <p className="na">Time series unavailable (no request_ts span).</p>}
          </ChartGrid>
        );
      }}
    >
      <p className="fine-print">
        The long-tail cutoff is the configured popularity percentile (80th by default) over distinct
        served items. Taken over exposures it would put that share of exposures below it by
        construction. Catalog coverage, Gini and repeat rate measure spread across the catalog and
        across each user&apos;s history, which per-slate averages cannot see; Gini counts unserved
        catalog items as zero exposures. Random serving repeats items too, so read the repeat rate
        against its uniform-serving baseline, not against zero. The sim serves near-uniform slates:
        the figures sit near what random serving gives, and the section exists to catch a
        recommender that narrows what people see.
      </p>
    </MeasurementSection>
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
