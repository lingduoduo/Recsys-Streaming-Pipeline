import { MetricTile } from "./ui";
import { SECTION_ROUTE } from "./groups";
import { num, share, maxByField } from "./format";

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
