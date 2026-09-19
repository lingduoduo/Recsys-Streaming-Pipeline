import { MetricTile } from "./ui";
import { SECTION_ROUTE } from "./groups";
import { num, share, maxByField } from "./format";

// Which single number represents each measurement on the scorecard. `field` must be a
// key the calculator actually publishes — the contract test enforces that. `select: "max"`
// resolves the row by the largest value of `field` instead of by `rowIndex`, which stays as
// the fallback for a section where no row published the field.
export const HEADLINES = {
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

// The seven measurement sections share one envelope, which is why HEADLINES covers them all. The
// six diagnostics do not: two publish a scalar, four need the largest value over a named row set,
// and the row set is not always called `rows`. `support` supplies the tile's n= -- from the section
// for a scalar spec, from the winning row for a row spec -- so one signature covers both.
const DIAGNOSTICS = {
  engagement: {
    label: "CTR", format: "pct", scalar: "ctr",
    support: (section) => section.funnel?.impression,
  },
  query: {
    label: "avg query length", format: "num", scalar: "average_query_length",
    support: (section) => section.by_length?.length,
  },
  keyword: {
    label: "largest divergence", format: "num", rows: "by_keyword", field: "divergence",
    support: (section) => section.by_keyword?.length,
  },
  recall: {
    label: "best recall@k", format: "num", rows: "rows", field: "recall_at_k",
    support: (section, row) => row?.users_evaluated,
  },
  ranking: {
    label: "best AUC", format: "num", rows: "rows", field: "auc",
    support: (section, row) => row?.n,
  },
  ope: {
    label: "best lift", format: "pct", rows: "rows", field: "lift_vs_logging",
    support: (section, row) => row?.n_events,
  },
};

// null when the section is absent or the field never appears, so a missing input renders an N/A
// tile rather than a confident zero.
function diagnosticTile(section, spec) {
  if (!section) return null;
  if (spec.scalar) {
    const value = section[spec.scalar];
    if (value === null || value === undefined) return null;
    return { value, sampleSize: spec.support?.(section) };
  }
  const best = maxByField(section[spec.rows] ?? [], spec.field);
  if (!best) return null;
  return { value: best[spec.field], sampleSize: spec.support?.(section, best) };
}

function figure(value, format) {
  return format === "pct" ? share(value) : num(value, 3);
}

function headlineRow(section, spec) {
  const rows = section.rows ?? [];
  const selected = spec.select === "max" ? maxByField(rows, spec.field) : null;
  return selected ?? rows[spec.rowIndex] ?? rows[0];
}

// A merged live-only row (see `_merge_live_row` in analysis_dashboard_report.py) can make a
// section "available" without publishing every offline field — e.g. the live safety row has
// no `unsafe_exposure_rate`. Check the key itself, not just its value, so a merged row that
// omits the headline field is never mistaken for a published-but-null one.
export function headlineFieldPublished(section, spec) {
  const row = headlineRow(section, spec);
  return !!row && Object.prototype.hasOwnProperty.call(row, spec.field);
}

export function headlineValue(section, spec) {
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
            href={SECTION_ROUTE[key]}
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
