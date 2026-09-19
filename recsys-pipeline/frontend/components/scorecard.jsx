import { MetricTile } from "./ui";
import { GROUPS, SECTIONS, SECTION_ROUTE } from "./groups";
import { num, share, maxByField, pickByField } from "./format";

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
    label: "largest gap", format: "num", rows: "by_keyword", field: "divergence", select: "abs",
    support: (section) => section.by_keyword?.length,
  },
  recall: {
    label: "best recall@10", format: "num", rows: "rows", field: "recall_at_k",
    where: (row) => row.k === 10,
    support: (section, row) => row?.users_evaluated,
  },
  ranking: {
    label: "worst AUC", format: "num", rows: "rows", field: "auc", select: "min",
    support: (section, row) => row?.n,
  },
  ope: {
    // "max" on purpose: the section asks whether any policy beats logging, so the best
    // candidate is the answer and the worst one is noise.
    label: "best lift", format: "pct", rows: "rows", field: "lift_vs_logging", select: "max",
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
  // `where` narrows before the extremum is taken. Recall publishes one row per method per k, and
  // recall@k rises with k by construction, so an extremum over the unfiltered set reports which k
  // it picked rather than how retrieval performed.
  const rows = (section[spec.rows] ?? []).filter(spec.where ?? (() => true));
  const best = pickByField(rows, spec.field, spec.select ?? "max");
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

// One tile per section. A measurement section resolves through HEADLINES and carries coverage, so
// it can read "low"; a diagnostic resolves through DIAGNOSTICS and is either present or absent.
function tile(key, data) {
  const section = data[key];
  const title = SECTIONS[key].label;
  // `key` is deliberately NOT in here: React requires it passed directly to the element,
  // and spreading an object that carries one is a runtime error rather than a warning.
  const common = { href: SECTION_ROUTE[key], title };

  const measurement = HEADLINES[key];
  if (measurement) {
    const available = section?.status === "available";
    const published = available && headlineFieldPublished(section, measurement);
    const status = !published ? "na" : (section.coverage ?? 1) <= LOW_COVERAGE ? "low" : "ok";
    return (
      <MetricTile
        key={key}
        {...common}
        value={published ? headlineValue(section, measurement) : "N/A"}
        label={measurement.label}
        sampleSize={section?.sampleSize}
        status={status}
        reason={published ? null : section?.warnings?.[0] || "measurement unavailable"}
      />
    );
  }

  const spec = DIAGNOSTICS[key];
  const resolved = diagnosticTile(section, spec);
  return (
    <MetricTile
      key={key}
      {...common}
      value={resolved ? figure(resolved.value, spec.format) : "N/A"}
      label={spec.label}
      sampleSize={resolved?.sampleSize}
      status={resolved ? "ok" : "na"}
      reason={resolved ? null : "no input for this section"}
    />
  );
}

export function Scorecard({ data }) {
  return (
    <div className="scorecard-groups">
      {GROUPS.map(({ key: group, label }) => (
        <section className="scorecard-group" key={group}>
          <h2 className="scorecard-label">{label}</h2>
          <div className="scorecard">
            {Object.entries(SECTIONS)
              .filter(([, catalogue]) => catalogue.group === group)
              .map(([key]) => tile(key, data))}
          </div>
        </section>
      ))}
    </div>
  );
}
