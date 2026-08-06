// Fail the build when data/dashboard.json does not satisfy the measurement contract.
import { readFileSync } from "node:fs";

const SECTIONS = ["relevance", "satisfaction", "freshness", "diversity", "fairness", "safety", "latency"];
const path = new URL("./data/dashboard.json", import.meta.url);
const data = JSON.parse(readFileSync(path, "utf8"));
const problems = [];

if (data.schemaVersion !== "2.0") {
  problems.push(`schemaVersion must be "2.0" (found ${JSON.stringify(data.schemaVersion)})`);
}

for (const key of SECTIONS) {
  const section = data[key];
  if (!section || typeof section !== "object") {
    problems.push(`missing measurement section "${key}"`);
    continue;
  }
  if (section.status !== "available" && section.status !== "unavailable") {
    problems.push(`"${key}" has invalid status ${JSON.stringify(section.status)}`);
    continue;
  }
  if (!Array.isArray(section.rows)) {
    problems.push(`"${key}" must carry a rows array`);
  }
  if (section.status === "available") {
    // Every published measurement states the support it was calculated from, and an
    // available section with no observations is an N/A wearing a green hat.
    if (!Number.isInteger(section.sampleSize) || section.sampleSize < 1) {
      problems.push(`available "${key}" must report a positive integer sampleSize`);
    }
    if (typeof section.coverage !== "number") {
      problems.push(`available "${key}" must report coverage`);
    }
    if (Array.isArray(section.rows) && section.rows.length === 0) {
      problems.push(`available "${key}" must publish at least one row`);
    }
  } else if (!Array.isArray(section.warnings) || section.warnings.length === 0) {
    problems.push(`unavailable "${key}" must explain why it is unavailable`);
  }
}

// Diagnostic sections serialize as null when their inputs are absent, which is a
// valid state. When one IS published, the fields its report reads must be present:
// a section that renders every column as N/A is a stale snapshot, not a measurement.
const DIAGNOSTIC_ROWS = {
  engagement: {
    by_query: ["query", "impressions", "clicks", "orders", "ctr", "cvr", "mean_score"],
    by_genre: ["genre", "impressions", "clicks", "orders", "ctr", "cvr", "mean_score"],
  },
  keyword: {
    by_keyword: ["keyword", "movie_impressions", "query_clicks", "query_orders",
                 "mean_score", "ctr", "cvr", "divergence"],
    by_subkeyword: ["subkeyword", "movie_impressions", "query_clicks", "query_orders",
                    "mean_score", "ctr", "cvr", "divergence"],
  },
  query: {
    top_queries: ["query", "impressions", "clicks", "orders", "query_len", "ctr", "cvr"],
    by_length: ["bucket", "queries", "impressions", "clicks", "orders", "ctr", "cvr"],
  },
  ranking: { rows: ["signal", "n", "positives", "positive_rate", "coverage", "auc", "logloss"] },
};

for (const [section, tables] of Object.entries(DIAGNOSTIC_ROWS)) {
  const published = data[section];
  if (published === null || published === undefined) continue;
  for (const [table, fields] of Object.entries(tables)) {
    const rows = published[table];
    if (!Array.isArray(rows)) {
      problems.push(`"${section}.${table}" must be an array`);
      continue;
    }
    if (rows.length === 0) continue;
    for (const field of fields) {
      if (!Object.prototype.hasOwnProperty.call(rows[0], field)) {
        problems.push(`"${section}.${table}" rows must carry "${field}"`);
      }
    }
  }
}

if (data.query && !Object.prototype.hasOwnProperty.call(data.query, "average_query_length")) {
  problems.push('"query" must carry average_query_length');
}

if (problems.length) {
  console.error("dashboard.json fails the measurement contract:");
  for (const problem of problems) console.error(`  - ${problem}`);
  process.exit(1);
}

console.log(`dashboard.json valid: ${SECTIONS.length} measurement sections, schema ${data.schemaVersion}`);
