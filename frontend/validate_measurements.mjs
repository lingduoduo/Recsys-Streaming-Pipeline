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

if (problems.length) {
  console.error("dashboard.json fails the measurement contract:");
  for (const problem of problems) console.error(`  - ${problem}`);
  process.exit(1);
}

console.log(`dashboard.json valid: ${SECTIONS.length} measurement sections, schema ${data.schemaVersion}`);
