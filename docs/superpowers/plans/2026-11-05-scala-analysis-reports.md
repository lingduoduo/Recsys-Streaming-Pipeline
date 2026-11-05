# Scala Analysis Reports Implementation Plan

> **For agentic workers:** Use independent workers for disjoint report subsystems and one integration review. Track completed steps here.

**Goal:** Generate the report's evaluation CSVs and React dashboard JSON in Scala without invoking Python.

**Architecture:** Add pure map-based computations plus Spark/Redis adapters in `com.demo.report`. Reuse the current Spark assembly, preserve Python tools as compatibility references, and switch documented invocations to Scala.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1, Jedis 5.1.5, Jackson, ScalaTest.

**Spec:** `docs/superpowers/specs/2026-11-05-scala-analysis-reports-design.md` (approved by user: “gog”).

## Global Constraints

- Preserve CSV fields and dashboard JSON schema; missing data remains unavailable/null.
- Port formulas faithfully; no Python subprocesses, unrelated producer or trainer rewrites, or frontend changes.
- Use existing dependencies. Pure calculations accept `Seq[Map[String, Any]]`; JSON/Spark adapters convert nested values recursively.
- Match deterministic calculations to 1e-9 before rounding, serialized metrics to their current decimal precision. Bootstrap uses a documented JVM seed; compare point estimates exactly and bounds with controlled deterministic resamples or bounded Monte Carlo tolerance.

## Review Focus

- Missing Redis metadata and optional columns must retain honest coverage and unavailable statuses.
- Nested Spark Row/map/array values must become the same structure as JSON inputs.
- Policy-only scores must never enter the reward model.
- Dashboard JSON must remain consumable by the existing TypeScript schema.
- Empty input and single-class evaluation must not produce misleading numbers.

## Shared interfaces

`ReportSupport` provides `type Record = Map[String, Any]`, `records(spark, path): Seq[Record]`, `readJson(path): Any`, `parseJson(text): Any`, `writeJson(path, value): Unit`, `writeCsv(path, columns, rows): Unit`, `options(args): Map[String,String]`, `number(value, default=0.0): Double`, `text(value, default=""): String`, `seq(value): Seq[Any]`, `obj(value): Record`, `round(value, digits=4): Double`, `catalog(host,port): Seq[Record]`, `popularity(host,port): Map[String,Double]`, `embeddings(host,port,prefix): Map[String,Seq[Double]]`, and `replay(host,port,key,limit): Seq[Record]`. Catalog records have `item_id`, `title`, `genres` (Seq), `release_year` (number or null). These are driver-side finite offline reports, as the originals are.

`EvaluationReports` provides `recall(samples, catalog, itemVectors, ks): Seq[Record]`, `ranking(samples,popularity,userVectors,itemVectors): Seq[Record]`, `ope(events,bootstrapSamples=1000,seed=20260716L): Record` with `rows`, `calibration`, and `headline`. Entry points: `RecallEvaluationReportJob`, `RankingEvaluationReportJob`, `OffPolicyEvaluationReportJob` with original long CLI flags.

`DashboardMeasurements.build(samples, slates: Option[Seq[Record]], live: Option[Record], config: Record): Record` produces seven measurement sections and `schemaVersion`; slates are enriched with item genres/popularity by dashboard layer. Optional catalog size passed as `config("catalog_size")`.

`AnalysisDashboardExportJob.build(samples,catalog,popularity,userVectors,itemVectors,events,slates,live,config): Record` composes all sections. Main supports original exporter flags; `--output` defaults to `frontend/data/dashboard.json` from `recsys-pipeline/`.

### Task 1: Shared I/O and invocation

Owner: main agent. Files: `ReportSupport.scala`, `ReportSupportSpec.scala`, `scripts/run-analysis-report.sh`.
- [x] Write and run failing nested JSON/Parquet, CSV quoting/null, CLI rejection and metadata conversion tests.
- [x] Implement shared APIs and launcher using pinned Spark.
- [x] Run focused ScalaTest checks.

### Task 2: Evaluation reports

Owner: evaluation worker. Files: `EvaluationReports.scala`, three evaluation entry point files, `EvaluationReportsSpec.scala` and test fixtures.
- [x] Write tests first from existing Python fixtures: recall leave-one-out and ties; ranking AUC, logloss and sparse coverage; OPE leakage exclusion, deterministic splits/picks, constant rewards and bootstrap.
- [x] Observe missing implementation failure, port pure formulas and command line jobs.
- [x] Compare Python reference calculations on fixed fixtures and run focused Scala tests.

### Task 3: Dashboard measurements

Owner: measurement worker. Files: `DashboardMeasurements.scala`, supporting `QualityMeasurements.scala` / `GovernanceMeasurements.scala` if helpful, related tests/fixtures.
- [x] Write tests first for measurement contracts, missing data, quality metrics, fairness, safety and latency using Python reference fixtures.
- [x] Observe failing tests and port `measurement_contract.py`, `quality_measurements.py`, `governance_measurements.py` and `build_measurement_dashboard` behavior.
- [x] Verify numerical/schema parity with focused Scala tests.

### Task 4: Dashboard composition/export

Owner: dashboard worker. Files: `AnalysisDashboardExportJob.scala`, `DashboardAnalysis.scala`, related tests/fixtures.
- [x] Write tests first for enrichment, keyword/query/engagement sections, truncation, strict JSON and empty input.
- [x] Port `analysis_dashboard_report.py` compute functions and `frontend/export_dashboard_json.py`, calling interfaces above for evaluations and measurements.
- [x] Verify JSON schema and fixture parity with focused Scala tests.

### Task 5: Integration and documentation

Owner: main agent. Files: analysis report, relevant README usage, launcher tests and integration fixture.
- [x] Assemble, run combined Scala suites and end-to-end fixture export with controlled Redis.
- [x] Update actual runnable commands and explain retained Python post-training tools plus JVM bootstrap RNG differences.
- [x] Review entire diff with a fresh reviewer, fix material findings, rerun affected checks, record results.

## Execution record

- User authorized implementation after reading the design. Continue without further approval gates.
- Work in the existing checkout on `feat/scala-analysis-reports` so changes remain visible; no external publish or merge.
- Parallel workers own disjoint files. Shared interface defined above before dispatch; main owns adapter changes.


## Verification record

- Existing ReportWindow baseline: 3 tests passed.
- Shared adapter tests initially failed for missing implementation; unreachable-Redis regression reproduced constructor failure before the fallback boundary and was fixed there.
- Python reference fixtures cover recall, ranking, OPE, dashboard aggregations and complete measurement output over four input cases.
- Review found dashboard-specific ranking must exclude absent popularity and derive positives from engagement labels. Fixed with a sparse/missing-popularity regression test. No other material review findings.
- Launcher/service-script/documentation checks: 44 tests passed.
- Assembled dashboard over fixture Parquet + temporary Redis matched the complete Python JSON reference at 1e-9, except documented seeded bootstrap bounds; existing frontend validator passed all seven sections.
- No live pipeline datasets or committed dashboard snapshot were modified.

- Final Scala report suite: 65/65 across 12 suites; assembly succeeded.
- Assembled recall (9 rows), ranking (3 rows) and OPE (6 rows, bootstrap disabled for exact CSV comparison) match Python reference CSVs.
- User requested commit, push and PR; target is the repository default branch `master`.
