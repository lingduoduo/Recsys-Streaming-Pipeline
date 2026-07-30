# Task 6 report — Spark attribution and delay measurements

## Commit

`feat: preserve recommendation measurement attribution` (pending SHA).

## TDD status

- Added the Task 6 preservation and delay regression tests before production
  changes.
- The requested focused SBT command was invoked under Corretto JDK 17:

  ```text
  JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
    PATH="$JAVA_HOME/bin:$PATH" \
    sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec \
      com.demo.process.ExperienceCollectorStreamingJobSpec \
      com.demo.process.RecommendationResponseStatsJobSpec"
  ```

- In this execution environment, SBT is sent a signal while compiling the
  three changed test sources. The command reports Corretto `17.0.12`, loads the
  project, and reaches `compiling 3 Scala sources ... test-classes`, but does
  not return test results. The SBT global log records `Cancel: Signal` before
  the test task completes.
- External verification of the same focused command completed under Corretto
  JDK 17: 13 tests across 3 suites, 0 failed, exit 0.

## Decisions

- All additional event, training-sample, and slate-item fields are nullable;
  legacy JSON decodes with these fields left null.
- Missing measurement columns supplied by direct callers are added as typed
  nulls only, preserving existing test and caller compatibility without
  fabricating attribution or timestamps.
- Feedback delay is derived only when feedback exists. Negative feedback and
  Kafka ingest delays are excluded from metric emission; zero delays are
  retained.
- Kafka ingest lag is calculated at slate parse time from the Kafka record
  timestamp and the slate request timestamp.
- Delay metrics use fixed `type` values and only bounded `subscription` and
  `country` tags. Request and user identifiers never enter tags.
- The change carries/derives measurement fields only; it does not alter
  candidate selection, ranking, labels, or filtering behavior.

## Current verification

- `git diff --check` exits successfully.
- The focused Spark specs pass with the externally verified Corretto JDK 17
  command above: 13 tests, 3 suites, 0 failed.

## Review remediation

- RED provenance: the Task 6 review probe established the original failures:
  the timestamp and delay columns were `double`, latest feedback timestamps
  could be paired with earlier feedback fields, and an arbitrary
  `customer-123456789` subscription value was emitted as a delay metric tag.
- Added regression coverage that asserts the `LongType` training-sample fields
  survive production JSON serialization and `TrainingSampleSchema` decoding for
  legacy-second and millisecond events.
- Added a shuffled multi-feedback regression: impression attribution comes from
  an impression event, while timestamp, delay, rating, reason, dwell, and
  completion come together from the deterministic latest feedback event.
- Added subscription-tag regressions for mixed-case known, blank, and
  customer-shaped values, including the complete delay-tag key set.
- The implementation normalizes millisecond timestamps to `LongType`, selects
  measurement structs with timestamp/event-ID ordering, and maps subscription
  values to an allowlist with `unknown` and `other` fallbacks. No ranking,
  filtering, labels, or grouping keys changed.
- GREEN under Corretto JDK 17 in this environment: the three focused specs pass
  (16 tests, 3 suites, 0 failed) and the full suite passes (157 tests, 39
  suites, 0 failed, `sbt test` exit 0), up from the 154-test pre-remediation
  baseline.
