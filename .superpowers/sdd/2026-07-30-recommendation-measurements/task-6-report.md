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
