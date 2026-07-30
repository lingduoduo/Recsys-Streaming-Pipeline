# Task 5 report — live recommendation measurements

## Commit

Pending commit: `feat: instrument live recommendation measurements`.

## TDD evidence

- RED: `mvn -Dtest=RecommendationMeasurementServiceTest test` failed at test
  compilation because `RecommendationMeasurementService`, `FilterDecision`, and
  `MeasurementSnapshot` did not exist.
- GREEN: the same JDK 17 Maven command passed all 3
  `RecommendationMeasurementServiceTest` tests after the implementation.
- RED: the controller/retrieval focused test command failed at test compilation
  before the measurement-aware `HybridRecommendationService` constructor existed.

## Verification

Executed with Corretto JDK 17:

```text
/usr/bin/env JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  /opt/homebrew/bin/mvn -Dtest=RecommendationMeasurementServiceTest test
```

Result: 3 tests, 0 failures, 0 errors.

The requested focused command and `mvn test` both compile production and test
sources, but cannot execute Mockito-based tests in this sandbox. Byte Buddy fails
to attach to the forked JVM (`AttachNotSupportedException` opening `.java_pid...`)
on both the default JDK and Corretto 17. After annotating the measurement-service
constructor, Spring starts successfully before that independent Mockito failure.

## Decisions

- Meter labels are strictly bounded: endpoints, stage names, filter reasons,
  feedback signal names, boolean outcomes, and the fixed freshness source only.
  IDs, request IDs, feedback text, rewards, and demographic-like values are never
  meter labels.
- Timers use the configured latency buckets and published p50/p95/p99 values.
  The snapshot is versioned as `2.0` and contains aggregate-only latency,
  freshness, safety, and feedback-coverage data.
- Observation exceptions are logged and swallowed locally. Legacy direct service
  construction receives a no-op measurement service, so ranking and selection are
  unchanged.
- The current live `MovieProfile` has no optional timestamp field. Freshness uses
  its existing `newRelease` boolean fallback; null selected profiles increase the
  denominator without fabricating a fresh/old value.

## Review remediation

- Latency snapshots now expose independent `/recommend` and `/feedback` timers
  in milliseconds, with separate bounded error and timeout counters. The stable
  snapshot also includes all five stage timers.
- Safety observations now include the configured policy version, evaluated
  denominator, every fixed reason/rate, and unknown share. History exclusions
  remain excluded without being called unknown; unclassified catalog candidates
  are observed as `unknown` without changing eligibility.
- Feedback coverage has a per-request denominator and stable signal map.
  Freshness updates and reads use one locked aggregate state.
- `redis_fetch` wraps popularity, candidate retrieval, cache warming, and batch
  reads; `scoring` wraps user/model/reward/scorer work; `selection` wraps both
  selector calls.

### RED/GREEN evidence

- RED: the new unclassified-candidate regression failed with a null `unknown`
  counter before the retriever recorded an eligible unclassified candidate.
- GREEN: after the change,
  `mvn -q -Dtest=RecommendationMeasurementServiceTest,RecommendationControllerTest,HybridRecommendationServiceTest test`
  exited 0 (27 tests: 6 measurement, 19 controller, 2 hybrid-service).
- GREEN: `mvn -q test` exited 0 under Corretto JDK 17; current Surefire reports
  contain 143 tests with zero failures/errors. Docker-backed tests continue to
  skip their container work when Docker is unavailable.
