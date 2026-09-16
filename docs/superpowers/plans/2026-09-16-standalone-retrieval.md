# Standalone Retrieval Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the retrieval service directory independently testable, packageable, and ready to move into the backend repository.

**Architecture:** Freeze contract snapshots inside service test resources, and move cross-checkout consistency checks to the pipeline. Keep runtime interfaces unchanged and prove independence with an isolated build in CI.

**Tech Stack:** Java 17, Spring Boot 3.3.5, Maven, JUnit 5, Python standard library, Docker, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-16-standalone-retrieval-design.md`

## Global Constraints

- Java 17 and Spring Boot 3.3.5 remain unchanged; no new runtime dependencies.
- Preserve HTTP APIs, Redis keys, Kafka event bytes, topic defaults, ranking behavior, and model loading behavior.
- Default Maven tests and packaging must require only the service directory, Maven/JDK, and normal Maven dependencies. Docker-backed tests retain their existing skip behavior when Docker is unavailable.
- Contract updates must update the producer canonical artifact and the service snapshot together after compatibility review. Local snapshots must not regenerate themselves from production resources during testing.

---

### Task 1: Deliver and verify the portable service boundary

**Files:**
- Modify `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/event/RecsysEventSchemaDriftTest.java` and `RecsysEventFixtureTest.java`.
- Modify `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/UserProfileIntegrationTest.java`.
- Modify `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/clients/RedisUserProfileClientTest.java` and `UserProfileValidationTest.java`.
- Create `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/support/ContractFixtures.java`.
- Create snapshots under `recsys-pipeline/services/java-retrieval-service/src/test/resources/contracts/`: `recsys-event-v3.avsc`, `serving-impression-v3.avro`, `user_profile_v1.json`.
- Create `recsys-pipeline/integration-tests/test_retrieval_contracts.py`.
- Create service-local `README.md`, `Dockerfile`, `.dockerignore`, `.gitignore`.
- Create `.github/workflows/retrieval-service.yml`.
- Modify `recsys-pipeline/README.md` to link the standalone guide.

**Interfaces:**
- Consumes canonical `recsys-pipeline/schemas/recsys-event-v3.avsc`, `recsys-pipeline/schemas/fixtures/serving-impression-v3.avro`, `recsys-pipeline/integration-tests/fixtures/user_profile_v1.json`.
- Produces `ContractFixtures.bytes(String name)` and `ContractFixtures.text(String name)` for classpath snapshots, and `RETRIEVAL_SERVICE_DIR` for pipeline-side comparison.

- [x] **Step 1: Prove the extraction currently fails.** Copy only the service to a temporary directory, omitting `target`, and run:

```sh
mvn -B -ntp -Dtest=RecsysEventSchemaDriftTest,RecsysEventFixtureTest,UserProfileValidationTest,RedisUserProfileClientTest test
```

Expected: missing `../../schemas` or `../../integration-tests` artifacts. Retain output as red-phase evidence; no new synthetic test is needed because existing assertions expose the regression.

- [x] **Step 2: Freeze resources and load them through the classpath.** Copy the three canonical artifacts into `src/test/resources/contracts/`. Implement the test helper using this pattern (UTF-8 for `text`):

```java
public static byte[] bytes(String name) {
    try (var input = ContractFixtures.class.getResourceAsStream("/contracts/" + name)) {
        if (input == null) throw new IllegalStateException("Missing contract fixture: " + name);
        return input.readAllBytes();
    } catch (java.io.IOException error) {
        throw new java.io.UncheckedIOException(error);
    }
}
public static String text(String name) {
    return new String(bytes(name), java.nio.charset.StandardCharsets.UTF_8);
}
```

Replace filesystem access in the five tests with the helper, preserving all assertions. Explain that the local schema is a frozen contract snapshot, not the canonical producer checkout.

- [x] **Step 3: Preserve pipeline contract comparisons.** Use a standard-library `unittest.TestCase` discoverable by pytest too. Resolve the pipeline root using `Path(__file__).resolve().parents[1]`; resolve the service using `RETRIEVAL_SERVICE_DIR` or the current service path. Compare schema JSON objects (both production schema and test snapshot) to the canonical schema, Avro bytes to the canonical fixture, and profile JSON objects to the producer fixture. Use explicit failures for missing paths. Run the test against both default and isolated locations. Temporarily mutate each artifact category and remove a fixture in a disposable copy to verify nonzero exit; restore afterward.

```sh
python3 -m unittest discover -s recsys-pipeline/integration-tests -p test_retrieval_contracts.py -v
```

- [x] **Step 4: Add standalone packaging and operation instructions.** Use a service-only multi-stage Maven/Java 17 Docker build, skip tests in the image build because separate verification runs them, run the resulting executable JAR with a non-root Java 17 runtime, and ignore build outputs/local secrets in the context. Document `mvn clean verify`, `mvn spring-boot:run`, `java -jar target/retrieval-service-0.0.1-SNAPSHOT.jar`, and `docker build -t retrieval-service .`. Document the existing env names `REDIS_HOST`, `REDIS_PORT`, `SERVER_PORT`, `RECSYS_GRPO_EMIT_EVENTS`, `KAFKA_BOOTSTRAP_SERVERS`, `ONLINE_JOINER_INPUT_TOPIC`, `ONNX_MODEL_PATH`, `ONNX_LOOKUPS_PATH`, `RECSYS_CATALOG_PATH`. Include relocation and contract-update instructions and link the guide from the pipeline README.

- [x] **Step 5: Automate the isolation check.** Add a workflow with read-only contents permissions, checkout and Java 17 setup, pipeline comparison via unittest, then copy the service into `$RUNNER_TEMP/retrieval-service` (excluding target) and run `mvn -B -ntp clean verify` there. Trigger for service, contract sources, comparison test, and workflow changes on pushes and PRs. Avoid coupling the isolated build to pipeline directories.

- [x] **Step 6: Verify and review.** Run complete Maven verification in a fresh external copy, Python comparisons, whitespace checks, and optionally Docker build/JAR smoke test as supported by the host. Record exact totals and skips. Review spec coverage and correctness, fix material findings, then commit.

- [x] **Step 7: Open the requested PR.** Push the feature branch and create a PR against master with a concise behavior summary, spec/plan links, and exact validation limits. Preserve the worktree for follow-up.


## Verification record

- Before extraction: focused tests in an isolated service copy failed on missing parent-directory schemas/profile fixtures (16 errors).
- After extraction: the same focused tests passed (16 tests, no failures/errors/skips).
- Full `mvn -B -ntp clean verify` in a fresh external service-only directory passed: 332 tests, 0 failures, 0 errors, 1 Docker-dependent skip. All 183 Maven input files match the tested copy. Executable Spring Boot JAR packaged successfully.
- Pipeline contract comparison passed all 3 tests against both default and relocated service paths. Individual schema, Avro, and profile mutations and a missing profile snapshot each failed as expected; restored snapshots passed.
- Packaged JAR launched from an unrelated working directory against temporary native Redis: health UP, ONNX model metadata loaded, prediction returned a score, and seeded recommendations returned both items.
- `actionlint` and `git diff --check` passed. Task review approved both spec compliance and code quality with no material findings.
- Docker image build remains unverified because the local Docker daemon is unavailable. The Docker-backed integration test was skipped for the same reason.

- Final whole-branch review found no material integration or requirements issues. PR opened: https://github.com/lingduoduo/Recsys-Streaming-Pipeline/pull/241.
