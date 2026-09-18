# Standalone retrieval service design

## Intent and approved scope

Prepare `recsys-pipeline/services/java-retrieval-service` for moving into the backend repository. The user approved the extraction proposal and requested Superpowers specs/plans, implementation, and a PR. No backend destination was supplied, so this PR makes the directory independently buildable and runnable in place; physical relocation is a follow-up once its destination is known.

## Current boundary

The service already uses the Spring Boot parent directly, Java 17, and published Maven dependencies. The pipeline POM only aggregates it. Production model files, lookup JSON, application configuration, and the event schema already reside inside the service. Five Java test classes access pipeline schemas or profile fixtures through parent-directory paths. There is no standalone README or container build.

## Alternatives

1. **Portable service directory (chosen):** local frozen contract resources, independently runnable tests and packaging, service documentation, optional pipeline-side comparison. Minimal deployment disruption and directly verifies portability.
2. Publish a shared contracts Maven artifact: central ownership but introduces artifact publishing and coordinated releases before the move. Defer until repositories need an established contract release process.
3. Move directly to another repository: requires a destination and its deployment conventions. Defer physical relocation while preparing the complete boundary here.

## Build and contract design

- Java 17 and Spring Boot 3.3.5 remain unchanged; no new runtime dependencies.
- Preserve HTTP APIs, Redis keys, Kafka event bytes, topic defaults, ranking behavior, and model loading behavior.
- Default Maven tests and packaging must require only the service directory, Maven/JDK, and normal Maven dependencies. Docker-backed tests retain their existing skip behavior when Docker is unavailable.
- Store frozen v3 schema and impression bytes plus the user-profile v1 JSON as local test classpath resources. Test helpers must close streams and fail clearly for missing resources. Keep the existing meaningful wire-byte, schema fingerprint, and profile assertions.
- Preserve comparisons to canonical pipeline artifacts in a separate pipeline-owned test. It must accept `RETRIEVAL_SERVICE_DIR` for a relocated service checkout, default to the current path, and fail on missing files or differences rather than silently skipping.
- Contract updates must update the producer canonical artifact and the service snapshot together after compatibility review. Local snapshots must not regenerate themselves from production resources during testing.

## Packaging and operation

Add a Dockerfile whose build context is the service directory, a local `.dockerignore`, `.gitignore`, and a standalone README. Use a Maven/Java 17 build stage and a Java 17 runtime with a non-root process and port 8080. Build the executable Spring Boot JAR without depending on a parent checkout.

Document build/test/run commands; Redis connectivity; optional Kafka emission and its topic; external model/lookup/catalog paths; the included demo artifacts; key HTTP endpoints; contract ownership; and migration steps. Documentation must distinguish source/build independence from required runtime data contracts. Redis remains external infrastructure and Kafka is optional when event emission is disabled. No duplicated pipeline orchestration or training components are needed.

Add GitHub Actions verification that copies only the service into a temporary location and runs Maven verify there. Separately run the pipeline contract comparison with Python's standard library. This catches accidental future parent-directory dependencies. Link the standalone guide from the pipeline README; preserve current orchestration until physical relocation.

## Validation and acceptance

1. Existing module tests establish baseline behavior.
2. Before changes, focused contract tests in an isolated copy reproduce missing parent-directory fixture failures.
3. After changes, all Maven tests and executable JAR packaging pass in a fresh isolated copy; report any Docker-dependent skips.
4. Pipeline comparisons pass for both default and explicit service locations; mutations and missing files produce failures.
5. Validate the container build if Docker is available; otherwise explicitly report the unverified image build and verify JAR startup directly if practical.
6. Review changes and open a PR against master containing this spec, the implementation plan, code, and test evidence.
