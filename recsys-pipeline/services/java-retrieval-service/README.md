# Retrieval service

The retrieval service is a Java 17 Spring Boot application that loads the included ONNX model and
lookup data, reads online features from Redis, and serves ranked recommendations. Its source,
Maven build, and tests work from this directory alone. Redis and optional Kafka integration remain
runtime data contracts supplied by the environment.

## Build and run

From this directory, with Java 17 and Maven 3.8 or later installed:

```sh
mvn clean verify
mvn spring-boot:run
java -jar target/retrieval-service-0.0.1-SNAPSHOT.jar
docker build -t retrieval-service .
```

The executable listens on port 8080 by default. Docker tests use Testcontainers and retain their
existing skip behavior when Docker is unavailable. The image build skips tests because Maven
verification runs separately.

Redis is required for normal online data access. Start Redis separately and set `REDIS_HOST` and
`REDIS_PORT` if it is not at `localhost:6379`. The service includes a small inline catalog and
bundled demo model artifacts, so it can start without an external catalog or trained model files.

Useful endpoints include:

- `GET /recommend/{user}`
- `GET /embedding/{item}`
- `GET /predict/{user}/{item}`
- `GET /users/{user}/profile`
- `GET /metrics`
- `GET /actuator/profile-audit` and `GET /actuator/profile-audit/{user}`

## Runtime configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `REDIS_HOST` | `localhost` | Redis host. |
| `REDIS_PORT` | `6379` | Redis port. |
| `SERVER_PORT` | `8080` | HTTP listener port. |
| `RECSYS_GRPO_EMIT_EVENTS` | `false` | Enables serving-impression event emission. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers when event emission is enabled. |
| `ONLINE_JOINER_INPUT_TOPIC` | `recsys_events` | Kafka topic for emitted serving events. |
| `ONNX_MODEL_PATH` | bundled resource | Filesystem path to replace the bundled ONNX model. |
| `ONNX_LOOKUPS_PATH` | bundled resource | Filesystem path to replace the bundled lookup JSON. |
| `RECSYS_CATALOG_PATH` | unset | Optional catalog JSON merged over the inline catalog. |

Kafka is optional while `RECSYS_GRPO_EMIT_EVENTS` is false. When it is enabled, the Kafka
bootstrap servers and topic must match the downstream online joiner. Redis remains external
infrastructure in every deployment.

## Frozen contract snapshots

The test resources under `src/test/resources/contracts/` are frozen snapshots of producer-owned
contracts: the v3 event schema, a serving-impression Avro payload, and a v1 user-profile JSON
fixture. Maven tests use those local classpath files so this service can be moved without a parent
checkout. They are snapshots, not the canonical producer artifacts.

While this service still lives in the pipeline checkout, run the pipeline-owned comparison from
the repository root:

```sh
python3 -m unittest discover -s recsys-pipeline/integration-tests -p test_retrieval_contracts.py -v
```

For a relocated service checkout, point the comparison at it:

```sh
RETRIEVAL_SERVICE_DIR=/path/to/java-retrieval-service \
  python3 -m unittest discover -s recsys-pipeline/integration-tests -p test_retrieval_contracts.py -v
```

When a producer contract changes, update its canonical pipeline artifact and the matching service
snapshot together after compatibility review. Do not regenerate snapshots from production resources
during service tests. Before moving the directory, copy it as a standalone checkout, run `mvn clean
verify`, and keep this comparison in the producer repository or another shared contract check.
