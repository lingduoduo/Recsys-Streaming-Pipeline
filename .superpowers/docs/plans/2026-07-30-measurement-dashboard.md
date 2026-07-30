# Measurement Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `./run-movie-category-sim.sh` produce a dashboard in which all seven recommendation measurement sections are `available` with real data, presented as a scorecard plus readable detail sections.

**Architecture:** The movie-category producer gains a documented measurement ground truth so the offline signals exist; `ExperienceCollectorStreamingJob` gains a Parquet sink so slates are reachable on disk; the sim runs that collector and captures a real `/metrics` snapshot from the retrieval service; the exporter hoists demographics out of `user_features`; and the React dashboard adds a seven-tile scorecard with per-section KPI tiles and one chart each.

**Tech Stack:** Python 3/pandas/pytest, Scala 2.12/Spark 3.5/scalatest, Java 17/Spring Boot 3.3, Next.js 15/React 19, bash.

**Spec:** [.superpowers/docs/specs/2026-07-30-measurement-dashboard-design.md](../specs/2026-07-30-measurement-dashboard-design.md)

## Global Constraints

- Measurement-only: do not change candidate generation, filtering decisions, ranking scores, or selection behavior.
- Missing signals stay unavailable with a named reason; nothing is zero-filled.
- No user ID, item ID, request ID, or arbitrary demographic value may become a metric label. Only `governance_measurements.DEFAULT_DIMENSIONS` keys may become fairness columns.
- Every producer effect is documented as a named constant so the dashboard can be validated against what was generated.
- Existing events, Kafka payloads, and dashboard JSON remain readable; every new sim step degrades to N/A rather than failing the run.
- Freshness window is 30 days; the freshness/unsafe/negative-feedback shares are the constants in Task 1.
- A scorecard tile never colors by whether a value is good — only by availability (`coverage < 0.50` is amber).
- Java and Spark builds require JDK 17 at `/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home`.

---

### Task 1: Producer Measurement Ground Truth

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/movie_segment_producer.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py`

**Interfaces:**
- Produces: `assign_users(num_users, rng) -> dict[str, dict]` with keys `gender`, `age_band`, `country`, `subscription`
- Produces: `user_click_bias(user_meta) -> float`
- Produces: `click_completion(meta, rng) -> float`
- Produces: `make_slate(user, user_meta, items, movies, rng) -> list[dict]` (gains the `user_meta` parameter in position 2)
- `assign_movies` gains per-movie keys `published_at` (epoch seconds), `new_release` (bool), `unsafe` (bool)
- Constants later tasks reference: `FRESHNESS_WINDOW_DAYS = 30`, `FRESH_SHARE = 0.15`, `UNSAFE_SHARE = 0.02`, `NEGATIVE_COMPLETION_CUTOFF = 0.10`

- [ ] **Step 1: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py`:

```python
def test_assign_movies_carries_freshness_and_safety_ground_truth():
    import movie_segment_producer as producer
    rng = random.Random(17)
    movies = producer.assign_movies(400, rng)

    fresh = [m for m in movies.values() if m["new_release"]]
    unsafe = [m for m in movies.values() if m["unsafe"]]
    assert 0.08 <= len(fresh) / len(movies) <= 0.24        # FRESH_SHARE 0.15 with sampling slack
    assert 0.00 <= len(unsafe) / len(movies) <= 0.06       # UNSAFE_SHARE 0.02 with sampling slack

    window_seconds = producer.FRESHNESS_WINDOW_DAYS * 86400
    now = int(time.time())
    for movie in movies.values():
        age = now - movie["published_at"]
        assert age >= 0
        assert (age <= window_seconds) == movie["new_release"]


def test_assign_users_is_stable_and_uses_only_allowlisted_dimensions():
    import movie_segment_producer as producer
    users = producer.assign_users(50, random.Random(17))
    again = producer.assign_users(50, random.Random(17))

    assert users == again                                   # seeded: same demographics every run
    assert set(next(iter(users.values()))) == {"gender", "age_band", "country", "subscription"}
    assert {u["subscription"] for u in users.values()} <= set(producer.SUBSCRIPTION_EFF)


def test_slate_events_carry_measurement_signals_on_the_right_event_types():
    import movie_segment_producer as producer
    rng = random.Random(3)
    movies = producer.assign_movies(20, rng)
    items = list(movies)
    user_meta = {"gender": "female", "age_band": "25-34", "country": "us", "subscription": "premium"}

    events = [e for _ in range(200) for e in producer.make_slate("user_1", user_meta, items, movies, rng)]
    impressions = [e for e in events if e["event_type"] == "impression"]
    clicks = [e for e in events if e["event_type"] == "click"]
    orders = [e for e in events if e["event_type"] == "order"]
    assert impressions and clicks and orders

    for event in impressions:
        assert event["user_features"] == user_meta
        assert isinstance(event["published_at"], int)
        assert isinstance(event["new_release"], bool)
        assert isinstance(event["unsafe_label"], bool)
        assert "dwell_millis" not in event and "rating" not in event

    for event in clicks:
        assert 0.0 <= event["completion_rate"] <= 1.0
        assert event["dwell_millis"] >= 0
        expected = "not_interested" if event["completion_rate"] < producer.NEGATIVE_COMPLETION_CUTOFF else None
        assert event["negative_feedback_reason"] == expected

    for event in orders:
        assert 3.0 <= event["rating"] <= 5.0


def test_user_click_bias_creates_a_documented_subscription_gap():
    import movie_segment_producer as producer
    assert producer.user_click_bias({"subscription": "premium"}) > producer.user_click_bias({"subscription": "free"})
    assert producer.user_click_bias({"subscription": "unknown_tier"}) == 0.0
```

Add `import random` and `import time` at the top of the file if they are not already imported.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
cd recsys-pipeline
python3 -m pytest -q integration-tests/python_modeling/test_movie_category_sim.py
```

Expected: the four new tests fail with `AttributeError: module 'movie_segment_producer' has no attribute 'assign_users'` and `KeyError: 'new_release'`.

- [ ] **Step 3: Add the ground-truth constants**

In `movie_segment_producer.py`, below the existing `DECADE_EFF` block:

```python
# Measurement ground truth (recoverable from the dashboard's measurement sections).
# Catalog availability is deliberately independent of release_year: an old film can
# enter the catalog last week, which is what freshness measures.
FRESHNESS_WINDOW_DAYS = 30
FRESH_SHARE = 0.15                 # share of items published inside the window
MAX_CATALOG_AGE_DAYS = 900         # oldest catalog availability instant
UNSAFE_SHARE = 0.02                # share of items an independent labeler flags unsafe
NEGATIVE_COMPLETION_CUTOFF = 0.10  # clicks below this completion report not_interested
GENDERS = ("female", "male", "unknown")
AGE_BANDS = ("18-24", "25-34", "35-49", "50+")
COUNTRIES = ("us", "ca", "gb", "de")
# Additive per-user click effect, so fairness has one explainable gap to report.
SUBSCRIPTION_EFF = {"premium": 0.03, "basic": 0.0, "free": -0.02}
```

- [ ] **Step 4: Extend `assign_movies` and add the user/feedback helpers**

Replace the body of `assign_movies` with:

```python
def assign_movies(num_items: int, rng: random.Random) -> dict[str, dict]:
    """Per-movie metadata: 1-3 genres (primary first), releaseYear 1980-2024, title,
    catalog availability instant, and an independent safety label."""
    movies = {}
    now = int(time.time())
    for i in range(1, num_items + 1):
        primary = rng.choice(GENRES)
        extras = rng.sample([g for g in GENRES if g != primary], rng.randint(0, 2))
        year = rng.randint(1980, 2024)
        fresh = rng.random() < FRESH_SHARE
        age_days = (rng.uniform(0, FRESHNESS_WINDOW_DAYS) if fresh
                    else rng.uniform(FRESHNESS_WINDOW_DAYS + 1, MAX_CATALOG_AGE_DAYS))
        movies[f"movie_{i}"] = {
            "title": f"Movie {i}",
            "genres": [primary] + extras,
            "release_year": year,
            "published_at": now - int(age_days * 86400),
            "new_release": fresh,
            "unsafe": rng.random() < UNSAFE_SHARE,
        }
    return movies


def assign_users(num_users: int, rng: random.Random) -> dict[str, dict]:
    """Per-user demographics, stable across every slate that user appears in.

    Only dimensions in governance_measurements.DEFAULT_DIMENSIONS are emitted, so
    nothing unbounded can reach a published fairness group.
    """
    return {
        f"user_{i}": {
            "gender": rng.choice(GENDERS),
            "age_band": rng.choice(AGE_BANDS),
            "country": rng.choice(COUNTRIES),
            "subscription": rng.choice(tuple(SUBSCRIPTION_EFF)),
        }
        for i in range(1, num_users + 1)
    }


def user_click_bias(user_meta: dict) -> float:
    """Documented additive click effect of the user's subscription tier."""
    return SUBSCRIPTION_EFF.get(user_meta.get("subscription"), 0.0)


def click_completion(meta: dict, rng: random.Random) -> float:
    """Completion tracks the item's appeal: higher-CTR items get watched further."""
    center = min(0.95, item_click_prob(meta) * 3.0)
    return min(1.0, max(0.0, rng.gauss(center, 0.15)))
```

- [ ] **Step 5: Carry the signals on the events**

Replace `make_slate` with:

```python
def make_slate(user: str, user_meta: dict, items, movies: dict, rng: random.Random) -> list[dict]:
    now_ms = int(time.time() * 1000)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    slate_items = rng.sample(items, min(SLATE_SIZE, len(items)))

    def base(item: str, event_type: str, timestamp_ms: int, position: int) -> dict:
        return {
            "event_id": str(uuid.uuid4()), "request_id": request_id, "session_id": session_id,
            "user_id": user, "item_id": item, "event_type": event_type,
            "timestamp_ms": timestamp_ms, "position": position,
            "user_features": dict(user_meta), "item_features": {}, "context_features": {},
        }

    events = []
    for position, item in enumerate(slate_items):
        meta = movies[item]
        impression = base(item, "impression", now_ms, position)
        impression["published_at"] = meta["published_at"]
        impression["new_release"] = meta["new_release"]
        impression["unsafe_label"] = meta["unsafe"]
        events.append(impression)

        # independent per-item click decision → per-item CTR reflects the item's category,
        # shifted by the user's documented subscription effect
        click_prob = min(0.6, max(0.02, item_click_prob(meta) + user_click_bias(user_meta)))
        if rng.random() < click_prob:
            completion = click_completion(meta, rng)
            click = base(item, "click", now_ms + rng.randint(1, 20) * 1000, position)
            click["completion_rate"] = round(completion, 4)
            click["dwell_millis"] = int(completion * 120_000)
            click["negative_feedback_reason"] = (
                "not_interested" if completion < NEGATIVE_COMPLETION_CUTOFF else None)
            events.append(click)

            if rng.random() < order_prob(meta):
                order = base(item, "order", now_ms + rng.randint(21, 120) * 1000, position)
                order["rating"] = round(min(5.0, 3.0 + 2.0 * completion), 1)
                events.append(order)
    return events
```

- [ ] **Step 6: Pass demographics through `main`**

In `main()`, replace the `users` assignment and the slate loop's call:

```python
    user_meta = assign_users(NUM_USERS, rng)
    users = list(user_meta)
```

and

```python
            user = rng.choice(users)
            events = make_slate(user, user_meta[user], items, movies, rng)
```

- [ ] **Step 7: Run the tests and verify GREEN**

Run the command from Step 2. Expected: all tests in the file pass, including the four pre-existing ones (`item_click_prob` keeps its single-argument signature, so they are untouched).

- [ ] **Step 8: Commit**

```bash
git add recsys-pipeline/services/python-modeling/movie_segment_producer.py \
  recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py
git commit -m "feat: emit measurement signals from the movie-category producer"
```

---

### Task 2: Demographic Columns in the Exporter

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `governance_measurements.DEFAULT_DIMENSIONS`
- Produces: `_with_demographic_columns(samples) -> pd.DataFrame`
- Called from `build_measurement_dashboard` alongside the existing `_with_published_timestamps`

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`:

```python
def test_demographics_are_hoisted_from_user_features_within_the_allowlist():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    samples = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "clicked": [1, 0],
        # dict shape (JSON input) and key/value-pair shape (pyarrow map) both occur
        "user_features": [
            {"gender": "female", "subscription": "premium", "email": "a@b.c"},
            [("gender", "male"), ("subscription", "free"), ("email", "d@e.f")],
        ],
    })

    hoisted = dash._with_demographic_columns(samples)

    assert list(hoisted["gender"]) == ["female", "male"]
    assert list(hoisted["subscription"]) == ["premium", "free"]
    assert "email" not in hoisted.columns          # outside DEFAULT_DIMENSIONS: never published
    assert "gender" not in samples.columns         # the input frame is not mutated


def test_demographic_hoisting_is_a_no_op_without_user_features():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    samples = pd.DataFrame({"user_id": ["u1"], "clicked": [1]})
    assert dash._with_demographic_columns(samples) is samples
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cd recsys-pipeline
python3 -m pytest -q integration-tests/python_modeling/test_analysis_dashboard.py -k demographic
```

Expected: `AttributeError: module 'analysis_dashboard_report' has no attribute '_with_demographic_columns'`.

- [ ] **Step 3: Implement the adapter**

In `analysis_dashboard_report.py`, directly below `_with_published_timestamps`:

```python
def _feature_map(value) -> dict:
    """Read a features column entry as a dict; Parquet maps decode as key/value pairs."""
    if isinstance(value, dict):
        return value
    if isinstance(value, (list, tuple)):
        return {pair[0]: pair[1] for pair in value if len(pair) == 2}
    return {}


def _with_demographic_columns(samples):
    """Hoist allowlisted demographics out of user_features into fairness columns.

    The allowlist is the cardinality guard: a key outside DEFAULT_DIMENSIONS is
    never promoted, so no arbitrary user attribute can become a published group.
    """
    from governance_measurements import DEFAULT_DIMENSIONS
    if "user_features" not in samples.columns:
        return samples
    features = [_feature_map(value) for value in samples["user_features"]]
    missing = [dimension for dimension in DEFAULT_DIMENSIONS
               if dimension not in samples.columns and any(dimension in entry for entry in features)]
    if not missing:
        return samples
    hoisted = samples.copy()
    for dimension in missing:
        hoisted[dimension] = [entry.get(dimension) for entry in features]
    return hoisted
```

- [ ] **Step 4: Call it from the dashboard builder**

In `build_measurement_dashboard`, replace the `measured` assignment:

```python
    measured = _with_demographic_columns(_with_published_timestamps(samples))
```

- [ ] **Step 5: Run the tests and verify GREEN**

```bash
cd recsys-pipeline
python3 -m pytest -q integration-tests/python_modeling/test_analysis_dashboard.py \
  integration-tests/python_modeling/test_dashboard_measurement_contract.py
```

Expected: all pass — the contract tests still pass because frames without `user_features` are returned unchanged.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
  recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat: hoist allowlisted demographics for fairness measurement"
```

---

### Task 3: Slate Parquet Sink

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/ExperienceCollectorStreamingJob.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/ExperienceCollectorStreamingJobSpec.scala`

**Interfaces:**
- Consumes: `com.demo.engine.ParquetSink(path, partitionCol, outputFiles, transform)` with `write(batch: DataFrame, batchId: Long)`
- Produces: `parquetSink(outputPath: String, outputFiles: Int): Option[ParquetSink]`
- Environment: `EXPERIENCE_COLLECTOR_OUTPUT_PATH` (unset = Kafka only), `EXPERIENCE_COLLECTOR_OUTPUT_FILES` (default 1)

- [ ] **Step 1: Write the failing test**

Append inside `ExperienceCollectorStreamingJobSpec`:

```scala
  it should "not create a Parquet sink when no output path is configured" in {
    ExperienceCollectorStreamingJob.parquetSink("", 1) shouldBe None
  }

  it should "write slates as date-partitioned Parquet when an output path is configured" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq(
      """{"sample_id":"s1","request_id":"r1","user_id":"u1","item_id":"item_1","position":0,"impression_ts":1753000000,"clicked":1,"ordered":0,"label":1.0}""",
      """{"sample_id":"s2","request_id":"r1","user_id":"u1","item_id":"item_2","position":1,"impression_ts":1753000000,"clicked":0,"ordered":0,"label":0.0}"""
    ).toDF("value")
    val slates = ExperienceCollectorStreamingJob.buildSlates(
      ExperienceCollectorStreamingJob.parseSamples(samples))

    val target = java.nio.file.Files.createTempDirectory("slate-sink").resolve("slates").toString
    ExperienceCollectorStreamingJob.parquetSink(target, 1).get.write(slates, 0L)

    val written = sparkSession.read.parquet(target)
    written.count() shouldBe 1
    written.columns should contain allOf ("slate_id", "request_id", "items", "date")
    written.select("items").first().getAs[Seq[org.apache.spark.sql.Row]](0).size shouldBe 2
  }
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cd recsys-pipeline/services/spark-streaming-job
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  PATH="$JAVA_HOME/bin:$PATH" \
  sbt -batch "testOnly com.demo.process.ExperienceCollectorStreamingJobSpec"
```

Expected: compilation fails with `value parquetSink is not a member of object ExperienceCollectorStreamingJob`.

- [ ] **Step 3: Implement the sink factory**

Add the imports at the top of `ExperienceCollectorStreamingJob.scala`:

```scala
import com.demo.engine.ParquetSink
```

`org.apache.spark.sql.functions._` is already imported, which supplies `from_unixtime` and `to_date`.

Add above `parseSamples`:

```scala
  /** Optional Parquet sink for slate experiences, mirroring the joiner's training-sample
    * sink. Returns None when no path is configured, leaving the Kafka path untouched. */
  def parquetSink(outputPath: String, outputFiles: Int): Option[ParquetSink] =
    if (outputPath.isEmpty) None
    else Some(new ParquetSink(outputPath, "date", math.max(1, outputFiles),
      (df: DataFrame) => df.withColumn("date", to_date(from_unixtime(col("request_ts"))))))
```

- [ ] **Step 4: Wire it into the streaming write**

In `main`, add below the `outputTopic` line:

```scala
    val outputPath  = sys.env.getOrElse("EXPERIENCE_COLLECTOR_OUTPUT_PATH", "")
    val outputFiles = sys.env.get("EXPERIENCE_COLLECTOR_OUTPUT_FILES").map(_.toInt).getOrElse(1)
    val slateSink   = parquetSink(outputPath, outputFiles)
```

and inside `foreachBatch`, after the existing Kafka `.save()` call:

```scala
        slateSink.foreach(_.write(slates, batchId))
```

- [ ] **Step 5: Run the test and verify GREEN**

Run the command from Step 2. Expected: all `ExperienceCollectorStreamingJobSpec` tests pass.

- [ ] **Step 6: Run the full Spark suite**

```bash
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  PATH="$JAVA_HOME/bin:$PATH" sbt -batch test
```

Expected: all suites pass, zero failures.

- [ ] **Step 7: Commit**

```bash
git add src/main src/test
git commit -m "feat: write slate experiences to Parquet"
```

---

### Task 4: Sim Wiring

**Files:**
- Modify: `recsys-pipeline/run-movie-category-sim.sh`
- Test: `recsys-pipeline/integration-tests/test_service_scripts.py`

**Interfaces:**
- Consumes: `EXPERIENCE_COLLECTOR_OUTPUT_PATH` from Task 3; the exporter's `--experiences` / `--live-metrics` flags
- Produces: `$SIM_ROOT/slates/` (Parquet), `$SIM_ROOT/live-metrics.json`
- Environment: `MEASUREMENT_BURST_REQUESTS` (default 50), `RETRIEVAL_SERVICE_PORT` (default 8080)

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/test_service_scripts.py`:

```python
SIM_SCRIPT = Path(__file__).parents[1] / "run-movie-category-sim.sh"


def test_movie_category_sim_wires_every_measurement_input() -> None:
    script = SIM_SCRIPT.read_text(encoding="utf-8")

    # slates: the collector must run and land Parquet the exporter can read
    assert "com.demo.process.ExperienceCollectorStreamingJob" in script
    assert "EXPERIENCE_COLLECTOR_OUTPUT_PATH" in script
    # latency: a real /metrics capture from the running service
    assert "/metrics" in script and "live-metrics.json" in script
    # export: both optional inputs reach the exporter, and the snapshot is validated
    assert "--experiences" in script and "--live-metrics" in script
    assert "validate:data" in script


def test_movie_category_sim_never_fails_on_a_missing_live_service() -> None:
    script = SIM_SCRIPT.read_text(encoding="utf-8")
    burst = script.split("SERVICE BURST")[1]
    # the service block must not abort the sim: every failure path continues
    assert "|| true" in burst or "continue" in burst
    assert "set -e" not in burst
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cd recsys-pipeline
python3 -m pytest -q integration-tests/test_service_scripts.py -k measurement
```

Expected: both fail — the script has no collector step, no burst block, and no export flags.

- [ ] **Step 3: Add the slate collector step**

In `run-movie-category-sim.sh`, add near the other path variables at the top:

```bash
SLATE_DIR="$SIM_ROOT/slates"
LIVE_METRICS="$SIM_ROOT/live-metrics.json"
SERVICE_PORT="${RETRIEVAL_SERVICE_PORT:-8080}"
BURST_REQUESTS="${MEASUREMENT_BURST_REQUESTS:-50}"
```

Directly after the existing `run_and_drain com.demo.process.OnlineJoinerStreamingJob ...` step:

```bash
echo
echo "==> SLATE EXPERIENCES (training_samples → slate Parquet for relevance/diversity)"
run_and_drain com.demo.process.ExperienceCollectorStreamingJob exp-ckpt slates \
  "find \"$SLATE_DIR\" -name '*.parquet' | wc -l" 0 \
  "EXPERIENCE_COLLECTOR_OUTPUT_PATH=$SLATE_DIR" \
  "EXPERIENCE_COLLECTOR_INPUT_TOPIC=$SAMPLES_TOPIC" \
  "EXPERIENCE_COLLECTOR_OUTPUT_TOPIC=$SLATES_TOPIC"
```

Add the two per-run topic names beside the existing `RECSYS_TOPIC` / `CONTEXT_TOPIC` definitions near the top of the script, following their naming pattern (Kafka auto-creates them, exactly as it does for the existing two — the script never issues `kafka-topics --create`):

```bash
SAMPLES_TOPIC="training_samples_${RUN_ID}"
SLATES_TOPIC="training_experiences_${RUN_ID}"
```

Then extend the existing joiner step so it publishes to the per-run samples topic that the collector consumes — change its env list to:

```bash
run_and_drain com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "find \"$OUT_DIR\" -name '*.parquet' | wc -l" 0 \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR" "ONLINE_JOINER_INPUT_TOPIC=$RECSYS_TOPIC" \
  "ONLINE_JOINER_OUTPUT_TOPIC=$SAMPLES_TOPIC"
```

- [ ] **Step 4: Add the live-service burst**

After the embeddings block and before the dashboard export:

```bash
echo
echo "==> SERVICE BURST (real /metrics latency, freshness, and filter decisions)"
service_pid=""
(cd services/java-retrieval-service && \
  JAVA_HOME="${MEASUREMENT_JAVA_HOME:-$JAVA_HOME}" \
  SERVER_PORT="$SERVICE_PORT" REDIS_HOST=localhost \
  mvn -q -DskipTests spring-boot:run >"$SIM_ROOT/service.log" 2>&1) &
service_pid=$!
for _ in $(seq 1 40); do
  curl -sf "http://localhost:$SERVICE_PORT/metrics" >/dev/null 2>&1 && break; sleep 3
done

if curl -sf "http://localhost:$SERVICE_PORT/metrics" >/dev/null 2>&1; then
  for i in $(seq 1 "$BURST_REQUESTS"); do
    user="user_$(( (i % 10) + 1 ))"
    item="$(curl -sf "http://localhost:$SERVICE_PORT/recommend/$user?limit=6" \
      | python3 -c 'import json,sys; d=json.load(sys.stdin).get("recommendations") or [{}]; print(d[0].get("item",""))' 2>/dev/null || true)"
    if [[ -n "$item" && $(( i % 2 )) -eq 0 ]]; then
      curl -sf -X POST "http://localhost:$SERVICE_PORT/feedback" \
        -H 'Content-Type: application/json' \
        -d "{\"user\":\"$user\",\"item\":\"$item\",\"clicked\":true,\"reward\":1.0,\"rating\":4.5,\"dwellMillis\":12000,\"completionRate\":0.75}" \
        >/dev/null 2>&1 || true
    fi
  done
  curl -sf "http://localhost:$SERVICE_PORT/metrics" > "$LIVE_METRICS" 2>/dev/null || true
  echo "   captured $(wc -c < "$LIVE_METRICS" 2>/dev/null || echo 0) bytes of live metrics"
else
  echo "   service did not start (see $SIM_ROOT/service.log) — latency stays N/A"
fi
kill "$service_pid" 2>/dev/null || true
wait "$service_pid" 2>/dev/null || true
```

- [ ] **Step 5: Pass the new inputs to the exporter**

Replace the dashboard export block's tail with a React-snapshot export that receives every input, keeping the existing HTML report call untouched:

```bash
echo
echo "==> REACT DASHBOARD SNAPSHOT (seven measurement sections)"
export_args=(--input "$OUT_DIR" --output ../frontend/data/dashboard.json)
[[ -d "$SLATE_DIR" ]] && export_args+=(--experiences "$SLATE_DIR")
[[ -s "$LIVE_METRICS" ]] && export_args+=(--live-metrics "$LIVE_METRICS")
REDIS_HOST=localhost REDIS_PORT=6379 \
  python3 ../frontend/export_dashboard_json.py "${export_args[@]}" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"
(cd ../frontend && npm run validate:data)
```

- [ ] **Step 6: Run the tests and verify GREEN**

Run the command from Step 2, then the whole file:

```bash
cd recsys-pipeline
python3 -m pytest -q integration-tests/test_service_scripts.py
```

Expected: the two new tests pass. `test_streaming_script_uses_consolidated_spark_service_path` fails only if a local Kafka is unavailable — that failure is pre-existing and unrelated.

- [ ] **Step 7: Check the script parses**

```bash
bash -n recsys-pipeline/run-movie-category-sim.sh
```

Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add recsys-pipeline/run-movie-category-sim.sh \
  recsys-pipeline/integration-tests/test_service_scripts.py
git commit -m "feat: capture slates and live metrics in the movie-category sim"
```

---

### Task 5: Scorecard

**Files:**
- Modify: `frontend/components/ui.jsx`
- Modify: `frontend/components/sections.jsx`
- Modify: `frontend/app/page.jsx`
- Modify: `frontend/app/globals.css`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py`

**Interfaces:**
- Produces: `MetricTile({ title, value, label, sampleSize, status, reason, href })` in `ui.jsx`
- Produces: `Scorecard({ data })` in `sections.jsx`, reading the seven measurement keys from the dashboard payload
- Produces: `HEADLINES` — the map from section key to `{ rowIndex, field, label, format }` used by both the scorecard and the test

- [ ] **Step 1: Write the failing test**

Append to `test_dashboard_measurement_contract.py`:

```python
def test_scorecard_headline_fields_exist_in_the_published_rows(tmp_path, monkeypatch):
    """Every field the scorecard reads must be a real key in that section's rows."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    monkeypatch.setenv("REDIS_PORT", "6399")

    samples = _samples_frame(pd)
    samples.to_parquet(tmp_path / "samples", index=False)
    _slates_frame(pd, samples).to_parquet(tmp_path / "experiences", index=False)
    (tmp_path / "live.json").write_text(json.dumps(_live_metrics()))
    output = _export(tmp_path, experiences=tmp_path / "experiences", live=tmp_path / "live.json",
                     extra_args=["--fairness-min-support", "1"])

    sections = (_REPO / "frontend" / "components" / "sections.jsx").read_text()
    headlines = re.search(r"const HEADLINES = \{(.*?)\n\};", sections, re.S)
    assert headlines, "sections.jsx must declare a HEADLINES map"

    declared = re.findall(r'(\w+):\s*\{[^}]*field:\s*"([^"]+)"', headlines.group(1))
    assert {key for key, _ in declared} == MEASUREMENT_KEYS

    for key, field in declared:
        published = {column for row in output[key]["rows"] for column in row}
        assert field in published, f"scorecard reads {key}.{field}, which no row publishes"
```

- [ ] **Step 2: Run the test and verify RED**

```bash
cd recsys-pipeline
python3 -m pytest -q integration-tests/python_modeling/test_dashboard_measurement_contract.py -k scorecard
```

Expected: `AssertionError: sections.jsx must declare a HEADLINES map`.

- [ ] **Step 3: Add the tile component**

Append to `frontend/components/ui.jsx`:

```jsx
// One scorecard tile. Status reflects DATA AVAILABILITY only — never whether the
// number is good, because no targets have been set for these measurements.
export function MetricTile({ title, value, label, sampleSize, status, reason, href }) {
  return (
    <a className={`metric-tile status-${status}`} href={href}>
      <span className="metric-title">{title}</span>
      <span className="metric-value">{value}</span>
      <span className="metric-label">{label}</span>
      <span className="metric-support">
        {status === "na" ? reason : `n=${(sampleSize ?? 0).toLocaleString()}`}
      </span>
    </a>
  );
}
```

- [ ] **Step 4: Add the scorecard**

In `frontend/components/sections.jsx`, above `MeasurementSection`:

```jsx
import { Section, NaCard, BarChart, DataTable, MetricTile } from "./ui";

// Which single number represents each measurement on the scorecard. `field` must be a
// key the calculator actually publishes — the contract test enforces that.
const HEADLINES = {
  relevance: { rowIndex: 1, field: "ndcg_at_k", label: "NDCG@10", format: "num" },
  satisfaction: { rowIndex: 0, field: "ctr", label: "CTR", format: "pct" },
  freshness: { rowIndex: 0, field: "fresh_share", label: "fresh share", format: "pct" },
  diversity: { rowIndex: 0, field: "normalized_genre_entropy", label: "genre entropy", format: "num" },
  fairness: { rowIndex: 0, field: "ctr_max_min_gap", label: "largest CTR gap", format: "num" },
  safety: { rowIndex: 0, field: "unsafe_exposure_rate", label: "unsafe exposure", format: "pct" },
  // Endpoint rows are emitted in sorted order over the fixed {feedback, recommend}
  // allowlist, so index 1 is always /recommend.
  latency: { rowIndex: 1, field: "p95", label: "p95 /recommend", format: "ms" },
};

const TITLES = {
  relevance: "Relevance", satisfaction: "Satisfaction", freshness: "Freshness",
  diversity: "Diversity", fairness: "Fairness", safety: "Safety", latency: "Latency",
};

const LOW_COVERAGE = 0.5;

function headlineValue(section, spec) {
  const row = section.rows?.[spec.rowIndex] ?? section.rows?.[0];
  const value = row?.[spec.field];
  if (value === null || value === undefined) return "N/A";
  if (spec.format === "pct") return share(value);
  if (spec.format === "ms") return `${num(value, 1)} ms`;
  return num(value, 3);
}

export function Scorecard({ data }) {
  return (
    <section className="scorecard">
      {Object.entries(HEADLINES).map(([key, spec]) => {
        const section = data[key];
        const available = section?.status === "available";
        const status = !available ? "na" : (section.coverage ?? 1) < LOW_COVERAGE ? "low" : "ok";
        return (
          <MetricTile
            key={key}
            href={`#${key}`}
            title={TITLES[key]}
            value={available ? headlineValue(section, spec) : "N/A"}
            label={spec.label}
            sampleSize={section?.sampleSize}
            status={status}
            reason={section?.warnings?.[0] || "measurement unavailable"}
          />
        );
      })}
    </section>
  );
}
```

Give each section an anchor by adding `id` to the `Section` wrapper in `MeasurementSection`: change its opening tag to `<Section title={title} headline={data.headline} id={title.toLowerCase()}>` and, in `ui.jsx`, change `Section` to accept and apply it: `export function Section({ title, headline, children, id })` with `<section className="report-card" id={id}>`. Apply the same `id` to `NaCard`: `export function NaCard({ title, reason, id })` with `<section className="report-card status-card" id={id}>`, and pass `id={title.toLowerCase()}` from `MeasurementSection`'s unavailable branch.

- [ ] **Step 5: Render it**

In `frontend/app/page.jsx`, import `Scorecard` from `../components/sections` and place it as the first child of `<div className="report-grid">`, above `<RelevanceSection …>`.

- [ ] **Step 6: Style it**

Append to `frontend/app/globals.css`:

```css
.scorecard {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
}

.metric-tile {
  display: grid;
  gap: 2px;
  padding: 16px 18px;
  border: 1px solid var(--line);
  border-left: 4px solid var(--muted);
  border-radius: 14px;
  background: var(--surface);
  box-shadow: var(--shadow);
  text-decoration: none;
  color: inherit;
}

.metric-tile.status-ok { border-left-color: #0d9488; }
.metric-tile.status-low { border-left-color: #b45309; }
.metric-tile.status-na { border-left-color: var(--muted); background: #f8fafc; }

.metric-title {
  font-size: 0.68rem;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
}

.metric-value {
  font-size: 1.6rem;
  font-weight: 800;
  letter-spacing: -0.02em;
  font-variant-numeric: tabular-nums;
}

.metric-label { font-size: 0.8rem; color: var(--muted); }
.metric-support { font-size: 0.72rem; color: var(--muted); }
```

- [ ] **Step 7: Run the test and the build**

```bash
cd recsys-pipeline && python3 -m pytest -q integration-tests/python_modeling/test_dashboard_measurement_contract.py
cd ../frontend && npm run validate:data && npm run build
```

Expected: tests pass; the build compiles and prerenders with no warnings.

- [ ] **Step 8: Commit**

```bash
git add frontend/app frontend/components \
  recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py
git commit -m "feat: summarize measurements in a scorecard"
```

---

### Task 6: Detail KPIs and Charts

**Files:**
- Modify: `frontend/components/ui.jsx`
- Modify: `frontend/components/sections.jsx`
- Modify: `frontend/app/globals.css`

**Interfaces:**
- Consumes: `HEADLINES`, `MeasurementSection` from Task 5
- Produces: `GroupedBarChart({ series, labels, title })` in `ui.jsx`, where `series` is `[{ name, values }]`
- Produces: `MeasurementSection` gains a `kpis` prop (`(rows) => [{label, value}]`) and a `chart` prop (`(rows) => ReactNode`), rendered in that order between the coverage line and the table
- Constraint: every field a `kpis` accessor reads must also appear in that section's `columns`, so the existing column-correspondence test already covers it — do not read a field the table does not show

- [ ] **Step 1: Load the visualization guidance**

Invoke the `dataviz` skill before writing any chart code, and take the categorical palette and axis/legend rules from it rather than inventing colors. The existing `BarChart` uses `#4f46e5`; the second series must come from that skill's palette, not an arbitrary hex.

- [ ] **Step 2: Add the grouped bar chart**

Append to `frontend/components/ui.jsx`:

```jsx
// Two-series horizontal bars sharing one scale — for comparisons where a single
// series would hide the relationship (ndcg vs mrr, fresh vs established).
export function GroupedBarChart({ series, labels, title, width = 520, barH = 14, gap = 6 }) {
  const colors = ["#4f46e5", "#0d9488"];
  const all = series.flatMap((s) => s.values).filter((v) => Number.isFinite(v));
  const vmax = Math.max(...all, 0) || 1;
  const groupH = series.length * (barH + 2) + gap;
  const h = Math.max(labels.length, 1) * groupH;
  return (
    <svg className="chart" role="img" viewBox={`0 -20 ${width} ${h + 24}`} width={width} fontFamily="sans-serif">
      {title ? <text x="0" y="-6" fontSize="13" fontWeight="bold">{title}</text> : null}
      {series.map((s, si) => (
        <text key={s.name} x={String(60 + si * 110)} y="-6" fontSize="11" fill={colors[si % colors.length]}>
          {s.name}
        </text>
      ))}
      {labels.map((label, li) => (
        <g key={`${label}-${li}`}>
          <text x="0" y={li * groupH + barH} fontSize="12">{label}</text>
          {series.map((s, si) => {
            const v = s.values[li] ?? 0;
            const w = Math.max(0, Math.round((v / vmax) * (width - 200)));
            return (
              <g key={s.name}>
                <title>{`${s.name} ${label}: ${v}`}</title>
                <rect x="150" y={li * groupH + si * (barH + 2)} width={w} height={barH} rx="4"
                      fill={colors[si % colors.length]} />
                <text x={155 + w} y={li * groupH + si * (barH + 2) + barH - 2} fontSize="10">{v}</text>
              </g>
            );
          })}
        </g>
      ))}
    </svg>
  );
}
```

- [ ] **Step 3: Give `MeasurementSection` KPI and chart slots**

In `sections.jsx`, change the signature and body of `MeasurementSection`:

```jsx
function MeasurementSection({ title, data, columns, kpis, chart, children }) {
  if (!data || data.status !== "available") {
    return <NaCard title={title} id={title.toLowerCase()} reason={data?.warnings?.[0] || "measurement unavailable"} />;
  }
  const rows = data.rows || [];
  const values = kpis ? kpis(rows) : [];
  return (
    <Section title={title} headline={data.headline} id={title.toLowerCase()}>
      <p className="fine-print">
        sample size {data.sampleSize?.toLocaleString() ?? "N/A"} · coverage {share(data.coverage)}
        {data.window ? ` · window ${data.window}` : ""}
      </p>
      {data.warnings?.length ? <p className="na">{data.warnings.join(" · ")}</p> : null}
      {values.length ? (
        <div className="kpi-row">
          {values.map((kpi) => (
            <div className="kpi" key={kpi.label}>
              <span className="kpi-value">{kpi.value}</span>
              <span className="kpi-label">{kpi.label}</span>
            </div>
          ))}
        </div>
      ) : null}
      {chart ? chart(rows) : null}
      <DataTable rows={rows} columns={columns} />
      {children}
    </Section>
  );
}
```

Add the KPI styles to `frontend/app/globals.css`:

```css
.kpi-row {
  display: flex;
  flex-wrap: wrap;
  gap: 28px;
}

.kpi { display: grid; gap: 2px; }

.kpi-value {
  font-size: 1.25rem;
  font-weight: 800;
  font-variant-numeric: tabular-nums;
}

.kpi-label {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--muted);
}
```

Then give each section a `kpis` accessor reading only fields its `columns` already list:

```jsx
// Relevance — the k=10 row
kpis={(rows) => {
  const row = rows.find((r) => r.k === 10) || rows[0] || {};
  return [
    { label: "NDCG@10", value: num(row.ndcg_at_k, 3) },
    { label: "MRR@10", value: num(row.mrr_at_k, 3) },
    { label: "recall@10", value: num(row.recall_at_k, 3) },
    { label: "slates", value: (row.evaluated_slate_count ?? 0).toLocaleString() },
  ];
}}

// Satisfaction — the offline row
kpis={(rows) => {
  const row = rows[0] || {};
  return [
    { label: "CTR", value: share(row.ctr) },
    { label: "order rate", value: share(row.order_rate) },
    { label: "mean rating", value: num(row.mean_rating, 2) },
    { label: "mean dwell", value: num(row.mean_dwell_millis, 0) },
  ];
}}

// Freshness
kpis={(rows) => {
  const row = rows[0] || {};
  return [
    { label: "fresh share", value: share(row.fresh_share) },
    { label: "mean age (days)", value: num(row.mean_content_age_days, 1) },
    { label: "fresh CTR", value: share(row.fresh_ctr) },
    { label: "established CTR", value: share(row.established_ctr) },
  ];
}}

// Diversity — the aggregate row
kpis={(rows) => {
  const row = rows.find((r) => r.scope === "aggregate") || rows[0] || {};
  return [
    { label: "genre entropy", value: num(row.normalized_genre_entropy, 3) },
    { label: "unique genres", value: num(row.unique_genres_at_k, 2) },
    { label: "intra-list distance", value: num(row.intra_list_genre_distance, 3) },
    { label: "long-tail share", value: share(row.long_tail_exposure_share) },
  ];
}}

// Fairness — the first dimension
kpis={(rows) => {
  const row = rows[0] || {};
  return [
    { label: "overall CTR", value: share(row.overall_ctr) },
    { label: "CTR gap", value: num(row.ctr_max_min_gap, 3) },
    { label: "groups", value: String(row.evaluated_group_count ?? 0) },
    { label: "suppressed", value: String(row.suppressed_group_count ?? 0) },
  ];
}}

// Safety — the offline row plus whichever row logged decisions
kpis={(rows) => {
  const offline = rows[0] || {};
  const filtered = rows.find((r) => r.filter_decision_rate !== null && r.filter_decision_rate !== undefined) || {};
  return [
    { label: "unsafe exposure", value: share(offline.unsafe_exposure_rate) },
    { label: "label coverage", value: share(offline.unsafe_label_coverage) },
    { label: "filter rate", value: share(filtered.filter_decision_rate) },
    { label: "policy", value: String(offline.policy_version ?? filtered.policy_version ?? "N/A") },
  ];
}}

// Latency — the /recommend endpoint row
kpis={(rows) => {
  const row = rows.filter((r) => r.scope === "endpoint")[1] || rows[0] || {};
  return [
    { label: "p50", value: `${num(row.p50, 1)} ms` },
    { label: "p95", value: `${num(row.p95, 1)} ms` },
    { label: "p99", value: `${num(row.p99, 1)} ms` },
    { label: "requests", value: (row.count ?? 0).toLocaleString() },
  ];
}}
```

- [ ] **Step 4: Give each section its chart**

Add a `chart` prop to each section. Relevance:

```jsx
      chart={(rows) => (
        <GroupedBarChart
          title="Relevance by cutoff"
          labels={rows.map((r) => `k=${r.k}`)}
          series={[
            { name: "ndcg", values: rows.map((r) => r.ndcg_at_k ?? 0) },
            { name: "mrr", values: rows.map((r) => r.mrr_at_k ?? 0) },
          ]}
        />
      )}
```

Satisfaction — which optional signals are instrumented:

```jsx
      chart={(rows) => {
        const row = rows[0] || {};
        const fields = ["rating_coverage", "negative_feedback_coverage", "dwell_coverage", "completion_coverage"];
        return (
          <BarChart title="Optional signal coverage"
            labels={fields.map((f) => f.replace("_coverage", ""))}
            values={fields.map((f) => row[f] ?? 0)} />
        );
      }}
```

Freshness — fresh vs established:

```jsx
      chart={(rows) => {
        const row = rows[0] || {};
        return (
          <BarChart title="CTR by content age"
            labels={["fresh", "established"]}
            values={[row.fresh_ctr ?? 0, row.established_ctr ?? 0]} />
        );
      }}
```

Diversity — the three 0–1 aggregate measures:

```jsx
      chart={(rows) => {
        const row = rows.find((r) => r.scope === "aggregate") || rows[0] || {};
        return (
          <BarChart title="Diversity (0–1)"
            labels={["genre entropy", "intra-list distance", "long-tail share"]}
            values={[row.normalized_genre_entropy ?? 0, row.intra_list_genre_distance ?? 0,
                     row.long_tail_exposure_share ?? 0]} />
        );
      }}
```

Fairness — CTR per group of the first dimension:

```jsx
      chart={(rows) => {
        const groups = rows[0]?.groups || [];
        return groups.length ? (
          <BarChart title={`CTR by ${rows[0].dimension} (overall ${num(rows[0].overall_ctr, 3)})`}
            labels={groups.map((g) => g.group)} values={groups.map((g) => g.ctr ?? 0)} />
        ) : null;
      }}
```

Safety — filter decisions by reason, taken from whichever row logged them:

```jsx
      chart={(rows) => {
        const counts = rows.map((r) => r.reason_counts).find((c) => c && Object.keys(c).length) || {};
        const entries = Object.entries(counts).filter(([, v]) => v !== null && v !== undefined);
        return entries.length ? (
          <BarChart title="Filter decisions by reason"
            labels={entries.map(([k]) => k)} values={entries.map(([, v]) => v)} />
        ) : null;
      }}
```

Latency — p95 per stage:

```jsx
      chart={(rows) => {
        const stages = rows.filter((r) => r.scope === "stage");
        return stages.length ? (
          <BarChart title="p95 by stage (ms)"
            labels={stages.map((r) => r.name)} values={stages.map((r) => r.p95 ?? 0)} />
        ) : null;
      }}
```

Import `GroupedBarChart` alongside the other `ui` imports.

- [ ] **Step 5: Build and inspect**

```bash
cd frontend && npm run validate:data && npm run build
```

Expected: compiles and prerenders with no warnings.

- [ ] **Step 6: Verify the charts render with real data**

Generate a populated snapshot into a scratch directory, build against it, confirm the charts appear in the prerendered HTML, then restore the committed snapshot:

```bash
cd recsys-pipeline
python3 - <<'PY'
import sys, json, pathlib
sys.path[:0] = ["integration-tests/python_modeling", "services/python-modeling", "../frontend"]
import pandas as pd, test_dashboard_measurement_contract as fixture, export_dashboard_json as exporter
out = pathlib.Path("/tmp/measurement-fixture"); out.mkdir(exist_ok=True)
samples = fixture._samples_frame(pd)
samples.to_parquet(out / "samples", index=False)
fixture._slates_frame(pd, samples).to_parquet(out / "experiences", index=False)
(out / "live.json").write_text(json.dumps(fixture._live_metrics()))
exporter.main(["--input", str(out / "samples"), "--experiences", str(out / "experiences"),
               "--live-metrics", str(out / "live.json"), "--fairness-min-support", "1",
               "--output", "../frontend/data/dashboard.json"])
PY
cd ../frontend && npm run build && grep -c "metric-tile" .next/server/app/index.html
cd .. && git checkout frontend/data/dashboard.json
```

Expected: the build succeeds and the grep reports 7 tiles. The final `git checkout` restores the committed snapshot — Task 7 replaces it with a real one.

- [ ] **Step 7: Commit**

```bash
git add frontend/app frontend/components
git commit -m "feat: chart each measurement section"
```

---

### Task 7: Real Run, Snapshot, and Documentation

**Files:**
- Modify: `frontend/data/dashboard.json`
- Modify: `recsys-pipeline/README.md`
- Modify: `frontend/README.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: everything from Tasks 1-6
- Produces: a committed snapshot in which all seven measurement sections are `available`

- [ ] **Step 1: Confirm the prerequisites**

The sim needs Docker running and Spark on `$SPARK_HOME`. Verify before starting:

```bash
docker info >/dev/null && echo "docker ok"
ls "$SPARK_HOME/bin/spark-submit" && echo "spark ok"
```

If either fails, stop and ask — do not fabricate a snapshot.

- [ ] **Step 2: Run the instrumented sim**

```bash
cd recsys-pipeline
./run-movie-category-sim.sh 2>&1 | tail -40
```

Expected: the run reaches `==> done`, having printed the slate-collector drain counts, a non-zero live-metrics byte count, and `dashboard.json valid: 7 measurement sections`.

- [ ] **Step 3: Verify every section is populated**

```bash
python3 - <<'PY'
import json
data = json.load(open("../frontend/data/dashboard.json"))
for key in ("relevance", "satisfaction", "freshness", "diversity", "fairness", "safety", "latency"):
    section = data[key]
    print(f"{key:14} {section['status']:12} n={section.get('sampleSize')} rows={len(section['rows'])}")
    assert section["status"] == "available", (key, section["warnings"])
    assert section["sampleSize"] >= 1
PY
```

Expected: seven `available` lines with positive sample sizes. If a section is unavailable, its warning names the missing input — fix that input rather than relaxing the assertion.

- [ ] **Step 4: Look at the rendered page**

```bash
cd frontend && npm run build && npm run start
```

Open `http://localhost:3000`, confirm the scorecard shows seven populated tiles and each detail section renders its chart and table, then stop the server.

- [ ] **Step 5: Document the new inputs**

In `recsys-pipeline/README.md`, replace the manual `kafka-console-consumer` slate-dump instructions in the "Capture the inputs" block with the sim's own outputs, since the collector now writes Parquet:

```bash
IN=/tmp/spark-recsys/movie-category-sim
REDIS_HOST=localhost python ../frontend/export_dashboard_json.py \
  --input "$IN/training-samples" \
  --experiences "$IN/slates" \
  --live-metrics "$IN/live-metrics.json" \
  --output ../frontend/data/dashboard.json
```

State that `./run-movie-category-sim.sh` does all of this in one command, and document `EXPERIENCE_COLLECTOR_OUTPUT_PATH`, `MEASUREMENT_BURST_REQUESTS`, and `RETRIEVAL_SERVICE_PORT` in the configuration table with their defaults. Apply the same correction to the `frontend/README.md` and root `README.md` export blocks.

- [ ] **Step 6: Run full verification**

```bash
cd recsys-pipeline/services/java-retrieval-service
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  PATH="$JAVA_HOME/bin:$PATH" mvn -q test

cd ../spark-streaming-job
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  PATH="$JAVA_HOME/bin:$PATH" sbt -batch test

cd ../..
python3 -m pytest -q

cd ../frontend
npm run validate:data && npm run build

cd ..
git diff --check
git status --short
```

Expected: Maven zero failures; SBT all suites pass; pytest passes except the pre-existing Kafka-dependent `test_streaming_script_uses_consolidated_spark_service_path`; frontend validation and build succeed; `git diff --check` silent.

- [ ] **Step 7: Commit**

```bash
git add frontend/data/dashboard.json README.md recsys-pipeline/README.md frontend/README.md
git commit -m "feat: publish a measured dashboard snapshot"
```

- [ ] **Step 8: Request final code review**

Invoke `superpowers:requesting-code-review` against the complete diff from the branch point. Address correctness, measurement-semantics, privacy/cardinality, and sim-robustness findings before declaring completion.
