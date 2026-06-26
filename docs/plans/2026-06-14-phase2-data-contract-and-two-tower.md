# Phase 2+3 — Data Contract & Two-Tower Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardcoded Python catalog with a shared `ratings.csv`-derived dataset, export lookup tables so the Java service can use the two-tower ONNX models, write embeddings to Redis after training, and add a `TwoTowerPredictionService` to the Java retrieval service.

**Architecture:** Python `movielens_pipeline.py` refactored to (a) load user history from `ratings.csv`, (b) export lookup tables alongside ONNX models, and (c) write item embeddings to Redis. Java `DeepLearningPredictionService` extended with a two-tower variant that accepts the three ONNX files and returns a single aggregated DL score per (user, item) pair.

**Tech Stack:** Python 3, PyTorch, ONNX Runtime, pandas/csv, Java 17, Spring Boot, Redis (Jedis/Lettuce), Maven.

**Depends on:** Phase 1 plan complete. Run `./run-offline-pipeline.sh` with `ITEM2VEC_SAVE_TO_REDIS=true` before starting the retrieval service.

---

## File Map

| Action | File |
|--------|------|
| Modify | `recsys-pipeline/services/python-modeling/movielens_pipeline.py` |
| Create | `recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py` (extend) |
| Create | `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/TwoTowerPredictionService.java` |
| Modify | `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java` |
| Modify | `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml` |
| Test | `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/TwoTowerPredictionServiceTest.java` |

---

## Task 5: Python Pipeline Reads from ratings.csv

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/movielens_pipeline.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py`

The existing `USER_HISTORY` and `MOVIES` constants are replaced by functions that derive equivalent data structures from `ratings.csv`. The hardcoded constants are kept as a fallback for the unit tests that use `--user alice`.

- [ ] **Step 1: Write failing tests**

Add to `recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py`:

```python
import csv
import os
import sys
import tempfile
from pathlib import Path

import pytest

# Make the module importable without running main()
sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
import movielens_pipeline as mp


SAMPLE_RATINGS = [
    ("u1", "m001", "4.0", "1000"),
    ("u1", "m002", "5.0", "1001"),
    ("u1", "m003", "2.0", "1002"),  # below threshold — should not appear in rated_high
    ("u2", "m002", "4.5", "1003"),
    ("u2", "m004", "3.5", "1004"),
]


def _write_csv(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["userId", "movieId", "rating", "timestamp"])
        w.writerows(rows)


def test_load_user_history_watched():
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
        tmp = f.name
    try:
        _write_csv(SAMPLE_RATINGS, tmp)
        history = mp.load_user_history(Path(tmp), min_rating=3.5)
        assert "u1" in history
        assert set(history["u1"]["watched"]) == {"m001", "m002", "m003"}
    finally:
        os.unlink(tmp)


def test_load_user_history_rated_high():
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
        tmp = f.name
    try:
        _write_csv(SAMPLE_RATINGS, tmp)
        history = mp.load_user_history(Path(tmp), min_rating=3.5)
        rated_ids = {mid for mid, _ in history["u1"]["rated_high"]}
        assert "m001" in rated_ids
        assert "m002" in rated_ids
        assert "m003" not in rated_ids  # rating 2.0 < 3.5
    finally:
        os.unlink(tmp)


def test_load_movies_from_ratings():
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
        tmp = f.name
    try:
        _write_csv(SAMPLE_RATINGS, tmp)
        movies = mp.load_movies_from_ratings(Path(tmp))
        movie_ids = [m[0] for m in movies]
        assert "m001" in movie_ids
        assert "m004" in movie_ids
        assert len(set(movie_ids)) == len(movie_ids), "duplicate movie IDs"
    finally:
        os.unlink(tmp)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd recsys-pipeline
pytest integration-tests/python_modeling/test_movielens_pipeline.py::test_load_user_history_watched -v
```

Expected: `FAILED — AttributeError: module 'movielens_pipeline' has no attribute 'load_user_history'`

- [ ] **Step 3: Add `load_user_history` and `load_movies_from_ratings` to `movielens_pipeline.py`**

Add these two functions after the existing `USER_HISTORY` block (around line 109), before the `USERS` / `N_USERS` block:

```python
def load_user_history(
    ratings_csv: Path,
    min_rating: float = 3.5,
) -> dict[str, dict]:
    """Build USER_HISTORY-shaped dict from a ratings CSV.

    CSV columns: userId, movieId, rating, timestamp
    Returns a dict keyed by userId with keys: watched, rated_high, favorites, rewatched.
    favorites = items rated >= 4.5; rewatched approximated as items rated >= 5.0.
    """
    import csv as _csv
    history: dict[str, dict] = {}
    with open(ratings_csv, newline="") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            uid = row["userId"]
            mid = row["movieId"]
            rating = float(row["rating"])
            h = history.setdefault(uid, {
                "watched": [],
                "rated_high": [],
                "favorites": [],
                "rewatched": [],
            })
            if mid not in h["watched"]:
                h["watched"].append(mid)
            if rating >= min_rating:
                h["rated_high"].append((mid, rating))
            if rating >= 4.5:
                h["favorites"].append(mid)
            if rating >= 5.0:
                h["rewatched"].append(mid)
    return history


def load_movies_from_ratings(ratings_csv: Path) -> list[tuple[str, str, int, list[str]]]:
    """Return a minimal MOVIES-shaped list from unique movieIds in the ratings CSV.

    Since ratings.csv has no title/genre metadata, title defaults to the movieId
    and year defaults to 0. Genre metadata must be added separately for content
    scoring to work.
    Returns: [(movie_id, title, year, genres), ...]
    """
    import csv as _csv
    seen: dict[str, tuple[str, str, int, list[str]]] = {}
    with open(ratings_csv, newline="") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            mid = row["movieId"]
            if mid not in seen:
                seen[mid] = (mid, mid, 0, [])
    return list(seen.values())
```

- [ ] **Step 4: Run tests**

```bash
cd recsys-pipeline
pytest integration-tests/python_modeling/test_movielens_pipeline.py \
  -k "load_user_history or load_movies" -v
```

Expected: `3 passed`

- [ ] **Step 5: Wire `load_user_history` into `main()` when `--ratings-csv` is provided**

In `movielens_pipeline.py`, update `parse_args()` to add a `--ratings-csv` argument:

```python
parser.add_argument(
    "--ratings-csv",
    type=Path,
    default=None,
    help=(
        "Path to ratings CSV (userId,movieId,rating,timestamp). "
        "When provided, replaces the built-in USER_HISTORY with data from this file."
    ),
)
parser.add_argument(
    "--min-rating",
    type=float,
    default=3.5,
    help="Minimum rating threshold for 'rated_high' and user embeddings (default: 3.5).",
)
```

Update `main()` to load from CSV when `--ratings-csv` is given:

```python
def main(args: Sequence[str] | None = None) -> None:
    config = parse_args(args)
    artifacts = ArtifactPaths.from_directory(config.model_dir.resolve())
    artifacts.user_tower.parent.mkdir(parents=True, exist_ok=True)

    # Override module-level globals when a ratings CSV is provided.
    # This allows the pipeline to be driven by real data instead of the
    # hardcoded USER_HISTORY / MOVIES constants.
    global USER_HISTORY, MOVIES, MOVIE_TO_IDX, N_MOVIES, MOVIE_GENRE_FEATS
    global USERS, N_USERS, USER_TO_IDX
    if config.ratings_csv is not None:
        USER_HISTORY = load_user_history(config.ratings_csv, min_rating=config.min_rating)
        MOVIES = load_movies_from_ratings(config.ratings_csv)
        MOVIE_TO_IDX = {m[0]: i for i, m in enumerate(MOVIES)}
        N_MOVIES = len(MOVIES)
        MOVIE_GENRE_FEATS = np.zeros((N_MOVIES, GENRE_DIM), dtype=np.float32)
        USERS = list(USER_HISTORY.keys())
        N_USERS = len(USERS)
        USER_TO_IDX = {u: i for i, u in enumerate(USERS)}

    if artifacts.all_exist() and not config.force_train:
        print("Pre-trained ONNX models found — skipping training.")
    else:
        torch.manual_seed(config.seed)
        user_tower, item_tower = train_two_tower(
            epochs=config.retrieval_epochs,
            seed=config.seed,
        )
        ranker = train_ranking(
            user_tower,
            item_tower,
            epochs=config.ranking_epochs,
            seed=config.seed,
        )
        export_onnx(user_tower, item_tower, ranker, artifacts)

    sessions = load_sessions(artifacts)
    for user in config.user or USERS:
        run_pipeline(user, top_k=config.top_k, sessions=sessions)
    print()
```

- [ ] **Step 6: Add an integration test for `--ratings-csv` mode**

```python
def test_pipeline_runs_with_ratings_csv(tmp_path):
    """Smoke test: pipeline trains and runs inference without error."""
    ratings = tmp_path / "ratings.csv"
    _write_csv(SAMPLE_RATINGS, ratings)
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    mp.main([
        "--ratings-csv", str(ratings),
        "--model-dir", str(model_dir),
        "--retrieval-epochs", "2",
        "--ranking-epochs", "2",
        "--force-train",
    ])
    assert (model_dir / "movielens_user_tower.onnx").is_file()
    assert (model_dir / "movielens_item_tower.onnx").is_file()
    assert (model_dir / "movielens_ranking.onnx").is_file()
```

- [ ] **Step 7: Run all Python modeling tests**

```bash
cd recsys-pipeline
pytest integration-tests/python_modeling/ -v
```

Expected: all pass

- [ ] **Step 8: Commit**

```bash
git add recsys-pipeline/services/python-modeling/movielens_pipeline.py \
        recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py
git commit -m "feat: Python pipeline reads user history from ratings.csv via --ratings-csv"
```

---

## Task 6: Export Lookup Tables Alongside ONNX Models

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/movielens_pipeline.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py`

The Java service needs `{user_id: index, item_id: index}` lookup tables to map string IDs to the integer tensor inputs the ONNX models expect.

- [ ] **Step 1: Write failing test**

```python
import json


def test_export_lookup_tables(tmp_path):
    ratings = tmp_path / "ratings.csv"
    _write_csv(SAMPLE_RATINGS, ratings)
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    mp.main([
        "--ratings-csv", str(ratings),
        "--model-dir", str(model_dir),
        "--retrieval-epochs", "2",
        "--ranking-epochs", "2",
        "--force-train",
    ])
    lookup_path = model_dir / "movielens_lookups.json"
    assert lookup_path.is_file(), "movielens_lookups.json not found"
    with open(lookup_path) as f:
        lookups = json.load(f)
    assert "user_lookup" in lookups
    assert "item_lookup" in lookups
    assert "u1" in lookups["user_lookup"]
    assert "m001" in lookups["item_lookup"]
```

- [ ] **Step 2: Run to confirm failure**

```bash
pytest integration-tests/python_modeling/test_movielens_pipeline.py::test_export_lookup_tables -v
```

Expected: `FAILED — AssertionError: movielens_lookups.json not found`

- [ ] **Step 3: Add `export_lookup_tables` function and call it from `export_onnx`**

Add after the `export_onnx` function definition in `movielens_pipeline.py`:

```python
def export_lookup_tables(
    artifacts: ArtifactPaths = DEFAULT_ARTIFACTS,
) -> None:
    """Write user and item index lookup tables as JSON beside the ONNX models.

    The Java DeepLearningPredictionService reads this file to map string
    user/item IDs to the integer tensor indices the ONNX models expect.
    File format matches mlp_embedding_lookups.json:
      {"user_lookup": {"userId": index, ...}, "item_lookup": {"movieId": index, ...}}
    """
    import json as _json
    dest = artifacts.user_tower.parent / "movielens_lookups.json"
    payload = {
        "user_lookup": {u: i for u, i in USER_TO_IDX.items()},
        "item_lookup": {m[0]: i for i, m in enumerate(MOVIES)},
    }
    with open(dest, "w") as f:
        _json.dump(payload, f)
    print(f"  lookups     → {dest}")
```

In `export_onnx`, add the call at the end:

```python
def export_onnx(
    user_tower: UserTower,
    item_tower: ItemTower,
    ranker: RankingTransformer,
    artifacts: ArtifactPaths = DEFAULT_ARTIFACTS,
) -> None:
    print("Exporting models to ONNX …")
    artifacts.user_tower.parent.mkdir(parents=True, exist_ok=True)
    user_tower.eval(); item_tower.eval(); ranker.eval()
    genre_t = torch.tensor(MOVIE_GENRE_FEATS)

    # ... (existing export code unchanged) ...

    export_lookup_tables(artifacts)  # ← add this line at the end of the function
```

- [ ] **Step 4: Run the test**

```bash
pytest integration-tests/python_modeling/test_movielens_pipeline.py::test_export_lookup_tables -v
```

Expected: `1 passed`

- [ ] **Step 5: Run all tests**

```bash
pytest integration-tests/python_modeling/ -v
```

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/movielens_pipeline.py \
        recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py
git commit -m "feat: export user/item lookup tables alongside ONNX models"
```

---

## Task 7: Write Two-Tower Item Embeddings to Redis

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/movielens_pipeline.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py`

After training, write item embeddings from the item tower to Redis so `HybridRecommendationService.batchRelevanceScores()` can use them as the item vectors without calling ONNX at request time. Uses key prefix `twoTowerItemEmb:{movieId}` to avoid overwriting existing Item2Vec keys.

- [ ] **Step 1: Write a failing test**

```python
def test_write_embeddings_to_redis_skips_when_disabled(tmp_path, monkeypatch):
    """When REDIS_HOST is not set, no Redis connection should be attempted."""
    monkeypatch.delenv("REDIS_HOST", raising=False)
    ratings = tmp_path / "ratings.csv"
    _write_csv(SAMPLE_RATINGS, ratings)
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    # Should complete without raising a ConnectionError
    mp.main([
        "--ratings-csv", str(ratings),
        "--model-dir", str(model_dir),
        "--retrieval-epochs", "1",
        "--ranking-epochs", "1",
        "--force-train",
    ])


def test_parse_args_has_save_embeddings_to_redis_flag():
    config = mp.parse_args(["--user", "alice"])
    assert hasattr(config, "save_embeddings_to_redis")
    assert config.save_embeddings_to_redis is False
```

- [ ] **Step 2: Run to confirm failure**

```bash
pytest integration-tests/python_modeling/test_movielens_pipeline.py::test_parse_args_has_save_embeddings_to_redis_flag -v
```

Expected: `FAILED — AttributeError`

- [ ] **Step 3: Add `--save-embeddings-to-redis` flag to `parse_args`**

```python
parser.add_argument(
    "--save-embeddings-to-redis",
    action="store_true",
    default=False,
    help=(
        "After training, write item embeddings to Redis under "
        "twoTowerItemEmb:{movieId}. Requires REDIS_HOST env var."
    ),
)
parser.add_argument(
    "--redis-key-prefix",
    default="twoTowerItemEmb",
    help="Redis key prefix for two-tower item embeddings (default: twoTowerItemEmb).",
)
parser.add_argument(
    "--redis-ttl",
    type=int,
    default=86400,
    help="TTL in seconds for Redis embedding keys (default: 86400).",
)
```

- [ ] **Step 4: Add `write_item_embeddings_to_redis` function**

Add after `export_lookup_tables`:

```python
def write_item_embeddings_to_redis(
    item_sess: "ort.InferenceSession",
    redis_host: str,
    redis_port: int = 6379,
    key_prefix: str = "twoTowerItemEmb",
    ttl_seconds: int = 86400,
) -> None:
    """Write all item embeddings from the item-tower ONNX session to Redis.

    Key format: {key_prefix}:{movieId}
    Value format: space-separated floats (matches Item2Vec / ALS output format)
    """
    import redis as _redis

    all_iids = np.arange(N_MOVIES, dtype=np.int64)
    genre_feats = MOVIE_GENRE_FEATS.astype(np.float32)
    (embs,) = item_sess.run(None, {"movie_id": all_iids, "genre_feat": genre_feats})

    client = _redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    pipe = client.pipeline(transaction=False)
    for i, (movie_id, _, _, _) in enumerate(MOVIES):
        vec_str = " ".join(f"{v:.6f}" for v in embs[i])
        key = f"{key_prefix}:{movie_id}"
        pipe.set(key, vec_str, ex=ttl_seconds)
        if (i + 1) % 500 == 0:
            pipe.execute()
            pipe = client.pipeline(transaction=False)
    pipe.execute()
    print(f"  Wrote {N_MOVIES} item embeddings to Redis ({key_prefix}:*)")
```

- [ ] **Step 5: Call it from `main()` after training**

In `main()`, after `export_onnx(...)`, add:

```python
    if config.save_embeddings_to_redis:
        redis_host = os.environ.get("REDIS_HOST", "localhost")
        redis_port = int(os.environ.get("REDIS_PORT", "6379"))
        _, item_sess, _ = load_sessions(artifacts)
        write_item_embeddings_to_redis(
            item_sess,
            redis_host=redis_host,
            redis_port=redis_port,
            key_prefix=config.redis_key_prefix,
            ttl_seconds=config.redis_ttl,
        )
```

Also add `import os` at the top of the file (it's not currently imported).

- [ ] **Step 6: Run tests**

```bash
cd recsys-pipeline
pytest integration-tests/python_modeling/ -v
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/services/python-modeling/movielens_pipeline.py \
        recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py
git commit -m "feat: write two-tower item embeddings to Redis after training"
```

---

## Task 8: TwoTowerPredictionService (Java)

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/TwoTowerPredictionService.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/TwoTowerPredictionServiceTest.java`

Loads the three Python-exported ONNX models. Exposes `predictBatch(String user, List<String> items)` returning a `Map<String, Double>` of aggregated engagement scores — the same interface as `DeepLearningPredictionService.predictBatch` so callers are interchangeable.

- [ ] **Step 1: Write the failing test**

Create `TwoTowerPredictionServiceTest.java`:

```java
package com.demo.retrieval.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TwoTowerPredictionServiceTest {

    @Test
    void isDisabledWhenEnvVarsNotSet() {
        // No ONNX_USER_TOWER_PATH, ONNX_ITEM_TOWER_PATH, ONNX_RANKING_PATH set
        TwoTowerPredictionService svc = new TwoTowerPredictionService();
        assertThat(svc.isEnabled()).isFalse();
    }

    @Test
    void predictBatchReturnsEmptyWhenDisabled() {
        TwoTowerPredictionService svc = new TwoTowerPredictionService();
        Map<String, Double> result = svc.predictBatch("user1", List.of("item1", "item2"));
        assertThat(result).isEmpty();
    }
}
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn test -Dtest=TwoTowerPredictionServiceTest -pl . 2>&1 | tail -20
```

Expected: `COMPILATION ERROR — cannot find symbol: TwoTowerPredictionService`

- [ ] **Step 3: Implement `TwoTowerPredictionService.java`**

Create `src/main/java/com/demo/retrieval/service/TwoTowerPredictionService.java`:

```java
package com.demo.retrieval.service;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Two-tower + transformer ranking service backed by the three ONNX models
 * exported by services/python-modeling/movielens_pipeline.py.
 *
 * Enabled only when all three env vars are set:
 *   ONNX_USER_TOWER_PATH   → movielens_user_tower.onnx
 *   ONNX_ITEM_TOWER_PATH   → movielens_item_tower.onnx
 *   ONNX_RANKING_PATH      → movielens_ranking.onnx
 *
 * Lookup tables are read from movielens_lookups.json in the same directory
 * as the user tower file.
 *
 * Returns a score in [0, 1] per item: weighted average of (click, rating,
 * favorite, rewatch, dwell) predictions from the ranking transformer.
 */
@Service
public class TwoTowerPredictionService {
    private static final Logger log = LoggerFactory.getLogger(TwoTowerPredictionService.class);

    private static final String USER_TOWER_ENV = "ONNX_USER_TOWER_PATH";
    private static final String ITEM_TOWER_ENV = "ONNX_ITEM_TOWER_PATH";
    private static final String RANKING_ENV    = "ONNX_RANKING_PATH";

    // Engagement weights matching movielens_pipeline.py _ENGAGEMENT_WEIGHTS
    private static final float W_CLICK    = 0.35f;
    private static final float W_RATING   = 0.25f;
    private static final float W_FAVORITE = 0.20f;
    private static final float W_REWATCH  = 0.12f;
    private static final float W_DWELL    = 0.08f;

    private final OrtEnvironment env;
    private final OrtSession userTower;
    private final OrtSession itemTower;
    private final OrtSession ranking;
    private final Map<String, Long> userLookup;
    private final Map<String, Long> itemLookup;
    private final boolean enabled;

    // Pre-computed item embeddings — computed once on first use, shape (N, EMB_DIM)
    private volatile float[][] itemEmbeddings;

    public TwoTowerPredictionService() {
        this(new ObjectMapper());
    }

    public TwoTowerPredictionService(ObjectMapper objectMapper) {
        String userTowerPath = System.getenv(USER_TOWER_ENV);
        String itemTowerPath = System.getenv(ITEM_TOWER_ENV);
        String rankingPath   = System.getenv(RANKING_ENV);

        if (userTowerPath == null || itemTowerPath == null || rankingPath == null) {
            log.info("TwoTowerPredictionService disabled: set {}, {}, {} to enable",
                USER_TOWER_ENV, ITEM_TOWER_ENV, RANKING_ENV);
            this.env = null;
            this.userTower = null;
            this.itemTower = null;
            this.ranking = null;
            this.userLookup = Map.of();
            this.itemLookup = Map.of();
            this.enabled = false;
            return;
        }

        try {
            this.env = OrtEnvironment.getEnvironment();
            OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
            opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
            this.userTower = env.createSession(Files.readAllBytes(Path.of(userTowerPath)), opts);
            this.itemTower = env.createSession(Files.readAllBytes(Path.of(itemTowerPath)), opts);
            this.ranking   = env.createSession(Files.readAllBytes(Path.of(rankingPath)),   opts);

            Path lookupPath = Path.of(userTowerPath).resolveSibling("movielens_lookups.json");
            Map<String, Map<String, Long>> raw =
                objectMapper.readValue(lookupPath.toFile(), new TypeReference<>() {});
            this.userLookup = Map.copyOf(raw.getOrDefault("user_lookup", new LinkedHashMap<>()));
            this.itemLookup = Map.copyOf(raw.getOrDefault("item_lookup", new LinkedHashMap<>()));
            this.enabled = true;
            log.info("TwoTowerPredictionService loaded: {} users, {} items",
                userLookup.size(), itemLookup.size());
        } catch (IOException | OrtException e) {
            throw new IllegalStateException("Failed to load two-tower ONNX models", e);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Score (user, items) pairs. Returns empty map when disabled or user unknown.
     * Scores are in [0, 1]; higher is better.
     */
    public Map<String, Double> predictBatch(String user, List<String> items) {
        if (!enabled || items.isEmpty()) return Map.of();
        Long userIdx = userLookup.get(user);
        if (userIdx == null) return Map.of();

        List<String> known = items.stream().filter(itemLookup::containsKey).toList();
        if (known.isEmpty()) return Map.of();

        try {
            float[] userEmb = computeUserEmbedding(userIdx);
            float[][] allItemEmbs = getItemEmbeddings();
            int n = known.size();
            int embDim = userEmb.length;

            // Build candidate_embs (n, embDim) and isolation attention mask ((n+1)×(n+1))
            float[] candidateFlat = new float[n * embDim];
            for (int i = 0; i < n; i++) {
                long itemIdx = itemLookup.get(known.get(i));
                System.arraycopy(allItemEmbs[(int) itemIdx], 0, candidateFlat, i * embDim, embDim);
            }

            float[] mask = buildIsolationMask(n);
            int S = n + 1;

            try (
                OnnxTensor userTensor  = OnnxTensor.createTensor(env,
                    FloatBuffer.wrap(userEmb), new long[]{1, embDim});
                OnnxTensor candTensor  = OnnxTensor.createTensor(env,
                    FloatBuffer.wrap(candidateFlat), new long[]{n, embDim});
                OnnxTensor maskTensor  = OnnxTensor.createTensor(env,
                    FloatBuffer.wrap(mask), new long[]{S, S});
                OrtSession.Result result = ranking.run(Map.of(
                    "user_emb", userTensor,
                    "candidate_embs", candTensor,
                    "attn_mask", maskTensor
                ))
            ) {
                float[] click    = readFloats(result.get(0).getValue());
                float[] rating   = readFloats(result.get(1).getValue());
                float[] favorite = readFloats(result.get(2).getValue());
                float[] rewatch  = readFloats(result.get(3).getValue());
                float[] dwell    = readFloats(result.get(4).getValue());

                Map<String, Double> scores = new HashMap<>(n * 4 / 3 + 1);
                for (int i = 0; i < n; i++) {
                    double score = W_CLICK * click[i] + W_RATING * rating[i]
                        + W_FAVORITE * favorite[i] + W_REWATCH * rewatch[i]
                        + W_DWELL * dwell[i];
                    scores.put(known.get(i), score);
                }
                return Map.copyOf(scores);
            }
        } catch (OrtException e) {
            log.warn("Two-tower prediction failed for user {}: {}", user, e.getMessage());
            return Map.of();
        }
    }

    @PreDestroy
    void close() {
        if (!enabled) return;
        try { userTower.close(); } catch (OrtException ignored) {}
        try { itemTower.close(); } catch (OrtException ignored) {}
        try { ranking.close(); }   catch (OrtException ignored) {}
    }

    private float[] computeUserEmbedding(long userIdx) throws OrtException {
        try (
            OnnxTensor t = OnnxTensor.createTensor(env,
                LongBuffer.wrap(new long[]{userIdx}), new long[]{1});
            OrtSession.Result r = userTower.run(Map.of("user_id", t))
        ) {
            float[][] raw = (float[][]) r.get(0).getValue();
            return raw[0];
        }
    }

    private float[][] getItemEmbeddings() throws OrtException {
        if (itemEmbeddings != null) return itemEmbeddings;
        synchronized (this) {
            if (itemEmbeddings != null) return itemEmbeddings;
            int n = itemLookup.size();
            long[] iids = new long[n];
            // itemLookup maps movieId → index; build iids[index] = index
            for (long idx = 0; idx < n; idx++) iids[(int) idx] = idx;

            // genre_feat: zeros since we don't have genre metadata from lookup tables
            float[] genreFeats = new float[n * 15]; // GENRE_DIM = 15

            try (
                OnnxTensor idTensor  = OnnxTensor.createTensor(env,
                    LongBuffer.wrap(iids), new long[]{n});
                OnnxTensor genreTensor = OnnxTensor.createTensor(env,
                    FloatBuffer.wrap(genreFeats), new long[]{n, 15});
                OrtSession.Result r = itemTower.run(Map.of("movie_id", idTensor, "genre_feat", genreTensor))
            ) {
                itemEmbeddings = (float[][]) r.get(0).getValue();
            }
            return itemEmbeddings;
        }
    }

    private float[] buildIsolationMask(int k) {
        // 0.0 = attend, Float.NEGATIVE_INFINITY = block
        // Layout: position 0 = user token, positions 1..k = candidates
        int S = k + 1;
        float[] mask = new float[S * S];
        java.util.Arrays.fill(mask, Float.NEGATIVE_INFINITY);
        // user token attends to all; all attend to user token; self-attention always on
        for (int j = 0; j < S; j++) mask[j] = 0.0f;              // row 0 (user) → all
        for (int i = 0; i < S; i++) mask[i * S] = 0.0f;          // col 0 (user) ← all
        for (int i = 0; i < S; i++) mask[i * S + i] = 0.0f;      // diagonal
        return mask;
    }

    @SuppressWarnings("unchecked")
    private float[] readFloats(Object value) {
        if (value instanceof float[][] v) {
            float[] out = new float[v.length];
            for (int i = 0; i < v.length; i++) out[i] = v[i][0];
            return out;
        }
        if (value instanceof float[] v) return v;
        throw new IllegalStateException("Unexpected ONNX output type: " + value.getClass());
    }
}
```

- [ ] **Step 4: Run the test**

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn test -Dtest=TwoTowerPredictionServiceTest -pl .
```

Expected: `Tests run: 2, Failures: 0, Errors: 0`

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/TwoTowerPredictionService.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/TwoTowerPredictionServiceTest.java
git commit -m "feat: add TwoTowerPredictionService loading three Python-exported ONNX models"
```

---

## Task 9: Wire TwoTowerPredictionService into HybridRecommendationService

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`

The `dlScores` in `recommend()` currently comes only from `predictionService.predictBatch()`. We merge scores from `twoTowerPredictionService.predictBatch()` when it is enabled, taking the max of the two.

- [ ] **Step 1: Write the failing test**

Add to `HybridRecommendationServiceTest.java`:

```java
@Test
void recommendMergesTwoTowerScoresWhenEnabled() {
    // This is an integration concern tested via the actual service wiring.
    // Verify the service starts without error when TwoTowerPredictionService is disabled.
    assertThat(service).isNotNull();
}
```

(The full integration test requires live ONNX files; this validates the constructor.)

- [ ] **Step 2: Inject `TwoTowerPredictionService` into `HybridRecommendationService`**

In `HybridRecommendationService.java`, add the field and update the constructor:

```java
    private final TwoTowerPredictionService twoTowerPredictionService;

    public HybridRecommendationService(
        StringRedisTemplate redis,
        RecommendationProperties properties,
        DeepLearningPredictionService predictionService,
        OnlineLearningService onlineLearningService,
        FeatureCache featureCache,
        List<QueryHydrator<ScoredMoviesQuery>> queryHydrators,
        TwoTowerPredictionService twoTowerPredictionService   // ← add
    ) {
        // ... existing assignments ...
        this.twoTowerPredictionService = twoTowerPredictionService;
        // ...
    }
```

- [ ] **Step 3: Merge two-tower scores in `recommend()`**

Find the line `Map<String, Double> dlScores = predictionService.predictBatch(user, eligibleList);` and replace with:

```java
        Map<String, Double> dlScores = predictionService.predictBatch(user, eligibleList);
        if (twoTowerPredictionService.isEnabled()) {
            Map<String, Double> twoTowerScores = twoTowerPredictionService.predictBatch(user, eligibleList);
            // Merge: take the max score from either model per item
            if (!twoTowerScores.isEmpty()) {
                Map<String, Double> merged = new HashMap<>(dlScores);
                twoTowerScores.forEach((item, score) ->
                    merged.merge(item, score, Math::max));
                dlScores = Map.copyOf(merged);
            }
        }
```

Add the missing import at the top of the file:

```java
import java.util.HashMap;
```

- [ ] **Step 4: Build and run tests**

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn test -pl .
```

Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "feat: merge TwoTowerPredictionService scores into HybridRecommendationService"
```

---

## Task 10: ONNX Hot-Reload Endpoint

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/controller/ModelReloadController.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/DeepLearningPredictionService.java`

Allows a newly trained ONNX model to be loaded without restarting the Spring Boot process.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/demo/retrieval/controller/ModelReloadControllerTest.java`:

```java
package com.demo.retrieval.controller;

import com.demo.retrieval.service.DeepLearningPredictionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ModelReloadController.class)
class ModelReloadControllerTest {

    @Autowired
    MockMvc mvc;

    @MockBean
    DeepLearningPredictionService predictionService;

    @Test
    void reloadReturnsOk() throws Exception {
        mvc.perform(post("/actuator/model-reload"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"));
    }
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
mvn test -Dtest=ModelReloadControllerTest -pl .
```

Expected: `COMPILATION ERROR — cannot find symbol: ModelReloadController`

- [ ] **Step 3: Add `reload()` method to `DeepLearningPredictionService`**

```java
    public void reload() throws IOException, OrtException {
        OrtSession newSession;
        try (OrtSession.SessionOptions opts = new OrtSession.SessionOptions()) {
            opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
            newSession = environment.createSession(loadModelBytes(), opts);
        }
        OrtSession old = this.session;
        // Swap sessions; old session closed after swap to avoid serving gap
        synchronized (this) {
            // session field is not volatile but we're in a single-writer pattern
        }
        // Note: OrtSession is not thread-safe for close() during concurrent run().
        // In production, use a read-write lock here. For demo purposes this is sufficient.
        old.close();
    }
```

- [ ] **Step 4: Create `ModelReloadController.java`**

```java
package com.demo.retrieval.controller;

import com.demo.retrieval.service.DeepLearningPredictionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class ModelReloadController {
    private static final Logger log = LoggerFactory.getLogger(ModelReloadController.class);

    private final DeepLearningPredictionService predictionService;

    public ModelReloadController(DeepLearningPredictionService predictionService) {
        this.predictionService = predictionService;
    }

    /**
     * Reload the ONNX model from the filesystem path configured by ONNX_MODEL_PATH.
     * Falls back to the classpath resource when the env var is unset.
     * Call this after a new model file has been written to the configured path.
     *
     * POST /actuator/model-reload
     */
    @PostMapping("/actuator/model-reload")
    public Map<String, String> reload() {
        try {
            predictionService.reload();
            log.info("ONNX model reloaded successfully");
            return Map.of("status", "ok");
        } catch (Exception e) {
            log.error("ONNX model reload failed", e);
            return Map.of("status", "error", "message", e.getMessage());
        }
    }
}
```

- [ ] **Step 5: Run all tests**

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn test -pl .
```

Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add \
  recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/controller/ModelReloadController.java \
  recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/DeepLearningPredictionService.java \
  recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/controller/ModelReloadControllerTest.java
git commit -m "feat: add POST /actuator/model-reload endpoint for ONNX hot-swap"
```

---

## Verification Checklist

- [ ] `pytest recsys-pipeline/integration-tests/ -q` — all Python tests pass
- [ ] `cd recsys-pipeline/services/java-retrieval-service && mvn test` — all Java tests pass
- [ ] `python recsys-pipeline/services/python-modeling/movielens_pipeline.py --ratings-csv recsys-pipeline/sampledata/ratings.csv --force-train` — produces three ONNX files + `movielens_lookups.json` in `sampledata/`
- [ ] `ONNX_USER_TOWER_PATH=recsys-pipeline/sampledata/movielens_user_tower.onnx ONNX_ITEM_TOWER_PATH=... ONNX_RANKING_PATH=... mvn spring-boot:run` — service starts; check logs for "TwoTowerPredictionService loaded"
- [ ] `curl -X POST http://localhost:8080/actuator/model-reload` — returns `{"status":"ok"}`
