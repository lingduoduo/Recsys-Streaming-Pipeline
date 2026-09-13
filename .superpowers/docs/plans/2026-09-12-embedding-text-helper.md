# Embedding Text Helper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One implementation of the `id:v1 v2 ...` embedding contract, used by ALS, the user job, and candidate generation, with the previously untested user job covered by a spec first.

**Architecture:** `com.demo.util.EmbeddingText` renders, reads, and writes the contract (text and Redis). Jobs keep their training code and drop their copies of the plumbing. Item2Vec is untouched.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17.

**Spec:** [One embedding text contract](../specs/2026-09-12-embedding-text-helper-design.md)

## Global Constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, JDK 17 for sbt; add no dependencies.
- Keep every output format, path default, Redis key prefix, TTL default, environment variable, positional argument, and script entry point unchanged.
- Keep `RedisWriter` as the low-level writer; the helper composes it.
- Keep `trainAlsEmbeddings`, `trainItem2vec`, `topKCandidates`, and both `trainUserEmbeddings` overloads callable as today, except that the user job's training output no longer carries `userEmbeddingStr`.
- Malformed lines are skipped, never fail the job.

## Execution notes

All paths are relative to `recsys-pipeline/services/spark-streaming-job/src/` unless they start
with `.superpowers`. Run sbt from `recsys-pipeline/services/spark-streaming-job/` with
`JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home`.
sbt holds a project lock; never overlap two runs. The work is on
`simplify/embedding-text-helper`, based on `origin/master`. Publish a PR against `master`.

Baseline (2026-09-12): `testOnly com.demo.task.AlsEmbeddingTrainingJobSpec com.demo.task.Item2VecTrainingJobSpec com.demo.process.ItemSequencePreprocessingJobSpec` → 10 tests, 10 succeeded.

---

### Task 1: Characterize the user job

**Files:**
- Create: `test/scala/com/demo/task/UserEmbeddingTrainingJobSpec.scala`

**Interfaces:**
- Consumes: `UserEmbeddingTrainingJob.trainUserEmbeddings(ratings: DataFrame, itemEmbeddings: DataFrame, minRating: Double): DataFrame` with `userId` and `userEmbedding` columns (existing).

- [x] **Step 1: Write the spec**

```scala
package com.demo.task

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserEmbeddingTrainingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def embeddings() = {
    val s = spark; import s.implicits._
    val ratings = Seq(
      ("u1", "m1", 5.0), ("u1", "m2", 4.0), ("u1", "m3", 1.0),   // m3 falls below the threshold
      ("u2", "m3", 2.0),                                         // nothing qualifies
      ("u3", "m9", 5.0)                                          // qualifies, but m9 has no vector
    ).toDF("userId", "movieId", "rating")
    val items = Seq(("m1", Seq(1.0, 0.0)), ("m2", Seq(0.0, 1.0)), ("m3", Seq(1.0, 1.0)))
      .toDF("movieId", "vector")
    UserEmbeddingTrainingJob.trainUserEmbeddings(ratings, items, minRating = 3.5)
      .collect()
      .map(row => row.getAs[String]("userId") -> row.getAs[Seq[Double]]("userEmbedding"))
      .toMap
  }

  "trainUserEmbeddings" should "average the item vectors of a user's qualifying ratings" in {
    embeddings()("u1") shouldBe Seq(0.5, 0.5)
  }

  it should "omit users with no qualifying rating and users whose items carry no vector" in {
    embeddings().keySet shouldBe Set("u1")
  }
}
```

- [x] **Step 2: Run it against the current code**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=... sbt -batch "testOnly com.demo.task.UserEmbeddingTrainingJobSpec"`
Expected: 2 tests, 2 succeeded. This is a characterization test; it must pass before any change. Observed: 2 succeeded.

- [x] **Step 3: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/UserEmbeddingTrainingJobSpec.scala
git commit -m "test: characterize UserEmbeddingTrainingJob's mean and threshold"
```

---

### Task 2: The helper and its spec

**Files:**
- Create: `main/scala/com/demo/util/EmbeddingText.scala`
- Create: `test/scala/com/demo/util/EmbeddingTextSpec.scala`

**Interfaces:**
- Produces: `EmbeddingText.vectorString(vector: Column): Column`; `EmbeddingText.read(spark: SparkSession, path: String): DataFrame` (`id: String`, `vector: Seq[Double]`); `EmbeddingText.writeText(df: DataFrame, idCol: String, vectorCol: String, path: String): Unit`; `EmbeddingText.writeRedis(df: DataFrame, idCol: String, vectorCol: String, redisHost: String, redisPort: Int, keyPrefix: String, ttlSeconds: Int): Unit`.

- [x] **Step 1: Write the failing spec**

```scala
package com.demo.util

import java.nio.file.Files

import com.demo.SparkTestSupport
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmbeddingTextSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def tmpDir(name: String): String = {
    val dir = Files.createTempDirectory(name)
    dir.toFile.deleteOnExit()
    dir.resolve("out").toString
  }

  "writeText then read" should "round-trip ids and vectors" in {
    val s = spark; import s.implicits._
    val path = tmpDir("emb-roundtrip")
    val df = Seq(("a", Seq(1.0, -0.5)), ("b", Seq(0.25, 2.0))).toDF("id", "vector")

    EmbeddingText.writeText(df, "id", "vector", path)
    val back = EmbeddingText.read(spark, path).collect()
      .map(row => row.getAs[String]("id") -> row.getAs[Seq[Double]]("vector")).toMap

    back shouldBe Map("a" -> Seq(1.0, -0.5), "b" -> Seq(0.25, 2.0))
  }

  "read" should "skip lines without a colon, with an empty id, or with a non-numeric token" in {
    val path = tmpDir("emb-malformed")
    val file = new java.io.File(path)
    Files.write(file.toPath, java.util.Arrays.asList(
      "ok:1.0 2.0", "nocolon 1.0", ":1.0", "bad:1.0 x", "also:3"))
    val ids = EmbeddingText.read(spark, path).collect().map(_.getAs[String]("id")).toSet
    ids shouldBe Set("ok", "also")
  }

  "vectorString" should "render a float array as space-separated values" in {
    val s = spark; import s.implicits._
    val rendered = Seq(Seq(1.5f, -2.0f)).toDF("v")
      .select(EmbeddingText.vectorString(col("v")).as("s")).collect().head.getString(0)
    rendered shouldBe "1.5 -2.0"
  }
}
```

- [x] **Step 2: Run to verify it fails to compile**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=... sbt -batch "testOnly com.demo.util.EmbeddingTextSpec"`
Expected: compilation error, `object EmbeddingText is not a member of package com.demo.util`. Observed: `not found: value EmbeddingText`.

- [x] **Step 3: Write the helper**

```scala
package com.demo.util

import com.demo.sink.RedisWriter
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array_join, col, concat_ws, transform}

import scala.util.Try

/** The `id:v1 v2 ...` text contract every embedding job writes and every consumer reads.
  *
  * One line per id; the vector is space-separated numbers. Redis carries the same string under
  * `prefix:id`, which is what the retrieval service's parseVector reads.
  */
object EmbeddingText {

  /** A numeric array column rendered as the space-joined string the contract carries. */
  def vectorString(vector: Column): Column =
    array_join(transform(vector, (x: Column) => x.cast("string")), " ")

  /** (id, vector) rows from a file or a directory of part files; malformed lines are skipped. */
  def read(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    spark.read.textFile(path).flatMap { line =>
      val sep = line.indexOf(':')
      if (sep <= 0) None
      else Try((line.substring(0, sep).trim,
                line.substring(sep + 1).trim.split("\\s+").map(_.toDouble).toSeq)).toOption
    }.toDF("id", "vector")
  }

  def writeText(df: DataFrame, idCol: String, vectorCol: String, path: String): Unit =
    df.select(concat_ws(":", col(idCol), vectorString(col(vectorCol))).as("value"))
      .write.mode("overwrite").text(path)

  def writeRedis(df: DataFrame, idCol: String, vectorCol: String,
                 redisHost: String, redisPort: Int, keyPrefix: String, ttlSeconds: Int): Unit =
    df.select(col(idCol).as("id"), vectorString(col(vectorCol)).as("value"))
      .foreachPartition { rows: Iterator[Row] =>
        RedisWriter.writeWithPipeline(
          redisHost, redisPort,
          rows.map(r => r.getAs[String]("id") -> r.getAs[String]("value")),
          keyPrefix, ttlSeconds)
      }
}
```

- [x] **Step 4: Run the spec**

Run the same sbt command as Step 2.
Expected: 3 tests, 3 succeeded. Observed: 3 succeeded.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/EmbeddingText.scala recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/EmbeddingTextSpec.scala
git commit -m "feat(util): one implementation of the id:vector embedding text contract"
```

---

### Task 3: Move the three jobs onto the helper

**Files:**
- Modify: `main/scala/com/demo/task/AlsEmbeddingTrainingJob.scala`
- Modify: `test/scala/com/demo/task/AlsEmbeddingTrainingJobSpec.scala`
- Modify: `main/scala/com/demo/task/UserEmbeddingTrainingJob.scala`
- Modify: `main/scala/com/demo/recommend/EmbeddingCandidateGenerationJob.scala`

**Interfaces:**
- Consumes: `EmbeddingText` from Task 2.

- [x] **Step 1: ALS**

In `AlsEmbeddingTrainingJob.scala`:

- add `EmbeddingText` to the `com.demo.util` import: `import com.demo.util.{EmbeddingText, Env, RatingsCsv, SparkSessions}`;
- delete `import com.demo.sink.RedisWriter`, `import org.apache.spark.sql.{DataFrame, Row, SparkSession}` becomes `import org.apache.spark.sql.{DataFrame, SparkSession}`;
- delete `private val vectorToString = ...`;
- in `main`, replace the two `writeFactors(...)` calls with

```scala
      EmbeddingText.writeText(userFactors, "userId", "userEmbedding", s"$outputPath/userFactors")
      EmbeddingText.writeText(itemFactors, "movieId", "itemEmbedding", s"$outputPath/itemFactors")
```

and the two `writeFactorsToRedis(...)` calls with

```scala
        EmbeddingText.writeRedis(userFactors, "userId", "userEmbedding", redisHost, redisPort,
          sys.env.getOrElse("ALS_USER_REDIS_KEY_PREFIX", "alsUserEmb"), redisTtlSeconds)
        EmbeddingText.writeRedis(itemFactors, "movieId", "itemEmbedding", redisHost, redisPort,
          sys.env.getOrElse("ALS_ITEM_REDIS_KEY_PREFIX", "alsItemEmb"), redisTtlSeconds)
```

- delete the `writeFactors` and `writeFactorsToRedis` methods entirely.

In `AlsEmbeddingTrainingJobSpec.scala`, delete the test `"write factors in id:f1 f2 f3 format readable by downstream consumers"` (the round trip now lives in `EmbeddingTextSpec`) and the imports it alone used (`java.nio.file.Files`, `scala.io.Source`).

- [x] **Step 2: User job**

In `UserEmbeddingTrainingJob.scala`:

- imports become `import com.demo.util.{EmbeddingText, Env, RatingsCsv, SparkSessions}` and `import org.apache.spark.sql.{Column, DataFrame, SparkSession}`; delete `import com.demo.sink.RedisWriter` and `import scala.util.Try`;
- delete `case class ItemEmbedding(...)`;
- in `main`, replace the text write and the Redis block with

```scala
      EmbeddingText.writeText(userEmbeddings, "userId", "userEmbedding", userEmbeddingPath)

      if (saveToRedis) {
        EmbeddingText.writeRedis(
          userEmbeddings, "userId", "userEmbedding",
          sys.env.getOrElse("REDIS_HOST", "localhost"), Env.int("REDIS_PORT", 6379),
          sys.env.getOrElse("USER_EMBEDDING_REDIS_KEY_PREFIX", "uEmb"),
          Env.int("USER_EMBEDDING_REDIS_TTL_SECONDS", DefaultRedisTtlSeconds))
        userEmbeddings.unpersist()
      }
```

keeping the `val userEmbeddings = if (saveToRedis) raw.cache() else raw` line and its comment;
- in the path overload, replace `val itemEmbeddings = readItemEmbeddings(sparkSession, itemEmbeddingPath)` with `val itemEmbeddings = EmbeddingText.read(sparkSession, itemEmbeddingPath).withColumnRenamed("id", "movieId")`;
- in the DataFrame overload, delete the `.withColumn("userEmbeddingStr", ...)` line and change the final select to `.select("userId", "userEmbedding")`;
- delete `readItemEmbeddings`.

- [x] **Step 3: Candidate generation**

In `EmbeddingCandidateGenerationJob.scala`, add `EmbeddingText` to the `com.demo.util` import, replace both `readEmbeddings(spark, ...)` calls in `main` with `EmbeddingText.read(spark, ...)`, delete the `readEmbeddings` method and its doc comment, and delete `import scala.util.Try` if nothing else uses it.

- [x] **Step 4: Verify**

Run: `grep -rn "def readEmbeddings\|def readItemEmbeddings\|def writeFactors\|vectorToString\|case class ItemEmbedding\|userEmbeddingStr" recsys-pipeline/services/spark-streaming-job/src/main/scala`
Expected: no output.

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=... sbt -batch "testOnly com.demo.task.* com.demo.recommend.* com.demo.process.ItemSequencePreprocessingJobSpec com.demo.util.EmbeddingTextSpec"`
Expected: all succeed; the counts include `UserEmbeddingTrainingJobSpec` 2, `EmbeddingTextSpec` 3, `AlsEmbeddingTrainingJobSpec` 2, `Item2VecTrainingJobSpec` 3. Observed: 51 tests, 51 succeeded across the selection.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/AlsEmbeddingTrainingJob.scala recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/AlsEmbeddingTrainingJobSpec.scala recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEmbeddingTrainingJob.scala recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/recommend/EmbeddingCandidateGenerationJob.scala
git commit -m "refactor: ALS, user, and candidate jobs share the embedding text contract"
```

---

### Task 4: Full suite, records, and PR

**Files:**
- Modify: `.superpowers/docs/specs/2026-09-12-embedding-text-helper-design.md` (status and verification record)
- Modify: this plan (check boxes, record observations)

- [x] **Step 1: Run the full Spark module suite in the background**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=... sbt -batch test`
Expected: all pass (424 on 2026-09-12 plus the 5 new tests, minus the 1 moved). If unrelated tests fail, confirm they fail on `origin/master` too before reporting. Observed: 428 succeeded in 4 min 16 s.

- [x] **Step 2: Update the spec status and verification record, tick this plan, and commit**

```bash
git add .superpowers/docs/specs/2026-09-12-embedding-text-helper-design.md .superpowers/docs/plans/2026-09-12-embedding-text-helper.md
git commit -m "docs: record embedding text helper verification"
```

- [x] **Step 3: Open the PR**

```bash
git push -u origin simplify/embedding-text-helper
gh pr create --base master --title "refactor: one implementation of the id:vector embedding contract" --body "$(cat <<'EOF'
## Summary
- The `id:v1 v2 ...` embedding contract was written three ways (ALS, user job, Item2Vec), parsed two ways (user job, candidate generation), and rendered three ways. `com.demo.util.EmbeddingText` now renders, reads, and writes it (text and Redis); ALS, the user job, and candidate generation use it.
- `UserEmbeddingTrainingJob` had no spec; one is added first as a characterization of the mean and threshold, then guards the refactor.
- Item2Vec is untouched: its single-file driver write is what the Python next-item reader opens, so it keeps that path and its direct Redis call.
- No change to output formats, path defaults, Redis prefixes, TTL defaults, environment variables, positional arguments, or scripts.

## Test plan
- [x] New `UserEmbeddingTrainingJobSpec` (2) passes on the old code and the new; new `EmbeddingTextSpec` (3) covers the round trip, malformed lines, and float rendering.
- [x] ALS (2), Item2Vec (3), candidate generation, and sequence preprocessing specs pass; ALS's format test moved into the helper's round trip.
- [x] Full Spark module suite under JDK 17: <fill from Step 1>.

Spec: `.superpowers/docs/specs/2026-09-12-embedding-text-helper-design.md`
Plan: `.superpowers/docs/plans/2026-09-12-embedding-text-helper.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Then record the PR link in the spec's status line and commit it with `docs: link embedding text helper PR`.

---

## Self-review

- Spec coverage: user-job characterization (Task 1), helper and spec (Task 2), the three job moves and the ALS spec change (Task 3), suite and PR (Task 4). Acceptance criteria 1-5 map to Task 1 Step 2 plus Task 3 Step 4, Task 2 Step 4, Task 3 Step 4, Task 3 Step 4, Task 4 Step 1.
- Placeholders: one `<fill from Step 1>` in the PR body supplied by Task 4 Step 1; `JAVA_HOME=...` abbreviates the path in the execution notes.
- Names: `vectorString`, `read`, `writeText`, `writeRedis`, `EmbeddingText` are consistent between spec and tasks; column names `id`/`vector` match the candidate-generation spec's fixtures.
