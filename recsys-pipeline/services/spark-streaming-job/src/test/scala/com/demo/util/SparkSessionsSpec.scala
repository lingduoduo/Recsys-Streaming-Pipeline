package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkSessionsSpec extends AnyFlatSpec with Matchers {

  "SparkSessions.adaptiveConfigs" should "enable AQE and partition coalescing" in {
    SparkSessions.adaptiveConfigs("spark.sql.adaptive.enabled") shouldBe "true"
    SparkSessions.adaptiveConfigs("spark.sql.adaptive.coalescePartitions.enabled") shouldBe "true"
  }

  // Asserts the constant, not the wiring: `create` uses `getOrCreate`, which returns whatever
  // session another suite already built in this JVM. Correctness of the date partitions is pinned
  // by TimePartitionsSpec, which exercises the expression directly.
  "SparkSessions.defaultTimeZone" should "be UTC so formatting does not vary by deploy host" in {
    SparkSessions.defaultTimeZone shouldBe "UTC"
  }

  "shufflePartitionsFor" should "keep the local default for every local master form" in {
    // local[*] is the default master and the only thing that executes today. 200 partitions on a
    // laptop is hundreds of tiny tasks and more scheduling overhead than work.
    SparkSessions.shufflePartitionsFor("local", 8) shouldBe 8
    SparkSessions.shufflePartitionsFor("local[1]", 8) shouldBe 8
    SparkSessions.shufflePartitionsFor("local[*]", 8) shouldBe 8
    SparkSessions.shufflePartitionsFor("local[4]", 4) shouldBe 4
  }

  it should "raise the default on every cluster master" in {
    SparkSessions.shufflePartitionsFor("yarn", 8) shouldBe SparkSessions.ClusterShufflePartitions
    SparkSessions.shufflePartitionsFor("k8s://https://host:6443", 8) shouldBe SparkSessions.ClusterShufflePartitions
    SparkSessions.shufflePartitionsFor("spark://host:7077", 8) shouldBe SparkSessions.ClusterShufflePartitions
    SparkSessions.shufflePartitionsFor("mesos://host:5050", 8) shouldBe SparkSessions.ClusterShufflePartitions
  }

  it should "treat an unrecognised master as a cluster" in {
    // Guessing "local" for something unknown funnels a real cluster's shuffles through 8
    // partitions, which is the failure this exists to prevent. Guessing "cluster" only costs a
    // laptop some scheduling overhead.
    SparkSessions.shufflePartitionsFor("something-new", 8) shouldBe SparkSessions.ClusterShufflePartitions
  }

  it should "not mistake a master merely containing the word local" in {
    SparkSessions.shufflePartitionsFor("spark://localhost:7077", 8) shouldBe SparkSessions.ClusterShufflePartitions
  }

  // eventLogSettings is the enabled/directory decision that `create` wires into the session
  // builder. It is tested here directly, rather than through a session built by `create`, because
  // `create` calls `getOrCreate`: any test that asserts on a *returned* session's config would pass
  // or fail depending on whichever suite in this JVM happened to build the session first -- see the
  // comment on `SparkSessions.defaultTimeZone` above. This is also where SPARK_EVENT_LOG_ENABLED's
  // case- and whitespace-sensitivity is pinned down: the only moment anyone sets that variable is
  // mid-incident, which is the worst possible time for it to silently no-op.
  "eventLogSettings" should "enable logging for a lowercase true" in {
    SparkSessions.eventLogSettings(Map("SPARK_EVENT_LOG_ENABLED" -> "true")) shouldBe
      Some("/tmp/spark-events")
  }

  it should "enable logging for an uppercase TRUE" in {
    SparkSessions.eventLogSettings(Map("SPARK_EVENT_LOG_ENABLED" -> "TRUE")) shouldBe
      Some("/tmp/spark-events")
  }

  it should "enable logging for true with surrounding whitespace" in {
    SparkSessions.eventLogSettings(Map("SPARK_EVENT_LOG_ENABLED" -> " true ")) shouldBe
      Some("/tmp/spark-events")
  }

  it should "stay off for false" in {
    SparkSessions.eventLogSettings(Map("SPARK_EVENT_LOG_ENABLED" -> "false")) shouldBe None
  }

  it should "stay off when unset" in {
    SparkSessions.eventLogSettings(Map.empty) shouldBe None
  }

  it should "use SPARK_EVENT_LOG_DIR when enabled and set" in {
    SparkSessions.eventLogSettings(
      Map("SPARK_EVENT_LOG_ENABLED" -> "true", "SPARK_EVENT_LOG_DIR" -> "/var/log/spark-events")
    ) shouldBe Some("/var/log/spark-events")
  }

  "ensureEventLogDir" should "create a missing directory and return it" in {
    val dir = java.nio.file.Files.createTempDirectory("evlog-test").resolve("nested").toString
    SparkSessions.ensureEventLogDir(dir) shouldBe Some(dir)
    java.nio.file.Files.isDirectory(java.nio.file.Paths.get(dir)) shouldBe true
  }

  it should "accept a directory that already exists" in {
    val dir = java.nio.file.Files.createTempDirectory("evlog-existing").toString
    SparkSessions.ensureEventLogDir(dir) shouldBe Some(dir)
  }

  it should "return None rather than throwing when the directory cannot be created" in {
    // Spark throws at session creation if spark.eventLog.dir does not exist -- it does not create
    // it. Letting that propagate would turn an observability feature into an outage: every job
    // would fail to start. A logging feature must never be the reason a job cannot run.
    val file = java.nio.file.Files.createTempFile("evlog-not-a-dir", ".txt")
    SparkSessions.ensureEventLogDir(file.resolve("under-a-file").toString) shouldBe None
  }

  it should "pass a remote URI through untouched rather than creating a junk local directory" in {
    // A plain java.nio.file.Paths.get("hdfs://host/path") happily creates a local directory
    // literally named "hdfs:" in the working directory and still reports success -- Spark, not
    // this code, is the thing that knows how to resolve a remote event-log location.
    SparkSessions.ensureEventLogDir("hdfs://namenode/spark-events") shouldBe
      Some("hdfs://namenode/spark-events")
    java.nio.file.Files.exists(java.nio.file.Paths.get("hdfs:")) shouldBe false
  }

  it should "pass an s3a URI through untouched" in {
    SparkSessions.ensureEventLogDir("s3a://bucket/spark-events") shouldBe
      Some("s3a://bucket/spark-events")
  }
}
