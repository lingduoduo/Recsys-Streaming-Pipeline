package com.demo.engine

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileContext, Options, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, lit, struct, to_json}

import scala.util.control.NonFatal

trait Sink {
  def write(batch: DataFrame, batchId: Long): Unit
}

/** Stable identity supplied to sinks on the migrated Avro path. */
final case class SinkWriteContext(
    queryIdentity: String,
    queryNamespace: String,
    sinkIdentity: String,
    sinkNamespace: String,
    batchId: Long
)

/** A sink that explicitly implements retry-safe effects for one stable query/sink/batch key.
  * Plain `Sink`s remain supported by the legacy engine overload but are rejected by the Avro path.
  */
trait DurableSink extends Sink {
  def sinkIdentity: String
  def writeDurably(batch: DataFrame, context: SinkWriteContext): Unit
}

/** Writes each row as key=keyCol, value=JSON of ALL columns, to a Kafka topic. */
class KafkaSink(bootstrapServers: String, topic: String, keyCol: String) extends DurableSink {
  require(Option(topic).exists(_.trim.nonEmpty), "Kafka topic must not be blank")
  require(Option(keyCol).exists(_.trim.nonEmpty), "Kafka idempotency key column must not be blank")

  override val sinkIdentity: String = s"kafka:$topic"

  def payload(df: DataFrame): DataFrame =
    df.select(
      col(keyCol).as("key"),
      to_json(struct(df.columns.map(col): _*)).as("value")
    )

  /** Stable keyed JSON plus retry metadata. Kafka delivery remains at-least-once; consumers of
    * this derived topic must deduplicate the stable key if a batch write fails partway through.
    */
  def durablePayload(df: DataFrame, context: SinkWriteContext): DataFrame =
    payload(df).withColumn(
      "headers",
      array(
        struct(
          lit("recsys_query_namespace").as("key"),
          lit(context.queryNamespace.getBytes(StandardCharsets.UTF_8)).as("value")),
        struct(
          lit("recsys_sink_namespace").as("key"),
          lit(context.sinkNamespace.getBytes(StandardCharsets.UTF_8)).as("value")),
        struct(
          lit("recsys_batch_id").as("key"),
          lit(context.batchId.toString.getBytes(StandardCharsets.UTF_8)).as("value"))
      ))

  def write(batch: DataFrame, batchId: Long): Unit =
    payload(batch).write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .save()

  override def writeDurably(batch: DataFrame, context: SinkWriteContext): Unit =
    durablePayload(batch, context).write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("kafka.enable.idempotence", "true")
      .option("topic", topic)
      .save()
}

/** Applies a pre-write transform, then appends partitioned Parquet, bounding file count. */
class ParquetSink(path: String, partitionCol: String, outputFiles: Int,
                  transform: DataFrame => DataFrame) extends DurableSink {
  require(Option(path).exists(_.trim.nonEmpty), "Parquet path must not be blank")
  require(Option(partitionCol).exists(_.trim.nonEmpty), "Parquet partition column must not be blank")

  override val sinkIdentity: String = s"parquet:$path"

  def write(batch: DataFrame, batchId: Long): Unit =
    transform(batch)
      .coalesce(math.max(1, outputFiles))
      .write
      .mode("append")
      .partitionBy(partitionCol)
      .format("parquet")
      .save(path)

  def committedBatchPath(context: SinkWriteContext): String =
    DurableParquetCommit.finalPath(path, context).toString

  override def writeDurably(batch: DataFrame, context: SinkWriteContext): Unit =
    DurableParquetCommit.write(
      transform(batch).coalesce(math.max(1, outputFiles)),
      path,
      Seq(partitionCol),
      context)
}

/** Deterministic Parquet batch commit used only by the migrated Avro path. */
object DurableParquetCommit {

  def finalPath(root: String, context: SinkWriteContext): Path =
    new Path(
      new Path(
        new Path(root),
        s"_queries/${context.queryNamespace}/_sinks/${context.sinkNamespace}/_batches"),
      context.batchId.toString)

  private def manifest(context: SinkWriteContext): String =
    s"version=1\nquery=${context.queryNamespace}\nkind=business-parquet\n" +
      s"sink=${context.sinkNamespace}\nbatch_id=${context.batchId}\n"

  def write(
      batch: DataFrame,
      root: String,
      partitionCols: Seq[String],
      context: SinkWriteContext
  ): Unit = {
    val destination = finalPath(root, context)
    val configuration = batch.sparkSession.sparkContext.hadoopConfiguration
    val fileSystem = destination.getFileSystem(configuration)
    val expectedManifest = manifest(context)
    if (fileSystem.exists(destination)) {
      validate(fileSystem, destination, expectedManifest)
      return
    }

    val attempts = new Path(destination.getParent, s"_attempts/${context.batchId}")
    val attempt = new Path(attempts, UUID.randomUUID().toString)
    try {
      batch.write.mode("errorifexists").partitionBy(partitionCols: _*).parquet(attempt.toString)
      writeManifest(fileSystem, attempt, expectedManifest)
      fileSystem.mkdirs(destination.getParent)
      val contextFs = FileContext.getFileContext(fileSystem.getUri, configuration)
      try {
        contextFs.rename(
          fileSystem.makeQualified(attempt),
          fileSystem.makeQualified(destination),
          Options.Rename.NONE)
      } catch {
        case _: FileAlreadyExistsException => fileSystem.delete(attempt, true)
      }
      if (!fileSystem.exists(destination))
        throw new IllegalStateException(s"failed to commit Parquet sink batch $destination")
      validate(fileSystem, destination, expectedManifest)
    } catch {
      case NonFatal(error) =>
        if (fileSystem.exists(attempt)) fileSystem.delete(attempt, true)
        throw error
    }
  }

  private def writeManifest(
      fileSystem: org.apache.hadoop.fs.FileSystem,
      directory: Path,
      contents: String
  ): Unit = {
    val output = fileSystem.create(new Path(directory, "_COMMITTED"), false)
    try output.write(contents.getBytes(StandardCharsets.UTF_8))
    finally output.close()
  }

  private def validate(
      fileSystem: org.apache.hadoop.fs.FileSystem,
      directory: Path,
      expectedManifest: String
  ): Unit = {
    val committed = new Path(directory, "_COMMITTED")
    val success = new Path(directory, "_SUCCESS")
    if (!fileSystem.exists(committed) || !fileSystem.exists(success))
      throw new IllegalStateException(s"uncommitted or incomplete Parquet sink batch $directory")
    val input = fileSystem.open(committed)
    val source = scala.io.Source.fromInputStream(input, StandardCharsets.UTF_8.name())
    val actual = try source.mkString finally source.close()
    if (actual != expectedManifest)
      throw new IllegalStateException(s"Parquet sink commit identity mismatch for $directory")
  }
}
