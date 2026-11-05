package com.demo.report

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.{Row, SparkSession}
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ScanParams
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

/** I/O for finite offline reports. Computation is driver-local, as in the reference reports. */
object ReportSupport {
  type Record = Map[String, Any]
  private val mapper = new ObjectMapper()

  def obj(value: Any): Record = value match {
    case m: scala.collection.Map[_, _] => m.iterator.map { case (k, v) => k.toString -> v }.toMap
    case m: java.util.Map[_, _] => m.asScala.iterator.map { case (k, v) => k.toString -> v }.toMap
    case row: Row => row.schema.fieldNames.zip(row.toSeq.map(normalize)).toMap
    case _ => Map.empty
  }

  def seq(value: Any): Seq[Any] = value match {
    case null => Seq.empty
    case xs: Seq[_] => xs
    case xs: Array[_] => xs.toSeq
    case xs: java.lang.Iterable[_] => xs.asScala.toSeq
    case _ => Seq.empty
  }

  def number(value: Any, default: Double = 0.0): Double = value match {
    case n: java.lang.Number => n.doubleValue()
    case b: Boolean => if (b) 1.0 else 0.0
    case s: String => Try(s.toDouble).getOrElse(default)
    case _ => default
  }

  def text(value: Any, default: String = ""): String = Option(value).fold(default)(_.toString)
  def round(value: Double, digits: Int = 4): Double =
    if (value.isNaN || value.isInfinity) value
    else BigDecimal.decimal(value).setScale(digits, BigDecimal.RoundingMode.HALF_EVEN).toDouble

  /** Normalize Spark and Java nested containers before handing values to pure computations. */
  def normalize(value: Any): Any = value match {
    case null => null
    case row: Row => row.schema.fieldNames.zip(row.toSeq.map(normalize)).toMap
    case m: scala.collection.Map[_, _] => m.iterator.map { case (k, v) => k.toString -> normalize(v) }.toMap
    case m: java.util.Map[_, _] => m.asScala.iterator.map { case (k, v) => k.toString -> normalize(v) }.toMap
    case xs: Seq[_] => xs.map(normalize)
    case xs: Array[_] => xs.toSeq.map(normalize)
    case xs: java.lang.Iterable[_] => xs.asScala.toSeq.map(normalize)
    case t: java.sql.Timestamp => t.toInstant.toString
    case d: java.sql.Date => d.toLocalDate.toString
    case d: java.time.temporal.TemporalAccessor => d.toString
    case d: java.math.BigDecimal => d.doubleValue()
    case d: scala.math.BigDecimal => d.toDouble
    case other => other
  }

  def records(spark: SparkSession, path: String): Seq[Record] =
    spark.read.parquet(path).collect().toSeq.map(row => obj(normalize(row)))

  private def fromNode(node: JsonNode): Any = {
    if (node == null || node.isNull) null
    else if (node.isObject) node.fields().asScala.map(e => e.getKey -> fromNode(e.getValue)).toMap
    else if (node.isArray) node.elements().asScala.map(fromNode).toVector
    else if (node.isBoolean) node.booleanValue()
    else if (node.isIntegralNumber) node.longValue()
    else if (node.isNumber) node.doubleValue()
    else node.textValue()
  }

  private def toJava(value: Any): AnyRef = normalize(value) match {
    case null => null
    case m: scala.collection.Map[_, _] =>
      m.toSeq.sortBy(_._1.toString).map { case (k, v) => k.toString -> toJava(v) }.toMap.asJava
    case xs: Seq[_] => xs.map(toJava).asJava
    case d: Double if d.isNaN || d.isInfinity => null
    case f: Float if f.isNaN || f.isInfinity => null
    case value: AnyRef => value
    case value => value.asInstanceOf[AnyRef]
  }

  def parseJson(value: String): Any = fromNode(mapper.readTree(value))
  def readJson(path: String): Any = parseJson(new String(Files.readAllBytes(Paths.get(path)), UTF_8))
  def json(value: Any): String = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toJava(value))

  private def prepare(path: String): java.nio.file.Path = {
    val file = Paths.get(path)
    Option(file.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    file
  }

  def writeJson(path: String, value: Any): Unit = {
    Files.write(prepare(path), (json(value) + "\n").getBytes(UTF_8))
  }

  def writeCsv(path: String, columns: Seq[String], rows: Seq[Record]): Unit = {
    def cell(value: Any): String = {
      val raw = value match {
        case null => ""
        case n: Double if n.isNaN || n.isInfinity => ""
        case other => other.toString
      }
      if (raw.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r'))
        "\"" + raw.replace("\"", "\"\"") + "\""
      else raw
    }
    val writer = Files.newBufferedWriter(prepare(path), UTF_8)
    try {
      writer.write(columns.map(cell).mkString(",") + "\n")
      rows.foreach(row => writer.write(columns.map(k => cell(row.getOrElse(k, null))).mkString(",") + "\n"))
    } finally writer.close()
  }

  def options(args: Array[String]): Map[String, String] = {
    var result = Map.empty[String, String]
    var index = 0
    while (index < args.length) {
      val arg = args(index)
      require(arg.startsWith("--") && arg.length > 2, s"Expected --option, got '$arg'")
      val key = arg.drop(2)
      require(!result.contains(key), s"Duplicate option --$key")
      if (key == "help") {
        result += key -> "true"
        index += 1
      } else {
        require(index + 1 < args.length && !args(index + 1).startsWith("--"), s"Missing value for --$key")
        result += key -> args(index + 1)
        index += 2
      }
    }
    result
  }

  def options(args: Array[String], allowed: Set[String]): Map[String, String] = {
    val result = options(args)
    require((result.keySet -- allowed - "help").isEmpty,
      s"Unknown options: ${(result.keySet -- allowed - "help").toSeq.sorted.map("--" + _).mkString(", ")}")
    result
  }

  private def withRedis[A](host: String, port: Int, fallback: => A)(f: Jedis => A): A = {
    var client: Jedis = null
    try {
      // Jedis can connect in its constructor, so creation belongs inside the fallback boundary.
      client = new Jedis(host, port, 3000)
      f(client)
    }
    catch {
      case NonFatal(e) =>
        System.err.println(s"[warn] Redis $host:$port report data unavailable: ${e.getMessage}")
        fallback
    } finally if (client != null) client.close()
  }

  private def keys(client: Jedis, pattern: String): Seq[String] = {
    val found = scala.collection.mutable.ArrayBuffer.empty[String]
    var cursor = "0"
    do {
      val page = client.scan(cursor, new ScanParams().`match`(pattern).count(500))
      found ++= page.getResult.asScala
      cursor = page.getCursor
    } while (cursor != "0")
    found.distinct.sorted.toVector
  }

  def catalog(host: String, port: Int): Seq[Record] = withRedis(host, port, Seq.empty[Record]) { client =>
    keys(client, "movie:*:features").flatMap { key =>
      val data = client.hgetAll(key).asScala
      if (data.isEmpty) None
      else Some(Map[String, Any](
        "item_id" -> key.stripPrefix("movie:").stripSuffix(":features"),
        "title" -> data.getOrElse("title", ""),
        "genres" -> data.getOrElse("genres", "").split(",").filter(_.nonEmpty).toSeq,
        "release_year" -> data.get("releaseYear").flatMap(s => Try(s.toInt).toOption).map(_.asInstanceOf[Any]).orNull))
    }
  }

  def popularity(host: String, port: Int): Map[String, Double] = withRedis(host, port, Map.empty[String, Double]) { client =>
    client.zrangeWithScores("global:item_popularity", 0, -1).asScala.map(t => t.getElement -> t.getScore).toMap
  }

  def embeddings(host: String, port: Int, prefix: String): Map[String, Seq[Double]] =
    withRedis(host, port, Map.empty[String, Seq[Double]]) { client =>
      keys(client, s"$prefix:*").flatMap { key =>
        Option(client.get(key)).filter(_.trim.nonEmpty).map { value =>
          key.stripPrefix(prefix + ":") -> value.trim.split("\\s+").map(_.toDouble).toSeq
        }
      }.toMap
    }

  def replay(host: String, port: Int, key: String = "replay:recommendations", limit: Int = -1): Seq[Record] =
    withRedis(host, port, Seq.empty[Record]) { client =>
      client.lrange(key, 0, limit).asScala.map(value => obj(parseJson(value))).toVector
    }
}
