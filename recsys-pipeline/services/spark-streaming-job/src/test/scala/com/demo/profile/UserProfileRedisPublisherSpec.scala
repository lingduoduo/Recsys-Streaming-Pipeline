package com.demo.profile

import com.demo.SparkTestSupport
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

private[profile] case class RedisProfileRow(user_id: String, run_id: String, profile_json: String)

private[profile] object RecordingRedisProfileStore extends RedisProfileStore {
  private val recordedEvents = new ConcurrentLinkedQueue[String]()
  @volatile var activeRun: Option[String] = None
  @volatile var failWrites: Boolean = false

  def reset(active: Option[String] = None, fail: Boolean = false): Unit = synchronized {
    recordedEvents.clear()
    activeRun = active
    failWrites = fail
  }

  def events: Seq[String] = recordedEvents.iterator().asScala.toSeq

  override def writeProfiles(runId: String, values: Iterator[(String, String)], ttlSeconds: Int): Unit = {
    if (failWrites) throw new RuntimeException("injected Redis write failure")
    values.foreach { case (key, value) =>
      recordedEvents.add(s"write:$runId:$key:$value:$ttlSeconds")
    }
  }

  override def activate(runId: String): Unit = {
    activeRun = Some(runId)
    recordedEvents.add(s"activate:$runId")
  }
}

class UserProfileRedisPublisherSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val redis = RedisProfileConfig(host = "redis.example", port = 6380, ttlSeconds = 321,
    keyPrefix = "user-profile:v1")

  private def rows(values: RedisProfileRow*): DataFrame = {
    val session = spark
    import session.implicits._
    values.toDF().repartition(2)
  }

  "publish" should "write run-scoped profile keys with the configured TTL before activation" in {
    RecordingRedisProfileStore.reset()

    UserProfileRedisPublisher.publish(rows(
      RedisProfileRow("user-a", "run-42", "{\"user_id\":\"user-a\"}"),
      RedisProfileRow("user-b", "run-42", "{\"user_id\":\"user-b\"}")
    ), redis, RecordingRedisProfileStore)

    val events = RecordingRedisProfileStore.events
    events should contain ("write:run-42:user-profile:v1:run-42:user-a:{\"user_id\":\"user-a\"}:321")
    events should contain ("write:run-42:user-profile:v1:run-42:user-b:{\"user_id\":\"user-b\"}:321")
    events.last shouldBe "activate:run-42"
    RecordingRedisProfileStore.activeRun shouldBe Some("run-42")
  }

  it should "leave the previous active run unchanged when a partition write fails" in {
    RecordingRedisProfileStore.reset(active = Some("run-before"), fail = true)

    an[SparkException] should be thrownBy UserProfileRedisPublisher.publish(rows(
      RedisProfileRow("user-a", "run-failed", "{}")
    ), redis, RecordingRedisProfileStore)

    RecordingRedisProfileStore.activeRun shouldBe Some("run-before")
    RecordingRedisProfileStore.events should not contain "activate:run-failed"
  }
}
