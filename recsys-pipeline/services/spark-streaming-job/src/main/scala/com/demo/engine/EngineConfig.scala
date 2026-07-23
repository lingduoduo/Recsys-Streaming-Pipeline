package com.demo.engine

final case class EngineConfig(
    bootstrapServers: String,
    inputTopic: String,
    startingOffsets: String,
    groupId: String,
    maxOffsetsPerTrigger: Int,
    triggerInterval: String,
    checkpointLocation: String,
    watermarkDelay: String,
    sinkMaxRetries: Int
)

object EngineConfig {
  def validate(cfg: EngineConfig): Either[List[String], EngineConfig] = {
    val errors = List.newBuilder[String]
    def nonBlank(name: String, v: String): Unit =
      if (v == null || v.trim.isEmpty) errors += s"$name must not be blank"
    nonBlank("bootstrapServers", cfg.bootstrapServers)
    nonBlank("inputTopic", cfg.inputTopic)
    nonBlank("checkpointLocation", cfg.checkpointLocation)
    nonBlank("triggerInterval", cfg.triggerInterval)
    nonBlank("watermarkDelay", cfg.watermarkDelay)
    if (cfg.maxOffsetsPerTrigger <= 0) errors += "maxOffsetsPerTrigger must be > 0"
    if (cfg.sinkMaxRetries < 0) errors += "sinkMaxRetries must be >= 0"
    val es = errors.result()
    if (es.isEmpty) Right(cfg) else Left(es)
  }
}
