package com.demo.engine

import org.apache.spark.sql.DataFrame

trait Stage {
  def apply(df: DataFrame): DataFrame
}

trait BatchStage {
  def apply(df: DataFrame, batchId: Long): DataFrame
}
