#!/usr/bin/env python3
"""Query analysis report (PySpark) — most-common queries + short-vs-long engagement.

A "query" is the search **intent** of a recommended impression, represented as its movie's
genre-combo string (e.g. "Sci-Fi Action"). The events carry no free-text query; the genre-combo
both repeats (so "most common" is meaningful) and varies in length (so short vs long is meaningful).

Entity definitions (counted from the training_samples grain — one row per recommended movie in a
request):
  impression = one training_samples row (a recommended movie shown)
  query      = concat_ws(" ", genres) of that movie  ("unknown" if no genres)
  user / session = distinct user_id / session_id
  clicks = Σ clicked   orders = Σ ordered
  CTR  = clicks / impressions
  CVR  = orders / impressions          (conversion over impressions)
  avg_rating = avg(label)              (implicit label: click→1.0, order→2.0, else 0.0)

Analyses:
  1. Most common queries — top genre-combo queries by impressions (with users/sessions/CTR/CVR).
  2. Short (≤10 chars) vs long (>10 chars) query engagement — CTR / CVR / avg_rating per bucket.

Genres come from the Parquet `genres` column if present, else Redis movie:{id}:features.

Run via the pinned Spark:
    REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" \
        services/python-modeling/query_analysis_report.py --input <training-samples parquet>
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from genre_meta import fetch_movie_meta

SHORT_MAX_CHARS = 10


def ensure_genres(spark: SparkSession, df: DataFrame, host: str, port: int) -> DataFrame:
    if "genres" in df.columns:
        return df
    rows = [{"item_id": r["item_id"], "genres": r["genres"]} for r in fetch_movie_meta(host, port)]
    schema = T.StructType([
        T.StructField("item_id", T.StringType()),
        T.StructField("genres", T.ArrayType(T.StringType())),
    ])
    return df.join(spark.createDataFrame(rows, schema), "item_id", "left")


def with_query(df: DataFrame) -> DataFrame:
    """Add `query` (genre-combo, 'unknown' if empty) and its char length + length bucket."""
    combo = F.concat_ws(" ", F.col("genres"))
    return (df
            .withColumn("query", F.when(combo == "", F.lit("unknown")).otherwise(combo))
            .withColumn("query_len", F.length(F.col("query")))
            .withColumn("query_length",
                        F.when(F.length(F.col("query")) <= SHORT_MAX_CHARS, F.lit("short (<=10)"))
                         .otherwise(F.lit("long (>10)"))))


def _engagement_aggs():
    return [
        F.count(F.lit(1)).alias("impressions"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("session_id").alias("sessions"),
        F.sum("clicked").alias("clicks"),
        F.sum("ordered").alias("orders"),
    ]


def _with_rates(grouped: DataFrame) -> DataFrame:
    return (grouped
            .withColumn("ctr", F.round(F.col("clicks") / F.col("impressions"), 4))
            .withColumn("cvr", F.round(F.col("orders") / F.col("impressions"), 4))
            .withColumn("avg_rating", F.round(F.col("rating_sum") / F.col("impressions"), 4))
            .drop("rating_sum"))


def most_common_queries(df: DataFrame) -> DataFrame:
    grouped = df.groupBy("query", "query_len").agg(
        *_engagement_aggs(), F.sum("label").alias("rating_sum"))
    return _with_rates(grouped).orderBy(F.col("impressions").desc(), F.col("query"))


def length_engagement(df: DataFrame) -> DataFrame:
    grouped = df.groupBy("query_length").agg(
        *_engagement_aggs(),
        F.countDistinct("query").alias("distinct_queries"),
        F.sum("label").alias("rating_sum"))
    return _with_rates(grouped).orderBy("query_length")


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movie-category-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--top", type=int, default=20, help="top-N most common queries")
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-queries"

    spark = SparkSession.builder.appName("QueryAnalysisReport").getOrCreate()
    base = ensure_genres(spark, spark.read.parquet(args.input),
                         os.environ.get("REDIS_HOST", "localhost"),
                         int(os.environ.get("REDIS_PORT", "6379")))
    df = with_query(base).cache()

    def emit(name, frame, n):
        print(f"=== {name} ===")
        frame.show(n, truncate=False)
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{outdir}/{name}")

    emit("top_queries", most_common_queries(df).limit(args.top), args.top)   # analysis 1
    emit("by_query_length", length_engagement(df), 10)                       # analysis 2

    print(f"wrote query-analysis CSVs under {outdir}")
    spark.stop()


if __name__ == "__main__":
    main()
