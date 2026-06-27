#!/usr/bin/env python3
"""Relevance analysis report (PySpark) — relevance-state distribution + query/genre relevance.

Relevance is the graded engagement label of a recommended impression:
  label 0.0 → impression_only   1.0 → clicked   2.0 → ordered

A "query" is the impression's genre-combo intent (`concat_ws(" ", genres)`), consistent with the
query-analysis job. Analyses:

  1. Relevance-state distribution — per state: score, impressions, proportion (score vs proportions).
  2. Query / genre relevance — relevance-state mix + mean score broken down by query AND
     (separately) by movie genre (exploded): mean_score, impression_only/clicked/ordered shares.

Genres come from the Parquet `genres` column if present, else Redis movie:{id}:features.

Run via the pinned Spark:
    REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" \
        services/python-modeling/relevance_analysis_report.py --input <training-samples parquet>
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from genre_meta import fetch_movie_meta


def ensure_genres(spark: SparkSession, df: DataFrame, host: str, port: int) -> DataFrame:
    if "genres" in df.columns:
        return df
    rows = [{"item_id": r["item_id"], "genres": r["genres"]} for r in fetch_movie_meta(host, port)]
    schema = T.StructType([
        T.StructField("item_id", T.StringType()),
        T.StructField("genres", T.ArrayType(T.StringType())),
    ])
    return df.join(spark.createDataFrame(rows, schema), "item_id", "left")


def with_relevance(df: DataFrame) -> DataFrame:
    """Add `relevance_state` (from label) and `query` (genre-combo)."""
    combo = F.concat_ws(" ", F.col("genres"))
    return (df
            .withColumn("relevance_state",
                        F.when(F.col("label") >= 2.0, F.lit("ordered"))
                         .when(F.col("label") >= 1.0, F.lit("clicked"))
                         .otherwise(F.lit("impression_only")))
            .withColumn("query", F.when(combo == "", F.lit("unknown")).otherwise(combo)))


def state_distribution(df: DataFrame) -> DataFrame:
    total = df.count()
    denom = float(total) if total else 1.0
    return (df.groupBy("relevance_state")
              .agg(F.first("label").alias("score"), F.count(F.lit(1)).alias("impressions"))
              .withColumn("proportion", F.round(F.col("impressions") / F.lit(denom), 4))
              .orderBy("score"))


def _relevance_breakdown(df: DataFrame, dim: str) -> DataFrame:
    """Per `dim`: impressions, mean score, and the share of each relevance state."""
    def share(state):
        return F.round(F.avg(F.when(F.col("relevance_state") == state, 1.0).otherwise(0.0)), 4)
    return (df.groupBy(dim)
              .agg(F.count(F.lit(1)).alias("impressions"),
                   F.round(F.avg("label"), 4).alias("mean_score"),
                   share("impression_only").alias("impression_only_share"),
                   share("clicked").alias("clicked_share"),
                   share("ordered").alias("ordered_share"))
              .orderBy(F.col("mean_score").desc(), F.col("impressions").desc()))


def by_query(df: DataFrame) -> DataFrame:
    return _relevance_breakdown(df, "query")


def by_genre(df: DataFrame) -> DataFrame:
    exploded = df.select(F.explode("genres").alias("genre"), "label", "relevance_state")
    return _relevance_breakdown(exploded, "genre")


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movie-category-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--top", type=int, default=20)
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-relevance"

    spark = SparkSession.builder.appName("RelevanceAnalysisReport").getOrCreate()
    base = ensure_genres(spark, spark.read.parquet(args.input),
                         os.environ.get("REDIS_HOST", "localhost"),
                         int(os.environ.get("REDIS_PORT", "6379")))
    df = with_relevance(base).cache()

    def emit(name, frame, n):
        print(f"=== {name} ===")
        frame.show(n, truncate=False)
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{outdir}/{name}")

    emit("by_state", state_distribution(df), 10)                  # analysis 1
    emit("by_query", by_query(df).limit(args.top), args.top)      # analysis 2 (query)
    emit("by_genre", by_genre(df), 30)                            # analysis 2 (movie genres)

    print(f"wrote relevance-analysis CSVs under {outdir}")
    spark.stop()


if __name__ == "__main__":
    main()
