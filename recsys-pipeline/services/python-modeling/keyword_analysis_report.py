#!/usr/bin/env python3
"""Keyword analysis report (PySpark) — two analyses over the engagement stream.

Reads the training_samples Parquet (user_id, session_id, item_id, clicked) and movie genres
(from the Parquet `genres` column if present — e.g. the OnlineJoiner catalog join — otherwise
from Redis movie:{id}:features). Per movie: keyword = first genre, subkeyword = second genre;
category l1 = genre family, l2 = primary genre, l3 = genre×decade (via movie_categories).

A "query" is a (user, session); its keywords are the genres of the movies the user **clicked**
in that session, so the query side below is the clicked rows.

Analyses:
  1. Keyword / subkeyword distribution — for movies (impressions + distinct movies) and queries
     (clicks): how often each genre is a movie's first vs second genre.
  2. Category top keywords (movies vs queries) — within each category value (l1/l2/l3), the top
     keywords (genres, exploded), ranked by movie impressions, with the query-click count beside
     them — surfacing where clicked interest diverges from what was shown.

Run via the project's pinned Spark:
    REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" \
        services/python-modeling/keyword_analysis_report.py --input <parquet>
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

import movie_categories as mc
from genre_meta import fetch_movie_meta

LEVELS = ["l1", "l2", "l3"]


def ensure_meta(spark: SparkSession, df: DataFrame, host: str, port: int) -> DataFrame:
    """Guarantee `genres` (array) + `release_year` columns; fetch from Redis if absent."""
    if "genres" in df.columns:
        return df if "release_year" in df.columns \
            else df.withColumn("release_year", F.lit(None).cast(T.IntegerType()))
    rows = fetch_movie_meta(host, port)
    schema = T.StructType([
        T.StructField("item_id", T.StringType()),
        T.StructField("genres", T.ArrayType(T.StringType())),
        T.StructField("release_year", T.IntegerType()),
    ])
    meta = spark.createDataFrame(rows, schema)
    return df.join(meta, "item_id", "left")


def with_keywords_and_categories(df: DataFrame) -> DataFrame:
    pg = F.udf(mc.primary_genre, T.StringType())
    sg = F.udf(mc.secondary_genre, T.StringType())
    l1 = F.udf(mc.l1, T.StringType())
    l3 = F.udf(lambda g, y: mc.l3(g, y), T.StringType())
    return (df
            .withColumn("keyword", pg(F.col("genres")))
            .withColumn("subkeyword", sg(F.col("genres")))
            .withColumn("l1", l1(F.col("genres")))
            .withColumn("l2", pg(F.col("genres")))           # l2 == primary genre
            .withColumn("l3", l3(F.col("genres"), F.col("release_year"))))


def keyword_distribution(df: DataFrame, dim: str) -> DataFrame:
    """Distribution of `dim` (keyword|subkeyword) over movies (impressions / distinct movies)
    and queries (clicks)."""
    return (df.groupBy(dim)
              .agg(F.count(F.lit(1)).alias("movie_impressions"),
                   F.countDistinct("item_id").alias("distinct_movies"),
                   F.sum("clicked").alias("query_clicks"))
              .orderBy(F.col("movie_impressions").desc()))


def category_top_keywords(df: DataFrame, level: str) -> DataFrame:
    """Within each category value, rank exploded genres by movie impressions (with query clicks)."""
    exploded = df.select(F.col(level), F.explode("genres").alias("keyword"),
                         F.col("item_id"), F.col("clicked"))
    grouped = (exploded.groupBy(level, "keyword")
               .agg(F.count(F.lit(1)).alias("movie_impressions"),
                    F.sum("clicked").alias("query_clicks")))
    rank = Window.partitionBy(level).orderBy(F.col("movie_impressions").desc(), F.col("keyword"))
    return grouped.withColumn("rank", F.row_number().over(rank)).orderBy(level, "rank")


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movie-category-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--top", type=int, default=10, help="top-N keywords per category")
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-keywords"

    spark = SparkSession.builder.appName("KeywordAnalysisReport").getOrCreate()
    # Ship the sibling modules the UDFs use so they import on the Spark workers too.
    _here = os.path.dirname(os.path.abspath(__file__))
    for _m in ("movie_categories.py", "genre_meta.py"):
        spark.sparkContext.addPyFile(os.path.join(_here, _m))

    base = ensure_meta(spark, spark.read.parquet(args.input),
                       os.environ.get("REDIS_HOST", "localhost"),
                       int(os.environ.get("REDIS_PORT", "6379")))
    df = with_keywords_and_categories(base).cache()

    def emit(name, frame):
        print(f"=== {name} ===")
        frame.show(args.top, truncate=False)
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{outdir}/{name}")

    # Analysis 1 — keyword / subkeyword distribution
    emit("by_keyword", keyword_distribution(df, "keyword"))
    emit("by_subkeyword", keyword_distribution(df, "subkeyword"))

    # Analysis 2 — category top keywords (movies vs queries)
    for level in LEVELS:
        emit(f"top_keywords_{level}", category_top_keywords(df, level).filter(F.col("rank") <= args.top))

    print(f"wrote keyword-analysis CSVs under {outdir}")
    spark.stop()


if __name__ == "__main__":
    main()
