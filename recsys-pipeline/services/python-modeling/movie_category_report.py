#!/usr/bin/env python3
"""Movie-category engagement report (PySpark).

Joins engagement with movie categories across the two real pipeline paths:
  • engagement: training_samples Parquet (per impression: clicked, ordered, item_id)
  • categories: Redis movie:{id}:features (genres, releaseYear) — written by
    MovieLensContextCollectorStreamingJob from the movielens_context stream.

For each category level (l1 family / l2 primary genre / l3 genre×decade) reports impressions
(sample size), CTR, order_rate, clicks_per_item, and CTR lift vs overall. l1/l2/l3 are derived
from the Redis genres/releaseYear via movie_categories.

Run through the project's pinned Spark; set REDIS_HOST/REDIS_PORT for the category join:
    REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" \
        services/python-modeling/movie_category_report.py --input <parquet>
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from movie_categories import l1, l2, l3

LEVELS = ["l1", "l2", "l3"]


def fetch_movie_features(host: str, port: int) -> list[dict]:
    """Read movie:{id}:features from Redis into rows with derived l1/l2/l3.

    Returns [] if Redis is unreachable (report then has nothing to break down by).
    """
    try:
        import redis
        client = redis.Redis(host=host, port=port, decode_responses=True)
        rows = []
        for key in client.scan_iter(match="movie:*:features"):
            h = client.hgetall(key)
            if not h:
                continue
            item_id = key.split(":")[1]
            genres, year = h.get("genres"), h.get("releaseYear")
            rows.append({"item_id": item_id, "l1": l1(genres), "l2": l2(genres),
                         "l3": l3(genres, year)})
        return rows
    except Exception as e:  # noqa: BLE001
        print(f"[warn] could not read movie features from Redis ({e})")
        return []


def per_item_engagement(df: DataFrame) -> DataFrame:
    return df.groupBy("item_id").agg(
        F.count(F.lit(1)).alias("impressions"),
        F.sum("clicked").alias("clicks"),
        F.sum("ordered").alias("orders"))


def category_metrics(joined: DataFrame, level: str, overall_ctr: float) -> DataFrame:
    grouped = joined.groupBy(level).agg(
        F.sum("impressions").alias("impressions"),
        F.sum("clicks").alias("clicks"),
        F.sum("orders").alias("orders"),
        F.countDistinct("item_id").alias("items"))
    return (grouped
            .withColumn("ctr", F.round(F.col("clicks") / F.col("impressions"), 4))
            .withColumn("order_rate", F.round(F.col("orders") / F.col("impressions"), 4))
            .withColumn("clicks_per_item", F.round(F.col("clicks") / F.col("items"), 2))
            .withColumn("ctr_lift_pct",
                        F.round((F.col("ctr") - F.lit(overall_ctr)) / F.lit(overall_ctr) * 100, 1))
            .orderBy(F.col("ctr").desc()))


def _movies_df(spark: SparkSession, rows: list[dict]) -> DataFrame:
    schema = T.StructType([T.StructField(c, T.StringType()) for c in ("item_id", "l1", "l2", "l3")])
    return spark.createDataFrame(rows, schema)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movie-category-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-categories"

    spark = SparkSession.builder.appName("MovieCategoryReport").getOrCreate()
    df = spark.read.parquet(args.input).cache()
    overall_ctr = round(df.agg(F.avg("clicked")).first()[0], 4)
    print(f"overall CTR = {overall_ctr}  (impressions={df.count()})\n")

    rows = fetch_movie_features(os.environ.get("REDIS_HOST", "localhost"),
                               int(os.environ.get("REDIS_PORT", "6379")))
    if not rows:
        print("no movie features in Redis — nothing to break down by; exiting")
        spark.stop()
        return

    joined = per_item_engagement(df).join(_movies_df(spark, rows), "item_id")
    for level in LEVELS:
        m = category_metrics(joined, level, overall_ctr)
        print(f"=== engagement by {level} (CTR desc; lift vs overall) ===")
        m.show(30, truncate=False)
        m.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{outdir}/by_{level}")

    print(f"wrote per-category CSVs under {outdir}")
    spark.stop()


if __name__ == "__main__":
    main()
