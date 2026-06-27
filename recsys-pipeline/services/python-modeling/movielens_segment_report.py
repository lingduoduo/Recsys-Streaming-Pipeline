#!/usr/bin/env python3
"""MovieLens-aligned user-segment engagement report (PySpark).

Joins engagement with demographics across the two real pipeline paths:
  • engagement: training_samples Parquet (per impression: clicked, ordered, user_id, platform)
  • demographics: Redis user:{id}:features (age, gender, occupation, zipCode) — written by
    MovieLensContextCollectorStreamingJob from the movielens_context stream.

Reports per segment: impressions (sample size), CTR, order_rate, clicks/user, CTR lift vs
overall. Platform comes straight from the Parquet context_features map; the demographic
dimensions (age_band, gender, occupation, geo) come from the Redis join. age→age_band and
zip→geo are derived with segment_features.

Run through the project's pinned Spark; set REDIS_HOST/REDIS_PORT for the demographics join:
    REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" \
        services/python-modeling/movielens_segment_report.py --input <parquet>
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from segment_features import derive_age_band, derive_geo

DEMO_DIMS = ["age_band", "gender", "occupation", "geo"]


def fetch_demographics(host: str, port: int) -> list[dict]:
    """Read user:{id}:features hashes from Redis into rows with derived age_band/geo.

    Returns [] if Redis is unreachable so the report still produces the platform breakdown.
    """
    try:
        import redis
        client = redis.Redis(host=host, port=port, decode_responses=True)
        rows = []
        for key in client.scan_iter(match="user:*:features"):
            h = client.hgetall(key)
            if not h:
                continue
            uid = key.split(":")[1]
            rows.append({
                "user_id": uid,
                "gender": h.get("gender"),
                "occupation": h.get("occupation"),
                "age_band": derive_age_band(h.get("age")),
                "geo": derive_geo(h.get("zipCode")),
                "user_avg_rating": float(h.get("avgRating") or 0.0),     # explicit feedback (RatingEvent)
                "user_rating_count": int(float(h.get("ratingCount") or 0)),
            })
        return rows
    except Exception as e:  # noqa: BLE001 - degrade to platform-only
        print(f"[warn] could not read demographics from Redis ({e}); platform-only report")
        return []


def per_user_engagement(df: DataFrame) -> DataFrame:
    return df.groupBy("user_id").agg(
        F.count(F.lit(1)).alias("impressions"),
        F.sum("clicked").alias("clicks"),
        F.sum("ordered").alias("orders"))


def _finalize(grouped: DataFrame, overall_ctr: float) -> DataFrame:
    return (grouped
            .withColumn("ctr", F.round(F.col("clicks") / F.col("impressions"), 4))
            .withColumn("order_rate", F.round(F.col("orders") / F.col("impressions"), 4))
            .withColumn("clicks_per_user", F.round(F.col("clicks") / F.col("users"), 3))
            .withColumn("ctr_lift_pct",
                        F.round((F.col("ctr") - F.lit(overall_ctr)) / F.lit(overall_ctr) * 100, 1))
            .orderBy(F.col("ctr").desc()))


def platform_metrics(parquet_df: DataFrame, overall_ctr: float) -> DataFrame:
    seg = parquet_df.select(F.col("context_features")["platform"].alias("platform"),
                            F.col("clicked"), F.col("ordered"), F.col("user_id"))
    grouped = seg.groupBy("platform").agg(
        F.count(F.lit(1)).alias("impressions"),
        F.sum("clicked").alias("clicks"),
        F.sum("ordered").alias("orders"),
        F.countDistinct("user_id").alias("users"))
    return _finalize(grouped, overall_ctr)


def demographic_metrics(joined: DataFrame, dim: str, overall_ctr: float) -> DataFrame:
    grouped = joined.groupBy(dim).agg(
        F.sum("impressions").alias("impressions"),
        F.sum("clicks").alias("clicks"),
        F.sum("orders").alias("orders"),
        F.countDistinct("user_id").alias("users"),
        # explicit-feedback avg rating, weighted by each user's rating count
        F.sum(F.col("user_avg_rating") * F.col("user_rating_count")).alias("_rsum"),
        F.sum("user_rating_count").alias("_rn"))
    return (_finalize(grouped, overall_ctr)
            .withColumn("avg_rating",
                        F.when(F.col("_rn") > 0, F.round(F.col("_rsum") / F.col("_rn"), 3)))
            .drop("_rsum", "_rn"))


def _demographics_df(spark: SparkSession, rows: list[dict]) -> DataFrame:
    schema = T.StructType(
        [T.StructField(c, T.StringType()) for c in
         ("user_id", "gender", "occupation", "age_band", "geo")]
        + [T.StructField("user_avg_rating", T.DoubleType()),
           T.StructField("user_rating_count", T.LongType())])
    return spark.createDataFrame(rows, schema)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movielens-segment-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-segments"

    spark = SparkSession.builder.appName("MovieLensSegmentReport").getOrCreate()
    df = spark.read.parquet(args.input).cache()
    overall_ctr = round(df.agg(F.avg("clicked")).first()[0], 4)
    print(f"overall CTR = {overall_ctr}  (impressions={df.count()})\n")

    def emit(name, frame):
        print(f"=== engagement by {name} (CTR desc; lift vs overall) ===")
        frame.show(truncate=False)
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{outdir}/by_{name}")

    emit("platform", platform_metrics(df, overall_ctr))  # event-context, from Parquet

    rows = fetch_demographics(os.environ.get("REDIS_HOST", "localhost"),
                              int(os.environ.get("REDIS_PORT", "6379")))
    if rows:
        joined = per_user_engagement(df).join(_demographics_df(spark, rows), "user_id")
        for dim in DEMO_DIMS:
            emit(dim, demographic_metrics(joined, dim, overall_ctr))
    else:
        print("no demographics in Redis — skipped age_band/gender/occupation/geo")

    print(f"wrote per-segment CSVs under {outdir}")
    spark.stop()


if __name__ == "__main__":
    main()
