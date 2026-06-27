#!/usr/bin/env python3
"""Session-level engagement report (PySpark).

Reads the training_samples Parquet (which now carries `session_id`, threaded through by
OnlineJoinerStreamingJob) and aggregates engagement by session:

  per session  : slates (distinct request_id), impressions, clicks, orders, session CTR
  rollups      : sessions, users, sessions/user, slates/session, impressions/session,
                 clicks/session, mean session CTR

Run through the project's pinned Spark:
    "$SPARK_HOME/bin/spark-submit" services/python-modeling/session_report.py --input <parquet>

Note: the current behavior producers mint a fresh session_id per slate, so on live sim data
slates/session ≈ 1. The aggregation itself is correct for real multi-slate sessions (see test);
grouping slates into multi-slate sessions is a producer-side follow-up.
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def per_session(df: DataFrame) -> DataFrame:
    """One row per (session_id, user_id) with its engagement; ignores rows without a session."""
    return (df.filter((F.col("session_id").isNotNull()) & (F.col("session_id") != ""))
              .groupBy("session_id", "user_id")
              .agg(F.countDistinct("request_id").alias("slates"),
                   F.count(F.lit(1)).alias("impressions"),
                   F.sum("clicked").alias("clicks"),
                   F.sum("ordered").alias("orders"))
              .withColumn("ctr", F.round(F.col("clicks") / F.col("impressions"), 4)))


def summary(sessions: DataFrame) -> dict:
    """Rollup metrics over the per-session frame."""
    a = sessions.agg(
        F.count(F.lit(1)).alias("sessions"),
        F.countDistinct("user_id").alias("users"),
        F.sum("slates").alias("slates"),
        F.sum("impressions").alias("impressions"),
        F.sum("clicks").alias("clicks"),
        F.sum("orders").alias("orders"),
        F.avg("ctr").alias("mean_session_ctr"),
    ).first()
    sess = a["sessions"] or 0
    users = a["users"] or 0
    return {
        "sessions": sess,
        "users": users,
        "sessions_per_user": round(sess / users, 3) if users else 0.0,
        "slates_per_session": round((a["slates"] or 0) / sess, 3) if sess else 0.0,
        "impressions_per_session": round((a["impressions"] or 0) / sess, 2) if sess else 0.0,
        "clicks_per_session": round((a["clicks"] or 0) / sess, 3) if sess else 0.0,
        "mean_session_ctr": round(a["mean_session_ctr"] or 0.0, 4),
    }


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/engagement-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-sessions"

    spark = SparkSession.builder.appName("SessionReport").getOrCreate()
    df = spark.read.parquet(args.input).cache()
    sessions = per_session(df).cache()

    print("=== session-level engagement summary ===")
    for k, v in summary(sessions).items():
        print(f"  {k}: {v}")

    sessions.write.mode("overwrite").option("header", "true").csv(f"{outdir}/by_session")
    print(f"\nwrote per-session CSV under {outdir}")
    spark.stop()


if __name__ == "__main__":
    main()
