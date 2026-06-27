#!/usr/bin/env python3
"""Engagement time-series report (PySpark).

Spark's distributed engine with Python's ecosystem: reads the date-partitioned
training_samples Parquet and computes CTR = avg(clicked) by date / hour-of-day /
day-of-week, writing CSVs. Same output as EngagementReportJob.scala and the pandas
engagement_report.py — pick this when you want Spark scalability but Python tooling
(e.g. to bolt statsmodels/ruptures onto the small collected aggregate later).

IMPORTANT — version match: run through the project's pinned Spark so PySpark and the
JVM agree (a pip pyspark that differs from $SPARK_HOME fails with "JavaPackage object
is not callable"). spark-submit also sets the JDK-17 --add-opens automatically:

    "$SPARK_HOME/bin/spark-submit" \
        services/python-modeling/engagement_report_pyspark.py \
        --input /tmp/spark-recsys/engagement-sim/training-samples
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def daily(df: DataFrame) -> DataFrame:
    """Daily CTR + impression count, ordered by day."""
    return (df.groupBy(F.to_date("impression_time").alias("day"))
              .agg(F.avg("clicked").alias("ctr"), F.count(F.lit(1)).alias("impressions"))
              .orderBy("day"))


def by_hour(df: DataFrame) -> DataFrame:
    """CTR by hour-of-day (0-23)."""
    return (df.groupBy(F.hour("impression_time").alias("hour"))
              .agg(F.avg("clicked").alias("ctr"))
              .orderBy("hour"))


def by_dow(df: DataFrame) -> DataFrame:
    """CTR by day-of-week. `dow_num` is Spark's 1=Sun..7=Sat; `dow` is the short name."""
    return (df.groupBy(F.dayofweek("impression_time").alias("dow_num"),
                       F.date_format("impression_time", "E").alias("dow"))
              .agg(F.avg("clicked").alias("ctr"))
              .orderBy("dow_num"))


def write_csv(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/engagement-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-pyspark"

    spark = SparkSession.builder.appName("EngagementReportPySpark").getOrCreate()
    df = spark.read.parquet(args.input).cache()

    d, h, w = daily(df), by_hour(df), by_dow(df)
    write_csv(d, f"{outdir}/ctr_daily")
    write_csv(h, f"{outdir}/ctr_by_hour")
    write_csv(w, f"{outdir}/ctr_by_dow")

    print("=== daily CTR ===");          d.show(50, truncate=False)
    print("=== CTR by hour ===");        h.show(24, truncate=False)
    print("=== CTR by day-of-week ==="); w.show(7, truncate=False)
    print(f"wrote CSVs under {outdir}")
    spark.stop()


if __name__ == "__main__":
    main()
