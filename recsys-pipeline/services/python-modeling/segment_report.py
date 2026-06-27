#!/usr/bin/env python3
"""User-segment engagement report (PySpark).

Reads the training_samples Parquet and breaks engagement down by user segment. Segment
attributes are read out of the user_features / context_features maps the OnlineJoiner carries:
  user_features:    cohort (new/existing), age_band, sex, education
  context_features: geo, platform
For each segment dimension it reports impressions (sample size), CTR = avg(clicked),
order_rate = avg(ordered), clicks_per_user = sum(clicked)/distinct users, and the CTR lift vs
the overall average. CSVs are written per dimension.

Run through the project's pinned Spark so PySpark and the JVM agree:
    "$SPARK_HOME/bin/spark-submit" services/python-modeling/segment_report.py \
        --input /tmp/spark-recsys/segment-sim/training-samples
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

USER_DIMS = ["cohort", "age_band", "sex", "education"]
CONTEXT_DIMS = ["geo", "platform"]
DIMS = USER_DIMS + CONTEXT_DIMS


def with_segment_columns(df: DataFrame) -> DataFrame:
    """Project the segment attributes out of the feature maps into top-level columns."""
    cols = [df["clicked"], df["ordered"], df["user_id"]]
    cols += [F.col("user_features")[d].alias(d) for d in USER_DIMS]
    cols += [F.col("context_features")[d].alias(d) for d in CONTEXT_DIMS]
    return df.select(*cols)


def segment_metrics(seg: DataFrame, dim: str, overall_ctr: float) -> DataFrame:
    """Per-value engagement metrics for one segment dimension."""
    return (seg.groupBy(dim)
               .agg(F.count(F.lit(1)).alias("impressions"),
                    F.round(F.avg("clicked"), 4).alias("ctr"),
                    F.round(F.avg("ordered"), 4).alias("order_rate"),
                    F.round(F.sum("clicked") / F.countDistinct("user_id"), 3)
                     .alias("clicks_per_user"))
               .withColumn("ctr_lift_pct",
                           F.round((F.col("ctr") - F.lit(overall_ctr)) / F.lit(overall_ctr) * 100, 1))
               .orderBy(F.col("ctr").desc()))


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/segment-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-segments"

    spark = SparkSession.builder.appName("SegmentReport").getOrCreate()
    seg = with_segment_columns(spark.read.parquet(args.input)).cache()

    overall_ctr = round(seg.agg(F.avg("clicked")).first()[0], 4)
    print(f"overall CTR = {overall_ctr}  (impressions={seg.count()})\n")

    for dim in DIMS:
        m = segment_metrics(seg, dim, overall_ctr)
        print(f"=== engagement by {dim} (CTR desc; lift vs overall) ===")
        m.show(truncate=False)
        m.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{outdir}/by_{dim}")

    print(f"wrote per-segment CSVs under {outdir}")
    spark.stop()


if __name__ == "__main__":
    main()
