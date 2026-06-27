#!/usr/bin/env python3
"""Engagement time-series report (SCAFFOLD).

Reads the date-partitioned training_samples Parquet produced by the engagement
simulation (run-engagement-sim.sh) and builds the engagement metric over time:

    CTR = mean(clicked)   aggregated by date, by hour-of-day, and by day-of-week.

This is the *data layer* + a scaffold for the analyses requested:
  - how has CTR trended over the last few weeks?      → daily series + rolling mean (done)
  - normal seasonal effect vs real change?            → by-dow / by-hour series (done)
  - sudden vs gradual?                                 → largest day-over-day delta (rough; see TODO)
  - more pronounced day / time of day?                → peak/trough dow & hour (done)
The deeper statistical pieces (STL/seasonal decomposition, formal changepoint
detection, confidence bands) are left as clearly-marked TODOs for the future report.

Usage:
    python engagement_report.py --input /tmp/spark-recsys/engagement-sim/training-samples \
                                [--outdir <dir>]
"""
from __future__ import annotations

import argparse
from pathlib import Path


def load_samples(input_dir: Path):
    import pandas as pd
    df = pd.read_parquet(input_dir)  # partition column `date` is recovered automatically
    df["impression_time"] = pd.to_datetime(df["impression_time"])
    df["day"] = df["impression_time"].dt.floor("D")
    df["hour"] = df["impression_time"].dt.hour
    df["dow"] = df["impression_time"].dt.dayofweek  # 0=Mon … 6=Sun
    return df


def build_series(df):
    """Return (daily, by_hour, by_dow) CTR frames — the time-series store."""
    daily = df.groupby("day").agg(ctr=("clicked", "mean"),
                                  impressions=("clicked", "size")).reset_index()
    daily["ctr_7d"] = daily["ctr"].rolling(7, min_periods=1).mean()  # trend
    by_hour = df.groupby("hour").agg(ctr=("clicked", "mean")).reset_index()
    by_dow = df.groupby("dow").agg(ctr=("clicked", "mean")).reset_index()
    return daily, by_hour, by_dow


def summarize(daily, by_hour, by_dow) -> str:
    dows = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    lines = [
        f"window: {daily['day'].min():%Y-%m-%d} → {daily['day'].max():%Y-%m-%d}  ({len(daily)} days)",
        f"overall CTR: {daily['ctr'].mean():.3f}   first day {daily['ctr'].iloc[0]:.3f} → "
        f"last day {daily['ctr'].iloc[-1]:.3f}",
    ]
    # Rough sudden-vs-gradual signal: biggest single day-over-day drop vs the average drift.
    delta = daily["ctr"].diff()
    if delta.notna().any():
        worst_i = delta.idxmin()
        lines.append(f"largest day-over-day drop: {delta.min():+.3f} on "
                     f"{daily['day'].iloc[worst_i]:%Y-%m-%d} "
                     f"(avg daily drift {delta.mean():+.4f})")
    peak_h, trough_h = by_hour.loc[by_hour.ctr.idxmax()], by_hour.loc[by_hour.ctr.idxmin()]
    lines.append(f"time-of-day: peak hour {int(peak_h.hour):02d}:00 ({peak_h.ctr:.3f}), "
                 f"trough {int(trough_h.hour):02d}:00 ({trough_h.ctr:.3f})")
    peak_d, trough_d = by_dow.loc[by_dow.ctr.idxmax()], by_dow.loc[by_dow.ctr.idxmin()]
    lines.append(f"day-of-week: highest {dows[int(peak_d.dow)]} ({peak_d.ctr:.3f}), "
                 f"lowest {dows[int(trough_d.dow)]} ({trough_d.ctr:.3f})")
    return "\n".join(lines)


# ── Future scope (deeper analysis the full report will add) ─────────────────────
def detect_changepoint(daily):
    """TODO: formal changepoint detection (e.g. ruptures/PELT) to separate a sudden
    step from gradual drift, with the change date + magnitude + confidence."""
    raise NotImplementedError("future scope: changepoint detection")


def seasonal_decompose(daily):
    """TODO: STL decomposition into trend / weekly-seasonal / residual to judge whether
    a move is a normal seasonal effect or a genuine shift."""
    raise NotImplementedError("future scope: seasonal decomposition")


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path,
                    default=Path("/tmp/spark-recsys/engagement-sim/training-samples"))
    ap.add_argument("--outdir", type=Path, default=None)
    args = ap.parse_args(argv)

    df = load_samples(args.input)
    daily, by_hour, by_dow = build_series(df)

    outdir = args.outdir or (args.input.parent / "report")
    outdir.mkdir(parents=True, exist_ok=True)
    daily.to_csv(outdir / "ctr_daily.csv", index=False)
    by_hour.to_csv(outdir / "ctr_by_hour.csv", index=False)
    by_dow.to_csv(outdir / "ctr_by_dow.csv", index=False)

    print(summarize(daily, by_hour, by_dow))
    print(f"\nwrote time-series CSVs to {outdir}")


if __name__ == "__main__":
    main()
