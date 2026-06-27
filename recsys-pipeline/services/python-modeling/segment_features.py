"""Pure feature-derivation helpers shared by the MovieLens segment producer and report.

No heavy imports (no kafka/pyspark) so it can be imported under spark-submit's Python too.
Derives segment buckets from the canonical UserEvent demographics (age:int, zip_code).
"""
from __future__ import annotations

_AGE_BINS = [(0, 24, "18-24"), (25, 34, "25-34"), (35, 44, "35-44"),
             (45, 54, "45-54"), (55, 200, "55+")]

# US ZIP first digit → coarse region (the canonical zip_code is a string).
_ZIP_REGION = {
    "0": "Northeast", "1": "Northeast", "2": "Mid-Atlantic", "3": "Southeast",
    "4": "Midwest", "5": "Midwest", "6": "South-Central", "7": "South-Central",
    "8": "Mountain", "9": "West",
}


def derive_age_band(age) -> str:
    try:
        a = int(age)
    except (TypeError, ValueError):
        return "unknown"
    for lo, hi, label in _AGE_BINS:
        if lo <= a <= hi:
            return label
    return "unknown"


def derive_geo(zip_code) -> str:
    s = str(zip_code) if zip_code is not None else ""
    return _ZIP_REGION.get(s[:1], "unknown")
