def safe_ratio(numerator, denominator):
    return None if denominator <= 0 else numerator / denominator


def available(headline, rows, sample_size, coverage, window=None, warnings=()):
    return {
        "status": "available",
        "headline": headline,
        "sampleSize": int(sample_size),
        "coverage": round(float(coverage), 4),
        "window": window,
        "warnings": list(warnings),
        "rows": list(rows),
    }


def unavailable(reason, warnings=()):
    return {
        "status": "unavailable",
        "headline": "N/A",
        "warnings": [reason, *warnings],
        "rows": [],
    }
