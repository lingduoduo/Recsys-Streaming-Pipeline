from collections.abc import Iterable


def safe_ratio(numerator: float, denominator: float) -> float | None:
    return None if denominator <= 0 else numerator / denominator


def available(
    headline: str,
    rows: Iterable[dict[str, object]],
    sample_size: int,
    coverage: float,
    window: object | None = None,
    warnings: Iterable[str] = (),
) -> dict[str, object]:
    return {
        "status": "available",
        "headline": headline,
        "sampleSize": int(sample_size),
        "coverage": round(float(coverage), 4),
        "window": window,
        "warnings": list(warnings),
        "rows": list(rows),
    }


def unavailable(reason: str, warnings: Iterable[str] = ()) -> dict[str, object]:
    return {
        "status": "unavailable",
        "headline": "N/A",
        "warnings": [reason, *warnings],
        "rows": [],
    }
