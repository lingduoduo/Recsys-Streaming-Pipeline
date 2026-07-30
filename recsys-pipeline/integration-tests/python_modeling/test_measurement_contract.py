import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

from measurement_contract import available, safe_ratio, unavailable  # noqa: E402


def test_available_keeps_support_and_coverage():
    result = available("CTR 25.0%", [{"ctr": 0.25}], 8, 0.8)
    assert result == {
        "status": "available",
        "headline": "CTR 25.0%",
        "sampleSize": 8,
        "coverage": 0.8,
        "window": None,
        "warnings": [],
        "rows": [{"ctr": 0.25}],
    }


def test_unavailable_does_not_fabricate_numeric_values():
    result = unavailable("missing rating")
    assert result["status"] == "unavailable"
    assert result["warnings"] == ["missing rating"]
    assert "sampleSize" not in result
    assert "coverage" not in result


def test_safe_ratio_rejects_empty_denominator():
    assert safe_ratio(3, 0) is None
    assert safe_ratio(3, 4) == 0.75
