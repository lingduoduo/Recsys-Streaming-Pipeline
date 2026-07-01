import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))


def _df(pd):
    # 3 impressions: item_2 shown twice (1 click), item_1 shown once (clicked+ordered)
    return pd.DataFrame({
        "user_id":    ["u1", "u2", "u1"],
        "session_id": ["s1", "s2", "s1"],
        "item_id":    ["item_2", "item_2", "item_1"],
        "label":      [1.0, 0.0, 2.0],
        "genres":     [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })


def test_load_samples_normalizes_columns(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash

    parquet = tmp_path / "samples"
    _df(pd).to_parquet(parquet, index=False)

    out = dash.load_samples(str(parquet))
    assert list(out["clicked"]) == [1, 0, 1]          # derived from label >= 1
    assert out["genres"].apply(type).eq(list).all()
    assert dash.query_of(["Sci-Fi", "Action"]) == "Sci-Fi Action"
    assert dash.query_of([]) == "unknown"
