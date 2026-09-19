"""The off-policy section reads two sources, and says which one it read.

compute_ope read Redis only. The four post-training arms -- tabQ, fqiQ, dpoScore,
grpoScore -- are written by post_train_q.py and post_train_dpo.py into each
candidate's modelPredictions and persisted with --output-parquet "for
ope_eval_report.py --parquet", so they existed on disk and the dashboard had no way
to read them. policy_names already admits them; only the input path was missing.

Nothing in this repository writes replay:recommendations -- no .scala file mentions
it and every Python reference is a reader -- so the Redis source is populated by the
serving path in lingduoduo/Recsys-Backend-Service, not from here.
"""

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "services" / "python-modeling"))
sys.path.insert(0, str(REPO / "frontend"))


def _event(i, extra_predictions=None):
    """One replay event, shaped like what the serving path writes."""
    rel = (i % 10) / 10.0
    reward = 1.0 if rel >= 0.5 else 0.0
    predictions = {"relevance": rel}
    predictions.update(extra_predictions or {})
    return {"requestId": f"r{i}", "user": "u", "action": f"m{i}", "actionPosition": 0,
            "coldStart": False, "modelPredictions": dict(predictions),
            "reward": reward, "clicked": int(reward),
            "actionSpace": [{"item": f"m{i}", "coldStart": False, "impressions": i % 50,
                             "clicks": int(reward), "modelPredictions": dict(predictions)}]}


def _scored_parquet(tmp_path, events):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    path = tmp_path / "scored-replay.parquet"
    pd.DataFrame(events).to_parquet(path, index=False)
    return path


def test_parquet_source_surfaces_the_post_training_arms(tmp_path):
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash

    events = [_event(i, {"dpoScore": 1.0 - (i % 10) / 10.0, "tabQ": (i % 10) / 5.0})
              for i in range(120)]
    path = _scored_parquet(tmp_path, events)

    result = dash.compute_ope("localhost", 6399, parquet=str(path), bootstrap_samples=20)

    policies = {row["policy"] for row in result["rows"]}
    assert "model:dpoScore" in policies, "post-training DPO arm must appear as a policy"
    assert "model:tabQ" in policies, "post-training tabular-Q arm must appear as a policy"
    assert "logging" in policies


def test_parquet_source_is_named_in_the_payload(tmp_path):
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash

    path = _scored_parquet(tmp_path, [_event(i, {"dpoScore": 0.5}) for i in range(120)])
    result = dash.compute_ope("localhost", 6399, parquet=str(path), bootstrap_samples=20)
    assert result["source"] == f"parquet:{path}"


def test_redis_source_is_named_in_the_payload(monkeypatch):
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash
    import ope_support

    events = [_event(i) for i in range(120)]
    monkeypatch.setattr(ope_support, "load_from_redis", lambda *a, **k: events)
    result = dash.compute_ope("localhost", 6399, bootstrap_samples=20)
    assert result["source"] == "redis:replay:recommendations"


def test_parquet_takes_precedence_over_a_reachable_redis(tmp_path, monkeypatch):
    """Precedence matches replay_dataset.load_events: --parquet wins.

    Proven by which policies come back, not by asserting a loader was not called: the
    Redis events carry no dpoScore, so its presence can only come from the Parquet.
    """
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash
    import ope_support

    monkeypatch.setattr(ope_support, "load_from_redis",
                        lambda *a, **k: [_event(i) for i in range(120)])
    path = _scored_parquet(tmp_path, [_event(i, {"dpoScore": 1.0 - (i % 10) / 10.0})
                                      for i in range(120)])

    result = dash.compute_ope("localhost", 6399, parquet=str(path), bootstrap_samples=20)
    assert "model:dpoScore" in {row["policy"] for row in result["rows"]}
    assert result["source"].startswith("parquet:")
