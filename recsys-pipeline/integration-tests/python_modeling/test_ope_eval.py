import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
import ope_eval_report as ope


def _event(rid, item, mp_rel, impressions, clicks, cold, reward, clicked, pos=0):
    # Real Java schema: impressions/clicks live ONLY inside actionSpace candidate dicts,
    # never at the top level. actionPosition is top-level-only (not a scoring feature).
    return {
        "requestId": rid, "user": "u", "action": item,
        "actionPosition": pos, "coldStart": cold,
        "modelPredictions": {"relevance": mp_rel},
        "reward": reward, "clicked": clicked,
        "actionSpace": [
            {"item": item, "coldStart": cold, "impressions": impressions,
             "clicks": clicks, "modelPredictions": {"relevance": mp_rel}},
        ],
    }


def _dataset(n=200):
    # reward driven by relevance: higher relevance -> reward 1
    events = []
    for i in range(n):
        rel = (i % 10) / 10.0
        reward = 1.0 if rel >= 0.5 else 0.0
        events.append(_event(f"r{i}", f"m{i}", rel, impressions=i % 50,
                             clicks=int(reward), cold=False,
                             reward=reward, clicked=int(reward)))
    return events


def test_feature_names_include_model_prediction_keys():
    names = ope.feature_names(_dataset(5))
    assert names[:3] == ["coldStart", "impressions", "clicks"]
    assert "relevance" in names


def test_reward_model_calibrates_on_learnable_signal():
    model = ope.fit_reward_model(_dataset(200))
    assert model.calibration["n_test"] > 0
    assert model.calibration["auc"] is None or model.calibration["auc"] >= 0.9
    assert model.calibration["mse"] <= 0.15


def test_logging_value_equals_mean_observed_reward():
    events = _dataset(100)
    model = ope.fit_reward_model(events)
    rows = ope.evaluate(events, model)
    logging_row = next(r for r in rows if r["policy"] == "logging")
    mean_reward = sum(e["reward"] for e in events) / len(events)
    assert abs(logging_row["value"] - mean_reward) < 1e-9
    assert logging_row["lift_vs_logging"] == 0.0


def test_relevance_policy_beats_random_on_learnable_signal():
    # candidates carry two items: one high-relevance, one low; reward tracks relevance
    events = []
    for i in range(150):
        hi = {"item": f"h{i}", "coldStart": False, "impressions": 5, "clicks": 5,
              "modelPredictions": {"relevance": 0.9}}
        lo = {"item": f"l{i}", "coldStart": False, "impressions": 5, "clicks": 0,
              "modelPredictions": {"relevance": 0.1}}
        taken = hi if i % 2 == 0 else lo
        events.append({
            "requestId": f"r{i}", "user": "u", "action": taken["item"],
            "actionPosition": 0, "coldStart": False,
            "impressions": taken["impressions"], "clicks": taken["clicks"],
            "modelPredictions": taken["modelPredictions"],
            "reward": 1.0 if taken is hi else 0.0,
            "clicked": 1 if taken is hi else 0,
            "actionSpace": [hi, lo],
        })
    model = ope.fit_reward_model(events)
    rows = {r["policy"]: r["value"] for r in ope.evaluate(events, model)}
    assert rows["model:relevance"] > rows["random"]


def test_main_reads_redis_and_writes_csv(tmp_path):
    import json
    from unittest.mock import MagicMock, patch
    events = _dataset(40)
    raw = [json.dumps(e).encode() for e in events]
    client = MagicMock(); client.lrange.return_value = raw
    out = tmp_path / "ope.csv"
    with patch("ope_eval_report.redis.Redis", return_value=client):
        rows = ope.main(["--output", str(out)])
    assert out.is_file()
    assert any(r["policy"] == "logging" for r in rows)


def test_estimator_reads_click_signal_from_actionspace():
    # Reward is driven ONLY by a click signal that lives inside actionSpace (relevance and
    # coldStart are constant). If the estimator sourced features from the top-level event
    # (the schema-skew bug), clicks would read 0 for every training row and AUC ~0.5.
    events = []
    for i in range(200):
        clicked_lot = i % 2 == 0
        clicks = 9 if clicked_lot else 0
        reward = 1.0 if clicked_lot else 0.0
        events.append({
            "requestId": f"c{i}", "user": "u", "action": f"m{i}",
            "coldStart": False, "modelPredictions": {"relevance": 0.5},
            "reward": reward, "clicked": int(reward),
            "actionSpace": [
                {"item": f"m{i}", "coldStart": False, "impressions": 10,
                 "clicks": clicks, "modelPredictions": {"relevance": 0.5}},
            ],
        })
    model = ope.fit_reward_model(events)
    assert model.calibration["auc"] is None or model.calibration["auc"] >= 0.9


def test_empty_test_split_does_not_crash(monkeypatch):
    # Force every event into the train split -> empty held-out set. Must degrade, not crash.
    monkeypatch.setattr(ope, "is_test", lambda rid: False)
    model = ope.fit_reward_model(_dataset(20))
    assert model.calibration["n_test"] == 0
    assert model.calibration["mse"] is None
    assert model.calibration["auc"] is None


def _interval_rows(events, samples=120, seed=17):
    model = ope.fit_reward_model(events)
    points = ope.evaluate(events, model)
    return ope.bootstrap_intervals(events, model, points, samples=samples, seed=seed)


def test_bootstrap_is_deterministic_and_does_not_mutate_points():
    events = _dataset(80)
    model = ope.fit_reward_model(events)
    points = ope.evaluate(events, model)
    snapshot = [dict(row) for row in points]
    assert (ope.bootstrap_intervals(events, model, points, 80, 31)
            == ope.bootstrap_intervals(events, model, points, 80, 31))
    assert points == snapshot


def test_bootstrap_intervals_contain_stable_points():
    rows = _interval_rows(_dataset(100), samples=160, seed=7)
    for row in rows:
        assert row["value_ci_low"] <= row["value"] <= row["value_ci_high"]
    logging = next(row for row in rows if row["policy"] == "logging")
    assert (logging["lift_ci_low"], logging["lift_ci_high"]) == (0.0, 0.0)


def test_bootstrap_edge_cases():
    one = _interval_rows([_dataset(1)[0]], samples=20, seed=3)
    assert all(row["value_ci_low"] == row["value_ci_high"] for row in one)
    disabled = _interval_rows(_dataset(20), samples=0)
    assert all(all(row[field] is None for field in ope.INTERVAL_FIELDS)
               for row in disabled)


def test_zero_reward_has_value_intervals_but_no_lift():
    events = _dataset(40)
    for event in events:
        event["reward"] = 0.0
        event["clicked"] = 0
    rows = _interval_rows(events, samples=50, seed=5)
    for row in rows:
        assert row["value_ci_low"] is not None
        assert row["lift_vs_logging"] is None
        assert row["lift_ci_low"] is None
        assert row["lift_ci_high"] is None


def test_negative_bootstrap_samples_are_rejected():
    events = _dataset(20)
    model = ope.fit_reward_model(events)
    with pytest.raises(ValueError, match="bootstrap samples must be nonnegative"):
        ope.bootstrap_intervals(events, model, ope.evaluate(events, model), samples=-1)
