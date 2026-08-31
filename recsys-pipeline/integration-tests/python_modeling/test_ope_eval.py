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


def test_main_reads_redis_and_writes_csv(tmp_path, capsys):
    import csv
    import json
    from unittest.mock import MagicMock, patch
    events = _dataset(40)
    client = MagicMock()
    client.lrange.return_value = [json.dumps(e).encode() for e in events]
    out = tmp_path / "ope.csv"
    with patch("ope_eval_report.redis.Redis", return_value=client):
        rows = ope.main(["--output", str(out), "--bootstrap-samples", "30",
                         "--bootstrap-seed", "11"])
    stdout = capsys.readouterr().out
    assert "conditional on fixed reward model" in stdout
    assert "95% CI" in stdout
    with out.open(newline="") as fh:
        csv_rows = list(csv.DictReader(fh))
    assert set(ope.INTERVAL_FIELDS).issubset(csv_rows[0])
    assert any(row["policy"] == "logging" for row in rows)


def test_main_zero_bootstrap_prints_na_and_writes_blank_intervals(tmp_path, capsys):
    import csv
    import json
    from unittest.mock import MagicMock, patch
    events = _dataset(40)
    client = MagicMock()
    client.lrange.return_value = [json.dumps(e).encode() for e in events]
    out = tmp_path / "ope.csv"
    with patch("ope_eval_report.redis.Redis", return_value=client):
        ope.main(["--output", str(out), "--bootstrap-samples", "0"])
    stdout = capsys.readouterr().out
    assert "95% CI=N/A" in stdout
    assert "95% lift CI=N/A" in stdout
    with out.open(newline="") as fh:
        csv_rows = list(csv.DictReader(fh))
    assert csv_rows
    assert all(row[field] == "" for row in csv_rows for field in ope.INTERVAL_FIELDS)


def test_main_rejects_negative_bootstrap_samples():
    with pytest.raises(SystemExit):
        ope.main(["--bootstrap-samples", "-1"])


def test_main_prints_na_lift_for_zero_reward(tmp_path, capsys):
    import json
    from unittest.mock import MagicMock, patch
    events = _dataset(40)
    for event in events:
        event["reward"] = 0.0
        event["clicked"] = 0
    raw = [json.dumps(e).encode() for e in events]
    client = MagicMock(); client.lrange.return_value = raw
    out = tmp_path / "ope.csv"
    with patch("ope_eval_report.redis.Redis", return_value=client):
        rows = ope.main(["--output", str(out)])
    assert "lift=N/A" in capsys.readouterr().out
    assert all(row["lift_vs_logging"] is None for row in rows)


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


def test_evaluate_uses_raw_statistics_before_rounding_report_rows():
    class ExactModel:
        calibration = {"auc": None, "mse": None}

        def predict_one(self, candidate):
            return float(candidate["impressions"]) / 7.0

    events = [_event("a", "a", 0.5, 1, 0, False, 1.0, 1),
              _event("b", "b", 0.5, 2, 0, False, 1.0, 1)]
    raw = {row["policy"]: row for row in
           ope._evaluate_statistics(events, ExactModel(), ope.policy_names(events))}
    report = {row["policy"]: row for row in ope.evaluate(events, ExactModel())}
    assert raw["popularity"]["value"] == pytest.approx(3.0 / 14.0)
    assert report["popularity"]["value"] == round(3.0 / 14.0, 4)
    assert raw["popularity"]["value"] != report["popularity"]["value"]


def test_sparse_model_policy_contributes_to_every_bootstrap_replicate(monkeypatch):
    events = _dataset(12)
    events[0]["modelPredictions"]["sparse"] = 0.9
    events[0]["actionSpace"][0]["modelPredictions"]["sparse"] = 0.9
    for event in events:
        event["reward"] = 1.0
        event["clicked"] = 1
    model = ope.fit_reward_model(events)
    points = ope.evaluate(events, model)
    sample_count = 40
    lengths = []
    original_percentile = ope._percentile_bounds

    def recording_percentile(values):
        lengths.append(len(values))
        return original_percentile(values)

    monkeypatch.setattr(ope, "_percentile_bounds", recording_percentile)
    rows = ope.bootstrap_intervals(events, model, points, samples=sample_count, seed=19)
    sparse = next(row for row in rows if row["policy"] == "model:sparse")
    assert sparse["value_ci_low"] is not None
    assert lengths
    assert all(length == sample_count for length in lengths)


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


def test_candidates_of_accepts_a_parquet_round_tripped_action_space(tmp_path):
    """Parquet turns nested lists into ndarrays; `or []` on one raises ValueError."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    event = {
        "requestId": "r1", "user": "u", "action": "m1", "reward": 1.0, "clicked": True,
        "modelPredictions": {"relevance": 0.8},
        "actionSpace": [
            {"item": "m1", "coldStart": False, "impressions": 20, "clicks": 10,
             "modelPredictions": {"relevance": 0.8}},
            {"item": "m2", "coldStart": False, "impressions": 20, "clicks": 1,
             "modelPredictions": {"relevance": 0.2}},
        ],
    }
    path = tmp_path / "replay.parquet"
    pd.DataFrame([event]).to_parquet(path, index=False)
    reloaded = pd.read_parquet(path).to_dict(orient="records")[0]

    candidates = ope.candidates_of(reloaded)

    assert [c["item"] for c in candidates] == ["m1", "m2"]
    assert ope.pick("ctr", reloaded)["item"] == "m1"


def test_candidates_of_returns_empty_for_a_missing_action_space():
    assert ope.candidates_of({"requestId": "r1"}) == []


def test_grpo_score_is_excluded_from_reward_model_features():
    """A model:* score fed to the reward model that grades it makes the policy grade itself.

    This is not hypothetical here: feature_names() builds the reward model's inputs from every
    modelPredictions key that is not registered as policy-only.
    """
    import ope_eval_report

    assert ope_eval_report.GRPO_PRED_KEY in ope_eval_report.POLICY_ONLY_PRED_KEYS

    event = {"modelPredictions": {"predictionScore": 0.4, ope_eval_report.GRPO_PRED_KEY: 0.9}}
    assert ope_eval_report.GRPO_PRED_KEY not in ope_eval_report.feature_names([event])
