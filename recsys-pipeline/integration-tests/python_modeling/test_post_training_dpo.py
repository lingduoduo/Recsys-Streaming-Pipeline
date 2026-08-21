import sys
from pathlib import Path

import pytest

_MODELING = Path(__file__).parents[2] / "services" / "python-modeling"
sys.path.insert(0, str(_MODELING))
sys.path.insert(0, str(_MODELING / "post-training"))

import ope_eval_report
import slate_pairs


def _candidate(item, relevance, prediction, impressions=20, clicks=5):
    return {
        "item": item,
        "coldStart": False,
        "impressions": impressions,
        "clicks": clicks,
        "modelPredictions": {"relevance": relevance, "predictionScore": prediction},
    }


def _event(request_id, user, action, items):
    """A replay event whose actionSpace carries every item of the slate."""
    return {
        "requestId": request_id,
        "user": user,
        "action": action,
        "state": {"genres": ["drama"], "tags": [], "recent": []},
        "reward": 1.0,
        "clicked": True,
        "timestamp": 1000,
        "actionSpace": [_candidate(i, r, p) for i, r, p in items],
    }


def _slate(request_id, user, items):
    """A slate row in the buildSlates shape: items is [(item_id, clicked, ordered, label)]."""
    return {
        "slate_id": f"{request_id}:{user}",
        "request_id": request_id,
        "user_id": user,
        "slate_size": len(items),
        "items": [
            {"position": n, "item_id": i, "clicked": c, "ordered": o, "label": lab}
            for n, (i, c, o, lab) in enumerate(items)
        ],
    }


def _fixture():
    """One slate: m1 clicked, m2 and m3 exposed but not engaged."""
    events = [_event("r1", "u1", "m1",
                     [("m1", 0.8, 0.7), ("m2", 0.2, 0.3), ("m3", 0.1, 0.2)])]
    slates = [_slate("r1", "u1", [("m1", 1, 0, 1.0), ("m2", 0, 0, 0.0), ("m3", 0, 0, 0.0)])]
    return slates, events


def test_one_clicked_against_two_exposed_yields_two_pairs():
    slates, events = _fixture()
    pairs, dropped = slate_pairs.build_pairs(slates, events)
    assert len(pairs) == 2
    assert dropped == 0
    assert {p.chosen_item for p in pairs} == {"m1"}
    assert {p.rejected_item for p in pairs} == {"m2", "m3"}


def test_a_slate_with_no_engagement_yields_no_pairs():
    events = [_event("r1", "u1", "m1", [("m1", 0.8, 0.7), ("m2", 0.2, 0.3)])]
    slates = [_slate("r1", "u1", [("m1", 0, 0, 0.0), ("m2", 0, 0, 0.0)])]
    assert slate_pairs.build_pairs(slates, events) == ([], 0)


def test_a_slate_where_everything_engaged_yields_no_pairs():
    events = [_event("r1", "u1", "m1", [("m1", 0.8, 0.7), ("m2", 0.2, 0.3)])]
    slates = [_slate("r1", "u1", [("m1", 1, 0, 1.0), ("m2", 1, 0, 1.0)])]
    assert slate_pairs.build_pairs(slates, events) == ([], 0)


def test_pairs_never_cross_slates():
    events = [_event("r1", "u1", "m1", [("m1", 0.8, 0.7), ("m2", 0.2, 0.3)]),
              _event("r2", "u2", "m3", [("m3", 0.9, 0.6), ("m4", 0.1, 0.1)])]
    slates = [_slate("r1", "u1", [("m1", 1, 0, 1.0), ("m2", 0, 0, 0.0)]),
              _slate("r2", "u2", [("m3", 1, 0, 1.0), ("m4", 0, 0, 0.0)])]
    pairs, _ = slate_pairs.build_pairs(slates, events)
    assert {(p.chosen_item, p.rejected_item) for p in pairs} == {("m1", "m2"), ("m3", "m4")}


def test_a_pair_without_a_replay_row_is_dropped_and_counted():
    # The slate mentions m9, which never appears in any replay actionSpace.
    events = [_event("r1", "u1", "m1", [("m1", 0.8, 0.7), ("m2", 0.2, 0.3)])]
    slates = [_slate("r1", "u1", [("m1", 1, 0, 1.0), ("m2", 0, 0, 0.0), ("m9", 0, 0, 0.0)])]
    pairs, dropped = slate_pairs.build_pairs(slates, events)
    assert len(pairs) == 1
    assert dropped == 1


def test_ordered_and_positive_label_also_count_as_chosen():
    assert slate_pairs.is_chosen({"clicked": 0, "ordered": 1, "label": 0.0}) is True
    assert slate_pairs.is_chosen({"clicked": 0, "ordered": 0, "label": 0.5}) is True
    assert slate_pairs.is_chosen({"clicked": 0, "ordered": 0, "label": 0.0}) is False


def test_a_thumb_down_item_is_a_rejected_item_not_a_dropped_one():
    events = [_event("r1", "u1", "m1", [("m1", 0.8, 0.7), ("m2", 0.2, 0.3)])]
    slates = [_slate("r1", "u1", [("m1", 1, 0, 1.0), ("m2", 0, 0, 0.0)])]
    slates[0]["items"][1]["negative_feedback_reason"] = "thumb_down"
    pairs, dropped = slate_pairs.build_pairs(slates, events)
    assert [(p.chosen_item, p.rejected_item) for p in pairs] == [("m1", "m2")]
    assert dropped == 0


def test_reference_scores_come_from_prediction_score():
    slates, events = _fixture()
    pairs, _ = slate_pairs.build_pairs(slates, events)
    by_rejected = {p.rejected_item: p for p in pairs}
    assert by_rejected["m2"].chosen_reference == pytest.approx(0.7)
    assert by_rejected["m2"].rejected_reference == pytest.approx(0.3)


def test_features_use_the_shared_ope_schema():
    slates, events = _fixture()
    names = ope_eval_report.feature_names(events)
    pairs, _ = slate_pairs.build_pairs(slates, events, names)
    assert len(pairs[0].chosen_features) == len(names)


def test_build_pairs_handles_a_parquet_round_tripped_slate(tmp_path):
    """Parquet returns the items array as an ndarray, for which `or []` raises."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    slates, events = _fixture()
    path = tmp_path / "slates.parquet"
    pd.DataFrame(slates).to_parquet(path, index=False)
    reloaded = pd.read_parquet(path).to_dict(orient="records")

    pairs, dropped = slate_pairs.build_pairs(reloaded, events)

    assert len(pairs) == 2
    assert dropped == 0


import math

import torch

import dpo


def _pair(chosen_features, rejected_features, chosen_reference, rejected_reference,
          request_id="r1", chosen_item="w", rejected_item="l"):
    return slate_pairs.PreferencePair(
        request_id=request_id,
        user="u",
        chosen_item=chosen_item,
        rejected_item=rejected_item,
        chosen_features=list(chosen_features),
        rejected_features=list(rejected_features),
        chosen_reference=chosen_reference,
        rejected_reference=rejected_reference,
    )


def test_a_policy_matching_the_reference_scores_exactly_log_two():
    """Zero reference-adjusted margin => -log sigmoid(0) = log 2. Analytic, no tolerance needed."""
    scores_w = torch.tensor([0.7])
    scores_l = torch.tensor([0.3])
    loss = dpo.dpo_loss(scores_w, scores_l, torch.tensor([0.7]), torch.tensor([0.3]), beta=1.0)
    assert loss.item() == pytest.approx(math.log(2.0), abs=1e-6)


def test_a_constant_reference_makes_the_loss_exactly_bpr():
    """When the reference scores both sides equally the margin vanishes and this IS BPR at beta=1."""
    scores_w = torch.tensor([0.9, 0.4])
    scores_l = torch.tensor([0.2, 0.6])
    equal_reference = torch.tensor([0.5, 0.5])
    loss = dpo.dpo_loss(scores_w, scores_l, equal_reference, equal_reference, beta=1.0)
    bpr = -torch.nn.functional.logsigmoid(scores_w - scores_l).mean()
    assert loss.item() == pytest.approx(bpr.item(), abs=1e-9)


def test_beating_the_reference_margin_costs_less_than_matching_it():
    reference_w, reference_l = torch.tensor([0.6]), torch.tensor([0.4])
    matching = dpo.dpo_loss(torch.tensor([0.6]), torch.tensor([0.4]), reference_w, reference_l, 1.0)
    beating = dpo.dpo_loss(torch.tensor([0.9]), torch.tensor([0.1]), reference_w, reference_l, 1.0)
    assert beating.item() < matching.item()


def test_fit_learns_to_rank_the_chosen_item_above_the_rejected_one():
    # One-hot features make the two sides separable; the reference gives no help (equal scores).
    pairs = [_pair([1.0, 0.0], [0.0, 1.0], 0.5, 0.5) for _ in range(8)]
    policy = dpo.fit(pairs, beta=1.0, epochs=400, hidden=8, lr=0.05)
    assert policy.score_one([1.0, 0.0]) > policy.score_one([0.0, 1.0])


def test_fit_is_deterministic_for_a_fixed_seed():
    pairs = [_pair([1.0, 0.0], [0.0, 1.0], 0.5, 0.5) for _ in range(4)]
    a = dpo.fit(pairs, beta=1.0, epochs=20, hidden=8, lr=0.05, seed=11)
    b = dpo.fit(pairs, beta=1.0, epochs=20, hidden=8, lr=0.05, seed=11)
    assert a.score_one([1.0, 0.0]) == pytest.approx(b.score_one([1.0, 0.0]), abs=1e-9)


def test_fit_rejects_an_empty_pair_list():
    with pytest.raises(ValueError, match="no preference pairs"):
        dpo.fit([], beta=1.0)


def test_pairwise_accuracy_counts_strict_wins():
    assert dpo.pairwise_accuracy([1.0, 0.0, 0.5], [0.0, 1.0, 0.5]) == pytest.approx(1 / 3)
    assert dpo.pairwise_accuracy([], []) is None


def test_score_many_handles_an_empty_batch():
    pairs = [_pair([1.0, 0.0], [0.0, 1.0], 0.5, 0.5)]
    policy = dpo.fit(pairs, beta=1.0, epochs=5, hidden=4, lr=0.05)
    assert policy.score_many([]) == []


import ope_support
import post_train_dpo


def _joined_fixture(n_slates=12):
    """Replay events and slates that agree on requestId, with a genuine preference: m1 wins."""
    events, slates = [], []
    for n in range(n_slates):
        rid = f"r{n}"
        events.append(_event(rid, f"u{n}", "m1",
                             [("m1", 0.8, 0.7), ("m2", 0.2, 0.3), ("m3", 0.1, 0.2)]))
        slates.append(_slate(rid, f"u{n}",
                             [("m1", 1, 0, 1.0), ("m2", 0, 0, 0.0), ("m3", 0, 0, 0.0)]))
    return slates, events


def test_split_pairs_uses_the_ope_held_out_hash():
    slates, events = _joined_fixture()
    pairs, _ = slate_pairs.build_pairs(slates, events)
    train, held_out = post_train_dpo.split_pairs(pairs)
    assert train and held_out
    assert all(not ope_eval_report.is_test(p.request_id) for p in train)
    assert all(ope_eval_report.is_test(p.request_id) for p in held_out)
    assert len(train) + len(held_out) == len(pairs)


def test_reference_accuracy_reports_what_the_logging_policy_already_knew():
    slates, events = _joined_fixture()
    pairs, _ = slate_pairs.build_pairs(slates, events)
    # m1's predictionScore (0.7) beats both rejected items, so the reference is already perfect.
    assert post_train_dpo.reference_pairwise_accuracy(pairs) == pytest.approx(1.0)


def test_score_events_writes_the_dpo_key_onto_every_candidate():
    slates, events = _joined_fixture()
    names = ope_eval_report.feature_names(events)
    pairs, _ = slate_pairs.build_pairs(slates, events, names)
    policy = dpo.fit(pairs, beta=1.0, epochs=20, hidden=8)
    scored = post_train_dpo.score_events(events, names, policy)
    for event in scored:
        for candidate in event["actionSpace"]:
            assert ope_eval_report.DPO_PRED_KEY in candidate["modelPredictions"]


def test_the_dpo_key_is_registered_as_policy_only():
    """A policy's own score must never become a reward-model feature."""
    assert ope_eval_report.DPO_PRED_KEY in ope_eval_report.POLICY_ONLY_PRED_KEYS
    slates, events = _joined_fixture()
    names_before = ope_eval_report.feature_names(events)
    pairs, _ = slate_pairs.build_pairs(slates, events, names_before)
    policy = dpo.fit(pairs, beta=1.0, epochs=20, hidden=8)
    scored = post_train_dpo.score_events(events, names_before, policy)
    assert ope_eval_report.feature_names(scored) == names_before
    assert f"model:{ope_eval_report.DPO_PRED_KEY}" in ope_eval_report.policy_names(scored)


def test_score_events_survives_a_null_model_predictions():
    slates, events = _joined_fixture(n_slates=2)
    events[0]["actionSpace"][0]["modelPredictions"] = None
    names = ope_eval_report.feature_names(events)
    pairs, _ = slate_pairs.build_pairs(slates, events, names)
    policy = dpo.fit(pairs, beta=1.0, epochs=10, hidden=8)
    scored = post_train_dpo.score_events(events, names, policy)
    assert ope_eval_report.DPO_PRED_KEY in scored[0]["actionSpace"][0]["modelPredictions"]


def test_main_writes_a_scored_parquet_the_ope_harness_can_read(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    slates, events = _joined_fixture()
    replay_path = tmp_path / "replay.parquet"
    slate_path = tmp_path / "slates.parquet"
    pd.DataFrame(events).to_parquet(replay_path, index=False)
    pd.DataFrame(slates).to_parquet(slate_path, index=False)
    destination = tmp_path / "scored.parquet"

    result = post_train_dpo.main([
        "--parquet", str(replay_path),
        "--slates", str(slate_path),
        "--output-parquet", str(destination),
        "--epochs", "20",
    ])

    assert destination.exists()
    reloaded = ope_support.load_from_parquet(destination)
    assert f"model:{ope_eval_report.DPO_PRED_KEY}" in ope_eval_report.policy_names(reloaded)
    assert result["n_pairs"] > 0
    assert result["n_dropped_pairs"] == 0


def test_main_reports_dropped_pairs_rather_than_hiding_them(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    slates, events = _joined_fixture()
    # A slate item with no replay row on either side of the pair.
    slates[0]["items"].append({"position": 3, "item_id": "m9", "clicked": 0, "ordered": 0,
                               "label": 0.0})
    replay_path = tmp_path / "replay.parquet"
    slate_path = tmp_path / "slates.parquet"
    pd.DataFrame(events).to_parquet(replay_path, index=False)
    pd.DataFrame(slates).to_parquet(slate_path, index=False)

    result = post_train_dpo.main([
        "--parquet", str(replay_path), "--slates", str(slate_path), "--epochs", "10",
    ])

    assert result["n_dropped_pairs"] == 1
