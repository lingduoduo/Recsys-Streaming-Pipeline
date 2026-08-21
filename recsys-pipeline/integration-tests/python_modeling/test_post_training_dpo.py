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
