import json
import sys
from pathlib import Path

import pytest

_MODELING = Path(__file__).parents[2] / "services" / "python-modeling"
sys.path.insert(0, str(_MODELING))
sys.path.insert(0, str(_MODELING / "post-training"))

import ope_eval_report

import grpo_offline_eval as goe


# ── fixtures ─────────────────────────────────────────────────────────────────────
def _packed(values):
    return "v2:" + ",".join(str(v) for v in values)


def _row(request_id, item_id, label, grpo_x=None, prediction_score="0.5", features=None):
    """One training_samples row: item_features carries grpo_x/prediction_score as strings,
    exactly as OnlineJoinerStreamingJob writes them (Map[String, String])."""
    item_features = {} if features is None else dict(features)
    if grpo_x is not None:
        item_features["grpo_x"] = grpo_x
    if prediction_score is not None:
        item_features["prediction_score"] = prediction_score
    return {
        "request_id": request_id,
        "user_id": f"u-{request_id}",
        "item_id": item_id,
        "label": label,
        "item_features": item_features,
    }


# A held-out and a non-held-out request_id, picked once and reused everywhere so fixtures don't
# have to re-derive the md5 split each time.
_HELD_OUT_RID = "r9"          # ope_eval_report.is_test("r9") is True
_TRAIN_ONLY_RID = "r1"        # ope_eval_report.is_test("r1") is False
assert ope_eval_report.is_test(_HELD_OUT_RID)
assert not ope_eval_report.is_test(_TRAIN_ONLY_RID)

# v2 (GrpoFeatures.DIM, java-retrieval-service) is 9-wide -- one less than v1, which carried a
# served-position feature that could not be realized at serve time (see grpo_offline_eval's module
# docstring). This fixture is 9-wide so it matches what the live scorer actually produces.
_NINE_X = _packed([1.0, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])


def _one_slate_fixture():
    """One held-out slate: m1 clicked (label 1.0), m2 and m3 shown-not-clicked (label 0.0).
    grpoScore favors m1 over both; prediction_score favors m2 instead, so the two arms disagree
    and neither AUC will read 1.0 or 0.0 by accident."""
    rows = [
        _row(_HELD_OUT_RID, "m1", 1.0, _packed([1.0] * 9), prediction_score="0.1"),
        _row(_HELD_OUT_RID, "m2", 0.0, _packed([0.0] * 9), prediction_score="0.9"),
        _row(_HELD_OUT_RID, "m3", 0.0, _packed([0.0] * 9), prediction_score="0.2"),
    ]
    weights = [1.0] * 9
    return rows, weights


# ── parse_packed_vector ──────────────────────────────────────────────────────────
def test_parse_packed_vector_splits_version_and_floats():
    assert goe.parse_packed_vector("v2:1.0,2.0,3.0") == ("v2", [1.0, 2.0, 3.0])


def test_parse_packed_vector_returns_none_without_a_colon():
    assert goe.parse_packed_vector("1.0,2.0,3.0") is None


def test_parse_packed_vector_returns_none_on_unparseable_floats():
    assert goe.parse_packed_vector("v2:1.0,not-a-number") is None


def test_parse_packed_vector_returns_none_for_missing_value():
    assert goe.parse_packed_vector(None) is None
    assert goe.parse_packed_vector("") is None


# ── the cross-implementation fixture: dot() must match GrpoPolicyScorer.dot ──────
def test_dot_matches_hand_computed_value_from_the_java_scorer_ordering():
    """Pinned fixture: weights = [1..9], grpo_x packed as their exact reverse.

    dot = sum_i w[i] * x[i] = 1*9 + 2*8 + 3*7 + 4*6 + 5*5 + 6*4 + 7*3 + 8*2 + 9*1
        = 9+16+21+24+25+24+21+16+9 = 165.0

    GrpoPolicyScorer.dot (java-retrieval-service/.../grpo/GrpoPolicyScorer.java) computes
    `sum += w[i] * x[i]` for i in [0, GrpoFeatures.DIM) over the SAME comma-separated, index-order-
    preserving parse of the "v2:..." wire format (GrpoFeatures.pack / GrpoSlates.parseFeatureVector
    on the Scala side). Because both sides parse by splitting on "," in encountered order and zip
    index-for-index against the weights, this fixture is reversed on purpose: an index reversal bug
    would land on sum-of-squares (1^2+..+9^2 = 285), and an off-by-one shift (pairing w[i] against
    x[i+1] instead of x[i]) would land on 120 -- both distinct from the correct 165, so either class
    of bug is caught rather than accidentally matching.
    """
    weights = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    version, x = goe.parse_packed_vector("v2:9.0,8.0,7.0,6.0,5.0,4.0,3.0,2.0,1.0")
    assert version == "v2"
    assert goe.dot(weights, x) == pytest.approx(165.0)


# ── feature-schema detection and the version/width guard ─────────────────────────
def test_detect_feature_schema_reads_version_and_dim_from_first_parseable_row():
    rows = [_row(_TRAIN_ONLY_RID, "m1", 0.0, grpo_x=None, prediction_score=None),
            _row(_TRAIN_ONLY_RID, "m2", 1.0, _NINE_X)]
    version, dim = goe.detect_feature_schema(rows)
    assert version == "v2"
    assert dim == 9


def test_detect_feature_schema_raises_when_no_row_has_grpo_x():
    rows = [_row(_TRAIN_ONLY_RID, "m1", 0.0, grpo_x=None, prediction_score="0.1")]
    with pytest.raises(SystemExit, match="grpo_x"):
        goe.detect_feature_schema(rows)


def test_refuses_on_a_feature_version_mismatch():
    # The exact cutover hazard: v1 weights left behind after the v2 rollout must be refused, not
    # silently applied against the v2 (9-wide) feature layout.
    with pytest.raises(SystemExit, match="feature_version"):
        goe.assert_feature_schema_matches(
            rows_version="v2", rows_dim=9, weights_version="v1", weights=[0.0] * 9)


def test_refuses_on_a_weight_width_mismatch():
    with pytest.raises(SystemExit, match="width|count"):
        goe.assert_feature_schema_matches(
            rows_version="v2", rows_dim=9, weights_version="v2", weights=[0.0] * 3)


def test_matching_schema_does_not_raise():
    goe.assert_feature_schema_matches(
        rows_version="v2", rows_dim=9, weights_version="v2", weights=[0.0] * 9)


def test_detect_feature_schema_picks_the_majority_schema_over_a_stale_leading_row():
    """A single leading row on a stale/different version must not abort an otherwise-fine run --
    the schema should be the one the data mostly agrees on, not whichever row happens to sort
    first."""
    stale = _row(_TRAIN_ONLY_RID, "m0", 0.0, "v0:0.0,0.0,0.0")
    rows = [stale] + [_row(_TRAIN_ONLY_RID, f"m{i}", 0.0, _NINE_X) for i in range(5)]
    version, dim = goe.detect_feature_schema(rows)
    assert version == "v2"
    assert dim == 9


# ── build_scored_rows: held-out split + data-availability guard ──────────────────
def test_build_scored_rows_keeps_only_the_held_out_split():
    rows = [
        _row(_HELD_OUT_RID, "m1", 1.0, _NINE_X),
        _row(_TRAIN_ONLY_RID, "m2", 1.0, _NINE_X),
    ]
    scored, dropped = goe.build_scored_rows(rows, weights=[0.1] * 9, version="v2", dim=9)
    assert [r["request_id"] for r in scored] == [_HELD_OUT_RID]
    assert dropped == 0


def test_build_scored_rows_drops_and_counts_rows_missing_grpo_x_or_prediction_score_or_label():
    rows = [
        _row(_HELD_OUT_RID, "m1", 1.0, grpo_x=None, prediction_score="0.5"),   # missing grpo_x
        _row(_HELD_OUT_RID, "m2", 1.0, _NINE_X, prediction_score=None),       # missing pred score
        _row(_HELD_OUT_RID, "m3", None, _NINE_X, prediction_score="0.5"),     # missing label
        _row(_HELD_OUT_RID, "m4", 1.0, _NINE_X, prediction_score="0.5"),      # usable
    ]
    scored, dropped = goe.build_scored_rows(rows, weights=[0.1] * 9, version="v2", dim=9)
    assert len(scored) == 1
    assert scored[0]["request_id"] == _HELD_OUT_RID
    assert dropped == 3


def test_build_scored_rows_drops_a_row_whose_grpo_x_version_does_not_match_the_pinned_schema():
    # The exact cutover hazard: a v1 grpo_x row left behind in training_samples after the v2
    # rollout must be dropped, not silently scored against the pinned v2 schema.
    rows = [_row(_HELD_OUT_RID, "m1", 1.0, "v1:" + ",".join(["0.1"] * 9), prediction_score="0.5")]
    scored, dropped = goe.build_scored_rows(rows, weights=[0.1] * 9, version="v2", dim=9)
    assert scored == []
    assert dropped == 1


def test_build_scored_rows_drops_a_row_whose_grpo_x_width_does_not_match_the_pinned_schema():
    rows = [_row(_HELD_OUT_RID, "m1", 1.0, "v2:" + ",".join(["0.1"] * 5), prediction_score="0.5")]
    scored, dropped = goe.build_scored_rows(rows, weights=[0.1] * 9, version="v2", dim=9)
    assert scored == []
    assert dropped == 1


def test_build_scored_rows_counts_a_nan_label_in_n_dropped():
    """A NaN label is neither a click (label > 0) nor shown-not-clicked (label == 0) -- it fits
    neither bucket evaluate_slates sorts on, so it must be dropped and counted, not silently kept
    as a "usable" row that happens to contribute no pairs."""
    rows = [_row(_HELD_OUT_RID, "m1", float("nan"), _NINE_X, prediction_score="0.5")]
    scored, dropped = goe.build_scored_rows(rows, weights=[0.1] * 9, version="v2", dim=9)
    assert scored == []
    assert dropped == 1


def test_build_scored_rows_counts_a_negative_label_in_n_dropped():
    rows = [_row(_HELD_OUT_RID, "m1", -1.0, _NINE_X, prediction_score="0.5")]
    scored, dropped = goe.build_scored_rows(rows, weights=[0.1] * 9, version="v2", dim=9)
    assert scored == []
    assert dropped == 1


def test_build_scored_rows_computes_grpo_score_as_the_pinned_dot_product():
    rows = [_row(_HELD_OUT_RID, "m1", 1.0, "v2:9.0,8.0,7.0,6.0,5.0,4.0,3.0,2.0,1.0",
                prediction_score="0.5")]
    weights = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    scored, _ = goe.build_scored_rows(rows, weights=weights, version="v2", dim=9)
    assert scored[0]["grpo_score"] == pytest.approx(165.0)
    assert scored[0]["prediction_score"] == pytest.approx(0.5)


def test_build_scored_rows_handles_a_real_map_column_round_tripped_through_parquet(tmp_path):
    """A genuine Spark map<string,string> column round-trips through pandas/pyarrow as a list of
    (key, value) tuples, not a dict -- the same class of gotcha the DPO tests hit for arrays.
    A synthetic pd.DataFrame(dict) fixture would NOT reproduce it (dicts round-trip as dicts), so
    this test builds the parquet file the way pyarrow actually encodes a map column."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    pd = pytest.importorskip("pandas")

    features = [("grpo_x", _NINE_X), ("prediction_score", "0.5")]
    arr = pa.array([features], type=pa.map_(pa.string(), pa.string()))
    table = pa.table({
        "request_id": [_HELD_OUT_RID],
        "user_id": ["u1"],
        "item_id": ["m1"],
        "label": [1.0],
        "item_features": arr,
    })
    path = tmp_path / "training_samples.parquet"
    pq.write_table(table, path)

    rows = pd.read_parquet(path).to_dict(orient="records")
    assert isinstance(rows[0]["item_features"], list)  # confirms the gotcha is really exercised

    scored, dropped = goe.build_scored_rows(rows, weights=[0.1] * 9, version="v2", dim=9)
    assert dropped == 0
    assert len(scored) == 1
    assert scored[0]["prediction_score"] == pytest.approx(0.5)


# ── evaluate_slates: pairwise AUC, ties, usable-slate accounting ─────────────────
def test_evaluate_slates_ignores_a_slate_with_no_positive_label():
    scored = [{"request_id": "r1", "grpo_score": 1.0, "prediction_score": 1.0, "label": 0.0},
              {"request_id": "r1", "grpo_score": 0.5, "prediction_score": 0.5, "label": 0.0}]
    result = goe.evaluate_slates(scored)
    assert result["n_usable_slates"] == 0
    assert result["n_slates_considered"] == 1
    assert result["n_pairs"] == 0
    assert result["grpo_auc"] is None
    assert result["prediction_auc"] is None


def test_evaluate_slates_ignores_a_slate_with_no_negative_label():
    scored = [{"request_id": "r1", "grpo_score": 1.0, "prediction_score": 1.0, "label": 1.0},
              {"request_id": "r1", "grpo_score": 0.5, "prediction_score": 0.5, "label": 2.0}]
    result = goe.evaluate_slates(scored)
    assert result["n_usable_slates"] == 0
    assert result["n_pairs"] == 0


def test_evaluate_slates_counts_a_tie_as_half_a_pair():
    scored = [{"request_id": "r1", "grpo_score": 1.0, "prediction_score": 1.0, "label": 1.0},
              {"request_id": "r1", "grpo_score": 1.0, "prediction_score": 0.0, "label": 0.0}]
    result = goe.evaluate_slates(scored)
    assert result["n_pairs"] == 1
    assert result["grpo_auc"] == pytest.approx(0.5)     # tied scores
    assert result["prediction_auc"] == pytest.approx(1.0)  # 1.0 > 0.0, correctly ordered


def test_evaluate_slates_aggregates_pairs_across_slates_rather_than_macro_averaging():
    """Slate A: 1 pair, wrong. Slate B: 3 pairs, all correct. Micro-aggregated AUC = 3/4, which
    differs from the macro average of the two per-slate AUCs (0/1 and 3/3 -> 0.5)."""
    scored = [
        {"request_id": "A", "grpo_score": 0.0, "prediction_score": 0.0, "label": 1.0},
        {"request_id": "A", "grpo_score": 1.0, "prediction_score": 0.0, "label": 0.0},
        {"request_id": "B", "grpo_score": 3.0, "prediction_score": 0.0, "label": 1.0},
        {"request_id": "B", "grpo_score": 1.0, "prediction_score": 0.0, "label": 0.0},
        {"request_id": "B", "grpo_score": 2.0, "prediction_score": 0.0, "label": 0.0},
        {"request_id": "B", "grpo_score": 0.5, "prediction_score": 0.0, "label": 0.0},
    ]
    result = goe.evaluate_slates(scored)
    assert result["n_usable_slates"] == 2
    assert result["n_pairs"] == 4
    assert result["grpo_auc"] == pytest.approx(3 / 4)


def test_evaluate_slates_returns_none_auc_when_no_slate_is_usable_at_all():
    scored = [{"request_id": "r1", "grpo_score": 1.0, "prediction_score": 1.0, "label": 0.0}]
    result = goe.evaluate_slates(scored)
    assert result["grpo_auc"] is None
    assert result["prediction_auc"] is None
    assert result["n_pairs"] == 0


# ── weight loading ────────────────────────────────────────────────────────────────
def test_parse_weight_fields_reads_the_redis_hash_shape():
    fields = {"weights": "0.1,0.2,0.3", "dim": "3", "feature_version": "v2"}
    version, weights = goe.parse_weight_fields(fields)
    assert version == "v2"
    assert weights == pytest.approx([0.1, 0.2, 0.3])


@pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity"])
def test_parse_weight_fields_refuses_a_non_finite_weight(bad_value):
    """float() happily parses "NaN"/"Infinity" -- unlike an unparseable string, which already
    raises. A non-finite weight makes every downstream dot product NaN, every comparison in
    evaluate_slates come out False, and the tool print grpo_auc=0.0 -- reading as "GRPO is
    maximally worse" when the weight vector is simply unusable. Both GrpoPolicyScorer.readWeights
    (Java) and this refuse rather than let that happen."""
    fields = {"weights": f"0.1,{bad_value},0.3", "dim": "3", "feature_version": "v2"}
    with pytest.raises(SystemExit, match=r"(?i)finite"):
        goe.parse_weight_fields(fields)


def test_parse_weight_fields_names_the_offending_index_on_a_non_finite_weight():
    fields = {"weights": "0.1,0.2,NaN", "dim": "3", "feature_version": "v2"}
    with pytest.raises(SystemExit, match="index 2"):
        goe.parse_weight_fields(fields)


def test_load_weight_fields_from_a_json_file(tmp_path):
    path = tmp_path / "weights.json"
    path.write_text(json.dumps({"weights": "1.0,2.0", "dim": "2", "feature_version": "v2"}))
    fields = goe.load_weight_fields_from_file(path)
    assert fields == {"weights": "1.0,2.0", "dim": "2", "feature_version": "v2"}


# ── end-to-end main() ──────────────────────────────────────────────────────────────
def _write_training_samples(tmp_path, rows):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    path = tmp_path / "training_samples.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _write_weights_file(tmp_path, weights, version="v2"):
    path = tmp_path / "weights.json"
    path.write_text(json.dumps({
        "weights": ",".join(str(w) for w in weights),
        "dim": str(len(weights)),
        "feature_version": version,
    }))
    return path


def test_main_reports_both_aucs_and_the_usable_slate_counts(tmp_path, capsys):
    rows, weights = _one_slate_fixture()
    parquet_path = _write_training_samples(tmp_path, rows)
    weights_path = _write_weights_file(tmp_path, weights)

    result = goe.main(["--parquet", str(parquet_path), "--weights", str(weights_path)])

    assert result["n_usable_slates"] == 1
    assert result["n_slates_considered"] == 1
    assert result["n_pairs"] == 2
    assert result["grpo_auc"] == pytest.approx(1.0)       # m1 (all 1.0) beats m2/m3 (all 0.0)
    assert result["prediction_auc"] == pytest.approx(0.0)  # m1 (0.1) loses to both m2/m3
    assert result["auc_diff"] == pytest.approx(1.0)

    out = capsys.readouterr().out
    assert "pairwise AUC" in out
    assert "usable" in out


def test_main_refuses_on_a_feature_version_mismatch(tmp_path):
    # The exact cutover hazard, exercised end to end: a --weights file left over from before the
    # v2 rollout (feature_version "v1") must be refused, not scored against v2 training_samples.
    rows, weights = _one_slate_fixture()
    parquet_path = _write_training_samples(tmp_path, rows)
    weights_path = _write_weights_file(tmp_path, weights, version="v1")

    with pytest.raises(SystemExit, match="feature_version"):
        goe.main(["--parquet", str(parquet_path), "--weights", str(weights_path)])


def test_main_refuses_on_a_weight_width_mismatch(tmp_path):
    rows, _ = _one_slate_fixture()
    parquet_path = _write_training_samples(tmp_path, rows)
    weights_path = _write_weights_file(tmp_path, [1.0, 2.0, 3.0])  # wrong width

    with pytest.raises(SystemExit, match="width|count"):
        goe.main(["--parquet", str(parquet_path), "--weights", str(weights_path)])


def test_main_raises_when_no_training_samples_rows(tmp_path):
    parquet_path = _write_training_samples(tmp_path, [])
    weights_path = _write_weights_file(tmp_path, [0.0] * 9)
    with pytest.raises(SystemExit):
        goe.main(["--parquet", str(parquet_path), "--weights", str(weights_path)])
