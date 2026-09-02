#!/usr/bin/env python3
"""Offline evaluation of the GRPO policy against held-out slates -- the flip criterion itself.

The design's promotion rule for `RECSYS_GRPO_MODE` is "shadow scores must rank held-out slates
better than the reference". Shadow mode's own log line (`GrpoPolicyScorer.recordShadowSlate`) does
not answer that: `pairwiseConcordance` measures agreement with the served order, which is
uninformative in both directions -- 1.0 means GRPO reproduces what already shipped (flipping
changes nothing) and 0.0 means it reverses it (which could be better OR worse; concordance alone
cannot say). No value of it justifies the flip.

The real question is answerable offline, from data that already exists. Every `training_samples`
row carries an `item_features` map with `grpo_x` (the packed GRPO feature vector, "v1:0.1,0.2,...")
and `prediction_score` (the behavior policy's logit, the baseline to beat), plus `label`,
`request_id`, `user_id`, `item_id` at the row level. GRPO's score is just `w . x` -- no serving
change needed. This CLI pins ONE weight vector (from the live `grpo:policy:weights` Redis hash, or
a `--weights` file for a reproducible run), scores every held-out slate with it, and reports
pairwise AUC against realized labels for grpoScore and prediction_score side by side.

    python3 grpo_offline_eval.py --parquet training_samples.parquet
    python3 grpo_offline_eval.py --parquet training_samples.parquet --weights pinned_weights.json

Scoring is restricted to the held-out split (`ope_eval_report.is_test`, the same 1-in-5 md5 hash
`post_train_dpo.py` and `post_train_q.py` fit their arms around) -- scoring everything would hand
GRPO an in-sample advantage the reference does not get.

A weight vector fit against one feature layout applied to a different one produces a
plausible-looking score that means nothing; `GrpoPolicyScorer` (Java, serving) and
`GrpoWeightStore`/`GrpoSlates` (Scala, training) both refuse rather than guess, and so does this.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

# The sibling modules live one directory up; this script is run directly, not imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ope_eval_report
import ope_support


# ── wire format: "v1:0.1,0.2,..." -> (version, [floats]) ─────────────────────────
def parse_packed_vector(packed):
    """Split a packed GRPO feature vector into (version, values), or None if malformed.

    Mirrors the parse both other implementations of this wire format perform:
    `GrpoSlates.parseFeatureVector` (Scala, spark-streaming-job) and the packing
    `GrpoFeatures.pack` (Java, java-retrieval-service) writes. Both split on the first ":" and then
    on "," in ENCOUNTERED order -- no reordering, no sorting -- so the ith token here is the same
    feature `GrpoPolicyScorer.dot` calls x[i]. Index 0 is the bias term, index 8 the
    position-in-slate normalized by slate size; this function does not need to know that, but the
    ordering it preserves is exactly what makes those indices mean the same thing on both sides.
    """
    if not packed or ":" not in packed:
        return None
    version, _, rest = packed.partition(":")
    parts = rest.split(",") if rest else []
    try:
        return version, [float(p) for p in parts]
    except ValueError:
        return None


def dot(weights, x) -> float:
    """w . x, index-for-index -- the same computation as GrpoPolicyScorer.dot (Java)."""
    return sum(w * v for w, v in zip(weights, x))


# GrpoFeatures.of (Java, java-retrieval-service) index 8 is position/slateSize -- the slot the
# item occupies in what was ALREADY served. A score used to decide selection cannot depend on the
# position selection itself produces: selection runs the scorer to decide who goes where, so the
# position does not exist yet at the moment the score would need it. GrpoPolicyScorer.
# recordShadowSlate documents exactly this ("modelPredictions ... is built before selection
# assigns positions") for why it never persists grpoScore onto the pre-selection prediction. Any
# AUC weight on this index buys measures agreement with the order that was already served, not
# whether GRPO would rank a fresh, not-yet-positioned slate better.
POSITION_FEATURE_INDEX = 8


def _position_free_weights(weights):
    """weights with the position-feature weight zeroed out -- see POSITION_FEATURE_INDEX."""
    if len(weights) <= POSITION_FEATURE_INDEX:
        return list(weights)
    zeroed = list(weights)
    zeroed[POSITION_FEATURE_INDEX] = 0.0
    return zeroed


# ── weights: Redis hash (live) or a JSON file with the same three fields (pinned) ─
def parse_weight_fields(fields: dict):
    """(feature_version, [weights]) from a dict shaped like the `grpo:policy:weights` Redis hash
    (or an equivalent JSON file): {"weights": "0.1,0.2,...", "dim": ..., "feature_version": ...}.

    The `dim` field is metadata only -- like `GrpoWeightStore.decode` on the Scala side, the actual
    parsed weight COUNT is the thing checked against the feature vector width, not the string that
    claims what it should be.
    """
    version = fields.get("feature_version")
    raw = str(fields.get("weights") or "")
    parts = raw.split(",") if raw else []
    try:
        weights = [float(p) for p in parts]
    except ValueError as e:
        raise SystemExit(f"unparseable weights '{raw}': {e}")
    # float() parses "nan"/"inf" without raising -- a diverged training run would otherwise be
    # read back as a "valid" vector whose every dot product comes out NaN. NaN compares False
    # against everything, so every pairwise comparison in evaluate_slates loses and the tool
    # prints grpo_auc=0.0 -- reading as "GRPO is maximally worse" when the vector is simply
    # unusable. GrpoPolicyScorer.readWeights (Java) checks Double.isFinite per weight for the
    # same reason; this is the offline side of that guard.
    for i, w in enumerate(weights):
        if not math.isfinite(w):
            raise SystemExit(f"weight at index {i} is not finite ('{parts[i]}') -- refusing to score")
    return version, weights


def load_weight_fields_from_file(path) -> dict:
    with open(path) as fh:
        return json.load(fh)


def load_weight_fields_from_redis(key: str) -> dict:
    import redis
    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    fields = client.hgetall(key)
    if not fields:
        raise SystemExit(f"no weights at Redis hash '{key}' -- nothing to score with")
    return fields


# ── feature-schema detection and the version/width guard ─────────────────────────
def _as_feature_map(raw) -> dict:
    """`item_features` normalized to a dict.

    A genuine Spark `Map[String, String]` column round-trips through pandas/pyarrow as a list of
    (key, value) tuples, not a dict -- while a dict built directly in Python (as this module's own
    test fixtures do) round-trips as a dict already. `dict()` accepts either shape via its
    two-tuple-iterable constructor; `None` (an absent nested map, e.g. `coalesce(..., empty map)`
    in OnlineJoinerStreamingJob) becomes an empty dict rather than raising.
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    return dict(raw)


def detect_feature_schema(rows):
    """(version, dim) the MAJORITY of parseable rows agree on -- the schema the data mostly
    carries, which the pinned weights are then checked against (not the other way around).

    Pinning off the first parseable row instead would let one stale-version row -- a late
    backfill, a run boundary -- abort an otherwise-fine evaluation just because it happened to
    sort first. Ties fall back to whichever schema was seen first, the same determinism the
    first-row approach gave callers when there was nothing to disagree about."""
    counts = Counter()
    first_seen_at = {}
    for idx, row in enumerate(rows):
        features = _as_feature_map(row.get("item_features"))
        parsed = parse_packed_vector(features.get("grpo_x"))
        if parsed is None:
            continue
        version, vector = parsed
        schema = (version, len(vector))
        counts[schema] += 1
        first_seen_at.setdefault(schema, idx)
    if not counts:
        raise SystemExit("no training_samples row carries a parseable grpo_x -- nothing to score")
    max_count = max(counts.values())
    majority = [schema for schema, n in counts.items() if n == max_count]
    return min(majority, key=lambda schema: first_seen_at[schema])


def assert_feature_schema_matches(rows_version, rows_dim, weights_version, weights):
    """Refuse rather than score on a mismatch -- see module docstring. Applying weights fit
    against one feature layout to another produces a plausible-looking number that means nothing,
    and nothing downstream could tell the difference; this is the offline side of the same guard
    `GrpoPolicyScorer.readWeights` (Java) and `GrpoWeightStore.decode` (Scala) already enforce."""
    if weights_version != rows_version:
        raise SystemExit(
            f"weights feature_version '{weights_version}' does not match the training rows' "
            f"grpo_x prefix '{rows_version}' -- refusing to score")
    if len(weights) != rows_dim:
        raise SystemExit(
            f"weight count {len(weights)} does not match the grpo_x vector width {rows_dim} "
            "found in the rows -- refusing to score")


# ── scoring: held-out split, both arms, dropped-row accounting ───────────────────
def build_scored_rows(rows, weights, version, dim):
    """One scored record per usable held-out training_samples row.

    Held-out only (`ope_eval_report.is_test`): scoring the training split too would give GRPO an
    in-sample advantage prediction_score does not get, exactly the reasoning post_train_dpo.py and
    post_train_q.py already document for their own arms.

    A row missing grpo_x, missing prediction_score, missing label, whose grpo_x parses to a
    version/width other than the pinned schema, or whose label is NaN or negative (fitting neither
    the positive nor the negative bucket evaluate_slates sorts on) is DROPPED and counted -- not
    zero-filled, which would silently bias the comparison in whatever direction the missing data
    happens to skew.
    """
    position_free_weights = _position_free_weights(weights)
    scored = []
    n_dropped = 0
    for row in rows:
        request_id = row.get("request_id")
        if not ope_eval_report.is_test(request_id):
            continue
        features = _as_feature_map(row.get("item_features"))
        parsed = parse_packed_vector(features.get("grpo_x"))
        pred_raw = features.get("prediction_score")
        label_raw = row.get("label")
        if parsed is None or pred_raw is None or label_raw is None:
            n_dropped += 1
            continue
        row_version, vector = parsed
        if row_version != version or len(vector) != dim:
            n_dropped += 1
            continue
        try:
            prediction_score = float(pred_raw)
            label = float(label_raw)
        except (TypeError, ValueError):
            n_dropped += 1
            continue
        # evaluate_slates buckets labels into positive (>0) and negative (==0) only; a NaN or
        # negative label fits neither bucket and would otherwise contribute no pairs while still
        # being counted "usable" -- an invisible drop, the same class of silent bias the
        # missing-field checks above exist to avoid. `label >= 0.0` is False for both NaN and
        # negatives, so one comparison catches both.
        if not (label >= 0.0):
            n_dropped += 1
            continue
        scored.append({
            "request_id": request_id,
            "grpo_score": dot(weights, vector),
            "grpo_score_position_free": dot(position_free_weights, vector),
            "prediction_score": prediction_score,
            "label": label,
        })
    return scored, n_dropped


def evaluate_slates(scored_rows):
    """Pairwise AUC for grpoScore and prediction_score, over the IDENTICAL (positive, negative)
    pairs of each usable slate, aggregated (micro-averaged) across slates rather than averaging
    per-slate AUCs -- so a handful of large slates isn't diluted by many small ones, and the two
    scores are compared on exactly the same denominator.

    A slate with no positive (label > 0) or no negative (label == 0) label contributes no pairs --
    there is nothing to rank against -- so it is counted in `n_slates_considered` but not
    `n_usable_slates`. At low click-through most slates will be unusable; that count is the
    caller's signal not to read a handful of slates' AUC as a verdict.
    """
    by_slate = defaultdict(list)
    for row in scored_rows:
        by_slate[row["request_id"]].append(row)

    n_pairs = 0
    concordant = {"grpo_score": 0.0, "grpo_score_position_free": 0.0, "prediction_score": 0.0}
    n_usable = 0
    for slate_rows in by_slate.values():
        positives = [r for r in slate_rows if r["label"] > 0.0]
        negatives = [r for r in slate_rows if r["label"] == 0.0]
        if not positives or not negatives:
            continue
        n_usable += 1
        for p in positives:
            for n in negatives:
                n_pairs += 1
                for key in concordant:
                    if p[key] > n[key]:
                        concordant[key] += 1.0
                    elif p[key] == n[key]:
                        concordant[key] += 0.5

    def _auc(key):
        return (concordant[key] / n_pairs) if n_pairs else None

    return {
        "n_slates_considered": len(by_slate),
        "n_usable_slates": n_usable,
        "n_pairs": n_pairs,
        "grpo_auc": _auc("grpo_score"),
        "grpo_auc_position_free": _auc("grpo_score_position_free"),
        "prediction_auc": _auc("prediction_score"),
    }


# ── CLI ────────────────────────────────────────────────────────────────────────
def _format(value, signed=False):
    if value is None:
        return "n/a"
    return f"{value:+.4f}" if signed else f"{value:.4f}"


def main(argv=None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", required=True,
                        help="training_samples Parquet dump (OnlineJoinerStreamingJob output)")
    parser.add_argument("--weights", default=None,
                        help="JSON file with {weights, dim, feature_version} for a reproducible, "
                             "pinned evaluation; default reads the live grpo:policy:weights "
                             "Redis hash")
    parser.add_argument("--weights-key", default="grpo:policy:weights",
                        help="Redis hash key, when --weights is not given")
    args = parser.parse_args(argv)

    rows = ope_support.load_from_parquet(args.parquet)
    if not rows:
        raise SystemExit("no training_samples rows -- nothing to evaluate")

    rows_version, rows_dim = detect_feature_schema(rows)
    weight_fields = (load_weight_fields_from_file(args.weights) if args.weights
                     else load_weight_fields_from_redis(args.weights_key))
    weights_version, weights = parse_weight_fields(weight_fields)
    assert_feature_schema_matches(rows_version, rows_dim, weights_version, weights)

    scored_rows, n_dropped = build_scored_rows(rows, weights, rows_version, rows_dim)
    if not scored_rows:
        raise SystemExit(
            f"no held-out row carries a usable grpo_x/prediction_score/label -- {n_dropped} "
            "held-out rows dropped for missing or malformed data")

    result = evaluate_slates(scored_rows)

    def _diff(grpo_key):
        return (result[grpo_key] - result["prediction_auc"]
                if result[grpo_key] is not None and result["prediction_auc"] is not None
                else None)

    auc_diff = _diff("grpo_auc")
    auc_diff_position_free = _diff("grpo_auc_position_free")

    summary = {
        "feature_version": rows_version,
        "feature_dim": rows_dim,
        "n_rows_usable": len(scored_rows),
        "n_rows_dropped": n_dropped,
        "auc_diff": auc_diff,
        "auc_diff_position_free": auc_diff_position_free,
        **result,
    }

    print(f"feature_version={rows_version} dim={rows_dim}; "
          f"{summary['n_rows_usable']} held-out rows usable, "
          f"{summary['n_rows_dropped']} dropped for missing/malformed data")
    print(f"held-out slates: {result['n_usable_slates']} usable of "
          f"{result['n_slates_considered']} considered ({result['n_pairs']} "
          "(positive, negative) pairs) -- a slate with no positive or no negative label "
          "contributes no pairs")
    if result["n_pairs"] == 0:
        print("no usable slate: no pairwise AUC can be computed from this data")
    else:
        # auc_diff (raw) includes index 8, position/slateSize -- the served slot, which cannot be
        # realized at serve time because the score has to exist BEFORE selection assigns
        # positions (see POSITION_FEATURE_INDEX). The position-free number, with that weight
        # zeroed, is what a live scorer could actually achieve, so it -- not the raw diff -- is
        # the one the shadow-to-on flip decision should read.
        print(f"pairwise AUC (raw, includes position/slateSize -- NOT realizable at serve time): "
              f"grpoScore={_format(result['grpo_auc'])} "
              f"prediction_score={_format(result['prediction_auc'])} "
              f"diff={_format(auc_diff, signed=True)}")
        print(f"pairwise AUC (position-free -- THE FLIP CRITERION): "
              f"grpoScore={_format(result['grpo_auc_position_free'])} "
              f"prediction_score={_format(result['prediction_auc'])} "
              f"diff={_format(auc_diff_position_free, signed=True)}")
        if (auc_diff is not None and auc_diff_position_free is not None
                and ((auc_diff > 0 and auc_diff_position_free < 0)
                     or (auc_diff < 0 and auc_diff_position_free > 0))):
            print("WARNING: raw and position-free diffs disagree in sign -- the apparent "
                  "improvement in the raw number is position bias, not a better-ranking policy; "
                  "read the position-free diff")
    print("caution: a number computed from a handful of slates is not a verdict -- read "
          "n_usable_slates before treating the AUCs as evidence for the shadow-to-on flip")
    return summary


if __name__ == "__main__":
    main()
