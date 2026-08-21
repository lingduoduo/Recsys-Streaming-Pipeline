#!/usr/bin/env python3
"""Offline Q-learning post-training over the RL replay buffer.

Fits two Q functions on the logged transitions -- a tabular baseline and a neural Fitted Q
Iteration model -- writes their per-candidate predictions back into the replay events as `tabQ`
and `fqiQ`, and reports the held-out Bellman residual for each.

The scored replay is written as Parquet so that ope_eval_report.py picks the two new policies
up with NO change to that module: its `pick()` already supports a generic `model:{key}` policy
and `policy_names()` discovers keys from candidate modelPredictions.

    REDIS_HOST=localhost python3 post_train_q.py --output-parquet /tmp/scored_replay.parquet
    python3 ../ope_eval_report.py --parquet /tmp/scored_replay.parquet

Both Q functions are fit on the NON-held-out split only, matching how ope_eval_report fits its
reward model. Fitting on everything would hand model:fqiQ an in-sample advantage that the
ctr and popularity baselines do not get.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# The sibling modules live one directory up; this script is run directly, not imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ope_eval_report
import ope_support

import fqi
import replay_dataset
import tabular_q


def split_transitions(transitions):
    """Train/held-out split on the same requestId hash ope_eval_report uses."""
    train = [t for t in transitions if not ope_eval_report.is_test(t.request_id)]
    test = [t for t in transitions if ope_eval_report.is_test(t.request_id)]
    if not train:
        return transitions, transitions
    return train, test


def tabular_td_residual(q, transitions, gamma: float):
    """Mean absolute Bellman error. None when there is nothing held out."""
    if not transitions:
        return None
    total = 0.0
    for transition in transitions:
        target = transition.reward + gamma * tabular_q.max_next_q(q, transition)
        predicted = tabular_q.score(q, transition.state_key, transition.action)
        total += abs(target - predicted)
    return total / len(transitions)


def held_out_coverage(q, transitions):
    """Fraction of held-out (state_key, action) pairs the fitted table actually holds.

    Required reading alongside `tabular_td_residual`: an unseen pair scores exactly 0.0, so its
    residual collapses to |reward| and an EMPTY table scores BETTER than a fitted one on a
    lower-is-better scale. The residual is only interpretable at the coverage reported here.
    """
    if not transitions:
        return None
    covered = sum(1 for t in transitions if (t.state_key, t.action) in q)
    return covered / len(transitions)


def fqi_td_residual(model, transitions, gamma: float):
    """Mean absolute Bellman error for the fitted network."""
    if not transitions:
        return None
    predicted = model.score_many([t.features for t in transitions])
    total = 0.0
    for transition, q_value in zip(transitions, predicted):
        if transition.terminal or not transition.next_features:
            next_value = 0.0
        else:
            next_value = max(model.score_many(transition.next_features))
        total += abs(transition.reward + gamma * next_value - q_value)
    return total / len(transitions)


def score_events(events, names, q, model):
    """Write the two policy-score keys into every candidate's modelPredictions, in place.

    Every key written here must be registered in ope_eval_report.POLICY_ONLY_PRED_KEYS, or the
    reward model will be fit on the scores it is then used to grade.
    """
    rows, targets = [], []
    for event in events:
        state = replay_dataset.state_key(event.get("state"))
        for candidate in replay_dataset.as_list(event.get("actionSpace")):
            rows.append(ope_eval_report._vec(candidate, names))
            targets.append((candidate, state))
    fqi_scores = model.score_many(rows)
    for (candidate, state), q_value in zip(targets, fqi_scores):
        # setdefault returns the STORED value when the key is present, so a null
        # modelPredictions -- which is what Parquet yields for an absent nested struct --
        # would come back as None and the assignment below would raise.
        predictions = candidate.get("modelPredictions")
        if predictions is None:
            predictions = candidate["modelPredictions"] = {}
        # Key names come from ope_eval_report so the producer and the schema-exclusion list cannot
        # drift apart; see POLICY_ONLY_PRED_KEYS there for why that matters.
        predictions[ope_eval_report.TABULAR_Q_PRED_KEY] = tabular_q.score(
            q, state, str(candidate.get("item")))
        predictions[ope_eval_report.FQI_Q_PRED_KEY] = float(q_value)
    return events


def _load_events(args):
    if args.parquet:
        return ope_support.load_from_parquet(args.parquet)
    import redis
    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        decode_responses=False,
    )
    return ope_support.load_from_redis(client, args.key, args.limit)


def _format(value):
    return "n/a" if value is None else f"{value:.4f}"


def main(argv=None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--key", default="replay:recommendations")
    parser.add_argument("--parquet", default=None,
                        help="read the replay from a Parquet dump instead of Redis")
    parser.add_argument("--limit", type=int, default=-1)
    parser.add_argument("--gamma", type=float, default=tabular_q.DEFAULT_GAMMA)
    parser.add_argument("--session-gap-minutes", type=float, default=30.0)
    parser.add_argument("--sweeps", type=int, default=tabular_q.DEFAULT_SWEEPS)
    parser.add_argument("--fqi-iterations", type=int, default=fqi.DEFAULT_ITERATIONS)
    parser.add_argument("--fqi-epochs", type=int, default=fqi.DEFAULT_EPOCHS)
    parser.add_argument("--seed", type=int, default=fqi.SEED)
    parser.add_argument("--output-parquet", default=None,
                        help="write the scored replay here for ope_eval_report.py --parquet")
    args = parser.parse_args(argv)

    events = [e for e in _load_events(args) if e.get("reward") is not None]
    if not events:
        raise SystemExit("no feedback-completed replay events (with reward) — nothing to fit")

    names = ope_eval_report.feature_names(events)
    gap_ms = int(args.session_gap_minutes * 60 * 1000)
    transitions = replay_dataset.build_transitions(events, names, gap_ms=gap_ms)
    if not transitions:
        raise SystemExit("no transitions built from the replay buffer — nothing to fit")

    train, held_out = split_transitions(transitions)
    q = tabular_q.fit(train, gamma=args.gamma, sweeps=args.sweeps)
    model = fqi.fit(train, gamma=args.gamma, iterations=args.fqi_iterations,
                    epochs=args.fqi_epochs, seed=args.seed)

    summary = {
        "n_events": len(events),
        "n_transitions": len(transitions),
        "n_train": len(train),
        "n_held_out": len(held_out),
        "n_terminal": sum(1 for t in transitions if t.terminal),
        "q_table_size": len(q),
        "tabular_td_residual": tabular_td_residual(q, held_out, args.gamma),
        "tabular_held_out_coverage": held_out_coverage(q, held_out),
        "fqi_td_residual": fqi_td_residual(model, held_out, args.gamma),
    }

    print(f"transitions={summary['n_transitions']} "
          f"(train={summary['n_train']} held-out={summary['n_held_out']} "
          f"terminal={summary['n_terminal']})")
    print(f"tabular Q-table entries: {summary['q_table_size']}")
    print(f"tabular: held-out mean |TD error| over the genre/tag state key = "
          f"{_format(summary['tabular_td_residual'])} "
          f"(held-out (state,action) coverage of the Q-table = "
          f"{_format(summary['tabular_held_out_coverage'])}; uncovered pairs score 0.0, which "
          f"makes the residual optimistic to read as fit quality)")
    print(f"fqi: held-out mean |TD error| over the per-candidate feature vector = "
          f"{_format(summary['fqi_td_residual'])}")
    print("note: the two arms fit different state representations, so the two residuals above "
          "are NOT directly comparable and must not be read as a head-to-head.")

    if args.output_parquet:
        import pandas as pd
        scored = score_events(events, names, q, model)
        destination = Path(args.output_parquet)
        destination.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(scored).to_parquet(destination, index=False)
        print(f"wrote {len(scored)} scored events to {destination}")
        print(f"evaluate with: python3 ../ope_eval_report.py --parquet {destination}")
    return summary


if __name__ == "__main__":
    main()
