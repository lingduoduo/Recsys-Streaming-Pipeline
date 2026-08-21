#!/usr/bin/env python3
"""Direct Preference Optimization over logged slates -- the third post-training arm.

Builds within-slate preference pairs (an engaged item against an exposed-but-not-engaged item from
the same slate), fits a scorer with the reference-anchored pairwise loss, writes its predictions
back into the replay as `dpoScore`, and reports held-out pairwise accuracy for both the fitted
policy and the reference it was anchored to.

    python3 post_train_dpo.py --parquet scored_by_q.parquet --slates slates.parquet \
                              --output-parquet scored.parquet
    python3 ../ope_eval_report.py --parquet scored.parquet

Chaining after post_train_q.py is safe: feature_names() excludes every key in
POLICY_ONLY_PRED_KEYS, so scoring an already-scored replay cannot feed one arm's output into
another arm's inputs.

The scorer is fit on the NON-held-out split only, matching how ope_eval_report fits its reward
model. Fitting on everything would hand model:dpoScore an in-sample advantage the ctr and
popularity baselines do not get.
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

import dpo
import replay_dataset
import slate_pairs


def split_pairs(pairs):
    """Train/held-out split on the same requestId hash ope_eval_report uses."""
    train = [p for p in pairs if not ope_eval_report.is_test(p.request_id)]
    held_out = [p for p in pairs if ope_eval_report.is_test(p.request_id)]
    if not train:
        return pairs, pairs
    return train, held_out


def reference_pairwise_accuracy(pairs):
    """How often the LOGGING policy already ranked the pair correctly.

    Read the fitted policy's accuracy against this one: if the reference already separates the
    pairs, the fitted policy has learned nothing the logging policy did not already know.
    """
    return dpo.pairwise_accuracy([p.chosen_reference for p in pairs],
                                 [p.rejected_reference for p in pairs])


def policy_pairwise_accuracy(policy, pairs):
    if not pairs:
        return None
    return dpo.pairwise_accuracy(policy.score_many([p.chosen_features for p in pairs]),
                                 policy.score_many([p.rejected_features for p in pairs]))


def score_events(events, names, policy):
    """Write dpoScore into every candidate's modelPredictions, in place.

    Every key written here must be registered in ope_eval_report.POLICY_ONLY_PRED_KEYS, or the
    reward model will be fit on the scores it is then used to grade.
    """
    rows, targets = [], []
    for event in events:
        for candidate in replay_dataset.as_list(event.get("actionSpace")):
            rows.append(ope_eval_report._vec(candidate, names))
            targets.append(candidate)
    scores = policy.score_many(rows)
    for candidate, score in zip(targets, scores):
        # setdefault returns the STORED value when the key is present, so a null modelPredictions
        # -- what Parquet yields for an absent nested struct -- would come back as None.
        predictions = candidate.get("modelPredictions")
        if predictions is None:
            predictions = candidate["modelPredictions"] = {}
        predictions[ope_eval_report.DPO_PRED_KEY] = float(score)
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
    parser.add_argument("--slates", required=True,
                        help="Parquet written by ExperienceCollectorStreamingJob's slate sink")
    parser.add_argument("--limit", type=int, default=-1)
    parser.add_argument("--beta", type=float, default=dpo.DEFAULT_BETA)
    parser.add_argument("--epochs", type=int, default=dpo.DEFAULT_EPOCHS)
    parser.add_argument("--hidden", type=int, default=dpo.DEFAULT_HIDDEN)
    parser.add_argument("--seed", type=int, default=dpo.SEED)
    parser.add_argument("--output-parquet", default=None,
                        help="write the scored replay here for ope_eval_report.py --parquet")
    args = parser.parse_args(argv)

    events = _load_events(args)
    if not events:
        raise SystemExit("no replay events — nothing to join against")
    slates = ope_support.load_from_parquet(args.slates)
    if not slates:
        raise SystemExit("no slates — nothing to build preference pairs from")

    names = ope_eval_report.feature_names(events)
    pairs, dropped = slate_pairs.build_pairs(slates, events, names)
    if not pairs:
        raise SystemExit(
            f"no preference pairs built ({dropped} dropped for want of a replay row). Every slate "
            f"either had no engagement or had no unengaged item to contrast it against.")

    train, held_out = split_pairs(pairs)
    policy = dpo.fit(train, beta=args.beta, epochs=args.epochs, hidden=args.hidden, seed=args.seed)

    total = len(pairs) + dropped
    summary = {
        "n_events": len(events),
        "n_slates": len(slates),
        "n_pairs": len(pairs),
        "n_dropped_pairs": dropped,
        "join_yield": (len(pairs) / total) if total else None,
        "n_train": len(train),
        "n_held_out": len(held_out),
        "policy_pairwise_accuracy": policy_pairwise_accuracy(policy, held_out),
        "reference_pairwise_accuracy": reference_pairwise_accuracy(held_out),
    }

    print(f"pairs={summary['n_pairs']} (train={summary['n_train']} "
          f"held-out={summary['n_held_out']}); dropped={summary['n_dropped_pairs']} "
          f"for want of a replay row, join yield={_format(summary['join_yield'])}")
    print(f"held-out pairwise accuracy: "
          f"policy={_format(summary['policy_pairwise_accuracy'])} "
          f"reference={_format(summary['reference_pairwise_accuracy'])}")
    print("note: the reference is the logged predictionScore. A policy that does not beat it has "
          "learned nothing the logging policy did not already know.")

    if args.output_parquet:
        import pandas as pd
        scored = score_events(events, names, policy)
        destination = Path(args.output_parquet)
        destination.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(scored).to_parquet(destination, index=False)
        print(f"wrote {len(scored)} scored events to {destination}")
        print(f"evaluate with: python3 ../ope_eval_report.py --parquet {destination}")
    return summary


if __name__ == "__main__":
    main()
