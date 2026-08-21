#!/usr/bin/env python3
"""Direct Preference Optimization over logged slates -- the third post-training arm.

Builds within-slate preference pairs (an engaged item against an exposed-but-not-engaged item from
the same slate), fits a scorer with the reference-anchored pairwise loss, writes its predictions
back into the replay as `dpoScore`, and reports held-out pairwise accuracy for both the fitted
policy and the reference it was anchored to.

    python3 post_train_dpo.py --parquet scored_by_q.parquet --slates slates.parquet \
                              --output-parquet scored.parquet
    python3 ../ope_eval_report.py --parquet scored.parquet

UNMET PREREQUISITE: the slate log and the replay buffer mint their request ids independently --
`f"req_{uuid4().hex[:12]}"` in movie_segment_producer versus `UUID.randomUUID().toString()` in the
Java serving path -- so on the data this repository produces today the requestId join matches
nothing and this CLI exits with 0 pairs. It becomes runnable once the serving path emits its own
requestId into the Kafka event stream the slate log is built from. The printed
"slate request_ids matched to a replay requestId" count is the number that says which case you are
in.

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
    """Train/held-out split on the same requestId hash ope_eval_report uses.

    Returns (train, held_out, degenerate). When the hash leaves no training pairs -- which a tiny
    input can do -- the whole set is returned for BOTH sides rather than crashing, and
    `degenerate` says so: the reported accuracies are then IN-SAMPLE, not held-out, and the caller
    must label them as such.
    """
    train = [p for p in pairs if not ope_eval_report.is_test(p.request_id)]
    held_out = [p for p in pairs if ope_eval_report.is_test(p.request_id)]
    if not train:
        return pairs, pairs, True
    return train, held_out, False


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
    pairs, dropped, diagnostics = slate_pairs.build_pairs_with_diagnostics(slates, events, names)
    if not pairs:
        raise SystemExit(
            f"no preference pairs built from {len(slates)} slates and {len(events)} replay events: "
            f"{dropped} candidate pairs dropped for want of a replay row, and only "
            f"{diagnostics.n_slate_request_ids_matched} of {diagnostics.n_slate_request_ids} slate "
            f"request_ids appear in the replay at all.\n"
            "A TOTAL join failure (0 matched) means the two sides mint their ids independently: "
            "the slate log's `request_id` comes from the Python producer "
            "(movie_segment_producer.make_slate, f\"req_{uuid4().hex[:12]}\") while the replay's "
            "`requestId` comes from the Java serving path (HybridRecommendationService, "
            "UUID.randomUUID().toString()). Different generators, different formats -- the values "
            "can never be equal, so the requestId join is structurally impossible on the data this "
            "repository produces today.\n"
            "Fixing it requires the SERVING path to emit its own requestId into the Kafka event "
            "stream the slate log is built from, so both sides carry one id. That is a "
            "serving-path change, outside this component.\n"
            "If ids DID match, the cause is instead that every slate had no engagement, or no "
            "unengaged item to contrast it against.")

    train, held_out, degenerate_split = split_pairs(pairs)
    policy = dpo.fit(train, beta=args.beta, epochs=args.epochs, hidden=args.hidden, seed=args.seed)

    total = len(pairs) + dropped
    summary = {
        "n_events": len(events),
        "n_slates": len(slates),
        "n_pairs": len(pairs),
        "n_dropped_pairs": dropped,
        "join_yield": (len(pairs) / total) if total else None,
        "n_slate_request_ids": diagnostics.n_slate_request_ids,
        "n_slate_request_ids_matched": diagnostics.n_slate_request_ids_matched,
        "n_missing_reference_sides": diagnostics.n_missing_reference_sides,
        "n_train": len(train),
        "n_held_out": len(held_out),
        "degenerate_split": degenerate_split,
        "policy_pairwise_accuracy": policy_pairwise_accuracy(policy, held_out),
        "reference_pairwise_accuracy": reference_pairwise_accuracy(held_out),
    }

    print(f"pairs={summary['n_pairs']} (train={summary['n_train']} "
          f"held-out={summary['n_held_out']}); dropped={summary['n_dropped_pairs']} "
          f"for want of a replay row, join yield={_format(summary['join_yield'])}")
    print(f"slate request_ids matched to a replay requestId: "
          f"{summary['n_slate_request_ids_matched']}/{summary['n_slate_request_ids']}"
          " — 0 here means the two sides use different id namespaces, not that data is sparse")
    if summary["n_missing_reference_sides"]:
        print(f"WARNING: {summary['n_missing_reference_sides']} pair sides carried no "
              f"{slate_pairs.REFERENCE_PRED_KEY}, so their reference was taken as 0.0. For those "
              "pairs the reference margin is degenerate and the loss degrades toward plain BPR, "
              "while the reported reference accuracy flatters the fitted policy.")
    if summary["degenerate_split"]:
        print("WARNING: the train/held-out split degenerated — no pair fell outside the held-out "
              "hash, so the scorer was fit on ALL pairs. The accuracies below are IN-SAMPLE, not "
              "held-out.")
    print(f"held-out pairwise accuracy: "
          f"policy={_format(summary['policy_pairwise_accuracy'])} "
          f"reference={_format(summary['reference_pairwise_accuracy'])}")
    print("note: the reference is the logged predictionScore. A policy that does not beat it has "
          "learned nothing the logging policy did not already know. The reference is rounded to "
          "three decimals at serve time, so it ties far more often than a continuous MLP output "
          "ever will, and ties count as a loss for both sides — a small policy win may be a "
          "tie-handling artifact rather than a real improvement.")

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
