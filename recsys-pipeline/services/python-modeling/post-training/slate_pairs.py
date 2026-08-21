"""Slate Parquet to preference pairs, joined to replay features.

Pairs come from slates rather than from the replay action space because every item in a slate was
actually EXPOSED: clicked-versus-shown-not-clicked inside one slate is a genuine preference, with
user, context and time held constant. A replay actionSpace candidate was never shown to anyone, so
calling it "rejected" would conflate not-preferred with not-exposed -- which trains a ranker to
reproduce its own logging policy.

The slate decides who won; the replay row supplies the feature vector. That split keeps training,
scoring and evaluation on one schema, which is what lets model:dpoScore land in the same OPE table
as the Q arms.
"""
from __future__ import annotations

from dataclasses import dataclass

import ope_eval_report

from replay_dataset import as_list

#: modelPredictions key holding the per-candidate score the ranker actually ordered by.
#: NOT the top-level banditScore: MovieLensServingSideEffects.candidateFeatures records that only
#: for the selected item, so a pair's rejected item would have none.
REFERENCE_PRED_KEY = "predictionScore"


@dataclass(frozen=True)
class PreferencePair:
    """One within-slate preference, with both sides' features and reference scores."""

    request_id: str
    user: str
    chosen_item: str
    rejected_item: str
    chosen_features: list[float]
    rejected_features: list[float]
    chosen_reference: float
    rejected_reference: float


def is_chosen(item) -> bool:
    """Engaged: a click, an order, or a positive label.

    Everything else in the same slate is a rejected candidate -- including an item carrying a
    thumb_down or a negative_feedback_reason. A disliked item IS a rejected item; this binary
    formulation deliberately makes no attempt to rank degrees of rejection.
    """
    if item.get("clicked"):
        return True
    if item.get("ordered"):
        return True
    label = item.get("label")
    return label is not None and float(label) > 0.0


def replay_index(events, names) -> dict:
    """(requestId, item) -> (feature vector, reference score) for every logged candidate."""
    index = {}
    for event in events:
        request_id = str(event.get("requestId", ""))
        for candidate in ope_eval_report.candidates_of(event):
            predictions = candidate.get("modelPredictions") or {}
            index[(request_id, str(candidate.get("item")))] = (
                ope_eval_report._vec(candidate, names),
                float(predictions.get(REFERENCE_PRED_KEY, 0.0) or 0.0),
            )
    return index


def build_pairs(slates, events, names=None) -> tuple[list[PreferencePair], int]:
    """Cross every chosen item with every rejected item WITHIN each slate.

    Returns (pairs, dropped). `dropped` counts pairs discarded because one side had no replay row
    to supply features. A low join yield is a finding worth reporting, never something to swallow.
    """
    if names is None:
        names = ope_eval_report.feature_names(events)
    index = replay_index(events, names)

    pairs: list[PreferencePair] = []
    dropped = 0
    for slate in slates:
        request_id = str(slate.get("request_id", ""))
        user = str(slate.get("user_id", ""))
        items = as_list(slate.get("items"))
        chosen = [item for item in items if is_chosen(item)]
        rejected = [item for item in items if not is_chosen(item)]
        for win in chosen:
            for lose in rejected:
                win_key = (request_id, str(win.get("item_id")))
                lose_key = (request_id, str(lose.get("item_id")))
                if win_key not in index or lose_key not in index:
                    dropped += 1
                    continue
                win_features, win_reference = index[win_key]
                lose_features, lose_reference = index[lose_key]
                pairs.append(PreferencePair(
                    request_id=request_id,
                    user=user,
                    chosen_item=win_key[1],
                    rejected_item=lose_key[1],
                    chosen_features=win_features,
                    rejected_features=lose_features,
                    chosen_reference=win_reference,
                    rejected_reference=lose_reference,
                ))
    return pairs, dropped
