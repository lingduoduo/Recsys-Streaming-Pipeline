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

#: modelPredictions key holding the per-candidate exploitation score, before the diversity re-rank.
#: `banditScore` -- the post-diversity final score -- is logged for every candidate too and is a
#: defensible alternative reference; `predictionScore` is used because it is the ranking signal the
#: model produced, unmixed with the diversity adjustment applied on top of it.
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


@dataclass(frozen=True)
class JoinDiagnostics:
    """Whether the slate-to-replay join worked at all, and whether the reference survived it.

    `n_slate_request_ids_matched` is the number that says instantly whether the join is working:
    slate request_ids that appear anywhere in the replay index. Zero means the two sides are in
    different id namespaces, not that the data is merely sparse.
    """

    n_slate_request_ids: int
    n_slate_request_ids_matched: int
    n_missing_reference_sides: int


def replay_index(events) -> tuple[dict, set]:
    """(requestId, item) -> the replay candidate that can supply a pair's features.

    Holds the candidate dict rather than an extracted feature vector. A pair exists only where a
    slate carried both an engagement and a non-engaged item, so at low click-through most
    candidates never reach one and extracting their features here is work thrown away -- measured
    at 95% discarded when 5% of slates are engaged, and 100% when the slate and replay id
    namespaces do not match at all. `build_pairs` extracts on first use instead.

    The returned set is every requestId that contributed at least one candidate, collected in this
    loop rather than recovered by a second pass over the keys. It answers only whether the join
    works at all, so it must stay the set of ids the index can actually serve: an event with an
    empty or absent actionSpace supplies no candidate and so is NOT a match, exactly as deriving
    the set from the index keys used to give. Counting it would report ids as matched while no
    pair could ever be built from them, which is the namespace mismatch this diagnostic exists to
    tell apart from mere sparsity.

    One deliberate consequence of deferring extraction: a malformed candidate -- a non-numeric
    `impressions`, say -- used to fail the whole run here, at index time, whether or not any pair
    needed it. Now it raises only if a pair actually uses it, so a bad candidate in a slate that
    produced no preference is skipped in silence. That is the right trade for a job whose input is
    a logged replay it does not control, but it does mean this function no longer validates the
    candidates it indexes.
    """
    index = {}
    request_ids = set()
    for event in events:
        request_id = str(event.get("requestId", ""))
        candidates = ope_eval_report.candidates_of(event)
        if candidates:
            request_ids.add(request_id)
        for candidate in candidates:
            index[(request_id, str(candidate.get("item")))] = candidate
    return index, request_ids


def build_pairs(slates, events, names=None):
    """Cross every chosen item with every rejected item WITHIN each slate.

    Returns (pairs, dropped, JoinDiagnostics). `dropped` counts pairs discarded because one side
    had no replay row to supply features. A low join yield is a finding worth reporting, never
    something to swallow -- and a zero yield is usually a namespace mismatch rather than sparsity,
    which is what the matched-request_id count exposes.
    """
    if names is None:
        names = ope_eval_report.feature_names(events)
    index, indexed_request_ids = replay_index(events)
    # (requestId, item) -> (features, reference, reference present), built on first use. An
    # absent predictionScore -- or a null modelPredictions, which is what Parquet yields for an
    # absent nested struct -- reads as 0.0, which silently zeroes the reference margin and
    # degrades the DPO loss to plain BPR. Recording presence lets that be counted and warned
    # about instead of disappearing into a default.
    sides: dict = {}

    def side(key):
        resolved = sides.get(key)
        if resolved is None:
            candidate = index[key]
            predictions = candidate.get("modelPredictions") or {}
            reference = predictions.get(REFERENCE_PRED_KEY)
            resolved = (ope_eval_report.candidate_features(candidate, names),
                        float(reference or 0.0), reference is not None)
            sides[key] = resolved
        return resolved

    pairs: list[PreferencePair] = []
    dropped = 0
    missing_reference_sides = 0
    slate_request_ids = set()
    for slate in slates:
        request_id = str(slate.get("request_id", ""))
        slate_request_ids.add(request_id)
        user = str(slate.get("user_id", ""))
        # One pass: is_chosen is evaluated once per item, and each item's index key is built once
        # here rather than inside the cross product below. Only the key is carried forward --
        # every PreferencePair field comes from the key or the slate, never from the item dict.
        chosen_keys, rejected_keys = [], []
        for item in as_list(slate.get("items")):
            key = (request_id, str(item.get("item_id")))
            (chosen_keys if is_chosen(item) else rejected_keys).append(key)
        for win_key in chosen_keys:
            for lose_key in rejected_keys:
                if win_key not in index or lose_key not in index:
                    dropped += 1
                    continue
                win_features, win_reference, win_has_reference = side(win_key)
                lose_features, lose_reference, lose_has_reference = side(lose_key)
                missing_reference_sides += (not win_has_reference) + (not lose_has_reference)
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
    diagnostics = JoinDiagnostics(
        n_slate_request_ids=len(slate_request_ids),
        n_slate_request_ids_matched=len(slate_request_ids & indexed_request_ids),
        n_missing_reference_sides=missing_reference_sides,
    )
    return pairs, dropped, diagnostics
