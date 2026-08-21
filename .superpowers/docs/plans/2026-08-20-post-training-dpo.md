# DPO Post-Training Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Direct Preference Optimization as a third arm of the post-training track, trained on within-slate preference pairs and evaluated as `model:dpoScore` in the existing off-policy harness.

**Architecture:** The Spark slate Parquet decides which item of a pair won — every item in a slate was actually exposed, so clicked-vs-shown-not-clicked is a genuine preference. The replay buffer supplies the feature vector, joined on `requestId` + item, so training, scoring, and evaluation all read one schema. Because the policy is a softmax over the slate's own candidates, DPO's partition functions cancel and the loss reduces exactly to BPR plus a reference margin.

**Tech Stack:** Python 3, numpy, torch, pandas/pyarrow, pytest. Reuses `ope_eval_report.py` (feature schema, held-out split, policy registry), `ope_support.py` (standardization), and component 1's `replay_dataset.as_list` and `fqi.QNetwork`.

**Spec:** `.superpowers/docs/specs/2026-08-20-post-training-dpo-design.md`

## Global Constraints

- **Branch stacks on component 1.** This work sits on `feat/post-training-dpo`, cut from `feat/post-training-offline-q` (PR #200, unmerged). Do not branch from `master` — `replay_dataset.py` and `POLICY_ONLY_PRED_KEYS` do not exist there.
- **No serving-path changes.** Nothing under `services/java-retrieval-service/` or `services/spark-streaming-job/`.
- **Nothing moves.** `ope_support.py`, `ope_eval_report.py`, `replay_export.py` stay in `services/python-modeling/`.
- **`ope_eval_report.py` stays numpy-only.** Torch is confined to `post-training/`.
- **`dpoScore` MUST be registered in `ope_eval_report.POLICY_ONLY_PRED_KEYS`** in the same commit that starts writing it. A guard test (`test_every_key_score_events_writes_is_registered_as_policy_only`) already fails otherwise. A policy's own score must never become an input feature of the reward model that grades it.
- **`f_ref` is `modelPredictions["predictionScore"]`**, never the top-level `banditScore` — `candidateFeatures` logs `banditScore` only for the selected item, so a pair's rejected item has none.
- **Chosen means** `clicked` or `ordered` truthy, or `label > 0`. Every other item in the same slate is rejected, including `thumb_down` items.
- **Pairs never cross slates.** Both items of a pair come from one `(request_id, user_id)` row.
- **Dropped pairs are counted and reported**, never silently discarded.
- **Fit on the non-held-out split only**, using the same `ope_eval_report.is_test(requestId)` hash as every other arm.
- **`beta` defaults to `1.0`.**
- **Parquet returns numpy arrays, not lists.** `value or []` raises on a 2+ element ndarray and `isinstance(x, (list, tuple))` is False for round-tripped lists. Use `replay_dataset.as_list` on every replay- or slate-sourced field.
- **Never stage `recsys-pipeline/frontend/data/dashboard.json`** — it is dirty from unrelated work.

## File Structure

| File | Responsibility |
|---|---|
| `services/python-modeling/post-training/slate_pairs.py` | Slate Parquet → `PreferencePair` list, joined to replay features; counts dropped pairs |
| `services/python-modeling/post-training/dpo.py` | The reference-anchored pairwise loss, its trainer, and pairwise accuracy |
| `services/python-modeling/post-training/post_train_dpo.py` | CLI: load → pair → split → fit → score → report |
| `services/python-modeling/ope_eval_report.py` | Modified: register `DPO_PRED_KEY` |
| `integration-tests/python_modeling/test_post_training_dpo.py` | Tests for all three new modules |
| `README.md`, `docs/recommendation_architecture/Analysis_Report.md` | Modified: document the new arm |

---

### Task 1: Slate pair construction

**Files:**
- Create: `recsys-pipeline/services/python-modeling/post-training/slate_pairs.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py`

**Interfaces:**
- Consumes: `ope_eval_report.feature_names(events)`, `ope_eval_report.candidates_of(event)`, `ope_eval_report._vec(cand, names)`, `replay_dataset.as_list(value)`.
- Produces: `PreferencePair` dataclass (fields: `request_id, user, chosen_item, rejected_item, chosen_features, rejected_features, chosen_reference, rejected_reference`), `is_chosen(item) -> bool`, `replay_index(events, names) -> dict`, `build_pairs(slates, events, names=None) -> tuple[list[PreferencePair], int]`, `REFERENCE_PRED_KEY`.

- [ ] **Step 1: Write the failing tests**

Create `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'slate_pairs'`

- [ ] **Step 3: Write the implementation**

Create `recsys-pipeline/services/python-modeling/post-training/slate_pairs.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -v`
Expected: PASS — 10 passed

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/slate_pairs.py \
        recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py
git commit -m "feat: build within-slate preference pairs joined to replay features"
```

---

### Task 2: The DPO loss and trainer

**Files:**
- Create: `recsys-pipeline/services/python-modeling/post-training/dpo.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py` (append)

**Interfaces:**
- Consumes: `slate_pairs.PreferencePair`, `ope_support.standardize`, `ope_support.apply_standardize`, `fqi.QNetwork`.
- Produces: `dpo_loss(chosen_scores, rejected_scores, chosen_reference, rejected_reference, beta) -> torch.Tensor`, `PreferencePolicy` with `.score_one(features) -> float` and `.score_many(rows) -> list[float]`, `fit(pairs, beta, epochs, hidden, lr, seed) -> PreferencePolicy`, `pairwise_accuracy(chosen_scores, rejected_scores) -> float | None`, `DEFAULT_BETA`, `DEFAULT_EPOCHS`, `DEFAULT_HIDDEN`, `DEFAULT_LR`, `SEED`.

- [ ] **Step 1: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dpo'`

- [ ] **Step 3: Write the implementation**

Create `recsys-pipeline/services/python-modeling/post-training/dpo.py`:

```python
"""Direct Preference Optimization over within-slate preference pairs.

Model the policy as a softmax over the candidates of the slate a pair came from. Both terms of
DPO's difference of log-ratios then carry the same partition function over the same candidate set,
and both cancel:

    log pi(w|s) - log pi(l|s) = f(w) - f(l)

so the objective reduces EXACTLY to

    -log sigmoid( beta * [ (f(w) - f(l)) - (f_ref(w) - f_ref(l)) ] )

which is this repository's existing BPR loss plus a reference margin. The partition function that
makes DPO delicate on language models never appears. The relationship to BPR is an identity, not a
limit: when the reference scores both items of a pair equally the margin is zero and this IS BPR at
beta = 1.

DPO is not traditional RL and is not a continuation of the Q arms -- it replaces the RL step rather
than extending it. The arms share a feature schema and an evaluation hookup, not a lineage.
"""
from __future__ import annotations

import numpy as np
import torch
import torch.nn.functional as F

import ope_support

# The same two-layer MLP the FQI arm uses. Its name there is Q-specific; the shape is not, and
# sharing it keeps all three arms on an identical function class so the comparison stays fair.
from fqi import QNetwork as ScoreNetwork

DEFAULT_BETA = 1.0
DEFAULT_EPOCHS = 200
DEFAULT_HIDDEN = 32
DEFAULT_LR = 0.01
SEED = 42


class PreferencePolicy:
    """A trained scorer plus the feature standardization it was fit under."""

    def __init__(self, net: ScoreNetwork, mean, std):
        self.net = net
        self.mean = mean
        self.std = std

    def score_many(self, feature_rows) -> list[float]:
        if not len(feature_rows):
            return []
        x = torch.tensor(
            ope_support.apply_standardize(np.asarray(feature_rows, dtype=float), self.mean, self.std),
            dtype=torch.float32,
        )
        with torch.no_grad():
            return self.net(x).reshape(-1).tolist()

    def score_one(self, features) -> float:
        return self.score_many([features])[0]


def dpo_loss(chosen_scores, rejected_scores, chosen_reference, rejected_reference, beta):
    """Reference-anchored pairwise loss, averaged over the batch."""
    policy_margin = chosen_scores - rejected_scores
    reference_margin = chosen_reference - rejected_reference
    return -F.logsigmoid(beta * (policy_margin - reference_margin)).mean()


def fit(pairs, beta: float = DEFAULT_BETA, epochs: int = DEFAULT_EPOCHS,
        hidden: int = DEFAULT_HIDDEN, lr: float = DEFAULT_LR, seed: int = SEED) -> PreferencePolicy:
    """Fit the scorer by minimizing the reference-anchored pairwise loss."""
    if not pairs:
        raise ValueError("no preference pairs to fit")
    torch.manual_seed(seed)

    # Standardize over BOTH sides at once, so a feature's scale is identical for the chosen and the
    # rejected item. Fitting the transform on one side would shift the margin the loss measures.
    stacked = np.array(
        [p.chosen_features for p in pairs] + [p.rejected_features for p in pairs], dtype=float)
    standardized, mean, std = ope_support.standardize(stacked)
    n = len(pairs)
    chosen_x = torch.tensor(standardized[:n], dtype=torch.float32)
    rejected_x = torch.tensor(standardized[n:], dtype=torch.float32)
    chosen_reference = torch.tensor([p.chosen_reference for p in pairs], dtype=torch.float32)
    rejected_reference = torch.tensor([p.rejected_reference for p in pairs], dtype=torch.float32)

    net = ScoreNetwork(chosen_x.shape[1], hidden)
    optimizer = torch.optim.Adam(net.parameters(), lr=lr)
    for _ in range(epochs):
        optimizer.zero_grad()
        loss = dpo_loss(net(chosen_x), net(rejected_x), chosen_reference, rejected_reference, beta)
        loss.backward()
        optimizer.step()

    return PreferencePolicy(net, mean, std)


def pairwise_accuracy(chosen_scores, rejected_scores):
    """Fraction of pairs the scorer ranks correctly. None when there are no pairs.

    Ties count as losses: a scorer that assigns every item the same value scores 0.0, not 0.5.
    """
    if not chosen_scores:
        return None
    wins = sum(1 for c, r in zip(chosen_scores, rejected_scores) if c > r)
    return wins / len(chosen_scores)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -v`
Expected: PASS — 18 passed

If `test_fit_learns_to_rank_the_chosen_item_above_the_rejected_one` does not separate, raise `epochs` (400 → 1200) rather than weakening the assertion to `>=`.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/dpo.py \
        recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py
git commit -m "feat: add the reference-anchored DPO loss and its trainer"
```

---

### Task 3: CLI and policy-key registration

**Files:**
- Create: `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py`
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py` (the constants block)
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py` (append)

**Interfaces:**
- Consumes: `slate_pairs.build_pairs`, `dpo.fit/pairwise_accuracy`, `ope_eval_report.feature_names/is_test/_vec/candidates_of/DPO_PRED_KEY`, `ope_support.load_from_parquet/load_from_redis`, `replay_dataset.as_list`.
- Produces: `split_pairs(pairs) -> tuple[list, list]`, `reference_pairwise_accuracy(pairs) -> float | None`, `policy_pairwise_accuracy(policy, pairs) -> float | None`, `score_events(events, names, policy) -> list[dict]`, `main(argv=None) -> dict`. In `ope_eval_report`: `DPO_PRED_KEY = "dpoScore"`, added to `POLICY_ONLY_PRED_KEYS`.

- [ ] **Step 1: Register the policy key**

In `recsys-pipeline/services/python-modeling/ope_eval_report.py`, the constants block currently reads:

```python
TABULAR_Q_PRED_KEY = "tabQ"
FQI_Q_PRED_KEY = "fqiQ"
POLICY_ONLY_PRED_KEYS = (TABULAR_Q_PRED_KEY, FQI_Q_PRED_KEY)
```

Change it to:

```python
TABULAR_Q_PRED_KEY = "tabQ"
FQI_Q_PRED_KEY = "fqiQ"
DPO_PRED_KEY = "dpoScore"
POLICY_ONLY_PRED_KEYS = (TABULAR_Q_PRED_KEY, FQI_Q_PRED_KEY, DPO_PRED_KEY)
```

This must happen in the same commit that starts writing `dpoScore`. The existing guard test
`test_every_key_score_events_writes_is_registered_as_policy_only` in `test_post_training_q.py`
enforces it for the Q arms; Step 2 adds the equivalent for this one.

- [ ] **Step 2: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py`:

```python
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'post_train_dpo'`

- [ ] **Step 4: Write the implementation**

Create `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py`:

```python
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -v`
Expected: PASS — 25 passed

- [ ] **Step 6: Run the full suite for regressions**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/ -q`
Expected: PASS — no previously-passing test fails. In particular `test_post_training_q.py` must
still pass: adding `dpoScore` to `POLICY_ONLY_PRED_KEYS` changes the tuple its guard test reads,
and that test asserts written keys are a subset of it, so a third entry is fine.

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py \
        recsys-pipeline/services/python-modeling/ope_eval_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py
git commit -m "feat: add the DPO post-training CLI and register its policy key"
```

---

### Task 4: End-to-end run and documentation

**Files:**
- Modify: `recsys-pipeline/README.md`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md`

**Interfaces:**
- Consumes: the `post_train_dpo.py` CLI from Task 3.
- Produces: no code interfaces; documentation only.

- [ ] **Step 1: Check for real data**

Run: `cd recsys-pipeline && ls -la sampledata/*.parquet 2>/dev/null; redis-cli LLEN replay:recommendations 2>/dev/null || echo "redis not reachable"`

DPO needs **both** a replay dump and a slate Parquet, correlated by `requestId`. If either is
absent, do **not** start Docker, Kafka, Redis, or any simulation to manufacture them. Report the
situation instead and continue with a synthetic smoke test, per Step 2.

- [ ] **Step 2: Run the CLI end to end**

Build a synthetic replay Parquet and a matching slate Parquet in a scratch directory outside the
repo (never committed), with several users, multi-item slates, and `requestId` values shared between
the two files. Then run:

```bash
cd services/python-modeling/post-training
python3 post_train_dpo.py --parquet /tmp/replay.parquet --slates /tmp/slates.parquet \
                          --output-parquet /tmp/scored_dpo.parquet
python3 ../ope_eval_report.py --parquet /tmp/scored_dpo.parquet
```

Record the pair count, dropped count, join yield, both pairwise accuracies, and the OPE table row
for `model:dpoScore`. Label these results unambiguously as a synthetic smoke test. They are
evidence about wiring, not about model quality, and none of them belongs in the documentation.

- [ ] **Step 3: Document the arm in the README**

In `recsys-pipeline/README.md`, immediately after the offline Q-learning post-training subsection
added by the previous component, add:

```markdown
#### DPO post-training

Fits a scorer on within-slate preference pairs — an engaged item against an exposed-but-not-engaged
item from the same slate — anchored to the score the logging policy actually ranked by. Because the
policy is a softmax over the slate's own candidates, DPO's partition functions cancel and the loss
is the repository's BPR loss plus a reference margin.

```bash
cd recsys-pipeline/services/python-modeling/post-training
python3 post_train_dpo.py --parquet /tmp/scored_replay.parquet \
                          --slates <slate-parquet> --output-parquet /tmp/scored_dpo.parquet
python3 ../ope_eval_report.py --parquet /tmp/scored_dpo.parquet
```

Pairs come from slates rather than from the replay action space because every slate item was
actually shown; a replay candidate that was never displayed is not a rejected item. Features still
come from the replay buffer, joined on `requestId`, so all three arms are scored on one schema.
Pairs whose replay row is missing are dropped and counted — check the reported join yield before
reading the accuracy numbers.

This path is offline only: it does not write to Redis and does not affect live ranking.
```

- [ ] **Step 4: Document the reading caveat in the analysis report**

In `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md`, after the offline
Q-learning paragraph added by the previous component, add:

```markdown
`post-training/post_train_dpo.py` adds `model:dpoScore` from the same replay dump, fit on
preference pairs drawn from the slate log. Read its held-out pairwise accuracy against the
reference accuracy printed beside it: the reference is the logged `predictionScore`, so a policy
that fails to beat it has learned nothing the logging policy did not already know. Check the
reported join yield first — pairs whose replay row is missing are dropped, and a low yield means
the accuracy figures describe a small and possibly unrepresentative subset of the slate log.
```

- [ ] **Step 5: Verify the docs reference the real script**

Run: `cd recsys-pipeline && grep -n "post_train_dpo" README.md docs/recommendation_architecture/Analysis_Report.md`
Expected: matches in both files.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/README.md \
        recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md
git commit -m "docs: document the DPO post-training arm and how to read its accuracy"
```

---

## Self-Review Notes

**Spec coverage.** Pair source and the chosen/rejected rule → Task 1. Feature join and the dropped
count → Task 1. The loss and its BPR identity → Task 2. Reference scores from `predictionScore` →
Task 1 (`REFERENCE_PRED_KEY`) consumed in Task 2. Policy MLP → Task 2. Pairwise accuracy for policy
and reference → Task 3. `model:dpoScore` and key registration → Task 3. Split discipline → Task 3.
File layout → Tasks 1-3. Composition after `post_train_q.py` → Task 3's docstring and Task 4's
README block. Success criteria 1-8 → tests in Tasks 1-3 plus the Task 4 run. Risk "neither source
exists locally" → Task 4 Step 1. Risk "join yield unknown" → reported by `main` and documented in
Task 4 Step 4. Risk "`predictionScore` is stochastic under Thompson" → recorded in the spec; no code
change, since the logged algorithm is `ucb` by default.

**Deliberate omissions.** Trained weights are not persisted: nothing reloads them, and the scored
Parquet is the hand-off. Graded preference strength is out of scope per the spec.

**Type consistency.** `PreferencePair` field names are identical across Tasks 1-3.
`dpo.pairwise_accuracy(chosen_scores, rejected_scores)` keeps one signature in `dpo.py` and both
call sites in `post_train_dpo.py`. `PreferencePolicy.score_many` takes a list of feature rows and
returns a list of floats everywhere it is called. `ope_eval_report.DPO_PRED_KEY` is the only
spelling of `dpoScore` outside the constants block.
