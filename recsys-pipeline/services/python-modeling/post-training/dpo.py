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
