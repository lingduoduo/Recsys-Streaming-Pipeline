"""Fitted Q Iteration over logged transitions -- the function-approximation arm.

Q(s, a) is approximated by a small MLP over the per-candidate feature vector. Those features are
already state-conditioned: modelPredictions such as relevance and contentScore are computed per
(user, item) pair at serve time, so one candidate vector IS a (state, action) vector.

Each iteration freezes the previous network, computes regression targets
`r + gamma * max_a' Q_prev(s', a')`, and refits against them. Freezing per iteration is what
makes the procedure a contraction rather than a regression against a moving target.
"""
from __future__ import annotations

import copy

import numpy as np
import torch
import torch.nn as nn

import ope_support

DEFAULT_GAMMA = 0.9
DEFAULT_ITERATIONS = 20
DEFAULT_EPOCHS = 100
DEFAULT_HIDDEN = 32
DEFAULT_LR = 0.01
SEED = 42


class QNetwork(nn.Module):
    """Two-layer MLP mapping a (state, action) feature vector to a scalar Q."""

    def __init__(self, n_features: int, hidden: int = DEFAULT_HIDDEN):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_features, hidden),
            nn.ReLU(),
            nn.Linear(hidden, 1),
        )

    def forward(self, x):
        return self.net(x).squeeze(-1)


class FittedQ:
    """A trained QNetwork plus the feature standardization it was fit under."""

    def __init__(self, net: QNetwork, mean, std):
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


def _padded_next_states(transitions, mean, std, n_features: int):
    """Ragged next-state action spaces as a padded (N, max_actions, F) tensor plus a mask."""
    width = max((len(t.next_features) for t in transitions), default=0)
    width = max(width, 1)
    feats = np.zeros((len(transitions), width, n_features), dtype=float)
    mask = np.zeros((len(transitions), width), dtype=bool)
    for i, transition in enumerate(transitions):
        if transition.terminal:
            continue
        for j, row in enumerate(transition.next_features):
            feats[i, j] = row
            mask[i, j] = True
    standardized = ope_support.apply_standardize(
        feats.reshape(-1, n_features), mean, std).reshape(feats.shape)
    return torch.tensor(standardized, dtype=torch.float32), torch.tensor(mask)


def fit(transitions, gamma: float = DEFAULT_GAMMA, iterations: int = DEFAULT_ITERATIONS,
        epochs: int = DEFAULT_EPOCHS, hidden: int = DEFAULT_HIDDEN,
        lr: float = DEFAULT_LR, seed: int = SEED) -> FittedQ:
    """Fit Q by iterated regression against frozen bootstrap targets."""
    if not transitions:
        raise ValueError("no transitions to fit")
    torch.manual_seed(seed)

    features = np.array([t.features for t in transitions], dtype=float)
    standardized, mean, std = ope_support.standardize(features)
    x = torch.tensor(standardized, dtype=torch.float32)
    rewards = torch.tensor([t.reward for t in transitions], dtype=torch.float32)

    n_features = x.shape[1]
    next_x, next_mask = _padded_next_states(transitions, mean, std, n_features)
    has_next = next_mask.any(dim=1)

    net = QNetwork(n_features, hidden)
    optimizer = torch.optim.Adam(net.parameters(), lr=lr)
    loss_fn = nn.MSELoss()

    for _ in range(iterations):
        frozen = copy.deepcopy(net)
        with torch.no_grad():
            q_next = frozen(next_x.reshape(-1, n_features)).reshape(next_mask.shape)
            q_next = q_next.masked_fill(~next_mask, float("-inf")).max(dim=1).values
            # Terminal rows have an all-false mask, so their max is -inf; zero them out.
            q_next = torch.where(has_next, q_next, torch.zeros_like(q_next))
            targets = rewards + gamma * q_next
        for _ in range(epochs):
            optimizer.zero_grad()
            loss = loss_fn(net(x), targets)
            loss.backward()
            optimizer.step()

    return FittedQ(net, mean, std)
