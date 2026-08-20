"""Batch tabular Q-learning over logged transitions -- the baseline arm of offline post-training.

Sweeps the whole dataset repeatedly rather than updating once per live feedback event, so a
value can propagate backwards through an episode instead of being overwritten by the next
request. Values are never clamped: offline ranking is argmax-based, so scale does not matter.
"""
from __future__ import annotations

from collections import defaultdict

DEFAULT_GAMMA = 0.9
DEFAULT_ALPHA = 0.5
DEFAULT_SWEEPS = 500
DEFAULT_TOL = 1e-9


def max_next_q(q: dict, transition) -> float:
    """Best Q over the FEASIBLE actions at s'. Unvisited actions score 0.0.

    Terminal transitions return 0.0, which is what keeps the target at plain `reward`.
    """
    if transition.terminal or not transition.next_actions:
        return 0.0
    return max(q.get((transition.next_state_key, action), 0.0)
               for action in transition.next_actions)


def fit(transitions, gamma: float = DEFAULT_GAMMA, alpha: float = DEFAULT_ALPHA,
        sweeps: int = DEFAULT_SWEEPS, tol: float = DEFAULT_TOL) -> dict[tuple[str, str], float]:
    """Sweep the dataset until the largest value change falls below `tol`."""
    q: dict[tuple[str, str], float] = defaultdict(float)
    for _ in range(sweeps):
        delta = 0.0
        for transition in transitions:
            target = transition.reward + gamma * max_next_q(q, transition)
            key = (transition.state_key, transition.action)
            updated = q[key] + alpha * (target - q[key])
            delta = max(delta, abs(updated - q[key]))
            q[key] = updated
        if delta < tol:
            break
    return dict(q)


def score(q: dict, state_key_value: str, action: str) -> float:
    """Q for one (state, action); 0.0 when the pair was never visited."""
    return q.get((state_key_value, action), 0.0)
