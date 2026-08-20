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
    """Sweep the dataset until the largest value change falls below `tol`.

    Each sweep is a BATCH backup: every transition sharing a (state_key, action) key
    contributes one Bellman target, those targets are averaged, and the key is updated once
    against the average. Updating once per occurrence instead would make the result a
    recency-weighted geometric average of the targets and would depend on list order -- one key
    seen with rewards {0, 1} would settle at 0.667 or 0.333 rather than 0.5. The state key is
    coarse (a genre/tag signature), so duplicate keys are the norm, not an edge case.

    Targets are computed against the values held at the START of the sweep, so the result does
    not depend on the iteration order of the grouping either.
    """
    grouped: dict[tuple[str, str], list] = defaultdict(list)
    for transition in transitions:
        grouped[(transition.state_key, transition.action)].append(transition)

    q: dict[tuple[str, str], float] = {}
    for _ in range(sweeps):
        updated = {}
        for key, rows in grouped.items():
            target = sum(t.reward + gamma * max_next_q(q, t) for t in rows) / len(rows)
            current = q.get(key, 0.0)
            updated[key] = current + alpha * (target - current)
        delta = max((abs(value - q.get(key, 0.0)) for key, value in updated.items()),
                    default=0.0)
        q = updated
        if delta < tol:
            break
    return q


def score(q: dict, state_key_value: str, action: str) -> float:
    """Q for one (state, action); 0.0 when the pair was never visited."""
    return q.get((state_key_value, action), 0.0)
