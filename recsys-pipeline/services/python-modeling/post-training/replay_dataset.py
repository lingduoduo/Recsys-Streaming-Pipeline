"""Replay buffer to episodes, for offline post-training.

Reads RL replay events through ope_support (Redis list or Parquet dump), groups them into
per-user sessions, and chains each session into (s, a, r, s') transitions carrying a terminal
flag and the feasible action space at s'.

A logged event carries `actionSpace` at s only. Chaining a session supplies the action space at
s' from the following event, so `max_a' Q(s', a')` ranges over actions the policy could actually
take -- not over every action ever recorded for that state, which is what the online
implementation does.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

# Reuses the sibling module's feature helpers on purpose: training, scoring, and evaluation must
# read an identical feature schema, which is the property ope_eval_report's own comments demand.
from ope_eval_report import _vec as candidate_features
from ope_eval_report import feature_names, taken_features

#: Inactivity gap that ends a session, in milliseconds.
DEFAULT_SESSION_GAP_MS = 30 * 60 * 1000


@dataclass(frozen=True)
class Transition:
    """One (s, a, r, s') tuple plus the feasible action space at s'."""

    request_id: str
    user: str
    state_key: str
    action: str
    reward: float
    features: list[float]
    next_state_key: str
    next_features: list[list[float]]
    next_actions: list[str]
    terminal: bool


def as_list(value) -> list:
    """Normalize a replay field to a plain list.

    Parquet-loaded events return nested JSON lists as numpy ndarrays, for which `value or []`
    raises and `isinstance(value, list)` is False. Both idioms are banned on replay fields;
    everything funnels through here instead. Strings are scalars, not sequences.
    """
    if value is None or isinstance(value, (str, bytes)):
        return []
    try:
        return list(value)
    except TypeError:
        return []


def _normalize(raw) -> str:
    return ",".join(sorted({str(v) for v in as_list(raw) if v is not None}))


def state_key(state) -> str:
    """Canonical order-independent genre/tag signature.

    Produces the same equivalence classes as the Java TabularStateKey, which hashes this exact
    canonical string. The raw string is kept here because offline code never shares Redis keys
    and the readable form is far easier to debug.
    """
    if not isinstance(state, dict):
        return "g:|t:"
    return "g:" + _normalize(state.get("genres")) + "|t:" + _normalize(state.get("tags"))


def event_timestamp(event: dict) -> int:
    """Epoch milliseconds. Feedback time is preferred; serve time is the fallback.

    Raw epoch integers only -- never a formatted date, which would make ordering depend on the
    runner's timezone.
    """
    for field in ("feedbackTimestamp", "timestamp"):
        value = event.get(field)
        if value is not None:
            return int(value)
    return 0


def split_sessions(events: list[dict], gap_ms: int) -> list[list[dict]]:
    """Split one user's time-sorted events wherever the inactivity gap is exceeded."""
    sessions: list[list[dict]] = []
    current: list[dict] = []
    for event in events:
        if current and event_timestamp(event) - event_timestamp(current[-1]) > gap_ms:
            sessions.append(current)
            current = []
        current.append(event)
    if current:
        sessions.append(current)
    return sessions


def build_transitions(events, names=None, gap_ms: int = DEFAULT_SESSION_GAP_MS) -> list[Transition]:
    """Chain feedback-completed events into transitions, one episode per user session."""
    rewarded = [e for e in events if e.get("reward") is not None]
    if names is None:
        names = feature_names(rewarded)

    by_user: dict[str, list[dict]] = defaultdict(list)
    for event in rewarded:
        by_user[str(event.get("user"))].append(event)

    transitions: list[Transition] = []
    for user in sorted(by_user):
        user_events = sorted(by_user[user], key=event_timestamp)
        for session in split_sessions(user_events, gap_ms):
            for index, event in enumerate(session):
                last = index == len(session) - 1
                nxt = None if last else session[index + 1]
                # State and action space are taken from the SAME next event, so the bootstrap
                # target reads one coherent snapshot of s'.
                next_candidates = as_list(nxt.get("actionSpace")) if nxt else []
                transitions.append(Transition(
                    request_id=str(event.get("requestId", "")),
                    user=user,
                    state_key=state_key(event.get("state")),
                    action=str(event.get("action")),
                    reward=float(event.get("reward", 0.0)),
                    features=taken_features(event, names),
                    next_state_key=state_key(nxt.get("state")) if nxt else "",
                    next_features=[candidate_features(c, names) for c in next_candidates],
                    next_actions=[str(c.get("item")) for c in next_candidates],
                    # No feasible action at s' means there is nothing to bootstrap from.
                    terminal=last or not next_candidates,
                ))
    return transitions
