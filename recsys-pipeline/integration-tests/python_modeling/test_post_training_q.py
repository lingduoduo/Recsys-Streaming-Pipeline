import sys
from pathlib import Path

import pytest

_MODELING = Path(__file__).parents[2] / "services" / "python-modeling"
sys.path.insert(0, str(_MODELING))
sys.path.insert(0, str(_MODELING / "post-training"))

import replay_dataset


def _event(rid, user, item, ts, reward, genres, action_space=None):
    """A replay event in the real Java ReplayEvent shape."""
    return {
        "requestId": rid,
        "user": user,
        "action": item,
        "state": {"genres": genres, "tags": [], "recent": []},
        "reward": reward,
        "clicked": reward > 0,
        "timestamp": ts,
        "actionSpace": action_space if action_space is not None else [
            {"item": item, "coldStart": False, "impressions": 10, "clicks": 3,
             "modelPredictions": {"relevance": 0.5}},
        ],
    }


def test_state_key_is_order_independent_and_deduplicated():
    assert replay_dataset.state_key({"genres": ["drama", "action"], "tags": []}) == \
           replay_dataset.state_key({"genres": ["action", "drama", "action"], "tags": []})


def test_state_key_handles_a_missing_state():
    assert replay_dataset.state_key(None) == "g:|t:"


def test_last_event_in_a_session_is_terminal():
    events = [_event("r1", "u", "m1", 1000, 1.0, ["drama"]),
              _event("r2", "u", "m2", 2000, 0.0, ["drama"])]
    transitions = replay_dataset.build_transitions(events)
    assert [t.terminal for t in transitions] == [False, True]


def test_session_gap_starts_a_new_episode():
    gap = replay_dataset.DEFAULT_SESSION_GAP_MS
    events = [_event("r1", "u", "m1", 0, 1.0, ["drama"]),
              _event("r2", "u", "m2", gap + 1, 1.0, ["drama"])]
    transitions = replay_dataset.build_transitions(events)
    assert all(t.terminal for t in transitions)


def test_next_action_space_comes_from_the_following_event():
    follow_up = [{"item": "m9", "coldStart": False, "impressions": 1, "clicks": 0,
                  "modelPredictions": {"relevance": 0.9}}]
    events = [_event("r1", "u", "m1", 1000, 1.0, ["drama"]),
              _event("r2", "u", "m2", 2000, 0.0, ["drama"], action_space=follow_up)]
    first = replay_dataset.build_transitions(events)[0]
    assert first.next_actions == ["m9"]
    assert len(first.next_features) == 1


def test_transition_without_a_next_action_space_is_terminal():
    events = [_event("r1", "u", "m1", 1000, 1.0, ["drama"]),
              _event("r2", "u", "m2", 2000, 0.0, ["drama"], action_space=[])]
    assert replay_dataset.build_transitions(events)[0].terminal is True


def test_events_without_reward_are_dropped():
    incomplete = _event("r1", "u", "m1", 1000, 0.0, ["drama"])
    incomplete["reward"] = None
    assert replay_dataset.build_transitions([incomplete]) == []


def test_users_do_not_share_episodes():
    events = [_event("r1", "alice", "m1", 1000, 1.0, ["drama"]),
              _event("r2", "bob", "m2", 2000, 1.0, ["drama"])]
    transitions = replay_dataset.build_transitions(events)
    assert all(t.terminal for t in transitions)


def test_state_key_survives_a_parquet_round_trip(tmp_path):
    """Parquet returns nested lists as ndarrays. An isinstance(list) check would silently
    collapse every event to the empty state key, training a one-row Q-table with no error."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    events = [_event("r1", "u", "m1", 1000, 1.0, ["drama", "action"])]
    path = tmp_path / "replay.parquet"
    pd.DataFrame(events).to_parquet(path, index=False)
    reloaded = pd.read_parquet(path).to_dict(orient="records")[0]

    assert replay_dataset.state_key(reloaded["state"]) == \
           replay_dataset.state_key({"genres": ["action", "drama"], "tags": []})
    assert replay_dataset.state_key(reloaded["state"]) != "g:|t:"


def test_build_transitions_handles_a_parquet_round_tripped_action_space(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    two_candidates = [
        {"item": "m1", "coldStart": False, "impressions": 20, "clicks": 10,
         "modelPredictions": {"relevance": 0.8}},
        {"item": "m2", "coldStart": False, "impressions": 20, "clicks": 1,
         "modelPredictions": {"relevance": 0.2}},
    ]
    events = [_event("r1", "u", "m1", 1000, 1.0, ["drama"], action_space=two_candidates),
              _event("r2", "u", "m2", 2000, 0.0, ["drama"], action_space=two_candidates)]
    path = tmp_path / "replay.parquet"
    pd.DataFrame(events).to_parquet(path, index=False)
    reloaded = pd.read_parquet(path).to_dict(orient="records")

    transitions = replay_dataset.build_transitions(reloaded)

    assert transitions[0].next_actions == ["m1", "m2"]


def test_as_list_normalizes_none_and_arrays():
    np = pytest.importorskip("numpy")
    assert replay_dataset.as_list(None) == []
    assert replay_dataset.as_list(np.array(["a", "b"])) == ["a", "b"]
    assert replay_dataset.as_list(["a"]) == ["a"]


import tabular_q


def _transition(state, action, reward, next_state="", next_actions=(), terminal=False,
                features=(1.0,), next_features=()):
    """A Transition built directly, so trainer tests do not depend on episode chaining."""
    return replay_dataset.Transition(
        request_id="r-" + state + action,
        user="u",
        state_key=state,
        action=action,
        reward=reward,
        features=list(features),
        next_state_key=next_state,
        next_features=[list(f) for f in next_features],
        next_actions=list(next_actions),
        terminal=terminal,
    )


# Analytic fixture: a self-loop at B paying 1.0 forever, A feeding into B for free, and a
# terminal C paying 0.5 once. With gamma=0.9 the optimal values are closed-form:
#   Q(B,go) = 1 / (1 - 0.9)      = 10.0
#   Q(A,go) = 0 + 0.9 * 10.0     =  9.0
#   Q(C,go) = 0.5                        (terminal: no bootstrap)
CHAIN = [
    _transition("B", "go", 1.0, next_state="B", next_actions=["go"], features=(0.0, 1.0, 0.0),
                next_features=[(0.0, 1.0, 0.0)]),
    _transition("A", "go", 0.0, next_state="B", next_actions=["go"], features=(1.0, 0.0, 0.0),
                next_features=[(0.0, 1.0, 0.0)]),
    _transition("C", "go", 0.5, terminal=True, features=(0.0, 0.0, 1.0)),
]


def test_tabular_q_recovers_the_analytic_self_loop_value():
    q = tabular_q.fit(CHAIN, gamma=0.9, sweeps=500)
    assert tabular_q.score(q, "B", "go") == pytest.approx(10.0, abs=1e-3)


def test_tabular_q_propagates_value_backwards_through_the_chain():
    q = tabular_q.fit(CHAIN, gamma=0.9, sweeps=500)
    assert tabular_q.score(q, "A", "go") == pytest.approx(9.0, abs=1e-3)


def test_tabular_q_does_not_bootstrap_past_a_terminal_transition():
    q = tabular_q.fit(CHAIN, gamma=0.9, sweeps=500)
    assert tabular_q.score(q, "C", "go") == pytest.approx(0.5, abs=1e-9)


def test_max_next_q_is_zero_for_a_terminal_transition():
    assert tabular_q.max_next_q({("B", "go"): 10.0}, CHAIN[2]) == 0.0


def test_max_next_q_ignores_actions_outside_the_next_action_space():
    q = {("B", "go"): 1.0, ("B", "unreachable"): 99.0}
    assert tabular_q.max_next_q(q, CHAIN[0]) == 1.0


def test_unvisited_state_action_scores_zero():
    assert tabular_q.score({}, "nowhere", "nothing") == 0.0
