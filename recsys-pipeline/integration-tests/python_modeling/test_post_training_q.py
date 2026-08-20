import sys
from pathlib import Path

import pytest

_MODELING = Path(__file__).parents[2] / "services" / "python-modeling"
sys.path.insert(0, str(_MODELING))
sys.path.insert(0, str(_MODELING / "post-training"))

import ope_eval_report
import ope_support
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


import fqi


def test_fqi_matches_the_tabular_arm_on_the_analytic_chain():
    # Same closed-form values as the tabular test: Q(B)=10.0, Q(A)=9.0, Q(C)=0.5.
    # One-hot features make the three state-actions linearly separable.
    model = fqi.fit(CHAIN, gamma=0.9, iterations=40, epochs=300, hidden=16, lr=0.01)
    assert model.score_one([0.0, 1.0, 0.0]) == pytest.approx(10.0, abs=1.0)
    assert model.score_one([1.0, 0.0, 0.0]) == pytest.approx(9.0, abs=1.0)
    assert model.score_one([0.0, 0.0, 1.0]) == pytest.approx(0.5, abs=1.0)


def test_fqi_orders_the_chain_correctly():
    model = fqi.fit(CHAIN, gamma=0.9, iterations=40, epochs=300, hidden=16, lr=0.01)
    assert model.score_one([0.0, 1.0, 0.0]) > model.score_one([1.0, 0.0, 0.0]) \
           > model.score_one([0.0, 0.0, 1.0])


def test_fqi_is_deterministic_for_a_fixed_seed():
    a = fqi.fit(CHAIN, gamma=0.9, iterations=5, epochs=20, hidden=8, lr=0.01, seed=7)
    b = fqi.fit(CHAIN, gamma=0.9, iterations=5, epochs=20, hidden=8, lr=0.01, seed=7)
    assert a.score_one([1.0, 0.0, 0.0]) == pytest.approx(b.score_one([1.0, 0.0, 0.0]), abs=1e-9)


def test_fqi_scores_an_empty_batch_without_error():
    model = fqi.fit(CHAIN, gamma=0.9, iterations=2, epochs=10, hidden=8, lr=0.01)
    assert model.score_many([]) == []


def test_fqi_rejects_an_empty_dataset():
    with pytest.raises(ValueError, match="no transitions"):
        fqi.fit([], gamma=0.9)


import ope_eval_report
import post_train_q


def _replay_fixture(n_users=12, per_user=4):
    """A replay log with a genuine preference: m1 pays, m2 does not."""
    events = []
    for u in range(n_users):
        for step in range(per_user):
            item = "m1" if step % 2 == 0 else "m2"
            events.append({
                "requestId": f"r{u}-{step}",
                "user": f"u{u}",
                "action": item,
                "state": {"genres": ["drama"], "tags": [], "recent": []},
                "reward": 1.0 if item == "m1" else 0.0,
                "clicked": item == "m1",
                "timestamp": 1_000 + step * 1_000,
                "actionSpace": [
                    {"item": "m1", "coldStart": False, "impressions": 20, "clicks": 10,
                     "modelPredictions": {"relevance": 0.8}},
                    {"item": "m2", "coldStart": False, "impressions": 20, "clicks": 1,
                     "modelPredictions": {"relevance": 0.2}},
                ],
            })
    return events


def test_split_transitions_uses_the_ope_held_out_hash():
    transitions = replay_dataset.build_transitions(_replay_fixture())
    train, test = post_train_q.split_transitions(transitions)
    assert train and test
    assert all(not ope_eval_report.is_test(t.request_id) for t in train)
    assert all(ope_eval_report.is_test(t.request_id) for t in test)
    assert len(train) + len(test) == len(transitions)


def test_score_events_writes_both_q_keys_onto_every_candidate():
    events = _replay_fixture()
    names = ope_eval_report.feature_names(events)
    transitions = replay_dataset.build_transitions(events, names)
    q = tabular_q.fit(transitions, gamma=0.9, sweeps=50)
    model = fqi.fit(transitions, gamma=0.9, iterations=3, epochs=20, hidden=8)
    scored = post_train_q.score_events(events, names, q, model)
    for event in scored:
        for candidate in event["actionSpace"]:
            assert "tabQ" in candidate["modelPredictions"]
            assert "fqiQ" in candidate["modelPredictions"]


def test_scored_replay_registers_both_policies_with_the_ope_harness():
    events = _replay_fixture()
    names = ope_eval_report.feature_names(events)
    transitions = replay_dataset.build_transitions(events, names)
    q = tabular_q.fit(transitions, gamma=0.9, sweeps=50)
    model = fqi.fit(transitions, gamma=0.9, iterations=3, epochs=20, hidden=8)
    scored = post_train_q.score_events(events, names, q, model)
    policies = ope_eval_report.policy_names(scored)
    assert "model:tabQ" in policies
    assert "model:fqiQ" in policies


def test_tabular_residual_is_zero_for_a_converged_fit():
    q = tabular_q.fit(CHAIN, gamma=0.9, sweeps=500)
    assert post_train_q.tabular_td_residual(q, CHAIN, gamma=0.9) == pytest.approx(0.0, abs=1e-3)


def test_residuals_are_none_without_held_out_transitions():
    assert post_train_q.tabular_td_residual({}, [], gamma=0.9) is None


def test_main_writes_a_scored_parquet_the_ope_harness_can_read(tmp_path):
    pd = pytest.importorskip("pandas")
    source = tmp_path / "replay.parquet"
    pd.DataFrame(_replay_fixture()).to_parquet(source, index=False)
    destination = tmp_path / "scored.parquet"

    result = post_train_q.main([
        "--parquet", str(source),
        "--output-parquet", str(destination),
        "--fqi-iterations", "3",
        "--fqi-epochs", "20",
    ])

    assert destination.exists()
    reloaded = ope_support.load_from_parquet(destination)
    assert "model:fqiQ" in ope_eval_report.policy_names(reloaded)
    assert result["n_transitions"] > 0


def test_score_events_assigns_correct_per_candidate_q_values_not_constant_or_misaligned():
    events = _replay_fixture()
    names = ope_eval_report.feature_names(events)
    transitions = replay_dataset.build_transitions(events, names)
    q = tabular_q.fit(transitions, gamma=0.9, sweeps=50)
    model = fqi.fit(transitions, gamma=0.9, iterations=3, epochs=20, hidden=8)
    scored = post_train_q.score_events(events, names, q, model)

    event = scored[0]
    state = replay_dataset.state_key(event["state"])
    candidates = {c["item"]: c for c in event["actionSpace"]}
    m1, m2 = candidates["m1"], candidates["m2"]

    for candidate in (m1, m2):
        expected_fqi = model.score_one(ope_eval_report._vec(candidate, names))
        assert candidate["modelPredictions"]["fqiQ"] == pytest.approx(expected_fqi, abs=1e-6)
        expected_tab = tabular_q.score(q, state, candidate["item"])
        assert candidate["modelPredictions"]["tabQ"] == pytest.approx(expected_tab, abs=1e-6)

    assert m1["modelPredictions"]["fqiQ"] != pytest.approx(m2["modelPredictions"]["fqiQ"], abs=1e-6)


def _fitted_arms(events):
    """Feature schema plus both fitted Q arms for a replay fixture."""
    names = ope_eval_report.feature_names(events)
    transitions = replay_dataset.build_transitions(events, names)
    q = tabular_q.fit(transitions, gamma=0.9, sweeps=50)
    model = fqi.fit(transitions, gamma=0.9, iterations=3, epochs=20, hidden=8)
    return names, q, model


def test_injected_q_keys_are_policies_but_never_reward_model_features():
    """C1/I5: a score produced by a policy must not become a feature of the reward model
    that judges it, or model:tabQ is evaluated by an estimator that already knows its answer."""
    events = _replay_fixture()
    names, q, model = _fitted_arms(events)
    scored = post_train_q.score_events(events, names, q, model)

    features = ope_eval_report.feature_names(scored)
    assert "tabQ" not in features
    assert "fqiQ" not in features

    policies = ope_eval_report.policy_names(scored)
    assert "model:tabQ" in policies
    assert "model:fqiQ" in policies

    # pick() reads modelPredictions directly, so the exclusion must not break policy resolution.
    event = scored[0]
    best = ope_eval_report.pick("model:tabQ", event)
    assert best is max(event["actionSpace"],
                       key=lambda c: c["modelPredictions"]["tabQ"])


def test_scoring_the_replay_does_not_perturb_the_other_policies():
    """C2: scoring must leave every pre-existing policy's OPE number byte-identical."""
    unscored = _replay_fixture()
    events = _replay_fixture()
    names, q, model = _fitted_arms(events)
    scored = post_train_q.score_events(events, names, q, model)

    scored_model = ope_eval_report.fit_reward_model(scored)
    unscored_model = ope_eval_report.fit_reward_model(unscored)
    assert scored_model.names == unscored_model.names

    def row(rows, policy):
        return next(r for r in rows if r["policy"] == policy)

    scored_rows = ope_eval_report.evaluate(scored, scored_model)
    unscored_rows = ope_eval_report.evaluate(unscored, unscored_model)
    for policy in ("ctr", "popularity", "random", "logging"):
        assert row(scored_rows, policy)["value"] == row(unscored_rows, policy)["value"]


def test_a_null_action_space_flows_through_the_ope_helpers():
    """I4: `.get(key, default)` never fires on a present-but-null key, and the Java
    ReplayEvent permits a feedback-completed event with no serve-time context."""
    events = _replay_fixture()
    events[0]["actionSpace"] = None

    names = ope_eval_report.feature_names(events)
    assert "relevance" in names
    assert "model:relevance" in ope_eval_report.policy_names(events)
    assert ope_eval_report.taken_features(events[0], names) == [0.0] * len(names)


def test_main_survives_a_replay_with_a_null_action_space(tmp_path):
    pd = pytest.importorskip("pandas")
    events = _replay_fixture()
    events[0]["actionSpace"] = None
    source = tmp_path / "replay.parquet"
    pd.DataFrame(events).to_parquet(source, index=False)
    destination = tmp_path / "scored.parquet"

    result = post_train_q.main([
        "--parquet", str(source),
        "--output-parquet", str(destination),
        "--fqi-iterations", "3",
        "--fqi-epochs", "20",
    ])

    assert result["n_transitions"] > 0
    assert destination.exists()


def _duplicate(reward):
    """A terminal transition on one shared (state, action) key."""
    return _transition("s", "a", reward, terminal=True)


def test_duplicate_state_action_converges_to_the_mean_of_its_targets():
    """C3: an incremental per-occurrence update yields 0.667/0.333 by order; the mean is 0.5."""
    q = tabular_q.fit([_duplicate(0.0), _duplicate(1.0)], gamma=0.9, sweeps=500)
    assert tabular_q.score(q, "s", "a") == pytest.approx(0.5, abs=1e-6)


def test_duplicate_state_action_is_not_recency_weighted():
    q = tabular_q.fit([_duplicate(0.0), _duplicate(0.0), _duplicate(1.0)],
                      gamma=0.9, sweeps=500)
    assert tabular_q.score(q, "s", "a") == pytest.approx(1.0 / 3.0, abs=1e-6)


def test_tabular_fit_is_order_independent():
    import random

    transitions = list(CHAIN) + [_duplicate(0.0), _duplicate(1.0), _duplicate(1.0)]
    shuffled = list(transitions)
    random.Random(20260820).shuffle(shuffled)

    baseline = tabular_q.fit(transitions, gamma=0.9, sweeps=500)
    reordered = tabular_q.fit(shuffled, gamma=0.9, sweeps=500)

    assert set(baseline) == set(reordered)
    for key, value in baseline.items():
        assert reordered[key] == pytest.approx(value, abs=1e-9)
