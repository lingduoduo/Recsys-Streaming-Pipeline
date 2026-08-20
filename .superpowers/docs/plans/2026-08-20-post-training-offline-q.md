# Offline Q-Learning Post-Training Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `post-training/` directory to `services/python-modeling/` that fits Q functions offline on the logged RL replay buffer — a tabular baseline and a neural Fitted Q Iteration model — and evaluates both through the existing off-policy evaluation harness.

**Architecture:** A dataset layer turns flat replay events into per-user session episodes with terminal flags and the feasible action space at `s'`. Two trainers consume those transitions. A CLI fits both, writes their per-candidate predictions back into the replay as `tabQ` and `fqiQ`, and emits a scored Parquet file. Because `ope_eval_report.py` already supports a generic `model:{key}` policy and discovers keys from candidate `modelPredictions`, the two fitted policies become evaluable with **zero changes to that module**.

**Tech Stack:** Python 3, numpy, torch, pandas/pyarrow, pytest. Existing modules reused: `ope_support.py` (replay loader, standardization), `ope_eval_report.py` (feature schema, held-out split).

**Spec:** `.superpowers/docs/specs/2026-08-20-post-training-offline-q-design.md`

## Global Constraints

- **No serving-path changes.** Nothing under `services/java-retrieval-service/` or `services/spark-streaming-job/` is touched. The fitted Q is never published to Redis.
- **Nothing moves.** `ope_support.py`, `ope_eval_report.py`, and `replay_export.py` stay in `services/python-modeling/`. They are imported, not relocated.
- **`ope_eval_report.py` stays numpy-only.** Torch is confined to `post-training/`. The two modules communicate through a scored Parquet file, never through a shared torch import.
- **Directory is hyphenated:** `services/python-modeling/post-training/`. Its modules are imported flat after a `sys.path` insert — never as a `post_training.` package, since hyphens are invalid in Python package names. Callers insert **both** `python-modeling/` and `python-modeling/post-training/`.
- **`gamma` default `0.9`** — matches the serving default so offline and online value scales stay comparable.
- **Session inactivity gap default 30 minutes.**
- **Q is never clamped.** Offline ranking is argmax-based; clamping is the bug this component exists to avoid.
- **Fit on the train split only.** `ope_eval_report._evaluate_statistics` scores policies over *all* events while `fit_reward_model` trains on the 80% split. The fitted Q must use the same `is_test(requestId)` split, or `model:fqiQ` gets an in-sample advantage that `ctr` and `popularity` do not have.
- **Trained artifacts** go to `sampledata/`, alongside the existing `.onnx` and `.pt` files.
- **Parquet returns numpy arrays, not lists.** `pd.read_parquet(...).to_dict("records")` turns every nested JSON list into an `ndarray` — verified empirically. Two consequences that all new code must respect: `value or []` raises `ValueError: truth value of an array with more than one element is ambiguous`, and `isinstance(value, (list, tuple))` is **False** for round-tripped lists. Never use either idiom on a field that came from the replay buffer; use the `as_list` helper from Task 1.

## File Structure

| File | Responsibility |
|---|---|
| `services/python-modeling/post-training/replay_dataset.py` | Replay events → `Transition` list. Session splitting, terminal flags, `s'` action space, feature vectors. |
| `services/python-modeling/post-training/tabular_q.py` | Batch tabular Q-learning. Baseline arm. |
| `services/python-modeling/post-training/fqi.py` | Fitted Q Iteration with a torch MLP. Main arm. |
| `services/python-modeling/post-training/post_train_q.py` | CLI: load → split → fit both → score replay → write Parquet → report TD residuals. |
| `integration-tests/python_modeling/test_post_training_q.py` | Tests for all four modules. |
| `services/python-modeling/requirements.txt` | Modified: add `torch`. |
| `services/python-modeling/ope_eval_report.py` | Modified: one-line fix so `--parquet` input does not crash. |
| `README.md`, `docs/recommendation_architecture/Analysis_Report.md` | Modified: document the new script. |

---

### Task 0: Make the OPE harness survive Parquet input

The headline workflow of this component is `post_train_q.py --output-parquet` followed by
`ope_eval_report.py --parquet`. That second command is **already broken today**, independent of
this feature: `candidates_of` does `event.get("actionSpace") or []`, and Parquet round-trips
`actionSpace` into an `ndarray`, so any slate with two or more candidates raises
`ValueError: The truth value of an array with more than one element is ambiguous`.

This is a pre-existing bug, fixed here because the feature cannot work without it. The fix is one
line and changes no behavior for the Redis path.

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py:115-117`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py` (append)

**Interfaces:**
- Consumes: nothing.
- Produces: `candidates_of(event) -> list[dict]` — same signature, now accepting ndarray input.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`:

```python
def test_candidates_of_accepts_a_parquet_round_tripped_action_space(tmp_path):
    """Parquet turns nested lists into ndarrays; `or []` on one raises ValueError."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    event = {
        "requestId": "r1", "user": "u", "action": "m1", "reward": 1.0, "clicked": True,
        "modelPredictions": {"relevance": 0.8},
        "actionSpace": [
            {"item": "m1", "coldStart": False, "impressions": 20, "clicks": 10,
             "modelPredictions": {"relevance": 0.8}},
            {"item": "m2", "coldStart": False, "impressions": 20, "clicks": 1,
             "modelPredictions": {"relevance": 0.2}},
        ],
    }
    path = tmp_path / "replay.parquet"
    pd.DataFrame([event]).to_parquet(path, index=False)
    reloaded = pd.read_parquet(path).to_dict(orient="records")[0]

    candidates = ope.candidates_of(reloaded)

    assert [c["item"] for c in candidates] == ["m1", "m2"]
    assert ope.pick("ctr", reloaded)["item"] == "m1"


def test_candidates_of_returns_empty_for_a_missing_action_space():
    assert ope.candidates_of({"requestId": "r1"}) == []
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py -k parquet_round_tripped -v`
Expected: FAIL — `ValueError: The truth value of an array with more than one element is ambiguous`

- [ ] **Step 3: Write the fix**

In `recsys-pipeline/services/python-modeling/ope_eval_report.py`, replace:

```python
def candidates_of(event: dict) -> list[dict]:
    return event.get("actionSpace") or []
```

with:

```python
def candidates_of(event: dict) -> list[dict]:
    # `or []` would raise on a Parquet-loaded event: pandas returns nested lists as ndarrays,
    # whose truth value is ambiguous for 2+ elements. Test explicitly for None instead.
    raw = event.get("actionSpace")
    return [] if raw is None else list(raw)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py -v`
Expected: PASS — every previously-passing test still passes, plus the two new ones.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
git commit -m "fix: stop candidates_of raising on a Parquet-loaded action space"
```

---

### Task 1: Replay dataset layer

**Files:**
- Create: `recsys-pipeline/services/python-modeling/post-training/replay_dataset.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py`

**Interfaces:**
- Consumes: `ope_eval_report.feature_names(events)`, `ope_eval_report.taken_features(event, names)`, `ope_eval_report._vec(cand, names)`.
- Produces: `Transition` dataclass (fields: `request_id, user, state_key, action, reward, features, next_state_key, next_features, next_actions, terminal`), `as_list(value) -> list`, `state_key(state) -> str`, `event_timestamp(event) -> int`, `split_sessions(events, gap_ms) -> list[list[dict]]`, `build_transitions(events, names=None, gap_ms=DEFAULT_SESSION_GAP_MS) -> list[Transition]`, `DEFAULT_SESSION_GAP_MS`.

- [ ] **Step 1: Write the failing tests**

Create `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_q.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'replay_dataset'`

- [ ] **Step 3: Write the implementation**

Create `recsys-pipeline/services/python-modeling/post-training/replay_dataset.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_q.py -v`
Expected: PASS — 11 passed

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/replay_dataset.py \
        recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py
git commit -m "feat: build episodes from the replay buffer for offline post-training"
```

---

### Task 2: Tabular Q-learning baseline

**Files:**
- Create: `recsys-pipeline/services/python-modeling/post-training/tabular_q.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py` (append)

**Interfaces:**
- Consumes: `replay_dataset.Transition`.
- Produces: `fit(transitions, gamma, alpha, sweeps, tol) -> dict[tuple[str, str], float]`, `max_next_q(q, transition) -> float`, `score(q, state_key_value, action) -> float`, `DEFAULT_GAMMA`, `DEFAULT_ALPHA`, `DEFAULT_SWEEPS`, `DEFAULT_TOL`.

- [ ] **Step 1: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_q.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tabular_q'`

- [ ] **Step 3: Write the implementation**

Create `recsys-pipeline/services/python-modeling/post-training/tabular_q.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_q.py -v`
Expected: PASS — 17 passed

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/tabular_q.py \
        recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py
git commit -m "feat: add batch tabular Q-learning as the post-training baseline arm"
```

---

### Task 3: Fitted Q Iteration

**Files:**
- Create: `recsys-pipeline/services/python-modeling/post-training/fqi.py`
- Modify: `recsys-pipeline/services/python-modeling/requirements.txt`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py` (append)

**Interfaces:**
- Consumes: `replay_dataset.Transition`, `ope_support.standardize`, `ope_support.apply_standardize`.
- Produces: `QNetwork(n_features, hidden)`, `FittedQ` with `.score_one(features) -> float` and `.score_many(feature_rows) -> list[float]`, `fit(transitions, gamma, iterations, epochs, hidden, lr, seed) -> FittedQ`, `DEFAULT_GAMMA`, `DEFAULT_ITERATIONS`, `DEFAULT_EPOCHS`, `DEFAULT_HIDDEN`, `DEFAULT_LR`, `SEED`.

- [ ] **Step 1: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_q.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'fqi'`

- [ ] **Step 3: Write the implementation**

Create `recsys-pipeline/services/python-modeling/post-training/fqi.py`:

```python
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
```

- [ ] **Step 4: Add torch to the requirements file**

`recsys-pipeline/services/python-modeling/requirements.txt` currently reads:

```
kafka-python
lz4
fastavro>=1.9,<2
pyarrow>=14,<24
```

Append `torch` so the file lists it:

```
kafka-python
lz4
fastavro>=1.9,<2
pyarrow>=14,<24
torch
```

(`movielens_pipeline.py` already imports torch without declaring it; this component is the second consumer, so the declaration is added here.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_q.py -v`
Expected: PASS — 22 passed

If `test_fqi_matches_the_tabular_arm_on_the_analytic_chain` misses the `abs=1.0` tolerance, raise `epochs` (300 → 800) rather than loosening the assertion. A tolerance wider than 1.0 on a target of 10.0 stops testing anything.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/fqi.py \
        recsys-pipeline/services/python-modeling/requirements.txt \
        recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py
git commit -m "feat: add fitted Q iteration with a torch MLP over candidate features"
```

---

### Task 4: CLI — train, score the replay, report residuals

**Files:**
- Create: `recsys-pipeline/services/python-modeling/post-training/post_train_q.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py` (append)

**Interfaces:**
- Consumes: `replay_dataset.build_transitions`, `replay_dataset.state_key`, `tabular_q.fit/score/max_next_q`, `fqi.fit`, `ope_eval_report.feature_names`, `ope_eval_report.is_test`, `ope_eval_report._vec`, `ope_support.load_from_redis/load_from_parquet`.
- Produces: `split_transitions(transitions) -> tuple[list, list]`, `tabular_td_residual(q, transitions, gamma) -> float | None`, `fqi_td_residual(model, transitions, gamma) -> float | None`, `score_events(events, names, q, model) -> list[dict]`, `main(argv=None) -> dict`.

- [ ] **Step 1: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py`:

```python
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
```

Add `import ope_support` to the import block at the top of the test file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_q.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'post_train_q'`

- [ ] **Step 3: Write the implementation**

Create `recsys-pipeline/services/python-modeling/post-training/post_train_q.py`:

```python
#!/usr/bin/env python3
"""Offline Q-learning post-training over the RL replay buffer.

Fits two Q functions on the logged transitions -- a tabular baseline and a neural Fitted Q
Iteration model -- writes their per-candidate predictions back into the replay events as `tabQ`
and `fqiQ`, and reports the held-out Bellman residual for each.

The scored replay is written as Parquet so that ope_eval_report.py picks the two new policies up
with NO change to that module: its `pick()` already supports a generic `model:{key}` policy and
`policy_names()` discovers keys from candidate modelPredictions.

    REDIS_HOST=localhost python3 post_train_q.py --output-parquet /tmp/scored_replay.parquet
    python3 ../ope_eval_report.py --parquet /tmp/scored_replay.parquet

Both Q functions are fit on the NON-held-out split only, matching how ope_eval_report fits its
reward model. Fitting on everything would hand model:fqiQ an in-sample advantage that the
ctr and popularity baselines do not get.
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

import fqi
import replay_dataset
import tabular_q


def split_transitions(transitions):
    """Train/held-out split on the same requestId hash ope_eval_report uses."""
    train = [t for t in transitions if not ope_eval_report.is_test(t.request_id)]
    test = [t for t in transitions if ope_eval_report.is_test(t.request_id)]
    if not train:
        return transitions, transitions
    return train, test


def tabular_td_residual(q, transitions, gamma: float):
    """Mean absolute Bellman error. None when there is nothing held out."""
    if not transitions:
        return None
    total = 0.0
    for transition in transitions:
        target = transition.reward + gamma * tabular_q.max_next_q(q, transition)
        predicted = tabular_q.score(q, transition.state_key, transition.action)
        total += abs(target - predicted)
    return total / len(transitions)


def fqi_td_residual(model, transitions, gamma: float):
    """Mean absolute Bellman error for the fitted network."""
    if not transitions:
        return None
    predicted = model.score_many([t.features for t in transitions])
    total = 0.0
    for transition, q_value in zip(transitions, predicted):
        if transition.terminal or not transition.next_features:
            next_value = 0.0
        else:
            next_value = max(model.score_many(transition.next_features))
        total += abs(transition.reward + gamma * next_value - q_value)
    return total / len(transitions)


def score_events(events, names, q, model):
    """Write tabQ and fqiQ into every candidate's modelPredictions, in place."""
    rows, targets = [], []
    for event in events:
        state = replay_dataset.state_key(event.get("state"))
        for candidate in replay_dataset.as_list(event.get("actionSpace")):
            rows.append(ope_eval_report._vec(candidate, names))
            targets.append((candidate, state))
    fqi_scores = model.score_many(rows)
    for (candidate, state), q_value in zip(targets, fqi_scores):
        predictions = candidate.setdefault("modelPredictions", {})
        predictions["tabQ"] = tabular_q.score(q, state, str(candidate.get("item")))
        predictions["fqiQ"] = float(q_value)
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
    parser.add_argument("--limit", type=int, default=-1)
    parser.add_argument("--gamma", type=float, default=tabular_q.DEFAULT_GAMMA)
    parser.add_argument("--session-gap-minutes", type=float, default=30.0)
    parser.add_argument("--sweeps", type=int, default=tabular_q.DEFAULT_SWEEPS)
    parser.add_argument("--fqi-iterations", type=int, default=fqi.DEFAULT_ITERATIONS)
    parser.add_argument("--fqi-epochs", type=int, default=fqi.DEFAULT_EPOCHS)
    parser.add_argument("--seed", type=int, default=fqi.SEED)
    parser.add_argument("--output-parquet", default=None,
                        help="write the scored replay here for ope_eval_report.py --parquet")
    args = parser.parse_args(argv)

    events = [e for e in _load_events(args) if e.get("reward") is not None]
    if not events:
        raise SystemExit("no feedback-completed replay events (with reward) — nothing to fit")

    names = ope_eval_report.feature_names(events)
    gap_ms = int(args.session_gap_minutes * 60 * 1000)
    transitions = replay_dataset.build_transitions(events, names, gap_ms=gap_ms)
    if not transitions:
        raise SystemExit("no transitions built from the replay buffer — nothing to fit")

    train, held_out = split_transitions(transitions)
    q = tabular_q.fit(train, gamma=args.gamma, sweeps=args.sweeps)
    model = fqi.fit(train, gamma=args.gamma, iterations=args.fqi_iterations,
                    epochs=args.fqi_epochs, seed=args.seed)

    summary = {
        "n_events": len(events),
        "n_transitions": len(transitions),
        "n_train": len(train),
        "n_held_out": len(held_out),
        "n_terminal": sum(1 for t in transitions if t.terminal),
        "q_table_size": len(q),
        "tabular_td_residual": tabular_td_residual(q, held_out, args.gamma),
        "fqi_td_residual": fqi_td_residual(model, held_out, args.gamma),
    }

    print(f"transitions={summary['n_transitions']} "
          f"(train={summary['n_train']} held-out={summary['n_held_out']} "
          f"terminal={summary['n_terminal']})")
    print(f"tabular Q-table entries: {summary['q_table_size']}")
    print("held-out mean |TD error|: "
          f"tabular={_format(summary['tabular_td_residual'])} "
          f"fqi={_format(summary['fqi_td_residual'])}")

    if args.output_parquet:
        import pandas as pd
        scored = score_events(events, names, q, model)
        destination = Path(args.output_parquet)
        destination.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(scored).to_parquet(destination, index=False)
        print(f"wrote {len(scored)} scored events to {destination}")
        print(f"evaluate with: python3 ../ope_eval_report.py --parquet {destination}")
    return summary


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_q.py -v`
Expected: PASS — 28 passed

- [ ] **Step 5: Run the full python-modeling suite for regressions**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/ -q`
Expected: PASS — no previously-passing test fails. The new `tabQ`/`fqiQ` keys only exist in files this CLI writes, so `test_ope_eval.py` must be unaffected.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/post_train_q.py \
        recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py
git commit -m "feat: add the offline Q post-training CLI with held-out TD residuals"
```

---

### Task 5: End-to-end run and documentation

**Files:**
- Modify: `recsys-pipeline/README.md`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md`

**Interfaces:**
- Consumes: the `post_train_q.py` CLI from Task 4.
- Produces: no code interfaces; documentation only.

- [ ] **Step 1: Check whether a real replay buffer exists**

Run: `cd recsys-pipeline && ls -la sampledata/*.parquet 2>/dev/null; redis-cli LLEN replay:recommendations 2>/dev/null || echo "redis not running"`

If neither a Parquet dump nor a populated Redis list exists, generate one with a simulation run (`scripts/run-movielens-segment-sim.sh`, then `scripts/run-retrain.sh` which invokes `replay_export.py --parquet`). **If no real data can be produced, say so explicitly in the final report rather than substituting synthetic data** — the point of this step is to confirm the component runs against real logged events.

- [ ] **Step 2: Run the trainer end to end against real data**

```bash
cd recsys-pipeline/services/python-modeling/post-training
python3 post_train_q.py --parquet ../../../sampledata/replay_training.parquet \
                        --output-parquet /tmp/scored_replay.parquet
python3 ../ope_eval_report.py --parquet /tmp/scored_replay.parquet
```

Expected: the trainer prints transition counts and both held-out TD residuals; the evaluator prints a table that now includes `model:tabQ` and `model:fqiQ` rows with values, bootstrap CIs, and lift against `logging`. Record the actual numbers — they go into the final report.

- [ ] **Step 3: Document the script in the README**

In `recsys-pipeline/README.md`, immediately after the existing off-policy-evaluation block around line 994 (`REDIS_HOST=localhost python services/python-modeling/ope_eval_report.py --output ope_eval.csv`), add:

```markdown
#### Offline Q-learning post-training

Fits two Q functions on the logged replay buffer — a tabular baseline and a neural Fitted Q
Iteration model — and scores the replay so the off-policy evaluator can rank them against the
existing baselines. Both are fit on the non-held-out split only, matching the reward model.

```bash
cd services/python-modeling/post-training
REDIS_HOST=localhost python3 post_train_q.py --output-parquet /tmp/scored_replay.parquet
python3 ../ope_eval_report.py --parquet /tmp/scored_replay.parquet
```

The second command's table gains `model:tabQ` and `model:fqiQ` rows. This path is offline only:
it does not write to Redis and does not affect live ranking.
```

- [ ] **Step 4: Document the estimator caveat in the analysis report**

In `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md`, after the existing
`ope_eval_report.py` description around line 137, add:

```markdown
`post-training/post_train_q.py` fits offline Q functions on the same replay buffer and injects
their per-candidate predictions as `tabQ` and `fqiQ`, which the evaluator discovers automatically
as `model:tabQ` and `model:fqiQ`. Two caveats when reading those rows: the Direct Method estimator
is single-step, so it cannot credit the long-horizon value that Q-learning exists to capture — the
held-out mean `|TD error|` printed by the trainer is the complementary check. And the fitted Q is
trained on the non-held-out split while policy values are computed over all events, the same
footing as the reward model itself.
```

- [ ] **Step 5: Verify the docs render and links resolve**

Run: `cd recsys-pipeline && grep -n "post_train_q" README.md docs/recommendation_architecture/Analysis_Report.md`
Expected: matches in both files.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/README.md \
        recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md
git commit -m "docs: document the offline Q post-training script and its estimator caveats"
```

---

## Self-Review Notes

**Spec coverage.** Data layer → Task 1. Tabular arm → Task 2. FQI arm → Task 3. `model:{key}` evaluation hookup and TD residual → Task 4. File layout → Tasks 1–4. Success criteria 1–5 → tests in Tasks 1–4 plus the Task 5 run. Risk "training data availability" → Task 5 Step 1. Risk "dependency declaration" → Task 3 Step 4. Risk "small-sample fits" → documented in Task 5 Step 4.

**Deviation from the spec.** The spec claims the evaluation hookup needs *zero* changes to `ope_eval_report.py`. That turned out to be false in one narrow respect, verified empirically while writing this plan: `candidates_of` crashes on Parquet-loaded events with 2+ candidates, so `--parquet` is broken today regardless of this feature. Task 0 fixes it in one line with a regression test. `pick()` and `policy_names()` are genuinely untouched, so the spec's substantive claim — that the fitted policies register themselves — holds.

**Why Task 0 comes first.** Every later task's tests round-trip fixtures through Parquet. Fixing the loader last would mean four tasks of tests written against a path known to be broken.

**Deliberate omissions.** Trained artifacts are not persisted to `sampledata/` in this plan: nothing consumes them yet, and the scored Parquet is the actual hand-off to the evaluator. Persisting weights is speculative until something reloads them. The spec's mention of artifact location stands as the convention for when that need arises.

**Type consistency.** `Transition` field names are identical across Tasks 1–4. `max_next_q(q, transition)` keeps one signature in `tabular_q` and `post_train_q`. `FittedQ.score_many` takes a list of feature rows and returns a list of floats everywhere it is called.
