# Optimize DPO Training Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop building feature vectors for replay candidates that no preference pair ever uses.

**Architecture:** `replay_index` holds candidate dicts instead of extracted features and returns the indexed request-id set it already walks. `build_pairs` resolves a key to `(features, reference, has_reference)` through a memo populated on first use, partitions each slate in one pass, and builds each item's key once.

**Tech Stack:** Python 3, numpy, torch, pytest.

**Spec:** [Optimize DPO training design](../specs/2026-09-14-optimize-dpo-training-design.md)

## Global Constraints

- Python 3 with numpy and torch as already pinned; no new dependencies.
- Preserve `build_pairs(slates, events, names=None)` and its `(pairs, dropped, JoinDiagnostics)` return shape. `PreferencePair`, `JoinDiagnostics` and `REFERENCE_PRED_KEY` keep their fields and values.
- Preserve `is_chosen(item)`, which is called directly by tests.
- Produce pairs in the same order, with the same field values, as today. `dropped`, `n_slate_request_ids`, `n_slate_request_ids_matched` and `n_missing_reference_sides` must be identical for every input.
- Preserve the reference semantics exactly: an absent or null `modelPredictions`, or an absent `predictionScore`, reads as 0.0 and counts toward `n_missing_reference_sides`.
- `replay_index` may change shape: it has no caller outside this module and no test of its own.
- Extract each used candidate's features at most once. The memo must not turn reuse across pairs into repeated extraction.

## Environment

Run tests with the modeling interpreter from the repository root:

```bash
/Users/linghuang/miniconda3/bin/python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py -q
```

## File Structure

- `recsys-pipeline/services/python-modeling/post-training/slate_pairs.py` — the whole change: `replay_index` reshaped, `build_pairs` internals reworked. No other module is touched.
- `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py` — gains an equivalence test against a frozen copy of today's implementation, plus the two call-count tests.

---

### Task 0: Publish the design first

**Files:**
- Create: `.superpowers/docs/specs/2026-09-14-optimize-dpo-training-design.md`
- Create: `.superpowers/docs/plans/2026-09-14-optimize-dpo-training.md`

**Interfaces:** Produces the branch and draft PR that Tasks 1-2 commit into. No code.

- [ ] **Step 1: Commit the spec and plan on a branch off `master`.**

```bash
git checkout master && git pull
git checkout -b optimize/dpo-training
git add .superpowers/docs/specs/2026-09-14-optimize-dpo-training-design.md \
        .superpowers/docs/plans/2026-09-14-optimize-dpo-training.md
git commit -m "docs: specify the DPO training optimization"
```

- [ ] **Step 2: Push and open a draft PR against `master`** titled `Optimize DPO training`, whose body carries the problem, the measured baseline table, and the note that `dpo.fit` is deliberately out of scope. Use `gh pr create --draft --body-file`. Record the PR number here.

---

### Task 1: Extract features only for the candidates a pair uses

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/post-training/slate_pairs.py:71-91` (`replay_index`) and `:93-142` (`build_pairs`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py`

**Interfaces:**
- Consumes: `ope_eval_report.candidates_of(event)`, `ope_eval_report.candidate_features(candidate, names)`, `ope_eval_report.feature_names(events)`, and `replay_dataset.as_list`, all unchanged.
- Produces: `replay_index(events) -> tuple[dict, set]` — `(requestId, item)` to the candidate dict, plus every indexed requestId. `build_pairs(slates, events, names=None)` keeps its exact signature and return shape.

- [ ] **Step 1: Write the equivalence and call-count tests.** Append to `test_post_training_dpo.py`. The equivalence test compares against a frozen copy of today's implementation, so it is an independent oracle rather than hand-written expectations. The two call-count tests fail against the current eager implementation.

```python
def _eager_build_pairs(slates, events, names=None):
    """A frozen copy of the pre-optimization implementation, as an equivalence oracle.

    Deliberately duplicated rather than imported: the point is to compare the optimized join
    against the eager one, so this must not change when slate_pairs does.
    """
    import ope_eval_report as oer
    from replay_dataset import as_list
    if names is None:
        names = oer.feature_names(events)
    index = {}
    for event in events:
        request_id = str(event.get("requestId", ""))
        for candidate in oer.candidates_of(event):
            predictions = candidate.get("modelPredictions") or {}
            reference = predictions.get(slate_pairs.REFERENCE_PRED_KEY)
            index[(request_id, str(candidate.get("item")))] = (
                oer.candidate_features(candidate, names),
                float(reference or 0.0),
                reference is not None,
            )
    indexed_request_ids = {key[0] for key in index}
    pairs, dropped, missing_reference_sides = [], 0, 0
    slate_request_ids = set()
    for slate in slates:
        request_id = str(slate.get("request_id", ""))
        slate_request_ids.add(request_id)
        user = str(slate.get("user_id", ""))
        items = as_list(slate.get("items"))
        chosen = [item for item in items if slate_pairs.is_chosen(item)]
        rejected = [item for item in items if not slate_pairs.is_chosen(item)]
        for win in chosen:
            for lose in rejected:
                win_key = (request_id, str(win.get("item_id")))
                lose_key = (request_id, str(lose.get("item_id")))
                if win_key not in index or lose_key not in index:
                    dropped += 1
                    continue
                win_features, win_reference, win_has = index[win_key]
                lose_features, lose_reference, lose_has = index[lose_key]
                missing_reference_sides += (not win_has) + (not lose_has)
                pairs.append(slate_pairs.PreferencePair(
                    request_id=request_id, user=user,
                    chosen_item=win_key[1], rejected_item=lose_key[1],
                    chosen_features=win_features, rejected_features=lose_features,
                    chosen_reference=win_reference, rejected_reference=lose_reference))
    return pairs, dropped, slate_pairs.JoinDiagnostics(
        n_slate_request_ids=len(slate_request_ids),
        n_slate_request_ids_matched=len(slate_request_ids & indexed_request_ids),
        n_missing_reference_sides=missing_reference_sides)


def _join_fixtures():
    """(label, slates, events) covering every join outcome the spec enumerates."""
    def candidate(item, prediction=0.5, **overrides):
        entry = {"item": item, "coldStart": False, "impressions": 10, "clicks": 2,
                 "modelPredictions": {"relevance": 0.4, "predictionScore": prediction}}
        entry.update(overrides)
        return entry

    def event(rid, cands):
        return {"requestId": rid, "user": "u", "action": cands[0]["item"], "reward": 1.0,
                "clicked": 1, "modelPredictions": {"relevance": 0.5}, "actionSpace": cands}

    def slate(rid, items):
        return {"request_id": rid, "user_id": "u", "items": items}

    def item(iid, clicked=0, label=None):
        return {"item_id": iid, "clicked": clicked, "label": label}

    ordinary_events = [event("r1", [candidate("a", 0.9), candidate("b", 0.2),
                                    candidate("c", 0.1)])]
    return [
        ("pairs exist", [slate("r1", [item("a", clicked=1), item("b"), item("c")])],
         ordinary_events),
        ("two chosen, two rejected",
         [slate("r1", [item("a", clicked=1), item("b", label=0.7), item("c")])],
         ordinary_events),
        ("total join failure", [slate("nope", [item("a", clicked=1), item("b")])],
         ordinary_events),
        ("no engagement", [slate("r1", [item("a"), item("b")])], ordinary_events),
        ("no unengaged item", [slate("r1", [item("a", clicked=1), item("b", clicked=1)])],
         ordinary_events),
        ("one side missing from the replay",
         [slate("r1", [item("a", clicked=1), item("ghost")])], ordinary_events),
        ("null modelPredictions",
         [slate("r1", [item("a", clicked=1), item("b")])],
         [event("r1", [candidate("a", 0.9), dict(candidate("b"), modelPredictions=None)])]),
        ("predictionScore absent",
         [slate("r1", [item("a", clicked=1), item("b")])],
         [event("r1", [candidate("a", 0.9),
                       dict(candidate("b"), modelPredictions={"relevance": 0.4})])]),
        ("no slates", [], ordinary_events),
        ("no events", [slate("r1", [item("a", clicked=1), item("b")])], []),
    ]


def test_build_pairs_matches_the_eager_join_on_every_outcome():
    """The lazy join must be indistinguishable from the eager one it replaces."""
    for label, slates, events in _join_fixtures():
        names = ope_eval_report.feature_names(events) if events else []
        actual_pairs, actual_dropped, actual_diag = slate_pairs.build_pairs(slates, events, names)
        want_pairs, want_dropped, want_diag = _eager_build_pairs(slates, events, names)
        assert actual_pairs == want_pairs, label
        assert actual_dropped == want_dropped, label
        assert actual_diag == want_diag, label


def test_build_pairs_extracts_features_only_for_candidates_a_pair_uses(monkeypatch):
    """At low click-through most candidates never reach a pair; their features cost nothing.

    Fails against an index that extracts every candidate's features up front.
    """
    def candidate(item):
        return {"item": item, "coldStart": False, "impressions": 3, "clicks": 1,
                "modelPredictions": {"relevance": 0.4, "predictionScore": 0.5}}

    # Two slates, one engaged: only the engaged slate's three items can form pairs.
    events = [
        {"requestId": "r1", "user": "u", "action": "a", "reward": 1.0, "clicked": 1,
         "modelPredictions": {}, "actionSpace": [candidate("a"), candidate("b"), candidate("c")]},
        {"requestId": "r2", "user": "u", "action": "d", "reward": 0.0, "clicked": 0,
         "modelPredictions": {}, "actionSpace": [candidate("d"), candidate("e"), candidate("f")]},
    ]
    slates = [
        {"request_id": "r1", "user_id": "u", "items": [
            {"item_id": "a", "clicked": 1}, {"item_id": "b", "clicked": 0},
            {"item_id": "c", "clicked": 0}]},
        {"request_id": "r2", "user_id": "u", "items": [
            {"item_id": "d", "clicked": 0}, {"item_id": "e", "clicked": 0},
            {"item_id": "f", "clicked": 0}]},
    ]
    extracted = []
    original = ope_eval_report.candidate_features
    monkeypatch.setattr(ope_eval_report, "candidate_features",
                        lambda cand, names: (extracted.append(cand["item"]),
                                             original(cand, names))[1])

    pairs, _, _ = slate_pairs.build_pairs(slates, events)

    assert len(pairs) == 2                       # a beats b, a beats c
    # r2 contributed nothing, so d/e/f are never extracted; `a` is on both pairs but extracted once
    assert sorted(extracted) == ["a", "b", "c"]


def test_build_pairs_tests_each_slate_item_for_engagement_once(monkeypatch):
    """One partitioning pass, not a comprehension per side."""
    events = [{"requestId": "r1", "user": "u", "action": "a", "reward": 1.0, "clicked": 1,
               "modelPredictions": {}, "actionSpace": [
                   {"item": i, "coldStart": False, "impressions": 1, "clicks": 0,
                    "modelPredictions": {"predictionScore": 0.5}} for i in ("a", "b", "c", "d")]}]
    slates = [{"request_id": "r1", "user_id": "u", "items": [
        {"item_id": "a", "clicked": 1}, {"item_id": "b", "clicked": 0},
        {"item_id": "c", "clicked": 0}, {"item_id": "d", "clicked": 0}]}]

    calls = []
    original = slate_pairs.is_chosen
    monkeypatch.setattr(slate_pairs, "is_chosen",
                        lambda item: (calls.append(item["item_id"]), original(item))[1])

    slate_pairs.build_pairs(slates, events)

    assert sorted(calls) == ["a", "b", "c", "d"]
```

- [ ] **Step 2: Run the three new tests and confirm the two call-count tests fail.**

```bash
/Users/linghuang/miniconda3/bin/python3 -m pytest \
  recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py -q \
  -k "matches_the_eager_join or extracts_features_only or engagement_once"
```

Expected: the equivalence test passes (it compares the current implementation against a copy of itself); the two call-count tests FAIL — `candidate_features` is called for all six candidates rather than three, and `is_chosen` is called eight times rather than four. Record both messages.

- [ ] **Step 3: Reshape `replay_index`.** Replace it entirely.

```python
def replay_index(events) -> tuple[dict, set]:
    """(requestId, item) -> the replay candidate that can supply a pair's features.

    Holds the candidate dict rather than an extracted feature vector. A pair exists only where a
    slate carried both an engagement and a non-engaged item, so at low click-through most
    candidates never reach one and extracting their features here is work thrown away -- measured
    at 95% discarded when 5% of slates are engaged, and 100% when the slate and replay id
    namespaces do not match at all. `build_pairs` extracts on first use instead.

    The returned set is every indexed requestId, collected in this loop rather than recovered by a
    second pass over the keys. It answers only whether the join works at all.
    """
    index = {}
    request_ids = set()
    for event in events:
        request_id = str(event.get("requestId", ""))
        request_ids.add(request_id)
        for candidate in ope_eval_report.candidates_of(event):
            index[(request_id, str(candidate.get("item")))] = candidate
    return index, request_ids
```

- [ ] **Step 4: Rework `build_pairs`'s body.** Keep the signature and docstring; replace from `index = replay_index(...)` to the end of the slate loop.

```python
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
```

- [ ] **Step 5: Run the three new tests and confirm they now pass.**

```bash
/Users/linghuang/miniconda3/bin/python3 -m pytest \
  recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py -q \
  -k "matches_the_eager_join or extracts_features_only or engagement_once"
```

Expected: 3 passed.

- [ ] **Step 6: Run the whole DPO suite, then the whole modeling suite.**

```bash
/Users/linghuang/miniconda3/bin/python3 -m pytest \
  recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py -q
/Users/linghuang/miniconda3/bin/python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q
```

Expected: zero failures in both. Record the counts.

- [ ] **Step 7: Commit.**

```bash
git add recsys-pipeline/services/python-modeling/post-training/slate_pairs.py \
        recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py
git commit -m "perf(dpo): extract features only for the candidates a pair uses"
```

---

### Task 2: Verify and deliver

**Files:** Task 1 files, plus this plan.

**Interfaces:** Consumes the tested diff; produces a reviewed PR against `master`.

- [ ] **Step 1: Benchmark before and after.** Create the throwaway harness below at the scratchpad path, not inside the repository.

```python
import sys, time, statistics
PM = "/Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/python-modeling"
sys.path.insert(0, PM); sys.path.insert(0, PM + "/post-training")
import numpy as np
import ope_eval_report, slate_pairs

def make(n_events=5000, ncand=10, engaged_frac=0.05, matched=True, seed=5):
    rng = np.random.default_rng(seed)
    events, slates = [], []
    for i in range(n_events):
        cands = [{"item": f"m{i}_{j}", "coldStart": False, "impressions": int(rng.integers(0,500)),
                  "clicks": int(rng.integers(0,40)),
                  "modelPredictions": {"relevance": float(rng.random()),
                                       "predictionScore": float(rng.random())}}
                 for j in range(ncand)]
        rid = f"req{i}"
        events.append({"requestId": rid, "user": "u", "action": cands[0]["item"], "reward": 1.0,
                       "clicked": 1, "modelPredictions": {"relevance": 0.5}, "actionSpace": cands})
        engaged = i < n_events * engaged_frac
        items = [{"item_id": c["item"], "clicked": 1 if (engaged and j == 0) else 0, "label": None}
                 for j, c in enumerate(cands)]
        slates.append({"request_id": rid if matched else f"unmatched{i}",
                       "user_id": "u", "items": items})
    return events, slates

def med(fn, reps=5):
    ts = []
    for _ in range(reps):
        t0 = time.perf_counter(); r = fn(); ts.append(time.perf_counter() - t0)
    return statistics.median(ts), r

for label, ef, matched in (("100% engaged", 1.0, True), ("30% engaged", 0.30, True),
                           ("5% engaged", 0.05, True), ("join fails", 1.0, False)):
    events, slates = make(engaged_frac=ef, matched=matched)
    names = ope_eval_report.feature_names(events)
    t_all, (pairs, dropped, diag) = med(lambda: slate_pairs.build_pairs(slates, events, names))
    print(f"  {label:14s} build_pairs {t_all*1000:8.1f} ms  -> {len(pairs)} pairs, {dropped} dropped")
```

Run it on this branch, then against the pre-change module, and record both columns:

```bash
SP=/private/tmp/claude-501/-Users-linghuang-Git-Recsys-Streaming-Pipeline/a758adab-7b7b-4b65-846d-707c251c9cb3/scratchpad
/Users/linghuang/miniconda3/bin/python3 $SP/bench_dpo.py                      # after
git stash push recsys-pipeline/services/python-modeling/post-training/slate_pairs.py
/Users/linghuang/miniconda3/bin/python3 $SP/bench_dpo.py                      # before
git stash pop
```

Compare against the spec's baseline: `build_pairs` 172.6 / 141.0 / 107.1 / 95.8 ms at 100% / 30% / 5% engaged and the zero-join case. Expect a large gain at 5% and zero-join, little or none at 100%.

- [ ] **Step 2: Confirm the tree is clean.**

```bash
git status --short
```

Expected: no harness inside the repository, no stash left behind.

- [ ] **Step 3: Request a read-only code review.** Ask for a review against every spec constraint, specifically: that pair order and every field are unchanged; that the memo cannot return a stale or shared-mutable feature list; that `missing_reference_sides` still counts once per pair side rather than once per distinct key; and that the `key not in index` check still precedes any extraction so a dropped pair costs nothing. Resolve substantive findings before publishing.

- [ ] **Step 4: Publish.** Push, record the measured before/after and suite counts in the PR body, fill in this plan's verification record, commit it, and mark the PR ready.

## Verification record

Execution pending.
