# OPE Bootstrap Confidence Intervals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic event-level 95% bootstrap confidence intervals for Direct Method policy value and lift.

**Architecture:** Keep reward-model fitting and point estimates unchanged, then enrich copied result rows with paired percentile-bootstrap intervals using whole replay events and a fixed fitted model. CLI flags control sample count and seed; console and CSV share explicit unavailable-value semantics.

**Tech Stack:** Python 3, NumPy, pytest, argparse, standard-library CSV.

## Global Constraints

- Do not change serving, replay schemas, policy definitions, dependencies, or reward-model fitting.
- Default to 1,000 resamples and seed `20260716`.
- State that intervals are conditional on the fixed reward model and exclude model-fit uncertainty.
- Represent unavailable intervals as `None` in Python, blank CSV cells, and `N/A` in console output.

---

## File Structure

- Modify `recsys-pipeline/services/python-modeling/ope_eval_report.py` for lift semantics, bootstrap calculation, CLI controls, and reporting.
- Modify `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py` for deterministic, edge-case, and output coverage.

### Task 1: Undefined Lift and Bootstrap Core

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py:145-178`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`

**Interfaces:**
- Consumes: existing `evaluate(events, model)` rows.
- Produces: `bootstrap_intervals(events, model, point_rows, samples=1000, seed=20260716) -> list[dict]` with `value_ci_low`, `value_ci_high`, `lift_ci_low`, and `lift_ci_high`.

- [ ] **Step 1: Write failing behavioral tests**

Add `import pytest` and append:

```python
def _interval_rows(events, samples=120, seed=17):
    model = ope.fit_reward_model(events)
    points = ope.evaluate(events, model)
    return ope.bootstrap_intervals(events, model, points, samples=samples, seed=seed)


def test_bootstrap_is_deterministic_and_does_not_mutate_points():
    events = _dataset(80)
    model = ope.fit_reward_model(events)
    points = ope.evaluate(events, model)
    snapshot = [dict(row) for row in points]
    assert (ope.bootstrap_intervals(events, model, points, 80, 31)
            == ope.bootstrap_intervals(events, model, points, 80, 31))
    assert points == snapshot


def test_bootstrap_intervals_contain_stable_points():
    rows = _interval_rows(_dataset(100), samples=160, seed=7)
    for row in rows:
        assert row["value_ci_low"] <= row["value"] <= row["value_ci_high"]
    logging = next(row for row in rows if row["policy"] == "logging")
    assert (logging["lift_ci_low"], logging["lift_ci_high"]) == (0.0, 0.0)


def test_bootstrap_edge_cases():
    one = _interval_rows([_dataset(1)[0]], samples=20, seed=3)
    assert all(row["value_ci_low"] == row["value_ci_high"] for row in one)
    disabled = _interval_rows(_dataset(20), samples=0)
    assert all(all(row[field] is None for field in ope.INTERVAL_FIELDS)
               for row in disabled)


def test_zero_reward_has_value_intervals_but_no_lift():
    events = _dataset(40)
    for event in events:
        event["reward"] = 0.0
        event["clicked"] = 0
    rows = _interval_rows(events, samples=50, seed=5)
    for row in rows:
        assert row["value_ci_low"] is not None
        assert row["lift_vs_logging"] is None
        assert row["lift_ci_low"] is None
        assert row["lift_ci_high"] is None


def test_negative_bootstrap_samples_are_rejected():
    events = _dataset(20)
    model = ope.fit_reward_model(events)
    with pytest.raises(ValueError, match="bootstrap samples must be nonnegative"):
        ope.bootstrap_intervals(events, model, ope.evaluate(events, model), samples=-1)
```

- [ ] **Step 2: Verify the tests fail for missing behavior**

Run `pytest -q recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py -k 'bootstrap or zero_reward'`.

Expected: failures because `bootstrap_intervals` and `INTERVAL_FIELDS` do not exist and zero-reward lift is currently `0.0`.

- [ ] **Step 3: Correct undefined lift in `evaluate`**

Replace its lift and row fields with:

```python
        lift = (value / logging_value - 1.0) if logging_value > 0.0 else None
        rows.append({
            "policy": name,
            "value": round(value, 4),
            "lift_vs_logging": round(lift, 4) if lift is not None else None,
            "n_events": count,
            "estimator_auc": model.calibration["auc"],
            "estimator_mse": model.calibration["mse"],
        })
```

- [ ] **Step 4: Implement pure paired bootstrap enrichment below `evaluate`**

```python
INTERVAL_FIELDS = ["value_ci_low", "value_ci_high", "lift_ci_low", "lift_ci_high"]


def _percentile_bounds(values):
    if not values:
        return None, None
    low, high = np.percentile(np.asarray(values, dtype=float), [2.5, 97.5])
    return round(float(low), 4), round(float(high), 4)


def bootstrap_intervals(events, model, point_rows, samples=1000, seed=20260716):
    if samples < 0:
        raise ValueError("bootstrap samples must be nonnegative")
    enriched = [{**row, **{field: None for field in INTERVAL_FIELDS}}
                for row in point_rows]
    if samples == 0 or not events:
        return enriched
    stats = {row["policy"]: {"value": [], "lift": []} for row in point_rows}
    rng = np.random.default_rng(seed)
    for _ in range(samples):
        indexes = rng.integers(0, len(events), size=len(events))
        sampled = [events[int(index)] for index in indexes]
        for row in evaluate(sampled, model):
            stats[row["policy"]]["value"].append(float(row["value"]))
            if row["lift_vs_logging"] is not None:
                stats[row["policy"]]["lift"].append(float(row["lift_vs_logging"]))
    for row in enriched:
        policy_stats = stats[row["policy"]]
        row["value_ci_low"], row["value_ci_high"] = _percentile_bounds(policy_stats["value"])
        row["lift_ci_low"], row["lift_ci_high"] = _percentile_bounds(policy_stats["lift"])
    return enriched
```

- [ ] **Step 5: Run OPE tests and commit**

Run `pytest -q recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`.

Expected: all OPE tests pass.

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
git commit -m "feat(ope): add paired bootstrap intervals"
```

### Task 2: CLI, Console, and CSV Integration

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py:181-212`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`

**Interfaces:**
- Consumes: `bootstrap_intervals` and `INTERVAL_FIELDS` from Task 1.
- Produces: `--bootstrap-samples`, `--bootstrap-seed`, formatted console intervals, and interval CSV columns.

- [ ] **Step 1: Replace the CSV integration test**

```python
def test_main_reads_redis_and_writes_csv(tmp_path, capsys):
    import csv
    import json
    from unittest.mock import MagicMock, patch
    events = _dataset(40)
    client = MagicMock()
    client.lrange.return_value = [json.dumps(e).encode() for e in events]
    out = tmp_path / "ope.csv"
    with patch("ope_eval_report.redis.Redis", return_value=client):
        rows = ope.main(["--output", str(out), "--bootstrap-samples", "30",
                         "--bootstrap-seed", "11"])
    stdout = capsys.readouterr().out
    assert "conditional on fixed reward model" in stdout
    assert "95% CI" in stdout
    with out.open(newline="") as fh:
        csv_rows = list(csv.DictReader(fh))
    assert set(ope.INTERVAL_FIELDS).issubset(csv_rows[0])
    assert any(row["policy"] == "logging" for row in rows)


def test_main_rejects_negative_bootstrap_samples():
    with pytest.raises(SystemExit):
        ope.main(["--bootstrap-samples", "-1"])
```

- [ ] **Step 2: Verify both CLI tests fail**

Run `pytest -q recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py -k 'main_'`.

Expected: failures because the new options and CSV fields are absent.

- [ ] **Step 3: Add CLI options and validation**

```python
    ap.add_argument("--bootstrap-samples", type=int, default=1000,
                    help="event bootstrap resamples; 0 disables intervals (default: 1000)")
    ap.add_argument("--bootstrap-seed", type=int, default=20260716,
                    help="deterministic bootstrap seed (default: 20260716)")
    args = ap.parse_args(argv)
    if args.bootstrap_samples < 0:
        ap.error("--bootstrap-samples must be nonnegative")
```

After fitting the model, replace row calculation with:

```python
    point_rows = evaluate(events, model)
    rows = bootstrap_intervals(events, model, point_rows,
                               samples=args.bootstrap_samples,
                               seed=args.bootstrap_seed)
```

- [ ] **Step 4: Add output formatting and fields**

Add above `main`:

```python
def _fmt_number(value, signed=False):
    if value is None:
        return "N/A"
    return f"{value:+.4f}" if signed else f"{value:.4f}"


def _fmt_interval(low, high, signed=False):
    if low is None or high is None:
        return "N/A"
    return f"[{_fmt_number(low, signed)}, {_fmt_number(high, signed)}]"
```

Replace row printing with:

```python
    print("95% event-bootstrap CIs are conditional on fixed reward model; model-fit uncertainty excluded")
    for r in rows:
        value_ci = _fmt_interval(r["value_ci_low"], r["value_ci_high"])
        lift_ci = _fmt_interval(r["lift_ci_low"], r["lift_ci_high"], signed=True)
        print(f"  {r['policy']:16s} value={_fmt_number(r['value'])}  95% CI={value_ci}  "
              f"lift={_fmt_number(r['lift_vs_logging'], signed=True)}  "
              f"95% lift CI={lift_ci}  n={r['n_events']}")
```

Use CSV fieldnames:

```python
            fieldnames = ["policy", "value", "lift_vs_logging", "n_events",
                          "estimator_auc", "estimator_mse", *INTERVAL_FIELDS]
            wr = csv.DictWriter(fh, fieldnames=fieldnames)
```

- [ ] **Step 5: Run the complete focused suite and commit**

Run:

```bash
pytest -q recsys-pipeline/integration-tests/python_modeling/test_logistic.py recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py recsys-pipeline/integration-tests/python_modeling/test_replay_buffer.py recsys-pipeline/integration-tests/python_modeling/test_replay_export.py
git diff --check
```

Expected: pytest reports zero failures and `git diff --check` prints nothing.

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
git commit -m "feat(ope): report bootstrap confidence intervals"
```

### Task 3: Final Design Verification

**Files:**
- Verify: `docs/superpowers/specs/2026-07-16-ope-bootstrap-confidence-intervals-design.md`
- Verify: the two implementation files above.

**Interfaces:**
- Consumes: completed commits from Tasks 1 and 2.
- Produces: fresh evidence for the handoff.

- [ ] **Step 1: Review the final diff against every acceptance criterion**

Run `git show --stat --oneline HEAD~2..HEAD` and `git diff HEAD~2..HEAD -- recsys-pipeline/services/python-modeling/ope_eval_report.py recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`.

Expected: deterministic paired bootstrap, four interval fields, two CLI flags, undefined lift, limitation text, and all specified edge-case tests are present.

- [ ] **Step 2: Run fresh verification**

Run:

```bash
pytest -q recsys-pipeline/integration-tests/python_modeling/test_logistic.py recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py recsys-pipeline/integration-tests/python_modeling/test_replay_buffer.py recsys-pipeline/integration-tests/python_modeling/test_replay_export.py
git diff --check
git status --short
```

Expected: zero test failures, no whitespace errors, and only pre-existing planning artifacts remain untracked.

- [ ] **Step 3: Report exact evidence**

The handoff must include the pytest pass count and duration, defaults `1000` and `20260716`, the conditional-on-fixed-model limitation, and the implementation commit hashes.
