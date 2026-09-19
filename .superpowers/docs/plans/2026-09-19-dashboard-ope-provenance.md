# Make the off-policy section reachable and honest — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Give the dashboard's off-policy section the Parquet input its sibling `ope_eval_report.py` already has, so the four post-training arms can appear; state which source produced the rows; and stop the N/A text and one README line from claiming things that are not true.

**Architecture:** Nothing about the estimator changes. `policy_names` already admits `dpoScore`, `tabQ`, `fqiQ` and `grpoScore` as `model:*` policies, and `feature_names` already excludes them from the reward model's features — verified by running the real functions over a real Parquet fixture. The only missing piece is an input path, so this is one new optional parameter threaded through two functions, plus three strings that were false.

**Tech Stack:** Python 3 / pytest (`pytest.ini` sets `testpaths = integration-tests`), pandas + pyarrow for the Parquet fixture, one JSX string in a React component. No Scala.

**Spec:** `.superpowers/docs/specs/2026-09-19-dashboard-ope-provenance-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `compute_ope`'s existing five parameters keep their names, order and defaults. `parquet` is added last, defaulting to `None`.
- `build()` in `export_dashboard_json.py` gains `ope_parquet` as its **last** parameter, after `config`. Its only caller passes all six arguments positionally at line 147, so inserting the new parameter next to the other inputs would land the config dict in `ope_parquet`.
- `MEASUREMENT_SCHEMA_VERSION` stays `"2.0"`. `validate_measurements.mjs` neither lists `ope` in `DIAGNOSTIC_ROWS` nor rejects unknown keys, so `source` needs no validator change and the unregenerated snapshot stays valid.
- `frontend/components/sections.jsx` is touched only inside `OpeSection`: the `NaCard` reason and one fine-print line. No other section, no layout, no CSS.
- Every consumer treats `source` as optional, because the committed snapshot predates it.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.
- Measured baseline at this branch point: **563 passed, 2 skipped**. This plan adds five tests, so the final expected result is **568 passed, 2 skipped**.

## Execution record

Executed 2026-09-19 on `fix/dashboard-ope-provenance`, four commits, all steps checked.
Final suite: **569 passed, 2 skipped**, from a 563/2 baseline.

Three deviations:

1. **Six tests, not five.** The Global Constraints line said five; the tasks define six (four data
   path, one exporter, one guard). 563 + 6 = 569, not the 568 predicted. Counting the tasks rather
   than trusting the summary line would have caught it.
2. **The guard had to become sentence-bounded.** The plan's version flagged any line containing both
   `ExperienceCollectorStreamingJob` and `replay:recommendations`. A markdown paragraph is one long
   line, so it fired on the *corrected* text too -- two sentences asserting the opposite of the false
   claim still put both tokens on one line. Now it splits on sentence boundaries. Verified both ways:
   it fails on the sentence removed and passes on the replacement.
3. **The skip set was incomplete.** The plan skipped only `node_modules` and `.git`, so the guard
   flagged `.worktrees/standalone-retrieval/` -- a whole second checkout, including historical
   `.superpowers` docs. Aligned with `test_retrieval_service_extracted.py`'s established
   `SKIP_DIRS`. A new repository-wide scan should copy that set rather than invent one.

Also worth recording: the acceptance command `git diff --name-only origin/master | grep 'dashboard.json'`
gives a false positive, because `export_dashboard_json.py` contains that substring. Use `grep -x` with
the full path.

## Pre-validated facts

These were measured before the plan was written, by running the real functions — do not re-derive them:

- A replay event with nested `modelPredictions` and `actionSpace` survives `to_parquet` → `ope_support.load_from_parquet`. `modelPredictions` returns as a `dict`; `actionSpace` returns as an `ndarray`, which `candidates_of` already handles deliberately.
- For events carrying `relevance`, `dpoScore` and `tabQ`, `ope_eval_report.policy_names` returns `['logging', 'popularity', 'ctr', 'random', 'model:dpoScore', 'model:relevance', 'model:tabQ']` and `feature_names` returns `['coldStart', 'impressions', 'clicks', 'relevance']` — the post-training keys excluded.
- `bootstrap_intervals` produces a row per policy for that fixture at `samples=20`.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py` (`compute_ope`, lines 259-286) | Optional Parquet source; `source` in the returned dict. |
| Modify: `recsys-pipeline/frontend/export_dashboard_json.py` (`build`, `parse_args`, `main`) | Thread `ope_parquet` from a new `--ope-parquet` flag. |
| Modify: `recsys-pipeline/frontend/components/sections.jsx` (`OpeSection`, lines 590-626) | Truthful N/A reason; show `source` when present. |
| Modify: `recsys-pipeline/README.md:314` | Correct the writer claim. |
| Create: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py` | The four data-path tests plus the false-claim guard. |
| Modify: `recsys-pipeline/frontend/README.md` · `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md` | Document the two sources and the flag. |

---

### Task 1: The Parquet source and its provenance

**Files:**
- Create: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py`
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py` (`compute_ope`)

**Interfaces:**
- Consumes: nothing.
- Produces: `compute_ope(host, port, key="replay:recommendations", limit=-1, bootstrap_samples=1000, parquet=None)` returning `{"headline": str, "rows": list[dict], "calibration": dict, "source": str}` where `source` is `f"parquet:{parquet}"` or `f"redis:{key}"`. Task 2 passes `parquet=` and reads `source`.

- [x] **Step 1: Write the failing tests**

Create `recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py`. The fixture mirrors `test_compute_ope_evaluates_from_replay_events` in `test_analysis_dashboard.py`, with two post-training keys added.

```python
"""The off-policy section reads two sources, and says which one it read.

compute_ope read Redis only. The four post-training arms -- tabQ, fqiQ, dpoScore,
grpoScore -- are written by post_train_q.py and post_train_dpo.py into each
candidate's modelPredictions and persisted with --output-parquet "for
ope_eval_report.py --parquet", so they existed on disk and the dashboard had no way
to read them. policy_names already admits them; only the input path was missing.

Nothing in this repository writes replay:recommendations -- no .scala file mentions
it and every Python reference is a reader -- so the Redis source is populated by the
serving path in lingduoduo/Recsys-Backend-Service, not from here.
"""

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "services" / "python-modeling"))
sys.path.insert(0, str(REPO / "frontend"))


def _event(i, extra_predictions=None):
    """One replay event, shaped like what the serving path writes."""
    rel = (i % 10) / 10.0
    reward = 1.0 if rel >= 0.5 else 0.0
    predictions = {"relevance": rel}
    predictions.update(extra_predictions or {})
    return {"requestId": f"r{i}", "user": "u", "action": f"m{i}", "actionPosition": 0,
            "coldStart": False, "modelPredictions": dict(predictions),
            "reward": reward, "clicked": int(reward),
            "actionSpace": [{"item": f"m{i}", "coldStart": False, "impressions": i % 50,
                             "clicks": int(reward), "modelPredictions": dict(predictions)}]}


def _scored_parquet(tmp_path, events):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    path = tmp_path / "scored-replay.parquet"
    pd.DataFrame(events).to_parquet(path, index=False)
    return path


def test_parquet_source_surfaces_the_post_training_arms(tmp_path):
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash

    events = [_event(i, {"dpoScore": 1.0 - (i % 10) / 10.0, "tabQ": (i % 10) / 5.0})
              for i in range(120)]
    path = _scored_parquet(tmp_path, events)

    result = dash.compute_ope("localhost", 6399, parquet=str(path), bootstrap_samples=20)

    policies = {row["policy"] for row in result["rows"]}
    assert "model:dpoScore" in policies, "post-training DPO arm must appear as a policy"
    assert "model:tabQ" in policies, "post-training tabular-Q arm must appear as a policy"
    assert "logging" in policies


def test_parquet_source_is_named_in_the_payload(tmp_path):
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash

    path = _scored_parquet(tmp_path, [_event(i, {"dpoScore": 0.5}) for i in range(120)])
    result = dash.compute_ope("localhost", 6399, parquet=str(path), bootstrap_samples=20)
    assert result["source"] == f"parquet:{path}"


def test_redis_source_is_named_in_the_payload(monkeypatch):
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash
    import ope_support

    events = [_event(i) for i in range(120)]
    monkeypatch.setattr(ope_support, "load_from_redis", lambda *a, **k: events)
    result = dash.compute_ope("localhost", 6399, bootstrap_samples=20)
    assert result["source"] == "redis:replay:recommendations"


def test_parquet_takes_precedence_over_a_reachable_redis(tmp_path, monkeypatch):
    """Precedence matches replay_dataset.load_events: --parquet wins.

    Proven by which policies come back, not by asserting a loader was not called: the
    Redis events carry no dpoScore, so its presence can only come from the Parquet.
    """
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash
    import ope_support

    monkeypatch.setattr(ope_support, "load_from_redis",
                        lambda *a, **k: [_event(i) for i in range(120)])
    path = _scored_parquet(tmp_path, [_event(i, {"dpoScore": 1.0 - (i % 10) / 10.0})
                                      for i in range(120)])

    result = dash.compute_ope("localhost", 6399, parquet=str(path), bootstrap_samples=20)
    assert "model:dpoScore" in {row["policy"] for row in result["rows"]}
    assert result["source"].startswith("parquet:")
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_ope_sources.py -q`
Expected: FAIL — three with `TypeError: compute_ope() got an unexpected keyword argument 'parquet'`, and `test_redis_source_is_named_in_the_payload` with `KeyError: 'source'`.

- [x] **Step 3: Add the Parquet source and the provenance key**

The Redis path keeps its `except Exception: return None` for an unreachable Redis. A Parquet path that does not exist is a caller error, not a missing optional input, so its exception propagates — matching `ope_eval_report.py --parquet`.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("services/python-modeling/analysis_dashboard_report.py")
t = p.read_text()

old_sig = '''def compute_ope(host, port, key="replay:recommendations", limit=-1, bootstrap_samples=1000):
    """Direct-Method off-policy evaluation over the Redis replay buffer (reuses ope_eval_report)."""
    try:
        import redis
        import ope_support as replay_buffer
        client = redis.Redis(host=host, port=port, decode_responses=False)
        events = replay_buffer.load_from_redis(client, key, limit)
    except Exception:  # noqa: BLE001 — Redis unreachable / no buffer
        return None'''
new_sig = '''def compute_ope(host, port, key="replay:recommendations", limit=-1, bootstrap_samples=1000,
                parquet=None):
    """Direct-Method off-policy evaluation over a replay buffer (reuses ope_eval_report).

    Two sources, with the same precedence as replay_dataset.load_events: `parquet` wins when
    given. Redis holds what the serving path scored -- this repository writes no
    replay:recommendations. The Parquet is what post_train_dpo.py / post_train_q.py write with
    --output-parquet, and is the only source carrying the post-training arms (dpoScore, tabQ,
    fqiQ, grpoScore). A missing Parquet path raises: it was asked for explicitly, unlike Redis,
    whose absence is an ordinary N/A.
    """
    import ope_support as replay_buffer
    if parquet:
        events = replay_buffer.load_from_parquet(parquet)
        source = f"parquet:{parquet}"
    else:
        try:
            import redis
            client = redis.Redis(host=host, port=port, decode_responses=False)
            events = replay_buffer.load_from_redis(client, key, limit)
        except Exception:  # noqa: BLE001 — Redis unreachable / no buffer
            return None
        source = f"redis:{key}"'''
assert t.count(old_sig) == 1, "compute_ope's head does not match verbatim"
t = t.replace(old_sig, new_sig)

old_ret = '''    return {"headline": headline, "rows": rows, "calibration": cal}'''
new_ret = '''    return {"headline": headline, "rows": rows, "calibration": cal, "source": source}'''
assert t.count(old_ret) == 1
p.write_text(t.replace(old_ret, new_ret))
PY
sed -n '259,268p' services/python-modeling/analysis_dashboard_report.py
```

Expected: the new signature and docstring.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_ope_sources.py integration-tests/python_modeling/test_analysis_dashboard.py -q`
Expected: PASS — 4 new + 21 existing = 25 passed. The two existing `compute_ope` tests still pass because `parquet` defaults to `None`.

- [x] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py \
        recsys-pipeline/services/python-modeling/analysis_dashboard_report.py
git commit -m "$(cat <<'MSG'
feat(dashboard): read off-policy events from a Parquet, not only Redis

The four post-training arms were unreachable from the dashboard, not absent.
post_train_dpo.py and post_train_q.py write dpoScore, tabQ and fqiQ into each
candidate's modelPredictions and persist them with --output-parquet "for
ope_eval_report.py --parquet"; compute_ope read Redis only, so the scored
replay sat on disk with no way in.

Nothing else had to change: policy_names already admits those keys as model:*
policies and feature_names already excludes them from the reward model's
features, which is the circularity protection they were given. Verified
against a real Parquet fixture -- policy_names returns model:dpoScore and
model:tabQ, feature_names returns neither.

The payload now names its source, because "which world am I looking at" is
otherwise unanswerable from the rows.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: Thread the flag, and stop the N/A text claiming an empty buffer

**Files:**
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py` (append)
- Modify: `recsys-pipeline/frontend/export_dashboard_json.py` (`build`, `parse_args`, `main`)
- Modify: `recsys-pipeline/frontend/components/sections.jsx` (`OpeSection`)

**Interfaces:**
- Consumes: `compute_ope(..., parquet=...)` and the `source` key from Task 1.
- Produces: `build(input_dir, host, port, experiences=None, live_metrics=None, config=None, ope_parquet=None)` and the `--ope-parquet` CLI flag.

- [x] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py`:

```python
def test_exporter_threads_the_ope_parquet_flag_through_to_compute(tmp_path, monkeypatch):
    """--ope-parquet must reach compute_ope as `parquet`, not be silently dropped.

    build() takes ope_parquet LAST because its only caller passes the first six
    arguments positionally; inserting it beside the other inputs would land the
    config dict in it.
    """
    import export_dashboard_json as exporter

    seen = {}

    def _fake_compute_ope(host, port, **kwargs):
        seen.update(kwargs)
        return {"headline": "h", "rows": [], "calibration": {}, "source": "parquet:x"}

    monkeypatch.setattr(exporter.dash, "compute_ope", _fake_compute_ope)
    monkeypatch.setattr(exporter.dash, "load_samples", lambda *a, **k: _frame())
    monkeypatch.setattr(exporter.dash, "load_slates", lambda *a, **k: None)
    monkeypatch.setattr(exporter.dash, "compute_recall", lambda *a, **k: None)
    monkeypatch.setattr(exporter.dash, "compute_ranking", lambda *a, **k: None)

    exporter.build("in", "localhost", 6399, None, None, None, str(tmp_path / "scored.parquet"))
    assert seen.get("parquet") == str(tmp_path / "scored.parquet")


def _frame():
    pd = pytest.importorskip("pandas")
    return pd.DataFrame({
        "user_id": ["u1", "u2", "u1"],
        "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"],
        "label": [1.0, 0.0, 2.0],
        "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_ope_sources.py::test_exporter_threads_the_ope_parquet_flag_through_to_compute -q`
Expected: FAIL with `TypeError: build() takes from 3 to 6 positional arguments but 7 were given`.

- [x] **Step 3: Thread the parameter and the flag**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("frontend/export_dashboard_json.py")
t = p.read_text()

old = '''def build(input_dir: str, host: str, port: int,
          experiences: str | None = None, live_metrics: str | None = None,
          config: dict | None = None) -> dict:'''
new = '''def build(input_dir: str, host: str, port: int,
          experiences: str | None = None, live_metrics: str | None = None,
          config: dict | None = None, ope_parquet: str | None = None) -> dict:'''
assert t.count(old) == 1
t = t.replace(old, new)

old_call = "    ope = dash.compute_ope(host, port)"
new_call = "    ope = dash.compute_ope(host, port, parquet=ope_parquet)"
assert t.count(old_call) == 1
t = t.replace(old_call, new_call)

old_arg = '''    ap.add_argument("--live-metrics", default=None,'''
new_arg = '''    ap.add_argument("--ope-parquet", default=None,
                    help="scored replay Parquet from post_train_dpo.py / post_train_q.py "
                         "--output-parquet; the only source carrying the post-training arms. "
                         "Without it the off-policy section reads Redis, which this repository "
                         "does not write")
    ap.add_argument("--live-metrics", default=None,'''
assert t.count(old_arg) == 1
t = t.replace(old_arg, new_arg)

old_main = "    data = build(args.input, host, port, args.experiences, args.live_metrics, {"
assert t.count(old_main) == 1
old_close = '''        "safety_policy_version": args.safety_policy_version,
    })'''
new_close = '''        "safety_policy_version": args.safety_policy_version,
    }, args.ope_parquet)'''
assert t.count(old_close) == 1
p.write_text(t.replace(old_close, new_close))
PY
python3 frontend/export_dashboard_json.py --help | grep -A4 'ope-parquet'
```

Expected: the flag and its help text appear.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_ope_sources.py integration-tests/python_modeling/test_dashboard_measurement_contract.py -q`
Expected: PASS — 5 new + 15 contract tests.

- [x] **Step 5: Replace the two strings in OpeSection**

The current N/A reason describes an empty buffer. The replacement names the absent writer and the offline route. The fine print gains the source, tolerating its absence because the committed snapshot predates the key.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("frontend/components/sections.jsx")
t = p.read_text()

old_na = '''  if (!data) return <NaCard title="Off-policy evaluation" id="ope" reason="no replay-buffer events with reward in Redis" />;'''
new_na = '''  if (!data) return <NaCard title="Off-policy evaluation" id="ope" reason="no replay events with reward — this repository writes no replay:recommendations (the serving path does); pass --ope-parquet to read a scored replay from post-training instead" />;'''
assert t.count(old_na) == 1
t = t.replace(old_na, new_na)

old_fine = '''        Direct Method · reward estimator AUC {num(cal.auc)} MSE {num(cal.mse)} (n_test {count(cal.n_test)}). 95%
        event-bootstrap CIs are conditional on the fixed reward model; model-fit uncertainty excluded.'''
new_fine = '''        Direct Method · source {data.source ?? "unrecorded (snapshot predates source tracking)"} · reward
        estimator AUC {num(cal.auc)} MSE {num(cal.mse)} (n_test {count(cal.n_test)}). 95%
        event-bootstrap CIs are conditional on the fixed reward model; model-fit uncertainty excluded.'''
assert t.count(old_fine) == 1
p.write_text(t.replace(old_fine, new_fine))
PY
grep -n 'replay:recommendations\|data.source' frontend/components/sections.jsx
```

Expected: both new strings present.

- [x] **Step 6: Verify the frontend still builds against the unregenerated snapshot**

Run: `cd recsys-pipeline/frontend && npm run validate:data && npm run build 2>&1 | tail -5`
Expected: `dashboard.json valid: 7 measurement sections, schema 2.0`, then a successful build. The snapshot has no `source`, so the fallback text is what renders — which is the point of the `??`.

- [x] **Step 7: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py \
        recsys-pipeline/frontend/export_dashboard_json.py \
        recsys-pipeline/frontend/components/sections.jsx
git commit -m "$(cat <<'MSG'
feat(dashboard): expose --ope-parquet and say which source the rows came from

The exporter can now be pointed at post-training's scored replay, and the
section states whether its rows came from that Parquet or from Redis.

The N/A text said "no replay-buffer events with reward in Redis", which reads
as an empty buffer. Nothing in this repository writes that key, so the buffer
is not empty -- it is unwritten. The new text says so and names the offline
route.

build() takes ope_parquet last: its only caller passes the first six arguments
positionally, so inserting it beside the other inputs would have landed the
config dict in it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: The false writer claim and its guard

**Files:**
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py` (append)
- Modify: `recsys-pipeline/README.md:314`

**Interfaces:**
- Consumes: `REPO` from Task 1.
- Produces: nothing.

- [x] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py`:

```python
def test_no_live_document_credits_the_collector_with_the_replay_buffer():
    """ExperienceCollectorStreamingJob writes Kafka and an optional Parquet slate sink.

    It performs no Redis write. README.md:314 said it populates
    replay:recommendations, which credited this repository with a writer it does not
    contain -- the same defect class as the served_history claim corrected in #247.

    This pairs the job name with the key inside one line. Prose that splits them
    across two sentences would pass while saying the same false thing; that is the
    limit of a prose-level guard.
    """
    offenders = []
    for path in sorted((REPO.parent).rglob("*.md")):
        relative = path.relative_to(REPO.parent)
        if relative.as_posix().startswith((".superpowers/", ".planning/")):
            continue
        if set(relative.parts) & {"node_modules", ".git"}:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "ExperienceCollectorStreamingJob" in line and "replay:recommendations" in line:
                offenders.append(f"{relative}:{number}")
    assert not offenders, (
        "these lines claim ExperienceCollectorStreamingJob writes replay:recommendations, "
        f"which it does not: {', '.join(offenders)}"
    )
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_ope_sources.py::test_no_live_document_credits_the_collector_with_the_replay_buffer -q`
Expected: FAIL naming `recsys-pipeline/README.md:314`.

- [x] **Step 3: Correct the claim**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("README.md")
t = p.read_text()
old = "The `replay:recommendations` Redis list is populated by `ExperienceCollectorStreamingJob`. Run `replay_export.py` standalone to inspect or back up the buffer:"
new = ("The `replay:recommendations` Redis list is written by the serving path in "
       "[lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service), "
       "from its feedback handler; nothing in this repository writes it. "
       "`ExperienceCollectorStreamingJob` writes the `training_experiences` Kafka topic and, when "
       "`EXPERIENCE_COLLECTOR_OUTPUT_PATH` is set, a Parquet slate sink. Run `replay_export.py` "
       "standalone to inspect or back up the buffer:")
assert t.count(old) == 1, "README line 314 does not match verbatim"
p.write_text(t.replace(old, new))
PY
sed -n '312,318p' README.md
```

- [x] **Step 4: Run the test to verify it passes**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_ope_sources.py -q`
Expected: 6 passed.

- [x] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_ope_sources.py \
        recsys-pipeline/README.md
git commit -m "$(cat <<'MSG'
docs: stop crediting the collector with a Redis write it does not perform

README.md:314 said ExperienceCollectorStreamingJob populates
replay:recommendations. That job writes the training_experiences Kafka topic
and an optional Parquet slate sink; grep finds no Redis write in it, and no
.scala file in this repository mentions the key at all. The serving path
writes it.

Guarded by an assertion that no live document pairs the job name with the key
on one line -- the same defect class as the served_history claim #247 removed.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 4: Documentation

**Files:**
- Modify: `recsys-pipeline/frontend/README.md` (the refresh block)
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md` (the off-policy passage)

**Interfaces:**
- Consumes: the `--ope-parquet` flag from Task 2.
- Produces: nothing.

- [x] **Step 1: Document the flag beside its siblings**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("frontend/README.md")
t = p.read_text()
old = '''  --experiences /tmp/spark-recsys/movie-category-sim/slates \\
  --live-metrics /tmp/spark-recsys/movie-category-sim/live-metrics.json
```'''
new = '''  --experiences /tmp/spark-recsys/movie-category-sim/slates \\
  --live-metrics /tmp/spark-recsys/movie-category-sim/live-metrics.json
```

The off-policy section reads one of two sources. Without `--ope-parquet` it reads the
`replay:recommendations` Redis list, which the serving path writes and this repository does not, so
it is N/A unless a backend has been running. With `--ope-parquet` it reads the scored replay that
`post_train_dpo.py` or `post_train_q.py --output-parquet` writes, which is the only source carrying
the post-training arms (`dpoScore`, `tabQ`, `fqiQ`, `grpoScore`). The section names whichever source
it used.'''
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("frontend/README.md documented")
PY
```

- [x] **Step 2: Document the two sources in the analysis reference**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("docs/recommendation_architecture/Analysis_Report.md")
t = p.read_text()
old = """`ope_eval_report.py` fits a dependency-light logistic reward model to logged taken-action features,"""
new = """The dashboard's off-policy section and `ope_eval_report.py` share this estimator and read the same
two sources: the `replay:recommendations` Redis list, written by the serving path, and a scored
replay Parquet from `post_train_dpo.py` / `post_train_q.py --output-parquet`, which is the only
source carrying `dpoScore`, `tabQ`, `fqiQ` and `grpoScore`. Pass `--ope-parquet` to the exporter or
`--parquet` to `ope_eval_report.py`; both name the source they used.

`ope_eval_report.py` fits a dependency-light logistic reward model to logged taken-action features,"""
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("Analysis_Report.md documented")
PY
```

- [x] **Step 3: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
python3 -m pytest integration-tests/test_doc_links.py -q 2>&1 | tail -2
(cd frontend && npm run validate:data)
grep -rn 'no replay-buffer events with reward in Redis' --include='*.jsx' --include='*.py' --include='*.md' . | grep -v '.superpowers' || echo "old N/A text gone"
git -C .. diff --name-only origin/master | grep 'dashboard.json' && echo "FAIL: snapshot changed" || echo "snapshot untouched"
```

Expected: `568 passed, 2 skipped`; `4 passed`; `dashboard.json valid: 7 measurement sections, schema 2.0`; `old N/A text gone`; `snapshot untouched`.

- [x] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/README.md \
        recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md
git commit -m "$(cat <<'MSG'
docs: describe the off-policy section's two sources

Redis holds what the serving path scored and this repository does not write it.
The post-training Parquet is the only source carrying dpoScore, tabQ, fqiQ and
grpoScore. Both the exporter and ope_eval_report.py take the Parquet, and both
name the source they read.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

## Self-Review

**Spec coverage.** Parquet source → Task 1 step 3. `source` key → Task 1 step 3. Exporter parameter and flag → Task 2 step 3. N/A reason and fine print → Task 2 step 5. False README claim and guard → Task 3. Documentation → Task 4. Spec acceptance items 1-9 map to Task 4 step 3 (items 1, 6, 7, 8), Task 1 steps 1-4 (items 2, 3, 4), Task 2 step 3 (item 5), and a final `git diff --check` in the PR step.

**Placeholders.** None. Every replacement asserts its match count before writing, so a drifted line fails loudly. The Parquet round-trip and the policy list were measured before writing, not predicted — see Pre-validated facts.

**Type consistency.** `compute_ope(..., parquet=None)` returning a dict with `source` is defined in Task 1 and consumed by name in Tasks 2 and 4. `build(..., ope_parquet=None)` is defined in Task 2 and referenced nowhere later. `REPO` is defined in Task 1's test module and reused in Task 3's appended test. `_event`, `_scored_parquet` and `_frame` are helpers in that one module; `_frame` is defined after its use in the file, which is fine at call time because Python resolves it when the test runs.

**One risk worth stating.** Task 2 step 5 edits JSX that no test covers. `npm run build` in step 6 catches a syntax error but not a wording regression, and the `??` fallback only renders when `data.source` is absent — which is exactly the committed snapshot's state, so step 6 does exercise it.
