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

import re
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


def test_no_live_document_credits_the_collector_with_the_replay_buffer():
    """ExperienceCollectorStreamingJob writes Kafka and an optional Parquet slate sink.

    It performs no Redis write. README.md:314 said it populates
    replay:recommendations, which credited this repository with a writer it does not
    contain -- the same defect class as the served_history claim corrected in #247.

    The check is sentence-bounded, not line-bounded: a markdown paragraph is one long
    line, so "the list is written by the serving path. The collector writes Kafka
    instead." puts both tokens on one line while asserting the opposite. Only a
    sentence claiming both is a claim. Prose contriving to split a single false
    assertion across two sentences would still pass -- that is the limit of a
    prose-level guard, and the claim it must catch is the one that existed.
    """
    offenders = []
    for path in sorted((REPO.parent).rglob("*.md")):
        relative = path.relative_to(REPO.parent)
        if relative.as_posix().startswith((".superpowers/", ".planning/")):
            continue
        # Same exclusions as test_retrieval_service_extracted.py: .worktrees holds whole
        # second checkouts, whose docs are not this one's live documentation.
        if set(relative.parts) & {".git", "target", "node_modules", ".next", "__pycache__",
                                  ".worktrees", ".pytest_cache", ".venv", "venv"}:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for sentence in re.split(r"(?<=\.)\s+", line):
                if ("ExperienceCollectorStreamingJob" in sentence
                        and "replay:recommendations" in sentence):
                    offenders.append(f"{relative}:{number}")
    assert not offenders, (
        "these lines claim ExperienceCollectorStreamingJob writes replay:recommendations, "
        f"which it does not: {', '.join(offenders)}"
    )
