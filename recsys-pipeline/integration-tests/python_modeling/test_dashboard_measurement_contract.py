"""The consolidated dashboard export always publishes the seven measurement sections."""
import json
import re
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).parents[3]
sys.path.insert(0, str(_REPO / "recsys-pipeline" / "services" / "python-modeling"))
sys.path.insert(0, str(_REPO / "frontend"))

MEASUREMENT_KEYS = {
    "relevance", "satisfaction", "freshness", "diversity", "fairness", "safety", "latency",
}


def _samples_frame(pd):
    """Two slates of two items with every optional measurement signal observed."""
    rows = []
    for slate, (user, gender) in enumerate((("u1", "female"), ("u2", "male"))):
        for position, (item, label) in enumerate((("item_1", 2.0), ("item_2", 0.0))):
            rows.append({
                "request_id": f"req-{slate}",
                "user_id": user,
                "session_id": f"s{slate}",
                "item_id": item,
                "position": position,
                "label": label,
                "clicked": int(label >= 1),
                "ordered": int(label >= 2),
                "reward": label / 2.0,
                "genres": ["Drama"] if item == "item_1" else ["Comedy"],
                "gender": gender,
                "rating": 5.0 if label >= 2 else None,
                "negative_feedback_reason": None if label >= 2 else "not_interested",
                "dwell_millis": 12000.0 if label >= 2 else None,
                "completion_rate": 0.75 if label >= 2 else None,
                # epoch seconds, matching the pipeline's LongType published_at
                "published_at": 1_753_000_000 if item == "item_1" else 1_600_000_000,
                "new_release": item == "item_1",
                "filter_reason": None if item == "item_1" else "muted_genre",
                "unsafe_label": False,
            })
    return pd.DataFrame(rows)


def _slates_frame(pd, samples):
    item_columns = ["position", "item_id", "label", "genres", "published_at", "new_release"]
    slates = []
    for request_id, group in samples.groupby("request_id"):
        items = group.sort_values("position")[item_columns].to_dict(orient="records")
        for item in items:
            item["popularity"] = 100.0 if item["item_id"] == "item_1" else 5.0
        slates.append({
            "request_id": request_id,
            "user_id": group["user_id"].iloc[0],
            "items": items,
        })
    return pd.DataFrame(slates)


def _live_metrics():
    def timings(count):
        return {"p50": 4.0, "p95": 12.0, "p99": 20.0, "count": count}

    return {
        "requestCount": 5,
        "measurements": {
            "schemaVersion": "2.0",
            "latency": {
                "availability": "available",
                "unit": "milliseconds",
                "endpoints": {
                    "feedback": {**timings(3), "errorCount": 0, "errorRate": 0.0,
                                 "timeoutCount": 0, "timeoutRate": 0.0},
                    "recommend": {**timings(5), "errorCount": 1, "errorRate": 0.2,
                                  "timeoutCount": 0, "timeoutRate": 0.0},
                },
                "stages": {
                    "hydration": timings(5), "redis_fetch": timings(5), "scoring": timings(5),
                    "selection": timings(5), "side_effects": timings(5),
                },
            },
            "freshness": {"availability": "available", "exposures": 50, "coverage": 1.0,
                          "freshShare": 0.4, "source": "boolean_new_release"},
            "safety": {
                "availability": "available", "policyVersion": "catalog-filter-v1",
                "evaluatedCandidates": 80, "totalDecisions": 8, "unknownShare": 0.0125,
                "reasons": {
                    "expired": {"count": 7, "rate": 0.0875},
                    "muted_genre": {"count": 0, "rate": 0.0},
                    "muted_keyword": {"count": 0, "rate": 0.0},
                    "muted_product_type": {"count": 0, "rate": 0.0},
                    "muted_title": {"count": 0, "rate": 0.0},
                    "unknown": {"count": 1, "rate": 0.0125},
                },
            },
            "feedbackCoverage": {
                "availability": "available", "total": 4,
                "signals": {
                    "request_id": {"present": 4, "coverage": 1.0},
                    "rating": {"present": 2, "coverage": 0.5},
                    "negative_feedback_reason": {"present": 1, "coverage": 0.25},
                    "dwell_millis": {"present": 2, "coverage": 0.5},
                    "completion_rate": {"present": 2, "coverage": 0.5},
                },
            },
        },
    }


def _export(tmp_path, *, experiences=None, live=None, extra_args=()):
    """Run the exporter entry point with Redis unreachable and return the JSON it wrote."""
    import export_dashboard_json as exporter

    out = tmp_path / "dashboard.json"
    argv = ["--input", str(tmp_path / "samples"), "--output", str(out)]
    if experiences:
        argv += ["--experiences", str(experiences)]
    if live:
        argv += ["--live-metrics", str(live)]
    exporter.main([*argv, *extra_args])
    return json.loads(out.read_text())


def test_exporter_publishes_every_measurement_section(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    monkeypatch.setenv("REDIS_PORT", "6399")  # nothing listening: Redis sections degrade to N/A

    samples = _samples_frame(pd)
    samples.to_parquet(tmp_path / "samples", index=False)
    _slates_frame(pd, samples).to_parquet(tmp_path / "experiences", index=False)
    (tmp_path / "live.json").write_text(json.dumps(_live_metrics()))

    output = _export(
        tmp_path,
        experiences=tmp_path / "experiences",
        live=tmp_path / "live.json",
        extra_args=["--fairness-min-support", "1", "--freshness-window-days", "30"],
    )

    assert output["schemaVersion"] == "2.0"
    assert set(output) >= MEASUREMENT_KEYS
    assert output["latency"]["status"] == "available"
    assert output["fairness"]["status"] in {"available", "unavailable"}
    json.dumps(output, allow_nan=False)

    # Every available section carries its own support, and no section fabricates one.
    for key in MEASUREMENT_KEYS:
        section = output[key]
        assert section["status"] in {"available", "unavailable"}
        if section["status"] == "available":
            assert isinstance(section["sampleSize"], int)
            assert section["coverage"] is not None
        else:
            assert section["warnings"] and "sampleSize" not in section

    latency_rows = {(row["scope"], row["name"]): row for row in output["latency"]["rows"]}
    assert latency_rows[("endpoint", "recommend")]["p95"] == 12.0
    assert latency_rows[("endpoint", "recommend")]["error_rate"] == 0.2
    assert latency_rows[("stage", "scoring")]["count"] == 5
    assert output["latency"]["rows"][0]["unit"] == "milliseconds"

    # Listwise measures need slate experiences; the two-item slates rank the label-2 item first.
    relevance = {row["k"]: row for row in output["relevance"]["rows"]}
    assert relevance[5]["ndcg_at_k"] == 1.0
    assert relevance[5]["mrr_at_k"] == 1.0
    assert output["diversity"]["rows"][0]["scope"] == "aggregate"
    assert output["diversity"]["rows"][0]["unique_genres_at_k"] == 2.0

    # Offline rows survive the live merge; live measurements arrive as their own scoped rows.
    scopes = {key: [row.get("scope") for row in output[key]["rows"]]
              for key in ("satisfaction", "freshness", "safety")}
    assert scopes["freshness"] == ["offline", "live_service"]
    assert scopes["safety"] == ["offline", "live_service"]
    assert scopes["satisfaction"] == ["offline", "live_service"]
    live_freshness = output["freshness"]["rows"][1]
    assert live_freshness["fresh_share"] == 0.4 and live_freshness["exposures"] == 50
    assert output["safety"]["rows"][1]["policy_version"] == "catalog-filter-v1"
    assert output["satisfaction"]["rows"][1]["rating_coverage"] == 0.5

    # Timestamped freshness reads the pipeline's epoch-second published_at.
    assert output["freshness"]["rows"][0]["freshness_source"] == "published_at"
    assert output["fairness"]["rows"][0]["dimension"] == "gender"

    # Compatibility: the pre-existing diagnostic sections are still exported.
    assert set(output) >= {"engagement", "keyword", "query", "recall", "ranking", "ope", "mdp"}
    assert output["engagement"]["funnel"] == {"impression": 4, "click": 2, "order": 2}


def test_measurement_sections_report_missing_prerequisites(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    monkeypatch.setenv("REDIS_PORT", "6399")

    pd.DataFrame({
        "user_id": ["u1", "u2"],
        "session_id": ["s1", "s2"],
        "item_id": ["item_1", "item_2"],
        "label": [1.0, 0.0],
        "genres": [["Drama"], ["Comedy"]],
    }).to_parquet(tmp_path / "samples", index=False)

    output = _export(tmp_path)

    assert output["schemaVersion"] == "2.0"
    assert set(output) >= MEASUREMENT_KEYS
    assert output["satisfaction"]["status"] == "available"
    unavailable = {key: output[key]["warnings"][0]
                   for key in MEASUREMENT_KEYS if output[key]["status"] == "unavailable"}
    assert unavailable == {
        "relevance": "missing slate experiences",
        "diversity": "missing slate experiences",
        "freshness": "missing published_at and new_release freshness signals",
        "fairness": "missing supported demographic dimensions",
        "safety": "missing filter_reason and unsafe_label safety signals",
        "latency": "missing live measurement snapshot",
    }
    json.dumps(output, allow_nan=False)


def test_zero_traffic_live_snapshot_never_claims_availability(tmp_path):
    """A /metrics capture taken before any traffic must not turn N/A into 'available'."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    startup = _live_metrics()
    measurements = startup["measurements"]
    for values in measurements["latency"]["endpoints"].values():
        values.update(count=0, errorCount=0, timeoutCount=0)
    for values in measurements["latency"]["stages"].values():
        values["count"] = 0
    measurements["freshness"].update(exposures=0, coverage=None, freshShare=None)
    measurements["safety"].update(evaluatedCandidates=0, totalDecisions=0, unknownShare=None)
    measurements["feedbackCoverage"]["total"] = 0
    for values in measurements["feedbackCoverage"]["signals"].values():
        values.update(present=0, coverage=None)

    samples = pd.DataFrame({"user_id": ["u1"], "item_id": ["item_1"], "label": [1.0], "clicked": [1]})
    output = dash.build_measurement_dashboard(samples, None, startup, None)

    assert output["latency"]["status"] == "unavailable"
    assert output["latency"]["warnings"] == ["no live requests recorded"]
    # Offline freshness and safety are unavailable here; an empty live snapshot cannot promote them.
    for key in ("freshness", "safety"):
        assert output[key]["status"] == "unavailable", f"{key} was promoted on zero observations"
    # Satisfaction is available offline; the empty live row must not be appended.
    assert [row.get("scope") for row in output["satisfaction"]["rows"]] == [None]


def test_dashboard_columns_match_the_published_measurement_keys(tmp_path, monkeypatch):
    """Every column the React sections request must exist in the rows the exporter emits."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    monkeypatch.setenv("REDIS_PORT", "6399")

    samples = _samples_frame(pd)
    samples.to_parquet(tmp_path / "samples", index=False)
    _slates_frame(pd, samples).to_parquet(tmp_path / "experiences", index=False)
    (tmp_path / "live.json").write_text(json.dumps(_live_metrics()))
    output = _export(tmp_path, experiences=tmp_path / "experiences", live=tmp_path / "live.json",
                     extra_args=["--fairness-min-support", "1"])

    sections = (_REPO / "frontend" / "components" / "sections.jsx").read_text()

    def columns_after(anchor):
        block = re.search(re.escape(anchor) + r"[\s\S]*?columns=\{\[(.*?)\]\}", sections, re.S)
        assert block, f"no columns declared after {anchor}"
        return re.findall(r'"([^"]+)"', block.group(1))

    for key in sorted(MEASUREMENT_KEYS):
        published = {column for row in output[key]["rows"] for column in row}
        requested = set(columns_after(f'title="{key.capitalize()}"'))
        assert requested <= published, f"{key} renders unknown columns {sorted(requested - published)}"

    groups = [group for row in output["fairness"]["rows"] for group in row["groups"]]
    group_keys = {column for group in groups for column in group}
    assert set(columns_after("rows={row.groups}")) <= group_keys


def test_measurement_config_rejects_out_of_range_values(tmp_path):
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    samples = pd.DataFrame({"user_id": ["u1"], "item_id": ["item_1"], "label": [1.0], "clicked": [1]})
    for invalid in ({"long_tail_percentile": 1.0}, {"freshness_window_days": -1},
                    {"fairness_min_support": 0}):
        with pytest.raises(ValueError):
            dash.build_measurement_dashboard(samples, None, None, invalid)


def test_scorecard_headline_fields_exist_in_the_published_rows(tmp_path, monkeypatch):
    """Every field the scorecard reads must be a real key at the specific row it indexes.

    Checking the pooled union of columns across every row (as an earlier version of this
    test did) can't tell "the field exists at rows[rowIndex]" from "the field exists
    somewhere in the section" — a reordered `ks` list or an extra endpoint sorted ahead of
    /recommend would silently point HEADLINES at the wrong row and this test would still
    pass. So it also pins the identity of the rows HEADLINES indexes, and checks the field
    at that exact row.
    """
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    monkeypatch.setenv("REDIS_PORT", "6399")

    samples = _samples_frame(pd)
    samples.to_parquet(tmp_path / "samples", index=False)
    _slates_frame(pd, samples).to_parquet(tmp_path / "experiences", index=False)
    (tmp_path / "live.json").write_text(json.dumps(_live_metrics()))
    output = _export(tmp_path, experiences=tmp_path / "experiences", live=tmp_path / "live.json",
                     extra_args=["--fairness-min-support", "1"])

    sections = (_REPO / "frontend" / "components" / "sections.jsx").read_text()
    headlines = re.search(r"const HEADLINES = \{(.*?)\n\};", sections, re.S)
    assert headlines, "sections.jsx must declare a HEADLINES map"

    declared = re.findall(r'(\w+):\s*\{\s*rowIndex:\s*(\d+),\s*field:\s*"([^"]+)"', headlines.group(1))
    assert {key for key, _, _ in declared} == MEASUREMENT_KEYS

    # HEADLINES assumes these specific row identities; if the exporter ever reorders them,
    # this must fail loudly instead of the scorecard silently mislabeling the wrong row.
    assert output["relevance"]["rows"][1]["k"] == 10
    assert output["latency"]["rows"][1]["name"] == "recommend"

    for key, row_index, field in declared:
        row = output[key]["rows"][int(row_index)]
        assert field in row, f"scorecard reads {key}.rows[{row_index}].{field}, which that row does not publish"


def test_relevance_publishes_the_denominator_its_ndcg_mean_is_taken_over():
    """NDCG drops slates with no positive label, so its denominator is not the slate count.

    `evaluated_slate_count` is what the section already showed; the NDCG mean is over
    `ndcg_evaluated_slate_count`. Stating a rate beside a denominator that is not its own
    breaks the global "every rate carries its denominator" rule, so the column and the KPI
    both have to surface it.
    """
    sections = (_REPO / "frontend" / "components" / "sections.jsx").read_text()
    relevance = re.search(r'title="Relevance"([\s\S]*?)\n    />', sections)
    assert relevance, "no Relevance section in sections.jsx"

    columns = re.search(r"columns=\{\[(.*?)\]\}", relevance.group(1), re.S)
    assert "ndcg_evaluated_slate_count" in re.findall(r'"([^"]+)"', columns.group(1))
    kpis = re.search(r"kpis=\{(.*?)\n      \}\}", relevance.group(1), re.S)
    assert "ndcg_evaluated_slate_count" in kpis.group(1), "the NDCG denominator is not shown as a KPI"


def test_fairness_scorecard_headlines_the_widest_gap_not_the_first_dimension(tmp_path):
    """The fairness tile claims "largest CTR gap", so it must not read a fixed row.

    `compute_fairness` emits one row per dimension in DEFAULT_DIMENSIONS order, which has
    nothing to do with gap size: here `gender` (rows[0]) has no gap at all while
    `subscription` has the largest one. A fixed `rowIndex` would report 0.0 under a label
    asserting it is the largest.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    samples = pd.DataFrame([
        {"user_id": "u1", "item_id": "i1", "gender": "f", "subscription": "free", "clicked": 0},
        {"user_id": "u2", "item_id": "i2", "gender": "f", "subscription": "premium", "clicked": 1},
        {"user_id": "u3", "item_id": "i3", "gender": "m", "subscription": "free", "clicked": 0},
        {"user_id": "u4", "item_id": "i4", "gender": "m", "subscription": "premium", "clicked": 1},
    ])
    rows = dash.build_measurement_dashboard(
        samples, None, None, {"fairness_min_support": 1})["fairness"]["rows"]

    gaps = {row["dimension"]: row["ctr_max_min_gap"] for row in rows}
    assert rows[0]["dimension"] == "gender" and gaps["gender"] == 0.0
    assert gaps["subscription"] == 1.0

    sections = (_REPO / "frontend" / "components" / "sections.jsx").read_text()
    headlines = re.search(r"const HEADLINES = \{(.*?)\n\};", sections, re.S)
    fairness = re.search(r"fairness:\s*\{([^}]*)\}", headlines.group(1))
    assert 'select: "max"' in fairness.group(1), (
        "the fairness headline resolves by row index, so it reports the first dimension's gap "
        "under a label claiming it is the largest"
    )
    # The section's own KPI and chart must agree with the tile rather than re-reading rows[0].
    section = re.search(r'title="Fairness"([\s\S]*?)\n    >', sections)
    assert section.group(1).count('maxByField(rows, "ctr_max_min_gap")') == 2


def test_null_filled_filter_column_keeps_the_offline_safety_row_out_of_the_filter_lookups(tmp_path):
    """The frontend's safety `find` lookups must land on the row that measured decisions.

    The joiner materializes `filter_reason` on every training sample, so the offline row's
    filter fields have to stay null when nothing was logged — otherwise the Safety KPI's
    `filter_decision_rate` lookup and the chart's `reason_counts` lookup both resolve to a
    fabricated all-zero offline row and the live service's real decisions never render.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    samples = pd.DataFrame([
        {"user_id": "u1", "item_id": "i1", "clicked": 1, "filter_reason": None, "unsafe_label": True},
        {"user_id": "u2", "item_id": "i2", "clicked": 0, "filter_reason": None, "unsafe_label": False},
    ])
    rows = dash.build_measurement_dashboard(samples, None, _live_metrics(), None)["safety"]["rows"]
    offline, live = rows[0], rows[1]

    assert offline["scope"] == "offline" and live["scope"] == "live_service"
    assert offline["filter_decisions"] is None and offline["filter_decision_rate"] is None
    assert all(count is None for count in offline["reason_counts"].values())
    # Which is what makes both frontend lookups resolve to the live row.
    assert next(r for r in rows if r.get("filter_decision_rate") is not None) is live
    assert next(r for r in rows
                if any(v is not None for v in (r.get("reason_counts") or {}).values())) is live


def test_satisfaction_coverage_chart_omits_the_series_that_cannot_show_coverage():
    """`negative_feedback_coverage` is the same expression as `negative_feedback_rate`.

    A sample only carries a `negative_feedback_reason` when the feedback fired, so its
    "coverage" is its rate. Plotted next to dwell and completion coverage it reads as "not
    instrumented" for a signal that is instrumented. It stays in the table beside its rate.
    """
    sections = (_REPO / "frontend" / "components" / "sections.jsx").read_text()
    satisfaction = re.search(r'title="Satisfaction"([\s\S]*?)\n    >', sections)
    assert satisfaction, "no Satisfaction section in sections.jsx"

    chart = re.search(r"chart=\{(.*?)\n      \}\}", satisfaction.group(1), re.S)
    fields = re.search(r"const fields = \[(.*?)\]", chart.group(1), re.S)
    assert "negative_feedback_coverage" not in re.findall(r'"([^"]+)"', fields.group(1))
    # Both columns stay in the table, where rate and coverage are legible together.
    columns = re.search(r"columns=\{\[(.*?)\]\}", satisfaction.group(1), re.S)
    assert {"negative_feedback_rate", "negative_feedback_coverage"} <= set(
        re.findall(r'"([^"]+)"', columns.group(1)))


def test_frontend_readme_points_at_the_paths_a_run_actually_writes():
    """`run-movie-category-sim.sh` writes under $SIM_ROOT, not directly under /tmp/spark-recsys."""
    readme = (_REPO / "frontend" / "README.md").read_text()

    assert "/tmp/spark-recsys/training-samples" not in readme
    assert "/tmp/spark-recsys/slates" not in readme
    assert "/tmp/spark-recsys/movie-category-sim/training-samples" in readme
    assert "/tmp/spark-recsys/movie-category-sim/slates" in readme


def test_readme_documents_that_an_uncovered_catalog_makes_every_decision_unknown():
    """The sim's live safety row is 100% `unknown`; that has to be written down.

    `ContentCandidateRetriever` records an `unknown` safety decision for any candidate with
    no catalog profile, and the decision is recorded for allowed candidates too — so
    `filter_decision_rate` is not a rejection rate.
    """
    readme = " ".join((_REPO / "recsys-pipeline" / "README.md").read_text().split())
    taxonomy = readme.index("Safety accounting is scoped to the catalog filter taxonomy")
    section = readme[taxonomy:taxonomy + 2000]

    assert "not a rejection rate" in section
    assert "RECSYS_CATALOG_PATH" in section
    assert "unknown_share" in section
    assert "follow-up" in section


def test_scorecard_treats_exactly_half_coverage_as_low_not_ok():
    """Half the envelope missing is amber, not green (partner ruling on the boundary).

    Safety is the live example: with `filter_reason` never logged, only the `unsafe_label`
    half of the envelope is instrumented, so the section lands on exactly 0.50 — the value a
    strict `<` comparison would have colored green.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    samples = pd.DataFrame([
        {"user_id": "u1", "item_id": "i1", "clicked": 1, "filter_reason": None, "unsafe_label": True},
        {"user_id": "u2", "item_id": "i2", "clicked": 0, "filter_reason": None, "unsafe_label": False},
    ])
    assert dash.build_measurement_dashboard(samples, None, None, None)["safety"]["coverage"] == 0.5

    sections = (_REPO / "frontend" / "components" / "sections.jsx").read_text()
    assert "<= LOW_COVERAGE" in sections, "coverage of exactly 0.50 must read as low, not ok"
    # The amber border is not perceivable to every reader, so the text has to agree too.
    assert "at or below 50%" in (_REPO / "frontend" / "components" / "ui.jsx").read_text()


def test_live_only_safety_row_omits_the_scorecard_headline_field(tmp_path):
    """A section can be "available" via a live-only merge without publishing every field.

    `_merge_live_row` promotes a section to "available" from the live row alone when the
    offline calculation is unavailable (see `analysis_dashboard_report._merge_live_row`).
    For safety, `_live_safety` never emits `unsafe_exposure_rate` — the offline-only
    field HEADLINES.safety reads. This proves the exporter really produces that shape, so
    the frontend fix (Scorecard falling back to the "na" tile state when the headline
    field is absent from the indexed row, not just when the section is unavailable) has a
    real payload to guard against.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    # No filter_reason/unsafe_label columns at all: compute_safety is unavailable offline.
    samples = pd.DataFrame({
        "user_id": ["u1"], "item_id": ["item_1"], "label": [1.0], "clicked": [1],
    })
    live = _live_metrics()  # measurements.safety.availability == "available"

    output = dash.build_measurement_dashboard(samples, None, live, None)

    safety = output["safety"]
    assert safety["status"] == "available"
    assert safety["rows"][0]["scope"] == "live_service"
    assert "unsafe_exposure_rate" not in safety["rows"][0]
    # The offline unavailability reason survives the merge as a warning — the scorecard's
    # "na" tile reuses it as the reason text for this exact case.
    assert safety["warnings"] == ["missing filter_reason and unsafe_label safety signals"]
