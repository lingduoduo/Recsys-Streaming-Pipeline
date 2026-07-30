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
    """Every field the scorecard reads must be a real key in that section's rows."""
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

    declared = re.findall(r'(\w+):\s*\{[^}]*field:\s*"([^"]+)"', headlines.group(1))
    assert {key for key, _ in declared} == MEASUREMENT_KEYS

    for key, field in declared:
        published = {column for row in output[key]["rows"] for column in row}
        assert field in published, f"scorecard reads {key}.{field}, which no row publishes"
