"""The report launcher must preserve user paths/flags and never provision Kafka."""
import os
import shutil
import subprocess
from pathlib import Path

import pytest


SOURCE = Path(__file__).resolve().parents[1] / "scripts/run-analysis-report.sh"


@pytest.mark.parametrize("report,job", [
    ("recall", "RecallEvaluationReportJob"),
    ("ranking", "RankingEvaluationReportJob"),
    ("ope", "OffPolicyEvaluationReportJob"),
    ("dashboard", "AnalysisDashboardExportJob"),
])
def test_report_launcher_preserves_flags_and_paths(tmp_path, report, job):
    pipeline = tmp_path / "pipeline with spaces"
    script = pipeline / "scripts/run-analysis-report.sh"
    script.parent.mkdir(parents=True)
    shutil.copy2(SOURCE, script)
    jar = pipeline / "services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar"
    jar.parent.mkdir(parents=True)
    jar.write_text("fixture")
    spark = tmp_path / "spark home"
    submit = spark / "bin/spark-submit"
    submit.parent.mkdir(parents=True)
    submit.write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$REPORT_ARGS"\npwd > "$REPORT_CWD"\n')
    submit.chmod(0o755)
    log, cwd = tmp_path / "args", tmp_path / "cwd"
    result = subprocess.run(["bash", str(script), report, "--input", "a path/with spaces",
                             "--output", "out.json"], cwd=tmp_path,
                            env={**os.environ, "SPARK_HOME": str(spark), "REPORT_ARGS": str(log),
                                 "REPORT_CWD": str(cwd), "SPARK_MASTER": "local[1]"},
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    args = log.read_text().splitlines()
    assert args[args.index("--class") + 1] == f"com.demo.report.{job}"
    assert args[-4:] == ["--input", "a path/with spaces", "--output", "out.json"]
    assert cwd.read_text().strip() == str(pipeline)
    assert "local[1]" in args


def test_report_launcher_rejects_unknown_report_before_starting_spark():
    result = subprocess.run(["bash", str(SOURCE), "unknown"], capture_output=True, text=True)
    assert result.returncode == 2
    assert "Usage:" in result.stderr
