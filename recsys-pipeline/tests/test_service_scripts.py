import os
import shutil
import stat
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def copy_pipeline_scripts(tmp_path: Path) -> Path:
    pipeline = tmp_path / "recsys-pipeline"
    pipeline.mkdir()
    shutil.copy2(REPO_ROOT / "run-streaming-job.sh", pipeline / "run-streaming-job.sh")
    shutil.copy2(REPO_ROOT / "run-offline-pipeline.sh", pipeline / "run-offline-pipeline.sh")
    return pipeline


def add_fake_spark_submit(spark_home: Path) -> Path:
    bin_dir = spark_home / "bin"
    bin_dir.mkdir(parents=True)
    log_file = spark_home / "spark-submit.args"
    spark_submit = bin_dir / "spark-submit"
    spark_submit.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$@\" > \"${SPARK_SUBMIT_LOG:?}\"\n",
        encoding="utf-8",
    )
    spark_submit.chmod(spark_submit.stat().st_mode | stat.S_IXUSR)
    return log_file


def add_spark_job_jar(pipeline: Path) -> Path:
    jar = pipeline / "services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar"
    jar.parent.mkdir(parents=True)
    jar.write_text("fake jar", encoding="utf-8")
    return jar


def run_script(script: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(script)],
        cwd=script.parent,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def base_env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    spark_home = tmp_path / "fake-spark"
    env["SPARK_HOME"] = str(spark_home)
    env["SPARK_SUBMIT_LOG"] = str(add_fake_spark_submit(spark_home))
    return env


def test_streaming_script_uses_consolidated_spark_service_path(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    jar = add_spark_job_jar(pipeline)
    env = base_env(tmp_path)

    result = run_script(pipeline / "run-streaming-job.sh", env)

    assert result.returncode == 0
    args = Path(env["SPARK_SUBMIT_LOG"]).read_text(encoding="utf-8").splitlines()
    assert "--class" in args
    assert "com.demo.task.UserEventStreamingJob" in args
    assert str(jar.relative_to(pipeline)) in args


def test_streaming_script_reports_missing_consolidated_jar(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    env = base_env(tmp_path)

    result = run_script(pipeline / "run-streaming-job.sh", env)

    assert result.returncode == 127
    assert "services/spark-streaming-job" in result.stderr
    assert "sbt assembly" in result.stderr


def test_offline_script_requires_ratings_input_before_spark(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    env = base_env(tmp_path)
    env.pop("RATINGS_INPUT_PATH", None)

    result = run_script(pipeline / "run-offline-pipeline.sh", env)

    assert result.returncode == 1
    assert "RATINGS_INPUT_PATH is required" in result.stderr


def test_offline_script_passes_ratings_and_embedding_paths_to_spark(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    add_spark_job_jar(pipeline)
    env = base_env(tmp_path)
    env["RATINGS_INPUT_PATH"] = "sampledata/ratings.csv"
    env["ITEM2VEC_EMBEDDING_PATH"] = "sampledata/custom_embedding.txt"
    env["ITEM2VEC_QUERY_ITEM"] = "42"

    result = run_script(pipeline / "run-offline-pipeline.sh", env)

    assert result.returncode == 0
    args = Path(env["SPARK_SUBMIT_LOG"]).read_text(encoding="utf-8").splitlines()
    assert "com.demo.task.Item2VecTrainingJob" in args
    assert "services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar" in args
    assert args[-3:] == ["sampledata/ratings.csv", "sampledata/custom_embedding.txt", "42"]
