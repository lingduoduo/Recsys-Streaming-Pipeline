import contextlib
import os
import shutil
import socket
import stat
import subprocess
import time
from pathlib import Path
from typing import Iterator

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


SCRIPTS_DIR = REPO_ROOT / "scripts"


def copy_pipeline_scripts(tmp_path: Path) -> Path:
    """Mirror the real layout: scripts live in scripts/ and cd up to the pipeline root."""
    pipeline = tmp_path / "recsys-pipeline"
    scripts = pipeline / "scripts"
    scripts.mkdir(parents=True)
    shutil.copy2(SCRIPTS_DIR / "run-streaming-job.sh", scripts / "run-streaming-job.sh")
    shutil.copy2(SCRIPTS_DIR / "run-offline-pipeline.sh", scripts / "run-offline-pipeline.sh")
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


def add_fake_kafka_topics(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "stub-bin"
    bin_dir.mkdir()
    kafka_topics = bin_dir / "kafka-topics"
    kafka_topics.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    kafka_topics.chmod(kafka_topics.stat().st_mode | stat.S_IXUSR)
    return bin_dir


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
    # The streaming script bootstraps its Kafka input topic before spark-submit; stub
    # the CLI so the test never blocks on a real broker.
    env["PATH"] = os.pathsep.join([str(add_fake_kafka_topics(tmp_path)), env["PATH"]])
    return env


@contextlib.contextmanager
def listening_socket() -> Iterator[str]:
    """Yield 'host:port' for a real socket that accepts connections.

    The preflight uses a bash builtin, so it cannot be stubbed on PATH. A real
    throwaway listener keeps the test hermetic without needing a broker.
    """
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    try:
        yield f"127.0.0.1:{sock.getsockname()[1]}"
    finally:
        sock.close()


def test_streaming_script_uses_consolidated_spark_service_path(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    jar = add_spark_job_jar(pipeline)
    env = base_env(tmp_path)

    with listening_socket() as bootstrap:
        env["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
        result = run_script(pipeline / "scripts" / "run-streaming-job.sh", env)

    assert result.returncode == 0
    args = Path(env["SPARK_SUBMIT_LOG"]).read_text(encoding="utf-8").splitlines()
    assert "--class" in args
    assert "com.demo.task.UserEventStreamingJob" in args
    assert str(jar.relative_to(pipeline)) in args


def test_streaming_script_reports_missing_consolidated_jar(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    env = base_env(tmp_path)

    result = run_script(pipeline / "scripts" / "run-streaming-job.sh", env)

    assert result.returncode == 127
    assert "services/spark-streaming-job" in result.stderr
    assert "sbt assembly" in result.stderr


def test_streaming_script_fails_fast_when_broker_unreachable(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    # The preflight runs after the spark-submit and jar checks, so without a jar
    # the script would exit 127 and this test would pass for the wrong reason.
    add_spark_job_jar(pipeline)
    env = base_env(tmp_path)
    env["KAFKA_BOOTSTRAP_SERVERS"] = "127.0.0.1:1"

    start = time.monotonic()
    result = run_script(pipeline / "scripts" / "run-streaming-job.sh", env)
    elapsed = time.monotonic() - start

    assert result.returncode == 1
    # Guards the 64s AdminClient retry loop this replaced; loose enough not to flake.
    assert elapsed < 5
    assert "127.0.0.1:1" in result.stderr
    assert "docker compose up -d" in result.stderr
    assert "Exception" not in result.stderr
    assert not Path(env["SPARK_SUBMIT_LOG"]).exists()


def test_offline_script_requires_ratings_input_before_spark(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    env = base_env(tmp_path)
    env.pop("RATINGS_INPUT_PATH", None)

    result = run_script(pipeline / "scripts" / "run-offline-pipeline.sh", env)

    assert result.returncode == 1
    assert "RATINGS_INPUT_PATH is required" in result.stderr


def test_offline_script_passes_ratings_and_embedding_paths_to_spark(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    add_spark_job_jar(pipeline)
    env = base_env(tmp_path)
    env["RATINGS_INPUT_PATH"] = "sampledata/ratings.csv"
    env["ITEM2VEC_EMBEDDING_PATH"] = "sampledata/custom_embedding.txt"
    env["ITEM2VEC_QUERY_ITEM"] = "42"

    result = run_script(pipeline / "scripts" / "run-offline-pipeline.sh", env)

    assert result.returncode == 0
    args = Path(env["SPARK_SUBMIT_LOG"]).read_text(encoding="utf-8").splitlines()
    assert "com.demo.task.Item2VecTrainingJob" in args
    assert "services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar" in args
    assert args[-3:] == ["sampledata/ratings.csv", "sampledata/custom_embedding.txt", "42"]


PIPELINE_DIR = os.path.join(os.path.dirname(__file__), "..")


def test_als_pipeline_script_exists():
    script = os.path.join(PIPELINE_DIR, "scripts", "run-als-pipeline.sh")
    assert os.path.isfile(script), "run-als-pipeline.sh not found"


def test_als_pipeline_script_is_executable():
    script = os.path.join(PIPELINE_DIR, "scripts", "run-als-pipeline.sh")
    assert os.access(script, os.X_OK), "run-als-pipeline.sh is not executable"


def test_als_pipeline_requires_ratings_input():
    script = os.path.join(PIPELINE_DIR, "scripts", "run-als-pipeline.sh")
    result = subprocess.run(
        ["bash", script],
        capture_output=True,
        text=True,
        env={**os.environ, "RATINGS_INPUT_PATH": ""},
    )
    assert result.returncode == 1
    assert "RATINGS_INPUT_PATH" in result.stderr


def test_user_embedding_script_exists():
    script = os.path.join(PIPELINE_DIR, "scripts", "run-user-embedding-pipeline.sh")
    assert os.path.isfile(script), "run-user-embedding-pipeline.sh not found"


def test_user_embedding_script_is_executable():
    script = os.path.join(PIPELINE_DIR, "scripts", "run-user-embedding-pipeline.sh")
    assert os.access(script, os.X_OK), "run-user-embedding-pipeline.sh is not executable"


def test_user_embedding_requires_ratings_input():
    script = os.path.join(PIPELINE_DIR, "scripts", "run-user-embedding-pipeline.sh")
    result = subprocess.run(
        ["bash", script],
        capture_output=True,
        text=True,
        env={**os.environ, "RATINGS_INPUT_PATH": "", "ITEM2VEC_EMBEDDING_PATH": "some.txt"},
    )
    assert result.returncode == 1
    assert "RATINGS_INPUT_PATH" in result.stderr


def test_user_embedding_requires_item_embedding():
    script = os.path.join(PIPELINE_DIR, "scripts", "run-user-embedding-pipeline.sh")
    result = subprocess.run(
        ["bash", script],
        capture_output=True,
        text=True,
        env={**os.environ, "RATINGS_INPUT_PATH": "ratings.csv", "ITEM2VEC_EMBEDDING_PATH": ""},
    )
    assert result.returncode == 1
    assert "ITEM2VEC_EMBEDDING_PATH" in result.stderr


SIM_SCRIPT = Path(__file__).parents[1] / "scripts" / "run-movie-category-sim.sh"


def test_movie_category_sim_wires_every_measurement_input() -> None:
    script = SIM_SCRIPT.read_text(encoding="utf-8")

    # slates: the collector must run and land Parquet the exporter can read
    assert "com.demo.process.ExperienceCollectorStreamingJob" in script
    assert "EXPERIENCE_COLLECTOR_OUTPUT_PATH" in script
    # latency: a real /metrics capture from the running service
    assert "/metrics" in script and "live-metrics.json" in script
    # export: both optional inputs reach the exporter, and the snapshot is validated
    assert "--experiences" in script and "--live-metrics" in script
    assert "validate:data" in script


def test_movie_category_sim_never_fails_on_a_missing_live_service() -> None:
    script = SIM_SCRIPT.read_text(encoding="utf-8")
    burst = script.split("SERVICE BURST")[1]
    # the service block must not abort the sim: every failure path continues
    assert "|| true" in burst or "continue" in burst
    assert "set -e" not in burst
