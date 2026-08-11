import contextlib
import os
import re
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
    shutil.copy2(SCRIPTS_DIR / "run-archive-replay.sh", scripts / "run-archive-replay.sh")
    shutil.copy2(SCRIPTS_DIR / "provision-kafka-topics.py", scripts / "provision-kafka-topics.py")
    shutil.copy2(SCRIPTS_DIR / "run-offline-pipeline.sh", scripts / "run-offline-pipeline.sh")
    config = pipeline / "config"
    config.mkdir()
    shutil.copy2(REPO_ROOT / "config" / "kafka-topics.json", config / "kafka-topics.json")
    python_modeling = pipeline / "services" / "python-modeling"
    python_modeling.mkdir(parents=True)
    shutil.copy2(
        REPO_ROOT / "services" / "python-modeling" / "topic_policy.py",
        python_modeling / "topic_policy.py",
    )
    shutil.copy2(
        REPO_ROOT / "services" / "python-modeling" / "archive_replay.py",
        python_modeling / "archive_replay.py",
    )
    return pipeline


def copy_user_profile_script(pipeline: Path) -> Path:
    script = pipeline / "scripts" / "run-user-profile-pipeline.sh"
    shutil.copy2(SCRIPTS_DIR / "run-user-profile-pipeline.sh", script)
    return script


def add_fake_spark_submit(spark_home: Path) -> Path:
    bin_dir = spark_home / "bin"
    bin_dir.mkdir(parents=True)
    log_file = spark_home / "spark-submit.args"
    spark_submit = bin_dir / "spark-submit"
    spark_submit.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$@\" > \"${SPARK_SUBMIT_LOG:?}\"\n"
        "if [[ -n \"${SPARK_SUBMIT_ENV_LOG:-}\" ]]; then env | sort > \"$SPARK_SUBMIT_ENV_LOG\"; fi\n",
        encoding="utf-8",
    )
    spark_submit.chmod(spark_submit.stat().st_mode | stat.S_IXUSR)
    return log_file


def add_fake_kafka_topics(tmp_path: Path, include_kafka_clis: bool = True) -> Path:
    bin_dir = tmp_path / "stub-bin"
    bin_dir.mkdir()
    commands = ("kafka-topics", "kafka-configs") if include_kafka_clis else ()
    for command in commands:
        executable = bin_dir / command
        executable.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
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


def base_env(tmp_path: Path, include_kafka_clis: bool = True) -> dict[str, str]:
    env = os.environ.copy()
    spark_home = tmp_path / "fake-spark"
    env["SPARK_HOME"] = str(spark_home)
    env["SPARK_SUBMIT_LOG"] = str(add_fake_spark_submit(spark_home))
    # The streaming script bootstraps its Kafka input topic before spark-submit; stub
    # the CLI so the test never blocks on a real broker.
    env["PATH"] = os.pathsep.join([str(add_fake_kafka_topics(tmp_path, include_kafka_clis)), env["PATH"]])
    return env


def add_fake_docker(bin_dir: Path, log_file: Path) -> None:
    docker = bin_dir / "docker"
    docker.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$1 $2 $3 $4\" == \"compose ps --status running\" ]]; then exit 0; fi\n"
        "printf '%s\\n' \"$*\" >> \"${DOCKER_LOG:?}\"\n",
        encoding="utf-8",
    )
    docker.chmod(docker.stat().st_mode | stat.S_IXUSR)


def block_host_kafka_cli(tmp_path: Path) -> Path:
    bash_env = tmp_path / "block-kafka-cli.bash"
    bash_env.write_text(
        "command() {\n"
        "  if [[ \"$1\" == \"-v\" && ( \"$2\" == \"kafka-topics\" || \"$2\" == \"kafka-configs\" ) ]]; then return 1; fi\n"
        "  builtin command \"$@\"\n"
        "}\n",
        encoding="utf-8",
    )
    return bash_env


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


def test_streaming_script_rejects_noncompose_local_endpoint_when_host_cli_is_absent(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    add_spark_job_jar(pipeline)
    env = base_env(tmp_path, include_kafka_clis=False)
    docker_log = tmp_path / "docker.log"
    add_fake_docker(Path(env["PATH"].split(os.pathsep)[0]), docker_log)
    env["DOCKER_LOG"] = str(docker_log)
    env["BASH_ENV"] = str(block_host_kafka_cli(tmp_path))

    with listening_socket() as bootstrap:
        env["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
        result = run_script(pipeline / "scripts" / "run-streaming-job.sh", env)

    assert result.returncode == 127
    assert "localhost:9092" in result.stderr
    assert not docker_log.exists()


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


def test_user_profile_script_requires_input_before_spark(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    script = copy_user_profile_script(pipeline)
    env = base_env(tmp_path)
    env.pop("USER_PROFILE_INPUT_PATH", None)

    result = run_script(script, env)

    assert result.returncode == 1
    assert "USER_PROFILE_INPUT_PATH is required" in result.stderr
    assert not Path(env["SPARK_SUBMIT_LOG"]).exists()


def test_user_profile_script_reports_missing_jar(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    script = copy_user_profile_script(pipeline)
    env = base_env(tmp_path)
    env["USER_PROFILE_INPUT_PATH"] = "sampledata/events"

    result = run_script(script, env)

    assert result.returncode == 127
    assert "Missing Spark job jar" in result.stderr
    assert "sbt assembly" in result.stderr


def test_user_profile_script_uses_batch_class_paths_and_environment(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    script = copy_user_profile_script(pipeline)
    add_spark_job_jar(pipeline)
    env = base_env(tmp_path)
    env.update({
        "USER_PROFILE_INPUT_PATH": "sampledata/profile-events",
        "USER_PROFILE_OUTPUT_PATH": "sampledata/profile-output",
        "USER_PROFILE_HALF_LIFE_SECONDS": "600",
        "USER_PROFILE_REDIS_TTL_SECONDS": "123",
        "REDIS_HOST": "redis.internal",
        "SPARK_SUBMIT_ENV_LOG": str(tmp_path / "spark-submit.env"),
    })

    result = run_script(script, env)

    assert result.returncode == 0
    args = Path(env["SPARK_SUBMIT_LOG"]).read_text(encoding="utf-8").splitlines()
    assert "com.demo.profile.UserBehaviorProfileBatchJob" in args
    assert args[-2:] == ["sampledata/profile-events", "sampledata/profile-output"]
    forwarded = Path(env["SPARK_SUBMIT_ENV_LOG"]).read_text(encoding="utf-8")
    assert "USER_PROFILE_HALF_LIFE_SECONDS=600" in forwarded
    assert "USER_PROFILE_REDIS_TTL_SECONDS=123" in forwarded
    assert "REDIS_HOST=redis.internal" in forwarded


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
    assert '--output "frontend/data/dashboard.json"' in script
    assert 'python3 frontend/export_dashboard_json.py' in script
    assert '(cd frontend && npm run validate:data)' in script
    assert "../frontend/" not in script


def test_frontend_documentation_uses_relocated_paths() -> None:
    pipeline_readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    analysis_report = (
        REPO_ROOT / "docs" / "recommendation_architecture" / "Analysis_Report.md"
    ).read_text(encoding="utf-8")
    quick_start_dashboard = pipeline_readme.split("### Dashboard", 1)[1].split(
        "### Step 1", 1
    )[0]

    assert "cd recsys-pipeline/frontend && npm run validate:data" in quick_start_dashboard
    assert "cd frontend && npm run validate:data" not in quick_start_dashboard
    assert "[pipeline README](../../README.md#canonical-finite-local-workflow)" in analysis_report
    assert "../../../README.md#canonical-finite-local-workflow" not in analysis_report


def test_movie_category_sim_never_fails_on_a_missing_live_service() -> None:
    script = SIM_SCRIPT.read_text(encoding="utf-8")
    burst = script.split("SERVICE BURST")[1]
    # the service block must not abort the sim: every failure path continues
    assert "|| true" in burst or "continue" in burst
    assert "set -e" not in burst


def test_archive_replay_script_requires_bounds_before_starting_python(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    env = base_env(tmp_path)
    python_log = tmp_path / "python.log"
    stub_bin = tmp_path / "python-bin"
    stub_bin.mkdir()
    (stub_bin / "python3").write_text(
        "#!/usr/bin/env bash\nprintf 'started' > \"${PYTHON_LOG:?}\"\n",
        encoding="utf-8",
    )
    (stub_bin / "python3").chmod(stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR)
    env["PATH"] = os.pathsep.join([str(stub_bin), env["PATH"]])
    env["PYTHON_LOG"] = str(python_log)
    env.update(
        {
            "REPLAY_ARCHIVE_PATH": "/tmp/archive",
            "REPLAY_START_DATE": "2024-06-15",
            "REPLAY_END_DATE": "2024-06-16",
            "REPLAY_MAX_ROWS": "10",
            "REPLAY_RECORDS_PER_SECOND": "1",
        }
    )

    result = run_script(pipeline / "scripts" / "run-archive-replay.sh", env)

    assert result.returncode == 1
    assert "REPLAY_ARCHIVE_QUERY_NAMESPACE is required" in result.stderr
    assert not python_log.exists()


def test_archive_replay_script_passes_exact_operator_bounds(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    env = base_env(tmp_path)
    python_log = tmp_path / "python.args"
    stub_bin = tmp_path / "python-bin"
    stub_bin.mkdir()
    (stub_bin / "python3").write_text(
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"${PYTHON_LOG:?}\"\n",
        encoding="utf-8",
    )
    (stub_bin / "python3").chmod(stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR)
    env["PATH"] = os.pathsep.join([str(stub_bin), env["PATH"]])
    env["PYTHON_LOG"] = str(python_log)
    env.update(
        {
            "REPLAY_ARCHIVE_PATH": "/data/archive",
            "REPLAY_ARCHIVE_QUERY_NAMESPACE": "query-namespace-1",
            "REPLAY_OPERATION_ID": "incident-2024-06-15",
            "REPLAY_START_DATE": "2024-06-15",
            "REPLAY_END_DATE": "2024-06-16",
            "REPLAY_MAX_ROWS": "123",
            "REPLAY_RECORDS_PER_SECOND": "4.5",
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "REPLAY_MANIFEST_DIR": "/data/manifests",
        }
    )

    result = run_script(pipeline / "scripts" / "run-archive-replay.sh", env)

    assert result.returncode == 0
    assert python_log.read_text(encoding="utf-8").splitlines() == [
        "services/python-modeling/archive_replay.py",
        "--archive-path", "/data/archive",
        "--archive-query-namespace", "query-namespace-1",
        "--operation-id", "incident-2024-06-15",
        "--start-date", "2024-06-15",
        "--end-date", "2024-06-16",
        "--max-rows", "123",
        "--records-per-second", "4.5",
        "--bootstrap-servers", "broker:9092",
        "--manifest-dir", "/data/manifests",
    ]


def test_engagement_sim_bounds_its_report_to_the_backfill_window() -> None:
    """The report defaults to 30 days, so a longer backfill would be silently truncated."""
    script = (SCRIPTS_DIR / "run-engagement-sim.sh").read_text(encoding="utf-8")

    assert "ENGAGEMENT_REPORT_LOOKBACK_DAYS=$BACKFILL_DAYS" in script
    # Hoisted out of the producer invocation, or it would be unset by the time it is printed.
    assert 'BACKFILL_DAYS="${BACKFILL_DAYS:-21}"\n' in script


SIMS = ["run-movielens-segment-sim.sh", "run-movie-category-sim.sh"]


@pytest.mark.parametrize("sim", SIMS)
def test_sim_starts_jobs_before_producing(sim: str) -> None:
    """A job started after the producer reads the whole backlog in one micro-batch."""
    script = (SCRIPTS_DIR / sim).read_text(encoding="utf-8")

    assert "start_job " in script
    assert "stop_job " in script
    first_start = script.index("start_job ")
    first_produce = script.index("python services/python-modeling/")
    assert first_start < first_produce, "jobs must be running before events are produced"


@pytest.mark.parametrize("sim", SIMS)
def test_sim_drain_waits_out_the_feedback_tail(sim: str) -> None:
    """Stability alone declares completion in ~18s, well before a 120s order arrives."""
    script = (SCRIPTS_DIR / sim).read_text(encoding="utf-8")

    assert "FEEDBACK_TAIL_SECONDS" in script
    assert "min_wait" in script
    # A plain substring check would pass even if no drain call site actually forwarded the
    # floor — assert the joiner's parquet drain receives $FEEDBACK_TAIL_SECONDS as its min_wait.
    assert re.search(
        r'drain parquet .*"\$FEEDBACK_TAIL_SECONDS"', script
    ), "the joiner's drain call must receive $FEEDBACK_TAIL_SECONDS as its min_wait"


@pytest.mark.parametrize("sim", SIMS)
def test_sim_exit_trap_covers_every_job_pid(sim: str) -> None:
    """A pid variable missing from the EXIT trap leaves that job's Spark process unreaped
    on Ctrl-C or an early `set -e` exit while it is running."""
    script = (SCRIPTS_DIR / sim).read_text(encoding="utf-8")

    pid_vars = set(re.findall(r'^(\w+_PID)="\$LAST_JOB_PID"', script, re.MULTILINE))
    assert pid_vars, "expected at least one *_PID variable capturing LAST_JOB_PID"

    trap_match = re.search(r"trap '([^']*)' EXIT", script)
    assert trap_match, "expected a single-quoted EXIT trap guarding job pids"
    job_trap = trap_match.group(1)

    missing = {v for v in pid_vars if f'"${{{v}:-}}"' not in job_trap}
    assert not missing, f"EXIT trap does not guard: {sorted(missing)}"
