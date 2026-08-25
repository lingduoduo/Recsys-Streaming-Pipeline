import json
import os
import re
from datetime import datetime

import yaml
import pytest

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "services",
    "java-retrieval-service",
    "src",
    "main",
    "resources",
    "application.yml",
)


README_PATH = os.path.join(os.path.dirname(__file__), "..", "README.md")


def test_readme_documents_item2vec_redis():
    with open(README_PATH) as f:
        content = f.read()
    assert "ITEM2VEC_SAVE_TO_REDIS=true" in content, (
        "README must show how to run Item2Vec with Redis output"
    )


def test_readme_documents_user_embedding_pipeline():
    with open(README_PATH) as f:
        content = f.read()
    assert "run-user-embedding-pipeline.sh" in content, (
        "README must mention run-user-embedding-pipeline.sh"
    )


def test_readme_documents_als_pipeline():
    with open(README_PATH) as f:
        content = f.read()
    assert "run-als-pipeline.sh" in content, (
        "README must mention run-als-pipeline.sh"
    )


def test_readme_documents_run_retrain():
    with open(README_PATH) as f:
        content = f.read()
    assert "run-retrain.sh" in content, (
        "README must document run-retrain.sh automated retraining"
    )


def test_readme_documents_dry_run():
    with open(README_PATH) as f:
        content = f.read()
    assert "DRY_RUN=1" in content, (
        "README must document DRY_RUN=1 mode for run-retrain.sh"
    )


CATALOG_PATH = os.path.join(os.path.dirname(__file__), "..", "sampledata", "catalog.json")


def test_readme_documents_every_measurement_environment_variable():
    # Operators can only tune what is written down; derive the list from the config
    # itself so a new measurement knob cannot ship undocumented.
    with open(CONFIG_PATH) as f:
        measurements = yaml.safe_load(f)["recsys"]["measurements"]
    variables = [re.match(r"\$\{([A-Z0-9_]+):", str(value)).group(1) for value in measurements.values()]
    with open(README_PATH) as f:
        content = f.read()
    missing = [variable for variable in variables if variable not in content]
    assert not missing, f"README must document the measurement variables {missing}"


def test_sample_catalog_carries_iso8601_publication_timestamps():
    with open(CATALOG_PATH) as f:
        catalog = json.load(f)
    timestamped = {item: profile["publishedAt"]
                   for item, profile in catalog.items() if "publishedAt" in profile}
    assert len(timestamped) >= 5, (
        "sample catalog must demonstrate timestamp freshness, not just the boolean fallback"
    )
    for item, value in timestamped.items():
        published = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        assert published.tzinfo is not None, f"{item} publishedAt must be a UTC instant"
    assert all("newRelease" in profile for profile in catalog.values()), (
        "newRelease must remain for services that have no publication timestamp"
    )
