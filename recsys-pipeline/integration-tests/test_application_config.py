import os
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


def test_deep_learning_weight_default_is_valid_optin_weight():
    # DeepLearningPredictionService blending is opt-in: the default weight is 0.0
    # (documented in README as "blend weight 0.0 by default, opt-in"), so the base
    # hybrid works without the ONNX model. Guard that the default is a valid blend
    # weight in [0.0, 1.0] rather than forcing it on.
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    raw = config["recsys"]["bandit"]["deep-learning-weight"]
    # Extract the default value from the Spring placeholder ${VAR:default}
    default_str = str(raw).split(":")[-1].rstrip("}")
    default_val = float(default_str)
    assert 0.0 <= default_val <= 1.0, (
        f"deep-learning-weight default is {default_val}; must be a blend weight in [0.0, 1.0]"
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
