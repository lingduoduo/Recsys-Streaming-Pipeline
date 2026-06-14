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


def test_deep_learning_weight_default_is_nonzero():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    raw = config["recsys"]["bandit"]["deep-learning-weight"]
    # Extract the default value from the Spring placeholder ${VAR:default}
    default_str = str(raw).split(":")[-1].rstrip("}")
    default_val = float(default_str)
    assert default_val > 0.0, (
        f"deep-learning-weight default is {default_val}; must be > 0.0 "
        "or the ONNX model score is ignored"
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
