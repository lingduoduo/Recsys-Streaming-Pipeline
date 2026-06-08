import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest
import torch


def load_pipeline_module():
    module_path = Path(__file__).with_name("movielens_pipeline.py")
    spec = importlib.util.spec_from_file_location("movielens_pipeline", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


pipeline = load_pipeline_module()


def test_isolation_mask_allows_user_and_self_attention_only():
    mask = pipeline.build_isolation_mask(3)

    assert mask.shape == (4, 4)
    assert torch.all(mask[0] == 0)
    assert torch.all(mask[:, 0] == 0)
    assert torch.all(torch.diag(mask) == 0)
    assert torch.isneginf(mask[1, 2])
    assert torch.isneginf(mask[3, 1])


def test_retrieve_top_indices_excludes_watched_movies_and_clamps_top_k():
    user_emb = np.array([[1.0, 0.0]], dtype=np.float32)
    item_embs = np.zeros((pipeline.N_MOVIES, 2), dtype=np.float32)
    item_embs[:, 0] = np.arange(pipeline.N_MOVIES, dtype=np.float32)
    watched = [movie[0] for movie in pipeline.MOVIES[:-2]]

    indices, scores = pipeline.retrieve_top_indices(user_emb, item_embs, watched, top_k=10)

    assert indices.tolist() == [pipeline.N_MOVIES - 1, pipeline.N_MOVIES - 2]
    assert scores.tolist() == [pipeline.N_MOVIES - 1, pipeline.N_MOVIES - 2]


def test_clamp_top_k_rejects_non_positive_values():
    with pytest.raises(ValueError, match="at least 1"):
        pipeline.clamp_top_k(0, watched_count=0)


def test_aggregate_engagement_scores_uses_configured_weights():
    ones = np.ones(2, dtype=np.float32)

    scores = pipeline.aggregate_engagement_scores(ones, ones, ones, ones, ones)

    np.testing.assert_allclose(scores, ones)


def test_parse_args_supports_selected_users_and_model_directory(tmp_path):
    args = pipeline.parse_args(
        ["--user", "alice", "--user", "bob", "--top-k", "4", "--model-dir", str(tmp_path)],
    )

    assert args.user == ["alice", "bob"]
    assert args.top_k == 4
    assert args.model_dir == tmp_path


@pytest.mark.skipif(
    not pipeline.DEFAULT_ARTIFACTS.all_exist(),
    reason="checked-in ONNX sample artifacts are unavailable",
)
def test_real_onnx_pipeline_returns_sorted_unwatched_recommendations():
    recommendations, _ = pipeline.score_recommendations(
        "alice",
        pipeline.load_sessions(),
        top_k=3,
    )

    watched = set(pipeline.USER_HISTORY["alice"]["watched"])
    assert len(recommendations) == 3
    assert all(rec.movie_id not in watched for rec in recommendations)
    assert [rec.final_score for rec in recommendations] == sorted(
        (rec.final_score for rec in recommendations),
        reverse=True,
    )
