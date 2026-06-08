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


@pytest.fixture(autouse=True)
def reset_item_emb_cache(monkeypatch):
    monkeypatch.setattr(pipeline, "_item_emb_cache", None)


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


def test_train_two_tower_produces_valid_normalised_embeddings():
    user_tower, item_tower = pipeline.train_two_tower(epochs=5, seed=0)
    genre_t = torch.tensor(pipeline.MOVIE_GENRE_FEATS)
    with torch.no_grad():
        u = user_tower(torch.tensor([0]))
        i = item_tower(torch.arange(pipeline.N_MOVIES), genre_t)
    assert not torch.isnan(u).any(), "user embedding contains NaN"
    assert not torch.isnan(i).any(), "item embeddings contain NaN"
    norms = i.norm(dim=-1)
    torch.testing.assert_close(norms, torch.ones(pipeline.N_MOVIES), atol=1e-5, rtol=0)


def test_train_two_tower_parameters_updated_by_training():
    torch.manual_seed(0)
    untrained = pipeline.UserTower(pipeline.N_USERS, pipeline.EMB_DIM)
    initial_weight = untrained.emb.weight.detach().clone()
    user_tower, _ = pipeline.train_two_tower(epochs=5, seed=0)
    assert not torch.equal(user_tower.emb.weight, initial_weight), \
        "embedding weights should change after training"


def test_train_ranking_produces_valid_predictions():
    user_tower, item_tower = pipeline.train_two_tower(epochs=5, seed=0)
    # Snapshot initial ranker weights to verify training actually updates them
    torch.manual_seed(0)
    initial_ranker = pipeline.RankingTransformer(pipeline.EMB_DIM, pipeline.D_MODEL,
                                                 pipeline.NHEAD, pipeline.N_LAYERS)
    initial_weight = initial_ranker.head_click.weight.detach().clone()
    ranker = pipeline.train_ranking(user_tower, item_tower, epochs=5, seed=0)
    assert not torch.equal(ranker.head_click.weight, initial_weight), \
        "ranker weights should change after training"
    genre_t = torch.tensor(pipeline.MOVIE_GENRE_FEATS)
    with torch.no_grad():
        u_emb = user_tower(torch.tensor([0]))
        all_item_embs = item_tower(torch.arange(pipeline.N_MOVIES), genre_t)
        cand_embs = all_item_embs[:5]
        mask = pipeline.build_isolation_mask(5)
        click_p, rating_p, fav_p, rew_p, dwell_p = ranker(u_emb, cand_embs, mask)
    for name, out in [("click", click_p), ("rating", rating_p), ("favorite", fav_p),
                      ("rewatch", rew_p), ("dwell", dwell_p)]:
        assert out.shape == (5,), f"{name} wrong shape"
        assert (out >= 0).all() and (out <= 1).all(), f"{name} out of [0,1]"


def test_get_or_compute_item_embs_caches_result():
    fake_embs = np.random.default_rng(7).random((pipeline.N_MOVIES, pipeline.EMB_DIM)).astype(np.float32)
    call_count = 0

    class FakeItemSession:
        def run(self, _output_names, _inputs):
            nonlocal call_count
            call_count += 1
            # .copy() is intentional: ensures `result1 is result2` only passes
            # if the cache, not a fresh array from run(), is returned second call.
            return [fake_embs.copy()]

    sess = FakeItemSession()
    result1 = pipeline._get_or_compute_item_embs(sess)
    result2 = pipeline._get_or_compute_item_embs(sess)

    assert call_count == 1, "ONNX session should only be called once"
    assert result1 is result2, "Second call must return the cached array"
    np.testing.assert_array_equal(result1, fake_embs)


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
