import csv
import importlib.util
import os
import sys
import tempfile
from pathlib import Path

import numpy as np
import pytest
import torch


def load_pipeline_module():
    module_path = Path(__file__).resolve().parents[2] / "services/python-modeling/movielens_pipeline.py"
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


# ──────────────────────────────────────────────────────────────────────────────
# ratings.csv loader tests
# ──────────────────────────────────────────────────────────────────────────────

SAMPLE_RATINGS = [
    ("u1", "m001", "4.0", "1000"),
    ("u1", "m002", "5.0", "1001"),
    ("u1", "m003", "2.0", "1002"),  # below threshold — should not appear in rated_high
    ("u2", "m002", "4.5", "1003"),
    ("u2", "m004", "3.5", "1004"),
]


def _write_csv(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["userId", "movieId", "rating", "timestamp"])
        w.writerows(rows)


def test_load_user_history_watched():
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
        tmp = f.name
    try:
        _write_csv(SAMPLE_RATINGS, tmp)
        history = pipeline.load_user_history(Path(tmp), min_rating=3.5)
        assert "u1" in history
        assert set(history["u1"]["watched"]) == {"m001", "m002", "m003"}
    finally:
        os.unlink(tmp)


def test_load_user_history_rated_high():
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
        tmp = f.name
    try:
        _write_csv(SAMPLE_RATINGS, tmp)
        history = pipeline.load_user_history(Path(tmp), min_rating=3.5)
        rated_ids = {mid for mid, _ in history["u1"]["rated_high"]}
        assert "m001" in rated_ids
        assert "m002" in rated_ids
        assert "m003" not in rated_ids  # rating 2.0 < 3.5
    finally:
        os.unlink(tmp)


def test_load_movies_from_ratings():
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
        tmp = f.name
    try:
        _write_csv(SAMPLE_RATINGS, tmp)
        movies = pipeline.load_movies_from_ratings(Path(tmp))
        movie_ids = [m[0] for m in movies]
        assert "m001" in movie_ids
        assert "m004" in movie_ids
        assert len(set(movie_ids)) == len(movie_ids), "duplicate movie IDs"
    finally:
        os.unlink(tmp)


def test_pipeline_runs_with_ratings_csv(tmp_path):
    """Smoke test: pipeline trains and runs inference without error."""
    ratings = tmp_path / "ratings.csv"
    _write_csv(SAMPLE_RATINGS, ratings)
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    pipeline.main([
        "--ratings-csv", str(ratings),
        "--model-dir", str(model_dir),
        "--retrieval-epochs", "2",
        "--ranking-epochs", "2",
        "--force-train",
    ])
    assert (model_dir / "movielens_user_tower.onnx").is_file()
    assert (model_dir / "movielens_item_tower.onnx").is_file()
    assert (model_dir / "movielens_ranking.onnx").is_file()


def test_export_lookup_tables(tmp_path):
    ratings = tmp_path / "ratings.csv"
    _write_csv(SAMPLE_RATINGS, ratings)
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    import json
    mp = pipeline
    mp.main([
        "--ratings-csv", str(ratings),
        "--model-dir", str(model_dir),
        "--retrieval-epochs", "2",
        "--ranking-epochs", "2",
        "--force-train",
    ])
    lookup_path = model_dir / "movielens_lookups.json"
    assert lookup_path.is_file(), "movielens_lookups.json not found"
    with open(lookup_path) as f:
        lookups = json.load(f)
    assert "user_lookup" in lookups
    assert "item_lookup" in lookups
    assert "u1" in lookups["user_lookup"]
    assert "m001" in lookups["item_lookup"]


def test_write_embeddings_to_redis_skips_when_disabled(tmp_path, monkeypatch):
    """When REDIS_HOST is not set, no Redis connection should be attempted."""
    monkeypatch.delenv("REDIS_HOST", raising=False)
    ratings = tmp_path / "ratings.csv"
    _write_csv(SAMPLE_RATINGS, ratings)
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    # Should complete without raising a ConnectionError
    pipeline.main([
        "--ratings-csv", str(ratings),
        "--model-dir", str(model_dir),
        "--retrieval-epochs", "1",
        "--ranking-epochs", "1",
        "--force-train",
    ])


def test_parse_args_has_save_embeddings_to_redis_flag():
    config = pipeline.parse_args([])
    assert hasattr(config, "save_embeddings_to_redis")
    assert config.save_embeddings_to_redis is False


# ──────────────────────────────────────────────────────────────────────────────
# --fine-tune-csv flag tests
# ──────────────────────────────────────────────────────────────────────────────

mp = pipeline

REPLAY_RATINGS = [
    ("u1", "m005", "4.0", "2000"),
    ("u3", "m001", "5.0", "2001"),
]


def test_fine_tune_merges_ratings(tmp_path):
    base_csv = tmp_path / "ratings.csv"
    replay_csv = tmp_path / "replay.csv"
    _write_csv(SAMPLE_RATINGS, base_csv)
    _write_csv(REPLAY_RATINGS, replay_csv)
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    mp.main([
        "--ratings-csv", str(base_csv),
        "--fine-tune-csv", str(replay_csv),
        "--model-dir", str(model_dir),
        "--retrieval-epochs", "1",
        "--ranking-epochs", "1",
        "--force-train",
    ])
    assert (model_dir / "movielens_user_tower.onnx").is_file()


def test_parse_args_has_fine_tune_csv():
    config = mp.parse_args([])
    assert hasattr(config, "fine_tune_csv")
    assert config.fine_tune_csv is None
