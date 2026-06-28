import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import recall_eval_report as r  # noqa: E402


def test_tokenize_and_bm25_ranks_matching_doc_first():
    corpus = {"m1": r.tokenize("Sci-Fi Action"), "m2": r.tokenize("Drama Romance")}
    bm = r.build_bm25(corpus)
    qterms = set(r.tokenize("sci fi"))
    assert r.bm25_score(bm, "m1", qterms) > r.bm25_score(bm, "m2", qterms)


def test_cosine_and_rrf():
    assert r.cosine([1.0, 0.0], [1.0, 0.0]) == 1.0
    assert r.cosine([1.0, 0.0], [0.0, 1.0]) == 0.0
    fused = r.rrf([["a", "b"], ["b", "a"]])
    assert fused["a"] == fused["b"]  # symmetric → tie


def test_rank_topk_is_deterministic_on_ties():
    assert r.rank_topk({"b": 0.0, "a": 0.0, "c": 0.0}, 2) == ["a", "b"]  # tie → id order


def test_evaluate_loo_bm25_vs_embedding():
    corpus = {
        "m1": r.tokenize("sci fi action"),
        "m2": r.tokenize("sci fi space"),
        "m3": r.tokenize("drama romance"),
        "m4": r.tokenize("comedy"),
    }
    vecs = {"m1": [1.0, 0.0], "m2": [0.9, 0.1], "m3": [0.0, 1.0], "m4": [-1.0, 0.0]}
    clicks = {"u1": ["m1", "m2"], "u2": ["m3", "m4"]}

    rows = {(x["method"], x["k"]): x for x in r.evaluate(clicks, corpus, vecs, ks=[1])}

    # u1: both methods recover the held-out sci-fi twin; u2: bm25 (no shared tokens) misses,
    # embedding recovers 1 of 2 folds. → hand-computed aggregates over 2 users:
    assert rows[("bm25", 1)]["recall_at_k"] == 0.5
    assert rows[("bm25", 1)]["hitrate_at_k"] == 0.5
    assert rows[("embedding", 1)]["recall_at_k"] == 0.75
    assert rows[("embedding", 1)]["hitrate_at_k"] == 1.0
    assert rows[("bm25", 1)]["users_evaluated"] == 2
    assert rows[("bm25", 1)]["instances"] == 4


def test_evaluate_skips_users_with_one_click():
    corpus = {"m1": ["a"], "m2": ["b"]}
    rows = r.evaluate({"u1": ["m1"]}, corpus, {}, ks=[1])
    assert all(x["users_evaluated"] == 0 for x in rows)
