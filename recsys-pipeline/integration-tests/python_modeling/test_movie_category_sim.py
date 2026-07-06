import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import movie_categories as mc  # noqa: E402
import movie_segment_producer as mp  # noqa: E402


# ── movie_categories (pure derivations) ─────────────────────────────────────────
def test_l1_l2_l3_from_list():
    assert mc.l1(["Sci-Fi", "Action"]) == "SciFi&Fantasy"
    assert mc.l2(["Sci-Fi", "Action"]) == "Sci-Fi"
    assert mc.l3(["Sci-Fi", "Action"], 2015) == "Sci-Fi·2010s"


def test_derivations_accept_comma_string_from_redis():
    assert mc.l1("Crime,Drama") == "Crime&Thriller"
    assert mc.l3("Comedy", 1997) == "Comedy·1990s"


def test_decade_and_unknowns():
    assert mc.decade(2003) == "2000s"
    assert mc.decade(None) == "unknown"
    assert mc.l2([]) == "unknown"
    assert mc.family_of("Documentary") == "Other"


# ── producer ground-truth model ─────────────────────────────────────────────────
def _movie(genres=("Sci-Fi",), year=2020):
    return {"title": "t", "genres": list(genres), "release_year": year}


def test_item_click_prob_family_and_recency_orderings():
    assert mp.item_click_prob(_movie(["Sci-Fi"])) > mp.item_click_prob(_movie(["Documentary"]))   # family
    assert mp.item_click_prob(_movie(["Drama"], 2020)) > mp.item_click_prob(_movie(["Drama"], 1985))  # recency


def test_item_click_prob_bounded():
    for g in mc.GENRES:
        for y in (1980, 1995, 2010, 2024):
            assert 0.02 <= mp.item_click_prob(_movie([g], y)) <= 0.6


def test_assign_movies_shape_and_deterministic():
    import random
    a = mp.assign_movies(30, random.Random(3))
    b = mp.assign_movies(30, random.Random(3))
    assert a == b and len(a) == 30
    for m in a.values():
        assert set(m) == {"title", "genres", "release_year"}
        assert 1 <= len(m["genres"]) <= 3
        assert 1980 <= m["release_year"] <= 2024


def test_movie_event_shape_matches_movieupdated():
    ev = mp.movie_event("movie_1", _movie(["Sci-Fi", "Action"], 2015))
    assert set(ev) == {"item_id", "title", "genres", "release_year", "timestamp"}
    assert ev["genres"] == ["Sci-Fi", "Action"] and isinstance(ev["release_year"], int)


def test_ratings_from_events_prefers_order_and_ignores_impressions():
    base = {"user_id": "user_1", "item_id": "movie_7", "timestamp_ms": 2000}
    rows = mp.ratings_from_events([
        {**base, "event_type": "impression"},
        {**base, "event_type": "click"},
        {**base, "event_type": "order", "timestamp_ms": 5000},
    ])
    assert rows == [{"userId": "user_1", "movieId": "movie_7",
                     "rating": 5.0, "timestamp": 5}]


# ── report: Redis movie-feature parsing (no Spark needed) ────────────────────────
def test_fetch_movie_features_parses_and_derives():
    import movie_category_report as rep

    fake = MagicMock()
    fake.scan_iter.return_value = ["movie:movie_1:features"]
    fake.hgetall.return_value = {"title": "Movie 1", "genres": "Sci-Fi,Action", "releaseYear": "2015"}
    with patch("redis.Redis", return_value=fake):
        rows = rep.fetch_movie_features("localhost", 6379)

    assert rows == [{"item_id": "movie_1", "l1": "SciFi&Fantasy",
                     "l2": "Sci-Fi", "l3": "Sci-Fi·2010s"}]


def test_fetch_movie_features_empty_when_redis_down():
    import movie_category_report as rep
    assert rep.fetch_movie_features("127.0.0.1", 6399) == []
