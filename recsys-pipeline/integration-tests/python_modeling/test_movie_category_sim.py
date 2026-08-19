import random
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import feature_derivations as mc  # noqa: E402
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
        assert set(m) == {"title", "genres", "release_year", "published_at", "new_release", "unsafe"}
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


def test_assign_movies_carries_freshness_and_safety_ground_truth():
    import movie_segment_producer as producer
    rng = random.Random(17)
    movies = producer.assign_movies(400, rng)

    fresh = [m for m in movies.values() if m["new_release"]]
    unsafe = [m for m in movies.values() if m["unsafe"]]
    assert 0.08 <= len(fresh) / len(movies) <= 0.24        # FRESH_SHARE 0.15 with sampling slack
    assert 0.00 <= len(unsafe) / len(movies) <= 0.06       # UNSAFE_SHARE 0.02 with sampling slack

    window_seconds = producer.FRESHNESS_WINDOW_DAYS * 86400
    now = int(time.time())
    for movie in movies.values():
        age = now - movie["published_at"]
        assert age >= 0
        assert (age <= window_seconds) == movie["new_release"]


def test_assign_users_is_stable_and_uses_only_allowlisted_dimensions():
    import movie_segment_producer as producer
    users = producer.assign_users(50, random.Random(17))
    again = producer.assign_users(50, random.Random(17))

    assert users == again                                   # seeded: same demographics every run
    assert set(next(iter(users.values()))) == {"gender", "age_band", "country", "subscription"}
    assert {u["subscription"] for u in users.values()} <= set(producer.SUBSCRIPTION_EFF)


def test_slate_events_carry_measurement_signals_on_the_right_event_types():
    import movie_segment_producer as producer
    rng = random.Random(3)
    movies = producer.assign_movies(20, rng)
    items = list(movies)
    user_meta = {"gender": "female", "age_band": "25-34", "country": "us", "subscription": "premium"}

    events = [e for _ in range(200) for e in producer.make_slate("user_1", user_meta, items, movies, rng)]
    impressions = [e for e in events if e["event_type"] == "impression"]
    clicks = [e for e in events if e["event_type"] == "click"]
    orders = [e for e in events if e["event_type"] == "order"]
    assert impressions and clicks and orders

    for event in impressions:
        assert event["user_features"] == user_meta
        assert isinstance(event["published_at"], int)
        assert isinstance(event["new_release"], bool)
        assert isinstance(event["unsafe_label"], bool)
        assert "dwell_millis" not in event and "rating" not in event

    for event in clicks:
        assert 0.0 <= event["completion_rate"] <= 1.0
        assert event["dwell_millis"] >= 0
        expected = "not_interested" if event["completion_rate"] < producer.NEGATIVE_COMPLETION_CUTOFF else None
        assert event["negative_feedback_reason"] == expected

    for event in orders:
        assert 3.0 <= event["rating"] <= 5.0


def test_orders_do_not_repeat_click_engagement_fields():
    """OnlineJoinerStreamingJob now attributes each feedback field to the latest event that
    actually set it, so an order carrying only `rating` no longer blanks the preceding click's
    dwell, completion, and negative-feedback signals. Producers need not copy them forward.
    """
    import movie_segment_producer as producer
    rng = random.Random(11)
    movies = producer.assign_movies(20, rng)
    items = list(movies)
    user_meta = {"gender": "male", "age_band": "35-44", "country": "us", "subscription": "premium"}

    events = [e for _ in range(200) for e in producer.make_slate("user_1", user_meta, items, movies, rng)]
    orders = [e for e in events if e["event_type"] == "order"]
    assert orders

    for order in orders:
        for field in ("completion_rate", "dwell_millis", "negative_feedback_reason"):
            assert field not in order, field


def test_user_click_bias_creates_a_documented_subscription_gap():
    import movie_segment_producer as producer
    assert producer.user_click_bias({"subscription": "premium"}) > producer.user_click_bias({"subscription": "free"})
    assert producer.user_click_bias({"subscription": "unknown_tier"}) == 0.0


def test_locale_and_timezone_are_stable_per_user():
    """Fails if context is drawn per slate: a user's locale must not change between slates."""
    import movie_segment_producer as producer
    rng = random.Random(17)
    movies = producer.assign_movies(20, rng)
    users = producer.assign_users(10, rng)
    items = list(movies)

    seen = {}
    for _ in range(40):
        user = rng.choice(list(users))
        for event in producer.make_slate(user, users[user], items, movies, rng):
            if event["event_type"] != "impression":
                continue
            previous = seen.setdefault(user, (event["locale"], event["timezone"]))
            assert previous == (event["locale"], event["timezone"])


def test_impressions_carry_typed_context_and_country_moves_to_user_features():
    import movie_segment_producer as producer
    rng = random.Random(17)
    movies = producer.assign_movies(20, rng)
    users = producer.assign_users(10, rng)
    user = next(iter(users))

    events = producer.make_slate(user, users[user], list(movies), movies, rng)
    impression = next(e for e in events if e["event_type"] == "impression")

    assert impression["surface"] in producer.SURFACES
    assert impression["device"] in ("ios", "android", "web")
    assert impression["locale"] in ("en-US", "en-CA", "fr-CA", "en-GB", "de-DE")
    assert "country" not in impression.get("context_features", {})
    assert impression["user_features"]["country"] == users[user]["country"]
    assert impression["context_features"] == {}


def test_low_completion_clicks_produce_an_abandon_and_high_ones_a_thumb_up():
    import movie_segment_producer as producer
    rng = random.Random(3)
    movies = producer.assign_movies(40, rng)
    users = producer.assign_users(20, rng)
    items = list(movies)

    types = set()
    for _ in range(200):
        user = rng.choice(list(users))
        for event in producer.make_slate(user, users[user], items, movies, rng):
            types.add(event["event_type"])

    assert "thumb_up" in types
    assert "abandon" in types
