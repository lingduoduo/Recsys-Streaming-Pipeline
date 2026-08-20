import json
import random
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import feature_derivations as mc  # noqa: E402
import movie_segment_producer as mp  # noqa: E402


# ── helpers ────────────────────────────────────────────────────────────────────
def _genre_in(family: str) -> str:
    """A genre whose l1 family is `family`. Inverts feature_derivations.l1 for tests."""
    from feature_derivations import GENRES, l1

    for genre in GENRES:
        if l1([genre]) == family:
            return genre
    if family == "Other":
        return "NotARealGenre"     # anything unmatched maps to Other
    raise AssertionError(f"no genre maps to family {family}")


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
    """Both event types are rare (abandon needs completion < 0.10; thumb_up needs
    completion >= 0.70 AND a 0.30 accept roll), so a small sample makes this a coin flip
    against whichever click-probability values happen to be in effect that run — a fixed
    200-slate sample was observed to flip across roughly 10% of seeds under an unrelated
    change to AFFINITY_STRENGTH. 5000 slates was verified empirically to produce both event
    types for every one of 200+ seeds tried (0 failures), so the assertion below holds for
    structural reasons — both events really do occur under this model — rather than by luck
    of this one fixed seed.
    """
    import movie_segment_producer as producer
    rng = random.Random(3)
    movies = producer.assign_movies(40, rng)
    users = producer.assign_users(20, rng)
    items = list(movies)

    types = set()
    for _ in range(5000):
        user = rng.choice(list(users))
        for event in producer.make_slate(user, users[user], items, movies, rng):
            types.add(event["event_type"])

    assert "thumb_up" in types
    assert "abandon" in types


# ── affinity (per-user taste) ──────────────────────────────────────────────────
def test_preferred_family_is_one_of_the_six_and_stable_per_user():
    import movie_segment_producer as producer

    families = set(producer.FAMILY_EFF)
    for n in range(1, 61):
        user = f"user_{n}"
        assert producer.user_preferred_family(user) in families
        assert producer.user_preferred_family(user) == producer.user_preferred_family(user)


def test_affinity_bonus_is_zero_mean_for_one_user():
    """The six bonuses a single user carries must cancel."""
    import movie_segment_producer as producer

    user = "user_3"
    bonuses = [
        producer.affinity_bonus(user, {"genres": [_genre_in(family)]})
        for family in producer.FAMILY_EFF
    ]

    assert sum(bonuses) == pytest.approx(0.0, abs=1e-12)


def test_affinity_bonus_is_zero_mean_across_the_population():
    """The identity that protects by_l1: averaged over users, each family nets zero.

    If this drifts, the movie-category report stops recovering FAMILY_EFF and the sim
    silently starts measuring something else.
    """
    import movie_segment_producer as producer

    users = [f"user_{n}" for n in range(1, 1201)]
    for family in producer.FAMILY_EFF:
        meta = {"genres": [_genre_in(family)]}
        mean = sum(producer.affinity_bonus(u, meta) for u in users) / len(users)
        assert mean == pytest.approx(0.0, abs=1e-9), family


def test_affinity_bonus_is_bounded_at_the_sim_default_population():
    """The zero-mean-across-users identity is exact only when NUM_USERS % 6 == 0 (1200 above
    is such a multiple). The sim's real default, 200, is not, so this pins the residual at
    N=200 to the bound documented on user_preferred_family: S*(6*n_f - N)/(5*N) for a family
    with n_f preferring users out of N. Uses the shipped AFFINITY_STRENGTH so this stays
    correct if that default is retuned.
    """
    import movie_segment_producer as producer

    n = 200
    s = producer.AFFINITY_STRENGTH
    users = [f"user_{i}" for i in range(1, n + 1)]

    for family in producer.FAMILY_EFF:
        n_f = sum(1 for u in users if producer.user_preferred_family(u) == family)
        expected = s * (6 * n_f - n) / (5 * n)
        meta = {"genres": [_genre_in(family)]}
        mean = sum(producer.affinity_bonus(u, meta) for u in users) / len(users)
        assert mean == pytest.approx(expected, abs=1e-9), family


def test_preferred_family_items_score_higher_for_that_user():
    import movie_segment_producer as producer

    user = "user_7"
    preferred = producer.user_preferred_family(user)
    other = next(f for f in producer.FAMILY_EFF if f != preferred)

    assert producer.affinity_bonus(user, {"genres": [_genre_in(preferred)]}) > 0
    assert producer.affinity_bonus(user, {"genres": [_genre_in(other)]}) < 0


def test_zero_strength_disables_the_effect(monkeypatch):
    """Provably opt-out: at strength 0 the bonus vanishes for every user and family."""
    import movie_segment_producer as producer

    monkeypatch.setattr(producer, "AFFINITY_STRENGTH", 0.0)
    for n in range(1, 25):
        for family in producer.FAMILY_EFF:
            assert producer.affinity_bonus(f"user_{n}", {"genres": [_genre_in(family)]}) == 0.0


def test_affinity_shifts_click_rate_toward_the_preferred_family():
    """End to end through make_slate: a user's clicks skew to their family.

    This is the property every downstream model depends on. Without it the data has a
    per-item effect and a per-user effect but no interaction, and personalization is
    unlearnable no matter how good the model is.
    """
    import movie_segment_producer as producer

    rng = random.Random(23)
    movies = producer.assign_movies(240, rng)
    users = producer.assign_users(40, rng)
    items = list(movies)

    user = "user_7"
    preferred = producer.user_preferred_family(user)
    from feature_derivations import l1

    preferred_clicks = other_clicks = 0
    preferred_impressions = other_impressions = 0
    for _ in range(400):
        events = producer.make_slate(user, users[user], items, movies, rng)
        clicked = {e["item_id"] for e in events if e["event_type"] == "click"}
        for event in events:
            if event["event_type"] != "impression":
                continue
            item = event["item_id"]
            if l1(movies[item]["genres"]) == preferred:
                preferred_impressions += 1
                preferred_clicks += item in clicked
            else:
                other_impressions += 1
                other_clicks += item in clicked

    preferred_ctr = preferred_clicks / preferred_impressions
    other_ctr = other_clicks / other_impressions
    assert preferred_ctr > other_ctr


def test_affinity_never_reaches_an_emitted_field():
    """Latent by construction. If this fails, a model could copy the answer."""
    import movie_segment_producer as producer

    rng = random.Random(11)
    movies = producer.assign_movies(60, rng)
    users = producer.assign_users(10, rng)
    user = "user_3"
    preferred = producer.user_preferred_family(user)

    for _ in range(30):
        for event in producer.make_slate(user, users[user], list(movies), movies, rng):
            flat = json.dumps(event, default=str)
            assert preferred not in flat
            assert "preferred_family" not in flat
            assert "affinity" not in flat


def test_zero_strength_makes_the_stream_immune_to_the_preferred_family(monkeypatch):
    """Opt-out, asserted exactly rather than statistically.

    At AFFINITY_STRENGTH = 0, affinity_bonus returns +0.0 for the preferred family and
    -0.0 for every other — both are no-ops in float addition — and the function draws no
    rng. So the preferred family cannot influence a single float in the generated stream:
    (1) the same seed must reproduce the same stream, and (2) the stream must be unchanged
    even if every user's preferred family is permuted to something else. A statistical
    threshold on the preferred-vs-other click gap (the previous version of this test) is
    not needed and was flaky: it failed on 8 of 100 seeds because ~333 preferred
    impressions carries se ~= 0.022, close to the 0.06 threshold.
    """
    import movie_segment_producer as producer

    monkeypatch.setattr(producer, "AFFINITY_STRENGTH", 0.0)

    def event_key(event: dict) -> tuple:
        return (event["user_id"], event["item_id"], event["event_type"], event["position"],
                event.get("completion_rate"), event.get("rating"))

    def generate() -> list[tuple]:
        rng = random.Random(29)
        movies = producer.assign_movies(240, rng)
        users = producer.assign_users(40, rng)
        items = list(movies)
        keys = []
        for _ in range(400):
            user = rng.choice(list(users))
            keys.extend(event_key(e) for e in producer.make_slate(user, users[user], items, movies, rng))
        return keys

    first_run = generate()
    assert first_run == generate()          # same seed, strength 0 -> identical stream

    families = sorted(producer.FAMILY_EFF)
    permuted = {f: families[(i + 1) % len(families)] for i, f in enumerate(families)}
    monkeypatch.setattr(producer, "user_preferred_family",
                        lambda user: permuted[families[producer._user_index(user) % len(families)]])
    assert generate() == first_run          # permuting preferred family changes nothing
