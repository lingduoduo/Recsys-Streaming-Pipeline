import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import feature_derivations as sf  # noqa: E402
import movielens_segment_producer as mp  # noqa: E402


# ── segment_features (pure derivations) ─────────────────────────────────────────
def test_derive_age_band():
    assert sf.derive_age_band(20) == "18-24"
    assert sf.derive_age_band(30) == "25-34"
    assert sf.derive_age_band(60) == "55+"
    assert sf.derive_age_band("33") == "25-34"
    assert sf.derive_age_band(None) == "unknown"


def test_derive_geo_by_zip_first_digit():
    assert sf.derive_geo("90210") == "West"
    assert sf.derive_geo("02139") == "Northeast"
    assert sf.derive_geo("70001") == "South-Central"
    assert sf.derive_geo("") == "unknown"


# ── producer ground-truth model ─────────────────────────────────────────────────
def _demo(age=30, gender="F", occupation="student", zip_code="90210"):
    return {"age": age, "gender": gender, "occupation": occupation, "zip_code": zip_code}


def test_click_prob_age_platform_occupation_geo_orderings():
    assert mp.click_prob(_demo(age=30), "ios", "home_feed") > \
        mp.click_prob(_demo(age=60), "ios", "home_feed")                                   # 25-34 > 55+
    assert mp.click_prob(_demo(), "ios", "home_feed") > \
        mp.click_prob(_demo(), "web", "home_feed")                                         # ios > web
    assert mp.click_prob(_demo(occupation="student"), "web", "home_feed") > \
        mp.click_prob(_demo(occupation="retired"), "web", "home_feed")                     # student > retired
    assert mp.click_prob(_demo(gender="F"), "web", "home_feed") > \
        mp.click_prob(_demo(gender="M"), "web", "home_feed")
    assert mp.click_prob(_demo(zip_code="90001"), "web", "home_feed") > \
        mp.click_prob(_demo(zip_code="70001"), "web", "home_feed")                         # West > South-Central


def test_click_prob_surface_ordering():
    # search_results (0.04) > home_feed (0.02) per SURFACE_EFF
    assert mp.click_prob(_demo(), "web", "search_results") > mp.click_prob(_demo(), "web", "home_feed")


def test_click_prob_bounded():
    for age in (18, 30, 64):
        for occ in mp.OCCUPATIONS:
            for plat in mp.PLATFORMS:
                for surface in mp.SURFACES:
                    p = mp.click_prob(_demo(age=age, occupation=occ), plat, surface)
                    assert 0.02 <= p <= 0.95


def test_assign_demographics_canonical_fields_and_deterministic():
    import random
    a = mp.assign_demographics(40, random.Random(5))
    b = mp.assign_demographics(40, random.Random(5))
    assert a == b and len(a) == 40
    for d in a.values():
        assert set(d) == {"age", "gender", "occupation", "zip_code"}
        assert 18 <= d["age"] <= 64
        assert len(d["zip_code"]) == 5 and d["zip_code"].isdigit()


def test_make_slate_threads_session_id_across_multiple_slates():
    import random
    rng = random.Random(0)
    items = [f"movie_{i}" for i in range(1, 11)]
    s1 = mp.make_slate("user_1", _demo(), items, rng, "sessX")
    s2 = mp.make_slate("user_1", _demo(), items, rng, "sessX")
    # every event in both slates carries the given session_id
    assert all(e["session_id"] == "sessX" for e in s1 + s2)
    # the two slates are distinct request_ids → a session spans multiple slates
    assert len({e["request_id"] for e in s1 + s2}) == 2


def test_demographics_event_shape_matches_userupdated():
    ev = mp.demographics_event("user_1", _demo())
    assert set(ev) == {"user_id", "age", "gender", "occupation", "zip_code", "timestamp"}
    assert isinstance(ev["age"], int)


def test_rating_mean_tracks_engagement_segments():
    assert mp.rating_mean(_demo(age=30)) > mp.rating_mean(_demo(age=60))            # 25-34 > 55+
    assert mp.rating_mean(_demo(occupation="student")) > \
        mp.rating_mean(_demo(occupation="retired"))
    assert 1.0 <= mp.rating_mean(_demo(age=60, occupation="retired")) <= 5.0


def test_rating_event_shape_matches_ratingevent():
    import random
    ev = mp.rating_event("user_1", "movie_3", _demo(), random.Random(0))
    assert set(ev) == {"user_id", "item_id", "event_type", "rating", "timestamp"}
    assert ev["event_type"] == "rating"
    assert 1.0 <= ev["rating"] <= 5.0


# ── typed context fields on impressions ─────────────────────────────────────────
def test_impressions_carry_the_four_typed_context_fields():
    import random
    rng = random.Random(0)
    items = [f"movie_{i}" for i in range(1, 11)]
    slate = mp.make_slate("user_1", _demo(), items, rng, "sess1")
    impressions = [e for e in slate if e["event_type"] == "impression"]
    assert impressions
    for e in impressions:
        assert e["surface"] in mp.SURFACES
        assert e["device"] in mp.PLATFORMS
        assert e["locale"] == "en-US"
        assert e["timezone"] in set(mp.ZIP_TIMEZONE.values())


def test_context_features_is_empty_on_every_event():
    import random
    rng = random.Random(0)
    items = [f"movie_{i}" for i in range(1, 11)]
    slate = mp.make_slate("user_1", _demo(), items, rng, "sess1")
    assert slate
    assert all(e["context_features"] == {} for e in slate)


def test_locale_is_always_en_us_movielens_zips_are_us_only():
    import random
    rng = random.Random(0)
    items = [f"movie_{i}" for i in range(1, 11)]
    for zip_code in ("90210", "02139", "70001", ""):
        slate = mp.make_slate("user_1", _demo(zip_code=zip_code), items, rng, "sessX")
        impressions = [e for e in slate if e["event_type"] == "impression"]
        assert impressions
        assert all(e["locale"] == "en-US" for e in impressions)


def test_timezone_maps_from_the_zip_region():
    import random
    rng = random.Random(0)
    items = [f"movie_{i}" for i in range(1, 11)]
    cases = {
        "02139": "America/New_York",   # Northeast
        "70001": "America/Chicago",    # South-Central
        "80001": "America/Denver",     # Mountain
        "90210": "America/Los_Angeles",  # West
    }
    for zip_code, expected_tz in cases.items():
        region = sf.derive_geo(zip_code)
        assert mp.ZIP_TIMEZONE[region] == expected_tz
        slate = mp.make_slate("user_1", _demo(zip_code=zip_code), items, rng, "sessX")
        impressions = [e for e in slate if e["event_type"] == "impression"]
        assert all(e["timezone"] == expected_tz for e in impressions)


def test_unknown_zip_region_yields_a_none_timezone():
    import random
    rng = random.Random(0)
    items = [f"movie_{i}" for i in range(1, 11)]
    assert sf.derive_geo("") == "unknown"
    slate = mp.make_slate("user_1", _demo(zip_code=""), items, rng, "sessX")
    impressions = [e for e in slate if e["event_type"] == "impression"]
    assert impressions
    assert all(e["timezone"] is None for e in impressions)
