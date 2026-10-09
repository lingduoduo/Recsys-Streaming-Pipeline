import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import feature_derivations as mc  # noqa: E402


def test_secondary_genre():
    assert mc.secondary_genre(["Sci-Fi", "Action"]) == "Action"
    assert mc.secondary_genre(["Drama"]) == "none"
    assert mc.secondary_genre("Crime,Thriller") == "Thriller"
    assert mc.secondary_genre([]) == "none"


def test_fetch_movie_meta_keeps_the_producers_genre_order(monkeypatch):
    # The primary genre is position 0, so the Redis parse must not reorder the list.
    import types

    class FakeRedis:
        def __init__(self, **_):
            pass

        def scan_iter(self, match):
            return iter(["movie:m1:features"])

        def hgetall(self, key):
            return {"genres": "Sci-Fi,Action,Comedy", "releaseYear": "2011"}

    monkeypatch.setitem(sys.modules, "redis", types.SimpleNamespace(Redis=FakeRedis))

    assert mc.fetch_movie_meta("h", 1) == [
        {"item_id": "m1", "genres": ["Sci-Fi", "Action", "Comedy"], "release_year": 2011}]


def _order_frame(pd, genre_lists):
    return pd.DataFrame({"item_id": [f"m{i}" for i in range(len(genre_lists))],
                         "label": [0.0] * len(genre_lists), "genres": genre_lists})


def test_genre_order_check_flags_alphabetical_lists():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    kw = dash.compute_keyword(_order_frame(pd, [["Action", "Drama"], ["Comedy", "Crime", "War"]] * 10))

    assert kw["genre_order"] == {"multi_genre_movies": 20, "sorted_share": 1.0, "chance_share": 0.3333}
    assert kw["headline"].startswith("genre lists look alphabetical")


def test_genre_order_check_stays_quiet_on_producer_order():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    kw = dash.compute_keyword(_order_frame(pd, [["Sci-Fi", "Action"], ["Action", "Drama"]] * 10))

    assert kw["genre_order"]["sorted_share"] == 0.5
    assert not kw["headline"].startswith("genre lists look alphabetical")


def test_genre_order_check_counts_distinct_multi_genre_movies():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    # One sorted movie repeated across 30 samples is one movie, below the 20-movie floor;
    # single-genre movies are sorted trivially and carry no evidence either way.
    df = pd.DataFrame({"item_id": ["m1"] * 30 + ["m2", "m3"], "label": [0.0] * 32,
                       "genres": [["Action", "Drama"]] * 30 + [["Drama"], ["Comedy"]]})
    kw = dash.compute_keyword(df)

    assert kw["genre_order"]["multi_genre_movies"] == 1
    assert not kw["headline"].startswith("genre lists look alphabetical")
