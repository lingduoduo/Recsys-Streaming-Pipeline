import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import movie_categories as mc  # noqa: E402


def test_secondary_genre():
    assert mc.secondary_genre(["Sci-Fi", "Action"]) == "Action"
    assert mc.secondary_genre(["Drama"]) == "none"
    assert mc.secondary_genre("Crime,Thriller") == "Thriller"
    assert mc.secondary_genre([]) == "none"
