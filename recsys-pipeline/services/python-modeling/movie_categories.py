"""Pure 3-level movie category derivations, shared by the movie producer and report.

No heavy imports (no kafka/pyspark) so it can be imported under spark-submit's Python too.
From a movie's genres + releaseYear:
  l1 = genre family (broad)         e.g. "SciFi&Fantasy"
  l2 = primary (first) genre        e.g. "Sci-Fi"
  l3 = primary genre x release decade  e.g. "Sci-Fi·2010s"
`genres` may be a list (producer) or a comma-joined string (from Redis).
"""
from __future__ import annotations

GENRES = [
    "Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary",
    "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance",
    "Sci-Fi", "Thriller", "War", "Western",
]

# l1 genre family
GENRE_FAMILY = {
    "Action": "Action&Adventure", "Adventure": "Action&Adventure",
    "War": "Action&Adventure", "Western": "Action&Adventure",
    "Sci-Fi": "SciFi&Fantasy", "Fantasy": "SciFi&Fantasy", "Animation": "SciFi&Fantasy",
    "Drama": "Drama&Romance", "Romance": "Drama&Romance", "Musical": "Drama&Romance",
    "Comedy": "Comedy", "Children": "Comedy",
    "Crime": "Crime&Thriller", "Thriller": "Crime&Thriller", "Mystery": "Crime&Thriller",
    "Film-Noir": "Crime&Thriller", "Horror": "Crime&Thriller",
    "Documentary": "Other",
}
FAMILIES = ["Action&Adventure", "SciFi&Fantasy", "Drama&Romance", "Comedy", "Crime&Thriller", "Other"]


def _as_list(genres) -> list[str]:
    if genres is None:
        return []
    if isinstance(genres, str):
        return [g for g in genres.split(",") if g]
    return list(genres)


def primary_genre(genres) -> str:
    g = _as_list(genres)
    return g[0] if g else "unknown"


def family_of(genre: str) -> str:
    return GENRE_FAMILY.get(genre, "Other")


def decade(year) -> str:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return "unknown"
    return f"{(y // 10) * 10}s"


def l1(genres) -> str:
    return family_of(primary_genre(genres))


def l2(genres) -> str:
    return primary_genre(genres)


def l3(genres, year) -> str:
    return f"{primary_genre(genres)}·{decade(year)}"
