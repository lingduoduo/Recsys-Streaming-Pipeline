"""Shared feature derivations + movie-metadata fetch for the producers and reports.

Consolidates three small, dependency-light helper modules (no kafka/pyspark, so they import
cleanly under spark-submit's Python too):

  • movie categories — 3-level genre derivations (l1 family / l2 primary genre / l3 genre×decade)
  • user segments    — age_band / geo buckets from the canonical UserEvent demographics
  • movie metadata   — fetch genres + release year from Redis movie:{id}:features

`genres` may be a list (producer) or a comma-joined string (from Redis).
"""
from __future__ import annotations

# ── movie categories ────────────────────────────────────────────────────────────
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


def secondary_genre(genres) -> str:
    """Second genre (the 'subkeyword'); 'none' when a movie has only one genre."""
    g = _as_list(genres)
    return g[1] if len(g) > 1 else "none"


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


# ── user segments ─────────────────────────────────────────────────────────────────
_AGE_BINS = [(0, 24, "18-24"), (25, 34, "25-34"), (35, 44, "35-44"),
             (45, 54, "45-54"), (55, 200, "55+")]

# US ZIP first digit → coarse region (the canonical zip_code is a string).
_ZIP_REGION = {
    "0": "Northeast", "1": "Northeast", "2": "Mid-Atlantic", "3": "Southeast",
    "4": "Midwest", "5": "Midwest", "6": "South-Central", "7": "South-Central",
    "8": "Mountain", "9": "West",
}


def derive_age_band(age) -> str:
    try:
        a = int(age)
    except (TypeError, ValueError):
        return "unknown"
    for lo, hi, label in _AGE_BINS:
        if lo <= a <= hi:
            return label
    return "unknown"


def derive_geo(zip_code) -> str:
    s = str(zip_code) if zip_code is not None else ""
    return _ZIP_REGION.get(s[:1], "unknown")


# ── movie metadata (Redis) ────────────────────────────────────────────────────────
def fetch_movie_meta(host: str, port: int) -> list[dict]:
    """Return [{item_id, genres: [..], release_year: int|None}] from movie:*:features.

    Returns [] if Redis is unreachable (the report then has no genres to break down by).
    """
    try:
        import redis
        client = redis.Redis(host=host, port=port, decode_responses=True)
        rows = []
        for key in client.scan_iter(match="movie:*:features"):
            h = client.hgetall(key)
            if not h:
                continue
            item_id = key.split(":")[1]
            genres = [g for g in (h.get("genres") or "").split(",") if g]
            try:
                year = int(h["releaseYear"])
            except (KeyError, ValueError, TypeError):
                year = None
            rows.append({"item_id": item_id, "genres": genres, "release_year": year})
        return rows
    except Exception as e:  # noqa: BLE001
        print(f"[warn] could not read movie meta from Redis ({e})")
        return []
