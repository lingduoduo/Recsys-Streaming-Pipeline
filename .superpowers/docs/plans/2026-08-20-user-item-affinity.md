# Joint User-Item Affinity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The movie-category sim generates a per-user item preference strong enough that the existing next-item harness detects it, without disturbing any ground truth the sim already injects.

**Architecture:** Each user gets one preferred l1 genre family, derived as a pure function of their user id so it never rides in `user_features`. Its click-probability bonus is zero-mean both per user and across the population, so the marginal per-family rates the `by_l1` report recovers are unchanged. The strength constant is then calibrated by measurement rather than chosen by argument.

**Tech Stack:** Python 3, pandas for the calibration harness, pytest. No Scala, no Spark, no Kafka in the implementation tasks.

**Spec:** [.superpowers/docs/specs/2026-08-20-user-item-affinity-design.md](../specs/2026-08-20-user-item-affinity-design.md)

## Global Constraints

- Production code changes land in exactly one file: `recsys-pipeline/services/python-modeling/movie_segment_producer.py`. Tests go in `recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py`. Task 2 adds one new file. Nothing else — no Scala, no other producer, no schema, no serving path.
- Python tests run from the repository root: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q`
- **`assign_users` must keep returning exactly `{gender, age_band, country, subscription}`.** `test_assign_users_is_stable_and_uses_only_allowlisted_dimensions` asserts that set exactly, and those keys are copied into `user_features`, which feeds `governance_measurements.DEFAULT_DIMENSIONS` — an allowlist whose members become published fairness groups. Do not add the affinity to that dict.
- **The affinity is latent.** It must never appear in `user_features`, `context_features`, or any typed event field. Deriving it as a pure function of the user id makes this structural rather than a filter someone could forget.
- The six l1 families are exactly the keys of `FAMILY_EFF`: `SciFi&Fantasy`, `Action&Adventure`, `Crime&Thriller`, `Comedy`, `Drama&Romance`, `Other`. `feature_derivations.l1()` returns `"Other"` for unmatched, empty, or unknown genres — verified — so every item maps to one of the six with no special case.
- The bonus must be zero-mean **twice**: the six bonuses for one user sum to zero, and across a uniformly-assigned population the mean bonus for any given item family is zero. The second identity is what protects the `by_l1` report.
- Follow the established pure-helper idiom already in this file: `user_locale(user, country)` and `user_timezone(country)` exist precisely because per-user attributes must stay out of `user_features`. `_user_index(user)` parses the trailing integer from a `user_N` id and returns 0 for anything else.

---

### Task 1: Per-user family affinity in the producer

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/movie_segment_producer.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py`

**Interfaces:**
- Consumes: `FAMILY_EFF` (the six families), `l1(genres)` from `feature_derivations`, `_user_index(user)`.
- Produces: `AFFINITY_STRENGTH` (module constant, env-tunable), `user_preferred_family(user) -> str`, `affinity_bonus(user, meta) -> float`. Task 2 imports all three.

- [ ] **Step 1: Write the failing test**

Add to `recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py`:

```python
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
```

Add this helper next to the other module-level helpers in that test file — it inverts `l1` so a test can name a family and get a genre that maps to it:

```python
def _genre_in(family: str) -> str:
    """A genre whose l1 family is `family`. Inverts feature_derivations.l1 for tests."""
    from feature_derivations import GENRES, l1

    for genre in GENRES:
        if l1([genre]) == family:
            return genre
    if family == "Other":
        return "NotARealGenre"     # anything unmatched maps to Other
    raise AssertionError(f"no genre maps to family {family}")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py -q`
Expected: FAIL — `AttributeError: module 'movie_segment_producer' has no attribute 'user_preferred_family'`.

- [ ] **Step 3: Write minimal implementation**

In `movie_segment_producer.py`, add the constant next to the other injected-effect constants (near `SUBSCRIPTION_EFF` and `SURFACE_EFF`):

```python
# Per-user taste. Items in a user's preferred l1 family gain AFFINITY_STRENGTH; each of the
# other five loses a fifth as much, so the six bonuses cancel for that user AND — because the
# preferred family is drawn uniformly over users — the mean bonus for any given family is zero
# across the population. That second identity is what keeps the by_l1 report recovering
# FAMILY_EFF unchanged: this signal sits underneath the existing ground truth, not on top of it.
#
# The default is calibrated by measurement, not argument — see the plan's Task 3 and the spec.
AFFINITY_STRENGTH = float(os.getenv("AFFINITY_STRENGTH", "0.15"))
```

Add the two functions after `user_timezone`:

```python
def user_preferred_family(user: str) -> str:
    """The l1 family this user favours: a pure function of the user id.

    Deliberately NOT stored in the dict assign_users returns. That dict is copied into
    user_features, which feeds governance_measurements.DEFAULT_DIMENSIONS — an allowlist whose
    members are published as fairness groups — and a taste attribute does not belong there.
    Deriving it here also keeps it latent by construction: a model must infer it from behaviour
    rather than read it off a field, which is the whole point of the signal.
    """
    families = sorted(FAMILY_EFF)
    return families[_user_index(user) % len(families)]


def affinity_bonus(user: str, meta: dict) -> float:
    """Click-probability bonus for showing `meta` to `user`.

    +AFFINITY_STRENGTH for the user's preferred family, -AFFINITY_STRENGTH/5 for each of the
    other five. Zero-mean per user by construction; zero-mean across users because
    user_preferred_family cycles uniformly over the families.
    """
    others = len(FAMILY_EFF) - 1
    return (AFFINITY_STRENGTH if l1(meta["genres"]) == user_preferred_family(user)
            else -AFFINITY_STRENGTH / others)
```

No import changes are needed: line 26 already reads `from feature_derivations import GENRES, l1, decade`, and `os` is imported at line 20.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py -q`
Expected: PASS.

Note `user_preferred_family` cycles over `sorted(FAMILY_EFF)` by user index rather than drawing from an rng. That is deliberate: it makes the across-user zero-mean identity exact rather than approximate, so the population test can assert `abs=1e-9` instead of a loose statistical tolerance, and a failure means a real bug rather than an unlucky seed.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/movie_segment_producer.py \
        recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py
git commit -m "feat: give each simulated user a preferred genre family"
```

---

### Task 2: Wire the affinity into click probability

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/movie_segment_producer.py:215-218` (the `click_prob` composition inside `make_slate`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py`

**Interfaces:**
- Consumes: `affinity_bonus(user, meta)` and `AFFINITY_STRENGTH` from Task 1.
- Produces: no new names. `make_slate` keeps its signature; only the click decision changes.

- [ ] **Step 1: Write the failing test**

Add to the test file:

```python
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


def test_zero_strength_removes_the_preferred_family_gap(monkeypatch):
    """Opt-out, asserted on the effect rather than on determinism.

    Comparing a strength-0 run against itself would only prove the rng is seeded. What
    matters is that at strength 0 the preferred family stops being special, so the sim
    behaves as it did before this change.
    """
    import movie_segment_producer as producer
    from feature_derivations import l1

    monkeypatch.setattr(producer, "AFFINITY_STRENGTH", 0.0)
    rng = random.Random(29)
    movies = producer.assign_movies(240, rng)
    users = producer.assign_users(40, rng)
    items = list(movies)

    user = "user_7"
    preferred = producer.user_preferred_family(user)
    pref_clicks = pref_imps = other_clicks = other_imps = 0
    for _ in range(400):
        events = producer.make_slate(user, users[user], items, movies, rng)
        clicked = {e["item_id"] for e in events if e["event_type"] == "click"}
        for event in events:
            if event["event_type"] != "impression":
                continue
            item = event["item_id"]
            if l1(movies[item]["genres"]) == preferred:
                pref_imps += 1
                pref_clicks += item in clicked
            else:
                other_imps += 1
                other_clicks += item in clicked

    # No taste term, so any remaining gap is the category effect plus sampling noise.
    assert abs(pref_clicks / pref_imps - other_clicks / other_imps) < 0.06
```

Add `import json` to the test file's imports if it is not already there.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py -q`
Expected: FAIL on `test_affinity_shifts_click_rate_toward_the_preferred_family` — the preferred and other CTRs are statistically indistinguishable because `click_prob` does not yet include the term.

- [ ] **Step 3: Write minimal implementation**

In `make_slate`, add the affinity term to the existing composition. The current lines read:

```python
        click_prob = min(0.6, max(0.02, item_click_prob(meta) + user_click_bias(user_meta)
                                  + SURFACE_EFF[surface]))
```

Replace with:

```python
        click_prob = min(0.6, max(0.02, item_click_prob(meta) + user_click_bias(user_meta)
                                  + SURFACE_EFF[surface] + affinity_bonus(user, meta)))
```

Update the comment two lines above it, which currently lists only the item, subscription, and surface effects, so it names the taste term too:

```python
        # independent per-item click decision → per-item CTR reflects the item's category,
        # shifted by the user's documented subscription effect, the surface it appeared on,
        # and the user's own taste for that item's genre family
```

Nothing else in `make_slate` changes. Slates are still drawn with `rng.sample`, so impressions remain a uniform sample of the catalog.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q`
Expected: PASS, the whole Python suite. `test_assign_users_is_stable_and_uses_only_allowlisted_dimensions` must still pass untouched — if it fails, the affinity leaked into `assign_users`, which is the one thing this design forbids.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/movie_segment_producer.py \
        recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py
git commit -m "feat: let user taste shift click probability by genre family"
```

---

### Task 3: Calibrate the strength by measurement

The spec deliberately does not name a default. This task measures one.

**Files:**
- Create: `recsys-pipeline/services/python-modeling/affinity_calibration.py`
- Modify: `recsys-pipeline/services/python-modeling/movie_segment_producer.py` (the `AFFINITY_STRENGTH` default only)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_affinity_calibration.py`

**Interfaces:**
- Consumes: `assign_movies`, `assign_users`, `make_slate`, `AFFINITY_STRENGTH` from the producer; `build_timelines`, `resolve_cutoff`, `split_timelines`, `most_popular`, `evaluate_system`, `train_next_item`, `model_rankings` from `next_item_model`.
- Produces: `slates_to_frame(...) -> pd.DataFrame` and `measure(strength, ...) -> dict`. Nothing later consumes them.

**Why this runs in-process.** The real acceptance path is a full sim through Kafka and Spark, which takes tens of minutes per strength value and cannot be iterated. But the joiner does not change *which items a user clicked* — it only reshapes events into rows. So for calibrating a click-probability constant, generating events in-process and assembling the `training_samples` columns directly is exact for the quantity being measured, and runs in seconds. The full-pipeline run is the verification at the end, not the calibration loop.

- [ ] **Step 1: Write the failing test**

Create `recsys-pipeline/integration-tests/python_modeling/test_affinity_calibration.py`:

```python
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import affinity_calibration as ac  # noqa: E402


def test_frame_has_the_columns_the_harness_reads():
    frame = ac.slates_to_frame(num_items=40, num_users=8, slates_per_user=5, seed=3)

    for column in ("user_id", "item_id", "impression_ts", "position", "clicked", "ordered"):
        assert column in frame.columns
    assert len(frame) > 0
    assert frame["clicked"].isin([0, 1]).all()


def test_impression_timestamps_advance_so_a_split_is_possible():
    """resolve_cutoff raises on a single-instant dataset; the frame must span time."""
    frame = ac.slates_to_frame(num_items=40, num_users=8, slates_per_user=5, seed=3)

    assert frame["impression_ts"].nunique() >= 2


def test_measure_reports_both_systems_and_the_support_it_used():
    result = ac.measure(strength=0.15, num_items=40, num_users=12, slates_per_user=6,
                        seed=3, epochs=5)

    assert set(result) >= {"strength", "test_users", "most_popular", "next_item_transformer"}
    assert result["strength"] == 0.15
    for system in ("most_popular", "next_item_transformer"):
        assert "hit_rate@10" in result[system]


def test_zero_strength_and_high_strength_are_distinguishable():
    """The calibration tool must be able to see the effect it exists to size.

    Not an assertion about which system wins — only that turning the knob moves the
    preferred-family share of clicks, which is the mechanism the harness reads.
    """
    flat = ac.preferred_share(strength=0.0, num_items=60, num_users=20, slates_per_user=8, seed=5)
    peaked = ac.preferred_share(strength=0.30, num_items=60, num_users=20, slates_per_user=8, seed=5)

    assert peaked > flat + 0.05
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_affinity_calibration.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'affinity_calibration'`.

- [ ] **Step 3: Write minimal implementation**

Create `affinity_calibration.py`:

```python
#!/usr/bin/env python3
"""Size AFFINITY_STRENGTH by measurement rather than argument.

Generates slates in-process, assembles the training_samples columns the next-item harness
reads, and reports what each strength buys. The full Kafka+Spark sim is the verification
run; it is far too slow to iterate a constant against, and the joiner does not change which
items a user clicked, so this shortcut is exact for the quantity being calibrated.

  python3 affinity_calibration.py --strengths 0.0 0.10 0.15 0.20 0.30
"""
from __future__ import annotations

import argparse
import random
from typing import Sequence

import pandas as pd

import movie_segment_producer as producer
import next_item_model as nim
from feature_derivations import l1


def slates_to_frame(num_items: int, num_users: int, slates_per_user: int,
                    seed: int) -> pd.DataFrame:
    """One row per impression, carrying the columns next_item_model reads."""
    rng = random.Random(seed)
    movies = producer.assign_movies(num_items, rng)
    users = producer.assign_users(num_users, rng)
    items = list(movies)

    rows = []
    stamp = 1_700_000_000
    for round_index in range(slates_per_user):
        for user in users:
            events = producer.make_slate(user, users[user], items, movies, rng)
            clicked = {e["item_id"] for e in events if e["event_type"] == "click"}
            ordered = {e["item_id"] for e in events if e["event_type"] == "order"}
            for event in events:
                if event["event_type"] != "impression":
                    continue
                rows.append({
                    "user_id": user,
                    "item_id": event["item_id"],
                    "impression_ts": stamp + round_index,
                    "position": event["position"],
                    "clicked": int(event["item_id"] in clicked),
                    "ordered": int(event["item_id"] in ordered),
                })
    return pd.DataFrame(rows)


def preferred_share(strength: float, num_items: int, num_users: int,
                    slates_per_user: int, seed: int) -> float:
    """Share of clicks landing in the clicking user's preferred family.

    The mechanism the harness ultimately reads. With six families, no affinity puts this
    near 1/6.
    """
    original = producer.AFFINITY_STRENGTH
    producer.AFFINITY_STRENGTH = strength
    try:
        rng = random.Random(seed)
        movies = producer.assign_movies(num_items, rng)
        users = producer.assign_users(num_users, rng)
        items = list(movies)
        hits = total = 0
        for _ in range(slates_per_user):
            for user in users:
                preferred = producer.user_preferred_family(user)
                for event in producer.make_slate(user, users[user], items, movies, rng):
                    if event["event_type"] != "click":
                        continue
                    total += 1
                    hits += l1(movies[event["item_id"]]["genres"]) == preferred
        return hits / total if total else 0.0
    finally:
        producer.AFFINITY_STRENGTH = original


def measure(strength: float, num_items: int, num_users: int, slates_per_user: int,
            seed: int, epochs: int) -> dict:
    """Score most_popular and the transformer on data generated at this strength."""
    original = producer.AFFINITY_STRENGTH
    producer.AFFINITY_STRENGTH = strength
    try:
        frame = slates_to_frame(num_items, num_users, slates_per_user, seed)
    finally:
        producer.AFFINITY_STRENGTH = original

    positives = frame[nim.positive_mask(frame)]
    timelines = nim.build_timelines(positives)
    split = nim.split_timelines(timelines, nim.resolve_cutoff(timelines))
    model, item_index = nim.train_next_item(split, epochs=epochs, seed=seed)

    return {
        "strength": strength,
        "test_users": len(split.targets),
        "catalog_size": len(item_index),
        "preferred_share": preferred_share(strength, num_items, num_users, slates_per_user, seed),
        "most_popular": nim.evaluate_system(nim.most_popular(split), split.targets),
        "next_item_transformer": nim.evaluate_system(
            nim.model_rankings(model, item_index, split), split.targets),
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Calibrate AFFINITY_STRENGTH by measurement.")
    parser.add_argument("--strengths", type=float, nargs="+",
                        default=[0.0, 0.10, 0.15, 0.20, 0.30])
    parser.add_argument("--num-items", type=int, default=400)
    parser.add_argument("--num-users", type=int, default=200)
    parser.add_argument("--slates-per-user", type=int, default=100)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--epochs", type=int, default=60)
    args = parser.parse_args(argv)

    print(f"{'strength':>9} {'pref_share':>11} {'pop hit@10':>11} {'model hit@10':>13} {'n':>5}")
    for strength in args.strengths:
        result = measure(strength, args.num_items, args.num_users,
                         args.slates_per_user, args.seed, args.epochs)
        print(f"{result['strength']:>9.2f} {result['preferred_share']:>11.3f} "
              f"{result['most_popular']['hit_rate@10']:>11.4f} "
              f"{result['next_item_transformer']['hit_rate@10']:>13.4f} "
              f"{result['test_users']:>5}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes, then calibrate**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_affinity_calibration.py -q`
Expected: PASS.

Then run the sweep at the real data shape:

```bash
cd recsys-pipeline/services/python-modeling
python3 affinity_calibration.py --strengths 0.0 0.10 0.15 0.20 0.30 0.45
```

Read the table against the criterion. Chance `hit_rate@10` is `10 / catalog_size`; one standard error is `sqrt(p(1-p)/n)` with that `p` and the reported `n`. **Choose the smallest strength whose `model hit@10` clears `most_popular` by more than two standard errors**, and set `AFFINITY_STRENGTH`'s default to it.

If no strength in the sweep clears it, widen the sweep once. If it still does not clear, **stop and report that** — per the spec, an implausibly large required strength is a finding, not a number to force. Say so in your report with the table; do not raise the strength past the point where a preferred-family item is near-certain to be clicked, because that is no longer a simulation of taste.

Record in your report: the full table, the chosen value, the `n` it was measured at, and the margin in standard errors.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/affinity_calibration.py \
        recsys-pipeline/services/python-modeling/movie_segment_producer.py \
        recsys-pipeline/integration-tests/python_modeling/test_affinity_calibration.py
git commit -m "feat: calibrate the affinity strength against the next-item harness"
```

---

## Verification

- [ ] `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q` passes from the repository root.
- [ ] `git grep -n "preferred_family\|affinity" recsys-pipeline/services/python-modeling/movie_segment_producer.py | grep -i "user_features\|context_features"` returns nothing — the affinity never enters an emitted map.
- [ ] `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py::test_assign_users_is_stable_and_uses_only_allowlisted_dimensions -q` passes — the governance allowlist is intact.
- [ ] `git diff master --stat` shows changes only under `services/python-modeling/` and `integration-tests/python_modeling/`.

## The full-pipeline acceptance run

The calibration above is in-process. The spec's acceptance criterion is a real sim, and it is
run once at the end by whoever holds the plan, not by a task implementer — it needs Kafka,
Redis, and Spark, and takes tens of minutes:

```bash
cd recsys-pipeline && docker compose up -d
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./scripts/run-movie-category-sim.sh
python3 services/python-modeling/next_item_model.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --output /tmp/spark-recsys/next-item/metrics.json \
  --vectors /tmp/spark-recsys/movie-category-sim/item-embedding.txt
```

Expected: `next_item_transformer` beats `most_popular` on `hit_rate@10` by more than two standard
errors at the reported `n`. The comparison before this change, for reference: every system sat
inside one standard error of chance, with `most_popular` at 0.0350 and the transformer at 0.0300
against a chance level of 0.0250 at n=200.

Also confirm the `by_l1` CSV under `/tmp/spark-recsys/movie-category-sim/report-categories` still
orders families the way `FAMILY_EFF` says it should. That ordering surviving is the whole reason
the bonus is zero-mean across users, and this is the only place it is checked end to end.
