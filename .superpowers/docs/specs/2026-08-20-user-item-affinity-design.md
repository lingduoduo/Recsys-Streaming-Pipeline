# Joint User-Item Affinity in the Movie-Category Sim — Design

**Date:** 2026-08-20
**Status:** approved
**Depends on:** [2026-08-20-next-item-prediction-design.md](2026-08-20-next-item-prediction-design.md)

## Problem

Nothing this pipeline generates contains a per-user item preference, so no model
trained on simulated data can learn personalization. This is not a subtle gap; it
is provable from the producers.

All four behavior producers select slate items with a uniform
`rng.sample(items, SLATE_SIZE)`, and none contains a single reference to history,
previous items, or sequence. More decisively, every click-probability function is
separable:

| Producer | Click probability depends on |
|---|---|
| `movie_segment_producer` | `item_click_prob(meta) + user_click_bias(user_meta) + SURFACE_EFF[surface]` |
| `movielens_segment_producer` | `click_prob(demo, platform, surface)` — no item term at all |
| `backfill_producer` | `click_prob(dt, ...)` — time only |
| `producer` | a fixed 0.35 slate-level probability |

So `p(click | user, item) = f(item) + g(user) + h(context)`, with no interaction
term anywhere. A model can learn "sci-fi gets clicked more" and "premium users
click more". It can never learn "*this* user likes sci-fi", because that fact does
not exist in the data.

The offline next-item experiment measured the consequence: on the flagship
movie-category dataset, all four systems — popularity, repeat-last, item2vec
neighbours, and a causal transformer — scored inside one standard error of chance
(2 to 7 hits out of 200 users against an expected 5). That experiment's harness was
separately validated by injecting a deterministic next-item chain, where the
transformer reached `hit_rate@10 = 0.99`. The harness works; the data has no signal
to find.

The same bound applies to everything else trained here — the two-tower model, the
CTR ranker, and the bandits can all fit marginal effects and none can fit
personalization.

## Goal

The movie-category sim generates a per-user item preference strong enough that the
existing next-item harness detects it, without disturbing any ground truth the sim
already injects.

## Non-goals

| Deferred | Why separate |
|---|---|
| Affinity in the other three sims | Only `movie_segment_producer` has item genres; the others emit bare `movie_N` ids and would need item metadata first |
| Affinity biasing slate composition | Impressions would stop being a uniform sample of the catalog, changing what every existing report measures and invalidating the OPE harness's assumptions |
| Item-to-item transition structure | A different generative mechanism from a stable per-user taste; worth its own spec if sequence-order effects are wanted |
| Re-tuning any model to exploit the new signal | This spec changes data, not models. Whether a model improves is measured, not engineered |
| Emitting affinity as a feature | See section 2 — it is deliberately latent |

## Approach

Two alternatives were weighed against the chosen one.

**A per-user preferred family with a zero-mean bonus** (chosen). Each user prefers
one of the six l1 families; items in it gain click probability and the other five
lose a fifth as much each. Simple, matches the additive-effects idiom the file
already uses, and provably leaves the existing marginal ground truth intact.

**A full per-user affinity vector over all 18 genres** (rejected). More expressive,
but with a 400-item catalog and roughly 107 positives per user, 18 buckets are too
sparse to learn from, and it would not be recoverable in the `by_l1` report the sim
exists to produce.

**Multiplicative affinity** (rejected). More realistic than an additive term, but
it interacts with the existing `[0.02, 0.6]` clamp in a way that distorts the
marginal per-family rates, which is exactly what this design must not do.

## 1. The affinity

Keyed on the six l1 families, matching `FAMILY_EFF`'s existing idiom and the
`by_l1` grouping the report already produces. Six buckets are learnable from a
median 107 positives per user; the 18 raw genres are not.

Each user draws one preferred family uniformly. Their click probability gains:

| Item's family | Bonus |
|---|---|
| The user's preferred family | `+AFFINITY_STRENGTH` |
| Each of the other five | `−AFFINITY_STRENGTH / 5` |

**This is zero-mean twice over, and both matter.** Per user, the bonuses sum to
zero across families. Across users, because family preference is drawn uniformly,
the expected bonus for any given item family is
`(1/6)(+S) + (5/6)(−S/5) = S/6 − S/6 = 0`.

The second identity is the one that protects the existing ground truth: the
marginal per-family click rate is unchanged in expectation, so `by_l1` still
recovers `FAMILY_EFF`, `by_l2` and `by_l3` still recover what they recover today,
and the personalization signal sits strictly underneath them rather than replacing
them.

The term is added alongside the existing ones, inside the same clamp:

```
click_prob = clamp(item_click_prob(meta) + user_click_bias(user_meta)
                   + SURFACE_EFF[surface] + affinity_bonus(user, meta))
```

## 2. The affinity is latent

It is never written to `user_features`, to `context_features`, or to any typed
event field. Two independent reasons:

- `user_features` feeds `governance_measurements.DEFAULT_DIMENSIONS`, an allowlist
  whose members are published as fairness groups. A taste attribute does not belong
  in that surface.
- A model that could read the affinity off a field would demonstrate feature
  copying, not personalization. The signal has to be inferable from behaviour, or
  the experiment proves nothing.

This mirrors how the sim already treats `FAMILY_EFF`, `DECADE_EFF`, and
`SUBSCRIPTION_EFF`: documented ground truth that lives in the generator and is
recovered from the data, never emitted alongside it.

A test asserts no emitted event field carries the affinity.

## 3. Strength is calibrated, not guessed

`AFFINITY_STRENGTH` is an env-tunable module constant, following the file's
convention that every injected effect is a documented constant.

**Its default is determined by measurement during implementation, not chosen in
this spec.** Worked arithmetic for a plausible value — `S = 0.15` against
`BASE_CTR = 0.15` — puts roughly a third of a user's clicks in their preferred
family and predicts `hit_rate@10` near 0.069 against a chance level of 0.025. At
n=200 that is about 1.9 standard errors above `most_popular`, which is marginal.
Writing that number into the spec and hoping is how a sim ends up with an effect
nobody can measure.

So the implementation runs the existing next-item harness against a regenerated
dataset, adjusts the constant, and records the measured result. If the strength
required to clear the noise band turns out to be implausibly large, **that is a
finding to report, not a number to force** — it would mean a 400-item catalog with
this slate size cannot express learnable taste, which is worth knowing.

## 4. Acceptance criterion

A regenerated movie-category dataset, scored by the unmodified next-item harness,
shows `next_item_transformer` beating `most_popular` on `hit_rate@10` by a margin
outside the noise band, and the result names the `n` it was measured at.

The harness reports `test_users`, so the band is computable from its own output:
one standard error at chance is `sqrt(p(1-p)/n)` with `p = k / catalog_size`. The
default sim has 200 users; `NUM_USERS` is already env-tunable, so a tighter band is
a knob on the verification run rather than a code change. The criterion must state
the `n` used.

Nothing about the harness changes. It takes `--input`, so this is a re-run.

## 5. What must not change

| Invariant | How it is protected |
|---|---|
| `by_l1` / `by_l2` / `by_l3` CTR orderings | The across-user zero-mean identity in section 1 |
| Fairness dimensions and their groups | Affinity never enters `user_features` |
| Every existing vocabulary — genres, surfaces, locales, countries, subscriptions | Untouched |
| Slates as a uniform sample of the catalog | Slate composition is explicitly out of scope |
| The v2 event contract | No new field; the affinity is latent |

## Error handling

| Condition | Behavior |
|---|---|
| `AFFINITY_STRENGTH = 0` | The sim behaves exactly as it does today; a test pins this, so the change is provably opt-out |
| An item whose genres yield an unrecognized l1 family | `l1` already returns `"Other"` for unmatched genres, which is one of the six families and needs no special case |
| A strength large enough to drive click probability outside `[0.02, 0.6]` | The existing clamp applies, as it already does for the other additive effects; the clamp is the reason strength must be calibrated by measurement rather than reasoning |

## Testing

- **Zero-mean per user** — the six bonuses for any one user sum to zero.
- **Zero-mean across users** — over the assigned population, the mean bonus for
  each item family is zero within tolerance. This is the invariant protecting the
  existing report, so it is tested directly rather than inferred.
- **Preference is real** — for one user, a preferred-family item has a strictly
  higher click probability than an otherwise identical non-preferred one.
- **Affinity is latent** — no emitted event field, in any event type, carries the
  affinity or the preferred family.
- **Opt-out** — with `AFFINITY_STRENGTH = 0`, generated slates are identical to
  today's for the same seed.
- **Stability** — a user's preferred family is the same across every slate they
  appear in, like their country and locale.
- **Acceptance** — the recorded next-item run, with its `n`, showing the margin.
