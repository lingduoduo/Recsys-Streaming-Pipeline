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

**The second identity is exact only when the user count is a multiple of six.**
Preferred family is assigned round-robin over the six sorted families by user
index, so for a population of `N` users, a family's count of preferring users
`n_f` is equal across all six only when `N % 6 == 0`. Otherwise the mean bonus
for family `f` carries a residual of `S · (6·n_f − N) / (5N)`, where `S` is
`AFFINITY_STRENGTH`. At the sim's real default, `N = 200`, this is at most
`0.004·S` (two families get 34 preferring users, the rest 33), small next to the
smallest true `FAMILY_EFF` gap of 0.010, but not zero — it is a bound, not an
elimination. The construction is left unchanged (round-robin assignment is simple
and the residual is negligible at realistic `N`); the bound is documented here,
on `user_preferred_family`, and pinned by a test at `N = 200`.

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

### 5a. Known side effect: affinity inflates zero-truth fairness spreads (documented, not fixed)

Section 1's zero-mean identity is exact over the six **families**, not over the **catalog**.
A user does not see six families equally often — they see a uniform sample of a catalog whose
family shares are unbalanced by construction (`assign_movies` draws genres uniformly over 18
genres, not uniformly over the 6 l1 families). So a user's expected bonus over a catalog draw,
`s_f·S − (1 − s_f)·S/5` where `s_f` is that family's catalog share, is zero only at `s_f = 1/6`.
The aggregate identity across families still cancels exactly (why section 1 and the `by_l1`
report are unaffected), but every individual user now carries a hidden CTR offset keyed to
`user_index % 6`, and that offset inflates the apparent spread on fairness dimensions —
gender, age_band, country — that have no injected ground-truth effect.

Measured at the shipped default (`AFFINITY_STRENGTH = 0.80`, the sim's real `SEED = 17`,
`NUM_USERS = 200`, `NUM_ITEMS = 400`), exact population expectation (mean `click_prob` over
every item × surface per user, so no sampling noise):

```
dimension      spread S=0   spread S=0.80   one se at default run scale
gender           0.0017        0.0072          ~0.0022  (~3 se)
age_band         0.0049        0.0114          ~0.0026  (~4 se)
country          0.0070        0.0036          ~0.0026  (this seed happens to shrink)
```

`gender` and `age_band` widen 2–4x into a range visible against sampling noise on a published
dashboard, at a run with no injected effect on either dimension. `country` moved the other way
at this seed — a reminder that the sign of the shift depends on which fairness groups happen to
correlate with `user_index % 6` for a given population draw, not a directional guarantee.

The affinity also **compresses** the one gap the sim exists to teach: `SUBSCRIPTION_EFF`'s
premium-vs-free spread is 0.0500 by construction, but at `S = 0.80` the measured spread is
0.0380 — a ~24% reduction. Mechanism: the top clamp (`click_prob` capped at 0.60) saturates for
the ~1-in-6 preferred-family impressions regardless of subscription tier, so a growing share of
impressions carry no room left for the subscription term to move the number. This is the same
clamp-saturation mechanism documented on `AFFINITY_STRENGTH` (section 3 / the code comment); it
is not a new mechanism, but it means the subscription gap is smaller at the shipped default than
section 1's "sits strictly underneath" language implies for a per-user, per-dimension read.

**This is not corrected in code.** Balancing the catalog's family shares or scaling the bonus by
observed share so the per-user catalog expectation is exactly zero are both real fixes but are a
separate design question from this spec's goal (make personalization learnable) — see Non-goals.
Anyone reading the fairness dashboard off a regenerated dataset should expect `gender` and
`age_band` spreads a few standard errors wider than their true (zero) effect, and the
`subscription` gap a few percent narrower than 0.05, both explained by this section rather than
by an unrelated data-quality regression.

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
