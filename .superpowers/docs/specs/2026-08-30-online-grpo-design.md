# Online GRPO — design

Group Relative Policy Optimization, trained continuously in Scala off the live slate stream,
serving through Redis behind a shadow flag.

## Why this document exists

The repository has no GRPO implementation. GRPO appears twice, both times as prose in a design
document, both times inside the phrase "classic post-training runs SFT → reward model → PPO/GRPO"
— named only as the foil that DPO is contrasted against. Nothing implements it, and nothing
implements PPO, policy gradients, advantage estimation, or a KL penalty either.

What exists is three *offline* post-training arms in `services/python-modeling/post-training/`:
tabular Q, fitted Q iteration, and DPO. All are batch Python CLIs over Parquet or a Redis replay
dump. None of them runs continuously, and none of them affects serving.

This design adds a fourth arm that is different in kind: it trains online, in Scala, inside the
existing Spark Structured Streaming fleet, and its output reaches the serving path.

## The insight this design rests on

GRPO's defining move is to replace the value network with a **group-relative baseline**: sample a
group of responses for one prompt, normalize their rewards within the group, and use that as the
advantage. It needs a group of comparable actions sharing one context.

A recommendation slate is exactly that group, and this repository already builds it, in Scala, in a
streaming job. `ExperienceCollectorStreamingJob.buildSlates` groups training samples by
`(request_id, user_id)` into a slate — items sorted by position, `slate_reward = sum(label)`,
per-item `label` / `clicked` / `ordered` / `rating` / `dwell_millis` / `completion_rate` — and
publishes to the `training_experiences` Kafka topic every trigger interval.

One prompt (user + context), N sampled actions, a reward each. The group structure GRPO needs is
not something this design invents; it is something already flowing through Kafka every ten seconds.

A second property makes GRPO *easier* here than in its native domain. The action space per group
is the slate: finite, small, and fully enumerated in the logged event. The softmax partition
function is therefore computable, and the KL term is **exact** rather than the k3 estimator LLM
implementations are forced into. This is the same structural reason `dpo.py` gives for why DPO's
partition function cancels in this setting.

## What is missing, and the prerequisite it forces

The group and the reward are online. The behavior policy's scores and the features are not.

GRPO's objective needs the importance ratio `r = π_θ(a|s) / π_old(a|s)`; the ratio is the thing
clipping clips. The serving path does compute per-candidate `predictionScore`, `banditScore`, and
`modelPredictions` — but `MovieLensServingSideEffects` writes them **only into the Redis replay
buffer**, never onto the Kafka event that becomes a slate. The `item_features` map that does reach
the slate is synthetic filler from the Python producers (`{"bucket": "b0".."b3"}`).

The cause is structural, and larger than a missing field. **The Java retrieval service does not
publish to Kafka in the serving path at all.** It is a consumer there. Every impression event on
`behavior_logs` originates from the four Python producers in `services/python-modeling/` — a
synthetic simulation. The real serving path touches only Redis. There are two disjoint worlds.

That disjointness is the root cause of a defect already documented elsewhere in the repository:
`post_train_dpo.py` carries an "UNMET PREREQUISITE" notice saying the slate log and the replay
buffer mint request ids independently, so its join matches nothing and the CLI exits with 0 pairs.
It is not two id generators that drifted. It is a synthetic Kafka world and a real serving world
that never touch.

So online GRPO has a hard prerequisite: **the serving path must emit its own impressions to
Kafka.** That work is not GRPO-specific. It is the thing standing between this repository and any
of its post-training arms running on real data, and it unblocks offline DPO as a side effect.

The plumbing is not missing. `kafka/core/KafkaProducer.java` exists and works, used today by
`MovieLensEventProcessor`. The serving path is simply not wired to it.

## Decisions taken

| Decision | Choice | Why |
|---|---|---|
| π_old source | Serving emits scores onto the event stream | True GRPO — a real clipped ratio and a real reference. Also fixes the requestId disjointness. |
| Policy class | Linear softmax over a feature vector | Analytically differentiable, ~10 doubles through Redis, a dot product at serving latency. Honestly cheap on both ends. |
| Job siting | New standalone Spark streaming job | Matches every other job here; failure isolated from the experience collector, which feeds the whole training-sample path. |
| Rollout | off / shadow / on, default off | Mirrors the sequence rollout operators already know. |
| Sequencing | One design, three pieces, plan sequences them | GRPO cannot be verified end to end until serving emits events. |

Rejected: putting the training loop inside `ExperienceCollectorStreamingJob`'s `foreachBatch` (a
GRPO bug would take down the producer feeding the entire training-sample path); a plain Kafka
consumer with no Spark (less machinery for a small model, but off-pattern here, and it would need
its own deployment, offset commits, and failure handling that Spark already provides); a 2-layer
MLP matching the offline arms' function class (fairer head-to-head, but it costs a hand-rolled
matmul in Java and a weight-blob transport); a tabular policy over the existing state key (cheapest,
but it inherits the v1→v2 state-schema fragility and generalizes poorly to unseen users).

## Architecture

```
Java serving (HybridRecommendationService → MovieLensServingSideEffects)
   │  NEW: per-slate impression event, serving's own requestId,
   │       per-candidate prediction_score (= π_old) and grpo_x (= features)
   ▼
Kafka: behavior_logs ──► OnlineJoinerStreamingJob ──► LateFeedbackJoin
                                                          │ feedback window closes
                                                          ▼
                                          ExperienceCollectorStreamingJob
                                                          │ slates, rewards attached
                                                          ▼
                                            Kafka: training_experiences
                                                          │
                                                          ▼
                                        NEW: GrpoPolicyStreamingJob (Scala)
                                                          │ w ← w − lr·∇L
                                                          ▼
                                          Redis: grpo:policy:weights
                                                          │
                                                          ▼
                                     NEW: GrpoPolicyScorer (Java), shadow
```

The loop closes through Redis, which is how every other online signal in this repository already
travels — `reward-model:*`, the sequence store, embeddings. No new transport is introduced.

Feedback lateness needs no new handling. `LateFeedbackJoin` already holds a slate until its
feedback window closes, so rewards are attached before the slate is ever published.

## Component 1 — Serving emits its own impressions (Java)

`MovieLensServingSideEffects` gains a Kafka publish alongside its existing Redis replay write,
through the existing `KafkaProducer`, to the topic `ONLINE_JOINER_INPUT_TOPIC` names (default
`recsys_events`) — the same variable `OnlineJoinerStreamingJob` subscribes with, so the two sides
align by configuration rather than by coincidence.

The event matches the Python producer's shape exactly, field for field, because
`OnlineJoinerStreamingJob.parseEvents` enforces non-null `request_id` / `user_id` / `item_id` and
every downstream schema is built on that shape:

```
event_id, request_id, session_id, user_id, item_id,
event_type = "impression", timestamp_ms, position,
user_features, item_features, context_features
```

Two additions inside `item_features`:

| Key | Source | Role |
|---|---|---|
| `prediction_score` | `ServedMovie.modelPredictions().predictionScore` | π_old logit |
| `grpo_x` | packed comma-separated doubles, prefixed with a feature version | policy features x |

`grpo_x` carries ten dimensions, all of which serving already computes per candidate during
scoring: `banditScore`, `weightedOutcome`, `qValue`, `posteriorMean`, `explorationBonus`,
`coldStart` as 0/1, `log1p(impressions)`, `log1p(clicks)`, `position / slateSize`, and a bias term.
No new model call is added to the request path — the cost is serialization only.

The vector is prefixed with a feature version (`v1:`). A consumer that reads a version it does not
know drops the row and counts it, rather than silently misaligning weights against features.

**Gate.** `recsys.grpo.emit-events` / `RECSYS_GRPO_EMIT_EVENTS`, default `false`.

**Failure policy.** Fire-and-forget with the existing warn-on-failure callback. A Kafka outage must
never fail a recommendation request. This is a logging side effect, not a serving dependency.

**Consequence beyond GRPO.** The `requestId` on the Kafka stream becomes serving's own. That is
precisely the prerequisite `post_train_dpo.py` documents as unmet, so offline DPO becomes runnable
for the first time. Its `slate_pairs` join stops returning zero.

## Component 2 — GrpoPolicyStreamingJob (Scala)

A standalone Structured Streaming job subscribing to `training_experiences`, with its own
checkpoint, `main`, and restart lifecycle, matching every other job in `spark-streaming-job`.

### The objective

For each slate (group) of N candidates with logged scores `s_i`, features `x_i`, rewards `R_i`:

```
π_old(i) = softmax_i(s_i / τ)                    logged serving policy
π_θ(i)   = softmax_i(w · x_i / τ)                current policy
Â_i      = (R_i − mean(R)) / (std(R) + ε)        group-relative advantage, no value network
r_i      = π_θ(i) / π_snap(i)                    importance ratio
L        = −(1/N) Σ min(r_i·Â_i, clip(r_i, 1±ε_clip)·Â_i) + β · KL(π_θ ‖ π_old)
```

`R_i = label_i`, the same per-item signal the other three arms train on, so a later head-to-head
comparison is not confounded by a different reward definition.

### Why the ratio and the KL anchor to different policies

They are deliberately not the same reference, and conflating them is a silent failure.

In shadow mode the serving policy never changes. If `r` were measured against `π_old`, then as
training proceeds `w` would drift steadily away from the frozen logged policy, `r` would grow
without bound, clipping would latch permanently active, and the surrogate gradient would go to
zero. The job would report healthy batches, a rising slate count, and no errors, while having
quietly stopped learning.

So:

- `π_snap` is `w` captured at the start of each micro-batch. Within-batch inner epochs then see a
  ratio that departs from 1 for the ordinary PPO reason, and clipping does the job it exists to do.
- The KL term anchors to the logged `π_old`, keeping the policy from drifting arbitrarily far from
  what is actually serving.

This mirrors what `dpo.py` already does with its reference-anchored margin, so the two arms stay
conceptually aligned.

### The update

Linear-plus-softmax has an analytic gradient:

```
∂ log π_i / ∂w = x_i − Σ_j π_j x_j
```

so **no ML library is required** — no Breeze, no DL4J, no autodiff. The whole update is a few dozen
lines over `Array[Double]`, which also makes it directly unit-testable without a Spark session.

Per micro-batch:

1. Read `w` (driver state; on startup, from Redis).
2. Snapshot `π_snap` parameters.
3. Filter degenerate groups.
4. Compute per-slate gradient contributions; sum them with `treeAggregate` so the driver never
   collects per-slate vectors.
5. Apply `w ← w − lr · g` for the configured number of inner epochs.
6. Write `w` to Redis.

The micro-batch is the minibatch. This is online SGD by construction, not a batch job on a timer.

### Hyperparameters

Read once at job start through the same `env => config` pattern as `SequenceJobConfig`, where only
an explicitly parseable value counts and anything else falls back to the default — a typo must not
silently change the objective.

| Symbol | Env var | Default | Meaning |
|---|---|---|---|
| `τ` | `GRPO_TEMPERATURE` | `1.0` | Softmax temperature, shared by π_θ and π_old so the ratio is scale-consistent. |
| `ε_clip` | `GRPO_CLIP_EPSILON` | `0.2` | PPO clip range. |
| `β` | `GRPO_KL_BETA` | `0.02` | Weight on the exact KL to the logged serving policy. |
| `lr` | `GRPO_LEARNING_RATE` | `0.01` | SGD step size, matching the offline arms' default. |
| — | `GRPO_INNER_EPOCHS` | `4` | Gradient steps per micro-batch against the same slates. |
| `ε` | — | `1e-8` | Advantage denominator floor. Not configurable; it guards division, and a slate needing a larger floor is a degenerate slate that must be dropped instead. |

`GRPO_INNER_EPOCHS` must be greater than 1 or clipping is dead code: with a single step per batch,
`π_θ` still equals `π_snap` when the ratio is formed, so `r = 1` everywhere and the `min` never
selects the clipped branch. The default of 4 is what makes the surrogate objective meaningful
rather than decorative.

### Degenerate groups are dropped, not fudged

A slate with `slate_size < 2`, or one where every item carries the same label — the common
no-click case — has zero reward variance and an undefined advantage. Dividing by `std + ε` there
does not produce a small gradient; it produces amplified noise scaled by `1/ε`.

Such slates are filtered before the update and counted through `DropMetrics`, matching how every
other job in this fleet reports drops. The counts are part of the job's normal log line, because
"almost every slate is degenerate" is the expected steady state at low click-through and an
operator needs to see the surviving fraction to know whether the job is learning from anything.

Rows whose `grpo_x` feature version is unrecognized are dropped and counted separately.

### State and restart

`w` lives in Redis at `grpo:policy:weights`, a hash with no TTL:

| Field | Meaning |
|---|---|
| `weights` | comma-separated doubles |
| `dim` | length, validated against the feature version |
| `feature_version` | the `grpo_x` version these weights were fit against |
| `updated_at` | epoch millis |
| `batch_id` | last applied micro-batch |
| `slates_applied` | cumulative non-degenerate slates |

On startup the job reads `w` from Redis rather than rebuilding it. A restart resumes training; it
does not reset the policy. If the stored `feature_version` does not match the job's, the job
refuses to start rather than applying stale weights to a changed feature layout.

Note that nothing time-based fires while the topic is quiet — `foreachBatch` is driven by arriving
data. This is correct for GRPO, whose updates are event-driven by nature, but it means "no weight
update for an hour" means "no traffic", not "the job is stuck".

## Component 3 — Shadow scorer (Java)

`GrpoPolicyScorer` reads the weight vector through `FeatureCache` on the same TTL pattern as
`reward-model:*`, computes `w · x` per candidate, and publishes it as `grpoScore` inside
`modelPredictions`.

**Mode.** `recsys.grpo.mode` / `RECSYS_GRPO_MODE`, one of `off | shadow | on`, default `off`,
mirroring `recsys.sequence.behavior-mode` so operators meet a shape they already know. An
unrecognized value logs a warning and is treated as `off`, matching `BehaviorSequencesQueryHydrator`.

| Mode | Behavior |
|---|---|
| `off` | Not computed. No Redis read, no cost. |
| `shadow` | Computed, logged with rank agreement against `predictionScore`, weighted **0.0** in the exploitation blend. |
| `on` | Claims part of the 0.15 of blend weight left unclaimed by the dead deep-learning term. |

`MovieLensOutcomeScorer` documents that its exploitation weights sum to 0.85, not 1.0, because a
fourth term was always 0.0 at runtime, and that the remainder is deliberately not renormalized.
`grpoScore` taking part of that unclaimed 0.15 means **no existing weight changes and no existing
score moves** when GRPO is enabled — the flip is additive, and reverting it is exact.

**Success criterion for the flip.** Shadow scores must rank held-out slates better than the
reference they were anchored to, measured on post-flip data only, before `on` is considered.

## Testing

Test-driven throughout.

The GRPO core is pure functions over arrays, so it is unit-testable with no Spark session:

- advantage normalization, including the zero-variance rejection
- exact KL over a finite slate softmax
- clipping activation at the boundary, and that it is inert at `r = 1`
- the analytic gradient checked against a finite-difference approximation
- degenerate-group filtering, per reason, with counts

Spark-level tests cover the micro-batch path: weight persistence across a simulated restart, and
refusal to start on a feature-version mismatch.

The Java emitter gets a contract test asserting its emitted JSON parses cleanly under
`ExperienceCollectorStreamingJob.TrainingSampleSchema`. That test is the guard against the two
worlds silently diverging again.

Spark-session tests require **JDK 17**. The default JDK 25 aborts them with a misleading
`getSubject` error that looks like an auth problem and is not.

## Evaluation trap to avoid by construction

If `grpoScore` is written into the replay for off-policy evaluation, it **must** be registered in
`ope_eval_report.POLICY_ONLY_PRED_KEYS`.

That tuple is what `feature_names()` excludes when fitting the reward model that grades policies.
A `model:*` score omitted from it becomes an input feature to the very reward model that then
grades it — the policy grades itself, and scores high for a reason that has nothing to do with
recommendation quality. This is a known live problem for other policies in this repository, and it
would be inherited here for free by simply forgetting a one-line registration.

## Scope boundaries

In scope: serving-side event emission, the Scala training job, the shadow scorer, their tests, and
the configuration and documentation for all three.

Out of scope: changing the reward definition away from `label`; retiring the Python producers;
migrating the other three post-training arms; any change to the exploitation blend's existing
weights; the `on`-mode flip itself, which is an operational decision made after shadow data exists.

## Risks

| Risk | Mitigation |
|---|---|
| Shadow mode freezes π_old and clipping latches on | Ratio anchors to a per-batch snapshot, not the logged policy. Stated above as the primary correctness argument. |
| Nearly all slates degenerate at low CTR | Surviving fraction is logged every batch, so it is visible rather than inferred. |
| Feature layout drifts from weight layout | Feature version on the wire, validated at job start and per row. |
| Serving latency regression | No new model call; the added work is serialization behind a default-off flag. |
| Kafka outage affects serving | Fire-and-forget send with warn-on-failure; never on the request's critical path. |
| grpoScore grades itself in OPE | Registered in `POLICY_ONLY_PRED_KEYS`, called out above as its own section. |
