# Cold-Start Candidates

After popular candidates are fetched, the retrieval service adds extra candidates from the configured catalog for users or items with no exposure history, so new users and unseen items can still surface.

### Configuration

**Candidate generation property**

| Property | Default |
|---|---|
| `recsys.candidate-generation.cold-start-pool-size` | `25` |

**Runtime override**

| Env var | Default |
|---|---|
| `RECSYS_COLD_START_POOL_SIZE` | `25` |

Cold-start items additionally receive a bandit exploration boost at scoring time
(`recsys.bandit.cold-start-boost`, `recsys.bandit.cold-start-exposure-threshold`); those knobs
live with the rest of the Bandit configuration in the main README.

## Cold-Start RL Extension Plan

The retrieval service is a natural starting point for a cold-start RL project: it already has content-based retrieval, item embeddings, online feedback, and a bandit exploration layer. The recommended path extends that foundation gradually rather than jumping straight to a full DQN ranker.

1. ✅ **Baseline** *(implemented)*
   - Content-based retrieval from genres, tags, popularity, and embeddings
   - Offline ONNX MLP score (`DeepLearningPredictionService`) blended via `deep-learning-weight`
   - Online reward model (`OnlineLearningService`) updated from the feedback stream
   - UCB/Thompson bandit exploration for low-exposure and cold-start items
   - Replay buffer storing `(user, context, candidates, action, reward, timestamp)`

2. **Establish the RL framing**
   - Model recommendation as contextual Q-learning: state = user/session context, action = recommended item, reward = `/feedback` signal
   - Keep the UCB ranker as the online-safe policy while the Q-value model trains offline or in shadow mode

3. **Scale with DQN ranking**
   - Replace tabular Q-values with a neural scorer over user, item, and content embeddings
   - Rank candidates by predicted Q-value rather than treating the full catalog as the action space
   - Train from the replay buffer using exposed-but-unclicked items as negatives

4. **Stabilize with Double DQN**
   - Use the online network to select the next best item; use the target network to evaluate it
   - Periodically sync the target network to reduce overestimation and ranking churn

5. **Improve cold-start efficiency with Dyna-Q planning**
   - Train a lightweight reward model from observed feedback
   - Generate simulated feedback for new or low-exposure items
   - Mix real and simulated replay at a lower weight for simulated samples

Each step adds one capability: Q-learning frames the loop as RL, DQN makes it scalable with embeddings, Double DQN stabilizes the learned ranker, and Dyna-Q improves cold-start efficiency through simulated experience.
