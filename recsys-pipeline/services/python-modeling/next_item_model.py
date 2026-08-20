#!/usr/bin/env python3
"""Offline next-item prediction: can this pipeline predict what a user engages with next?

Builds per-user timelines from the training_samples Parquet store, splits them at a
timestamp cutoff, and scores a small sequential transformer against three trivial
baselines through one shared metric implementation.

Offline only: nothing here touches Spark, Redis, the serving path, or ONNX.
"""
from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass

import pandas as pd

from recall_eval_report import cosine, rank_topk

SEED = 42

#: Columns whose presence makes a row a positive engagement.
POSITIVE_MASK_COLUMNS = ("clicked", "ordered", "thumb")


def positive_mask(df: pd.DataFrame) -> pd.Series:
    """Rows the user engaged with: a click, an order, or a thumb up.

    A thumb down (-1) states dislike and an abandon states early exit. Neither is a
    positive, and neither is used as a negative — they are simply not prediction targets.
    """
    mask = pd.Series(False, index=df.index)
    if "clicked" in df.columns:
        mask |= df["clicked"].fillna(0).astype(int) == 1
    if "ordered" in df.columns:
        mask |= df["ordered"].fillna(0).astype(int) == 1
    if "thumb" in df.columns:
        mask |= df["thumb"].fillna(0).astype(int) == 1
    return mask


def load_positives(input_dir: str) -> pd.DataFrame:
    """Read the training-samples Parquet directory and keep only positive engagements."""
    df = pd.read_parquet(input_dir)
    positives = df[positive_mask(df)] if len(df) else df
    if positives.empty:
        raise ValueError(
            f"No positive engagements found at {input_dir}. "
            "Run a simulation first, e.g. scripts/run-movie-category-sim.sh"
        )
    return positives


def build_timelines(positives: pd.DataFrame) -> dict[str, list[tuple[int, str]]]:
    """Per-user (impression_ts, item_id) pairs in engagement order.

    Sorted by (impression_ts, position, item_id). The tie-break is load-bearing: every
    impression in one slate carries the same impression_ts, so sorting on the timestamp
    alone leaves a slate's positives in whatever order the Parquet happened to hold them
    and makes "the next item" differ between runs on identical input.
    """
    ordered = positives.sort_values(
        ["user_id", "impression_ts", "position", "item_id"], kind="mergesort")
    timelines: dict[str, list[tuple[int, str]]] = {}
    for user, item, ts in zip(ordered["user_id"], ordered["item_id"], ordered["impression_ts"]):
        timelines.setdefault(str(user), []).append((int(ts), str(item)))
    return timelines


def repeat_rate(timelines: dict[str, list[tuple[int, str]]]) -> float:
    """Share of events whose item that user already engaged with earlier.

    Reported so a reader can judge how much of any system's score is explained by
    re-engagement rather than discovery.
    """
    total = repeats = 0
    for events in timelines.values():
        seen: set[str] = set()
        for _, item in events:
            total += 1
            if item in seen:
                repeats += 1
            seen.add(item)
    return repeats / total if total else 0.0


DEFAULT_HOLDOUT_QUANTILE = 0.9


@dataclass(frozen=True)
class Split:
    """One global time cutoff applied to every user.

    `train` holds each user's pre-cutoff timeline, `targets` the single item they engaged
    with first at or after the cutoff. Every system — baselines and model — sees exactly
    this, so their numbers are comparable.
    """

    train: dict[str, list[tuple[int, str]]]
    targets: dict[str, str]
    cutoff_ts: int
    dropped: dict[str, int]


def resolve_cutoff(
    timelines: dict[str, list[tuple[int, str]]],
    quantile: float = DEFAULT_HOLDOUT_QUANTILE,
    absolute: int | None = None,
) -> int:
    """The timestamp that separates training from held-out events.

    A quantile of event timestamps, not a date. The movie-category sim writes its entire
    run inside one UTC date — measured at 4 minutes 50 seconds across 99,415 rows — so a
    date-partition holdout there selects everything or nothing. The engagement backfill
    sim spans 21 days. A quantile works on both.
    """
    if absolute is not None:
        return int(absolute)
    stamps = [ts for events in timelines.values() for ts, _ in events]
    if len(set(stamps)) < 2:
        raise ValueError(
            "Need at least 2 distinct event timestamps to form a time split; "
            f"found {len(set(stamps))}. A single-instant dataset cannot be split in time."
        )
    return int(pd.Series(stamps).quantile(quantile))


def split_timelines(timelines: dict[str, list[tuple[int, str]]], cutoff_ts: int) -> Split:
    """Apply the cutoff, dropping users who cannot contribute a (history, next) pair."""
    train: dict[str, list[tuple[int, str]]] = {}
    targets: dict[str, str] = {}
    dropped = {"fewer_than_two_positives": 0, "no_pre_cutoff_history": 0, "no_target": 0}

    for user, events in timelines.items():
        if len(events) < 2:
            dropped["fewer_than_two_positives"] += 1
            continue
        history = [event for event in events if event[0] < cutoff_ts]
        after = [item for ts, item in events if ts >= cutoff_ts]
        if not history:
            dropped["no_pre_cutoff_history"] += 1
            continue
        if not after:
            dropped["no_target"] += 1
            continue
        train[user] = history
        targets[user] = after[0]

    if not targets:
        raise ValueError(
            f"Split at cutoff_ts={cutoff_ts} left no test users. "
            f"Dropped: {dropped}. Lower the holdout quantile or use a longer-running dataset."
        )
    return Split(train=train, targets=targets, cutoff_ts=int(cutoff_ts), dropped=dropped)


KS = (5, 10, 20)
MRR_K = 10
NDCG_K = 10


def evaluate_system(
    rankings: dict[str, list[str]], targets: dict[str, str]
) -> dict[str, float]:
    """Score one system's rankings against the held-out targets.

    Named hit_rate rather than recall on purpose. Each user contributes exactly one
    target, so the fraction of relevant items retrieved is 0 or 1 and its mean is a hit
    rate — and `recall@k` already names a different, set-based quantity in
    recall_eval_report.py. With a single relevant item the ideal DCG is 1, so ndcg@10 is
    the reciprocal log rank.

    The denominator is every user in `targets`, so a system that cannot rank a user
    scores a miss for them rather than quietly excluding them from its own average.
    """
    hits = {k: 0 for k in KS}
    mrr_total = 0.0
    ndcg_total = 0.0

    for user, target in targets.items():
        ranking = rankings.get(user, [])
        try:
            rank = ranking.index(target) + 1
        except ValueError:
            continue
        for k in KS:
            if rank <= k:
                hits[k] += 1
        if rank <= MRR_K:
            mrr_total += 1.0 / rank
        if rank <= NDCG_K:
            ndcg_total += 1.0 / math.log2(rank + 1)

    users = len(targets)
    metrics = {f"hit_rate@{k}": hits[k] / users for k in KS}
    metrics["mrr@10"] = mrr_total / users
    metrics["ndcg@10"] = ndcg_total / users
    return metrics


TOP_K = 20


def load_item_vectors(path: str) -> dict[str, list[float]]:
    """Read the `item_id:v1 v2 ...` embedding file the sims already write."""
    vectors: dict[str, list[float]] = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or ":" not in line:
                continue
            item, raw = line.split(":", 1)
            values = [float(part) for part in raw.split() if part]
            if values:
                vectors[item] = values
    return vectors


def most_popular(split: Split, k: int = TOP_K) -> dict[str, list[str]]:
    """The null hypothesis: the most-engaged training items, same list for everyone.

    A learned model that does not beat this has demonstrated nothing.
    """
    counts = Counter(item for events in split.train.values() for _, item in events)
    ranking = rank_topk({item: float(n) for item, n in counts.items()}, k)
    return {user: list(ranking) for user in split.targets}


def repeat_last(split: Split, k: int = TOP_K) -> dict[str, list[str]]:
    """The user's own most recent distinct items, most recent first."""
    rankings: dict[str, list[str]] = {}
    for user in split.targets:
        seen: list[str] = []
        for _, item in reversed(split.train.get(user, [])):
            if item not in seen:
                seen.append(item)
            if len(seen) == k:
                break
        rankings[user] = seen
    return rankings


def item2vec_neighbors(
    split: Split, vectors: dict[str, list[float]], k: int = TOP_K
) -> dict[str, list[str]]:
    """Nearest neighbours of the user's last training item in the item2vec space.

    Returns an empty ranking when the anchor has no embedding — that is a real property
    of this baseline, and evaluate_system counts it as a miss rather than dropping the
    user from the denominator.
    """
    rankings: dict[str, list[str]] = {}
    for user in split.targets:
        history = split.train.get(user, [])
        anchor = history[-1][1] if history else None
        if anchor is None or anchor not in vectors:
            rankings[user] = []
            continue
        query = vectors[anchor]
        scores = {item: cosine(query, vec) for item, vec in vectors.items()}
        rankings[user] = rank_topk(scores, k)
    return rankings


MAX_SEQUENCE = 64
EMBED_DIM = 64
ATTENTION_HEADS = 4
LAYERS = 2
EPOCHS = 60
LEARNING_RATE = 1e-3


def _torch():
    """Import torch on demand with an actionable message when it is absent.

    Only movielens_pipeline.py depends on torch today and it is a very large install, so
    it stays out of requirements.txt and the producers keep working without it.
    """
    try:
        import torch
        return torch
    except ModuleNotFoundError as exc:  # pragma: no cover - environment dependent
        raise SystemExit(
            "Missing model dependencies. Run: python -m pip install torch"
        ) from exc


def _build_model(torch, catalog_size: int):
    nn = torch.nn

    class NextItemTransformer(nn.Module):
        """Causal transformer over item ids, softmax over the catalog."""

        def __init__(self) -> None:
            super().__init__()
            self.item = nn.Embedding(catalog_size, EMBED_DIM)
            self.position = nn.Embedding(MAX_SEQUENCE, EMBED_DIM)
            layer = nn.TransformerEncoderLayer(
                d_model=EMBED_DIM, nhead=ATTENTION_HEADS,
                dim_feedforward=EMBED_DIM * 2, batch_first=True, dropout=0.0)
            self.encoder = nn.TransformerEncoder(layer, num_layers=LAYERS)
            self.out = nn.Linear(EMBED_DIM, catalog_size)

        def forward(self, ids):
            length = ids.shape[1]
            positions = torch.arange(length, device=ids.device).unsqueeze(0)
            hidden = self.item(ids) + self.position(positions)
            mask = torch.triu(torch.ones(length, length, dtype=torch.bool), diagonal=1)
            return self.out(self.encoder(hidden, mask=mask.to(ids.device)))

    return NextItemTransformer()


def _sequences(split: Split, item_index: dict[str, int]) -> list[list[int]]:
    return [
        [item_index[item] for _, item in events[-MAX_SEQUENCE:]]
        for events in split.train.values()
        if len(events) >= 2
    ]


def train_next_item(split: Split, epochs: int = EPOCHS, seed: int = SEED):
    """Train the causal transformer on the pre-cutoff timelines.

    Returns the model and the item->index map; both are needed to rank.
    """
    torch = _torch()
    torch.manual_seed(seed)

    items = sorted({item for events in split.train.values() for _, item in events})
    item_index = {item: n for n, item in enumerate(items)}
    sequences = _sequences(split, item_index)
    if not sequences:
        raise ValueError("No training sequence has two or more events; nothing to learn from.")

    model = _build_model(torch, len(items))
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
    loss_fn = torch.nn.CrossEntropyLoss()
    width = max(len(seq) for seq in sequences)
    padded = torch.zeros(len(sequences), width, dtype=torch.long)
    keep = torch.zeros(len(sequences), width, dtype=torch.bool)
    for row, seq in enumerate(sequences):
        padded[row, : len(seq)] = torch.tensor(seq, dtype=torch.long)
        keep[row, : len(seq)] = True

    model.train()
    for _ in range(epochs):
        optimizer.zero_grad()
        logits = model(padded[:, :-1])
        targets = padded[:, 1:]
        valid = keep[:, 1:]
        loss = loss_fn(logits[valid], targets[valid])
        loss.backward()
        optimizer.step()
    # Only positions 0..width-2 ever appear as a training input (the loop above always
    # feeds padded[:, :-1]), so a query longer than width-1 would read logits off an
    # untrained position embedding. Recorded on the model so model_rankings can cap its
    # query length without a signature change.
    model.trained_input_length = width - 1
    return model, item_index


def model_rankings(model, item_index: dict[str, int], split: Split, k: int = TOP_K):
    """Rank the catalog for each test user from their training history.

    Queries with the user's newest events — including the very last one, which
    `repeat_last` and `item2vec_neighbors` also anchor on, so the model sees the same
    signal the baselines do. Truncated to `model.trained_input_length` from the front
    (keeping the tail) rather than from the back: `train_next_item` only ever trains
    positions 0..width-2 as inputs, so a longer query would read logits off an untrained
    position embedding. A history shorter than that length is used in full.

    Seen items stay in the candidate set, matching the baselines and the spec.
    """
    torch = _torch()
    index_item = {n: item for item, n in item_index.items()}
    model.eval()
    rankings: dict[str, list[str]] = {}
    with torch.no_grad():
        for user in split.targets:
            history = [item for _, item in split.train.get(user, [])[-MAX_SEQUENCE:]]
            ids = [item_index[item] for item in history if item in item_index]
            ids = ids[-model.trained_input_length:]
            if not ids:
                rankings[user] = []
                continue
            logits = model(torch.tensor([ids], dtype=torch.long))[0, -1]
            order = torch.argsort(logits, descending=True)[:k].tolist()
            rankings[user] = [index_item[n] for n in order]
    return rankings
