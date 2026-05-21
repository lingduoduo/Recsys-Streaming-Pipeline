#!/usr/bin/env python3
"""
movielens_pipeline.py — MovieLens two-stage recommendation pipeline.

Stage 1 – Retrieval  : two-tower dot-product ANN over the full movie catalog.
Stage 2 – Ranking    : transformer with candidate isolation attention masking.
           Multi-task heads: click, rating, favorite, rewatch, dwell.

Run:
  python spark-recsys/movielens_pipeline.py

Models are trained on first run and cached as ONNX files under sampledata/.
Subsequent runs skip training and load from disk.
"""

import io
import json
import os
import warnings

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
import onnxruntime as ort

warnings.filterwarnings("ignore", category=UserWarning)

# ──────────────────────────────────────────────────────────────────────────────
# Movie catalog and user interaction history
# ──────────────────────────────────────────────────────────────────────────────

ALL_GENRES = [
    "Action", "Adventure", "Biography", "Comedy", "Crime", "Drama",
    "Fantasy", "History", "Horror", "Music", "Mystery", "Romance",
    "Sci-Fi", "Thriller", "War",
]
GENRE_TO_IDX = {g: i for i, g in enumerate(ALL_GENRES)}
GENRE_DIM = len(ALL_GENRES)  # 15

# (movie_id, title, year, genres)
MOVIES = [
    ("m001", "The Shawshank Redemption",         1994, ["Drama"]),
    ("m002", "The Godfather",                     1972, ["Crime", "Drama"]),
    ("m003", "The Dark Knight",                   2008, ["Action", "Crime", "Drama", "Thriller"]),
    ("m004", "Pulp Fiction",                      1994, ["Crime", "Drama", "Thriller"]),
    ("m005", "Forrest Gump",                      1994, ["Drama", "Romance"]),
    ("m006", "Inception",                         2010, ["Action", "Adventure", "Sci-Fi", "Thriller"]),
    ("m007", "The Matrix",                        1999, ["Action", "Sci-Fi"]),
    ("m008", "Goodfellas",                        1990, ["Biography", "Crime", "Drama"]),
    ("m009", "Fight Club",                        1999, ["Drama", "Thriller"]),
    ("m010", "The Silence of the Lambs",          1991, ["Crime", "Drama", "Thriller"]),
    ("m011", "Schindler's List",                  1993, ["Biography", "Drama", "History"]),
    ("m012", "Interstellar",                      2014, ["Adventure", "Drama", "Sci-Fi"]),
    ("m013", "The Lord of the Rings: Fellowship", 2001, ["Adventure", "Drama", "Fantasy"]),
    ("m014", "Gladiator",                         2000, ["Action", "Adventure", "Drama"]),
    ("m015", "The Prestige",                      2006, ["Drama", "Mystery", "Sci-Fi", "Thriller"]),
    ("m016", "Whiplash",                          2014, ["Drama", "Music"]),
    ("m017", "Get Out",                           2017, ["Horror", "Mystery", "Thriller"]),
    ("m018", "Parasite",                          2019, ["Comedy", "Drama", "Thriller"]),
    ("m019", "1917",                              2019, ["Drama", "Thriller", "War"]),
    ("m020", "Joker",                             2019, ["Crime", "Drama", "Thriller"]),
    ("m021", "Avengers: Endgame",                 2019, ["Action", "Adventure", "Sci-Fi"]),
    ("m022", "Mad Max: Fury Road",                2015, ["Action", "Adventure", "Sci-Fi", "Thriller"]),
    ("m023", "The Grand Budapest Hotel",          2014, ["Adventure", "Comedy", "Crime"]),
    ("m024", "Gone Girl",                         2014, ["Drama", "Mystery", "Thriller"]),
    ("m025", "La La Land",                        2016, ["Comedy", "Drama", "Music", "Romance"]),
    ("m026", "Her",                               2013, ["Drama", "Romance", "Sci-Fi"]),
    ("m027", "The Social Network",                2010, ["Biography", "Drama"]),
    ("m028", "Arrival",                           2016, ["Drama", "Mystery", "Sci-Fi"]),
    ("m029", "Blade Runner 2049",                 2017, ["Drama", "Mystery", "Sci-Fi", "Thriller"]),
    ("m030", "Dunkirk",                           2017, ["Action", "Drama", "History", "Thriller", "War"]),
]

MOVIE_TO_IDX: dict[str, int] = {m[0]: i for i, m in enumerate(MOVIES)}
N_MOVIES = len(MOVIES)

# Multi-hot genre feature matrix — (N_MOVIES, GENRE_DIM)
MOVIE_GENRE_FEATS = np.zeros((N_MOVIES, GENRE_DIM), dtype=np.float32)
for _i, (_, _, _, _genres) in enumerate(MOVIES):
    for _g in _genres:
        MOVIE_GENRE_FEATS[_i, GENRE_TO_IDX[_g]] = 1.0

# User interaction histories
USER_HISTORY: dict[str, dict] = {
    "alice": {
        # Drama / crime-thriller fan
        "watched":    ["m001", "m002", "m004", "m005", "m008", "m009", "m010", "m011", "m016", "m018"],
        "rated_high": [("m001", 5.0), ("m010", 4.5), ("m011", 4.5),
                       ("m004", 4.0), ("m018", 4.0), ("m016", 4.0), ("m002", 4.5)],
        "favorites":  ["m001", "m010", "m011"],
        "rewatched":  ["m001"],
    },
    "bob": {
        # Sci-Fi / mystery enthusiast
        "watched":    ["m006", "m007", "m012", "m015", "m021", "m022", "m026", "m028", "m029"],
        "rated_high": [("m006", 5.0), ("m028", 4.5), ("m012", 4.5),
                       ("m029", 4.0), ("m007", 4.0), ("m026", 4.0), ("m015", 4.5)],
        "favorites":  ["m006", "m028", "m015"],
        "rewatched":  ["m007", "m006"],
    },
    "charlie": {
        # Action / adventure blockbuster fan
        "watched":    ["m003", "m006", "m013", "m014", "m021", "m022", "m030"],
        "rated_high": [("m003", 5.0), ("m013", 4.5), ("m021", 4.5),
                       ("m014", 4.0), ("m022", 4.0), ("m030", 4.0)],
        "favorites":  ["m003", "m021"],
        "rewatched":  ["m003", "m021"],
    },
}

USERS = list(USER_HISTORY.keys())
N_USERS = len(USERS)
USER_TO_IDX = {u: i for i, u in enumerate(USERS)}

# ──────────────────────────────────────────────────────────────────────────────
# Hyperparameters
# ──────────────────────────────────────────────────────────────────────────────

EMB_DIM  = 32   # two-tower embedding dimension
D_MODEL  = 64   # transformer model dimension
NHEAD    = 4
N_LAYERS = 2

SAMPLEDATA  = os.path.join(os.path.dirname(__file__), "sampledata")
USER_TOWER_ONNX = os.path.join(SAMPLEDATA, "movielens_user_tower.onnx")
ITEM_TOWER_ONNX = os.path.join(SAMPLEDATA, "movielens_item_tower.onnx")
RANKING_ONNX    = os.path.join(SAMPLEDATA, "movielens_ranking.onnx")

# ──────────────────────────────────────────────────────────────────────────────
# Model definitions
# ──────────────────────────────────────────────────────────────────────────────

class UserTower(nn.Module):
    """User ID → L2-normalised embedding for dot-product retrieval."""

    def __init__(self, n_users: int, emb_dim: int = EMB_DIM) -> None:
        super().__init__()
        self.emb = nn.Embedding(n_users, emb_dim)
        self.mlp = nn.Sequential(
            nn.Linear(emb_dim, 64), nn.ReLU(), nn.Linear(64, emb_dim),
        )

    def forward(self, user_id: torch.Tensor) -> torch.Tensor:
        x = self.emb(user_id)
        return F.normalize(x + self.mlp(x), dim=-1)  # residual + L2-norm


class ItemTower(nn.Module):
    """(movie_id, genre_features) → L2-normalised embedding."""

    def __init__(self, n_items: int, genre_dim: int = GENRE_DIM,
                 emb_dim: int = EMB_DIM) -> None:
        super().__init__()
        self.id_emb     = nn.Embedding(n_items, emb_dim)
        self.genre_proj = nn.Linear(genre_dim, emb_dim)
        self.mlp = nn.Sequential(
            nn.Linear(emb_dim * 2, 64), nn.ReLU(), nn.Linear(64, emb_dim),
        )

    def forward(self, item_id: torch.Tensor,
                genre_feat: torch.Tensor) -> torch.Tensor:
        x = self.mlp(torch.cat([self.id_emb(item_id),
                                 self.genre_proj(genre_feat)], dim=-1))
        return F.normalize(x, dim=-1)


class _TransformerLayer(nn.Module):
    """
    Single pre-norm transformer encoder layer with manual MHA.

    Manual MHA avoids nn.MultiheadAttention's internal fixed-shape Reshape ops,
    which the legacy ONNX tracer bakes in as static constants and breaks when
    inference uses a different sequence length than the export trace.
    """

    def __init__(self, d_model: int, nhead: int, dropout: float = 0.0) -> None:
        super().__init__()
        assert d_model % nhead == 0
        self.nhead    = nhead
        self.head_dim = d_model // nhead
        self.scale    = self.head_dim ** -0.5
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)
        self.q_proj  = nn.Linear(d_model, d_model, bias=False)
        self.k_proj  = nn.Linear(d_model, d_model, bias=False)
        self.v_proj  = nn.Linear(d_model, d_model, bias=False)
        self.out_proj = nn.Linear(d_model, d_model)
        self.ffn   = nn.Sequential(
            nn.Linear(d_model, d_model * 2), nn.GELU(), nn.Linear(d_model * 2, d_model),
        )

    def forward(self, x: torch.Tensor,
                attn_mask: torch.Tensor) -> torch.Tensor:
        # x: (S, d_model),  attn_mask: (S, S)
        S = x.shape[0]
        h = self.norm1(x)
        # (S, d_model) → (nhead, S, head_dim)
        q = self.q_proj(h).view(S, self.nhead, self.head_dim).transpose(0, 1)
        k = self.k_proj(h).view(S, self.nhead, self.head_dim).transpose(0, 1)
        v = self.v_proj(h).view(S, self.nhead, self.head_dim).transpose(0, 1)
        # (nhead, S, S) — attn_mask broadcasts from (S, S)
        weights = (q @ k.transpose(-2, -1)) * self.scale + attn_mask
        weights = torch.softmax(weights, dim=-1)
        # (nhead, S, head_dim) → (S, d_model)
        attn_out = (weights @ v).transpose(0, 1).reshape(S, self.nhead * self.head_dim)
        x = x + self.out_proj(attn_out)
        x = x + self.ffn(self.norm2(x))
        return x


class RankingTransformer(nn.Module):
    """
    Transformer ranker with candidate isolation attention masking.

    Sequence layout:  [user_token, cand_0, cand_1, ..., cand_{K-1}]

    Isolation mask (S = K+1):
      • user token (row 0)   attends to all positions
      • candidate i (row i)  attends ONLY to the user token (col 0) and
        itself (col i) — candidates are fully isolated from each other.

    The mask is passed in as a pre-built float tensor (0 = attend, -inf = block)
    so the ONNX graph only needs to handle the transformer computation, not
    the dynamic mask construction.

    Outputs: (click, rating, favorite, rewatch, dwell) — each shape (K,).
    """

    def __init__(self, emb_dim: int = EMB_DIM, d_model: int = D_MODEL,
                 nhead: int = NHEAD, n_layers: int = N_LAYERS) -> None:
        super().__init__()
        self.user_proj = nn.Linear(emb_dim, d_model)
        self.item_proj = nn.Linear(emb_dim, d_model)
        self.layers    = nn.ModuleList(
            [_TransformerLayer(d_model, nhead) for _ in range(n_layers)]
        )
        self.norm_out  = nn.LayerNorm(d_model)
        # Multi-task heads
        self.head_click    = nn.Linear(d_model, 1)
        self.head_rating   = nn.Linear(d_model, 1)
        self.head_favorite = nn.Linear(d_model, 1)
        self.head_rewatch  = nn.Linear(d_model, 1)
        self.head_dwell    = nn.Linear(d_model, 1)

    def forward(
        self,
        user_emb:      torch.Tensor,   # (1, emb_dim)
        candidate_embs: torch.Tensor,  # (K, emb_dim)
        attn_mask:     torch.Tensor,   # (K+1, K+1) isolation mask
    ) -> tuple:
        u   = self.user_proj(user_emb)        # (1, d_model)
        c   = self.item_proj(candidate_embs)  # (K, d_model)
        seq = torch.cat([u, c], dim=0)        # (K+1, d_model)

        for layer in self.layers:
            seq = layer(seq, attn_mask)

        cand = self.norm_out(seq[1:])          # (K, d_model)  skip user token

        click    = torch.sigmoid(self.head_click(cand)).squeeze(-1)
        rating   = torch.sigmoid(self.head_rating(cand)).squeeze(-1)
        favorite = torch.sigmoid(self.head_favorite(cand)).squeeze(-1)
        rewatch  = torch.sigmoid(self.head_rewatch(cand)).squeeze(-1)
        dwell    = torch.sigmoid(self.head_dwell(cand)).squeeze(-1)
        return click, rating, favorite, rewatch, dwell


def build_isolation_mask(K: int) -> torch.Tensor:
    """
    Candidate isolation attention mask for K candidates.

    Returns float (K+1, K+1) tensor where 0.0 means 'attend' and -inf means 'block'.
    Layout: position 0 = user token, positions 1..K = candidates.
    """
    S    = K + 1
    mask = torch.full((S, S), float("-inf"))
    mask[0, :]   = 0.0  # user token: attend to all
    mask[:, 0]   = 0.0  # all tokens: attend to user
    mask.fill_diagonal_(0.0)  # self-attention always allowed
    return mask

# ──────────────────────────────────────────────────────────────────────────────
# Training data generators
# ──────────────────────────────────────────────────────────────────────────────

def _bpr_triples() -> list[tuple[int, int, int]]:
    """(user_idx, pos_movie_idx, neg_movie_idx) triples for BPR two-tower training."""
    rng = np.random.default_rng(42)
    triples: list[tuple[int, int, int]] = []
    for user, hist in USER_HISTORY.items():
        uid = USER_TO_IDX[user]
        pos_ids = {mid for mid, r in hist["rated_high"] if r >= 3.5}
        neg_ids = [m[0] for m in MOVIES if m[0] not in set(hist["watched"])]
        for pos in pos_ids:
            sampled_neg = rng.choice(neg_ids, size=min(6, len(neg_ids)), replace=False)
            for neg in sampled_neg:
                triples.append((uid, MOVIE_TO_IDX[pos], MOVIE_TO_IDX[neg]))
    return triples


def _ranking_labels() -> list[tuple[int, int, dict[str, float]]]:
    """(user_idx, movie_idx, {signal: label}) per (user, movie) pair."""
    rows: list[tuple[int, int, dict[str, float]]] = []
    for user, hist in USER_HISTORY.items():
        uid = USER_TO_IDX[user]
        rated_map = {mid: r for mid, r in hist["rated_high"]}
        fav_set   = set(hist["favorites"])
        rew_set   = set(hist["rewatched"])
        watched   = set(hist["watched"])
        for m_id, _, _, _ in MOVIES:
            r = rated_map.get(m_id, 0.0)
            rows.append((uid, MOVIE_TO_IDX[m_id], {
                "click":    1.0 if m_id in watched else 0.0,
                "rating":   r / 5.0,
                "favorite": 1.0 if m_id in fav_set else 0.0,
                "rewatch":  1.0 if m_id in rew_set  else 0.0,
                "dwell":    min(1.0, r / 4.0) if r > 0 else 0.0,
            }))
    return rows

# ──────────────────────────────────────────────────────────────────────────────
# Training
# ──────────────────────────────────────────────────────────────────────────────

def train_two_tower(epochs: int = 300) -> tuple[UserTower, ItemTower]:
    print("Training two-tower retrieval model …")
    triples = _bpr_triples()
    user_tower = UserTower(N_USERS, EMB_DIM)
    item_tower = ItemTower(N_MOVIES, GENRE_DIM, EMB_DIM)
    genre_t    = torch.tensor(MOVIE_GENRE_FEATS)
    optim = torch.optim.Adam(
        list(user_tower.parameters()) + list(item_tower.parameters()), lr=2e-3,
    )
    idx_arr = np.arange(len(triples))
    for epoch in range(epochs):
        np.random.shuffle(idx_arr)
        total = 0.0
        for i in idx_arr:
            uid, pos_iid, neg_iid = triples[i]
            u = user_tower(torch.tensor([uid]))
            p = item_tower(torch.tensor([pos_iid]), genre_t[pos_iid: pos_iid + 1])
            n = item_tower(torch.tensor([neg_iid]), genre_t[neg_iid: neg_iid + 1])
            loss = -F.logsigmoid((u * p).sum() - (u * n).sum())
            optim.zero_grad(); loss.backward(); optim.step()
            total += loss.item()
        if (epoch + 1) % 100 == 0:
            print(f"  epoch {epoch + 1}/{epochs}  BPR loss = {total / len(triples):.4f}")
    return user_tower, item_tower


def train_ranking(
    user_tower: UserTower,
    item_tower: ItemTower,
    epochs: int = 300,
) -> RankingTransformer:
    print("Training ranking transformer …")
    rows    = _ranking_labels()
    ranker  = RankingTransformer(EMB_DIM, D_MODEL, NHEAD, N_LAYERS)
    genre_t = torch.tensor(MOVIE_GENRE_FEATS)
    optim   = torch.optim.Adam(ranker.parameters(), lr=1e-3)

    # Pre-compute frozen two-tower item embeddings
    with torch.no_grad():
        all_iids  = torch.arange(N_MOVIES)
        all_item_embs = item_tower(all_iids, genre_t)  # (N_MOVIES, EMB_DIM)

    idx_arr = np.arange(len(rows))
    for epoch in range(epochs):
        np.random.shuffle(idx_arr)
        total = 0.0
        for i in idx_arr:
            uid, mid, lbl = rows[i]
            with torch.no_grad():
                u_emb = user_tower(torch.tensor([uid]))  # (1, EMB_DIM)
            cand  = all_item_embs[mid].unsqueeze(0)      # (1, EMB_DIM)
            mask  = build_isolation_mask(1)              # (2, 2)
            click_p, rating_p, fav_p, rew_p, dwell_p = ranker(u_emb, cand, mask)
            loss = (
                F.binary_cross_entropy(click_p[0],  torch.tensor(lbl["click"]))
                + F.mse_loss(rating_p[0],            torch.tensor(lbl["rating"]))
                + F.binary_cross_entropy(fav_p[0],  torch.tensor(lbl["favorite"]))
                + F.binary_cross_entropy(rew_p[0],  torch.tensor(lbl["rewatch"]))
                + F.mse_loss(dwell_p[0],             torch.tensor(lbl["dwell"]))
            )
            optim.zero_grad(); loss.backward(); optim.step()
            total += loss.item()
        if (epoch + 1) % 100 == 0:
            print(f"  epoch {epoch + 1}/{epochs}  multi-task loss = {total / len(rows):.4f}")
    return ranker

# ──────────────────────────────────────────────────────────────────────────────
# ONNX export
# ──────────────────────────────────────────────────────────────────────────────

def export_onnx(
    user_tower: UserTower,
    item_tower: ItemTower,
    ranker: RankingTransformer,
) -> None:
    print("Exporting models to ONNX …")
    os.makedirs(SAMPLEDATA, exist_ok=True)
    user_tower.eval(); item_tower.eval(); ranker.eval()
    genre_t = torch.tensor(MOVIE_GENRE_FEATS)

    # ── User tower: user_id → user_emb ──────────────────────────────────
    torch.onnx.export(
        user_tower,
        (torch.tensor([0]),),
        USER_TOWER_ONNX,
        input_names=["user_id"],
        output_names=["user_emb"],
        dynamic_axes={"user_id": {0: "batch"}, "user_emb": {0: "batch"}},
    )
    print(f"  user tower  → {USER_TOWER_ONNX}")

    # ── Item tower: (movie_id, genre_feat) → item_emb ───────────────────
    torch.onnx.export(
        item_tower,
        (torch.arange(2), genre_t[:2]),
        ITEM_TOWER_ONNX,
        input_names=["movie_id", "genre_feat"],
        output_names=["item_emb"],
        dynamic_axes={
            "movie_id":  {0: "batch"},
            "genre_feat": {0: "batch"},
            "item_emb":  {0: "batch"},
        },
    )
    print(f"  item tower  → {ITEM_TOWER_ONNX}")

    # ── Ranking transformer: (user_emb, candidate_embs, attn_mask) → 5 signals
    # Use dynamo=False (legacy TorchScript tracer) — the dynamo exporter rejects
    # float -inf attention masks with dynamic shapes; the legacy path handles them correctly.
    K_trace    = 5
    mask_trace = build_isolation_mask(K_trace)
    torch.onnx.export(
        ranker,
        (torch.randn(1, EMB_DIM), torch.randn(K_trace, EMB_DIM), mask_trace),
        RANKING_ONNX,
        dynamo=False,
        input_names=["user_emb", "candidate_embs", "attn_mask"],
        output_names=["click", "rating", "favorite", "rewatch", "dwell"],
        dynamic_axes={
            "candidate_embs": {0: "num_candidates"},
            "attn_mask":      {0: "seq_len", 1: "seq_len"},
            "click":          {0: "num_candidates"},
            "rating":         {0: "num_candidates"},
            "favorite":       {0: "num_candidates"},
            "rewatch":        {0: "num_candidates"},
            "dwell":          {0: "num_candidates"},
        },
        opset_version=17,
    )
    print(f"  ranking     → {RANKING_ONNX}")

# ──────────────────────────────────────────────────────────────────────────────
# Inference pipeline
# ──────────────────────────────────────────────────────────────────────────────

_ENGAGEMENT_WEIGHTS = {
    "click": 0.35, "rating": 0.25, "favorite": 0.20, "rewatch": 0.12, "dwell": 0.08,
}


def run_pipeline(user: str, top_k: int = 10) -> None:
    hist = USER_HISTORY.get(user)
    if hist is None:
        print(f"Unknown user: {user}"); return

    print(f"\n{'═' * 100}")
    print(f"  MovieLens Recommendation Pipeline   user = {user}")
    print(f"{'═' * 100}")

    # Load ONNX sessions
    user_sess = ort.InferenceSession(USER_TOWER_ONNX)
    item_sess = ort.InferenceSession(ITEM_TOWER_ONNX)
    rank_sess = ort.InferenceSession(RANKING_ONNX)

    # ── Step 1: User interaction history ─────────────────────────────────
    watched_titles = [MOVIES[MOVIE_TO_IDX[m]][1] for m in hist["watched"]]
    fav_titles     = [MOVIES[MOVIE_TO_IDX[m]][1] for m in hist["favorites"]]
    print(f"\n[Step 1] User interaction history")
    print(f"  Watched   ({len(hist['watched'])}): " + ", ".join(watched_titles))
    print(f"  Rated ≥4  ({len(hist['rated_high'])}): "
          + ", ".join(f"{MOVIES[MOVIE_TO_IDX[m]][1]} ({r:.1f})"
                      for m, r in hist["rated_high"]))
    print(f"  Favorites ({len(fav_titles)}): " + ", ".join(fav_titles))
    print(f"  Rewatched ({len(hist['rewatched'])}): "
          + ", ".join(MOVIES[MOVIE_TO_IDX[m]][1] for m in hist["rewatched"]))

    # ── Step 2: User embedding via two-tower user tower ───────────────────
    print(f"\n[Step 2] Two-tower user tower  →  user embedding")
    uid_arr   = np.array([USER_TO_IDX[user]], dtype=np.int64)
    (user_emb,) = user_sess.run(None, {"user_id": uid_arr})  # (1, EMB_DIM)
    print(f"  user_emb  shape={user_emb.shape}  "
          f"L2-norm={np.linalg.norm(user_emb):.4f} (normalised ≈ 1.0)")

    # ── Step 3: Item embeddings for all movies via two-tower item tower ───
    print(f"\n[Step 3] Two-tower item tower  →  embeddings for all {N_MOVIES} movies")
    all_iids   = np.arange(N_MOVIES, dtype=np.int64)
    genre_feats = MOVIE_GENRE_FEATS.astype(np.float32)
    (all_item_embs,) = item_sess.run(
        None, {"movie_id": all_iids, "genre_feat": genre_feats},
    )  # (N_MOVIES, EMB_DIM)
    print(f"  item_embs shape={all_item_embs.shape}  "
          f"mean L2-norm={np.linalg.norm(all_item_embs, axis=1).mean():.4f}")

    # ── Step 4: Dot-product nearest-neighbour search ──────────────────────
    print(f"\n[Step 4] Dot-product ANN retrieval  →  top-{top_k} candidates")
    watched_set = set(hist["watched"])
    scores_all  = (user_emb @ all_item_embs.T)[0]  # (N_MOVIES,)
    # Exclude already-watched movies
    for mid in watched_set:
        scores_all[MOVIE_TO_IDX[mid]] = -1e9
    top_indices  = np.argsort(-scores_all)[:top_k]
    cand_ids     = [MOVIES[i][0] for i in top_indices]
    cand_titles  = [MOVIES[i][1] for i in top_indices]
    retrieval_scores = scores_all[top_indices]
    print("  Retrieved candidates (dot-product score):")
    for rank, (title, sc) in enumerate(zip(cand_titles, retrieval_scores), 1):
        print(f"    {rank:2d}. {title:<44}  dot={sc:+.4f}")

    # ── Step 5: Transformer ranking with candidate isolation masking ──────
    print(f"\n[Step 5] Transformer ranking  ({N_LAYERS}-layer, {NHEAD}-head, "
          f"d_model={D_MODEL})  with candidate isolation attention mask")
    K         = len(top_indices)
    cand_embs = all_item_embs[top_indices].astype(np.float32)  # (K, EMB_DIM)
    iso_mask  = build_isolation_mask(K).numpy().astype(np.float32)  # (K+1, K+1)

    # Show mask structure (compact)
    attend_counts = (iso_mask == 0.0).sum(axis=1)
    print(f"  Isolation mask ({K + 1}×{K + 1}):")
    print(f"    user token attends to {int(attend_counts[0])} positions  "
          f"(all {K + 1})")
    print(f"    each candidate attends to "
          f"{int(attend_counts[1])} positions  (user + self only)")

    click_p, rating_p, fav_p, rew_p, dwell_p = rank_sess.run(
        None,
        {"user_emb": user_emb.astype(np.float32),
         "candidate_embs": cand_embs,
         "attn_mask": iso_mask},
    )

    # ── Step 6: Aggregate engagement signals → final ranking ─────────────
    print(f"\n[Step 6] Multi-task engagement predictions")
    w = _ENGAGEMENT_WEIGHTS
    final_score = (
        w["click"]    * click_p
        + w["rating"] * rating_p
        + w["favorite"] * fav_p
        + w["rewatch"]  * rew_p
        + w["dwell"]    * dwell_p
    )
    order = np.argsort(-final_score)

    print(f"\n  Final ranked recommendation list")
    W = 105
    hdr = (f"{'Rank':<5} {'Movie':<44} {'Year':<6}"
           f" {'Click':>7} {'Rating':>7} {'Fav':>6} {'Rewatch':>8} {'Dwell':>6}"
           f" {'Score':>8}")
    print(f"\n  {'─' * W}")
    print(f"  {hdr}")
    print(f"  {'─' * W}")
    for rank, idx in enumerate(order, 1):
        mid = cand_ids[idx]
        _, title, year, genres = MOVIES[MOVIE_TO_IDX[mid]]
        genre_str = "|".join(genres)
        print(
            f"  {rank:<5} {title:<44} {year:<6}"
            f" {click_p[idx]:>6.1%} {rating_p[idx]:>7.1%}"
            f" {fav_p[idx]:>5.1%} {rew_p[idx]:>7.1%} {dwell_p[idx]:>6.1%}"
            f" {final_score[idx]:>8.4f}"
        )
    print(f"  {'─' * W}")
    print(f"\n  Weights: click={w['click']:.0%}  rating={w['rating']:.0%}"
          f"  favorite={w['favorite']:.0%}  rewatch={w['rewatch']:.0%}"
          f"  dwell={w['dwell']:.0%}")

# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    os.makedirs(SAMPLEDATA, exist_ok=True)

    onnx_files = [USER_TOWER_ONNX, ITEM_TOWER_ONNX, RANKING_ONNX]
    if all(os.path.exists(p) for p in onnx_files):
        print("Pre-trained ONNX models found — skipping training.")
    else:
        torch.manual_seed(42)
        np.random.seed(42)
        user_tower, item_tower = train_two_tower(epochs=300)
        ranker = train_ranking(user_tower, item_tower, epochs=300)
        export_onnx(user_tower, item_tower, ranker)

    for user in USERS:
        run_pipeline(user, top_k=10)
    print()


if __name__ == "__main__":
    main()
