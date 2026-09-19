// The dashboard's catalogue: which sections exist, what they are called, and where each
// one lives. Pure data with no imports, because the sidebar is a client component and
// must not pull chart and table code into the browser bundle -- section-registry.jsx
// holds the component map and is imported only by the server page.
export const GROUPS = [
  { key: "online", label: "Online prediction" },
  { key: "offline", label: "Offline prediction" },
];

// Online sections describe what the serving path actually did; offline sections are models
// re-scored afterwards. Only latency is purely live telemetry -- satisfaction, freshness
// and safety are offline rows with live ones merged in, and intents and the off-policy arms
// are computed from logged Parquet.
export const SECTIONS = {
  query: {
    group: "online", label: "Intents",
    description: "What users asked for, and how query length moves click-through.",
  },
  keyword: {
    group: "online", label: "Keyword gap",
    description: "Where catalog supply diverges from query demand.",
  },
  engagement: {
    group: "online", label: "Engagement funnel",
    description: "Impression to click to order, broken down by query and genre.",
  },
  satisfaction: {
    group: "online", label: "Satisfaction",
    description: "Observed satisfaction of the slates that were served.",
  },
  freshness: {
    group: "online", label: "Freshness",
    description: "How recent the served catalog is, against the freshness window.",
  },
  diversity: {
    group: "online", label: "Diversity",
    description: "Genre spread across served slates.",
  },
  fairness: {
    group: "online", label: "Fairness",
    description: "Outcome parity across the supported demographic groups.",
  },
  safety: {
    group: "online", label: "Safety",
    description: "Candidate safety policy accounting and unsafe exposure.",
  },
  latency: {
    group: "online", label: "Latency",
    description: "Live request and stage latency reported by the backend.",
  },
  recall: {
    group: "offline", label: "Candidate recall",
    description: "recall@k and hit rate for each retrieval method.",
  },
  ranking: {
    group: "offline", label: "Ranking quality",
    description: "AUC and log loss for each scoring signal.",
  },
  relevance: {
    group: "offline", label: "Relevance",
    description: "Graded relevance — NDCG and MRR — across labeled slates.",
  },
  ope: {
    group: "offline", label: "Off-policy evaluation",
    description: "Estimated value and lift of each candidate policy on logged events.",
  },
};

// Derived so a section cannot be listed in one place and routed to another.
export const SECTION_ROUTE = Object.fromEntries(
  Object.entries(SECTIONS).map(([key, { group }]) => [key, `/${group}/${key}`]),
);
