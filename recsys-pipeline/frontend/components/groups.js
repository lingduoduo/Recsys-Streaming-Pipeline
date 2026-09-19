// The dashboard's catalogue: which sections exist, what they are called, and where each
// one lives. Pure data with no imports, because the sidebar is a client component and
// must not pull chart and table code into the browser bundle -- section-registry.jsx
// holds the component map and is imported only by the server page.
//
// Grouped by the question a reader arrives with. An earlier split was "online" and
// "offline" prediction, which mislabelled three sections: intents is what users searched
// for, keyword gap is catalog supply against demand, and the engagement funnel is observed
// outcomes -- none of them a prediction, and latency is serving health rather than one too.
//
// Where the data comes from is a separate matter and still worth knowing: latency is the
// only section sourced purely from live telemetry, while satisfaction, freshness and safety
// merge live rows into offline ones when a backend is running. Everything else, including
// the model-evaluation group, is computed from logged Parquet and Redis.
export const GROUPS = [
  { key: "demand", label: "Demand & content" },
  { key: "serving", label: "Serving & outcomes" },
  { key: "models", label: "Model evaluation" },
];

export const SECTIONS = {
  query: {
    group: "demand", label: "Intents",
    description: "What users asked for, and how query length moves click-through.",
  },
  keyword: {
    group: "demand", label: "Keyword gap",
    description: "Where catalog supply diverges from query demand.",
  },
  engagement: {
    group: "serving", label: "Engagement funnel",
    description: "Impression to click to order, broken down by query and genre.",
  },
  satisfaction: {
    group: "serving", label: "Satisfaction",
    description: "Observed satisfaction of the slates that were served.",
  },
  freshness: {
    group: "serving", label: "Freshness",
    description: "How recent the served catalog is, against the freshness window.",
  },
  diversity: {
    group: "serving", label: "Diversity",
    description: "Genre spread across served slates.",
  },
  fairness: {
    group: "serving", label: "Fairness",
    description: "Outcome parity across the supported demographic groups.",
  },
  safety: {
    group: "serving", label: "Safety",
    description: "Candidate safety policy accounting and unsafe exposure.",
  },
  latency: {
    group: "serving", label: "Latency",
    description: "Live request and stage latency reported by the backend.",
  },
  recall: {
    group: "models", label: "Candidate recall",
    description: "recall@k and hit rate for each retrieval method.",
  },
  ranking: {
    group: "models", label: "Ranking quality",
    description: "AUC and log loss for each scoring signal.",
  },
  relevance: {
    group: "models", label: "Relevance",
    description: "Graded relevance — NDCG and MRR — across labeled slates.",
  },
  ope: {
    group: "models", label: "Off-policy evaluation",
    description: "Estimated value and lift of each candidate policy on logged events.",
  },
};

// Derived so a section cannot be listed in one place and routed to another.
export const SECTION_ROUTE = Object.fromEntries(
  Object.entries(SECTIONS).map(([key, { group }]) => [key, `/${group}/${key}`]),
);
