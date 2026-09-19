// Where each dashboard section lives. One map, three readers: Scorecard builds its tile
// hrefs from it, Nav renders ROUTES, and test_dashboard_routes.py checks that every key
// here is mounted on the page it names.
//
// The split is by what a number means, not where its data came from. "Online" sections
// describe what the serving path actually did; "offline" sections are models re-scored
// afterwards. Only latency is purely live telemetry -- satisfaction, freshness and safety
// are offline rows with live ones merged in, and intents and the off-policy arms are
// computed from logged Parquet.
export const ROUTES = [
  { href: "/", label: "Overview" },
  { href: "/online", label: "Online prediction analysis" },
  { href: "/offline", label: "Offline prediction analysis" },
];

export const SECTION_ROUTE = {
  query: "/online",
  keyword: "/online",
  engagement: "/online",
  satisfaction: "/online",
  freshness: "/online",
  diversity: "/online",
  fairness: "/online",
  safety: "/online",
  latency: "/online",
  recall: "/offline",
  ranking: "/offline",
  ope: "/offline",
  relevance: "/offline",
};
