import {
  RelevanceSection, SatisfactionSection, FreshnessSection, DiversitySection,
  FairnessSection, SafetySection, LatencySection,
} from "./measurements";
import {
  EngagementSection, QuerySection, RecallSection, RankingSection, OpeSection,
} from "./diagnostics";
import { KeywordSection } from "./keyword-report";

// Key -> the component that renders it. Kept apart from groups.js on purpose: the sidebar
// imports groups.js and is a client component, so pulling this map -- and through it every
// chart and table -- into that bundle would be a needless cost.
export const SECTION_COMPONENTS = {
  query: QuerySection,
  keyword: KeywordSection,
  engagement: EngagementSection,
  satisfaction: SatisfactionSection,
  freshness: FreshnessSection,
  diversity: DiversitySection,
  fairness: FairnessSection,
  safety: SafetySection,
  latency: LatencySection,
  recall: RecallSection,
  ranking: RankingSection,
  relevance: RelevanceSection,
  ope: OpeSection,
};
