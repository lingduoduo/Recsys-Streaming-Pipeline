import data from "../data/dashboard.json";
import { Scorecard } from "../components/sections";
import {
  RelevanceSection,
  SatisfactionSection,
  FreshnessSection,
  DiversitySection,
  FairnessSection,
  SafetySection,
  LatencySection,
  EngagementSection,
  QuerySection,
  RecallSection,
  RankingSection,
  OpeSection,
  MdpSection,
} from "../components/sections";

export default function Page() {
  return (
    <main className="page-shell">
      <header className="hero">
        <div>
          <span className="eyebrow">RECOMMENDER ANALYTICS</span>
          <h1>Recsys Analysis Dashboard</h1>
          <p>
            Engagement, intent, retrieval, ranking, and offline policy evaluation — {data.rows.toLocaleString()} rows
            from <code>{data.input}</code>.
          </p>
        </div>
        <span className="report-badge">NEXT.JS</span>
      </header>

      <div className="report-grid">
        <Scorecard data={data} />
        <RelevanceSection data={data.relevance} />
        <SatisfactionSection data={data.satisfaction} />
        <FreshnessSection data={data.freshness} />
        <DiversitySection data={data.diversity} />
        <FairnessSection data={data.fairness} />
        <SafetySection data={data.safety} />
        <LatencySection data={data.latency} />
        <EngagementSection data={data.engagement} />
        <QuerySection data={data.query} />
        <RecallSection data={data.recall} />
        <RankingSection data={data.ranking} />
        <OpeSection data={data.ope} />
        <MdpSection data={data.mdp} />
      </div>
    </main>
  );
}
