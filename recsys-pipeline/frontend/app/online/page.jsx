import data from "../../data/dashboard.json";
import {
  SatisfactionSection, FreshnessSection, DiversitySection, FairnessSection,
  SafetySection, LatencySection,
} from "../../components/measurements";
import { EngagementSection, QuerySection } from "../../components/diagnostics";
import { KeywordSection } from "../../components/keyword-report";

// What the serving path actually did. Every number here describes served traffic, as
// opposed to a model re-scored afterwards -- see components/groups.js.
export default function Page() {
  return (
    <div className="report-grid">
      <h2 className="group-heading">Demand — what was asked for</h2>
      <QuerySection data={data.query} />
      <KeywordSection data={data.keyword} />
      <EngagementSection data={data.engagement} />

      <h2 className="group-heading">Served quality — what came back</h2>
      <SatisfactionSection data={data.satisfaction} />
      <FreshnessSection data={data.freshness} />
      <DiversitySection data={data.diversity} />
      <FairnessSection data={data.fairness} />
      <SafetySection data={data.safety} />

      <h2 className="group-heading">Serving health</h2>
      <LatencySection data={data.latency} />
    </div>
  );
}
