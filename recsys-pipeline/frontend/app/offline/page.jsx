import data from "../../data/dashboard.json";
import { RelevanceSection } from "../../components/measurements";
import { RecallSection, RankingSection, OpeSection } from "../../components/diagnostics";

// Models re-scored after the fact: what a policy or retriever would have done, measured
// against logged outcomes -- see components/groups.js.
export default function Page() {
  return (
    <div className="report-grid">
      <h2 className="group-heading">Retrieval and ranking</h2>
      <RecallSection data={data.recall} />
      <RankingSection data={data.ranking} />
      <RelevanceSection data={data.relevance} />

      <h2 className="group-heading">Policy</h2>
      <OpeSection data={data.ope} />
    </div>
  );
}
