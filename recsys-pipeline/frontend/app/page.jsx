import data from "../data/dashboard.json";
import { Scorecard } from "../components/scorecard";

export default function Page() {
  return (
    <div className="report-grid">
      <Scorecard data={data} />
    </div>
  );
}
