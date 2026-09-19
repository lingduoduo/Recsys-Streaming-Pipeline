import data from "../data/dashboard.json";
import { Scorecard } from "../components/scorecard";

export default function Page() {
  return (
    <article className="section-page">
      <header className="page-header">
        <span className="eyebrow">Overview</span>
        <h1>Measurement scorecard</h1>
        <p className="page-description">
          Seven measurement envelopes at a glance. Each tile opens that section&apos;s page.
        </p>
      </header>
      <Scorecard data={data} />
    </article>
  );
}
