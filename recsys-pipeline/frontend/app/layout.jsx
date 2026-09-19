import "./globals.css";
import data from "../data/dashboard.json";
import { Nav } from "../components/nav";

export const metadata = {
  title: "Recsys Analysis Dashboard",
  description: "Engagement, intent, retrieval, ranking, and offline policy evaluation.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <main className="page-shell">
          <header className="hero">
            <div>
              <span className="eyebrow">RECOMMENDER ANALYTICS</span>
              <h1>Recsys Analysis Dashboard</h1>
              <p>
                {data.rows.toLocaleString()} rows from <code>{data.input}</code>.
              </p>
            </div>
            <span className="report-badge">NEXT.JS</span>
          </header>
          <Nav />
          {children}
        </main>
      </body>
    </html>
  );
}
