import "./globals.css";
import data from "../data/dashboard.json";
import { Sidebar } from "../components/sidebar";

export const metadata = {
  title: "Recsys Analysis Dashboard",
  description: "Engagement, intent, retrieval, ranking, and offline policy evaluation.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <div className="app-shell">
          <Sidebar />
          <main className="app-main">
            {children}
            <footer className="app-footer">
              {data.rows.toLocaleString()} rows from <code>{data.input}</code>
            </footer>
          </main>
        </div>
      </body>
    </html>
  );
}
