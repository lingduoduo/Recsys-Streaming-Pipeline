import "./globals.css";

export const metadata = {
  title: "Recsys Analysis Dashboard",
  description: "Engagement, intent, retrieval, ranking, and offline policy evaluation.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
