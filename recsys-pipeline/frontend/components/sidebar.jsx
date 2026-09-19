"use client";

import { usePathname } from "next/navigation";
import { GROUPS, SECTIONS, SECTION_ROUTE } from "./groups";

// A client component because it marks the active link from the current path. It imports
// groups.js only -- pure data -- so no chart or table code reaches this bundle. Every page
// and every section stays a server component; keyword-report.jsx is the other client
// component, for its Top-K selector.
export function Sidebar() {
  const pathname = usePathname();

  const link = (href, label) => (
    <a key={href} href={href}
       className={pathname === href ? "sidebar-link sidebar-active" : "sidebar-link"}
       aria-current={pathname === href ? "page" : undefined}>
      {label}
    </a>
  );

  return (
    <nav className="sidebar" aria-label="Dashboard sections">
      <div className="sidebar-brand">
        <span className="sidebar-mark">RC</span>
        <span>
          <strong>Recsys Analysis</strong>
          <em>Streaming pipeline</em>
        </span>
      </div>
      <div className="sidebar-group">{link("/", "Overview")}</div>
      {GROUPS.map(({ key, label }) => (
        <div className="sidebar-group" key={key}>
          <span className="sidebar-label">{label}</span>
          {Object.entries(SECTIONS)
            .filter(([, spec]) => spec.group === key)
            .map(([section, spec]) => link(SECTION_ROUTE[section], spec.label))}
        </div>
      ))}
    </nav>
  );
}
