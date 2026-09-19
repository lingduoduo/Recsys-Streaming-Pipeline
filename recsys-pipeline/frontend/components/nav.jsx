"use client";

import { useEffect } from "react";
import { usePathname } from "next/navigation";
import { ROUTES } from "./groups";

// A client component because it needs the current path to mark the active link, and
// because it opens the section a hash points at. Every page and every section stays a
// server component; keyword-report.jsx is the other client component, for its Top-K
// selector.
export function Nav() {
  const pathname = usePathname();

  // Sections are collapsed by default, so a link to #satisfaction would land on a closed
  // row. Open whatever the hash names, on arrival and on every later hash change.
  useEffect(() => {
    const openTarget = () => {
      const target = document.getElementById(decodeURIComponent(window.location.hash.slice(1)));
      if (target instanceof HTMLDetailsElement) target.open = true;
    };
    openTarget();
    window.addEventListener("hashchange", openTarget);
    return () => window.removeEventListener("hashchange", openTarget);
  }, [pathname]);
  return (
    <nav className="nav" aria-label="Dashboard sections">
      {ROUTES.map(({ href, label }) => {
        const active = pathname === href;
        return (
          <a key={href} href={href} className={active ? "nav-active" : undefined}
             aria-current={active ? "page" : undefined}>
            {label}
          </a>
        );
      })}
    </nav>
  );
}
