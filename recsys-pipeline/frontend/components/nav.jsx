"use client";

import { usePathname } from "next/navigation";
import { ROUTES } from "./groups";

// The only client component in the app: it needs the current path to mark the active
// link. Every page and section stays a server component.
export function Nav() {
  const pathname = usePathname();
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
