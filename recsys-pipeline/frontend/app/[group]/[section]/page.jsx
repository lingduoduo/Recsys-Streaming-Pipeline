import { notFound } from "next/navigation";
import data from "../../../data/dashboard.json";
import { GROUPS, SECTIONS } from "../../../components/groups";
import { SECTION_COMPONENTS } from "../../../components/section-registry";

// Every valid group/section pair, so all thirteen prerender as static content.
export function generateStaticParams() {
  return Object.entries(SECTIONS).map(([section, { group }]) => ({ group, section }));
}

export default async function Page({ params }) {
  const { group, section } = await params;
  const spec = SECTIONS[section];
  if (!spec || spec.group !== group) notFound();

  const Component = SECTION_COMPONENTS[section];
  const groupLabel = GROUPS.find((entry) => entry.key === group)?.label ?? group;

  return (
    <article className="section-page">
      <header className="page-header">
        <span className="eyebrow">{groupLabel}</span>
        <h1>{spec.label}</h1>
        <p className="page-description">{spec.description}</p>
      </header>
      <Component data={data[section]} />
    </article>
  );
}
