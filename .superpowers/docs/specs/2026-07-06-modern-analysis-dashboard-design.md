# Modern Analysis Dashboard Design

## Goal

Refresh the generated analysis dashboard into a modern light product-analytics interface while preserving its self-contained, dependency-free HTML output.

## Visual direction

- Use a soft gray canvas and centered responsive content area.
- Add an indigo gradient hero with the report title, a short description, and a report badge.
- Render each analysis section as a white card with rounded corners, a subtle border, and restrained shadow.
- Emphasize section headlines as compact insight callouts.
- Use an indigo, teal, orange, and rose chart palette with rounded bars and stronger line styling.
- Present unavailable sections as clear amber status panels.

## Tables and charts

- Wrap tables in horizontally scrollable containers for small screens.
- Use sticky table headers, zebra striping, hover states, tabular numeric alignment, and comfortable spacing.
- Make SVG charts responsive while retaining their accessible tooltip titles.
- Keep all existing metrics, ordering, and section content unchanged.

## Architecture

The redesign remains inside `analysis_dashboard_report.py`. Renderer helpers will emit slightly richer semantic wrappers, and `render_html` will provide all CSS inline. There will be no JavaScript, external fonts, CDN assets, or additional runtime dependencies.

## Compatibility and accessibility

- Preserve the standalone `index.html` artifact and current CLI.
- Include a viewport declaration and responsive breakpoints.
- Maintain readable contrast, visible table structure, and reduced-motion-safe styling.
- Continue escaping all data-derived strings.

## Verification

- Extend renderer tests to assert the new structural hooks and responsive metadata.
- Run the focused dashboard test suite.
- Regenerate `report-dashboard/index.html` from the existing simulation data.
- Verify that the served page at `http://localhost:8000` contains every analysis section.
