# Move Frontend Under `recsys-pipeline` Design

## Goal

Make `recsys-pipeline/frontend/` the single canonical location for the Next.js
analysis dashboard after moving it from the repository root.

## Scope

Update active code and user-facing documentation that still refers to the old
root-level `frontend/` directory. This includes repository navigation,
ignore rules, executable scripts, operational documentation, and command
examples inside the moved frontend.

Historical `.superpowers` specifications and implementation plans remain
unchanged because they document the repository layout that existed when those
plans were written.

## Path Contract

- Commands documented from the repository root use
  `recsys-pipeline/frontend/...`.
- Commands documented or executed from `recsys-pipeline/` use `frontend/...`.
- Scripts resolve paths according to their existing working-directory contract
  and write the dashboard snapshot to
  `recsys-pipeline/frontend/data/dashboard.json`.
- No root-level compatibility symlink, duplicate directory, or dual-path
  discovery logic is introduced.

## Changes

1. Preserve the user's filesystem move from `frontend/` to
   `recsys-pipeline/frontend/`.
2. Update active root and pipeline READMEs, architecture documentation, and the
   moved frontend README and exporter examples.
3. Update `.gitignore` so generated Python cache files remain ignored at the
   new location.
4. Update `run-movie-category-sim.sh` so dashboard export uses the new canonical
   location from the script's execution context.
5. Search active files for stale root-level frontend references, excluding
   `.git`, dependencies, and archival `.superpowers` documents.

## Error Handling and Compatibility

The change intentionally fails fast if callers continue using the old root
path. Keeping one canonical location prevents commands from silently reading or
writing stale dashboard snapshots. Existing frontend runtime behavior and data
formats do not change.

## Verification

- Confirm Git recognizes the frontend files as moves rather than duplicated
  tracked content.
- Verify no stale active path references remain.
- Run the script integration tests that cover dashboard export commands.
- Run the frontend validation and production build from
  `recsys-pipeline/frontend/`.
- Run `git diff --check`.
