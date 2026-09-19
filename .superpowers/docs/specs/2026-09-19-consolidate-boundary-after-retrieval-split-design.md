# Consolidate the repository boundary after the retrieval split design

## Problem and decision

PR #241 made the Java retrieval service portable, #242 deleted it, #243 corrected five stale
references, #244 fixed the route contract and deleted the MDP orphan chain, #245 pinned the four
cross-repository contract files, and #246 recorded that the service-side provenance README was
dropped. The structural work is finished. This change is about what the documentation still says
that is false, what it links to that does not exist, and the one guard that would have caught both.

An audit of the 20 live markdown files found four things.

**Ten inbound anchors are broken, two of them by #244 itself.** That commit rewrote all ten headings
in `docs/recommendation_architecture/API.md` to carry the new `/api/v1/retrieval` prefix -- `## GET
/users/{user}/profile` became `## GET /api/v1/retrieval/users/{user}/profile`, `## GET
/actuator/profile-audit` became `## GET /api/v1/retrieval/profile-audit` -- and updated the prose
paths in `recsys-pipeline/README.md` that described them. It did not update the two anchors in that
same prose, at lines 238 and 243, which still read `#get-usersuserprofile` and
`#get-actuatorprofile-audit`. Both now resolve to nothing.

The other eight are older and share one sentence. Every one of the nine flow docs opens with "For
the complete local startup sequence, follow the [root quick start]", and eight of them point that
link at `../../../README.md#recsys-pipeline`. From `recsys-pipeline/docs/recommendation_flows/`,
`../../../` is the repository root, whose README has never had a heading yielding that anchor --
confirmed against the pre-split README at `885c9b9`. The ninth, `9_Track_Metrics.md`, points at
`../../../README.md#canonical-finite-local-workflow`, which is the right heading text at the wrong
depth: that heading is at `recsys-pipeline/README.md:744`, which is `../../`, not `../../../`.
`Data_Pipeline.md:473` makes the identical depth error. `Analysis_Report.md:5` gets it right, so the
correct form is already in the tree to copy.

**The route migration is incomplete, and #244 claimed it was not.** That commit's message is "state
the repository boundary once and correct every stale endpoint path". Twenty bare paths survive it:
two in `8_Store_Context.md`, four in `6_Predicting_Scoring.md`, seven in `9_Track_Metrics.md`, one in
`Analysis_Report.md:140`, and five in `recsys-pipeline/README.md`. A reader who copies `curl
localhost:8080/recommend/user_1` out of any of them gets a 404 from the current backend -- the same
failure #244 fixed in the simulation script and left standing in the prose around it.

**One claim is false.** `1_Query_Hydration.md:28` says "The in-repo serving side-effect writer
maintains `user:{id}:served_history` and `user:{id}:impressions` after a request selects at least
one item." That writer was part of the service and left with it. `served_history` now appears in
exactly two files repository-wide, `1_Query_Hydration.md` and `8_Store_Context.md`, both
documentation: there is no writer, no reader, and no test for it in this checkout. The sentence
asserts this repository does something it cannot do.

**Thirty Java class names are documented here and compiled nowhere here.**
`HybridRecommendationService`, `TopKScoreSelector`, eleven query hydrators, three filters, and the
rest are named across the flow docs as statements of present fact. They are the other repository's
to rename, and nothing in either repository would notice when it happens.

Fix all four. Correct the anchors and the paths, correct the false claim, and mark the boundary in
the eight flow docs that do not mark it -- using the treatment `7_Shuffling.md` already carries.
Then add the guard.

## What this does and does not claim

It claims that after this change every relative link in every live markdown file resolves, to a file
that exists and to an anchor that exists, and that a test fails if that stops being true. It claims
every retrieval endpoint named in live documentation carries the prefix at which the backend
actually serves it, with two named exceptions, and that a test fails if a bare one appears.

It does not claim the class names are correct. They were correct when the service lived here and
they are unverifiable now: this repository has no Java to compile them against and no view into the
other repository's renames. The change marks them as as-of-the-split rather than pretending to
currency, which is the only honest option that keeps them. Deleting them was considered and rejected
-- they are how a reader connects a Redis key documented here to the consumer that reads it -- but a
reader must be told they may have drifted.

It does not claim the nine-stage narrative describes code in this repository. Eight of the nine
stages run entirely in `lingduoduo/Recsys-Backend-Service`. What this repository owns, and what
stays authoritative in those documents, is the Redis key contract: which streaming job writes which
key, and which keys have no writer here at all.

It makes no claim about `frontend/data/dashboard.json`, which is not touched, and none about the
two ASCII diagrams beyond the note added above them.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Three commits, reviewable independently and in this order: the corrections, the boundary notes,
  the guard. The guard is last because it must pass against the first two.
- Documentation and one new test file only. No `.scala`, no `.py` under `services/`, no `.jsx`, no
  `.json`, no workflow, no script. The frontend is not rebuilt because nothing it reads changes.
- The two aligned ASCII blocks in `recsys-pipeline/README.md` -- the Experiment Pipeline block at
  76-92 and the architecture block at 696-700 -- keep their arrow alignment. They get a
  prefix note above the fence; the paths inside them are not expanded. Inlining 18 characters into
  `GET /recommend/{user} ──►` would shift every arrow on the line.
- The heading `## Retrieval Service Configuration` in `recsys-pipeline/README.md` and the heading
  `## Canonical finite local workflow` at line 744 keep their exact text. Inbound links depend on
  both anchors, and this change adds ten more to the second.
- `## Repository boundary` in the root README stays canonical. The eight flow docs link to it; none
  of them restates it.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten, and the
  `.py`/`.scala` provenance comments naming Java classes are left alone.
- The ten `API.md` headings are not renamed again. The anchors move to them, not them to the
  anchors: they match the routes the backend serves, which is the property worth keeping.

## Implementation

### Commit 1 -- the corrections

Retarget the two API anchors in `recsys-pipeline/README.md` to the headings #244 created:
line 238 to `#get-apiv1retrievalusersuserprofile`, line 243 to
`#get-apiv1retrievalprofile-audit`. The prose around them already names the new paths and does not
change.

Retarget the ten flow-and-architecture links to `../../README.md#canonical-finite-local-workflow`
-- eight from `#recsys-pipeline` in flow docs 1 through 8, plus `9_Track_Metrics.md:8` and
`Data_Pipeline.md:473`, which have the right anchor at the wrong depth. The link text becomes
`canonical finite local workflow`, because the target is no longer the root README and calling it
"root quick start" is what made the wrong depth plausible.

Add the prefix to the sixteen bare paths that sit in prose: two in `8_Store_Context.md` (16, 25),
four in `6_Predicting_Scoring.md` (17, 26, and two on 28), eight in `9_Track_Metrics.md` (11, 12,
21, 28, 29, 32, 67, 71), one in `Analysis_Report.md` (140), and one in `recsys-pipeline/README.md`
(231). The remaining four of the twenty are inside the two ASCII blocks -- `README.md` 79, 83, 89
and 698 -- and are not expanded. Those blocks get the line `Paths are relative to the service prefix
`/api/v1/retrieval`.` above the fence instead.

Delete the Maven line from the root README's Requirements list. The boundary section thirty lines
above it already says "Maven is a prerequisite of that repository, not of this checkout. Nothing
here has a `pom.xml`" -- so the Requirements entry is not merely stale, it is a second, weaker
statement of a fact #244 made canonical at line 30. Listing another repository's build tool under
this repository's requirements is the leftover; the sentence that qualifies it away is the evidence
it should not be in the list. The other four live mentions stay: the boundary table at line 16 and
that sentence at line 30 are about the other repository by design, `recsys-pipeline/README.md:190`
tells a reader what to run alongside this checkout, and the architecture diagram labels the service
zone.

### Commit 2 -- the boundary notes and the false claim

Rewrite the `1_Query_Hydration.md` bullet. The in-repo side-effect writer is gone, so
`user:{id}:served_history` and `user:{id}:impressions` move into the bullet below, which already
lists the keys the service reads and this repository does not write. The merged bullet says the
service maintains them and that this checkout has neither writer nor reader -- which is what
`grep -rn served_history` shows.

Give flow docs 1 through 6, 8 and 9 the treatment `7_Shuffling.md` already has, as a short block
under the References line: the stage runs in the retrieval service, now in
`lingduoduo/Recsys-Backend-Service`, with a link to `#repository-boundary`; the Redis key contract
below is owned by this repository and authoritative; the class and bean names are as of the split
and may have been renamed there. One block, same wording, eight files. `7_Shuffling.md` gets the
same block for consistency, replacing the inline sentences that currently carry the same three
facts in prose -- except the property-specific half, which no other document carries: that
`recsys.candidate-generation.top-n-randomization-pool` is declared in the service's own
`application.yml` and that `TopKScoreSelector` does not read it. That stays in the body. Only the
clause naming the repository and linking the boundary is removed from the prose, because the block
above now carries it.

### Commit 3 -- the guard

Add `integration-tests/test_doc_links.py`, following the conventions of
`test_retrieval_service_extracted.py`: `GIT_ROOT` from `parents[2]`, the same `HISTORICAL_PREFIXES`
and `SKIP_DIRS` exclusions, a module docstring stating what is deliberately not scanned.

Two tests. The first walks every live markdown file, extracts every `](target)` whose target is not
`http`, `mailto:` or a bare fragment, and asserts the file resolves and -- when the target is a
`.md` and carries a fragment -- that the fragment matches a GitHub-style slug of one of its
headings. The slug function lowercases, strips backticks, asterisks, underscores, brackets and
parentheses, drops every remaining character outside `[a-z0-9 -]`, and replaces spaces with hyphens.
The second asserts no live markdown file names a retrieval endpoint without the prefix, matching
`(GET|POST) /(recommend|feedback|metrics|predict|embedding|users)`, with the two ASCII blocks
allowlisted by the note that precedes them.

Both assertions fail with the offending file, line and target listed, because a guard that says only
"a link is broken" across 20 files costs more to act on than it saves.

## Validation and acceptance

1. The link test passes: every relative link in the 20 live markdown files resolves to an existing
   file, and every `.md` fragment to an existing heading. Before this change it reports 12
   failures; after, zero.
2. The prefix test passes: `grep -rnoE '(GET|POST) /(recommend|feedback|metrics|predict|embedding|users)'`
   over live documentation returns only the two allowlisted ASCII blocks.
3. `grep -rn 'in-repo serving side-effect writer'` returns nothing.
4. Each of the nine flow docs contains exactly one boundary block naming
   `Recsys-Backend-Service` and linking `#repository-boundary`.
5. The root README's Requirements list names no Maven. The four other live mentions are unchanged,
   and `spark-analysis/Algebird.md:37` (Maven Central, unrelated to the service) is not touched.
6. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline on this branch point is
   561 passed, 2 skipped. This change adds two tests and deletes none, so the expected result is
   563 passed, 2 skipped, zero failures.
7. `git diff --stat` touches only `.md` files and one new `.py` test. No `.scala`, `.jsx`, `.json`,
   `.sh`, `.yml`.
8. `git diff --check` clean.

## Limits

The prefix test pins the string, not the route. It fails when documentation names
`GET /recommend/{user}`, and passes when documentation names
`GET /api/v1/retrieval/recommend/{user}` -- including on the day the backend moves that route to
`/api/v2/retrieval`, after which every prefixed path in this repository is wrong and the test is
green. Only a live probe against a running backend can catch that, which is the limit #244 recorded
for the simulation's drift branch and this change does not lift. What the test catches is the
regression that actually happened twice: a path written without the prefix at all.

The link test cannot see outside this repository. Links to
`github.com/lingduoduo/Recsys-Backend-Service` are skipped as `http` targets, so a file renamed in
that repository breaks a link here silently. Fetching them would make the suite depend on the
network and on that repository's default branch, which is a worse trade than the gap.

The boundary blocks are prose, and nothing tests that they stay accurate beyond the presence check
in acceptance item 4. If a stage is later moved back into this repository, its block becomes the new
false claim, in the same way `1_Query_Hydration.md:28` is the false claim this change removes. The
presence check would still pass.

Caveating the class names does not stop them drifting; it stops a reader trusting them. The drift
itself remains detected by nothing in either repository, which is the standing consequence of
splitting a documented interface across two repositories whose CI systems cannot see each other --
recorded in `schemas/CONTRACTS.md` for the four pinned files and true of everything not pinned.
