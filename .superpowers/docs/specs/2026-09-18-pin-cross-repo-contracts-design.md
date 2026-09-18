# Pin the cross-repository contracts design

## Problem and decision

The retrieval-service migration is complete. `lingduoduo/Recsys-Backend-Service` merged its
consolidation (PR #332), gates a retrieval suite in CI, and holds 118 retrieval sources. PR #244
corrected this repository's route contract. What the migration left behind is a *payload* contract
split across two repositories with nothing holding the halves together.

Four files exist in both checkouts and are byte-identical today:

| File | Canonical location here | Copy in the service |
|---|---|---|
| `recsys-event-v3.avsc` | `schemas/` | `src/test/resources/contracts/` |
| `serving-impression-v3.avro` | `schemas/fixtures/` | `src/test/resources/contracts/` |
| `user_profile_v1.json` | `integration-tests/fixtures/` | `src/test/resources/contracts/` |
| `sequence-schema.json` | `services/spark-streaming-job/src/test/resources/` | `src/test/resources/` |

The service's `src/test/resources/contracts/` directory was created on the migration date and
carries no provenance of any kind -- no README, no comment, nothing naming where the files came
from.

The decisive detail is in the service's own test. `RecsysEventSchemaDriftTest` is documented:

> This frozen producer-contract snapshot stays in the service so its tests do not require the
> pipeline checkout. **The pipeline-owned contract comparison verifies it against the canonical
> producer schema.**

That comparison does not exist. This repository contains no test that reads the service's copies,
compares them, or even names them. The service's test is internally consistent -- it checks its
frozen snapshot against its own codec's fingerprint -- but nothing anywhere ties that snapshot to
the schema this repository's producers actually write. Each repository's design assumes the other
end is covered. Neither end is.

Build the comparison the service already expects, and record the provenance the service is missing.

## What this does and does not claim

It claims that after this change, editing any of the four contract files in this repository fails a
test until the editor updates a manifest, and that the failure names the service path holding the
other copy. The intent is not detection for its own sake but forcing a moment of deliberation at
the point where the two copies would otherwise silently diverge.

It claims that when a service checkout is available at `RECSYS_BACKEND_REPO`, the same test
compares the four files against it directly and fails on real divergence.

It does not claim drift is caught in CI. The manifest check runs everywhere, but it only detects
edits made *in this repository*. Divergence originating in the service -- someone editing
`src/test/resources/contracts/recsys-event-v3.avsc` alone -- is caught only by the opportunistic
comparison, which runs only when someone has both checkouts and sets the variable. Neither
repository's CI has the other, so neither can catch it. This is the same honest limit PR #244
recorded for route drift, and the same reason a checked-in route table was rejected there.

It does not claim byte-equality is the right notion of compatibility. It is stricter. Avro wire
compatibility turns on the parsing fingerprint, which is insensitive to whitespace and field
ordering, so a formatting-only edit to the `.avsc` would fail the pin while remaining perfectly
compatible. That strictness is deliberate: the guard's purpose is to make a human confirm the
second copy, and a formatting edit that reaches one copy and not the other is exactly the state
worth interrupting. But the guard is about file identity, not semantic compatibility, and it should
not be read as the latter.

It makes no behavioral change. No production code path, Spark job, script or serving route is
touched. The four contract files themselves are not edited -- their bytes are the fixed point the
whole change is built around.

## Global constraints

- Branch and pull request only in both repositories. Nothing is committed to `master` directly.
- The four contract files are not modified. Any diff touching their bytes is a defect in this change.
- The manifest records SHA-256 over exact file bytes, with no normalization, and the current values
  are:
  - `49895bff1fff0f03036723d096b10b2817723dc88aa4db0e7eb4116e3d10ae30` `schemas/recsys-event-v3.avsc`
  - `925e1d3198eb294e652fc7f96827e393d90869e305c6095b241e21bbdee2d6d5` `schemas/fixtures/serving-impression-v3.avro`
  - `013a7481a31c66ef2c22995d989f5ca50991e8a0dc711448d92b48dbb2020fbe` `integration-tests/fixtures/user_profile_v1.json`
  - `9ac9b6c9274eeeb91eca41f8bd63ac65223298309a62b0505e644e2414ed271c` `services/spark-streaming-job/src/test/resources/sequence-schema.json`
- The new test is pure Python with no third-party imports, so it runs in the existing suite without
  touching dependencies. `pytest.ini` sets `testpaths = integration-tests`; the test goes there.
- The opportunistic comparison SKIPS, never fails, when `RECSYS_BACKEND_REPO` is unset or does not
  point at a readable checkout. A missing sibling repository must never fail this suite.
- The service-side change is documentation only: no Java, no test, no build file.
- `.worktrees/standalone-retrieval` is removed. Its branch `feat/standalone-retrieval` is merged and
  stays on the remote, so nothing is lost; merged branches are NOT pruned.

## Implementation

**The manifest.** `recsys-pipeline/schemas/CONTRACTS.md` is a document, not a data file, because its
job is as much to explain the coupling as to record hashes. It states that this repository is the
source of truth, lists the four files with their SHA-256 and the exact service path that must match,
explains that the service froze its copies so its tests need no pipeline checkout, and tells an
editor what to do: update both copies, then update the hash here.

**The test.** `recsys-pipeline/integration-tests/test_cross_repo_contracts.py` parses the manifest
rather than duplicating the table, so the document cannot drift from what is enforced. Three tests:

1. Every listed file exists and hashes to its recorded value. On failure the message names the
   service path from the manifest, so the person who broke it is told where the other copy is.
2. The manifest lists exactly the four files, guarding against a contract being added to one
   repository and quietly left out of the pairing.
3. When `RECSYS_BACKEND_REPO` names a readable directory, each file is compared byte-for-byte with
   the service path recorded in the manifest; otherwise the test skips with a reason naming the
   variable.

**The service pointer.** A `README.md` in `src/test/resources/contracts/` naming
`lingduoduo/Recsys-Streaming-Pipeline` as the source of each file, its canonical path there, and the
fact that the pipeline holds the comparison. This is what makes `RecsysEventSchemaDriftTest`'s
Javadoc locatable instead of aspirational. `sequence-schema.json` sits one directory up, outside
`contracts/`, so the README covers it explicitly by path rather than by implication.

**The worktree.** `git worktree remove` the merged `standalone-retrieval` tree, which holds the only
remaining copy of the excised `java-retrieval-service` in this checkout.

## Validation and acceptance

1. `git diff --stat` shows no change to any of the four contract files.
2. `python3 -m pytest -q` from `recsys-pipeline` reports 561 passed, 2 skipped -- the 559/1 baseline plus two passing tests and one that skips.
3. Mutating any contract file by one byte makes test 1 fail, and the failure text contains the
   service path for that file. Revert after checking.
4. Removing a row from the manifest makes test 2 fail.
5. With `RECSYS_BACKEND_REPO` unset, test 3 reports skipped, not passed and not failed.
6. With `RECSYS_BACKEND_REPO=/Users/linghuang/Git/Recsys-Backend-Service`, test 3 passes against the
   real checkout.
7. `git worktree list` shows only the main checkout, and `git branch -r` still lists
   `origin/feat/standalone-retrieval`.
8. `git diff --check` clean in both repositories.

## Limits

The manifest catches edits in this repository and nothing else. The asymmetry is real and worth
stating plainly: this repository is declared the source of truth, but the copy that can be changed
without anyone noticing is the one in the service, and the guard added here is weakest exactly
there. Closing that would require the service's CI to fetch this repository, which makes every
service build depend on a second checkout -- a cost neither repository currently pays for a set of
files that has not yet drifted once.

The opportunistic comparison is opt-in through an environment variable, which means in practice it
runs when someone remembers. It is included because it is nearly free and because it is the only
mechanism here that can catch service-originated drift at all, not because it can be relied on.

Recording hashes in a document a human must update by hand introduces a small failure mode of its
own: an editor who updates the contract and the hash but forgets the service copy satisfies every
test in this repository. The manifest's wording is the only thing standing against that, which is
why the document explains the coupling rather than merely tabulating it.
