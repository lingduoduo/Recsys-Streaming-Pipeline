# Pin the Cross-Repository Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pipeline-owned contract comparison that `RecsysEventSchemaDriftTest` in
`lingduoduo/Recsys-Backend-Service` already documents but which was never written, record the
provenance the service's `contracts/` directory is missing, and remove the merged
`standalone-retrieval` worktree.

**Architecture:** A manifest document in this repository names the four shared contract files, their
SHA-256, and the service path holding each copy. A pure-Python test parses that document — rather
than duplicating its table — and enforces it three ways: hashes match, the file set is exactly four,
and, when a service checkout is pointed at by `RECSYS_BACKEND_REPO`, the copies match byte for byte.

**Tech Stack:** Python 3 (pytest, `hashlib`, `pathlib` — no third-party imports), Markdown.

**Spec:** `.superpowers/docs/specs/2026-09-18-pin-cross-repo-contracts-design.md`

## Global Constraints

- Branch and pull request only, in both repositories. Nothing is committed to `master` directly.
- **The four contract files are never modified.** Their bytes are the fixed point of this change;
  any diff touching them is a defect.
- SHA-256 over exact file bytes, no normalization. Current values:
  - `49895bff1fff0f03036723d096b10b2817723dc88aa4db0e7eb4116e3d10ae30` `schemas/recsys-event-v3.avsc`
  - `925e1d3198eb294e652fc7f96827e393d90869e305c6095b241e21bbdee2d6d5` `schemas/fixtures/serving-impression-v3.avro`
  - `013a7481a31c66ef2c22995d989f5ca50991e8a0dc711448d92b48dbb2020fbe` `integration-tests/fixtures/user_profile_v1.json`
  - `9ac9b6c9274eeeb91eca41f8bd63ac65223298309a62b0505e644e2414ed271c` `services/spark-streaming-job/src/test/resources/sequence-schema.json`
- The new test uses only the standard library. `pytest.ini` sets `testpaths = integration-tests`, so
  the test file goes directly under `recsys-pipeline/integration-tests/`.
- The cross-checkout comparison **skips, never fails**, when `RECSYS_BACKEND_REPO` is unset or
  unreadable. A missing sibling repository must never fail this suite.
- The service-side change is documentation only — no Java, no test, no build file.
- Baseline before any change: `python3 -m pytest -q` from `recsys-pipeline` gives
  **559 passed, 1 skipped**. Expected after: **561 passed, 2 skipped** (three new tests, one of which
  skips without `RECSYS_BACKEND_REPO`).
- All pytest commands run from the `recsys-pipeline` directory.

---

### Task 1: The manifest and its enforcement

**Files:**
- Create: `recsys-pipeline/schemas/CONTRACTS.md`
- Test: `recsys-pipeline/integration-tests/test_cross_repo_contracts.py`

**Interfaces:**
- Consumes: nothing.
- Produces: the manifest table format `| \`<pipeline path>\` | \`<sha256>\` | \`<service path>\` |`,
  parsed by `_manifest_rows()` in the test, which returns a list of
  `(pipeline_path: str, sha256: str, service_path: str)` triples. Task 2 consumes the service paths
  as prose only, not programmatically.

- [x] **Step 1: Write the manifest**

Create `recsys-pipeline/schemas/CONTRACTS.md`:

```markdown
# Cross-repository contracts

Four files exist in both this repository and
[lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service).
**This repository is the source of truth**: its producers write the events and profiles the
service reads, so the definition here is canonical and the service's copies are snapshots of it.

The service froze its copies deliberately, so its tests need no pipeline checkout. Its
`RecsysEventSchemaDriftTest` checks its snapshot against its own codec and documents that the
pipeline owns the comparison against the canonical schema. That comparison is
`integration-tests/test_cross_repo_contracts.py`, enforced from the table below.

| Canonical path (this repository) | SHA-256 | Copy in Recsys-Backend-Service |
|---|---|---|
| `schemas/recsys-event-v3.avsc` | `49895bff1fff0f03036723d096b10b2817723dc88aa4db0e7eb4116e3d10ae30` | `src/test/resources/contracts/recsys-event-v3.avsc` |
| `schemas/fixtures/serving-impression-v3.avro` | `925e1d3198eb294e652fc7f96827e393d90869e305c6095b241e21bbdee2d6d5` | `src/test/resources/contracts/serving-impression-v3.avro` |
| `integration-tests/fixtures/user_profile_v1.json` | `013a7481a31c66ef2c22995d989f5ca50991e8a0dc711448d92b48dbb2020fbe` | `src/test/resources/contracts/user_profile_v1.json` |
| `services/spark-streaming-job/src/test/resources/sequence-schema.json` | `9ac9b6c9274eeeb91eca41f8bd63ac65223298309a62b0505e644e2414ed271c` | `src/test/resources/sequence-schema.json` |

## Changing one of these

1. Edit the file here.
2. Copy it to the service path in the right-hand column, in that repository, on its own branch.
3. Update the SHA-256 in this table: `shasum -a 256 <path>` from `recsys-pipeline/`.
4. Land both pull requests. Neither repository's CI can see the other, so nothing will remind you
   about step 2 — the test here fails until step 3 is done, and step 3 is the moment to do step 2.

To check both copies directly, point at a service checkout:

```bash
RECSYS_BACKEND_REPO=~/Git/Recsys-Backend-Service python3 -m pytest \
  integration-tests/test_cross_repo_contracts.py -v
```

Without that variable the cross-checkout comparison skips; the hash check always runs.

## What this does not cover

Byte equality is stricter than wire compatibility: a whitespace-only edit to the `.avsc` leaves the
Avro parsing fingerprint unchanged but fails the hash check. That is intended — it forces a human to
confirm the second copy — but it means these hashes track file identity, not semantic compatibility.

Drift originating in the service is caught only by the opt-in comparison above. Neither repository's
CI has the other checkout, so neither can catch it automatically.
```

- [x] **Step 2: Write the failing test**

Create `recsys-pipeline/integration-tests/test_cross_repo_contracts.py`:

```python
"""The four contract files shared with lingduoduo/Recsys-Backend-Service.

This is the "pipeline-owned contract comparison" that RecsysEventSchemaDriftTest in that
repository documents. It reads schemas/CONTRACTS.md rather than repeating its table, so the
document cannot drift from what is enforced.
"""

import hashlib
import os
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = REPO_ROOT / "schemas" / "CONTRACTS.md"

ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|\s*`([^`]+)`\s*\|$", re.M)


def _manifest_rows():
    """(pipeline path, sha256, service path) for every row of the manifest table."""
    return ROW.findall(MANIFEST.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_every_contract_file_matches_its_recorded_hash():
    for pipeline_path, expected, service_path in _manifest_rows():
        target = REPO_ROOT / pipeline_path
        assert target.exists(), f"{pipeline_path} is listed in CONTRACTS.md but missing"
        assert _sha256(target) == expected, (
            f"{pipeline_path} changed. Recsys-Backend-Service holds a copy at {service_path} "
            f"which must be updated in that repository too; then refresh the hash in "
            f"schemas/CONTRACTS.md."
        )


def test_the_manifest_lists_exactly_the_known_shared_contracts():
    """A new shared contract must be paired here, not added to one repository alone."""
    assert {row[0] for row in _manifest_rows()} == {
        "schemas/recsys-event-v3.avsc",
        "schemas/fixtures/serving-impression-v3.avro",
        "integration-tests/fixtures/user_profile_v1.json",
        "services/spark-streaming-job/src/test/resources/sequence-schema.json",
    }


def test_contract_copies_match_a_real_backend_checkout():
    """Opt-in: the only check here that can catch drift originating in the service."""
    backend = os.environ.get("RECSYS_BACKEND_REPO")
    if not backend or not Path(backend).is_dir():
        pytest.skip("set RECSYS_BACKEND_REPO to a Recsys-Backend-Service checkout to compare copies")

    for pipeline_path, _, service_path in _manifest_rows():
        theirs = Path(backend) / service_path
        assert theirs.exists(), f"{service_path} is missing from {backend}"
        assert theirs.read_bytes() == (REPO_ROOT / pipeline_path).read_bytes(), (
            f"{pipeline_path} and {service_path} have diverged"
        )
```

- [x] **Step 3: Run the test to verify it fails before the manifest exists**

If Step 1 has already been done, temporarily rename the manifest to confirm the test depends on it:

Run: `cd recsys-pipeline && mv schemas/CONTRACTS.md /tmp/ && python3 -m pytest integration-tests/test_cross_repo_contracts.py -q; mv /tmp/CONTRACTS.md schemas/`
Expected: failures citing a missing file, then the manifest is restored.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/test_cross_repo_contracts.py -v`
Expected: 2 passed, 1 skipped — the cross-checkout test skips without `RECSYS_BACKEND_REPO`.

- [x] **Step 5: Prove the hash check actually bites**

Run:
```bash
cd recsys-pipeline
printf '\n' >> schemas/recsys-event-v3.avsc
python3 -m pytest integration-tests/test_cross_repo_contracts.py -q 2>&1 | grep -c 'src/test/resources/contracts/recsys-event-v3.avsc'
git checkout schemas/recsys-event-v3.avsc
```
Expected: a non-zero count — the failure names the service path — and the file is restored
afterwards. Confirm with `git diff --stat schemas/` that nothing remains changed.

- [x] **Step 6: Prove the comparison runs against the real checkout**

Run: `cd recsys-pipeline && RECSYS_BACKEND_REPO=/Users/linghuang/Git/Recsys-Backend-Service python3 -m pytest integration-tests/test_cross_repo_contracts.py -v`
Expected: 3 passed, 0 skipped.

- [x] **Step 7: Run the whole suite**

Run: `cd recsys-pipeline && python3 -m pytest -q`
Expected: **562 passed, 2 skipped**.

- [x] **Step 8: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git add recsys-pipeline/schemas/CONTRACTS.md recsys-pipeline/integration-tests/test_cross_repo_contracts.py
git commit -m "test: build the pipeline-owned contract comparison the service expects

RecsysEventSchemaDriftTest in Recsys-Backend-Service documents that 'the pipeline-owned
contract comparison verifies it against the canonical producer schema'. No such comparison
existed. Four contract files sit in both repositories, byte-identical by timing rather than
by any check, and the service's contracts/ directory carried no provenance at all.

schemas/CONTRACTS.md names this repository as the source of truth and pairs each file with
the service path holding its copy. The test parses that table rather than repeating it, so
editing a contract here fails until the hash is refreshed -- and the failure names the file
in the other repository that must move with it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Remove the merged worktree

`.worktrees/standalone-retrieval` sits on `feat/standalone-retrieval`, merged as PR #241. It is
152 MB and holds the only remaining copy of the excised `java-retrieval-service` and its `pom.xml`
in this checkout. The branch stays on the remote; merged branches are not pruned.

**Files:** none tracked — this changes only the working tree.

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

- [x] **Step 1: Confirm the worktree is clean before removing it**

Run: `cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && git -C .worktrees/standalone-retrieval status --short`
Expected: no output. If anything is listed, stop and report it rather than discarding work.

> **BLOCKED — this is what happened.** The worktree is not clean. It carries an uncommitted
> two-line addition to `.github/workflows/retrieval-service.yml` adding a
> `docker build --tag retrieval-service:ci` step. That workflow file does not exist on `master`;
> it left with the service, and `pipeline.yml` is now the only workflow here. So the change is an
> unfinished idea from the PR #241 standalone-retrieval work, edited against a file this
> repository no longer has. The idea, if still wanted, belongs in Recsys-Backend-Service.
>
> Per this step's own instruction the removal stopped here rather than discarding it. The diff is
> saved to the session scratchpad as `standalone-retrieval-uncommitted.patch`. Steps 2-4 below are
> unrun pending a decision.

- [ ] **Step 2: Confirm the branch is merged and pushed**

Run: `git branch -r --merged origin/master | grep standalone-retrieval`
Expected: `origin/feat/standalone-retrieval` — the work survives removal.

- [ ] **Step 3: Remove it**

Run: `git worktree remove .worktrees/standalone-retrieval && git worktree prune`

- [ ] **Step 4: Verify**

Run: `git worktree list && git branch -r | grep standalone-retrieval`
Expected: only the main checkout is listed, and `origin/feat/standalone-retrieval` still exists.

No commit — nothing tracked changed.

---

### Task 3: Record provenance in the service

**Files:**
- Create: `src/test/resources/contracts/README.md` in `/Users/linghuang/Git/Recsys-Backend-Service`

**Interfaces:**
- Consumes: the service paths from Task 1's manifest, as prose.
- Produces: nothing programmatic.

- [x] **Step 1: Branch in the service repository**

Run: `cd /Users/linghuang/Git/Recsys-Backend-Service && git checkout -b docs/contract-provenance`

- [x] **Step 2: Write the README**

Create `src/test/resources/contracts/README.md`:

```markdown
# Frozen producer contracts

These files are **copies**. They are owned by
[lingduoduo/Recsys-Streaming-Pipeline](https://github.com/lingduoduo/Recsys-Streaming-Pipeline),
whose jobs produce the events and profiles this service consumes. They are frozen here so the
service's tests run without a pipeline checkout — see `ContractFixtures`.

| File | Canonical source in Recsys-Streaming-Pipeline |
|---|---|
| `recsys-event-v3.avsc` | `recsys-pipeline/schemas/recsys-event-v3.avsc` |
| `serving-impression-v3.avro` | `recsys-pipeline/schemas/fixtures/serving-impression-v3.avro` |
| `user_profile_v1.json` | `recsys-pipeline/integration-tests/fixtures/user_profile_v1.json` |

One more shared contract lives outside this directory:
`src/test/resources/sequence-schema.json`, whose source is
`recsys-pipeline/services/spark-streaming-job/src/test/resources/sequence-schema.json`.

`RecsysEventSchemaDriftTest` checks the snapshot above against this service's own codec
fingerprint. The comparison against the canonical pipeline schema is owned by the pipeline, at
`recsys-pipeline/integration-tests/test_cross_repo_contracts.py`, and the pairing is recorded in
`recsys-pipeline/schemas/CONTRACTS.md`.

**Do not edit these files here alone.** Change the canonical copy first, then mirror it here, then
refresh the hash in the pipeline's `CONTRACTS.md`. Neither repository's CI can see the other, so
nothing will catch a one-sided edit automatically.
```

- [x] **Step 3: Verify nothing else changed**

Run: `cd /Users/linghuang/Git/Recsys-Backend-Service && git status --short && git diff --check`
Expected: one untracked README and no whitespace errors. No `.java`, `.xml` or test file appears.

- [x] **Step 4: Commit and push**

```bash
cd /Users/linghuang/Git/Recsys-Backend-Service
git add src/test/resources/contracts/README.md
git commit -m "docs(contracts): name Recsys-Streaming-Pipeline as the source of these snapshots

RecsysEventSchemaDriftTest documents that 'the pipeline-owned contract comparison verifies
it against the canonical producer schema', but this directory recorded nothing about where
its files came from, and the pipeline had no such comparison. It does now. Record which
pipeline path each snapshot mirrors, including sequence-schema.json one directory up, and
point at the pairing that enforces it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
git push -u origin docs/contract-provenance
```

---

## Final verification

- [x] `cd recsys-pipeline && python3 -m pytest -q` → **561 passed, 2 skipped**
- [x] `RECSYS_BACKEND_REPO=/Users/linghuang/Git/Recsys-Backend-Service python3 -m pytest integration-tests/test_cross_repo_contracts.py -q` → 3 passed
- [x] `git diff --stat` in the pipeline shows **no change** to any of the four contract files
- [ ] `git worktree list` → only the main checkout — **blocked, see Task 2**
- [x] `git branch -r | grep standalone-retrieval` → still present on the remote
- [x] `git diff --check` clean in both repositories
- [x] Two pull requests open: contracts + test here, provenance README in the service

## Correction to this plan, as executed

The Global Constraints originally predicted **562 passed, 2 skipped**. That was bad arithmetic —
it counted the opt-in comparison as both passing and skipping. Three new tests against a 559/1
baseline give two more passes and one more skip: **561 passed, 2 skipped**, which is what ran.
Corrected above and in the spec.
