# Retire stale service references design

## Problem and decision

PR #242 removed the Java retrieval service from this repository and rewrote every document that
named its location. Five references survived, and they survived for one reason: none of them
contains the string the excision searched for. `test_retrieval_service_extracted.py` greps live
documents for `java-retrieval-service`, so a sentence that describes the service without naming
its path is invisible to it. Two such sentences were already caught by reading rather than
grepping during #242 — a claim that the simulation "starts the service", and a root-README line
asserting this repository contains a Spring Boot service. These five are the rest of that class.

They are not cosmetic. Each one tells a reader something false about the repository they are
looking at:

`README.md:57` lists `Maven 3.8+ (retrieval service)` among the requirements of this checkout, and
`recsys-pipeline/README.md:109` lists `Maven 3.8+` inline among Java, Spark, sbt, Docker Compose
and Python as Quick Start prerequisites. No Maven build remains here; #242 deleted the only
`pom.xml`. A reader installs a build tool this repository cannot use.

`recsys-pipeline/scripts/run-movie-category-sim.sh:246` prints the banner
`==> MDP POLICY EVALUATION (uniform vs greedy over the generated ratings)` above three lines that
can only ever report the card unmeasured, because the evaluator that produced `mdp_eval.csv` left
with the service. The banner promises work the script can no longer do.

`recsys-pipeline/docs/recommendation_architecture/API.md:9` links
`[retrieval-service workflow](../../../README.md#3-experiment-pipeline--retrieval-service-8080)`.
That link is broken twice over. `../../../` resolves to the repository root rather than
`recsys-pipeline/`, so it lands on the wrong README — a defect that predates #242 — and the anchor
`#3-experiment-pipeline--retrieval-service-8080` does not exist in either file. The section it
wants is `## Optional reference: experiment pipeline — retrieval service :8080` at
`recsys-pipeline/README.md:978`.

`recsys-pipeline/docs/recommendation_flows/7_Shuffling.md:15` says `RECSYS_RANDOMIZATION_POOL`
"is declared in `application.yml`" with no qualification. That file now lives in
`lingduoduo/Recsys-Backend-Service`; a reader searching this checkout for it finds nothing.
`recsys-pipeline/README.md:334` already qualifies the identical reference, so this is an
inconsistency as much as an inaccuracy.

Fix all five as documentation edits. Requalify the two Maven lines rather than deleting them,
because Quick Start Step 2 still instructs a reader to clone and run
`lingduoduo/Recsys-Backend-Service`, which genuinely is a Spring Boot/Maven project — Maven is a
prerequisite of that repository, not of this one, and the current wording fails to say which.

## What this does and does not claim

It claims that after this change no live document or script in this repository states something
false about where the retrieval service lives or what this checkout can build, for the five
references named above.

It does not claim the grep-based guard has been made able to catch this class of defect. It
cannot: `test_retrieval_service_extracted.py` matches a literal identifier, and prose that
describes the service without naming its path will always slip through. The only detection method
for identifier-free staleness is reading, and this change is the product of one such read, not of
a new automated check. A future edit could reintroduce the same class.

It does not claim the five are exhaustive beyond the sweep that produced them. The sweep searched
live `.md`, `.html` and `.sh` files for claims about starting or building the service, for
`application.yml`, for `mvn`/Maven, and for the MDP evaluator. A differently-worded stale claim
could remain.

It makes no behavioral change. No `.py`, `.scala`, `.yml` or configuration file is touched; the
only non-document edit is one `echo` banner in a shell script, and the lines beneath it that
decide the card's value are untouched.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Documentation and one shell-script banner only. No test, source, workflow or dependency file changes.
- The heading `## Retrieval Service Configuration` in `recsys-pipeline/README.md` keeps its exact
  text: `docs/recommendation_flows/3_Cold_Start.md:45` and `6_Predicting_Scoring.md:75` link to its anchor.
- The `SERVICE BURST` block of `run-movie-category-sim.sh` is not touched. Two existing tests split
  the script on that marker and assert the block contains `|| true` or `continue` and no `set -e`.
- Behavioral statements that remain true stay as written — in particular `7_Shuffling.md`'s claim
  that `TopKScoreSelector` does not read the randomization property.
- Historical records under `.superpowers/docs/**`, `docs/superpowers/**` and `.planning/**` are not
  rewritten, and `.py`/`.scala` provenance comments naming the Java classes that produced fixtures
  are left alone.
- The Python suite must remain at 558 passed, 1 skipped.

## Implementation

**The two Maven prerequisites.** In `README.md:57`, requalify the bullet so it attributes Maven to
the separate repository rather than to this checkout. In `recsys-pipeline/README.md:109`, remove
`Maven 3.8+` from the inline list of what this checkout needs and attach it instead to the step
that needs it, which is Step 2's clone-and-run of the backend repository.

**The MDP banner.** Reword the `echo` at `run-movie-category-sim.sh:246` so it states the card is
not measured from a pipeline-only checkout instead of announcing an evaluation. The comment beneath
it already explains why `MDP_CSV` stays defined; the banner should agree with that comment rather
than contradict it.

**The API.md link.** Correct the path from `../../../README.md` to `../../README.md` and the anchor
to `#optional-reference-experiment-pipeline--retrieval-service-8080`, matching the heading at
`recsys-pipeline/README.md:978`. The double hyphen is correct: the em dash in the heading
contributes nothing while the spaces around it each become a hyphen, which is why the original
link had `pipeline--retrieval` too.

**The 7_Shuffling.md reference.** Qualify `application.yml` the way `recsys-pipeline/README.md:334`
already does — the retrieval service's own file, now in `lingduoduo/Recsys-Backend-Service` —
and leave the sentence's behavioral half untouched.

## Validation and acceptance

1. No live document or script contains the five stale strings: `Maven 3.8+ (retrieval service)`,
   `sbt, Maven 3.8+`, `MDP POLICY EVALUATION (uniform vs greedy`, `../../../README.md#3-experiment`,
   and an unqualified "declared in `application.yml`".
2. `recsys-pipeline/docs/recommendation_architecture/API.md`'s link resolves: the target file exists
   at the corrected relative path and contains a heading whose GitHub anchor matches.
3. `bash -n recsys-pipeline/scripts/run-movie-category-sim.sh` is clean, and the script's
   `SERVICE BURST` block is byte-identical to before.
4. `python3 -m pytest -q` from `recsys-pipeline` reports 558 passed, 1 skipped — unchanged. This is
   the real gate: several tests assert README content, and a reword can break a test in a file this
   change never opens.
5. `grep -rn 'java-retrieval-service'` still returns only historical records and `.py`/`.scala`
   provenance comments, unchanged by this work.
6. `git diff --check` clean.

## Limits

This is a read-driven fix for a class of defect no test can catch, which means it offers no
protection against recurrence. Anyone adding prose about the retrieval service can reintroduce
exactly this staleness and both the hygiene test and CI will stay green. The honest mitigation is
not a better grep but an awareness that the guard covers identifiers and not claims; that awareness
belongs in whatever review touches these documents next.

The `API.md` depth bug is fixed opportunistically. It predates the extraction entirely — the link
was already landing on the wrong README before #242 — and strictly it is unrelated to the service
move. It is included because the same line needs its anchor corrected anyway, and leaving a known
broken path in a line being edited would be worse than fixing it.

Requalifying rather than deleting the Maven prerequisites keeps a tool in the requirements list
that this repository never invokes, which a reader skimming only that list may still find
misleading. The alternative — deleting both lines — would leave a reader who follows Quick Start
Step 2 to discover the missing tool at the point of failure. Attribution was judged the smaller
harm, and it is the reason the wording, not just the presence, matters here.
