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
