# mojito pre-commit gate (mojito-async)

Every commit runs `precommit/gate.sh`:

- **Tier 0 — validators (always block):** staged whitespace errors, build
  artifacts/junk staged, unresolved conflict markers.
- **Tier 1 — suite (blocks unless known-red):** `precommit/run-suite.sh` when
  present. The A0 spike lanes wire the MAVS-style suites (A0-T1 … A0-T16
  semantic, race hooks, allocation/latency measurements) into that script as
  they land; the A1.1 runtime suite (`mojito_async/test/run.sh`) and the
  AOT benches — `bench/run.sh` (A2.8 scheduler-scale) and
  `bench/run_timer_scale.sh` (A6.5 F6 timer-scale, issue #88) — are wired
  in the same way: each bench's PASS/RED verdict is a hard gate condition,
  its numeric JSONL rows are a report artifact (drift tolerated, never a
  flaky pass/fail threshold).
- **TDD:** intentional red tests are allow-listed in
  `precommit/known-red.tsv` (with tracking issue) and removed when green.
- **`MOJITO_GATE_FAST=1`** skips the suite step.

## Install

```sh
precommit/install-hooks.sh        # == git config core.hooksPath .githooks
```

## Host rules (same as claude/OX agents on this host)

- The gate never deletes or modifies anything outside the workspace/ tree.
- No gate step may invoke tools that delete outside workspace/ (e.g.
  `brew untap`, `git clean` of other trees); retirement of anything outside
  workspace/ uses `mv <path> <path>.superseded` and asks first.