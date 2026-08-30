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

## Cost tiers (issue #169)

The gate picks a Tier 1 cost tier from what's actually staged, so cost stops
being a reason to reach for `--no-verify`:

| tier | when | what runs |
|---|---|---|
| `hermetic` | every staged path is `*.md` / `docs/**` | gate self-test only |
| `affected` | every staged path is test-only (`mojito_async/test/**`, `spike/colorless_runtime/tests/**`, `bench/**`, or `precommit/known-red.tsv`) | gate self-test, plus only the suites/drivers the diff touches |
| `full` | anything else (runtime code, vendor/, Makefile, the gate itself, a mixed diff, or nothing staged at all) | everything, unscoped |

`MOJITO_GATE_TIER=full|affected|hermetic` overrides the auto-pick. CI always
runs `full` explicitly (a checkout has nothing staged, so it would already
default there, but `ci.yml` sets it anyway so the workflow's intent doesn't
depend on that fallback). This is enforcement-adjacent ergonomics, not the
enforcement itself: the tier only changes what the local hook checks before
a commit lands — CI's `full` run on the pushed branch is what actually gates
the merge (branch protection on `main` requires it).

## Install

```sh
precommit/install-hooks.sh        # == git config core.hooksPath .githooks
```

## The `GATE:` trailer

If you use `git commit --no-verify` (the emergency escape hatch below), add
a trailer to the commit message naming why and what ran instead:

```
GATE: skipped — <reason>. Ran instead: <what you actually verified, e.g.
"mojo run -Xlinker libmojito_spike.dylib mojito_async/test/unit/t50_*.mojo,
PASS">
```

This is not an enforcement mechanism — nothing in git records whether a
hook actually ran, so a trailer is a claim like any commit message, not
proof (issue #169's own finding: two of the four `mojito-sys` remediation
commits that used `--no-verify` were missing exactly this trailer, and one
PR's prose Gate section contradicted its own commits' trailers). Enforcement
lives in CI + branch protection now, not in the local hook or its trailers.
The trailer's job is narrower: it is a breadcrumb for whoever reads the
commit later, so "why was this skipped" doesn't require asking the author.

## Host rules (same as claude/OX agents on this host)

- The gate never deletes or modifies anything outside the workspace/ tree.
- No gate step may invoke tools that delete outside workspace/ (e.g.
  `brew untap`, `git clean` of other trees); retirement of anything outside
  workspace/ uses `mv <path> <path>.superseded` and asks first.