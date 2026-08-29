# ADR-015 — Timer wheel gate: hold the min-heap (issue #87, TQ11)

**Status:** Accepted (HOLD) — gate #1, 2026-08-28.
**Part of:** #6 (timer scheduling). **Depends on:** #36 (A1.4 `TimerHeap`),
#88 (A6.5 F6 bench extension, in flight concurrently with this ADR).
**Resolves:** spec §117 open technical question 11 — "At what point should
timer heaps become hierarchical wheels?"

## Context

Spec §31 requires monotonic time, O(log n) as "acceptable for initial
correctness," cancellation, generation-token stale-timer suppression,
configurable-tolerance coalescing, and owner-worker-only wake — and says
"at scale, prefer a hierarchical timer wheel or sharded wheel." Issue #87
does not mandate the swap; it mandates *deciding*, on published F6
evidence, whether the swap is warranted yet, and fixing the concrete
numeric gate so future re-evaluations are mechanical rather than
re-litigated from scratch.

## Interface contract the wheel (if ever built) MUST preserve

Any replacement selected at the module-factory seam (never a vtable/typed
callback — b2 has no function-typed struct fields) must implement exactly
the `TimerHeap`-facing surface consumed by `timer_service.mojo` and
`sleep.mojo`, so the swap is invisible to every caller:

- `arm(mut self, id: Int, tcbaddr: Int, deadline: UInt64) raises -> Int` —
  grants and returns a fresh generation token.
- `cancel(mut self, id: Int) -> Bool` — whole-timer removal.
- `cancel_token(mut self, id: Int, gen: Int) -> Bool` — exact-generation
  removal; a stale token must never cancel a newer arm.
- `live_gen(self, id: Int) -> Int` — most-recent granted generation (0 =
  none); the stale-suppression predicate `is_stale_against`.
- `min_deadline(self) -> UInt64` — earliest pending deadline, or the
  `NO_DEADLINE` sentinel (`UInt64` max) when empty.
- `has_due(self, now: UInt64) -> Bool`, `is_empty(self) -> Bool`,
  `size(self) -> Int`.
- `collect_due(mut self, now: UInt64) raises -> List[TimerEntry]` — pops
  every timer due at `now` in deadline order; the caller (`service_timers`)
  filters stale generations and wakes live `WAITING` tasks exactly once via
  `unpark_current`.

No caller of `timer_heap.mojo` — `timer_service.service_timers`/
`drive_step`, `sleep.sleep_current`/`sleep_until_current`, or the A6.2/A6.3
timer/select branches — changes if this contract holds. This ADR does not
touch any of those files.

## Trigger metric mix + threshold (TQ11 resolution)

Inputs, all drawn from `bench/timer_scale_aot.mojo`'s JSONL rows (F6, spec
§78.7 format) at n = 1k/10k/100k/1M, median of >= 5 runs per spec §78.4
("do not use p99 from a trivially small sample"):

- `arm_scale` ns/op and `expire_scale` ns/op (this ADR's evidence, landed
  with #36).
- `deadline_sweep_scale` / `cancel_scale` / `service_pass_scale` (#88, A6.5
  — landing concurrently with this ADR; not yet published as of gate #1).

**Fire condition (build the wheel):** on **two consecutive** scheduled gate
evaluations (spec §78.5 discipline — a single anomalous run is noise, not
a regression; also an explicit dependency of #87), at least one of:

1. `arm_scale` or `expire_scale` ns/op at n=1,000,000 exceeds **2.5x** the
   same run's ns/op at n=100,000 (one decade of n). Pure O(log n) sift
   predicts roughly log2(1e6)/log2(1e5) ≈ 1.2x; 2.5x is a wide margin above
   that but tight enough to catch a genuine complexity-class change (e.g.
   cache-locality collapse turning the amortized cost effectively
   superlinear) without reacting to bench jitter.
2. `deadline_sweep_scale` (once #88 lands it) shows total sweep cost for a
   128k+ deadline cluster growing faster than a fitted `a + b*log2(n)`
   curve by more than 50% at any measured size — the "super-linear at
   128k+" language in #87's suggested rule, made countable.

**Hold condition:** either evaluation misses both legs, or fewer than two
consecutive evaluations exist yet (this is the case at gate #1 by
definition — there is no prior gate to be "consecutive" with).

## Evidence — gate #1 (2026-08-28)

Built and run 5x locally (arm64 darwin, `mojo build -I . bench/timer_scale_aot.mojo`,
single-worker unit-test embedding, no other load on the host):

| case | n | run values (ns/op) | median |
|---|---|---|---|
| arm_scale | 1,000 | 5,5,5,5,11 | 5 |
| arm_scale | 10,000 | 5,5,7,11,131 | 7 |
| arm_scale | 100,000 | 4,4,11,11,12 | 11 |
| arm_scale | 1,000,000 | 5,7,10,14,24 | 10 |
| expire_scale | 1,000 | 44,45,47,49,79 | 47 |
| expire_scale | 10,000 | 118,119,124,146,165 | 124 |
| expire_scale | 100,000 | 140,184,210,226,226 | 210 |
| expire_scale | 1,000,000 | 210,246,328,356,582 | 328 |

`arm_scale` 100k→1M ratio: 10/11 ≈ **0.9x** (flat — well under the 2.5x
trigger). `expire_scale` 100k→1M ratio: 328/210 ≈ **1.56x** — under the
2.5x trigger, and the decade-over-decade ratio trend (1k→10k = 2.64x,
10k→100k = 1.69x, 100k→1M = 1.56x) is *decreasing*, the signature of a
fixed per-call overhead plus a genuine `log2(n)` term (ratio → 1 as n
grows), not the *increasing* ratio a superlinear/polynomial blowup would
show. `deadline_sweep_scale`/`cancel_scale`/`service_pass_scale` are not
yet published (#88 in flight); leg 2 of the fire condition is therefore
unevaluated this gate.

## Decision

**HOLD.** `mojito_async/time/timer_heap.mojo` remains canonical.
`mojito_async/time/timer_wheel.mojo` is **not** built at this time.

Reason: (a) #87's own stated dependency requires evidence "reviewed over
two consecutive gates" before a build fires — this is gate #1, so the fire
condition is structurally unreachable this round regardless of the
numbers; (b) the leg-1 evidence that IS available shows sub-threshold,
decelerating growth consistent with the heap staying in its documented
O(log n) budget (spec §31); (c) leg 2 (`deadline_sweep_scale`) has no data
yet pending #88.

## Consequences

- No code changes to the timer lane; every caller of `timer_heap.mojo` is
  unaffected.
- Gate #2 is scheduled for the next F6 bench run after #88 (A6.5) merges
  and publishes `deadline_sweep_scale`/`cancel_scale`/`service_pass_scale`.
  That run should evaluate both legs above against this ADR's thresholds
  and append a new "Evidence — gate #2" section (and a fresh Decision, if
  it changes) to this same file rather than opening a new ADR — this file
  is the running decision record per #87's acceptance criteria.
- The 2.5x/decade and the-log-fit-plus-50% thresholds are the fixed,
  reusable gate for all future evaluations; they were not re-tuned down
  because gate #1's own numbers stayed comfortably under them (0.9x and
  1.56x vs. the 2.5x bar) — no evidence yet suggests the threshold itself
  is miscalibrated.
- If a future gate fires: implement per the plan already on file in issue
  #87 (cascaded per-power-of-two-level ring, `TimerEntry{id, gen,
  tcbaddr}` per slot, generation-token suppression preserved across
  cascade migration, coalescing within a configured tolerance, selected at
  the driver/embedding module-factory seam) and add a parity driver named
  exactly `test/unit/t29_timer_wheel.mojo` per #87's acceptance criteria
  (the `t29` numeric prefix is already shared by `t29_cancel_tree_aot.mojo`
  and `t29_with_scope.mojo` on `main` — this codebase's `t[0-9][0-9]_*`
  convention allows multiple drivers per number — but the exact filename
  `t29_timer_wheel.mojo` does not exist yet, so no rename is needed then).
