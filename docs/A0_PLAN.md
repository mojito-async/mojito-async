# A0 spike — implementation plan (mojito-async, issue #8)

Status: planning. Owner: Main. Lanes: A0.1–A0.11 (spec §112 Epic ASYNC-A).
This document is the contract for the lane batch: interfaces, file
ownership, dependency order, risk, and the TDD discipline. Lanes read it
before starting; they do NOT negotiate interfaces with each other.

## 1. What A0 proves (spec §100 A0.1–A0.11, tests A0-T1..A0-T16)

One OS worker; ordinary `def` tasks; spawn; park at `Event.wait()`; another
task runs on the SAME worker; wake exactly once; join; cancellation;
structured scope exit; no hidden OS blocking; no per-park allocation.
Ends in `SPIKE_REPORT.md` with GO / CONDITIONAL GO / NO-GO.

## 2. Frozen substrate (consumed, not modified)

Vendored snapshot of the `mojito-sys` S0 spike at
`github.com/mojito-async/mojito-sys` main `8454212` (all six lanes merged,
SPIKE_REPORT GO):

```
spike/colorless_runtime/vendor/mojito-sys/
    include/mojito_spike.h      # frozen v2: ms_ctx_t 168B (x19-x30, d8-d15, sp)
    native_stack.c  aarch64_switch.S  ms_ctx.c
    mojito_spike.mojo           # b2 bindings (entry_pointer, ms_* externs)
    VENDORED_AT.txt             # source SHA + commit message + date
```

Boundary rule (spec §100 A0.2): the spike uses ONLY the vendored
`mojito-sys` surface (`NativeStack`/`NativeContext` analogues:
`ms_stack_alloc/free`, `ms_ctx_make/switch`) — never raw `mmap`/`pthread`/
asm. macOS arm64 only (as the substrate). Build: dylib from vendored C/asm,
tests linked `-Xlinker` per S0 conventions.

## 3. Interfaces (cross-lane contract — do not change without Main)

```text
mojito_async/spike/
  runtime.mojo     struct Runtime:   create()/run(root_fn)/shutdown(),
                     one worker = calling thread; owns scheduler state.
  task.mojo        struct TaskControlBlock: state (atomic), parent/scope link,
                     embedded WaitNode, result slot, stack+ctx pointers.
                   state enum: NEW RUNNABLE RUNNING PARKING WAITING COMPLETED
                   CANCELLED (A0.5). Transitions per A0.5 state machine ONLY.
  fiber.mojo       struct Fiber: wraps vendored ms_stack/ms_ctx; entry =
                     @export abi("C") trampoline per S0 entry_pointer mechanism.
  queue.mojo       struct FifoQueue[T]: push/pop/len, mutex+condvar-free
                     (single worker), allocation-free pop path.
  spawn.mojo       struct JoinHandle[T]: one-shot join(), consume-once,
                     move-only; result move semantics; abandoned-result dtor.
  event.mojo       struct Event: wait()/set()/is_set(); generation counter;
                     waiters list = embedded TCB WaitNode (A0.7).
  scope.mojo       struct Scope: enter/exit; child registry; failure policy
                     (first failure cancels siblings, A0-T9); exit requires
                     live children == 0.
  cancel.mojo      cancellation flag per scope/task; cooperative checkpoints.
  race_hooks.mojo  deterministic hooks: force wake-before-park,
                     double-wake, duplicate set (A0-T10..A0-T12).
  measure.mojo     allocation + latency counters (A0-T16, A0.7 table).
```

Lane→file ownership (disjoint sets; last writer owns merge of shared
touched docs only):

| Lane | Owns | Depends on (merged) |
|---|---|---|
| A0.1 | vendor/, contract-verify script + its tests | — |
| A0.2 | runtime.mojo skeleton, spike harness, Makefile/run.sh | A0.1 |
| A0.3 | task.mojo + state-machine tests (pure Mojo, no ctx) | A0.1 |
| A0.4 | fiber.mojo, trampoline, stack lifecycle tests | A0.1, A0.2 |
| A0.5 | queue.mojo + FIFO tests | A0.1 |
| A0.6 | spawn.mojo, JoinHandle + T4/T5/T6/T7 tests | A0.1–A0.5 |
| A0.7 | event.mojo + park/wake + T2/T3/T15 (worker reuse) | A0.1–A0.6 |
| A0.8 | cancel.mojo + race_hooks.mojo + T9/T10/T11/T12 | A0.1–A0.7 |
| A0.9 | scope.mojo + T13/T14 (containment, nesting) | A0.1–A0.8 |
| A0.10 | measure.mojo + microbench + allocation table | A0.1–A0.9 |
| A0.11 | SPIKE_REPORT.md (Main owns; lanes contribute data only) | all |

## 4. Test suite first (TDD — step 1 of execution order)

Each lane's PR FIRST commits the failing tests for its features
(red, labeled `tdd-red,wip`), plus a `precommit/known-red.tsv` row naming
the test and tracking issue IN THE SAME COMMIT, so the gate passes the
intentional red. The implementation lands in later commits of the same PR;
the known-red row is deleted in the same commit that turns the test green.

Suite layout (mirrors S0 conventions):
```
spike/colorless_runtime/tests/
    run.sh                  # full matrix, nonzero exit on any unexpected FAIL
    tN_<feature>.mojo       # one file per A0-Tx (T1..T16), PASS/FAIL verdict
```
`precommit/run-suite.sh` (async repo gate hook) invokes
`spike/colorless_runtime/tests/run.sh` — the A0.2 lane wires this so the
gate covers the spike from the first red commit.

A0-T mapping: T1 colorlessness (compile-checks ordinary defs)
T2 worker reuse while parked (same-thread identity)
T3 exact resume point
T4 join-before-completion (parent parks, sibling runs)
T5 join-after-completion (no parking)
T6 one-shot join (double join rejected)
T7 result destruction exactly once
T8 child error propagation after park/resume (join re-raises)
T9 sibling cancellation (scope cancels parked sibling)
T10 cancellation/readiness race (exactly one winner)
T11 wake-before-park race (no lost wakeup)
T12 duplicate wake defense (single enqueue per generation)
T13 scope containment (live children == 0 at exit)
T14 nested scopes (structured order)
T15 no hidden OS wait (instrumented worker: runnable tasks keep the
    worker switching, not blocking)
T16 allocation accounting (task/spawn/park fast paths; zero per-park alloc)

## 5. Risk register

| Change | Risk | Mitigation |
|---|---|---|
| Vendor substrate | LOW (frozen, already proven) | SHA-pinned; contract-verify test asserts sizeof/offsets |
| Trampoline entry (fiber) | MED — b2 entry-pointer mechanism is subtle | reuse S0's proven mechanism verbatim; tests reuse T8 probe conventions |
| Park/wake races | HIGH — lost wakeup, double wake | two-phase park + generation counter; deterministic hooks |
| Mojo b2 ergonomics (move-only JoinHandle, closures) | MED | owned-capture spawn only (A0.6); document conditional-GO items |
| Scope/cancel interplay | MED | state machine as single source of truth; T9+T13 stress both |
| Gate vs TDD red | LOW | known-red.tsv mechanism proven in S0; gate already merged |

## 6. Execution order (waves)

0. (done) gates merged in all repos.
1. A0_PLAN.md (this), A0 sub-issues created, epic #8 linked.
2. Wave A: lane A0.1 (vendor + verify) — merges first; blocks everyone.
3. Wave B: A0.2, A0.3, A0.5 parallel (disjoint files).
4. Wave C: A0.4 (fiber), then A0.6 — A0.4 owns trampoline; A0.6 consumes.
   A0.6 + A0.7 must land after A0.4; A0.7 after A0.6 (park path needs join).
5. Wave D: A0.8 (races), A0.9 (scope) — need A0.7.
6. Wave E: A0.10 (measurement) — needs everything.
7. Wave F: reviews (4 expert roles × merged PRs), fold findings,
   re-test, merge. Main writes SPIKE_REPORT.md; epic #8 closes.

Parallelism cap: 5 concurrent lanes (S0 precedent; host + GitHub headroom).
Before each wave the lane set is checked against live agents (hub list) to
avoid duplicate/conflicting work.

## 7. Review & merge discipline (user standing rules)

- Every PR gets the 4 expert roles (Architect, Systems/Perf, Safety,
  API) via fresh no-context agents with the four skill briefs; consolidated
  comment; findings folded (<50 LOC into the PR; >=50 LOC → new issue +
  spawn agent). Merge only when tests green post-fold. No user review.
- Direction forks: 5-expert adversarial panel → 3 options → consensus →
  proceed without waiting.