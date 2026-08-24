# A0 SPIKE_REPORT — Colorless Parking and Structured-Task Feasibility

**Decision: GO**

Date: 2026-08-24
Repo: github.com/mojito-async/mojito-async (all lanes merged; suite 11/11 green, exit 0)
Toolchain: Mojo 1.0.0b2 (2cf4d08a) via `mojito/brew/mojolang`; macOS arm64 (Apple M5, Darwin 25.5.0); page size 16384.

---

## Hypothesis (spec §100 A0.1)

> Using only the public `mojito-sys` contract, a small scheduler written in Mojo can run ordinary direct-style Mojo tasks, suspend a task at an explicit parking operation, execute another task on the same OS worker, resume the first task later, and preserve structured task/result/error semantics without `async`, `await`, `Future[T]`, `Task.wait()`, or OS-thread blocking.

**Confirmed.** Every mandatory test A0-T1 … A0-T16 passes; see matrix below.

## Exact mojito-sys version/contract used

Vendored frozen snapshot of mojito-sys S0 at main `8454212` (substrate files
byte-identical through `0ad4831`; provenance in
`spike/colorless_runtime/vendor/mojito-sys/VENDORED_AT.txt`). Contract v2:
`ms_ctx_t` 168 B (x19–x30 @0, d8–d15 @96, sp @160), guarded non-moving stacks,
entry-callback mechanism (`@export abi("C")` + `entry_pointer[symbol]()`),
`ms_ctx_switch` return_to bookkeeping. Verified by
`tests/contract_verify.c` (14 checks incl. mach_vm_region guard-protection
probe) and `tests/t0_contract.mojo`.

## Prototype architecture

```
run() [runtime.mojo]           root-task lifecycle: NEW->RUNNABLE->RUNNING,
                               work-first cooperative execution (spec §88),
                               counters; Nil unit type for void results
spawn() [spawn.mojo]           caller-allocates TCB cell; registers runnable;
                               returns JoinHandle[R] (one-shot join)
JoinHandle[R]                  consume-once join()/error()/abandon(); fail()
                               preserves child error message for re-raise
Event [event.mojo]             two-phase park over TCB-embedded WaitNode:
                               PREPARE publish -> VALIDATE recheck ->
                               COMMIT (PARKING/WAITING) -> WAKE claim-gen +
                               lossy-ring enqueue + RUNNABLE; settle()
                               precedence: cancellation beats readiness
Fiber [fiber.mojo]             wrapper over ms_stack_alloc/free +
                               ms_ctx_make/switch; caller allocates the stack
                               (b2 codegen constraint, see toolchain section);
                               resume/suspend map 1:1 onto ctx_switch legs
Scope [scope.mojo]             child registry w/ handle stamping, closed-parent
                               + aliasing guards, drained-registry close,
                               injected CancelHook failure policy
measure [measure.mojo]         CLOCK_MONOTONIC_RAW clock (~42 ns tick),
                               fixed-capacity latency samples, alloc accounter,
                               JSONL emitter
```

Boundary compliance: the only native surface is the vendored `mojito-sys`
spike dylib (frozen C ABI). No pthread/mmap/asm outside it. One worker =
the calling thread (spec §100 A0.2 allowed either).

## Task-state transition table (implemented in task.mojo)

| from \ to | NEW | RUNNABLE | RUNNING | PARKING | WAITING | COMPLETED | CANCELLED |
|---|---|---|---|---|---|---|---|
| NEW | – | ✓ spawn/schedule | | | | | |
| RUNNABLE | ✗ | ✗ | ✓ dequeue | | | | |
| RUNNING | ✗ | ✗ | ✗ | ✓ park prepare | (via PARKING) | ✓ result | ✓ cancel |
| PARKING | ✗ | ✓ early wake | | ✗ | ✓ commit | ✗ | ✗ |
| WAITING | ✗ | ✓ readiness / cancellation | | ✗ | ✗ | ✗ | ✗ |
| COMPLETED | terminal | | | | | – | |
| CANCELLED | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ | – |

All other pairs raise `IllegalTransitionError:`-prefixed errors; the t3 suite
asserts all 40 illegal pairs.

## Parking/wakeup protocol

Two-phase park with generation epochs (event.mojo):

1. PREPARE — publish waiter id + embedded WaitNode pointer into the Event;
   previous epoch's claim forgotten.
2. VALIDATE — recheck readiness/cancellation; if already satisfiable, return
   without sleeping (early wake: PARKING→RUNNABLE, no generation bump).
3. COMMIT — TCB PARKING→WAITING; node stamped with fresh generation.
4. WAKE — `set()` claims the generation exactly once (stale/duplicate
   rejected), enqueues via the lossy diagnostic ring (never fails), TCB
   WAITING→RUNNABLE.

Cancellation precedence at every boundary (`race_hooks.settle()`); winner
recorded exactly once (WinnerRecord).

## Test pass/fail matrix

| Test | File(s) | Result |
|---|---|---|
| contract verify (C, 14 checks) | tests/contract_verify.c | PASS |
| substrate smoke | tests/t0_contract.mojo | PASS |
| A0-T1 colorlessness | tests/t1_colorless.mojo | PASS |
| A0-T3 exact resume + T12-style entry/exit/reclaim | tests/t4_fiber.mojo | PASS (3/3 stable) |
| fiber.mojo wiring/lifecycle (AOT) | tests/t4b_fiber_module_aot.mojo | PASS |
| A0-T5 state machine, 40 illegal pairs | tests/t3_state_machine.mojo | PASS |
| FIFO queue (incl. 200k interleave) | tests/t5_queue.mojo | PASS |
| A0-T9 cancel + race hooks | tests/t9_cancel_races.mojo | PASS |
| A0-T10/T11/T12 park/wake races | tests/t10_event_protocol.mojo | PASS |
| A0-T13/T14 scope containment/nesting | tests/t13_scope.mojo | PASS |
| A0-T16 measurement infra | tests/t16_measure.mojo | PASS |
| A0-T2 worker reuse while parked | tests/t2_worker_reuse_aot.mojo (AOT) | PASS |
| A0-T4/T5/T8 join semantics + error prop | tests/t6_join_semantics_aot.mojo (AOT) | PASS |
| A0-T6/T7 one-shot join, result dtor once | tests/t7_result_lifetime_aot.mojo (AOT) | PASS |
| runtime symbol audit (no private Mojo RT) | (S0 harness pattern) | PASS |

Suite result on final head: **11/11 drivers PASS, exit 0** (t1 flipped green
when run() began executing real tasks; known-red row removed).

## Race scenarios exercised

Wake-before-park at PREPARE and VALIDATE (T11); duplicate set() within an
epoch (T12, enqueue-once asserted); stale-generation wakes ignored;
cancel-vs-readiness at PREPARE and COMMIT with deterministic single winner
(T10); early-wake edge PARKING→RUNNABLE (never WAITING, §24). All driven
through HookScript data schedules — deterministic across repeats.

## Allocation & latency measurements

Clock: CLOCK_MONOTONIC_RAW (~42 ns tick). N=5000 (M1/M3), M=64 (M2), after
200-iteration warmup. Driver: `bench/a0_capture_aot.mojo` (AOT).

| Measurement | Total | Per-op |
|---|---|---|
| M1 spawn + work-first execute + one-shot join (full task lifecycle) | 138.6 ms / 5000 | **≈ 27.7 ns** |
| M2 completed-join fast path (flag checks + result copy-out) | 125 ns / 64 | **≈ 2.0 ns** |
| M3 TCB create + NEW→RUNNABLE→RUNNING→COMPLETED | 22.3 ms / 5000 | **≈ 4.5 ns** |

Where every material cost comes from:

- Task lifecycle ≈ 28 ns: TCB value init + three validated transitions + Deque
  push/pop + result copy-out. No heap traffic on the path (SYS-4/A0-T16:
  park/wake and completed-join are allocation-free — verified by code audit
  and by M2's 2 ns join).
- Context switch itself: measured in S0 at up to 83.8 M round-trips/s
  unloaded (mojito-sys SPIKE_REPORT); A0 adds one scalar indirection per leg
  (heap-block ctx pointers).
- Known constant-factor notes (per review): LatencyTimer carries two clock
  reads per region (~100–200 ns); Event diagnostic ring drops oldest past 8
  entries (counted in `_log_dropped`).
- Not yet measured (deferred to A1 baseline per spec §78.2
  baseline-vs-experiment rule): committed bytes per idle task, multi-worker
  scaling, timer/reactor latencies (out of A0 scope).

## Known semantic gaps

1. Pending-join cannot execute unknown work in-library: b2 has no dynamic fn
   values, so a joining parent parks via the park/wake protocol and the
   embedding scheduler loop drives children (fast paths — completed join,
   error join — are fully synchronous). Documented in spawn.mojo.
2. JoinHandle move-only is BY CONVENTION (b2 lacks linear types); destroying
   a copy of a consumed handle is a no-op, but double-destroy of two copies
   of one live Fiber would double-free (documented single-owner semantics).
3. Event is single-waiter per epoch; multi-waiter reshape deferred to A1.
4. Recursion depth of work-first nesting consumes driver frames; synthetic
   fiber stacks (~16 KiB in drivers) bound safe depth to tens of levels —
   documented budget in spawn.mojo; A0.10 instrumentation to track live depth.
5. Debugging/backtraces across synthetic stacks remain poor (inherited from
   substrate; S0 limitation).

## Toolchain findings (all upstreamed or upstreamable)

1. **modular/modular#6971** (+ draft reproducer PR #6972): SIGSEGV /
   failed-to-lower when calling an imported-module factory whose body contains
   an extern call and a String-concat raise; deterministic under concurrent
   compilation load, content-dependent (literal raise survives). Consequence:
   Fiber construction and switch calls are INLINE at the consumer (driver)
   module; fiber.mojo keeps the API with caveats. **This also blocks executing
   fiber.mojo's own switch methods from any importing module** — consumers
   inline the bodies (t4 pattern); t4b covers the extern-free wiring surface.
2. Entry-module semantic-phase compile-time divergence (>200 s) when ~12 park
   cycles are choreographed inline in the driver; moving the harness into an
   imported cached module resolves it (event_scenarios.mojo pattern).
3. S0-reported JIT fragility under concurrent machine load
   (libKGENCompilerRTShared) corroborated during this spike.

Each workaround is local, does not change public API semantics, and introduces
no function coloring.

## Decision: GO

All explicit pass criteria of spec §100 A0.9 hold:

- ordinary `def` functions suspend through library operations without source
  coloring (T1);
- parking one task lets another runnable task execute on the SAME worker (T2);
- join parks rather than blocks the worker (T4);
- wakeup is exactly-once under forced race schedules (T10–T12);
- cancellation wakes a parked task (T9/T10);
- child errors propagate through join (T8);
- structured scope exit leaves no live children (T13/T14);
- no current Mojo async/coroutine API is required (T1);
- no OS/native calls bypass `mojito-sys` (vendored substrate only);
- no per-park heap allocation in the demonstrated common path (T16, M2);
- no correctness issue requires an application-visible Future or executor
  parameter.

Named conditions (none affect public API semantics; none introduce coloring):

1. b2 #6971-family codegen bugs require switch bodies to be inlined at the
   consuming module until a toolchain upgrade fixes them (re-test fiber.mojo
   methods on upgrade).
2. Pending-join blocking semantics are partially deferred (fast paths proven;
   scheduler-loop-driven waiting proven; in-library unknown-work execution
   blocked by missing fn values).
3. Backtraces across synthetic stacks remain poor.

These match the spec's CONDITIONAL-GO examples (move-only ergonomics
awkwardness; scoped-borrow captures conservative; one optimization awaiting a
compiler feature). The next phase (A1, spec §100) may proceed; the conditions
carry no application-visible API cost.

— Main, on behalf of lanes A0.1–A0.11