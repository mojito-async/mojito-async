# mojito-async — Colorless Concurrency Library Implementation Specification

**Status:** Architecture / implementation specification  
**Target:** Mojo 1.0-era toolchain, starting with Mojo 1.0.0b2-compatible public APIs  
**Date:** 2026-08-21  
**Package:** `mojito-async`  
**Program:** Mojito systems libraries  
**Required dependency:** `mojito-sys`  
**Primary objective:** Colorless, direct-style structured concurrency with pay-for-what-you-use runtime cost, implemented on the `mojito-sys` systems substrate and designed for eventual compiler-supported zero-overhead suspension.

---

## 0. Executive decision

The library SHALL implement **structured concurrency over ordinary Mojo `def` functions**, using **stackful one-shot fibers** as the initial suspension representation and an **M:N scheduler** as the execution substrate.

User code SHALL NOT require:

- `async def`
- `await`
- `Future[T]` as the universal return type
- duplicated synchronous/asynchronous APIs
- explicit executor parameters on ordinary functions
- scheduler polling in ordinary function calls

Instead, potentially blocking library operations such as `Channel.recv()`, `Mutex.lock()`, `Task.join()`, `sleep()`, and reactor-backed I/O SHALL **park the current fiber** and return normally when the operation can complete.

The semantic model is:

```text
ordinary Mojo function
        |
        v
ordinary direct call
        |
        +---- operation immediately ready ----> return normally
        |
        `---- operation would block
                    |
                    v
              park current fiber
                    |
                    v
             scheduler runs work
                    |
                    v
              event becomes ready
                    |
                    v
              resume same fiber
                    |
                    v
                return value
```

The implementation MUST treat stackful fibers as a **representation**, not as the public concurrency abstraction.

The long-term compiler seam SHALL model suspension as a **one-shot `Suspend` effect / continuation operation**. A future compiler MAY lower the same source-level operation to:

1. no suspension machinery at all,
2. a stackless state machine,
3. a stackful continuation/fiber,
4. a device-specific continuation,

without changing the public library API.

---

# 0A. Mandatory async spike before production runtime work

After `mojito-sys` passes its handoff gate, `mojito-async` begins with **A0: Colorless Parking and Structured-Task Feasibility**.

A0 must demonstrate a one-worker direct-style runtime in which one ordinary Mojo task can park, another task can run on the same worker, and the first can later resume with correct join, cancellation, error, and scope behavior.

**Spike decision:** `GO`, `CONDITIONAL GO`, or `NO-GO`.

See **Phase A0 in Section 100** for the complete spike contract.

---

# 0B. Dependency and responsibility boundary

`mojito-async` SHALL consume low-level mechanisms exclusively through `mojito-sys`.

The package boundary is:

```text
Application
    |
    v
mojito-async
    |
    +-- Scope / Task / JoinHandle
    +-- Scheduler / Worker policy
    +-- ParkingLot / cancellation
    +-- task-aware synchronization
    +-- channels / select
    +-- timer scheduling
    +-- reactor semantics
    +-- direct-style networking
    +-- blocking pool policy
    `-- parallel task semantics
    |
    v
mojito-sys
    |
    +-- NativeStack
    +-- NativeContext
    +-- NativeThread / TLS
    +-- NativeEvent / OS wait-wake
    +-- MonotonicClock
    +-- NativeSocket
    `-- readiness/completion pollers
    |
    v
OS / C ABI / architecture assembly
```

`mojito-async` MUST NOT contain direct calls to `pthread_*`, `mmap`, `epoll`, `kqueue`, IOCP, platform TLS APIs, or architecture-specific assembly context switches.

This keeps **machine/OS mechanism** in `mojito-sys` and **concurrency policy/semantics** in `mojito-async`.

The full scheduler implementation is gated on the handoff criteria defined by `mojito-sys`, particularly proof that ordinary Mojo frames, references, destructors, and errors survive external context switching correctly.

---

# 1. Research and design basis

This architecture incorporates the following recent language/compiler research and current Mojo constraints.

## 1.1 Direct-style effects can be library-hosted in systems languages

**Effect Handlers for C via Coroutines**, OOPSLA 2024, demonstrates that structured effect handling can be exposed directly to programmers in C using a coroutine-based library rather than requiring a new source language.

Architecture implication:

- It is viable to prototype the control abstraction as a library.
- The public abstraction should be more semantic than the low-level context-switch mechanism.
- The low-level suspension substrate can later be replaced by compiler support.

Reference:

- https://dl.acm.org/doi/10.1145/3689798

## 1.2 Lexical handlers can compile to efficient stack switching

**Lexical Effect Handlers, Directly**, OOPSLA 2024, demonstrates direct compilation of lexical effect handlers to low-level stack machinery.

Architecture implication:

- Treat `park_current()` as the initial implementation of a future `Suspend` operation.
- Do not leak stack-switching details into application APIs.
- Preserve lexical/structured scope so future handler-based lowering remains possible.

Reference:

- https://dl.acm.org/doi/10.1145/3689770

## 1.3 Effect polymorphism should avoid higher-order API duplication

**Associated Effects: Flexible Abstractions for Effectful Programming**, PLDI 2024, shows how higher-order abstractions can remain polymorphic over effects instead of requiring separate effect-specialized APIs.

Architecture implication:

- `map`, `retry`, `with_timeout`, `for_each`, etc. should accept ordinary functions.
- Do not create `async_map`, `async_retry`, or callback types colored by suspension.
- Future Mojo compiler support should infer the effects/capabilities of callees and closures.

Reference:

- https://dl.acm.org/doi/10.1145/3656393

## 1.4 Capabilities/capture checking are a path to colorless effect tracking

Martin Odersky's **Capabilities for Control** work and Scala capture checking treat capabilities as compile-time tracked resources/effects without requiring conventional effect-row notation at every call boundary.

Architecture implication:

- Scheduler/suspension authority should conceptually be an ambient capability.
- Ordinary function signatures should not expose that capability.
- Spawn boundaries are the right place to reason about captures.
- A future `no_suspend` restriction is preferable to coloring all suspendable functions.

References:

- https://icfp24.sigplan.org/details/icfp-2024-papers/38/Capabilities-for-Control
- https://docs.scala-lang.org/scala3/reference/experimental/cc.html

## 1.5 Capture tracking can scale through generic containers

**What's in the Box: Ergonomic and Expressive Capture Tracking over Generic Data Structures**, OOPSLA 2025, develops reach capabilities and reports a Scala capture-checking implementation applied to collections and an asynchronous library with minimal-to-zero notational overhead in most cases.

Architecture implication:

- Do not bake `'static`-style lifetime requirements into the public API prematurely.
- Design `Scope` and task handles so Mojo origins can eventually prove that task captures remain live.
- Keep task containers and handles origin-aware.

Reference:

- https://dl.acm.org/doi/10.1145/3763112

## 1.6 Zero-overhead lexical handlers are a credible compiler target

**Zero-Overhead Lexical Effect Handlers**, OOPSLA 2025, demonstrates a design intended to impose no handler overhead when the effect is not raised.

Architecture implication:

- The target zero-cost rule is: *ordinary execution pays nothing merely because suspension is possible somewhere below it*.
- Pay at suspension points, not at every call.
- Compiler metadata may track suspension without changing source-level function types.

Reference:

- https://dl.acm.org/doi/10.1145/3763177

## 1.7 Continuation representation is an optimization dimension

**Virtualizing Continuations**, PLDI 2026, extends efficient lexical continuation work and demonstrates that continuation representation can be engineered independently from source-level effect semantics.

Architecture implication:

- The public API must not imply a permanent fiber representation.
- Initial continuations SHALL be one-shot.
- Compiler/runtime evolution may change continuation representation without changing application source.

Reference:

- https://dl.acm.org/doi/10.1145/3808289

## 1.8 Concurrency safety should be enforced where parallel separation matters

**Degrees of Separation: A Flexible Type System for Data Race Prevention** proposes capture/alias tracking that permits ordinary aliasing but constrains aliases where necessary for concurrent execution.

Architecture implication:

- `spawn()` is a semantic concurrency boundary and should eventually receive stronger capture analysis than ordinary calls.
- Function coloring is not a substitute for race safety.
- Ownership/capture rules and suspension representation are separate concerns.

Reference:

- https://arxiv.org/abs/2308.07474

## 1.9 Parallel effects and concurrency need not be separate language universes

**Parallel Algebraic Effect Handlers** provides a formal basis for combining parallel computations and effect handlers.

Architecture implication:

- Structured CPU parallelism and I/O concurrency should share scope/task semantics.
- They need not share identical execution mechanisms.
- GPU/device work should participate in dependency/lifetime structure without pretending a GPU kernel is a CPU fiber.

Reference:

- https://arxiv.org/abs/2110.07493

---

# 2. Current Mojo baseline and constraints

This specification assumes the public Mojo 1.0-era language and standard library.

Important current properties:

1. Mojo has a compile-time ownership/lifetime system with **origins**.
2. `OriginSet` is already used to track values captured by parametric closures/coroutines.
3. Mojo's current `Task` wraps a coroutine.
4. `Task.__await__()` suspends an async function.
5. `Task.wait()` explicitly blocks the current thread.
6. Mojo provides low-level atomics and memory orderings.
7. Mojo has public C FFI via `std.ffi`.
8. Some compiler-runtime async functionality is private/undocumented and MUST NOT be treated as a stable dependency.
9. As of Mojo 1.0.0b2, `fn` has been removed; examples in this specification use `def`.

Relevant current documentation:

- Ownership/origins: https://mojolang.org/docs/manual/values/lifetimes/
- Current Task: https://mojolang.org/docs/std/runtime/asyncrt/Task/
- Current TaskGroup: https://mojolang.org/docs/std/runtime/asyncrt/TaskGroup/
- Atomics: https://mojolang.org/docs/std/atomic/
- C FFI: https://mojolang.org/docs/manual/c-ffi/
- Mojo stdlib source policy/private compiler-runtime note: https://github.com/modular/modular/blob/main/mojo/stdlib/docs/faq.md

## 2.1 Consequence

The MVP MUST NOT implement colorlessness by calling current `Task.wait()` internally.

That would produce:

```text
fiber-like user API
       |
       v
Task.wait()
       |
       v
BLOCK OS THREAD
```

and would fail the primary scalability objective.

The MVP therefore requires either:

- a small native context-switch + stack-management substrate, or
- a future public compiler/runtime continuation intrinsic.

For Phase 1, this specification assumes the former.

---

# 3. Goals

## 3.1 Functional goals

The package SHALL provide:

- ordinary-function concurrency;
- structured task scopes;
- task spawning;
- one-shot joins;
- cooperative suspension;
- cancellation propagation;
- deadlines/timeouts;
- channels;
- task-aware mutexes;
- semaphores;
- events;
- condition variables;
- timers;
- task yielding;
- bounded blocking-FFI integration;
- reactor-backed network I/O;
- CPU task parallelism;
- observability and deterministic test hooks.

## 3.2 Performance goals

The design SHALL target:

- zero concurrency overhead in programs that do not initialize the runtime;
- zero scheduler lookup on ordinary non-concurrency calls;
- zero `Future` allocation per call;
- zero state-machine allocation merely because a callee may suspend;
- no heap allocation on a successful uncontended lock fast path;
- no heap allocation on channel fast paths when queue storage is preallocated;
- no heap allocation per suspension in the common case after task creation;
- O(1) task state transitions;
- O(1) local scheduler enqueue/dequeue;
- work stealing for unstarted work in Phase 1;
- bounded contention on global queues;
- stable stack addresses for live Mojo references.

## 3.3 Ergonomic goals

User code SHOULD look synchronous:

```mojo
def fetch_profile(id: Int) raises -> Profile:
    var bytes = http.get("/profiles/" + String(id))
    return parse_profile(bytes)
```

Concurrent composition SHOULD look structured:

```mojo
def load_dashboard(id: Int) raises -> Dashboard:
    with Scope() as scope:
        var profile = scope.spawn(lambda: fetch_profile(id))
        var activity = scope.spawn(lambda: fetch_activity(id))
        return Dashboard(profile.join(), activity.join())
```

No `async`/`await` color appears in application code.

---

# 4. Non-goals for the MVP

The MVP SHALL NOT attempt to implement:

- arbitrary algebraic effect handlers;
- multishot continuations;
- user-visible continuation capture;
- transparent preemptive suspension at arbitrary instructions;
- migration of already-started fibers across worker threads;
- unified CPU/GPU stack semantics;
- a replacement for all OS threads;
- implicit conversion of arbitrary blocking foreign calls into non-blocking calls;
- distributed scheduling;
- actor-system semantics;
- green-thread signal delivery;
- fork-safety across an active multithreaded scheduler without explicit handling.

---

# 5. Design invariants

The implementation MUST preserve these invariants.

## INV-1 — Ordinary function ABI remains ordinary

A function does not receive a hidden `Future`, `Poll`, `Context`, `Waker`, or executor parameter merely because a descendant may suspend.

## INV-2 — Suspension is explicit in the runtime, not necessarily in the source type

Only known library/runtime operations MAY park a task in Phase 1.

## INV-3 — Every child task belongs to a scope

Safe public spawning requires a parent scope. Detached tasks, if later offered, MUST be explicitly unsafe or runtime-owned.

## INV-4 — Join is one-shot

A `JoinHandle[T]` represents one result and SHOULD be consumed by `join()`.

## INV-5 — Started fibers are worker-affine in Phase 1

An unstarted task may be stolen. Once first entered, the fiber remains assigned to one worker until completion.

## INV-6 — Live stacks never move

Fiber stack virtual addresses MUST remain stable while frames are live.

## INV-7 — Blocking worker threads is opt-in and isolated

Potentially blocking foreign work MUST use a dedicated blocking pool or an explicitly blocking scope.

## INV-8 — Scheduler policy is Mojo; context switching is mechanism

The C/assembly layer MUST NOT contain scheduling policy.

## INV-9 — Cancellation is cooperative and structured

Cancellation is observed at parking/check points and propagates from parent scope to child tasks.

## INV-10 — Concurrency safety is distinct from colorlessness

The library MUST not claim data-race freedom merely because functions are colorless.

---

# 6. Proposed package layout

```text
mojito_async/
├── __init__.mojo
├── scope.mojo
├── task.mojo
├── cancellation.mojo
├── deadline.mojo
├── runtime/
│   ├── runtime.mojo
│   ├── worker.mojo
│   ├── scheduler.mojo
│   ├── task_control_block.mojo
│   ├── fiber.mojo                 # not implemented (fiber/ is a sibling package)
│   ├── parking_lot.mojo           # not implemented
│   ├── timer_wheel.mojo           # not implemented (timer_heap.mojo in time/)
│   ├── inject_queue.mojo
│   ├── local_queue.mojo           # not implemented (queue.mojo owns local work)
│   ├── remote_ready.mojo          # not implemented
│   ├── task_allocator.mojo        # not implemented
│   ├── stack_cache.mojo           # not implemented (fiber/stack_pool.mojo)
│   ├── blocking_pool.mojo         # not implemented — blocking calls wedge a worker
│   └── metrics.mojo               # not implemented
├── integration/
│   └── sys.mojo              # the only low-level adapter to mojito-sys
├── sync/
│   ├── mutex.mojo
│   ├── rwlock.mojo
│   ├── semaphore.mojo
│   ├── event.mojo
│   ├── condvar.mojo
│   ├── once.mojo                  # not implemented
│   └── barrier.mojo
├── channel/
│   ├── channel.mojo
│   ├── bounded.mojo               # not implemented (rendezvous.mojo / oneshot.mojo)
│   ├── unbounded.mojo
│   └── select.mojo
├── time/
│   ├── timer_heap.mojo
│   ├── timer_wheel.mojo           # not implemented
│   └── sleep.mojo
├── io/
│   ├── reactor.mojo               # not implemented (reactor/ is a sibling package)
│   ├── operation.mojo             # not implemented
│   ├── registration.mojo          # not implemented
│   ├── tcp.mojo                   # not implemented (net/tcp_stream.mojo)
│   ├── udp.mojo                   # not implemented
│   └── dns.mojo                   # not implemented — DNS wedges a worker (no blocking pool)
├── blocking/
│   ├── pool.mojo                  # not implemented — no blocking pool; blocking calls wedge a worker
│   └── blocking.mojo              # not implemented
├── parallel/
│   ├── parallel_for.mojo          # not implemented
│   ├── join.mojo                  # not implemented
│   └── reduction.mojo             # not implemented
├── unsafe/
│   ├── detached.mojo              # not implemented
│   └── borrowed_spawn.mojo        # not implemented
├── test/
│   ├── unit/
│   ├── deterministic/
│   ├── stress/
│   ├── model/
│   └── integration/
└── benchmark/
    ├── ordinary_call.mojo         # not implemented
    ├── spawn_join.mojo            # not implemented
    ├── context_switch_integration.mojo  # not implemented
    ├── park_unpark.mojo           # not implemented
    ├── channel_pingpong.mojo      # not implemented
    ├── mutex_contention.mojo      # not implemented
    ├── timer_scale.mojo
    ├── tcp_echo.mojo              # not implemented
    ├── cpu_parallel.mojo          # not implemented
    └── memory_per_task.mojo       # not implemented
```

There is deliberately **no `native/` directory and no platform-specific I/O directory** in this repository. Those belong to `mojito-sys`.

# 7. Public API

## 7.1 Root surface

The stable root package SHOULD re-export only high-level concepts:

```mojo
from mojito_async import (
    Scope,
    JoinHandle,
    CancellationToken,
    Deadline,
    sleep,
    yield_now,
    checkpoint,
)

from mojito_async.sync import (
    Mutex,
    RwLock,
    Semaphore,
    Event,
    CondVar,
    Barrier,
)

from mojito_async.channel import (
    Channel,
    Sender,
    Receiver,
)

from mojito_async.parallel import (
    parallel_for,
    join,
)
```

The root SHALL NOT expose:

- `Fiber`;
- `Context`;
- scheduler internals;
- raw waiter nodes;
- reactor op tokens;
- raw continuation addresses.

---

# 8. `Scope`

`Scope` is the primary structured-concurrency object.

Illustrative Mojo API:

```mojo
struct Scope:
    var _state: UnsafePointer[ScopeState, MutUntrackedOrigin]

    def __init__(out self) raises:
        self._state = _scope_create(parent=_current_scope())

    def __enter__(ref self) -> ref[origin_of(self)] Self:
        _push_scope(self._state)
        return self

    def spawn[
        T: Movable,
        origins: OriginSet,
    ](
        ref self,
        var work: Callable[() -> T, origins=origins],
    ) raises -> JoinHandle[T, origins]:
        return _spawn(self._state, work)

    def cancel(self):
        _scope_cancel(self._state)

    def join_all(self) raises:
        _scope_join_all(self._state)

    def __exit__(mut self) raises:
        _pop_scope(self._state)
        _scope_close_and_join(self._state)
```

> **Syntax note:** exact callable/origin generic syntax MUST track the public Mojo version used by the implementation. The semantic contract is normative; snippets are implementation sketches where Mojo's evolving callable syntax differs.

## 8.1 Scope exit

On scope exit:

1. mark scope `CLOSING`;
2. prevent new public children;
3. if a body error is propagating, request cancellation of children;
4. wait until child count reaches zero;
5. collect/resolve child failures according to the configured error policy;
6. release scope-owned resources.

A scope MUST NOT be destroyed while a child is still live.

## 8.2 Default failure policy

Recommended default:

```text
first child failure
        |
        v
record primary error
        |
        v
cancel sibling tasks
        |
        v
join all siblings
        |
        v
raise primary error
```

An optional aggregate policy MAY be provided later.

---

# 9. `JoinHandle[T]`

The handle SHALL be move-only if current Mojo traits allow an ergonomic implementation.

Conceptual API:

```mojo
struct JoinHandle[
    T: Movable,
    origins: OriginSet,
]:
    var _task: UnsafePointer[TaskControlBlock, MutUntrackedOrigin]
    var _consumed: Bool

    def join(mut self) raises -> T:
        _debug_assert(not self._consumed)
        self._consumed = True
        return _join_task[T](self._task)

    def cancel(ref self):
        _request_cancel(self._task)

    def is_finished(ref self) -> Bool:
        return _task_is_finished(self._task)
```

## 9.1 One-shot semantics

`join()` SHOULD consume the logical result.

Do not support repeated joins returning copied results by default. Repeated readers belong in a separate shared-result abstraction.

Benefits:

- continuation/result path remains linear;
- no implicit reference counting;
- result storage can be moved directly into the caller;
- aligns with one-shot continuation semantics;
- simplifies destruction.

---

# 10. Task state model

Use a compact state enum encoded in an atomic integer.

```text
NEW
 |
 v
RUNNABLE
 |
 v
RUNNING ----------------------.
 |                            |
 +--> PARKING --> WAITING ----+--> RUNNABLE
 |
 +--> CANCELLING
 |
 +--> COMPLETING
 |
 `--> COMPLETED
```

Recommended encoded states:

```mojo
alias TASK_NEW = UInt32(0)
alias TASK_RUNNABLE = UInt32(1)
alias TASK_RUNNING = UInt32(2)
alias TASK_PARKING = UInt32(3)
alias TASK_WAITING = UInt32(4)
alias TASK_CANCELLING = UInt32(5)
alias TASK_COMPLETING = UInt32(6)
alias TASK_COMPLETED = UInt32(7)
```

A separate flags word SHOULD contain:

```text
CANCEL_REQUESTED
HAS_ERROR
HAS_RESULT
STARTED
PINNED
IN_BLOCKING_REGION
TRACE_ENABLED
```

Do not multiply the main state machine for every orthogonal property.

---

# 11. `TaskControlBlock`

The TCB is scheduler metadata and SHOULD be allocated independently from the fiber stack.

Proposed conceptual layout:

```mojo
struct TaskControlBlock:
    # Hot scheduler state — first cache line where practical.
    var state: Atomic[DType.uint32]
    var flags: Atomic[DType.uint32]
    var owner_worker: UInt32
    var priority: UInt16
    var generation: UInt16

    # Intrusive queue links.
    var run_next: UnsafePointer[TaskControlBlock, MutUntrackedOrigin]
    var wait_next: UnsafePointer[TaskControlBlock, MutUntrackedOrigin]

    # Execution representation.
    var fiber: Fiber
    var entry: TaskEntry

    # Structured concurrency.
    var parent_scope: UnsafePointer[ScopeState, MutUntrackedOrigin]
    var cancel_state: UnsafePointer[CancelState, MutUntrackedOrigin]

    # One embedded waiter eliminates common suspension allocations.
    var waiter: WaitNode

    # Cold/optional metadata SHOULD move out of the hot cache line.
    var cold: UnsafePointer[TaskColdState, MutUntrackedOrigin]
```

## 11.1 Hot/cold splitting

`TaskColdState` SHOULD contain infrequently accessed data:

- task name;
- tracing metadata;
- extended error diagnostics;
- debug spawn location;
- large result metadata;
- task-local map;
- profiling counters.

The scheduler loop should not fetch cold state.

---

# 12. Runtime lifecycle

The scheduler SHALL be lazily initialized.

## 12.1 Required behavior

A program that imports `concurrency` but never calls concurrency APIs MUST NOT:

- create worker threads;
- allocate scheduler queues;
- initialize an I/O reactor;
- reserve fiber stacks;
- add checks to ordinary function calls.

## 12.2 Initialization trigger

First use of any operation requiring a concurrency context:

```text
Scope()
run(...)
spawn root scope
parallel operation using this runtime
```

may call:

```mojo
def _ensure_runtime() raises -> ref Runtime:
    ...
```

Use a one-time initialization primitive with a fast initialized path.

---

# 13. Root execution API

Provide an explicit bridge from ordinary process startup into the scheduler:

```mojo
def run[T: Movable](var main: Callable[() -> T]) raises -> T:
    var runtime = _ensure_runtime()
    return runtime.run_root(main)
```

Example:

```mojo
def concurrent_main() raises -> Int:
    with Scope() as scope:
        var a = scope.spawn(work_a)
        var b = scope.spawn(work_b)
        return a.join() + b.join()

def main() raises:
    print(run(concurrent_main))
```

A future compiler/runtime integration MAY allow an implicit root scope, but the library MVP should keep root runtime entry explicit.

---

# 14. Fiber representation

## 14.1 Requirements

A `Fiber` is an internal `mojito-async` execution object composed from `mojito-sys` mechanisms:

```text
Fiber
 |
 +-- mojito_sys.NativeContext
 +-- mojito_sys.NativeStack
 +-- task control block link
 +-- started flag
 +-- owner-worker identity
 `-- scheduler bookkeeping
```

Conceptual wrapper:

```mojo
struct Fiber:
    var context: SysNativeContext
    var stack: SysNativeStack
    var started: Bool

    def resume(mut self):
        _sys_context_switch_to_fiber(self)

    def suspend(mut self):
        _sys_context_switch_to_scheduler(self)
```

Only runtime internals may invoke these methods.

## 14.2 One-shot continuation interpretation

A parked fiber semantically represents the current one-shot continuation of a task.

For each suspension episode:

```text
running continuation
        |
        v
park
        |
        v
waiting continuation
        |
        v
single winning wake
        |
        v
resume
```

The public API MUST NOT expose continuation cloning, native context pointers, or raw stacks.

## 14.3 Representation independence

`Fiber` MUST remain replaceable by a compiler-generated continuation representation later.

No public API may rely on:

- stack size;
- context layout;
- stack address;
- worker context ABI;
- assembly switch implementation.

# 15. Fiber stack and context dependency on `mojito-sys`

`mojito-async` SHALL NOT allocate or manage virtual-memory stacks through raw operating-system APIs.

Each task fiber owns or borrows:

```text
mojito_sys.memory.NativeStack
+
mojito_sys.context.NativeContext
```

Requirements inherited from `mojito-sys`:

- live stack addresses never relocate;
- guard pages detect overflow;
- stack commit may grow without address changes;
- context switching preserves the platform ABI;
- switching is allocation-free;
- Mojo stack locals, references, destructors, and `raises` behavior have passed the `mojito-sys` handoff tests.

`mojito-async` MAY implement a per-worker **stack cache**, but the stack object itself remains a `mojito-sys` mechanism.

Recommended cache policy:

- cache completed stack reservations per worker;
- bound by stack count, reserved bytes, and committed bytes;
- decommit cold pages before retaining large idle reservations;
- never recycle a stack until the task and any result/destructor path are unquestionably complete.

The runtime SHALL treat the fiber representation as replaceable. No public API exposes `NativeStack` or `NativeContext`.

# 18. Scheduler topology

Phase 1 scheduler:

```text
                   Global Injection Queue
                           |
              .------------+------------.
              |            |            |
              v            v            v
           Worker 0     Worker 1     Worker N
              |            |            |
          local deque   local deque   local deque
              |            |            |
              `---------- steal ----------'
```

Each worker owns:

- one OS thread;
- one scheduler/native context;
- one local runnable deque;
- one stack cache;
- one task slab/cache;
- one timer shard or timer submission path;
- worker-local metrics.

---

# 19. Worker-affinity policy — Phase 1

## 19.1 Unstarted task

`NEW`/`RUNNABLE` tasks that have never entered user code MAY be stolen.

## 19.2 Started task

Once a task first executes:

```text
STARTED = true
owner_worker = current worker
```

and remains worker-affine.

When the task wakes:

```text
waiting task
    |
    v
enqueue into owner worker's remote-ready queue
```

rather than becoming generally stealable.

## 19.3 Rationale

This avoids early complexity around:

- thread-local data;
- foreign libraries;
- lock ownership;
- platform TLS;
- stack-unwinding assumptions;
- CPU affinity;
- worker-local allocators.

Migration MAY become a Phase 5+ optimization behind an explicit task property.

---

# 20. Local run queue

Use a work-stealing deque optimized for:

- owner push/pop at one end;
- thieves steal from the opposite end.

For Phase 1 correctness, it is acceptable to begin with a simpler locked deque and replace it after scheduler semantics stabilize.

Recommended implementation progression:

1. **P0:** mutex/spinlock-protected per-worker `Deque`;
2. **P1:** Chase-Lev-style deque for unstarted tasks;
3. **P2:** specialized intrusive queue where profiling justifies it.

Do not begin by writing a sophisticated lock-free deque before task-state correctness is proven.

---

# 21. Scheduler loop

Conceptual worker loop:

```mojo
def worker_loop(mut worker: Worker):
    while not worker.runtime.shutdown_requested():
        if var task = worker.pop_local():
            worker.run_task(task^)
            continue

        if var task = worker.pop_remote_ready():
            worker.run_task(task^)
            continue

        if var task = worker.runtime.inject_queue.pop():
            worker.run_task(task^)
            continue

        if var task = worker.try_steal_unstarted():
            worker.run_task(task^)
            continue

        worker.process_timers()
        worker.poll_reactor_nonblocking()

        if worker.has_no_immediate_work():
            worker.park_os_thread_until_event()
```

Policy MUST prevent indefinite I/O starvation under CPU load. Use a fairness budget such as:

```text
run at most K ready tasks
then service reactor/timers
```

or time-based polling.

---

# 22. Scheduler re-entry and TLS

Only concurrency primitives need the current task/runtime.

Conceptually:

```text
TLS.current_worker
TLS.current_task
TLS.current_scope
```

Ordinary functions do not read these values.

Provide internal accessors:

```mojo
def _current_worker() raises -> UnsafePointer[Worker, MutUntrackedOrigin]
def _current_task() raises -> UnsafePointer[TaskControlBlock, MutUntrackedOrigin]
def _current_scope() raises -> UnsafePointer[ScopeState, MutUntrackedOrigin]
```

A concurrency primitive invoked outside `run()` SHALL either:

- return a clear `NoConcurrencyRuntime` error, or
- use a documented process-root fallback.

Prefer explicit `run()` in Phase 1.

---

# 23. Parking protocol

Parking is the most correctness-sensitive mechanism in the library.

The runtime MUST prevent the classic lost-wakeup race:

```text
Task A                      Task B
------                      ------
checks condition false
                            makes condition true
                            wakes nobody
parks forever
```

## 23.1 Semantic primitive

```mojo
def _park_current(
    key: WaitKey,
    prepare: ParkPrepare,
    cancel: CancellationToken,
    deadline: Optional[Deadline],
) raises
```

A parking operation must atomically coordinate:

- condition validation;
- waiter publication;
- transition to `WAITING`;
- wake/cancellation/timeout races.

## 23.2 Recommended two-phase park

```text
1. PREPARE
   - create/initialize embedded waiter
   - publish waiter under resource synchronization

2. VALIDATE
   - recheck resource condition after publication
   - if resource is now ready, abort parking

3. COMMIT
   - CAS task RUNNING -> PARKING
   - hand control to scheduler
   - scheduler finalizes PARKING -> WAITING

4. WAKE
   - winner changes WAITING/PARKING -> RUNNABLE
   - queue exactly once
```

Use a waiter generation number/epoch to reject stale wakeups.

---

# 24. `WaitNode`

Each TCB SHOULD contain one reusable embedded wait node.

```mojo
struct WaitNode:
    var task: UnsafePointer[TaskControlBlock, MutUntrackedOrigin]
    var next: UnsafePointer[WaitNode, MutUntrackedOrigin]
    var generation: UInt32
    var status: Atomic[DType.uint32]
    var payload: UInt64
```

This removes per-suspension heap allocation for the common case where one task waits on one resource.

Operations requiring multiple simultaneous registrations, such as `select`, MAY allocate auxiliary registration nodes from a worker-local slab.

---

# 25. Wake protocol

A wake operation MUST be idempotent with respect to a waiter generation.

Conceptual algorithm:

```mojo
def _wake(waiter: UnsafePointer[WaitNode, MutUntrackedOrigin]):
    if not waiter[].try_claim():
        return

    var task = waiter[].task
    if _make_runnable(task):
        _enqueue_owner(task)
```

Exactly one of these wins:

- resource readiness;
- cancellation;
- deadline expiration;
- explicit close/error.

The winner stores the wake reason/result.

---

# 26. `ParkingLot`

Use a central reusable waiting abstraction for synchronization primitives.

Conceptual interface:

```mojo
struct ParkingLot:
    def park(
        key: WaitKey,
        condition: ParkCondition,
        deadline: Optional[Deadline],
        cancellation: CancellationToken,
    ) raises -> WakeReason:
        ...

    def unpark_one(self, key: WaitKey) -> Bool:
        ...

    def unpark_all(self, key: WaitKey) -> Int:
        ...
```

Internal implementation options:

- hash-bucketed waiter queues;
- direct resource-owned queues;
- hybrid direct fast paths + hashed overflow.

Start simple, profile later.

---

# 27. `yield_now()`

`yield_now()` is a concurrency operation but not a blocking operation.

```mojo
def yield_now():
    var task = _current_task()
    _mark_runnable(task)
    _switch_to_scheduler()
```

It MUST NOT:

- sleep the worker thread;
- register an I/O waiter;
- allocate.

---

# 28. `checkpoint()`

Provide a cheap cooperative cancellation/fairness point:

```mojo
def checkpoint() raises:
    if _current_task_cancel_requested():
        raise CancelledError()
```

A later implementation MAY combine a scheduler fairness check.

Do not insert `checkpoint()` implicitly into every ordinary function.

---

# 29. Cancellation model

## 29.1 Cancellation tree

```text
root scope
 |
 +-- child task A
 |    `-- child scope A1
 |         +-- task A1.1
 |         `-- task A1.2
 |
 `-- child task B
```

Cancelling a scope recursively requests cancellation of descendants.

## 29.2 Cooperative semantics

Cancellation SHALL become observable at:

- `join`;
- channel send/receive;
- mutex/semaphore waits;
- event/condvar waits;
- timers/sleep;
- reactor-backed I/O;
- explicit `checkpoint()`.

Cancellation SHALL NOT asynchronously interrupt arbitrary user instructions in Phase 1.

## 29.3 Shielding

Do not include broad cancellation shielding in MVP unless required for cleanup.

If added later, make it lexical and narrow:

```mojo
with cancellation.shield():
    cleanup()
```

---

# 30. Deadline and timeout semantics

Use absolute monotonic deadlines internally.

```mojo
struct Deadline:
    var ticks: UInt64
```

Never store wall-clock timestamps for scheduler deadlines.

Public helpers:

```mojo
def sleep(duration: Duration) raises
def sleep_until(deadline: Deadline) raises

def with_timeout[T: Movable](
    duration: Duration,
    var operation: Callable[() -> T],
) raises -> T
```
`sleep()` raises when called without a driven scheduler frame (Mojo b2 has no
global current-task pointer); use `sleep_current(rt, h, heap, clock, duration)`
from inside the dispatcher. `sleep_until()` raises for the same reason; use
`sleep_until_current(rt, h, heap, clock, deadline)` instead. `with_timeout`
SHOULD create a child scope with a deadline rather than inventing a separate
timeout-specific task model.

---

# 31. Timer subsystem

Initial implementation MAY use a min-heap.

At scale, prefer a hierarchical timer wheel or sharded wheel.

Required properties:

- monotonic time;
- O(log n) acceptable for initial correctness implementation;
- cancellation of timers;
- stale timer suppression via generation tokens;
- coalescing allowed within configurable tolerance;
- deadlines wake owner worker without making started tasks migratable.

Benchmark at:

- 1k timers;
- 10k;
- 100k;
- 1M.

---

# 32. Errors

Concurrency should compose with Mojo's normal `raises` model.

Do not standardize on:

```text
Future[Result[T, E]]
```

A task entry may raise; the TCB stores either:

```text
result T
or
error
```

and `join()` reproduces normal control flow.

Conceptually:

```mojo
def child() raises -> Int:
    ...

with Scope() as scope:
    var h = scope.spawn(child)
    var value = h.join()  # returns Int or raises child's error
```

If exact typed error propagation evolves in Mojo, preserve the public direct-style form.

---

# 33. Panic/fatal-error policy

Distinguish:

1. ordinary raised errors;
2. cooperative cancellation;
3. runtime invariant failure;
4. unrecoverable native/ABI corruption.

Runtime invariant failure SHOULD terminate the current runtime/process in release builds where recovery cannot be made sound.

Never attempt to resume a fiber after stack/context corruption.

---

# 34. Task-aware `Mutex[T]`

Prefer a mutex that owns/protects a value rather than exposing a bare lock where practical.

Conceptual API:

```mojo
struct Mutex[T: Movable]:
    var _state: Atomic[DType.uint32]
    var _value: T
    var _waiters: WaitQueue

    def lock(mut self) raises -> MutexGuard[T]:
        if self._try_lock_fast():
            return MutexGuard(self)

        self._lock_slow()
        return MutexGuard(self)
```

## 34.1 Fast path

Uncontended lock:

```text
atomic CAS UNLOCKED -> LOCKED
return guard
```

No allocation. No scheduler lookup beyond what is needed after fast-path failure.

## 34.2 Slow path

Contended lock:

1. publish embedded task waiter;
2. retry acquisition;
3. park;
4. wake;
5. acquire ownership;
6. return guard.

## 34.3 Unlock

Unlock SHOULD hand off to a waiter when contention exists.

Avoid thundering-herd `unpark_all`.

---

# 35. Blocking OS locks vs task-aware locks

The package SHALL clearly distinguish:

```text
std/utils thread lock
    => may block OS worker thread

mojito_async.sync.Mutex
    => parks current fiber
```

Internal scheduler data structures MAY use short non-suspending spinlocks where bounded and carefully profiled.

User-facing synchronization in concurrent code SHOULD use task-aware primitives.

---

# 36. Semaphore

Conceptual API:

```mojo
struct Semaphore:
    var _permits: Atomic[DType.int64]
    var _waiters: WaitQueue

    def acquire(mut self, n: Int = 1) raises -> Permit:
        ...

    def try_acquire(mut self, n: Int = 1) -> Optional[Permit]:
        ...

    def release(mut self, n: Int = 1):
        ...
```

`Permit` SHOULD use RAII for automatic return unless deliberately consumed.

Fairness policy MUST be documented. FIFO fairness is easier to reason about but may reduce throughput.

---

# 37. Event

Support a manual-reset or one-shot event.

```mojo
struct Event:
    def wait(self) raises:
        ...

    def set(mut self):
        ...

    def reset(mut self):
        ...
```

If both one-shot and manual-reset semantics are useful, expose distinct types rather than a runtime mode flag:

```text
Event
OneShotEvent
```

---

# 38. Condition variable

`CondVar.wait()` MUST release the associated task-aware mutex and park atomically with respect to notifications.

Public use:

```mojo
with mutex.lock() as guard:
    while not predicate(guard):
        cond.wait(guard)
```

Spurious wakeups MAY be permitted if clearly documented, retaining conventional predicate-loop semantics.

---

# 39. Channel model

Provide bounded channels first.

```mojo
var channel = Channel[Message](capacity=1024)
var tx = channel.sender()
var rx = channel.receiver()
```

API:

```mojo
tx.send(value)
var value = rx.recv()

if tx.try_send(value):
    ...

if var value = rx.try_recv():
    ...
```

`send` and `recv` are ordinary functions that may park.

---

# 40. Bounded channel algorithm

Initial implementation:

```text
ring buffer
+
head/tail indices
+
sender wait queue
+
receiver wait queue
+
closed flags
```

Optimize in stages.

Correctness-first implementation MAY use a short internal lock around ring metadata and park outside the lock.

Later versions may use specialized MPMC algorithms after benchmarking.

## 40.1 Fast receive

```text
if item exists:
    move item from ring
    possibly wake one sender
    return item
```

No allocation.

## 40.2 Slow receive

```text
register receiver waiter
recheck queue
park
resume
consume item / observe close / cancellation
```

---

# 41. Channel close semantics

Closing the last sender:

- wakes all receivers;
- receivers drain buffered values;
- subsequent receive returns a closed-channel result/error.

Closing the last receiver:

- wakes blocked senders;
- subsequent sends fail.

Avoid implicit process-level cancellation merely because a channel closes.

---

# 42. `select`

`select` is a Phase 2 feature due to multi-registration complexity.

Desired public concept:

```mojo
select(
    recv(ch1),
    recv(ch2),
    deadline(timer),
)
```

Implementation needs:

- one `SelectState` winner atomic;
- N registration nodes;
- each operation attempts CAS to claim the selection;
- losing registrations are logically cancelled;
- generation tokens prevent late wake corruption.

Do not implement `select` by polling every channel in a tight loop.

---

# 43. Reactor abstraction

`mojito-async` owns reactor **semantics and task integration**; `mojito-sys` owns the platform pollers and native handles.

Conceptual interface:

```mojo
trait Reactor:
    def register_read(... ) raises -> IoToken
    def register_write(... ) raises -> IoToken
    def submit(... ) raises -> IoToken
    def cancel(self, token: IoToken)
    def poll(mut self, timeout: Optional[Duration]) raises -> Int
```

Internally the reactor adapts one of:

```text
mojito_sys.io.ReadinessPoller
mojito_sys.io.CompletionPoller
```

The exact trait syntax may evolve; the semantic contract is normative.

The reactor SHALL own:

- task/op registration;
- generation tokens;
- cancellation races;
- mapping native readiness/completion to task wakeups;
- owner-worker routing;
- scheduler fairness integration.

It SHALL NOT own platform syscall bindings.

# 44. Platform I/O backend contract through `mojito-sys`

Platform mechanisms are selected below this package:

```text
Linux     -> epoll baseline; optional io_uring completion backend
macOS/BSD -> kqueue
Windows   -> IOCP when target support is available
```

`mojito-async` must not know the native event structure layouts.

The reactor MAY use readiness and completion adapters differently because their semantics differ. The public direct-style networking API remains identical.

Operations that cannot be made usefully non-blocking on a target SHALL use the `mojito-async` blocking pool rather than blocking scheduler workers.

# 47. Reactor operation lifecycle

Each operation receives an opaque token containing:

```text
slot index
generation
operation kind
owner worker/task
```

Use generation counters to prevent stale completions from waking a recycled task/op slot.

Conceptual flow:

```text
socket.read()
    |
    +-- immediate bytes -> return
    |
    `-- would block
            |
            v
       register op
            |
            v
          park
            |
            v
      reactor completion
            |
            v
    claim waiter generation
            |
            v
      queue owner task
            |
            v
          resume
```

---

# 48. Networking API

Target direct-style interfaces:

```mojo
struct TcpStream:
    def read(mut self, buffer: Span[UInt8]) raises -> Int
    def write(mut self, data: Span[UInt8]) raises -> Int
    def write_all(mut self, data: Span[UInt8]) raises
    def shutdown(mut self) raises

struct TcpListener:
    def accept(mut self) raises -> TcpStream

def connect(address: SocketAddress) raises -> TcpStream
```

No `AsyncTcpStream` type.

No `read_async()`.

---

# 49. Blocking foreign calls

A colorless API must not falsely imply that every foreign function is scheduler-aware.

Provide explicit blocking integration:

```mojo
def blocking[T: Movable](
    var work: Callable[() -> T]
) raises -> T
```

Behavior:

1. enqueue work into dedicated bounded blocking pool;
2. park current fiber;
3. blocking worker executes work;
4. completion wakes the fiber.

Use for:

- legacy C APIs;
- synchronous DNS resolver if no async resolver is available;
- blocking filesystem calls;
- compression/codec libraries where execution time may be long and uncontrollable;
- foreign runtimes that block.

---

# 50. Blocking pool

Properties:

- separate from scheduler workers;
- bounded maximum threads;
- idle thread retirement;
- backpressure when saturated;
- task cancellation cannot forcibly interrupt arbitrary C safely;
- cancellation may abandon the result but must retain any required storage until foreign work completes.

Do not create one OS thread per blocking call.

---

# 51. CPU parallel work

Use the same structured task semantics for CPU work:

```mojo
with Scope() as scope:
    var left = scope.spawn(lambda: compute_left(data))
    var right = scope.spawn(lambda: compute_right(data))
    combine(left.join(), right.join())
```

For regular data parallelism, add specialized APIs:

```mojo
parallel_for(...)
parallel_map(...)
parallel_reduce(...)
join(f, g)
```

These may use optimized scheduler entry paths and chunking.

---

# 52. Parallelism heuristics

`parallel_for` SHOULD:

1. avoid spawning for tiny ranges;
2. estimate grain size;
3. split recursively or by chunks;
4. keep current worker productive;
5. cap task creation;
6. use work stealing for unstarted chunks.

Expose tuning only when measurement demonstrates need.

---

# 53. GPU/device work

Share **structure**, not stack mechanics.

Desired semantic model:

```text
Scope
 |
 +-- CPU task
 +-- CPU task
 `-- Device operation
```

A device task may complete via:

- stream/event callback;
- host polling;
- backend-specific completion.

It does not own a CPU fiber while executing on-device.

A waiting CPU task may park until the device event becomes ready.

---

# 54. Spawn capture safety — MVP

This is a difficult boundary because library code cannot invent compiler lifetime rules.

Use a conservative staged model.

## 54.1 Safe owned spawn

Primary safe API requires the task closure/environment to own values needed beyond the immediate call.

```mojo
scope.spawn(move lambda: work(owned_value))
```

Exact syntax should follow current Mojo closure capture features.

## 54.2 Scoped borrow spawn

If current origins can encode the relationship safely, provide an origin-parametric scoped spawn whose handle cannot outlive the `Scope` or captured values.

Conceptual relationship:

```text
lifetime(child) <= lifetime(scope)
lifetime(child) <= lifetime(captured borrow)
```

If public Mojo cannot express/prove this in the target release, DO NOT emulate safety with untracked pointers.

Keep borrowed spawning experimental or unavailable until it can be sound.

## 54.3 Unsafe borrowed spawn

If needed for research:

```mojo
from mojito_async.unsafe import spawn_borrowed_unchecked
```

The name MUST clearly state unsafety.

---

# 55. Future compiler capture checking

The compiler extension SHOULD treat `Scope.spawn` as a concurrency boundary.

At spawn, classify captures:

```text
capture
 |
 +-- moved unique value -----------------> safe
 |
 +-- immutable shareable ----------------> safe
 |
 +-- atomic/synchronized shared value ---> safe
 |
 +-- disjoint mutable region ------------> safe if proven
 |
 `-- unsynchronized mutable alias -------> reject
```

Ordinary calls should not require the same separation rule merely because they might suspend.

This follows the separation principle: strict alias reasoning is activated where true concurrent overlap is introduced.

---

# 56. `Sendable` / `Shareable` traits

Do not stabilize these trait names until Mojo's own concurrency memory model is clearer.

If library-local traits are required:

```mojo
trait Sendable:
    pass

trait Shareable:
    pass
```

Semantics:

- `Sendable`: ownership may cross a worker/task execution boundary safely.
- `Shareable`: immutable/shared reference may be concurrently observed safely.

Treat unsafe blanket implementations as internal hazards requiring audit.

---

# 57. No-suspend regions — future compiler feature

The source language should remain colorless by default.

Instead of:

```text
async def foo()
```

introduce, eventually:

```text
@no_suspend
def critical_section():
    ...
```

or an equivalent effect constraint.

Use cases:

- scheduler internals;
- spin-lock critical sections;
- signal/interrupt-like handlers;
- allocator critical paths;
- foreign callbacks that cannot yield;
- some GPU/device code.

Compiler rule:

```text
@no_suspend function
    cannot call a path whose inferred effects contain Suspend
```

This is a **restriction color**, not a pervasive concurrency color.

---

# 58. Future `Suspend` effect

Internal compiler concept:

```text
effect Suspend
```

Operations that may park carry/infer it.

Examples:

```text
Channel.recv   -> Suspend
Mutex.lock slow path -> Suspend
Task.join incomplete -> Suspend
TcpStream.read would-block -> Suspend
sleep -> Suspend
```

A function that calls one need not change source syntax.

The compiler may infer:

```text
effects(foo) = union(effects(callees), local effects)
```

without adding `async` to source types.

---

# 59. Future capability interpretation

Equivalent conceptual form:

```text
CanSuspend capability
```

The concurrency runtime/root scope provides it.

Tasks capture/access it implicitly.

Compiler capture checking can track the authority without requiring a visible executor parameter in ordinary application signatures.

Higher-order callables should be capability/effect polymorphic by default where possible.

---

# 60. Compiler lowering seam

All library parking MUST funnel through a minimal internal abstraction.

Phase 1:

```mojo
def _suspend_current(reason: SuspendReason) raises:
    _park_and_native_stack_switch(reason)
```

Future compiler-recognized form:

```text
intrinsic suspend(reason)
```

Do not scatter raw context-switch calls throughout synchronization and I/O implementations.

---

# 61. Proposed compiler IR model

Long-term:

```text
Mojo source
   |
   v
typed Mojo IR
   |
   v
Concurrency / Effect IR
   |
   +-- suspend
   +-- spawn
   +-- join
   +-- cancel
   +-- scope
   |
   v
continuation analysis
   |
   +-- prove no suspension -> erase effect machinery
   |
   +-- local bounded suspension -> stackless lowering
   |
   +-- arbitrary direct-style suspension -> stackful continuation
   |
   `-- device op -> device-specific lowering
   |
   v
lower-level MLIR / LLVM / device target
```

---

# 62. Adaptive lowering target

The compiler should ultimately choose representation.

Given:

```mojo
def foo() raises -> Int:
    var x = read_value()
    return x + 1
```

possible lowering:

### Case A — `read_value()` proven non-suspending

Ordinary machine stack and call.

### Case B — suspension points statically manageable

Stackless continuation/state-machine representation.

### Case C — deep/opaque/FFI direct-style suspension

Stackful fiber/continuation.

The choice SHALL NOT change `foo`'s source signature.

---

# 63. Memory ordering policy

Use the weakest ordering proven correct.

Do not default every scheduler atomic to sequential consistency permanently.

Suggested policy:

- state publication: release;
- state observation: acquire;
- counters with no dependency publication: relaxed;
- ownership transfer: release/acquire pair;
- lock algorithms: documented acquire/release;
- diagnostic counters: relaxed.

During initial development, stronger orderings MAY be used for correctness and reduced after stress/model testing.

Every non-relaxed atomic in core scheduler code SHOULD contain a comment explaining the synchronization relationship.

---

# 64. Avoid false sharing

Worker-local frequently mutated counters MUST NOT share cache lines between workers.

Use padded/aligned structures where Mojo provides the required alignment control.

Candidates:

- queue head/tail;
- runnable count;
- steal counters;
- reactor completion counters;
- task allocator free-list heads.

Do not pad blindly; verify using hardware counters.

---

# 65. Task allocation

Use worker-local slabs/pools.

Task creation path:

```text
worker-local free TCB?
 |
 +-- yes -> reuse
 |
 `-- no  -> allocate slab/block
```

A completed task whose join/result lifetime has ended may return its TCB to the owner cache.

Cross-worker frees should be batched or routed to the owning allocator to reduce allocator contention.

---

# 66. Result storage

Small results SHOULD be stored inline where practical.

Larger or non-trivial results may use task-owned storage.

Requirements:

- exactly-once initialization;
- exactly-once move into `join()` caller;
- destruction if handle/result is abandoned;
- no read before completion;
- error/result tagged state.

Do not require result `Copyable`.

---

# 67. Scheduler fairness

MVP MUST prevent:

- timer starvation under CPU work;
- reactor starvation;
- one task monopolizing a worker via runtime-level loops.

Because arbitrary user code is cooperative, CPU-bound user code that never yields may monopolize its worker. Document this.

Specialized parallel loops SHOULD insert internal scheduling opportunities at chunk boundaries where appropriate.

---

# 68. Preemption

Do not add asynchronous stack preemption in MVP.

Potential later options:

- compiler-inserted safepoints;
- allocation/checkpoint safepoints;
- time-budget cooperative yield;
- signal-based preemption only after detailed platform/FFI safety work.

Colorlessness does not require preemption.

---

# 69. Thread-local and worker-local state

Phase 1 worker affinity avoids many TLS hazards.

The runtime adapter to `mojito-sys` SHALL establish worker-thread-local pointers for:

```text
current_worker
current_task
current_scope
```

using `mojito-sys.NativeTlsKey` or a later stable Mojo TLS replacement.

Document:

- native TLS is OS-worker-local, not task-local;
- task-local values require a separate `TaskLocal[T]` abstraction;
- user code must not assume OS TLS follows a task if migration is enabled in a future version;
- ordinary application function calls do not read runtime TLS unless they invoke a concurrency primitive.

# 70. Task-local values

Phase 2 MAY add:

```mojo
struct TaskLocal[T]:
    ...
```

Prefer lexical context passing/capabilities for performance-sensitive APIs over a general dynamic task-local dictionary.

Task-local storage is useful for:

- tracing spans;
- request identifiers;
- logging context.

It should not become the primary dependency-injection mechanism.

---

# 71. Observability

Runtime metrics SHOULD include:

```text
runtime_workers
tasks_created_total
tasks_completed_total
tasks_runnable
tasks_waiting
task_steals_total
park_total
wake_total
spurious_wake_total
context_switch_total
reactor_ops_inflight
reactor_completions_total
blocking_pool_active
blocking_pool_queued
stack_reserved_bytes
stack_committed_bytes
stack_cache_bytes
timer_count
```

Tracing SHOULD support:

- spawn;
- first run;
- park;
- wake;
- migration if ever supported;
- cancellation;
- completion.

Tracing MUST be removable/near-zero when disabled.

---

# 72. Task names

Debug builds or explicit tracing MAY allow:

```mojo
scope.spawn_named("fetch-profile", work)
```

Do not store names in the hot TCB by default.

---

# 73. Mojito Async Validation Suite (MAVS)

`mojito-async` SHALL maintain a first-class validation program called the **Mojito Async Validation Suite (MAVS)**.

There is no single external suite that can establish the correctness and performance of an async/concurrency runtime. Mature runtimes use complementary forms of validation:

- deterministic schedule exploration;
- probabilistic concurrency stress;
- state-machine/model tests;
- fuzzing;
- sanitizers and memory-safety tooling;
- feature/platform integration tests;
- statistically controlled microbenchmarks;
- end-to-end macrobenchmarks;
- long-running soak tests.

MAVS SHALL combine these methods rather than rely on ordinary unit tests alone.

## 73.1 External reference suites and practices

The implementation team SHOULD continuously compare MAVS coverage against the following projects.

### Tokio test stack

Tokio is a particularly relevant reference because it combines:

- integration tests;
- feature-matrix testing;
- **Loom** schedule-permutation tests;
- fuzz targets;
- Miri checks;
- AddressSanitizer testing;
- Valgrind-related CI;
- Criterion benchmarks;
- cross-target compilation checks.

Reference:

- https://github.com/tokio-rs/tokio/blob/master/docs/contributing/pull-requests.md
- https://github.com/tokio-rs/tokio/blob/master/.github/workflows/ci.yml
- https://github.com/tokio-rs/loom

MAVS SHALL emulate this *layered testing philosophy*. Loom itself cannot directly model Mojo atomics/runtime state unless a compatible adapter is developed, so `mojito-async` requires its own deterministic schedule explorer.

### Loom

Loom repeatedly executes small concurrent programs while permuting legal interleavings under a modeled memory model and applies state-reduction techniques.

Reference:

- https://github.com/tokio-rs/loom

MAVS SHALL borrow the key concepts:

```text
small bounded state space
+
explicit instrumented synchronization
+
systematic scheduling permutations
+
state reduction / pruning
+
replayable failing schedule
```

Passing randomized stress tests is not a substitute for schedule exploration.

### OpenJDK `jcstress`

`jcstress` is explicitly designed to test concurrency support in the JVM, class libraries, and hardware. It uses small actor-based tests, collects observed terminal states, and grades those states as acceptable or forbidden.

Reference:

- https://openjdk.org/projects/code-tools/jcstress/
- https://github.com/openjdk/jcstress

MAVS SHALL adopt an equivalent **actor/outcome** test style for low-level scheduler and synchronization races.

Example conceptual test:

```text
Initial state:
    task = WAITING
    waiter = registered

Actor A:
    cancel(task)

Actor B:
    wake(waiter)

Arbiter:
    inspect:
        task state
        queue membership
        wake reason
        waiter ownership

Allowed:
    exactly one winner
    task RUNNABLE exactly once

Forbidden:
    queued twice
    still WAITING
    two terminal wake reasons
```

### Deterministic simulation / virtual time

Tokio's simulation work demonstrates the value of a deterministic runtime with simulated time and controlled task ordering.

Reference:

- https://github.com/tokio-rs/simulation

MAVS SHALL implement virtual monotonic time for timer and deadline tests so that:

```text
30-minute timeout test
```

can execute as a deterministic state transition rather than requiring 30 minutes of wall-clock time.

### OpenJDK JMH

JMH is OpenJDK's harness for nano-, micro-, milli-, and macro-benchmarks on the JVM.

Reference:

- https://openjdk.org/projects/code-tools/jmh/

MAVS does not need to reproduce JMH's JVM-specific machinery, but its benchmark harness SHALL adopt the same discipline of separating:

```text
setup
warmup
measurement
teardown
result analysis
```

and avoiding one-off stopwatch measurements.

### Go performance suites: `bent` and `Sweet`

The Go project separates collections of microbenchmarks from broader end-to-end workloads. It also explicitly recommends comparing an experiment against a baseline rather than interpreting isolated absolute numbers.

References:

- https://go.dev/wiki/PerformanceMonitoring
- https://pkg.go.dev/golang.org/x/benchmarks/cmd/bent
- https://pkg.go.dev/golang.org/x/benchmarks/sweet

MAVS SHALL similarly have:

```text
microbenchmarks
+
representative macrobenchmarks
+
same-machine baseline-vs-experiment comparisons
```

The benchmark corpus SHOULD evolve when workloads cease to represent real use.

### `liburing` regression tests

Linux `io_uring` is sufficiently complex that the liburing project states that much of its repository is regression/unit testing for both liburing and kernel support.

Reference:

- https://github.com/axboe/liburing

MAVS SHALL use the installed liburing/kernel regression state as **environment qualification** for Linux `io_uring` testing. A failing kernel/liburing regression must be separated from a `mojito-async` reactor defect.

## 73.2 MAVS suite families

MAVS SHALL contain these top-level suites:

```text
mavs/
├── conformance/
├── schedule/
├── stress/
├── fuzz/
├── io/
├── safety/
├── benchmark/
├── macro/
└── soak/
```

### `conformance/`

Tests public semantic contracts:

- `Scope`;
- spawn;
- join;
- cancellation;
- deadlines;
- channels;
- synchronization;
- shutdown;
- direct-style I/O.

### `schedule/`

Systematically explores bounded interleavings.

### `stress/`

Runs actor/outcome concurrency tests at high iteration counts on real threads.

### `fuzz/`

Generates operation sequences, payloads, task trees, channel operations, cancellation patterns, and reactor lifecycle sequences.

### `io/`

Cross-platform network/reactor integration and fault tests.

### `safety/`

Sanitizers, memory ownership, stack lifetime, leak checks, API-boundary audits, and compile-fail tests where applicable.

### `benchmark/`

Repeatable microbenchmarks.

### `macro/`

Representative end-to-end workloads.

### `soak/`

Long-duration, high-cardinality reliability and resource-leak workloads.

---

# 74. Deterministic schedule exploration

`mojito-async` SHALL implement a test-only deterministic scheduler inspired by Loom.

This is not an optional debugging convenience; it is a core correctness tool for the parking lot, channels, task lifecycle, and cancellation.

## 74.1 Instrumentable scheduling points

Test builds SHALL be able to insert scheduling decisions around:

- atomic load/store/CAS;
- queue insertion/removal;
- waiter publication;
- waiter claim;
- task state transition;
- timer insertion/removal;
- cancellation propagation;
- scope child-count updates;
- reactor completion publication;
- result publication;
- join waiter registration.

Production builds SHALL compile these hooks out.

## 74.2 Deterministic scheduler capabilities

The test scheduler MUST support:

- fixed deterministic task-choice order;
- seed-based pseudo-random choice;
- bounded exhaustive enumeration;
- bounded preemption count;
- bounded branch count;
- virtual monotonic time;
- manually injected I/O completions;
- forced wake-before-park;
- forced cancel-before-wake;
- forced wake/cancel ties;
- task-choice replay.

A failure MUST print enough information to reproduce the exact schedule.

Minimum replay record:

```text
test name
runtime build hash
random seed, if used
decision sequence
virtual-time events
injected I/O events
task IDs/generations
```

## 74.3 State reduction

The first implementation MAY use simple bounded enumeration.

As test-state size grows, add reductions such as:

- do not branch when only one runnable task exists;
- collapse consecutive non-shared operations;
- bound repeated equivalent wakeups;
- canonicalize task IDs where tasks are symmetric;
- stop exploring after a terminal forbidden state.

Do not sacrifice reproducibility for clever pruning.

## 74.4 Required schedule-exploration subjects

At minimum:

```text
JoinHandle
Event
Mutex
Semaphore
bounded Channel
channel close
scope child completion
scope cancellation
timer cancellation
select winner
remote wake
task shutdown
```

Every synchronization primitive must have at least one small schedule-model test.

---

# 75. Actor/outcome stress and model tests

MAVS SHALL include a `jcstress`-inspired actor/outcome harness in addition to deterministic schedule exploration.

The two methods catch different classes of defects:

```text
schedule model
    => systematically explores a small abstract state space

real stress
    => exercises compiler, CPU, cache, memory-ordering, OS-thread,
       and timing behavior on actual hardware
```

## 75.1 Test shape

Each stress test SHOULD define:

```text
initial state
actors
optional arbiter
acceptable outcomes
forbidden outcomes
interesting-but-acceptable outcomes
```

Conceptual schema:

```yaml
name: cancellation_vs_wake
actors:
  - cancel_task
  - wake_waiter
arbiter:
  - inspect_task
outcomes:
  runnable_once:
    expect: ACCEPTABLE
  completed_cancelled:
    expect: ACCEPTABLE
  queued_twice:
    expect: FORBIDDEN
  waiting_forever:
    expect: FORBIDDEN
```

The exact file format is implementation-defined.

## 75.2 Mandatory actor/outcome tests

Required races include:

1. wake vs park publication;
2. wake vs `PARKING -> WAITING`;
3. cancel vs wake;
4. timeout vs wake;
5. timeout vs cancellation;
6. channel close vs send;
7. channel close vs receive;
8. sender drop vs receiver wake;
9. task completion vs join registration;
10. task completion vs cancellation;
11. scope close vs child spawn;
12. scope cancellation vs child completion;
13. mutex unlock vs waiter cancellation;
14. semaphore release vs waiter cancellation;
15. select branch A vs branch B;
16. I/O completion vs I/O cancellation;
17. I/O completion vs descriptor close;
18. worker shutdown vs remote wake;
19. result publication vs result observation;
20. task-cache reuse vs stale generation/wake.

## 75.3 Stress duration modes

Define standard modes:

```text
quick       developer/PR smoke
standard    full PR validation
nightly     high iteration count
release     extended hardware stress
```

Do not rely on one hard-coded iteration count across machines.

Collect:

- total executions;
- observed outcome histogram;
- forbidden outcome count;
- wall time;
- architecture;
- compiler/runtime version.

---

# 76. Functional, integration, and I/O conformance

## 76.1 Public API conformance

Every stable public operation MUST have black-box tests that use only the public package surface.

This prevents tests from passing because they know internal scheduler state.

Public conformance includes:

- nested scopes;
- child completion ordering;
- first-failure sibling cancellation;
- result move semantics;
- double-join prevention;
- channel FIFO behavior where guaranteed;
- bounded-channel backpressure;
- close semantics;
- mutex exclusion;
- semaphore permit accounting;
- deadline expiration;
- timeout nesting;
- runtime shutdown.

## 76.2 Colorlessness conformance

Maintain compile-and-run tests proving representative call chains remain ordinary functions.

The test corpus MUST include:

```text
ordinary caller
  -> generic helper
    -> trait-dispatched helper
      -> direct-style I/O/synchronization call
```

No test should require propagating:

```text
async
await
Future[T]
executor parameter
Poll[T]
Waker
```

through the chain.

## 76.3 `mojito-sys` integration tests

`mojito-async` does not duplicate architecture register-conformance tests. Those belong to `mojito-sys`.

Integration tests MUST verify the higher-level contract:

- a task suspends from a deep ordinary Mojo call chain and resumes correctly;
- task-local stack-backed values remain valid;
- a parked task can be awakened from another OS worker;
- completed fibers return stacks to the cache only after all destruction/result handling;
- scheduler shutdown never releases a stack still reachable by a task;
- `NativeContext` errors or unsupported-platform conditions fail during runtime initialization rather than corrupting execution.

The `mojito-sys` validation status for the target architecture is a prerequisite for `mojito-async` release support on that target.

## 76.4 Reactor/I/O conformance

For each supported reactor backend, test:

- connect success/refusal;
- accept;
- read readiness;
- write readiness;
- EOF;
- half-close;
- peer reset;
- local close while waiting;
- descriptor/handle reuse;
- stale completion generation rejection;
- many simultaneous registrations;
- cancellation;
- timeout;
- multiple waiters where supported;
- edge-trigger/one-shot semantics where applicable;
- readiness that arrives before park;
- completion that arrives after logical cancellation;
- shutdown with inflight operations.

Linux `io_uring` runs SHOULD record:

```text
kernel version
liburing version if used
relevant liburing regression status
supported opcode/features
```

Do not mark the runtime broken solely because an unsupported/known-broken kernel feature fails.

## 76.5 Virtual-time timer conformance

Timer tests SHALL primarily use virtual time.

Required:

- equal deadlines;
- timer cancellation;
- timer reset;
- extremely distant deadlines;
- zero-duration timer;
- nested timeouts;
- cancellation at exact deadline;
- large timer population;
- monotonic-clock discontinuity resistance.

Wall-clock timer integration tests remain necessary but should be a small subset.

---

# 77. Fuzzing, memory safety, and systems-boundary validation

## 77.1 Stateful fuzzing

Fuzz operation sequences rather than only byte parsers.

Useful generated operations:

```text
spawn
join
cancel
yield
event set/reset
mutex acquire/release
semaphore acquire/release
channel send/recv/close
timer create/cancel
select registration
runtime shutdown
```

The fuzzer SHOULD maintain a simple reference model and reject impossible generated operations or explicitly test their failure behavior.

Any discovered failure MUST save:

- minimized operation sequence;
- scheduling seed;
- payload seed;
- runtime configuration.

## 77.2 Fuzzing targets

At minimum:

- intrusive run queue;
- wait queue;
- channel ring/index arithmetic;
- timer wheel/heap;
- `select` registration cleanup;
- reactor token generation/reuse;
- task state transitions;
- cancellation tree;
- scope child registry.

Tokio's practice of maintaining dedicated fuzz targets is the reference model.

## 77.3 Sanitizers and interpreters

Where the Mojo/native toolchain permits, CI SHOULD include:

- AddressSanitizer;
- ThreadSanitizer;
- UndefinedBehaviorSanitizer for native code;
- leak detection;
- memory/provenance tooling available in the Mojo ecosystem;
- Valgrind-like checks where applicable.

Tokio currently combines Loom with Miri/ASan/Valgrind-related validation; MAVS should preserve the same principle even when the exact Mojo tool differs.

A sanitizer run is not a substitute for deterministic schedule testing.

## 77.4 Systems-boundary safety tests

Test the dependency boundary:

- no runtime module calls OS APIs directly;
- no scheduler module imports architecture-specific context symbols;
- native handles are owned by `mojito-sys` wrappers;
- blocking platform operations appear only in the blocking pool or worker-idle path;
- reactor tokens survive native poller round trips unchanged;
- no `mojito-sys` object is destroyed while a task still depends on it.

Add a lint/repository rule where practical to prevent direct platform imports outside a small approved integration module.

## 77.5 Feature and configuration matrix

Borrow Tokio's practice of testing configuration permutations.

MAVS MUST test meaningful combinations of:

- single-worker / multi-worker;
- tracing on/off;
- debug assertions on/off;
- reactor backend;
- stack size policy;
- worker counts: 1, 2, physical core count, oversubscribed;
- blocking pool enabled/disabled where valid;
- optional synchronization/channel features.

Do not attempt an exponential full powerset if combinations are semantically redundant; define pairwise/targeted coverage.

---

# 78. Performance benchmark methodology

Every major optimization requires benchmark evidence.

Performance results SHALL be produced by a repeatable harness, not ad-hoc stopwatch code.

## 78.1 Benchmark classes

MAVS uses three performance levels:

```text
Level P1 — microbenchmarks
    one runtime mechanism

Level P2 — composed runtime benchmarks
    several mechanisms interacting

Level P3 — end-to-end macrobenchmarks
    representative application workload
```

## 78.2 Baseline-vs-experiment rule

Adopt the Go performance-monitoring rule:

> Performance changes are evaluated against a baseline in the same controlled benchmark session, not from isolated historical absolute numbers.

For a change under review, run:

```text
baseline commit/configuration
vs
candidate commit/configuration
```

on the same machine.

Historical dashboards are useful for trends but SHALL NOT be the only evidence for a small regression/improvement claim.

## 78.3 Measurement protocol

For performance-gating runs:

1. use a dedicated or otherwise quiet machine;
2. record CPU model, core topology, RAM, OS/kernel, Mojo version, compiler flags;
3. pin benchmark threads/workers where appropriate;
4. record CPU frequency/governor state;
5. perform warmup before measurement;
6. repeat enough times to estimate variance;
7. randomize/interleave baseline and experiment where practical;
8. keep workload inputs deterministic;
9. avoid network dependencies outside the benchmark host;
10. collect CPU and memory metrics together;
11. retain raw samples.

On Linux, use `perf stat`/hardware counters where available for mechanisms where cycles, instructions, branch misses, cache misses, context switches, and migrations explain a result.

## 78.4 Reported statistics

A benchmark result SHOULD report as applicable:

```text
median
mean
standard deviation / confidence interval
p50
p95
p99
ops/s
ns/op
cycles/op
instructions/op
allocations/op
allocated bytes/op
RSS / peak RSS
committed stack bytes/task
CPU time
```

Do not use p99 from a trivially small sample.

## 78.5 Regression thresholds

Define two thresholds:

```text
warning threshold
blocking regression threshold
```

Thresholds MUST reflect observed benchmark noise.

Example policy:

- statistically unclear: rerun;
- small but statistically clear regression below warning threshold: record;
- regression above warning threshold: requires explanation;
- regression above blocking threshold: fails performance gate unless explicitly approved.

Do not hard-code one universal percentage for all benchmarks.

## 78.6 External comparators

Where practical, implement equivalent benchmark workloads against:

- ordinary Mojo direct calls;
- current Mojo async/runtime facilities;
- OS threads;
- Rust Tokio;
- Go goroutines;
- Java virtual threads for selected direct-style workloads;
- Boost.Context for raw context-switch comparison;
- C/C++ native event-loop/fiber primitives where useful.

Cross-runtime numbers are **informational**, not release gates.

Primary gating compares `mojito-async` against itself.

## 78.7 Benchmark result format

Use a machine-readable result format, e.g. JSON Lines:

```json
{
  "benchmark": "park_unpark/local",
  "commit": "...",
  "platform": "linux-aarch64",
  "workers": 1,
  "iterations": 1000000,
  "ns_per_op_median": 84.2,
  "p99_ns": 101.7,
  "allocs_per_op": 0
}
```

The harness SHOULD also emit a concise human-readable table.

---

# 79. Microbenchmark and composed-runtime suite

## 79.1 Baseline/no-use benchmarks

### B1 — import/no-use tax

Measure a program that imports the package but never enters the runtime.

Required checks:

```text
worker threads == 0
scheduler allocation == 0
fiber stack reservation == 0
reactor initialization == 0
```

### B2 — runtime initialized ordinary call

Measure deep ordinary call chains inside `run()` without suspension.

Target:

```text
no measurable per-call scheduler tax
```

## 79.2 Task lifecycle

### B3 — spawn + completed join

Measure:

- cold allocator;
- warm TCB/stack cache;
- one worker;
- N workers.

### B4 — spawn + pending join

Join before child completion so the parent parks.

### B5 — task completion

Measure result publication and waiter wake separately where possible.

## 79.3 Scheduling

### B6 — fiber context switch

Two runnable fibers ping-pong.

Report:

- ns/switch;
- cycles/switch;
- instructions/switch.

`mojito-sys` raw-context benchmark is the lower-bound reference. The difference is scheduler overhead.

### B7 — yield

One task repeatedly calls `yield_now()`.

### B8 — local park/unpark

Event waiter and setter on one worker.

### B9 — cross-worker wake

Parked task owned by worker A; wake emitted from worker B.

### B10 — remote-ready queue

Isolate remote enqueue/dequeue cost.

### B11 — work steal

Measure unstarted task steal and failed steal.

## 79.4 Synchronization

### B12 — Mutex uncontended

No allocation.

### B13 — Mutex contended

2, 4, 8, N workers/tasks.

### B14 — Semaphore

Single permit and batched permits.

### B15 — Event

ready fast path and park/wake slow path.

## 79.5 Channels

### B16 — bounded SPSC

Capacities:

```text
0/1 if rendezvous is supported
1
16
64
1024
```

### B17 — MPSC

1, 2, 4, 8, N producers.

### B18 — channel backpressure

Producer faster than consumer.

### B19 — close/drain

Cost of close with 0/1/N waiters.

## 79.6 Timers

### B20 — timer insert/cancel

### B21 — timer expiry latency

At:

```text
1k
10k
100k
1M
```

live timers where feasible.

### B22 — simultaneous expiry storm

Large group with equal deadline.

## 79.7 Cancellation

### B23 — cancellation checkpoint

No cancellation requested.

### B24 — subtree cancellation

Vary tree depth and fan-out.

### B25 — cancellation wake storm

Many parked tasks.

## 79.8 Reactor/I/O

### B26 — poller idle round trip

### B27 — readiness registration/cancellation

### B28 — loopback TCP read/write

Payloads:

```text
1 B
64 B
1 KiB
16 KiB
1 MiB
```

### B29 — many idle connections

Track RSS and CPU at increasing connection counts.

### B30 — completion storm

Many sockets become ready together.

## 79.9 Blocking bridge

### B31 — `blocking()` trivial call

Measures bridge overhead floor.

### B32 — blocking pool saturated

Measure queueing latency, scheduler progress, and bounded-resource behavior.

## 79.10 Memory

### B33 — idle task memory

At:

```text
1k
10k
100k
1M
```

where host virtual-address/RAM capacity permits.

Measure separately:

- reserved virtual bytes;
- committed stack bytes;
- TCB/control metadata;
- total RSS.

---

# 80. Macrobenchmarks, soak tests, and CI tiers

## 80.1 Macrobenchmarks

The suite SHALL include at least:

1. **TCP echo** — small and medium messages, increasing concurrent connections.
2. **Fan-out/fan-in** — one request spawns N child I/O operations and joins them.
3. **Bounded producer/consumer pipeline** — exercises backpressure and channels.
4. **Connection proxy** — bidirectional copy between sockets.
5. **Timer-heavy service** — large numbers of deadlines/timeouts.
6. **Mixed I/O + CPU** — ensures reactor work is not starved by CPU tasks.
7. **CPU divide-and-conquer** — evaluates task spawn/steal/join.
8. **Many-idle-task workload** — memory and idle scheduler overhead.
9. **Cancellation storm** — large structured task tree cancelled from the root.
10. **Blocking-FFI saturation** — scheduler remains healthy while blocking pool is saturated.
11. **Device-completion workload** — once GPU/device completion is implemented.

Macrobenchmarks SHOULD use deterministic local fixtures.

## 80.2 Cross-runtime comparison corpus

Maintain a small separate repository/directory containing semantically equivalent versions for:

```text
mojito-async
Tokio
Go
Java virtual threads
native OS-thread baseline
```

Only compare operations with equivalent semantics.

For example, do not compare a bounded-channel workload to an unbounded queue without disclosing the difference.

## 80.3 Soak tests

Required soak scenarios:

- millions of spawn/join cycles;
- repeated runtime create/shutdown;
- 24h-equivalent virtual timer churn where virtual time can substitute;
- prolonged real network churn;
- repeated connection open/close with descriptor reuse;
- randomized cancellation;
- stack cache grow/shrink;
- blocking-pool saturation/recovery;
- memory-usage steady-state check.

Soak tests MUST detect:

- monotonically growing RSS after warm stabilization;
- leaked tasks;
- leaked stacks;
- leaked descriptors/handles;
- unbounded queue growth;
- stuck workers;
- stuck scopes.

## 80.4 CI test tiers

Adopt a tiered model similar in spirit to OpenJDK's test tiers.

### Tier 0 — developer fast loop

Target: seconds.

Run:

- focused conformance;
- tiny deterministic schedule tests;
- compile checks;
- selected microbenchmark smoke checks.

### Tier 1 — every pull request

Target: practical presubmit duration.

Run:

- full public conformance;
- deterministic schedule tests with conservative bounds;
- actor/outcome quick mode;
- integration tests;
- feature/configuration matrix;
- fuzz target build + bounded fuzz smoke;
- sanitizer subset where available;
- benchmark compilation;
- selected baseline-vs-candidate performance smoke.

### Tier 2 — nightly

Run:

- higher-bound schedule exploration;
- long actor/outcome stress;
- fuzzing for extended durations;
- ASan/TSan/UBSan/native memory checks;
- full reactor/backend integration;
- complete microbenchmark suite;
- macrobenchmarks;
- memory scaling.

### Tier 3 — weekly/release qualification

Run:

- long soak workloads;
- highest feasible task/timer/socket cardinality;
- dedicated-hardware performance A/B;
- all supported architectures/OSes;
- kernel/backend qualification;
- cross-runtime informational comparisons;
- profiler/debugger compatibility checks.

## 80.5 Performance dashboard

Store historical benchmark results keyed by:

```text
commit
Mojo version
mojito-sys version
OS/kernel
architecture
CPU
runtime configuration
benchmark schema version
```

A benchmark whose implementation changes materially MUST increment its schema/version so old and new values are not silently treated as directly comparable.

## 80.6 Release validation rule

A release candidate cannot be declared production-ready only because unit/integration tests pass.

Required evidence includes:

```text
conformance PASS
schedule-exploration PASS
stress forbidden outcomes = 0
fuzz/sanitizer gate PASS
target reactor integration PASS
performance regression review complete
soak/resource-leak gate PASS
```

A known failing external kernel/liburing regression may be waived only when documented as an environment defect with a reproducible upstream reference.

---

# 81. Initial performance budgets

These are engineering targets, not promises.

## 81.1 No-use tax

- no worker threads;
- no runtime heap allocation;
- no scheduler TLS access on ordinary calls.

## 81.2 Fiber creation

Prefer amortized allocation from pools.

## 81.3 Context switch

Target competitive low-level fiber switch cost; establish platform-specific budgets after the first native prototype rather than inventing one universal number.

## 81.4 Idle task memory

Primary metric is **committed physical memory per idle task**, not virtual reservation size.

The implementation SHOULD pursue a committed-memory target measured in tens of KiB initially and reduce it through evidence.

---

# 82. Runtime shutdown

`Runtime.shutdown()` sequence:

```text
reject new root work
        |
        v
cancel root scopes
        |
        v
join/cancel outstanding tasks
        |
        v
drain reactor completions
        |
        v
stop blocking pool
        |
        v
stop workers
        |
        v
release caches/stacks
```

Define a bounded forced-shutdown API separately if needed.

Never silently abandon Mojo-owned values requiring destruction.

---

# 83. Process `fork()`

On POSIX, `fork()` with active worker threads is hazardous.

MVP policy:

- do not support continuing the concurrency runtime in the child after `fork()`;
- require `exec()` or explicit reinitialization if a supported path is later implemented;
- document this prominently.

---

# 84. Signals

Do not allow signal handlers to invoke general concurrency APIs.

Only async-signal-safe native operations may occur in signal context.

If signal integration is desired, use a pipe/eventfd-like bridge into the reactor.

---

# 85. Priority

Do not implement complex priorities initially.

If a priority field exists, keep it internal/experimental.

Priority scheduling interacts with:

- fairness;
- inversion;
- mutex ownership;
- work stealing.

Add only with a concrete workload.

---

# 86. Backpressure

Backpressure must be explicit.

Examples:

- bounded channels park senders when full;
- blocking pool has bounded queue/thread count;
- reactor submission may park/retry when kernel queue is saturated;
- global task injection may use limits for externally submitted unbounded work.

Structured concurrency does not by itself prevent resource explosion.

---

# 87. Recursive spawn control

For recursive parallel algorithms, avoid allocating an unbounded number of tasks.

Pattern:

```text
if work <= grain:
    compute inline
else:
    spawn one half
    compute other half locally
    join spawned half
```

This retains work-first behavior and reduces task overhead.

---

# 88. Work-first vs help-first policy

Default CPU task spawn SHOULD prefer work-first semantics where practical:

```text
spawn child
continue useful work in parent
```

or a measured variant that improves locality.

Benchmark divide-and-conquer workloads before finalizing.

---

# 89. Runtime configuration

Provide explicit optional configuration:

```mojo
struct RuntimeConfig:
    var worker_count: Int
    var max_blocking_threads: Int
    var stack_reserve_bytes: Int
    var stack_initial_commit_bytes: Int
    var enable_tracing: Bool
```

Defaults should use detected hardware/runtime guidance.

Do not expose dozens of scheduler knobs in the stable API.

---

# 90. Global runtime vs explicit runtime

MVP recommendation:

- `run()` creates/uses one process-default runtime;
- internal runtime handle is ambient through worker/task context;
- tests may construct isolated runtime instances through an advanced API.

Ordinary functions do not accept `Runtime&`.

---

# 91. API rule: never duplicate for async

Prohibited naming pattern:

```text
read()
read_async()

send()
send_async()

map()
map_async()
```

Preferred:

```text
read()
send()
map()
```

If a non-blocking probe is useful, call it by semantics:

```text
try_read()
try_send()
```

This is not function coloring; it represents different behavior.

---

# 92. API rule: distinguish concurrency from parallelism only where semantics differ

`spawn()` means an independently schedulable child.

`parallel_for()` communicates bulk data-parallel intent and permits chunking/vectorization/backend choices.

Both use ordinary functions.

---

# 93. API rule: avoid executor infection

Do not require:

```mojo
def foo(executor: Executor):
    bar(executor)
```

The runtime is ambient only to operations that need it.

An explicit custom runtime/executor API may exist at the root or advanced layer.

---

# 94. API rule: cancellation token optional in signatures

Do not require every function to accept `CancellationToken`.

The current task has structured cancellation state.

Pass explicit tokens only when crossing or composing cancellation domains.

---

# 95. API rule: timeout wrappers should not create colored callback types

Good:

```mojo
var result = with_timeout(Seconds(2), lambda: fetch())
```

Avoid:

```text
with_timeout(async closure -> Future[T])
```

---

# 96. Example: concurrent request fan-out

```mojo
from mojito_async import Scope, run
from mojito_async.io import http

def load_user_page(user_id: Int) raises -> Page:
    with Scope() as scope:
        var profile = scope.spawn(
            lambda: http.get("/profile/" + String(user_id))
        )
        var posts = scope.spawn(
            lambda: http.get("/posts/" + String(user_id))
        )
        var permissions = scope.spawn(
            lambda: http.get("/permissions/" + String(user_id))
        )

        return Page(
            parse_profile(profile.join()),
            parse_posts(posts.join()),
            parse_permissions(permissions.join()),
        )

def app() raises:
    var page = load_user_page(42)
    render(page)

def main() raises:
    run(app)
```

Every function is ordinary direct style.

---

# 97. Example: channel pipeline

```mojo
def producer(tx: Sender[Int]) raises:
    for i in range(1_000_000):
        tx.send(i)
    tx.close()

def consumer(rx: Receiver[Int]) raises -> Int:
    var total = 0
    while var value = rx.recv():
        total += value^
    return total

def pipeline() raises -> Int:
    with Scope() as scope:
        var channel = Channel[Int](capacity=1024)

        var p = scope.spawn(
            lambda: producer(channel.sender())
        )
        var c = scope.spawn(
            lambda: consumer(channel.receiver())
        )

        p.join()
        return c.join()
```

No callback inversion and no Future chain.

---

# 98. Example: task-aware mutex

```mojo
def update(counter: Mutex[Int]) raises:
    with counter.lock() as value:
        value[] += 1
```

If uncontended, `lock()` is an atomic fast path.

If contended, the task parks without blocking its OS worker.

---

# 99. Example: blocking FFI

```mojo
def load_legacy_record(id: Int) raises -> Record:
    return blocking(
        lambda: legacy_c_database_lookup(id)
    )
```

The function remains colorless, but the source explicitly acknowledges a blocking foreign boundary.

---

# 100. Implementation phases

## Phase A0 SPIKE — colorless parking and structured-task feasibility

This is the **mandatory `mojito-async` go/no-go spike**. It begins only after the required `mojito-sys` handoff gate has passed.

The `mojito-sys` spike proves that the machine can safely suspend and resume an ordinary Mojo stack.

This spike answers the next, different question:

> **Can that mechanism support a minimal colorless structured-concurrency system in which ordinary Mojo functions spawn children, park without blocking the worker, wake exactly once, join results, propagate errors/cancellation, and preserve scope lifetimes?**

The spike MUST prove the concurrency semantics before the project commits to a production M:N scheduler, channels, networking, or compiler integration.

### A0.1 Spike hypothesis

The falsifiable hypothesis is:

> Using only the public `mojito-sys` contract, a small scheduler written in Mojo can run ordinary direct-style Mojo tasks, suspend a task at an explicit parking operation, execute another task on the same OS worker, resume the first task later, and preserve structured task/result/error semantics without `async`, `await`, `Future[T]`, `Task.wait()`, or OS-thread blocking.

### A0.2 Spike scope

Implement only:

```text
run()
 |
 v
one OS worker
 |
 +--> root Scope
 |
 +--> minimal TaskControlBlock
 |
 +--> spawn()
 |
 +--> JoinHandle.join()
 |
 +--> one reusable WaitNode per task
 |
 +--> Event.wait()/set()
 |
 +--> cancellation flag
 |
 `--> minimal FIFO runnable queue
```

The spike SHALL use:

- one `mojito-sys.NativeThread` or the calling thread as the sole worker;
- `mojito-sys.NativeStack`;
- `mojito-sys.NativeContext`;
- `mojito-sys` TLS only if required;
- a simple queue, not a lock-free work-stealing deque;
- a simple event/wait primitive sufficient to exercise parking.

The spike SHALL NOT implement:

- multiple workers;
- work stealing;
- networking;
- epoll/kqueue/IOCP;
- general channels;
- `select`;
- blocking pool;
- CPU parallel algorithms;
- task migration;
- general algebraic effects;
- compiler changes.

### A0.3 Required prototype

The minimum successful user-facing demonstration is:

```mojo
def child(event: Event) raises -> Int:
    var x = ComplexValue(...)
    event.wait()               # parks this fiber, not the OS worker
    return use(x)

def other_work(event: Event) raises:
    # Must execute while child is parked.
    do_observable_work()
    event.set()

def demo() raises -> Int:
    with Scope() as scope:
        var a = scope.spawn(lambda: child(event))
        var b = scope.spawn(lambda: other_work(event))

        b.join()
        return a.join()

def main() raises:
    print(run(demo))
```

Required execution behavior:

```text
root task
   |
   +-- spawn child A
   +-- spawn child B
   |
run A
   |
Event.wait()
   |
park A ----------------------.
   |                         |
   v                         |
scheduler                    |
   |                         |
run B                        |
   |                         |
Event.set() -----------------'
   |
wake A exactly once
   |
complete B
   |
run A
   |
resume after Event.wait()
   |
complete A
   |
join results
   |
scope closes with zero children
```

At no point may `Event.wait()` block the OS worker thread while runnable task B exists.

### A0.4 Mandatory semantic tests

#### A0-T1 — ordinary-function colorlessness

All application-level functions in the spike use ordinary `def`.

The spike fails this test if correctness requires:

- `async`;
- `await`;
- `Future`;
- polling callbacks;
- a different function type for suspendable functions.

#### A0-T2 — actual worker reuse while parked

When task A parks, task B MUST execute on the same OS worker before A is resumed.

This proves the system is not disguising OS-thread blocking.

#### A0-T3 — exact resume point

After wake, A resumes immediately after the parking call with its ordinary stack state intact.

#### A0-T4 — join-before-completion

Parent calls `join()` before child completion.

`join()` parks the parent and another runnable task progresses.

#### A0-T5 — join-after-completion

Completed result moves into the joiner without parking.

#### A0-T6 — one-shot join

A result is consumed exactly once; double join is rejected or impossible by the chosen type/state model.

#### A0-T7 — result destruction

An unconsumed/abandoned result is destroyed exactly once.

#### A0-T8 — child error propagation

A child raises after at least one park/resume cycle.

`join()` reproduces the expected error behavior.

#### A0-T9 — sibling cancellation

One child fails; the scope requests cancellation of a sibling parked on the test event.

The parked sibling becomes runnable due to cancellation and terminates cleanly.

#### A0-T10 — cancellation/readiness race

Run readiness and cancellation against the same waiter repeatedly.

Exactly one wake reason wins.

#### A0-T11 — wake-before-park race

Force the event to become ready during the prepare/validate/commit window.

The task MUST NOT sleep forever.

#### A0-T12 — duplicate wake defense

Issue repeated `set()`/wake attempts.

The same waiter generation MUST NOT enqueue the task more than once.

#### A0-T13 — scope containment

On scope exit:

```text
live child count == 0
```

No child continues after the scope storage or borrowed state it is allowed to use has disappeared.

#### A0-T14 — nested scopes

A child opens its own scope, parks descendants, and exits in correct structured order.

#### A0-T15 — no hidden OS wait

Instrument the worker so the test can establish that task parking uses `NativeContext` switching rather than a blocking OS synchronization wait while runnable tasks remain.

#### A0-T16 — allocation accounting

Record allocations for:

- task creation;
- first stack creation;
- park/wake;
- completed join.

The common park/wake operation SHOULD use the TCB's embedded waiter and perform no per-park heap allocation.

### A0.5 Minimal task state machine to prove

The spike SHALL implement and test at least:

```text
NEW
 |
 v
RUNNABLE
 |
 v
RUNNING
 |     |     \ completion
 |      v
 |   COMPLETED
 |
 v
PARKING
 |
 +---- early wake ----> RUNNABLE
 |
 v
WAITING
 |
 +---- readiness -----> RUNNABLE
 |
 `---- cancellation --> RUNNABLE
```

The spike is specifically intended to validate ownership of these transitions before production optimization.

### A0.6 Parking algorithm to prove

Implement the simplest correct version of:

```text
PREPARE
  publish waiter

VALIDATE
  recheck condition

COMMIT
  RUNNING -> PARKING
  switch to scheduler

WAKE
  one actor claims waiter generation
  PARKING/WAITING -> RUNNABLE
  enqueue once
```

The spike MUST contain deterministic hooks that force wakes at each boundary.

### A0.7 Required measurements

Measure at minimum:

- spawn + completed join;
- park + wake + resume round trip;
- context switches per park/wake cycle;
- allocations per spawn;
- allocations per park;
- bytes committed per task stack;
- task-control-block size;
- Event fast-path latency;
- completed-join fast-path latency.

The spike does not need production performance, but it MUST identify where every material cost comes from.

### A0.8 Spike deliverables

The spike SHALL produce:

1. `spike/colorless_runtime/`;
2. minimal `run`;
3. minimal `Scope`;
4. minimal TCB;
5. minimal fiber wrapper using only `mojito-sys`;
6. FIFO run queue;
7. `spawn`;
8. `JoinHandle.join`;
9. `Event`;
10. cancellation flag/tree sufficient for the tests;
11. deterministic race hooks;
12. tests A0-T1 through A0-T16;
13. microbenchmark harness;
14. `SPIKE_REPORT.md`.

The report MUST contain:

```text
Hypothesis
Exact mojito-sys version/contract used
Prototype architecture
Task-state transition table
Parking/wakeup protocol
Test pass/fail matrix
Race scenarios exercised
Allocation measurements
Latency measurements
Known semantic gaps
Go / conditional-go / no-go recommendation
Changes required in mojito-sys, if any
Changes requiring Mojo compiler support, if any
```

### A0.9 Explicit pass criteria

The spike is a **GO** only if:

- ordinary `def` functions can suspend through library operations without source coloring;
- parking one task allows another runnable task to execute on the same worker;
- join parks rather than blocks the worker;
- wakeup is exactly-once under forced race schedules;
- cancellation can wake a parked task;
- child errors propagate through join;
- structured scope exit leaves no live children;
- no current Mojo async/coroutine API is required;
- no OS/native calls bypass `mojito-sys`;
- no per-park heap allocation is required in the demonstrated common path;
- no correctness issue requires an application-visible `Future` or executor parameter.

### A0.10 Conditional-go criteria

A **CONDITIONAL GO** is acceptable for bounded issues such as:

- move-only callable ergonomics are awkward in the current Mojo release;
- scoped borrowed captures require a temporarily conservative owned-only API;
- debugging/backtraces are incomplete;
- scheduler performance is poor but state semantics are correct;
- one optimization requires a future Mojo compiler feature.

Each condition must have a contained mitigation that does not introduce function coloring.

### A0.11 No-go criteria

The library-first colorless route is a **NO-GO** if correctness requires any of the following:

- blocking the only OS worker when a task waits;
- exposing `async`/`await` or Future-returning function types throughout call chains;
- private Modular async/compiler-runtime APIs;
- bypassing structured lifetimes in ways Mojo cannot make sound;
- task parking that cannot be made race-free with the available atomics/context primitives;
- a hidden executor/context parameter threaded through ordinary function calls;
- reliance on task migration to make basic semantics work.

A no-go result SHOULD trigger a focused compiler proposal for a public one-shot `Suspend`/continuation primitive rather than a compromised library API.

### A0.12 Spike completion rule

A0 is complete only when `SPIKE_REPORT.md` contains one of:

```text
GO
CONDITIONAL GO
NO-GO
```

and the decision explicitly covers:

```text
colorlessness
worker non-blocking behavior
parking correctness
structured lifetime behavior
error/cancellation behavior
allocation model
```

Merely reproducing the `mojito-sys` context-switch test is insufficient.

---

## Phase A1 — single-worker structured fibers

Deliver:

- `run`;
- `Scope`;
- `spawn`;
- `JoinHandle`;
- TCB/result storage;
- single worker scheduler;
- `_suspend_current`;
- park/wake;
- cancellation;
- `yield_now`;
- `Event`;
- task-aware `Mutex`;
- bounded `Channel`;
- timer heap.

Exit criteria:

- 100k task lifecycle stress test;
- lost-wakeup suites pass;
- nested scopes pass;
- cancellation storm passes;
- no task/result/stack leaks.

---

## Phase A2 — M:N worker scheduler

Deliver:

- N `mojito-sys.NativeThread` workers;
- local queues;
- global injection;
- remote-ready queues;
- unstarted task stealing;
- worker-affine started fibers;
- per-worker TCB/stack caches;
- worker sleep/wake through `mojito-sys`.

Exit criteria:

- CPU scaling demonstrated;
- no started fiber migrates;
- cross-worker wake stress passes;
- thread sanitizer / model tests clean where supported.

---

## Phase A3 — reactor networking

Deliver:

- reactor;
- readiness/completion adapter to `mojito-sys`;
- TCP connect/accept/read/write;
- I/O cancellation;
- deadline integration;
- scheduler fairness between CPU and I/O work.

Exit criteria:

- high-concurrency echo benchmark;
- no scheduler worker blocks on socket readiness;
- readiness/cancellation/timeout race tests pass.

---

## Phase A4 — blocking pool + production hardening

Deliver:

- bounded blocking pool using `mojito-sys.NativeThread`;
- `blocking()`;
- shutdown semantics;
- metrics/tracing;
- task naming;
- stack/TCB cache tuning;
- fault injection.

Exit criteria:

- blocking saturation preserves scheduler progress;
- shutdown/leak tests pass;
- documented platform limitations.

---

## Phase A5 — richer synchronization and `select`

Deliver:

- semaphore;
- condvar;
- barrier;
- RW lock if justified;
- multi-wait `select`;
- timer/select integration.

Exit criteria:

- model tests for selection winner;
- cancellation/timeout/readiness race tests pass.

---

## Phase A6 — origin-aware capture safety

Deliver as public Mojo capabilities permit:

- sound scoped borrowed captures;
- move-only task environments;
- experimental `Sendable`/`Shareable` traits only if needed;
- compile-fail tests.

If public Mojo cannot express a guarantee soundly, keep the capability unavailable or explicitly unsafe.

---

## Phase A7 — CPU parallel library

Deliver:

- `join`;
- `parallel_for`;
- chunk scheduler;
- reduction;
- work-first heuristics;
- benchmarks against existing Mojo parallel primitives.

---

## Phase A8 — compiler `Suspend` prototype

Requires compiler collaboration/fork/public extension point.

Deliver:

- compiler-recognized one-shot suspension intrinsic;
- effect inference;
- `no_suspend` verification;
- stackful lowering compatible with current runtime semantics;
- `mojito-sys.NativeContext` remains fallback.

---

## Phase A9 — adaptive continuation lowering

Deliver research prototype:

- suspension/control-flow analysis;
- stackless lowering when profitable;
- stackful fallback;
- full elimination when suspension is unreachable;
- unchanged public application source.

---

## Phase A10 — capability/capture integration

Deliver research prototype:

- compiler suspension capability;
- higher-order effect polymorphism;
- spawn-boundary separation checking;
- origins/capture integration;
- minimal notation.

# 101. Milestone dependency graph

```text
mojito-sys S0-S5 handoff
          |
          v
A0 colorless-runtime SPIKE
          |
          v
A1 structured single-worker productionization
          |
          v
A2 M:N scheduler
       /     \
      v       v
A3 reactor   A7 CPU parallel
      |
      v
A4 hardening/blocking
      |
      v
A5 select/sync
      |
      v
A6 capture safety
      |
      v
A8 compiler Suspend intrinsic
      |
      v
A9 adaptive lowering
      |
      v
A10 capability/effect integration
```

Compiler research may begin in parallel, but the library implementation should not bypass the `mojito-sys` handoff gate.

# 102. Definition of zero-cost for this project

The term **zero-cost abstraction** SHALL have an explicit engineering meaning.

## ZC-1 — No-use zero cost

If the concurrency runtime is never entered:

```text
threads created = 0
fiber stacks reserved = 0
scheduler heap allocations = 0
reactor initialized = false
ordinary-call checks = 0
```

## ZC-2 — Ordinary-call zero cost

Once inside `run()`, calling an ordinary non-concurrency function does not:

- allocate;
- construct Future state;
- inspect scheduler state;
- poll cancellation automatically;
- perform virtual dispatch because concurrency exists elsewhere.

## ZC-3 — Fast-path cost only where semantic work exists

Examples:

- uncontended mutex: atomic operation(s);
- channel with ready data: ring operations;
- completed join: result state check + move;
- ready socket operation: syscall/library path as required.

## ZC-4 — Suspension pays suspension cost

Actual suspension legitimately pays for:

- waiter registration;
- context save;
- scheduler transition;
- queue operations;
- later restoration.

The project SHALL NOT claim suspension itself costs zero.

---

# 103. Performance anti-patterns to reject in review

Reject changes that introduce any of these without compelling benchmark evidence:

- one heap-allocated Future per operation;
- reference counting every task merely for joins;
- virtual dispatch on every scheduler transition;
- global lock for every enqueue;
- one OS thread per task;
- blocking scheduler workers on user synchronization;
- copying stacks during growth;
- universal sequentially-consistent atomics;
- task scanning to find completed I/O;
- O(n) cancellation walks on hot paths where indexing/tree structure can avoid it;
- per-call runtime-context propagation;
- hidden scheduler checks in ordinary functions.

---

# 104. Code-review checklist

Every runtime PR SHOULD answer:

1. Can this operation suspend?
2. If yes, where is the exact suspension boundary?
3. Can a wake occur before parking?
4. Which actor owns each state transition?
5. What memory ordering publishes the transition?
6. Can the task be queued twice?
7. Can cancellation race readiness?
8. Can a stale event reference a recycled task?
9. Does any path block an OS scheduler worker?
10. Is there a heap allocation on the fast path?
11. Can a Mojo reference become invalid because storage moved?
12. Does the change preserve scope lifetime invariants?
13. What happens during runtime shutdown?
14. What benchmark proves the optimization?
15. What deterministic/stress test covers the race window?

---

# 105. Security/correctness considerations

Treat runtime memory unsafety as security-critical.

Particular risks:

- context ABI mismatch;
- stack overflow into adjacent mappings;
- stale waiter pointer;
- use-after-free TCB;
- recycled I/O token;
- double wake;
- double join result move;
- scope destroyed with live child;
- native callback after task destruction;
- foreign operation retaining Mojo pointers;
- cross-worker misuse of worker-local data.

Use:

- generation counters;
- guard pages;
- explicit state ownership;
- intrusive lifetime rules;
- debug poison values;
- optional quarantine of recycled TCBs/stacks in debug builds.

---

# 106. Debug mode

A debug runtime SHOULD enable:

- task-state transition assertions;
- queue-membership assertions;
- waiter generation checks;
- owner-worker checks;
- stack guard checks;
- double-join detection;
- scope-child leak detection;
- cancellation-tree validation;
- expensive scheduler consistency scans on demand.

Release mode should compile most checks out.

---

# 107. Fault injection

Test-only hooks SHOULD be able to:

- fail allocation;
- delay wake;
- reorder ready queue insertion;
- force cancellation at each park phase;
- force timer/readiness ties;
- force reactor EAGAIN/retry paths;
- exhaust blocking pool;
- force stack growth;
- force worker shutdown while work remains.

---

# 108. Documentation requirements

Public docs MUST explain:

- colorless does not mean preemptive;
- CPU-bound tasks must yield or use parallel APIs;
- task-aware vs OS-thread locks;
- blocking FFI must use `blocking()`;
- cancellation is cooperative;
- scope exit waits for children;
- no duplicated async APIs are necessary;
- fibers are an implementation detail;
- current platform limitations.

Do not teach users the context-switch ABI in the introductory guide.

---

# 109. Compatibility rules

Before stable 1.0 of this library:

- runtime/native ABI may evolve;
- internal fiber layout may change;
- scheduler policy may change;
- public structured API should remain as stable as possible.

After stable 1.0:

- `Scope`, task/join semantics, cancellation behavior, direct-style synchronization behavior become compatibility commitments;
- representation remains explicitly non-ABI-stable unless separately documented.

---

# 110. Proposed public v0.1 surface

Start small:

```text
run
Scope
JoinHandle
CancellationToken
Deadline
sleep
yield_now
checkpoint

Mutex
Semaphore
Event

Channel
Sender
Receiver

blocking
```

Do not ship every synchronization primitive before the parking kernel is proven.

---

# 111. Proposed internal v0.1 surface

```text
Runtime
Worker
TaskControlBlock
Fiber
FiberStack
NativeContext
LocalRunQueue
InjectQueue
RemoteReadyQueue
ParkingLot
WaitNode
CancelState
ScopeState
TimerHeap
TaskAllocator
StackCache
```

---

# 112. Initial repository work breakdown

## Epic ASYNC-A — A0 colorless-runtime spike + task runtime
- A0.1 consume and verify the `mojito-sys` handoff contract
- A0.2 build the one-worker spike harness
- A0.3 minimal TCB and task state machine
- A0.4 minimal fiber wrapper over `NativeStack`/`NativeContext`
- A0.5 FIFO runnable queue
- A0.6 `spawn` + one-shot `JoinHandle`
- A0.7 `Event`-based park/wake path
- A0.8 cancellation/readiness race harness
- A0.9 deterministic wake-before-park tests
- A0.10 allocation and latency instrumentation
- A0.11 `SPIKE_REPORT.md` and go/no-go review
- A1.1 production runtime initialization after spike approval
- A1.2 production TCB/task allocator
- A1.3 entry/completion trampoline integration
- A1.4 result storage and lifecycle hardening

## Epic ASYNC-B — structured scopes
- B1 scope tree
- B2 child registration
- B3 close/join
- B4 failure propagation
- B5 cancellation tree
- B6 nested-scope tests

## Epic ASYNC-C — parking kernel
- C1 embedded waiter
- C2 task state transitions
- C3 prepare/validate/commit park
- C4 idempotent wake
- C5 remote-ready routing
- C6 generation protection
- C7 deterministic scheduler
- C8 race/fault tests

## Epic ASYNC-D — synchronization
- D1 Event
- D2 Mutex
- D3 Semaphore
- D4 bounded Channel
- D5 close semantics
- D6 stress/model tests

## Epic ASYNC-E — M:N scheduler
- E1 worker pool
- E2 local queue
- E3 injection queue
- E4 unstarted-task stealing
- E5 started-fiber affinity
- E6 idle worker sleep/wake
- E7 fairness
- E8 CPU scaling benchmark

## Epic ASYNC-F — timing
- F1 timer heap
- F2 sleep
- F3 deadlines
- F4 timeout scope
- F5 timer cancellation
- F6 timer-scale benchmark

## Epic ASYNC-G — reactor
- G1 native-poller adapter
- G2 operation tokens/generations
- G3 TCP connect
- G4 accept
- G5 read/write
- G6 I/O cancellation
- G7 readiness/completion race tests
- G8 echo benchmark

## Epic ASYNC-H — blocking compatibility
- H1 bounded blocking pool
- H2 `blocking()`
- H3 backpressure
- H4 shutdown
- H5 cancellation result-abandon semantics
- H6 saturation benchmark

## Epic ASYNC-I — hardening
- I1 metrics
- I2 tracing
- I3 debug assertions
- I4 fault injection
- I5 leak/stress CI
- I6 API docs
- I7 platform support matrix

## Epic ASYNC-J — compiler evolution
- J1 semantic `Suspend` proposal
- J2 intrinsic prototype
- J3 `no_suspend`
- J4 effect inference
- J5 adaptive lowering
- J6 capture/capability integration

# 113. Prototype acceptance test

The first compelling demonstration SHOULD be a program with this shape:

```mojo
def fetch(id: Int) raises -> Bytes:
    var conn = TcpStream.connect(server)
    conn.write_all(make_request(id))
    return conn.read_to_end()

def service(ids: List[Int]) raises -> List[Bytes]:
    with Scope() as scope:
        var handles = List[JoinHandle[Bytes]]()

        for id in ids:
            handles.append(
                scope.spawn(lambda id=id: fetch(id))
            )

        var results = List[Bytes]()
        for var handle in handles:
            results.append(handle^.join())

        return results

def main() raises:
    var results = run(lambda: service(load_ids()))
```

Acceptance criteria:

- none of `fetch`, `service`, helpers, parsing functions, or callbacks is `async`;
- no `await`;
- no `Future`;
- blocked sockets do not block scheduler workers;
- thousands of requests can be outstanding;
- scope exit cannot leak children;
- cancellation tears down pending operations;
- ordinary computation remains ordinary compiled Mojo.

---

# 114. Compiler-collaboration proposal

Once the library validates semantics and benchmarks, propose a minimal compiler/runtime interface rather than a large async redesign.

Minimal requested primitives:

```text
continuation.suspend(reason)
continuation.resume(handle)
current_task_context()
no_suspend verification
effect/capability metadata channel
```

The first compiler proposal SHOULD NOT request general algebraic effects.

Prove the single `Suspend` use case first.

---

# 115. Why this path is preferable to rewriting Mojo async immediately

A library-first approach provides:

- real workloads before language syntax is frozen;
- measured context-switch/stack costs;
- measured task-memory requirements;
- synchronization semantics tested independently of compiler transformations;
- stable user semantics that compiler optimization can target;
- an escape hatch if compiler priorities differ;
- direct evidence for which primitives need first-class compiler support.

The library is therefore both a useful runtime and an executable language-design experiment.

---

# 116. Architectural decisions record

## ADR-000 — Mojito package split

**Decision:** infrastructure is `mojito-sys`; colorless concurrency is `mojito-async`.  
**Reason:** create a coherent package family while enforcing a strict mechanism/policy dependency boundary.


## ADR-001 — Direct style is the public model

**Decision:** ordinary `def`; no mandatory `async`/`await`.  
**Reason:** eliminate function coloring rather than rename it.

## ADR-002 — Stackful fibers are the MVP suspension representation

**Decision:** preserve call stacks across park.  
**Reason:** only practical library-first route to arbitrary-depth direct-style suspension.

## ADR-003 — Fibers are not public API

**Decision:** public concepts are scope/task/join/synchronization.  
**Reason:** preserve future representation freedom.

## ADR-004 — Structured concurrency is mandatory for safe spawn

**Decision:** safe tasks belong to scopes.  
**Reason:** lifetime, cancellation, error and resource containment.

## ADR-005 — One-shot join/continuations

**Decision:** results are consumed once.  
**Reason:** simpler ownership and zero-refcount fast path.

## ADR-006 — Started fibers are worker-affine in Phase 1

**Decision:** steal only unstarted tasks.  
**Reason:** reduce TLS/FFI/migration complexity.

## ADR-007 — Live stacks never relocate

**Decision:** virtual reserve + commit.  
**Reason:** protect Mojo stack references.

## ADR-008 — Scheduler is lazy

**Decision:** initialize on explicit concurrency entry.  
**Reason:** preserve pay-for-what-you-use semantics.

## ADR-009 — Blocked tasks park; blocked foreign calls use a blocking pool

**Decision:** scheduler workers do not wait on user/legacy blocking operations.  
**Reason:** maintain M:N scalability.

## ADR-010 — Suspension funnels through one internal semantic seam

**Decision:** `_suspend_current`.  
**Reason:** future compiler replacement.

## ADR-011 — Future effect analysis is inferred, not source-colored

**Decision:** compiler may know a function can suspend without requiring `async`.  
**Reason:** separate analysis from API coloring.

## ADR-012 — Future negative annotation is `no_suspend`

**Decision:** constrain rare non-suspendable regions.  
**Reason:** color the restriction, not the common capability.

## ADR-013 — Capture safety is enforced at concurrency boundaries

**Decision:** focus stronger checking at `spawn`.  
**Reason:** parallel overlap, not suspension alone, causes race risk.

## ADR-014 — CPU/GPU share structure, not execution representation

**Decision:** common scopes/dependencies; backend-specific execution.  
**Reason:** avoid false abstraction.

---

# 117. Open technical questions

These require prototype evidence or Mojo compiler collaboration.

1. Can current `OriginSet` express a sound scoped borrowed task closure without compiler changes?
2. What callable trait syntax provides zero-overhead generic task entries without type erasure?
3. Can TCB/result containers remain fully move-oriented under the target Mojo stdlib?
4. Should `mojito-async` reuse any future public Modular CPU scheduling primitive, and at which abstraction boundary?
5. Which operations, if any, justify started-fiber migration after the worker-affine MVP?
6. What scheduler policy best balances reactor latency and CPU throughput?
7. Should task cancellation remain a flag/tree or become a compiler-visible capability later?
8. How should child-error aggregation interact with Mojo's evolving typed `raises` model?
9. What is the minimum sound public API for scoped borrowed captures?
10. Should `select` be a built-in library primitive or reducible to a generalized waiter registration interface?
11. At what point should timer heaps become hierarchical wheels?
12. When `mojito-sys` offers both readiness and completion pollers, should the reactor expose one normalized operation state machine or specialized backends?
13. How should profiler task/fiber identities be surfaced independently of native stack representations?
14. Can continuation/suspension metadata be expressed through public compiler/MLIR extension points without private Modular dialect dependencies?
15. What evidence threshold should trigger replacing a stackful suspension with compiler-generated stackless lowering?

# 118. Explicit implementation rule for evolving Mojo syntax

Mojo is still stabilizing. Therefore:

- code snippets in this specification express required semantics;
- the implementation SHALL use the latest stable public equivalent syntax;
- it SHALL NOT depend on private compiler-runtime symbols solely to make a snippet compile;
- any deviation caused by public API limitations MUST be documented in an ADR;
- public concurrency semantics should remain stable even if internal Mojo syntax or traits change.

---

# 119. Final architecture

```text
                    APPLICATION CODE
              ordinary colorless Mojo defs
                          |
                          v
          +--------------------------------+
          |      structured API surface    |
          | Scope / spawn / join / channel |
          | mutex / timer / I/O / blocking |
          +----------------+---------------+
                           |
                           v
          +--------------------------------+
          |      semantic suspension seam  |
          |       _suspend_current()        |
          +----------------+---------------+
                           |
              Phase 1      |      Future
             .-------------+--------------.
             v                            v
     +---------------+            +----------------+
     | stackful fiber|            | compiler        |
     | context switch|            | Suspend effect  |
     +-------+-------+            | / continuation  |
             |                    +--------+---------+
             |                             |
             '--------------.  .-----------'
                            v  v
                     +-------------+
                     |  scheduler  |
                     +------+------+ 
                            |
               .------------+-------------.
               v                          v
         +-----------+              +-----------+
         | CPU work  |              | I/O/react |
         | M:N pool  |              | + timers  |
         +-----------+              +-----------+

Compiler evolution:

ordinary source
      |
      v
infer Suspend/captures
      |
      +--> no suspension reachable --> ordinary machine code
      |
      +--> profitable stackless -----> state-machine continuation
      |
      +--> deep/opaque suspension ----> stackful continuation
      |
      `--> device work ---------------> device-specific lowering
```

The user-visible concurrency model does not change as these optimizations arrive.

---

# 120. Implementation mandate

The project is conditional on `mojito-sys`.

Before `mojito-async` advances beyond Phase A0, the infrastructure handoff MUST establish:

1. **ordinary Mojo call stacks suspend and resume correctly through `mojito-sys.NativeContext`;**
2. **live stack-backed Mojo references remain valid because `NativeStack` never relocates;**
3. **destructor and `raises` behavior remain sound across suspension;**
4. **worker threads, TLS, wait/wake, monotonic time, sockets, and a primary poller are available without private compiler-runtime dependencies;**
5. **the measured context-switch cost and committed stack memory are competitive enough to justify the stackful MVP.**

If those conditions fail, do not wrap a thread-blocking API and market it as colorless concurrency.

Instead, narrow the project to the compiler-facing route:

```text
ordinary Mojo def
      |
      v
compiler one-shot Suspend intrinsic
      |
      v
continuation/runtime implementation
```

If the handoff succeeds, proceed with:

```text
mojito-sys
    |
    v
stackful colorless mojito-async
    |
    v
compiler-recognized Suspend
    |
    v
adaptive compiler-selected continuation representation
```

The public concurrency API should remain stable across that evolution.

# Appendix A — `mojito-sys` handoff assumptions

The architecture-specific context layout, C ABI, virtual-memory implementation, and OS poller bindings are specified in `mojito-sys_IMPLEMENTATION_SPEC.md`.

`mojito-async` assumes only the following semantic contract:

```text
NativeStack
    stable address
    guarded
    commit/grow without relocation

NativeContext
    create/capture
    allocation-free switch
    ABI-correct preservation

NativeThread
    create/join
    stable TLS association

NativeEvent
    efficient OS-worker sleep/wake

MonotonicClock
    deadline-safe time base

NativeSocket
    explicit non-blocking operations

NativePoller
    readiness/completion delivery
    opaque token round-trip
    explicit wake
```

Architecture-specific register layouts are deliberately not duplicated here.

---

# Appendix B — Suggested task transition ownership

| Transition | Initiator | Required synchronization |
|---|---|---|
| NEW → RUNNABLE | spawn/runtime | publish-release |
| RUNNABLE → RUNNING | owner worker | queue ownership + state |
| RUNNING → PARKING | running task | CAS/release |
| PARKING → WAITING | owner scheduler | release |
| WAITING → RUNNABLE | wake winner | CAS acquire-release |
| PARKING → RUNNABLE | early wake winner | CAS acquire-release |
| RUNNING → COMPLETING | running task | owner-only |
| COMPLETING → COMPLETED | running task | result publish-release |
| COMPLETED observed by joiner | joiner | acquire |
| any live → cancellation flag | parent/token | atomic release |
| cancellation observed | task/waiter | acquire |

The precise memory model MUST be validated against the finalized queue implementation.

---

# Appendix C — Fast/slow path philosophy

Every commonly used primitive should have this shape:

```text
operation
   |
   v
cheap local/atomic fast path
   |
   +-- success -> return
   |
   `-- contention/not-ready
             |
             v
         slow path
       register waiter
             |
             v
           park
```

This is more important than minimizing the line count of the implementation.

---

# Appendix D — Language/compiler research summary for implementers

The project should internalize five lessons from the research corpus:

1. **Direct style and sophisticated control effects are compatible.**
2. **Effect knowledge does not have to become source-level function coloring.**
3. **One-shot continuations are the relevant low-complexity subset for concurrency.**
4. **Capture/capability tracking can make higher-order effectful code ergonomic.**
5. **Continuation representation should be selected by the compiler/runtime, not embedded in APIs.**

Those lessons are the reason the public library is deliberately designed above the fiber layer.

---

# Appendix E — Source bibliography

### Mojito infrastructure

- `mojito-sys_IMPLEMENTATION_SPEC.md` — required native systems substrate, feasibility gate, and handoff contract.

### Mojo

- Mojo Manual: https://mojolang.org/docs/manual/
- Ownership: https://mojolang.org/docs/manual/values/ownership/
- Lifetimes, origins, and references: https://mojolang.org/docs/manual/values/lifetimes/
- C FFI: https://mojolang.org/docs/manual/c-ffi/
- `Task`: https://mojolang.org/docs/std/runtime/asyncrt/Task/
- `TaskGroup`: https://mojolang.org/docs/std/runtime/asyncrt/TaskGroup/
- Atomics: https://mojolang.org/docs/std/atomic/
- Mojo standard-library repository: https://github.com/modular/modular/tree/main/mojo/stdlib
- Mojo stdlib FAQ / compiler-runtime status: https://github.com/modular/modular/blob/main/mojo/stdlib/docs/faq.md
- Mojo 1.0.0b2 release notes: https://github.com/modular/modular/blob/main/mojo/docs/releases/v1.0.0b2.md

### Runtime testing and performance methodology

- Tokio contributor testing/benchmarking guidance: https://github.com/tokio-rs/tokio/blob/master/docs/contributing/pull-requests.md
- Tokio CI matrix: https://github.com/tokio-rs/tokio/blob/master/.github/workflows/ci.yml
- Loom concurrency permutation testing: https://github.com/tokio-rs/loom
- Tokio deterministic simulation: https://github.com/tokio-rs/simulation
- OpenJDK `jcstress`: https://openjdk.org/projects/code-tools/jcstress/
- OpenJDK JMH: https://openjdk.org/projects/code-tools/jmh/
- OpenJDK tiered testing model: https://github.com/openjdk/jdk/blob/master/doc/testing.md
- Go performance monitoring methodology: https://go.dev/wiki/PerformanceMonitoring
- Go `bent`: https://pkg.go.dev/golang.org/x/benchmarks/cmd/bent
- Go `Sweet`: https://pkg.go.dev/golang.org/x/benchmarks/sweet
- liburing regression tests: https://github.com/axboe/liburing
- Clang ThreadSanitizer: https://clang.llvm.org/docs/ThreadSanitizer.html
- Linux `perf bench`: https://man7.org/linux/man-pages/man1/perf-bench.1.html
- lmbench context-switch benchmark: https://lmbench.sourceforge.net/man/lat_ctx.8.html

### Concurrency, effects, and colorless programming

- Alvarez-Picallo, Freund, Ghica, Lindley. **Effect Handlers for C via Coroutines.** OOPSLA 2024. https://dl.acm.org/doi/10.1145/3689798
- Ma, Ge, Lee, Zhang. **Lexical Effect Handlers, Directly.** OOPSLA 2024. https://dl.acm.org/doi/10.1145/3689770
- Lutze, Madsen. **Associated Effects: Flexible Abstractions for Effectful Programming.** PLDI 2024. https://dl.acm.org/doi/10.1145/3656393
- Odersky. **Capabilities for Control.** ICFP 2024 keynote. https://icfp24.sigplan.org/details/icfp-2024-papers/38/Capabilities-for-Control
- Xu, Bračevac, Pham, Odersky. **What's in the Box: Ergonomic and Expressive Capture Tracking over Generic Data Structures.** OOPSLA 2025. https://dl.acm.org/doi/10.1145/3763112
- Ma, Ge, Jung, Zhang. **Zero-Overhead Lexical Effect Handlers.** OOPSLA 2025. https://dl.acm.org/doi/10.1145/3763177
- Ma, Jung, Zhang. **Virtualizing Continuations.** PLDI 2026. https://dl.acm.org/doi/10.1145/3808289
- Xu, Boruch-Gruszecki, Odersky. **Degrees of Separation: A Flexible Type System for Data Race Prevention.** https://arxiv.org/abs/2308.07474
- Xie, Johnson, Maclaurin, Paszke. **Parallel Algebraic Effect Handlers.** https://arxiv.org/abs/2110.07493
- Scala capture checking documentation: https://docs.scala-lang.org/scala3/reference/experimental/cc.html

---

# Appendix F — Panel consensus

The design deliberately reflects the complementary concerns of the expert-review perspectives used for this architecture:

**Language/API design:** keep direct style, preserve composability, avoid implementation-shaped user APIs.

**Compiler/runtime performance:** make ordinary calls free of concurrency tax; concentrate costs at actual scheduling/suspension boundaries; preserve representation freedom.

**Async/concurrency design:** use structured scopes, cancellation trees, scheduler-aware parking, and direct-style operations rather than Future plumbing.

**Parallel systems design:** share dependency and lifetime structure across I/O and CPU/device parallelism while allowing backend-specific execution.

**Systems/zero-cost design:** expose control where needed, avoid hidden OS-thread blocking, keep the native substrate tiny, and make the performance contract measurable.

The resulting core rule is:

> **Color the rare restriction (`no_suspend`), not the ordinary function. Track suspension internally, preserve structured task lifetimes, and let the compiler ultimately choose the continuation representation.**
