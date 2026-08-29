# mojito_async/time/worker_timer.mojo
#
# A6.3 (issue #86) — deadline delivery + idle-worker timer wake ACROSS
# WORKERS under the M:N scheduler (spec §31: "deadlines wake the owner
# worker without making started tasks migratable").
#
# EPIC #2 (M:N scheduler, #67-#74) is now on main: every worker owns its own
# Runtime, local/remote-ready queues (queue.mojo, #68) and idle-sleeps on the
# pool's shared NativeEvent when it finds no work (idle.mojo/thread_entry.mojo,
# #72).  What A1.4's timer heap (timer_heap.mojo, #36) and its single-worker
# service hook (timer_service.mojo, #39) do NOT yet address is: (1) a timer
# heap is caller-owned and singular — under a pool there must be ONE per
# worker, never a shared/locked heap on the expiry hot path; (2) the ARMING
# call for a task's deadline may happen from a context that is not that
# task's owner worker (an external coordinator/embedder arming a timeout on
# behalf of a started, worker-pinned task — the "remote arm" case); and (3)
# an idle-parked worker must wake EXACTLY when ITS nearest timer is due, not
# only on the pool's generic work-stealing wake.
#
# This module fills exactly that gap, composing (never duplicating) A1.4:
#   - WorkerTimerHandle: one worker's OWN TimerHeap plus a DEDICATED
#     NativeEvent used ONLY for targeted deadline-wake delivery (kept
#     separate from worker_pool.mojo's shared, breadth-one work-stealing
#     event on purpose — see the struct docstring).
#   - WorkerTimerTable: a caller-owned, stride-addressed array of
#     WorkerTimerHandle cells (one per worker) — the "owner_id -> heap"
#     map the issue calls for.  Ownership is STRUCTURAL: index i IS worker
#     i's heap, resolved through the SAME TaskControlBlock.owner_worker()
#     stamp (A2.5, #71) unpark_current already uses to route a cross-worker
#     WAKE (park.mojo's `_owner_rt`) — arm_remote below is the timer-arming
#     mirror of that exact routing.
#   - service_worker_timers / min_deadline / next_park_deadline_ns: the
#     per-worker service pass and the idle-sleep bound it feeds ("a worker
#     sleeps until its nearest deadline, not forever").
#   - arm_remote / deliver_deadline: arming a deadline whose owner is a
#     DIFFERENT worker lands it on the OWNER's heap (never the caller's)
#     and delivers exactly one targeted wake signal so an already-sleeping
#     owner re-evaluates its park bound immediately instead of oversleeping
#     past a newly-armed, possibly-earlier deadline.
#   - idle_park_worker_timers: one idle-with-timers park cycle (E6's
#     park_os_thread_until_event companion, scoped to this dedicated event).
#
# NEVER RELOCATES A STARTED TASK (spec invariant, #71): every operation here
# only reads/writes the OWNER's heap and signals the OWNER's event; the TCB's
# owner_worker/owner_runtime stamps are never touched by this module — a
# deadline crossing worker boundaries is a WAKE NOTIFICATION, never a
# migration.
#
# Mojo 1.0.0b2 workarounds (matching every other time/ module): def-only,
# module-level factories; no globals/TLS — the per-worker heap array and its
# NativeEvents ride the explicit WorkerTimerTable frame the caller threads
# through every call (mirrors the (mut rt, mut heap) discipline sleep.mojo/
# timer_service.mojo already use); WorkerTimerHandle embeds a TimerHeap
# (Movable, not ImplicitlyCopyable) and a NativeEvent, so the table is a
# stride-addressed pointer array (worker_pool.mojo's Worker/WorkerEntryCell
# cell-array convention) rather than a std List (List's grow-by-value
# semantics are the wrong fit for a non-copyable element); the vendored
# mojito-sys NativeEvent wrappers are called directly at plain module scope
# exactly like idle.mojo does (extern-free from this module's own
# perspective) — the abi("C") 3-layer mis-lowering bug (thread_entry.mojo's
# header) only bites inside an abi("C") trampoline, which this module is
# not.
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle
from mojito_async.time.timer_heap import NO_DEADLINE, TimerHeap
from mojito_async.time.timer_service import service_timers
from mojito_async.vendor.mojito_sys import (
    NativeEvent,
    monotonic_now_ns,
    native_event_signal,
    native_event_wait_until,
)


# ---------------------------------------------------------------------------
# WorkerTimerHandle — one worker's timer-service state
# ---------------------------------------------------------------------------

struct WorkerTimerHandle(Movable, ImplicitlyDeletable):
    """One worker's private timer-service state (issue #86 deliverable 1):
    its OWN TimerHeap (#36) — no lock, no cross-worker access, ever — plus a
    DEDICATED NativeEvent used ONLY to deliver a TARGETED deadline wake to
    THIS worker.

    Why a dedicated event instead of reusing worker_pool.mojo's shared pool
    event: that event's wake_one() is intentionally BREADTH-ONE-ARBITRARY
    (any one idle sleeper — correct for interchangeable work-stealing
    wakes).  A deadline wake is NOT interchangeable: it must reach EXACTLY
    the owner worker, or the true owner keeps sleeping on its OLD (now
    stale) park bound while an unrelated peer wakes, finds nothing to do,
    and re-parks — the deadline would then only be honored once that
    unrelated peer's own bounded backstop happens to elapse, which is
    neither "wakes the owner worker" (spec §31) nor "no cross-worker
    duplicate delivery" (issue #86).  A private, per-worker event makes the
    delivery targeted by construction.

    heap        — this worker's own A1.4 TimerHeap.
    owner_id    — this handle's worker/pool index (matches the
                  TaskControlBlock.owner_worker() stamp, A2.5 #71 — the
                  value WorkerTimerTable indexes by).
    deliveries  — count of deliver_deadline() calls: the F6 bench
                  instrumentation feed (issue #86 deliverable 3) and a
                  test-visible "exactly once" accessor.
    parks       — count of idle_park_worker_timers() cycles: proves the
                  worker returns to sleep after each service pass instead
                  of spinning (issue #86 acceptance).
    """

    var heap: TimerHeap
    var owner_id: Int
    var _event: NativeEvent
    var deliveries: Int
    var parks: Int

    def __init__(out self):
        self.heap = TimerHeap()
        self.owner_id = 0
        self._event = NativeEvent()
        self.deliveries = 0
        self.parks = 0

    def __init__(out self, owner_id: Int):
        self.heap = TimerHeap()
        self.owner_id = owner_id
        self._event = NativeEvent()
        self.deliveries = 0
        self.parks = 0

    def bind_event(mut self, e: NativeEvent):
        """Bind the OS-level wake target this worker's idle loop parks on
        (issue #86 deliverable 4).  Deterministic (virtual-clock) callers
        never bind one: deliver_deadline() then only bumps the diagnostic
        counter — there is nothing to signal and nothing parked on it."""
        self._event = e

    def has_event(self) -> Bool:
        return self._event.alive()

    def event_handle(self) -> Int:
        return self._event.handle()


# ---------------------------------------------------------------------------
# WorkerTimerTable — caller-owned per-worker heap registry (the "ownership
# map owner_id -> heap" the issue calls for; deliverable 1)
# ---------------------------------------------------------------------------

struct WorkerTimerTable(ImplicitlyCopyable, ImplicitlyDeletable):
    """Thin, caller-owned VIEW over an array of WorkerTimerHandle POINTERS
    (one per worker) — mirrors worker.mojo's own `_peers: UnsafePointer[
    UnsafePointer[Worker, ...], ...]` pointer-array convention exactly,
    rather than a stride-addressed inline block: each WorkerTimerHandle
    may live anywhere with a STABLE address for the caller's lifetime — a
    plain local `var`, a c_malloc'd cell, a pool-owned array — and this
    struct only ADDRESSES them, never allocates or owns storage itself (a
    std List is the wrong fit regardless: WorkerTimerHandle embeds a
    TimerHeap and a NativeEvent, neither ImplicitlyCopyable, see the
    struct's own docstring).  The pointer array itself is POD (plain
    addresses, no destructor), so the caller may build it with a bare
    `stack_allocation` with none of b2's no-placement-new hazards a
    stride-addressed array of a destructor-owning struct would hit.
    Ownership is STRUCTURAL: slot i IS worker i's handle — no runtime map
    lookup, no lock, matching the caller-owned, explicit-frame discipline
    every time/ module in this codebase uses (no globals, no TLS)."""

    var _slots: UnsafePointer[
        UnsafePointer[WorkerTimerHandle, MutAnyOrigin], MutAnyOrigin
    ]
    var _n: Int

    def __init__(
        out self,
        slots: UnsafePointer[
            UnsafePointer[WorkerTimerHandle, MutAnyOrigin], MutAnyOrigin
        ],
        n: Int,
    ):
        self._slots = slots
        self._n = n

    def count(self) -> Int:
        return self._n

    def at(self, worker_id: Int) raises -> UnsafePointer[WorkerTimerHandle, MutAnyOrigin]:
        """The (caller-owned) address of worker `worker_id`'s handle.
        Raises on an out-of-range id — a caller resolving a bogus/
        unstamped owner is a programming error, never a silent wrong-heap
        arm."""
        if worker_id < 0 or worker_id >= self._n:
            raise Error(
                "WorkerTimerTable.at: worker_id " + String(worker_id)
                + " out of range [0, " + String(self._n) + ")"
            )
        return self._slots[worker_id]


def make_worker_timer_table(
    slots: UnsafePointer[
        UnsafePointer[WorkerTimerHandle, MutAnyOrigin], MutAnyOrigin
    ],
    n: Int,
) -> WorkerTimerTable:
    return WorkerTimerTable(slots, n)


# ---------------------------------------------------------------------------
# Per-worker service pass + idle-sleep bound (deliverable 1)
# ---------------------------------------------------------------------------

def service_worker_timers[R: ResultValue](
    mut rt: Runtime, mut wt: WorkerTimerHandle, now: UInt64
) raises -> Int:
    """THIS worker services ONLY its own heap (issue #86: "each worker
    services only its own heap; no global lock on the expiry/delivery hot
    path").  A thin, documented specialization of the A1.4 service_timers
    hook (#36/#39) scoped to one worker's private WorkerTimerHandle — the
    isolation is STRUCTURAL (the caller can only ever reach this worker's
    own `wt`, never a peer's), so no lock is needed here: two workers each
    calling this on their OWN handle never contend."""
    return service_timers[R](rt, wt.heap, now)


def min_deadline(wt: WorkerTimerHandle) -> UInt64:
    """This worker's nearest pending deadline (or NO_DEADLINE when its heap
    is empty) — feeds next_park_deadline_ns below (issue #86 deliverable 1:
    "min_deadline() feeds the sleep logic")."""
    return wt.heap.min_deadline()


def next_park_deadline_ns(wt: WorkerTimerHandle, now_ns: Int, backstop_ns: Int) -> Int:
    """The absolute CLOCK_MONOTONIC deadline THIS worker should idle-park
    on (issue #86: "a worker sleeps until its nearest deadline, not
    forever"): the EARLIER of its own nearest timer and the shutdown/
    liveness backstop slice (the same bounded-slice discipline
    thread_entry.IDLE_PARK_SLICE_NS already uses for the pool's shared
    event) — so a timer due sooner than the backstop wakes the worker on
    schedule instead of oversleeping until the next backstop tick, while a
    worker with no timers armed parks the FULL backstop (no spurious wake
    while every timer is in the future, issue #86 acceptance).  A deadline
    already at-or-before `now_ns` returns `now_ns` (park returns
    immediately; the caller's next service pass finds it due)."""
    var md = wt.heap.min_deadline()
    if md == NO_DEADLINE:
        return now_ns + backstop_ns
    var md_i = Int(md)
    if md_i <= now_ns:
        return now_ns
    var backstop_at = now_ns + backstop_ns
    if md_i < backstop_at:
        return md_i
    return backstop_at


# ---------------------------------------------------------------------------
# Cross-worker arm + targeted delivery (deliverables 2 + 4)
# ---------------------------------------------------------------------------

def deliver_deadline(mut wt: WorkerTimerHandle):
    """Wake the OWNER worker `wt` belongs to if it is (or may be)
    event-sleeping on its dedicated timer NativeEvent, so it re-enters its
    park loop, recomputes next_park_deadline_ns() against the freshly armed
    entry, and services its heap on its next pass (issue #86 deliverable 4:
    "the owner worker runs the expiry hook next service pass").  Signaling
    an event nobody is parked on is harmless — mojito-sys's NativeEvent is a
    STICKY, breadth-one token (vendor/mojito_sys.mojo): the next park call
    consumes it immediately and loops once more, exactly the discipline
    worker_pool.wake_one_force already relies on for shutdown.  A handle
    with no bound event (deterministic/virtual-clock callers, see
    bind_event's docstring) only bumps the diagnostic counter — there is
    nothing to signal."""
    wt.deliveries += 1
    if wt.has_event():
        native_event_signal(wt._event)


def arm_remote[R: ResultValue](
    mut table: WorkerTimerTable, h: JoinHandle[R], deadline_ticks: UInt64
) raises -> Int:
    """Arm a deadline for `h` on ITS OWNER worker's heap (issue #86
    deliverable 2) — resolved from the TCB's owner_worker() stamp (A2.5,
    #71), the EXACT routing park.mojo's `unpark_current`/`_owner_rt`
    already perform for a cross-worker WAKE.  A caller running on ANY
    worker (or no worker at all — an embedder/coordinator context, e.g. a
    supervisory timeout set up by a parent task on a different worker than
    its worker-pinned child) always lands the arm on the CORRECT per-worker
    heap, never its own: `owner_worker()` is read from the TCB, not
    threaded in by the caller, so there is no way to accidentally arm the
    wrong heap.  `owner_worker() == 0` (never started / no pool) arms table
    index 0 — the same "0 = no pool, the sole worker IS the owner"
    convention every other cross-worker-aware module in this codebase uses
    (park.mojo's `_owner_rt`, scheduler.mojo's `wake_target_worker`).

    Then delivers the SINGLETON cross-worker wake (deliver_deadline): a
    caller may run arm_remote from a worker OTHER than the owner, so an
    already event-sleeping owner must be nudged to re-evaluate its park
    bound immediately — never a busy poll, exactly one signal per arm call
    (issue #86: "arming a deadline whose owner is another worker enqueues
    the SINGLETON cross-worker wake").  Returns the heap-granted generation
    token (see TimerHeap.arm) so the caller can cancel_token() it later."""
    var owner = h.tcb()[].owner_worker()
    var wt = table.at(owner)
    var gen = wt[].heap.arm(h.id(), Int(h.tcb()), deadline_ticks)
    deliver_deadline(wt[])
    return gen


# ---------------------------------------------------------------------------
# Idle-wake integration (deliverable 4 / issue #72 companion)
# ---------------------------------------------------------------------------

def idle_park_worker_timers(mut wt: WorkerTimerHandle, backstop_ns: Int) -> Bool:
    """ONE idle-with-timers park cycle for a worker with no local/remote/
    injection/steal work (E6's park_os_thread_until_event, #72, scoped to
    THIS dedicated timer-wake channel instead of the pool's shared
    work-stealing event): parks on `wt`'s NativeEvent until either a
    deliver_deadline() signal arrives or its own nearest timer's deadline
    elapses (next_park_deadline_ns) — never forever, never a busy spin.

    Returns True iff a real token was CONSUMED (a deliver_deadline signal
    or any other explicit signal — distinct from a plain deadline-slice
    timeout, mirrors native_event_wait_until's own contract); the caller
    re-checks/services its heap and re-parks either way (mirrors
    idle.idle_park_worker's return contract, #72) — "the sleeping worker
    wakes on that signal, services its heap, and returns to sleep" (issue
    #86 acceptance)."""
    wt.parks += 1
    var now = monotonic_now_ns()
    var deadline = next_park_deadline_ns(wt, now, backstop_ns)
    return native_event_wait_until(wt.event_handle(), deadline)
