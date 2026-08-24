# spike/colorless_runtime/runtime.mojo
#
# A0.2 + A0.6 (issues #11, #15) — the colorless one-worker scheduler core.
#
# A0.6 replaces the A0.2 compile-surface stub with the REAL root-task
# lifecycle plus the scheduler state every other lane drives:
#
#   - Runtime owns the FIFO runnable queue (TaskRecord payloads), the root
#     scope handle, the shutdown flag, and observable scheduling counters.
#   - run[task] executes the ROOT task on the CALLING thread: it creates the
#     root TCB, walks NEW -> RUNNABLE -> RUNNING, invokes the task, and marks
#     COMPLETED on both the normal and the raising path (the error message is
#     preserved and re-raised to the caller — root tasks have no joiners, so
#     run() is their joiner).
#   - Spawned children never block the worker invisibly: registration,
#     parking, waking, and joining go through the explicit protocol in
#     spawn.mojo; see that module's header for the exact proven split between
#     in-library cooperative execution and driver-side fiber orchestration.
#
# INV kept (spec §13/A0-T1): run() creates NO OS thread and performs NO
# hidden blocking — everything executes synchronously on the caller's stack.
#
# Mojo 1.0.0b2 (def-only) constraints honored here:
#   - no `fn`, no `async`/`await`/`Future`, no module-level mutable globals;
#   - first-class `def` values are nominal (a bare `def work()` cannot be
#     converted to a trait-typed value), so the task argument is taken as a
#     generic constrained to the `def()` callable trait — the b2-legal way to
#     accept an ordinary def/closure;
#   - b2 has no static methods inside structs: the module-level `create()`
#     factory below is the constructor surface;
#   - this module is deliberately EXTERN-FREE: everything here runs correctly
#     when imported (modular/modular#6971 makes imported-module extern calls
#     unsafe; raw context switching stays in the embedding driver, mirroring
#     tests/t4_fiber.mojo).
from queue import FifoQueue
from task import TaskControlBlock, ResultValue


# ---------------------------------------------------------------------------
# Nil result (root tasks return nothing)
# ---------------------------------------------------------------------------

struct Nil(ResultValue):
    """Void stand-in for the root TCB's result slot.

    TaskControlBlock[T] requires T: ResultValue (copyable, deletable,
    default-constructible); b2 has no unit type usable there, so the root
    task's TCB carries Nil and never marks a result.
    """

    var _tag: Int

    def __init__(out self):
        self._tag = 0


# ---------------------------------------------------------------------------
# TaskRecord — the queue payload
# ---------------------------------------------------------------------------

struct TaskRecord(ImplicitlyCopyable, ImplicitlyDeletable):
    """Type-erased runnable-task record (data only — inject DATA schedules).

    b2 forbids storing heterogeneous thunks behind one queue element type
    (function-typed fields are rejected; fn pointers/dynamic fn values do
    not exist).  The record therefore carries the ADDRESS of the task's
    caller-allocated TaskControlBlock cell plus its scheduler id.  Executing
    an unstarted record is a GENERIC operation performed at a call site that
    statically knows the task body (runtime.run for roots, spawn.execute for
    children, or the embedding driver's fiber trampoline) — never a dynamic
    dispatch through this record.
    """

    var tcb_addr: Int
    var task_id: Int

    def __init__(out self, tcb_addr: Int, task_id: Int):
        self.tcb_addr = tcb_addr
        self.task_id = task_id


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

struct Runtime:
    """One-worker colorless scheduler core (calling thread IS the worker).

    State:
      _ready      — FIFO of RUNNABLE TaskRecords (queue.mojo; mutex-free:
                    single worker, no cross-thread handoff exists).
      _shutdown   — latched by shutdown(); run()/spawn refuse afterwards.
      _scope      — root scope handle (Int handle; A0.9 refines).
      _next_id    — monotonic task-id allocator (ids start at 1; 0 = none).
      counters    — observable scheduling effects: tasks_started/_completed
                    count ROOT-task executions by run(); enqueued counts
                    runnable registrations (spawn/wake).  Tests observe these
                    instead of trusting silent success.
    """

    comptime NO_SCOPE = Int(0)

    var _ready: FifoQueue[TaskRecord]
    var _shutdown: Bool
    var _scope: Int
    var _next_id: Int
    var _tasks_started: Int
    var _tasks_completed: Int
    var _enqueued: Int

    def __init__(out self):
        self._ready = FifoQueue[TaskRecord]()
        self._shutdown = False
        self._scope = Self.NO_SCOPE
        self._next_id = 1
        self._tasks_started = 0
        self._tasks_completed = 0
        self._enqueued = 0

    # --- root-task execution (A0-T1) ----------------------------------------

    def run[T: def() -> None](mut self, task: T) raises:
        """Execute the ROOT task on the CALLING thread (work-first, spec §88).

        Full TCB lifecycle on the spot: NEW -> RUNNABLE -> RUNNING, invoke,
        then RUNNING -> COMPLETED.  On a raising task the TCB still reaches
        COMPLETED (the terminal state per the A0.5 machine) and the error
        message is preserved and re-raised to the caller: the root has no
        joiner, so run() itself is the joiner.

        Raises on a shut-down runtime; re-raises root-task errors prefixed
        "runtime.run: root task raised:".
        """
        if self._shutdown:
            raise Error("runtime.run: runtime is shut down")
        var root = TaskControlBlock[Nil]()
        root.transition(TaskControlBlock.RUNNABLE)
        root.transition(TaskControlBlock.RUNNING)
        self._tasks_started += 1
        try:
            task()
        except e:
            root.transition(TaskControlBlock.COMPLETED)
            self._tasks_completed += 1
            raise Error("runtime.run: root task raised: " + String(e))
        root.transition(TaskControlBlock.COMPLETED)
        self._tasks_completed += 1

    # --- runnable-queue services (used by spawn.mojo and drivers) ------------

    def next_id(mut self) -> Int:
        """Allocate a monotonically increasing task id (never 0)."""
        var id = self._next_id
        self._next_id += 1
        return id

    def enqueue(mut self, tcb_addr: Int, task_id: Int) raises:
        """Register a RUNNABLE task record (FIFO order)."""
        if self._shutdown:
            raise Error("runtime.enqueue: runtime is shut down")
        self._ready.push(TaskRecord(tcb_addr, task_id))
        self._enqueued += 1

    def pop_ready(mut self) raises -> TaskRecord:
        """Dequeue the next RUNNABLE record; raises on an empty queue."""
        return self._ready.pop()

    def has_ready(self) -> Bool:
        return not self._ready.is_empty()

    def pending(self) -> Int:
        """Number of currently RUNNABLE records."""
        return len(self._ready)

    # --- observability -------------------------------------------------------

    def tasks_started(self) -> Int:
        return self._tasks_started

    def tasks_completed(self) -> Int:
        return self._tasks_completed

    def enqueued(self) -> Int:
        return self._enqueued

    def scope_handle(self) -> Int:
        return self._scope

    # --- lifecycle -----------------------------------------------------------

    def shutdown(mut self) -> None:
        """Latch the shutdown flag (idempotent).  The spike owns no worker
        stack/context to reclaim: fibers are driver-owned by construction."""
        self._shutdown = True

    def is_shutdown(self) -> Bool:
        return self._shutdown


# Module-level factory (b2 has no static methods). Mirrors the mojito-sys
# convention of exposing module-level constructors.
def create() -> Runtime:
    return Runtime()
