# mojito_async/runtime/runtime.mojo
#
# A1.1 runtime (issue #33) — the colorless one-worker scheduler core.
#
# Productionized from spike/colorless_runtime/runtime.mojo (A0.2 + A0.6,
# issues #11, #15); semantics carried forward VERBATIM.  `Runtime` owns the
# FIFO runnable queue (TaskRecord payloads), the shutdown flag, the task-id
# allocator, and the observable scheduling counters.  `run[T: def() -> None]`
# executes the ROOT task on the CALLING thread (work-first, spec §88): it
# creates the root TCB, walks NEW -> RUNNABLE -> RUNNING, invokes the task,
# and marks COMPLETED on both the normal and the raising path (root tasks
# have no joiners, so run() is their joiner — the error message is preserved
# and re-raised).
#
# INV kept (spec §13/A0-T1): run() creates NO OS thread and performs NO
# hidden blocking — everything executes synchronously on the caller's stack.
#
# Mojo 1.0.0b2 (def-only) constraints honored (see task_control_block.mojo):
#   no `fn`, no `async`/`await`, no module-level mutable globals; first-class
#   `def` values are nominal, so the task argument is a generic constrained
#   to the `def()` callable trait; no static methods (module `create()` is
#   the constructor surface); EXTERN-FREE (modular/modular#6971: extern calls
#   stay in the embedding *_aot drivers).
from mojito_async.runtime.queue import FifoQueue, TaskRecord
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock


# ---------------------------------------------------------------------------
# Nil result (root tasks return nothing)
# ---------------------------------------------------------------------------

struct Nil(ResultValue):
    """Void stand-in for the root TCB's result slot (b2 has no unit type
    usable as a ResultValue; the root's TCB carries Nil and never marks a
    result)."""

    var _tag: Int

    def __init__(out self):
        self._tag = 0


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

struct Runtime:
    """One-main concept single-worker scheduler core.

    State:
      _ready     — FIFO of RUNNABLE TaskRecords (mutex-free: single worker,
                     no cross-thread handoff exists).
      _shutdown  — latched by shutdown(); run()/spawn refuse afterwards.
      _scope     — root scope handle (Int cell handle; refined upward).
      _next_id   — monotonic task-id allocator (ids start at 1; 0 = none).
      counters   — observable scheduling effects: _tasks_started/_completed
                     count ROLE-task executions by run(); _enqueued counts
                     runnable registrations (spawn/wake).  Tests observe
                     these instead of trusting silent success.
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

    def run[T: def() raises -> None](mut self, task: T) raises:
        """Execute the ROOT task on the CALLING thread (work-first, spec §88).

        Full TCB lifecycle on the spot: NEW -> RUNNABLE -> RUNNING, invoke,
        then RUNNING -> COMPLETED.  On a raising task the TCB still reaches
        COMPLETED and the error message is preserved and re-raised: the root
        has no joiner, so run() itself is the joiner.

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

    # --- runnable-queue services (spawn.mojo, scheduler, drivers) -----------

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

    def set_scope_handle(mut self, h: Int):
        self._scope = h

    # --- lifecycle -----------------------------------------------------------

    def shutdown(mut self) -> None:
        """Latch the shutdown flag (idempotent)."""
        self._shutdown = True

    def is_shutdown(self) -> Bool:
        return self._shutdown


def create() -> Runtime:
    """Module-level factory (b2 has no static methods).  Mirrors the
    mojito-sys convention of exposing module-level constructors."""
    return Runtime()

def run[T: def() raises -> None](mut rt: Runtime, task: T) raises:
    """Module-level `run`: execute the ROOT task on the calling thread.

    Convenience wrapper mirroring the spikes `runtime.run(task)`: the root
    TCB lifecycle is NEW -> RUNNABLE -> RUNNING -> COMPLETED on the calling
    thread (errors preserved and re-raised prefixed "runtime.run: root task
    raised:").  Synchronous, no OS thread, no hidden blocking.
    """
    rt.run(task)
