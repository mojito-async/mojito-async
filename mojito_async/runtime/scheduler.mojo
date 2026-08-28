# §60 (park via runtime.park).
#
# On ONE worker with no fibers and no dynamic function values (b2), there is
# no invisible preemption: a task yields or parks ONLY at points where it
# calls these primitives (work-first, spec §88).  This module is extern-free:
#
#   - `yield_now(mut rt, h)`  — the RUNNING task is put back on the runnable
#     queue as RUNNABLE (spec §27) WITHOUT blocking; it MUST NOT sleep the
#     worker or register a wait.  `rt` re-queues FIFO; enqueue-once is
#     guarded (an already-RUNNABLE task is never re-enqueued).
#   - the cooperative PARK/WAKE primitives live in runtime/park.mojo now
#     (issue #39 single source): park_current (RUNNING -> PARKING -> WAITING,
#     generation-bumped epoch, spec §25, wait reason stamped) and
#     unpark_current (readiness delivered ONCE, WAITING -> RUNNABLE +
#     re-enqueue).  The A1.1 `_suspend_current` / `resume_current` spellings
#     were deleted; every consumer and lane driver imports park.mojo.
#   - `scheduler_loop` — the single-worker cooperative drive loop.  It is
#     GENERIC over a statically-known task-owner dispatcher (b2 cannot store
#     function values): the caller supplies the body executor for THIS task
#     tree, so unstarted records are executed exactly where their bodies are
#     known (the spike's proven model; NEVER dynamic dispatch through the
#     record).  The loop dequeues RUNNABLE records, runs each via the
#     dispatcher to its next checkpoint, and stops when the queue is empty.
#     A popped record whose TCB is not RUNNABLE (stale duplicate) is
#     SKIPPED — counted via rt.skipped(), never dispatched.
#
# No per-suspension allocation on the hot path (AMORTIZED: no allocation
# beyond the runnable queue's amortized ring growth): yield/park reuse the
# TCB's embedded WaitNode and the Runtime's FIFO; the caller supplies every
# TCB cell (stack-allocated) — code, not heap, owns task storage.
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Nil, Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock


# ---------------------------------------------------------------------------
# yield_now — cooperative reschedule without blocking (spec §27)
# ---------------------------------------------------------------------------

def yield_now[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Reschedule the currently RUNNING task without blocking.

    RUNNING -> PARKING -> RUNNABLE is taken through the machine's early-wake
    edge (spec A0.5): the task is never WAITING, claims no wait epoch, and
    re-enters the runnable queue FIFO - the worker picks the next RUNNABLE
    record next.  A real WAITING suspend would use runtime.park's `park_current`.
    Doesn't sleep the worker or register a wait; no allocation beyond the
    queue's amortized growth.  Enqueue-once: if the task is ALREADY RUNNABLE
    (its record still queued), this is a no-op - never double-enqueued."""
    if h.state() == TaskControlBlock.RUNNABLE:
        return  # already reschedulable; do not double-enqueue
    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)
    rt.enqueue(Int(h.tcb()), h.id())



# ---------------------------------------------------------------------------
# scheduler_loop — the single-worker cooperative drive loop (spec §21 / C7)
# ---------------------------------------------------------------------------

# Dispatcher slot: given the worker's Runtime plus a RUNNABLE record, execute
# that record's task up to its next state.  The dispatcher KNOWS the task
# bodies (b2 cannot store heterogeneous thunks); the lopp is generic over it.
# Passing `rt` lets the dispatcher park (via `park_current`), yield (via
# `yield_now`), or wake (via `unpark_current`) as it drives.
def scheduler_loop[F: def(mut Runtime, Int, Int, BytePtr) raises -> Int, R: ResultValue = Nil](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
) raises -> Int:
    """Drive the ONE worker until its runnable queue is quiet.

    For each popped record, SKIP it (counted) when its TCB is not RUNNABLE
    (stale duplicate — never dispatched), else hand (rt, tcb_addr, task_id,
    ud) to `dispatcher`, which executes that task to its next state.  A task
    that parks (via `park_current`), yields (via `yield_now`), or is
    completed is handled by the dispatcher over rt.  Returns the number of
    records SERVED (observable progress); skipped records are observable via
    `rt.skipped()`.
    """
    var slices = 0
    while rt.has_ready():
        var rec = rt.pop_ready()
        var checker = UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=rec.tcb_addr
        )
        if checker[].state() != TaskControlBlock.RUNNABLE:
            rt.note_skipped()
            continue
        slices += 1
        _ = dispatcher(rt, rec.tcb_addr, rec.task_id, ud)
    return slices