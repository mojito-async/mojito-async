# mojito_async/runtime/scheduler.mojo
#
# A1.1 runtime (issue #33) — the single-worker scheduler primitives and the
# cooperative drive loop, productionized over the A0.6 park/wake protocol
# (issue #15) and spec §21 ("Scheduler loop"), §27 ("yield_now"),
# §60 (`_suspend_current`).
#
# On ONE worker with no fibers and no dynamic function values (b2), there is
# no invisible preemption: a task yields or parks ONLY at points where it
# calls these primitives (work-first, spec §88).  This module is extern-free:
#
#   - `yield_now(mut rt, h)`  — the RUNNING task is put back on the runnable
#     queue as RUNNABLE (spec §27) WITHOUT blocking; it MUST NOT sleep the
#     worker, register a wait, or allocate.  `rt` re-queues FIFO; the
#     embedded WaitNode is not touched, so this is purely a reschedule.
#   - `_suspend_current(mut rt, h, reason)` — the cooperative park commit:
#     RUNNING -> PARKING -> WAITING (generation-bumped epoch, spec §25) and
#     drops off the runnable queue.  The worker is free to run other RUNNABLE
#     tasks; `resume_current` (same as task.wake) delivers readiness ONCE per
#     epoch and re-enqueues.
#   - `scheduler_loop` — the single-worker cooperative drive loop.  It is
#     GENERIC over a statically-known task-owner dispatcher (b2 cannot store
#     function values): the caller supplies the body executor for THIS task
#     tree, so unstarted records are executed exactly where their bodies are
#     known (the spike's proven model; NEVER dynamic dispatch through the
#     record).  The loop dequeues RUNNABLE records, runs each via the
#     dispatcher to its next checkpoint, and stops when the queue is empty
#     (worker idle — the determinism that t2/t6 prove).
#
# No hidden allocation on the hot path: yield/suspend reuse the TCB's
# embedded WaitNode and the Runtime's FIFO; the caller supplies every TCB
# cell (stack-allocated) — code, not heap, owns task storage.
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.runtime.task_control_block import WaitNode
from mojito_async.task import JoinHandle, SuspendReason


# ---------------------------------------------------------------------------
# yield_now — cooperative reschedule without blocking (spec §27)
# ---------------------------------------------------------------------------

def yield_now[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Reschedule the currently RUNNING task without blocking.

    RUNNING -> PARKING -> RUNNABLE is taken through the machine's early-wake
    edge (spec A0.5): the task is never WAITING, claims no wait epoch, and
    re-enters the runnable queue FIFO — the worker picks the next RUNNABLE
    record next.  A real WAITING suspend would use `_suspend_current`.
    Doesn't sleep the worker, register a wait, or allocate."""

    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)
    rt.enqueue(Int(h.tcb()), h.id())


# ---------------------------------------------------------------------------
# _suspend_current — cooperative park (spec §60)
# ---------------------------------------------------------------------------

def _suspend_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    reason: Int = SuspendReason.PARK,
) raises:
    """Park the current task on the single worker: RUNNING -> PARKING -->
    WAITING (fresh epoch; stale-wakeup defense built into the TCB).  The
    worker is free for other RUNNABLE tasks.  Resume later via
    `mojito_async.runtime.scheduler.resume_current` (== task.wake)."""
    _ = reason  # keep the wait-reason provenance available to callers
    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].transition(TaskControlBlock.WAITING)


def resume_current[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Deliver readiness ONCE: WAITING -> RUNNABLE and re-enqueue (FIFO).
    Equivalent to mojito_async.task.wake; keeps the original scheduler id."""
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)
    rt.enqueue(Int(h.tcb()), h.id())


# ---------------------------------------------------------------------------
# scheduler_loop — the single-worker cooperative drive loop (spec §21 / C7)
# ---------------------------------------------------------------------------

# Slot of the dispatcher: given the worker's Runtime plus a RUNNABLE record,
# execute that record's task up to its next state.  The dispatcher KNOWS the
# task bodies (b2 cannot store heterogeneous thunks); the loop is generic
# over it.  Passing `rt` lets the dispatcher park (via `_suspend_current`),
# yield (via `yield_now`), or wake (via `resume_current`) as it drives.
def scheduler_loop[F: def(mut Runtime, Int, Int, BytePtr) raises -> Int](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
) raises -> Int:
    """Drive the ONE worker until its runnable queue is quiet.

    For each RUNNABLE record, pop it and hand (rt, tcb_addr, task_id, ud) to
    `dispatcher`, which executes that task to its next state.  A task that
    parks (via `_suspend_current`), yields (via `yield_now`), or is completed
    is handled by the dispatcher over rt; the loop exchanges records until
    empty.  Returns the number of records served (observable progress).

    This is the single-worker scheduler loop the A0 drivers hand-wrote
    (t2_worker_reuse_aot, t6_join_semantics_aot), now a library primitive.
    """
    var slices = 0
    while rt.has_ready():
        var rec = rt.pop_ready()
        slices += 1
        _ = dispatcher(rt, rec.tcb_addr, rec.task_id, ud)
    return slices