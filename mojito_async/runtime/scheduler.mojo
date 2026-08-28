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
#
# A1.5 (issue #53) — the FIBER-BACKED drive.  This module stays EXTERN-FREE
# and UNCHANGED in its mechanics: the single-worker loop pops a RUNNABLE
# record and hands it to the statically-known dispatcher.  The frame
# migration lives one module over, in runtime/fiber_seam.mojo, and the
# fiber handle is THREADED THROUGH THE DRIVER VALUE (b2 design decision #4,
# never dynamic dispatch): an *_aot driver's dispatcher drives each record's
# fiber via the seam — first entry makes the fresh context (ms_ctx_make),
# a park is the body's seam_park_switch (fiber -> caller; the frame leaves
# the worker's native context), the park/wake state commit is
# fiber_suspend_current / fiber_yield_now / fiber_resume_current (#39 kernel
# spellings), and the next slice re-enters the fiber at its exact saved
# frame.  Non-parking tasks never touch a fiber: the cheap path is this
# loop + plain execute() on the worker's native context, and the Runtime
# fiber-path toggle (fiber_drives/fiber_switches) stays flat — the fast-path
# regression guard.  Keep this module import-free of fibers so the JIT unit
# drivers (t11..t18/t20..t22) keep linking without the dylib (#6971).
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

# ---------------------------------------------------------------------------
# A1.5 fiber seam (issue #53): the fiber-backed DRIVE lives in
# runtime/fiber_seam.mojo — seam_drive returns the frame-reported
# DriveVerdict (Parked | Completed; T3), seam_park_switch stamps the frame,
# and seam_destroy_slot raises on a parked/suspended (live) frame.  The
# runtime fiber-path counters are comptime-gated (FIBER_TOGGLE).
#
# A1.3 affinity seam (issue #51) — worker-affine started fibers (ADR-006/007)
# ---------------------------------------------------------------------------
#
# spec §19.2: STARTED tasks are NOT stealable; a started fiber's wake is
# routed to its OWNER worker's run queue ("remote-ready routing"), NEVER the
# general stealable set.  On the single worker the owner IS the sole worker,
# so this always resolves to the local FIFO (spec §88 — today's behavior,
# preserved unchanged).  The decision surface below is what EPIC #2's M:N
# worker pool snaps to (E5 started-fiber remote-ready): the pool calls
# wake_target_worker(f.owner_worker(), this_worker_id) at wake time and
# enqueues onto that worker's (remote-ready) queue when the returned target
# differs from the waker; on one worker the target is always the sole queue.
def wake_target_worker(owner_worker: Int, local_worker: Int) -> Int:
    """Resolve the enqueue target for a woken (started) task/fiber.

    owner_worker — the woken fiber's pinned owner (from Fiber.owner_worker();
                   0 = not started / not pinned: no affinity yet).
    local_worker — the worker performing the wake (explicit identity; b2 has
                   no TLS, so worker identity is threaded by value).

    Returns the worker whose run queue must receive the wake:
      - owner == local_worker (intra-worker wake, spec §88) -> local_worker,
        the wake stays on this worker's FIFO (today's behavior);
      - owner == 0 (unstarted/unpinned) -> local_worker, the general
        runnable-set fallback (nothing to be affine to yet);
      - otherwise (foreign wake) -> owner_worker: the wake lands on the
        OWNER worker's remote-ready queue (spec §19.2), never the stealable
        set.  EPIC #2 enqueues there in the E5 seam.
    """
    if owner_worker == 0 or owner_worker == local_worker:
        return local_worker
    return owner_worker
