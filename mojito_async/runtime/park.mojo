# mojito_async/runtime/park.mojo
#
# A1 follow-up (issue #39) — the ONE park/wake kernel over the TCB parked-
# state machinery (spec §24 "ParkingLot / cancellation"; see
# task_control_block.mojo's embedded WaitNode for the allocation-free wait
# cell).
#
# Single source of truth for the cooperative suspend choreography.  Before
# this module the SAME transition sequence was re-implemented in four places:
#   - scheduler._suspend_current / resume_current   (the A1.1 canonical seam);
#   - task.suspend_commit / wake (+ dead park_prepare / park_commit);
#   - parking_lot.park_current / unpark_current + the ParkingLot struct;
#   - sync/event.WaitEvent (a readiness cell nobody consumed).
# Those four spellings are now ONE pair here; every consumer (mutex,
# semaphore, channel, timer, scheduler, the *_aot drivers and the lane test
# drivers) calls park_current / unpark_current.  The dead duplicates were
# deleted (task.park_prepare/park_commit/suspend_commit/wake, parking_lot,
# WaitEvent).
#
# Execution discipline:
#   park_current  — commit the CURRENT task as a waiter: RUNNING -> PARKING
#       -> WAITING, stamping the wait REASON on the embedded WaitNode (spec
#       §24/§25, generation-bumped epoch).  Raises IllegalTransitionError if
#       the task is not RUNNING.
#   unpark_current — deliver readiness ONCE per epoch: WAITING -> RUNNABLE +
#       FIFO re-enqueue.  An already-RUNNABLE task is a no-op (enqueue-once);
#       any other state (e.g. COMPLETED) raises — a stale wake never silently
#       double-enqueues (t15 asserts this).
#
# No hidden allocation, no OS-thread synchronization.  The worker owns no
# task storage: every TCB cell is caller-allocated and the caller passes its
# own JoinHandle.
#
# RACE-PROTOCOL NOTE (A0.7 two-phase parking, issue #16): the spike's
# PREPARE/VALIDATE/COMMIT pipeline exists to close a lost-wakeup window
# between publishing a waiter and parking.  On the A1 SINGLE cooperative
# worker there is no interleaving inside a dispatcher slice: publish+park
# (register_* then park_current) is atomic with respect to other tasks, so a
# release always finds its waiter already parked, and the protocol's
# VALIDATE re-check is a no-op.  The lanes proved this (mutex/channel
# hands-off are FIFO handoffs to parked waiters; no lost wakeups in the
# stress suites).  The two-phase protocol is therefore carried as the MODEL
# for the A2 multi-worker seam (where real races return), documented in
# PARKING-LOT-ADAPTER below, instead of ceremony on a race-free worker.
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.runtime.join_handle import JoinHandle, SuspendReason


def park_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    reason: Int = SuspendReason.PARK,
) raises:
    """Park the CURRENT task on the single worker: RUNNING -> PARKING -->
    WAITING (fresh epoch; stale-wakeup defense built into the TCB).  The wait
    REASON is stamped on the embedded WaitNode so a later wake can inspect
    why the task waits.  The worker is free for other RUNNABLE tasks.  Resume
    later via unpark_current."""
    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].wait_node()[].set_reason(reason)
    h.tcb()[].transition(TaskControlBlock.WAITING)


def unpark_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    required_gen: Int = 0,
) raises:
    """Deliver readiness ONCE: WAITING -> RUNNABLE and re-enqueue (FIFO).
    Keeps the task's original scheduler id.  Enqueue-once: an already-RUNNABLE
    task is not re-enqueued; any other state (COMPLETED etc.) is an illegal
    transition and raises (never silently enqueued twice).

    Generation consumption (T5, issue #51): the wake path consumes the
    waiter's epoch via TaskControlBlock.wake_claim.  Pass `required_gen` = the
    generation a producer captured at WAITING commit to REJECT a stale wake
    from a previous epoch (a no-op when the current generation no longer
    matches — required for EPIC #2's cross-worker producer so a stale wake can
    never re-transition a task that already woke).  Pass 0 (default) for
    today's single worker, where the epoch is trivially current; the state
    edge and enqueue-once still hold."""
    if h.state() == TaskControlBlock.RUNNABLE:
        return
    if h.tcb()[].wake_claim(required_gen):
        rt.enqueue(Int(h.tcb()), h.id())
        return
    if h.state() == TaskControlBlock.WAITING:
        # blocked WAITING but required_gen was stale -> reject silently; never
        # double-enqueue, never transition.
        return
    # not RUNNABLE and not successfully claimed -> transition (raises for an
    # illegal pair, preserving the A1 loud surface on COMPLETED etc.).
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)


# ---------------------------------------------------------------------------
# PARKING-LOT-ADAPTER (A2 seam; not wired on A1)
# ---------------------------------------------------------------------------
#
# On the A2 M:N scheduler a wake may come from another worker, so the A0.7
# two-phase protocol applies and the primitive set grows to:
#
#   park_prepare(h)   — RUNNING -> PARKING, open the early-wake window;
#                        publish the embedded WaitNode + waiter id;
#   park_validate()   — re-check readiness; ready => runnable WITHOUT a
#                        generation bump (task never left RUNNING);
#   park_commit(h, r) — close the window: readiness/cancel unwinds via
#                        PARKING -> RUNNABLE (WAITING never entered); else
#                        PARKING -> WAITING, fresh generation, reason `r`;
#   unpark_current     — as above, claiming the waiter's generation EXACTLY
#                        ONCE per epoch (spec §23; A0-T11/A0-T12).
#
# That adapter is the spike's event.mojo park_pipeline promoted into this
# module when #2 lands; A1 ships only the WAITING-side pair this worker can
# actually race on (which is none).
