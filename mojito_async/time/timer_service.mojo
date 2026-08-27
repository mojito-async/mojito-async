# mojito_async/time/timer_service.mojo
#
# A1.4 timer lane (issue #36) — timer expiry SERVICE = the scheduler-loop
# servicing hook (spec §31 / issue #39 "single-source park/wake").
#
# The min-heap (timer_heap.mojo) only orders deadlines.  Expiry is driven
# HERE: `service_timers` pops every due timer (deadline <= now) in deadline
# order, suppresses stale generations (a popped entry whose granted gen no
# longer matches the live armed gen for its id is DROPPED, never a wake),
# and wakes each live WAITING task through the A1.1 canonical park/wake path
# (`resume_current`: WAITING -> RUNNABLE + re-enqueue, once per epoch —
# issue #39 single-source).  `drive_step` composes the A1.1 scheduler loop
# with the timer service: run ready tasks, then service due timers — the
# deterministic virtual-clock stepping the tests/drivers perform.
#
# Extern-free; the wake path allocates nothing beyond the heap's owned
# storage: touching exactly the embedded TCBs + the runnable queue.
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Nil, Runtime
from mojito_async.runtime.scheduler import resume_current, scheduler_loop
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.task import JoinHandle
from mojito_async.time.timer_heap import TimerHeap


def service_timers[R: ResultValue](
    mut rt: Runtime,
    mut heap: TimerHeap,
    now: UInt64,
) raises -> Int:
    """Deadline-integration hook: pop every timer due at `now` in deadline
    order; skip stale generations (superseded/cancelled arms) and tasks no
    longer WAITING; wake each live sleeper ONCE via the canonical park/wake.
    Returns the number of tasks woken.  After the call the runnable queue
    holds one record per woken task."""
    var woke = 0
    while heap.has_due(now):
        var e = heap.pop_min()
        if heap.live_gen(e.id) != e.gen:
            continue  # stale generation — superseded arm, drop
        var h = JoinHandle[R](
            UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
                unsafe_from_address=e.tcbaddr
            ),
            e.id,
        )
        if h.state() == TaskControlBlock.WAITING:
            resume_current(rt, h)
            woke += 1
    return woke


def drive_step[
    F: def(mut Runtime, Int, Int, BytePtr) raises -> Int,
    R: ResultValue = Nil,
](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
    mut heap: TimerHeap,
    now: UInt64,
) raises -> Int:
    """One scheduler service step: drive all READY tasks (A1.1
    scheduler_loop) then service due timers.  Returns total records served +
    waiters woken.  The driver/virtual-clock owner advances `now` between
    steps (spec §76.5 virtual time)."""
    var served = scheduler_loop(rt, dispatcher, ud)
    var woke = service_timers[R](rt, heap, now)
    return served + woke