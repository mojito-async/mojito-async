# mojito_async/time/sleep.mojo
#
# A1.4 timer lane (issue #36) — the real timer-based sleep.
#
# Replaces the A1.1 raising stub (which landed as a compile-surface only,
# "typed exactly as A1.4 will ship it") with the ACTUAL monotonic-deadline
# park on the timer heap.  Signatures stay stable:
#   - `sleep(duration: Duration) raises` — the §7.1 surface.  Under b2
#     (def-only, no module globals, no TLS) a bare free function CANNOT
#     reach the current task's TCB or the runtime without an explicit
#     context argument — exactly why every runtime primitive threads
#     `(mut rt, h)` explicitly (yield_now, _suspend_current, resume_current,
#     wake).  Called bare (no driven frame) it raises a precise, documented
#     error rather than pretending to time out or silently parking nowhere.
#   - `sleep_current(rt, h, heap, clock, duration)` — the REAL park: arms a
#     monotonic timer (deadline = clock.now() + duration) and suspends the
#     CURRENT task through the canonical RUNNING -> PARKING -> WAITING path
#     with SuspendReason.TIMER.  No OS-thread block, no spin: the task drops
#     off the runnable queue and the scheduler-loop servicing hook
#     (timer_service.service_timers) wakes it once its deadline is due.
#   - `sleep_until_current(rt, h, heap, clock, deadline)` — the absolute-
#     deadline variant (sleep_until).  Deadline is the A1.1 ms Deadline;
#     mapped onto the ns clock by the lane's tick convention.
#
# The wake path is the single canonical park/wake (issue #39): arm FIRST
# (publish the timer), THEN park — under the single cooperative worker there
# is no concurrent service pass inside a task body, so the arm-before-park
# order removes any park/expiry race for the unit surface.
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import _suspend_current
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle, SuspendReason
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Deadline, Duration
from mojito_async.time.timer_heap import TimerHeap


def sleep(duration: Duration) raises:
    """§7.1 `sleep(Duration)` surface — stable signature.

    A bare call has no runtime to park (b2: no global current task), so it
    raises a precise context error.  The implemented park is
    `sleep_current(rt, h, heap, clock, duration)` — use it from a driven
    scheduler frame (dispatcher/task body with the frame handle)."""
    raise Error(
        "sleep: A1.4 timer lane: parking requires a driven scheduler frame "
        "(b2 has no global current task); call "
        "sleep_current(rt, h, heap, clock, duration) inside the dispatcher"
    )


def sleep_until(deadline: Deadline) raises:
    """§7.1 `sleep_until(Deadline)` surface — stable signature; same context
    note as sleep().  Implemented park: `sleep_until_current(rt, h, heap,
    clock, deadline)` from a driven scheduler frame."""
    raise Error(
        "sleep_until: A1.4 timer lane: parking requires a driven scheduler "
        "frame (b2 has no global current task); call "
        "sleep_until_current(rt, h, heap, clock, deadline) inside the "
        "dispatcher"
    )


def sleep_current[R: ResultValue](
    mut rt: Runtime,
    mut h: JoinHandle[R],
    mut heap: TimerHeap,
    clock: MonotonicClock,
    duration: Duration,
) raises:
    """Park the CURRENT task for `duration` (monotonic ns) via a timer.

    Arms the heap (deadline = clock.now() + duration.ticks()) and parks
    through the canonical path (RUNNING -> PARKING -> WAITING, reason TIMER).
    The task is woken by the scheduler-loop servicing hook when the deadline
    is due.  No OS-thread block; no per-suspend allocation beyond the heap's
    owned storage."""
    var deadline = clock.now() + duration.ticks()
    _ = heap.arm(h.id(), Int(h.tcb()), deadline)
    _suspend_current(rt, h, SuspendReason.TIMER)


def sleep_until_current[R: ResultValue](
    mut rt: Runtime,
    mut h: JoinHandle[R],
    mut heap: TimerHeap,
    clock: MonotonicClock,
    deadline: Deadline,
) raises:
    """Park the CURRENT task until the absolute monotonic `deadline` (A1.1
    ms Deadline, mapped onto the ns clock by tick*1000000).  No OS-thread
    block; timer-based park like sleep_current."""
    var ticks = UInt64(deadline.at_ms()) * 1000000
    var now = clock.now()
    if ticks <= now:
        # Already expired: park with an immediately-due timer so the service
        # hook still wakes this task through the canonical path (no spin).
        ticks = now
    _ = heap.arm(h.id(), Int(h.tcb()), ticks)
    _suspend_current(rt, h, SuspendReason.TIMER)