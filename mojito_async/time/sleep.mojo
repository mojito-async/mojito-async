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
#     `(mut rt, h)` explicitly (yield_now, park_current, unpark_current,
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
# The wake path is the single canonical park/wake (issue #39).  Arm FIRST
# (publish the timer under the heap's own guard), THEN park — mirroring the
# ordering mutex.lock()/semaphore.acquire() already use (publish as a
# potential waiter BEFORE parking).
#
# TWO-PHASE PARK (#112, issue #112 item 3 — migrated from single-phase
# `park_current`): worker_timer.mojo's cross-worker deadline delivery
# (A6.3, issue #86: `arm_remote`/`deliver_deadline`, routed through the
# SAME owner_worker stamp `unpark_current`/`_owner_rt` use) means a timer
# armed here can expire and wake this task from ANOTHER worker while this
# call is still between `heap.arm` and the WAITING commit — the identical
# lost-wakeup window A4.1 (issue #55) closed for Mutex/Semaphore: a single-
# phase `park_current` never consults the early-wake latch, so a foreign
# expiry landing in that window would silently park the task FOREVER (the
# timer already fired and will never fire again).  `park_prepare` +
# `park_validate` + `park_commit` close it exactly like mutex.lock()'s
# slow path: a validate hit unwinds PARKING -> RUNNABLE in THIS SAME call
# (the task never actually slept) and `claim_running` re-establishes
# RUNNING before the caller's own code resumes past the sleep — a sleep
# whose deadline the very act of arming it made immediately due (or that
# raced a foreign delivery) returns exactly as if it had already elapsed,
# instead of losing the wakeup.  On the A1 single worker (owner_runtime
# == 0) this is a no-op fast path identical to the old single-phase
# behavior (park.mojo's module header: "no cross-worker interleaving").
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle, SuspendReason, claim_running
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Deadline, Duration
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.runtime.park import park_commit, park_prepare, park_validate


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

    Arms the heap (deadline = clock.now() + duration.ticks()) then parks
    through the TWO-PHASE kernel (module header, issue #112 item 3):
    PARKING -> WAITING (reason TIMER) normally, or PARKING -> RUNNABLE in
    THIS SAME call if a cross-worker deadline delivery already landed in
    the window.  The task is woken by the scheduler-loop servicing hook
    (or resumes immediately, in the race case) — no OS-thread block; no
    per-suspend allocation beyond the heap's owned storage."""
    var deadline = clock.now() + duration.ticks()
    _ = heap.arm(h.id(), Int(h.tcb()), deadline)
    park_prepare(h)
    if park_validate(h):
        _ = park_commit(h)
        claim_running(h)
        return
    if not park_commit(h, SuspendReason.TIMER):
        _ = heap.cancel(h.id())
        claim_running(h)


def sleep_until_current[R: ResultValue](
    mut rt: Runtime,
    mut h: JoinHandle[R],
    mut heap: TimerHeap,
    clock: MonotonicClock,
    deadline: Deadline,
) raises:
    """Park the CURRENT task until the absolute monotonic `deadline` (A1.1
    ms Deadline, mapped onto the ns clock by tick*1000000).  No OS-thread
    block; two-phase timer-based park like sleep_current (module header,
    issue #112 item 3)."""
    var ticks = UInt64(deadline.at_ms()) * 1000000
    var now = clock.now()
    if ticks <= now:
        # Already expired: park with an immediately-due timer so the service
        # hook still wakes this task through the canonical path (no spin).
        ticks = now
    _ = heap.arm(h.id(), Int(h.tcb()), ticks)
    park_prepare(h)
    if park_validate(h):
        _ = park_commit(h)
        claim_running(h)
        return
    if not park_commit(h, SuspendReason.TIMER):
        _ = heap.cancel(h.id())
        claim_running(h)