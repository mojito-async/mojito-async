# mojito_async/test/unit/t54_timer_generation.mojo
#
# RED driver for issue #147 — no timer/timeout/cancel producer passes
# `required_gen`, so the entire epoch machinery is bypassed on every live
# path, and the racy `state() == WAITING` pre-check that stands in for it
# drops wakes.
#
# `time/timer_service.mojo:48-50`:
#
#     if h.state() == TaskControlBlock.WAITING:
#         unpark_current(rt, h)          # required_gen defaults to 0
#
# `time/timeout_scope.mojo:450,462` and `reactor/cancel.mojo:127` have the
# same shape.  The stale/duplicate defences — `_stale_or_duplicate`,
# `claimed_epoch`, H2 of PR #109 — only engage when `required_gen != 0`, so
# none of them ever run.
#
# Two failures, one driver, both deterministic on a single worker.
#
# SCENARIO A — DROPPED EXPIRY.  `service_timers` POPS the heap entry and only
# then asks whether the task is WAITING.  A task that is mid-PARKING (the
# two-phase window is open, WAITING has not committed yet) fails that check,
# so the wake is not delivered — but the entry is already gone from the heap,
# so nobody will ever deliver it.  `unpark_current` handles PARKING correctly
# through the early-wake latch; it is the CALLER's pre-check that defeats it.
# The task then commits to WAITING and sleeps forever.
#
# SCENARIO B — WRONG-EPOCH CLAIM.  A timer armed for one wait, left armed
# after that wait ended (which `channel/select.mojo:538-579`'s close-winner
# path does by design, and timer ids are task ids so the collision is with
# yourself), fires into a LATER, unrelated wait.  `state() == WAITING` is true
# — for a different epoch — and the gen-0 wake claims it.  For a mutex waiter
# that means a resume with no GRANT marker and a second FIFO self-append.
#
# Verdict: exit 0 + "PASS"; any failure prints the details and raises.
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import (
    park_commit,
    park_current,
    park_prepare,
    park_validate,
    unpark_current,
)
from mojito_async.runtime.join_handle import SuspendReason
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.time.timer_service import service_timers
from mojito_async.sync import Mutex
from mojito_async.task import JoinHandle, claim_running, spawn


comptime TB = TaskControlBlock[IntResult]


# ---------------------------------------------------------------------------
# Scenario A — the expiry that lands mid-PARKING is dropped for good.
# ---------------------------------------------------------------------------

def dispatch_prepare_only(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    """Open the two-phase window and return, leaving the task PARKING —
    the exact state `sleep_until_current` is in between publishing its arm
    and committing to WAITING."""
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    park_prepare(h)
    return 1


def scenario_dropped_expiry(mut failures: List[String]) raises:
    var rt = create()
    var heap = TimerHeap()
    var sc = 0
    var scp = UnsafePointer[Int, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var tcbp = UnsafePointer[TB, MutAnyOrigin](to=tcb)
    var h = spawn(rt, tcbp, 0)

    # Arm the deadline the way a sleeper does, then open the park window.
    _ = heap.arm(h.id(), Int(tcbp), UInt64(100))
    _ = scheduler_loop(rt, dispatch_prepare_only, ud)
    if h.state() != TaskControlBlock.PARKING:
        failures.append("A: the task must be PARKING for this window (state "
                        + String(h.state()) + ")")
        return
    if heap.size() != 1:
        failures.append("A: the timer must be armed before the expiry")
        return

    # The deadline passes while the task is still mid-PARKING.
    var enq_before = rt.enqueued()
    var woke = service_timers[IntResult](rt, heap, UInt64(100))

    if heap.size() != 0:
        failures.append("A: the heap entry should have been popped")
    if woke != 0:
        failures.append("A: expected the pre-check to refuse the wake")

    # Close the window.  Nothing latched early readiness, because
    # service_timers never called unpark_current at all.
    if park_validate(h):
        failures.append(
            "A: unexpected early-wake latch — service_timers DID deliver;"
            + " this scenario no longer reproduces as written"
        )
        park_commit(h)
        return
    park_commit(h)

    if h.state() != TaskControlBlock.WAITING:
        failures.append("A: the task should have committed to WAITING")
    if rt.enqueued() != enq_before:
        failures.append("A: something enqueued the task after all")

    # The task is WAITING with its deadline in the past and no timer left
    # anywhere. Nothing will ever wake it.
    if h.state() == TaskControlBlock.WAITING and heap.size() == 0:
        failures.append(
            "A: DROPPED EXPIRY — the task is WAITING with its deadline"
            + " already past and the heap entry consumed. service_timers"
            + " popped the entry, then refused to deliver because the racy"
            + " state() == WAITING pre-check saw PARKING. unpark_current"
            + " handles PARKING correctly via the early-wake latch; the"
            + " caller's pre-check is what defeats it (timer_service.mojo:48)."
            + " This task sleeps forever."
        )


# ---------------------------------------------------------------------------
# Scenario B — a stale arm claims a later, unrelated wait.
# ---------------------------------------------------------------------------

struct BScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var step: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.step = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def dispatch_b(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    """Step 0: a plain timed wait (the operation the arm belongs to).
    Step 1 onwards: contend for the mutex — a LATER, unrelated wait."""
    var bs = ud.bitcast[BScene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    var s = bs[].step[]
    bs[].step[] = s + 1
    if s == 0:
        park_current(rt, h, SuspendReason.TIMER)
        return 1
    _ = bs[].mtx[].lock(rt, h)
    return 1


def scenario_stale_arm(mut failures: List[String]) raises:
    var rt = create()
    var heap = TimerHeap()
    var mtx = Mutex[Int](0)
    var step = 0
    var bs = BScene()
    bs.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    bs.step = UnsafePointer[Int, MutAnyOrigin](to=step)
    var bsp = UnsafePointer[BScene, MutAnyOrigin](to=bs)
    var ud = bsp.bitcast[Byte]()

    if not mtx.try_lock():
        failures.append("B: could not take the lock for the setup")
        return

    var tcb = TB.create()
    var tcbp = UnsafePointer[TB, MutAnyOrigin](to=tcb)
    var h = spawn(rt, tcbp, 0)

    # The task arms a deadline and enters its first wait.
    _ = heap.arm(h.id(), Int(tcbp), UInt64(500))
    _ = scheduler_loop(rt, dispatch_b, ud)
    if h.state() != TaskControlBlock.WAITING:
        failures.append("B: the first wait did not commit")
        return
    var gen1 = tcb.generation()

    # The operation completes EARLY — readiness wins, well before the
    # deadline. The arm is left in the heap: this is select's close-winner
    # path, which does not cancel the timers of the branches that lost.
    unpark_current(rt, h, required_gen=gen1, win_reason=SuspendReason.READY)
    if heap.size() != 1:
        failures.append("B: the stale arm should still be in the heap")
        return

    # The task runs on and parks on a completely unrelated wait: the mutex.
    _ = scheduler_loop(rt, dispatch_b, ud)
    if h.state() != TaskControlBlock.WAITING:
        failures.append("B: the mutex wait did not commit (state "
                        + String(h.state()) + ")")
        return
    var gen2 = tcb.generation()
    if gen2 == gen1:
        failures.append("B: the second wait must be a fresh epoch")
    if mtx.waiter_count() != 1:
        failures.append("B: the task must be queued on the mutex FIFO")
        return

    # Now the stale deadline passes.  Timer ids are task ids, so this entry
    # still resolves to the same handle; live_gen still matches, because
    # nothing cancelled the arm; and state() == WAITING is true — for the
    # WRONG epoch.
    var woke = service_timers[IntResult](rt, heap, UInt64(500))

    if woke == 0:
        failures.append(
            "B: the stale arm did not fire; scenario no longer reproduces"
        )
        return

    if h.state() == TaskControlBlock.RUNNABLE:
        failures.append(
            "B: WRONG-EPOCH CLAIM — a timer armed for epoch " + String(gen1)
            + " claimed the unrelated mutex wait at epoch " + String(gen2)
            + ". timer_service passes required_gen=0, so"
            + " _stale_or_duplicate never engages."
        )
    if not mtx.holds_grant(h):
        failures.append(
            "B: the task was resumed from a mutex wait with NO grant marker,"
            + " so lock() cannot tell it owns anything"
        )
    if mtx.waiter_count() != 1:
        failures.append(
            "B: the timer removed the waiter from the mutex FIFO, which it"
            + " has no way to do; expected it still queued"
        )

    # Re-dispatch: lock() finds no marker, `_locked` is still True (it is
    # held across a handoff by design), so the task appends to the FIFO a
    # SECOND time.
    _ = scheduler_loop(rt, dispatch_b, ud)
    if mtx.waiter_count() > 1:
        failures.append(
            "B: DUPLICATED FIFO ENTRY — one task is queued "
            + String(mtx.waiter_count())
            + " times on the same mutex after the stale timer resumed it"
        )


def main() raises:
    var failures = List[String]()
    scenario_dropped_expiry(failures)
    scenario_stale_arm(failures)
    if len(failures) == 0:
        print("T54 timer generation: PASS")
    else:
        print("T54 timer generation: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        raise Error("T54 timer generation: RED")
