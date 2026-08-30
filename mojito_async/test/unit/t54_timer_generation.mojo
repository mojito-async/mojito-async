# mojito_async/test/unit/t54_timer_generation.mojo
#
# GREEN driver for issue #147 — timer/timeout/cancel producers now pass
# `required_gen` to `unpark_current`, and the racy `state() == WAITING`
# pre-check that used to drop wakes for PARKING tasks has been removed.
#
# Two scenarios, each verifying a distinct facet of the fix:
#
# SCENARIO A — DROPPED EXPIRY PREVENTED.  `service_timers` previously had:
#
#     if h.state() == TaskControlBlock.WAITING:
#         unpark_current(rt, h)    # required_gen defaults to 0
#
# A task mid-PARKING (two-phase window open, WAITING not yet committed)
# failed that check — the heap entry was consumed but the wake was never
# delivered.  The fix removes the WAITING pre-check and calls
# `unpark_current` unconditionally (with `required_gen = wait_node().
# generation()`).  `unpark_current` handles PARKING via the early-wake latch
# (`park_validate` then sees readiness and `park_commit` unwinds to RUNNABLE
# rather than committing to WAITING).
#
# SCENARIO B — H2 DUPLICATE REJECTED DURING PARKING.  A timer from a
# PREVIOUS wait epoch (generation already claimed, `_claim_epoch` set)
# fires while the task is mid-PARKING for a NEW, unrelated wait.  With
# `required_gen = 0` the `_stale_or_duplicate` guard never engages, setting
# a phantom early-readiness latch — the task would unwind to RUNNABLE for
# the wrong reason.  With `required_gen = wait_node().generation()` (which
# equals the previous epoch's committed generation while the task is still
# PARKING, before the new WAITING commit bumps it), `_stale_or_duplicate`
# finds `claimed_epoch() == required_gen` — the epoch was already consumed —
# and rejects the early latch silently.
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
from mojito_async.task import JoinHandle, claim_running, spawn


comptime TB = TaskControlBlock[IntResult]


# ---------------------------------------------------------------------------
# Scenario A — the expiry delivered during PARKING via the early-wake latch.
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
    var woke = service_timers[IntResult](rt, heap, UInt64(100))

    if heap.size() != 0:
        failures.append("A: the heap entry should have been popped")
        return

    # After the fix: service_timers calls unpark_current unconditionally,
    # which latches early readiness for the PARKING task.
    # park_validate must now return True (early latch was set).
    if not park_validate(h):
        failures.append(
            "A: DROPPED EXPIRY — service_timers did not latch early readiness"
            + " for the task mid-PARKING; the wake was dropped and the task"
            + " would wait forever. Fix: remove the state==WAITING pre-check"
            + " and call unpark_current unconditionally with the correct"
            + " required_gen so the early-wake latch is properly set."
        )
        _ = park_commit(h)
        return

    # Consume the early latch. park_commit must unwind to RUNNABLE (not
    # WAITING): the task never truly slept, the expiry was delivered.
    _ = park_commit(h)
    if h.state() == TaskControlBlock.WAITING:
        failures.append(
            "A: park_commit committed to WAITING despite an early-wake latch;"
            + " the expired timer should have unbound the park."
        )


# ---------------------------------------------------------------------------
# Scenario B — stale timer from a previous epoch rejected during PARKING.
# ---------------------------------------------------------------------------

struct BStep(ImplicitlyCopyable, ImplicitlyDeletable):
    var step: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.step = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def dispatch_b(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    """Step 0: a plain timed park (first epoch).
    Step 1: park_prepare only — open the two-phase window for the second
    epoch without committing, so the stale timer can fire mid-PARKING."""
    var bs = ud.bitcast[BStep]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    var s = bs[].step[]
    bs[].step[] = s + 1
    if s == 0:
        park_current(rt, h, SuspendReason.TIMER)
        return 1
    # s == 1: open the two-phase window, leave in PARKING
    park_prepare(h)
    return 1


def scenario_stale_arm(mut failures: List[String]) raises:
    var rt = create()
    var heap = TimerHeap()
    var step = 0
    var bs = BStep()
    bs.step = UnsafePointer[Int, MutAnyOrigin](to=step)
    var bsp = UnsafePointer[BStep, MutAnyOrigin](to=bs)
    var ud = bsp.bitcast[Byte]()

    var tcb = TB.create()
    var tcbp = UnsafePointer[TB, MutAnyOrigin](to=tcb)
    var h = spawn(rt, tcbp, 0)

    # Arm a deadline and enter the first wait (gen bumps to 2, wait_node.gen=2).
    _ = heap.arm(h.id(), Int(tcbp), UInt64(500))
    _ = scheduler_loop(rt, dispatch_b, ud)
    if h.state() != TaskControlBlock.WAITING:
        failures.append("B: the first wait did not commit")
        return
    var gen1 = tcb.generation()  # == 2

    # The operation completes EARLY via a non-timer wake (e.g. READY).
    # The arm is intentionally LEFT in the heap — this simulates select's
    # close-winner path where losing branches' timers are not cancelled.
    # After wake_claim(gen1=2): _claim_epoch = 2.
    unpark_current(rt, h, required_gen=gen1, win_reason=SuspendReason.READY)
    if heap.size() != 1:
        failures.append("B: the stale arm should still be in the heap after early wake")
        return
    if tcb.claimed_epoch() != gen1:
        failures.append("B: _claim_epoch should equal gen1 after the first wake claim")
        return

    # The task runs again and opens the two-phase window for a SECOND sleep
    # (park_prepare -> PARKING). wait_node().generation() is still gen1=2
    # because no new WAITING commit has happened yet.
    _ = scheduler_loop(rt, dispatch_b, ud)
    if h.state() != TaskControlBlock.PARKING:
        failures.append("B: dispatch_b step 1 must leave the task PARKING (state "
                        + String(h.state()) + ")")
        return

    # Verify: at PARKING, wait_node.generation() is the PREVIOUS epoch's gen.
    # _stale_or_duplicate(h, gen1) will fire: claimed_epoch()==gen1==required_gen.
    if tcb.wait_node()[].generation() != gen1:
        failures.append("B: wait_node.generation() should still be gen1 during PARKING"
                        + " (got " + String(tcb.wait_node()[].generation()) + ")")

    # Now the stale deadline fires.  With the fix, service_timers reads
    # wait_node().generation() == gen1 and passes required_gen=gen1.
    # _stale_or_duplicate sees claimed_epoch()==gen1==required_gen -> REJECTED.
    # The early-wake latch must NOT be set.
    var woke = service_timers[IntResult](rt, heap, UInt64(500))

    if heap.size() != 0:
        failures.append("B: the heap entry should have been popped")

    # The stale wake must be silently rejected — no early latch, no
    # spurious RUNNABLE transition.
    if park_validate(h):
        failures.append(
            "B: SPURIOUS EARLY LATCH — the stale timer (epoch " + String(gen1)
            + ", already claimed, _claim_epoch=" + String(tcb.claimed_epoch())
            + ") set the early-readiness latch during PARKING for the new epoch."
            + " Fix: service_timers must pass required_gen=wait_node().generation()"
            + " so _stale_or_duplicate detects claimed_epoch()==required_gen and"
            + " rejects the duplicate early latch."
        )
        _ = park_commit(h)
        return

    # No spurious latch: park_commit should commit properly to WAITING (gen=3).
    var committed = park_commit(h, SuspendReason.TIMER)
    if not committed:
        failures.append(
            "B: park_commit returned False (early wake unwind) but"
            + " park_validate returned False — inconsistent state"
        )
        return
    var gen2 = tcb.generation()
    if gen2 == gen1:
        failures.append("B: second WAITING commit must produce a fresh epoch")
    if h.state() != TaskControlBlock.WAITING:
        failures.append("B: the task should be WAITING for the second sleep (state "
                        + String(h.state()) + ")")


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
