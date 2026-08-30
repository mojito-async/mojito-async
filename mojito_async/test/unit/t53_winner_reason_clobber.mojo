# mojito_async/test/unit/t53_winner_reason_clobber.mojo
#
# RED driver for issue #146 — the winner reason is stamped OUTSIDE the guard,
# before the claim, so a losing canceller overwrites the winner's outcome.
#
# `runtime/park.mojo:412-424`, `wake_cancelled`.  Its docstring is a precise
# description of behaviour the code does not have:
#
#   "If readiness already claimed the wake (the task is no longer WAITING),
#    unpark_current's existing no-op path fires and `_reason` is left
#    untouched — the earlier winner's stamp (or lack of one) stands; this
#    call never overwrites a settled outcome."
#
# The very next two lines:
#
#     h.tcb()[].wait_node()[].set_reason(SuspendReason.CANCEL)
#     unpark_current(rt, h)
#
# Unconditional, before the claim.  `reactor/cancel.mojo:131-132` does the
# same for CANCEL/CLOSED.  Both contradict the kernel's own A4.4 contract,
# which requires the winner reason be stamped "INSIDE THE SAME owner
# remote-queue guard section that performs wake_claim" — and the mechanism
# for that already exists and is unused here: `unpark_current`'s `win_reason`
# parameter, which t37_winner_reason already proves is correct.
#
# THE CONSEQUENCE.  On resume, `raise_if_cancel_wake` sees CANCEL and raises
# CancellationError.  So a task that genuinely WON readiness raises instead of
# proceeding, and with a mutex a granted lock is leaked, because nobody
# unlocks it.
#
# Both scenarios below are single-worker and fully deterministic.  There is no
# race to time: the defect is that the stamp is unconditional, so simply
# calling the loser SECOND, after the winner has already settled the outcome,
# is enough to show it.
#
# Scenario 1 is the kernel contract, tested directly against the docstring
# above.  Scenario 2 is the live consequence through the real Mutex, using a
# TIMER as the winner: `time/timer_service.mojo:48-50` wakes a WAITING task
# with `unpark_current` and does NOT remove it from any primitive's FIFO, so
# a canceller arriving afterwards still finds the waiter queued, still passes
# its own pre-checks, and still stamps.  That is exactly the issue's note that
# "the timer and reactor paths have no FIFO to remove from at all".
#
# NOT covered here: `reactor/cancel.mojo`'s `_cancel_with_reason` carries a
# `state() != WAITING` pre-check, so on one worker it returns before stamping.
# Its stamp lands only when the pre-check passes and readiness then wins
# before the `set_reason` line executes, which needs two threads.  I am not
# claiming a red I did not observe, so that one is named and left alone.
#
# Verdict: exit 0 + "PASS"; any failure prints the details and raises.
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import (
    is_cancel_wake,
    park_current,
    raise_if_cancel_wake,
    unpark_current,
    wake_cancelled,
)
from mojito_async.runtime.join_handle import SuspendReason
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import Mutex
from mojito_async.cancellation import CancelFlag, CancellationToken, make_cancel_flag
from mojito_async.task import JoinHandle, claim_running, spawn


comptime TB = TaskControlBlock[IntResult]


def _name(reason: Int) -> String:
    if reason == SuspendReason.NONE:
        return "NONE"
    if reason == SuspendReason.PARK:
        return "PARK"
    if reason == SuspendReason.CANCEL:
        return "CANCEL"
    if reason == SuspendReason.TIMER:
        return "TIMER"
    if reason == SuspendReason.READY:
        return "READY"
    if reason == SuspendReason.CLOSED:
        return "CLOSED"
    return "?(" + String(reason) + ")"


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """slice counter @0, task id @1."""

    var slice: UnsafePointer[Int, MutAnyOrigin]
    var a_id: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.slice = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.a_id = self.slice


# ---------------------------------------------------------------------------
# Scenario 1 — the kernel contract wake_cancelled's own docstring states.
# ---------------------------------------------------------------------------

def dispatch_park_once(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    park_current(rt, h)
    return 1


def scenario_kernel(mut failures: List[String]) raises:
    var rt = create()
    var sc = Scene()
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    _ = scheduler_loop(rt, dispatch_park_once, ud)
    if h.state() != TaskControlBlock.WAITING:
        failures.append("S1: task must be WAITING before the race")
        return

    var gen = tcb.generation()

    # READINESS WINS.  This is the correct, in-guard stamp: unpark_current
    # only writes the reason on a successful claim.
    unpark_current(rt, h, required_gen=gen, win_reason=SuspendReason.READY)
    if h.state() != TaskControlBlock.RUNNABLE:
        failures.append("S1: readiness claim did not make the task RUNNABLE")
    if tcb.wait_node()[].reason() != SuspendReason.READY:
        failures.append("S1: winner did not stamp READY (got "
                        + _name(tcb.wait_node()[].reason()) + ")")

    # THE LOSER, arriving second against an already-settled outcome.
    var enq_before = rt.enqueued()
    wake_cancelled(rt, h)

    if rt.enqueued() != enq_before:
        failures.append("S1: the losing wake_cancelled enqueued the task again")
    if h.state() != TaskControlBlock.RUNNABLE:
        failures.append("S1: the losing wake_cancelled changed the task state")
    if tcb.wait_node()[].reason() != SuspendReason.READY:
        failures.append(
            "S1: wake_cancelled CLOBBERED the settled outcome — reason is "
            + _name(tcb.wait_node()[].reason())
            + ", expected READY. Its own docstring says 'this call never"
            + " overwrites a settled outcome'; it stamps unconditionally"
            + " before the claim (park.mojo:412-424)."
        )
    if is_cancel_wake(h):
        failures.append("S1: is_cancel_wake reports CANCEL won, but READY did")
    var raised = False
    try:
        raise_if_cancel_wake(h)
    except e:
        raised = True
    if raised:
        failures.append(
            "S1: raise_if_cancel_wake raised on a task whose wake was won by"
            + " READY — a task that won readiness raises CancellationError"
        )


# ---------------------------------------------------------------------------
# Scenario 2 — the live consequence: a mutex waiter woken by a TIMER, then
# stamped CANCEL by a canceller that lost.
# ---------------------------------------------------------------------------

struct MScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var flag: UnsafePointer[CancelFlag, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]
    var got: UnsafePointer[Int, MutAnyOrigin]
    var raised: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.flag = UnsafePointer[CancelFlag, MutAnyOrigin](unsafe_from_address=1)
        self.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.got = self.phase
        self.raised = self.phase


def dispatch_lock(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    """The waiter's body, written the way a caller with a cancellation token
    writes it.  `lock_cancellable`'s FIRST act is `raise_if_cancel_wake(h)`:
    the post-resume winner check that is supposed to fire only when THIS
    waiter's own cancel won the race."""
    var ms = ud.bitcast[MScene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    var token = CancellationToken(ms[].flag)
    try:
        if ms[].mtx[].lock_cancellable(rt, h, token):
            ms[].got[] = 1
    except e:
        ms[].raised[] = 1
    return 1


def scenario_mutex_timer(mut failures: List[String]) raises:
    var rt = create()
    var mtx = Mutex[Int](0)
    var flag = make_cancel_flag()
    var ms = MScene()
    var cells = stack_allocation[4, Int]()
    for i in range(4):
        cells[i] = 0
    ms.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    ms.flag = UnsafePointer[CancelFlag, MutAnyOrigin](to=flag)
    ms.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(cells))
    ms.got = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(cells) + 8)
    ms.raised = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(cells) + 16)
    var msp = UnsafePointer[MScene, MutAnyOrigin](to=ms)
    var ud = msp.bitcast[Byte]()

    # Somebody else holds the lock, so our waiter takes the slow park path.
    if not mtx.try_lock():
        failures.append("S2: could not take the lock for the setup")
        return

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    _ = scheduler_loop(rt, dispatch_lock, ud)
    if h.state() != TaskControlBlock.WAITING:
        failures.append("S2: the waiter must be WAITING on the mutex (state "
                        + String(h.state()) + ")")
        return
    if mtx.waiter_count() != 1:
        failures.append("S2: the waiter must be queued on the mutex FIFO")
        return

    # A TIMER wins the wake.  This is exactly what timer_service.mojo:48-50
    # does for a lock-with-timeout: it wakes the WAITING task through
    # unpark_current and removes it from NO primitive's FIFO.
    var gen = tcb.generation()
    unpark_current(rt, h, required_gen=gen, win_reason=SuspendReason.TIMER)
    if tcb.wait_node()[].reason() != SuspendReason.TIMER:
        failures.append("S2: the timer did not win the claim (reason "
                        + _name(tcb.wait_node()[].reason()) + ")")
        return

    # The canceller runs second and LOSES the claim.  It still finds the
    # waiter in the mutex FIFO — the timer left it there — so its own
    # FIFO-removal pre-check passes and it goes on to stamp.
    var won = mtx.cancel_lock_wait[IntResult](rt, h)
    if tcb.wait_node()[].reason() != SuspendReason.TIMER:
        failures.append(
            "S2: the losing canceller CLOBBERED the timer's winning stamp —"
            + " reason is " + _name(tcb.wait_node()[].reason())
            + ", expected TIMER (cancel_lock_wait reported won=" + String(won)
            + ", but the wake claim was already settled)"
        )

    # Re-dispatch the waiter.  With CANCEL on the node, the very first line
    # of lock_cancellable raises.
    _ = scheduler_loop(rt, dispatch_lock, ud)
    if cells[2] != 0:
        failures.append(
            "S2: the resumed waiter raised CancellationError even though the"
            + " TIMER won its wake — and the mutex is left _locked="
            + String(mtx.is_locked()) + " with waiter_count="
            + String(mtx.waiter_count())
        )


def main() raises:
    var failures = List[String]()
    scenario_kernel(failures)
    scenario_mutex_timer(failures)
    if len(failures) == 0:
        print("T53 winner reason clobber: PASS")
    else:
        print("T53 winner reason clobber: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        raise Error("T53 winner reason clobber: RED")
