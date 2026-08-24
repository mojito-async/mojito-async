# spike/colorless_runtime/tests/t10_event_protocol.mojo
#
# A0.7 (issue #16) — Event-based park/wake: two-phase parking against the
# REAL TaskControlBlock state machine (TDD: written RED against event.mojo,
# goes GREEN once the implementation lands in this lane's branch).
#
# This is the integration counterpart of t9's modeled pipeline: the same
# HookScript schedules force races deterministically, but the four phases
# now drive a genuine TaskControlBlock (states, transitions, embedded
# WaitNode generation) plus a genuine Event (spec §23):
#
#     PREPARE  publish waiter as DATA (task id + pointer to the TCB's
#              embedded WaitNode); settle(cancel, readiness)
#     VALIDATE recheck readiness (the lost-wakeup window); readiness HERE
#              means wait() returns without sleeping (A0-T11)
#     COMMIT   RUNNING->PARKING->WAITING (generation bumps, node stamped);
#              early wake in the PARKING window -> RUNNABLE, never WAITING;
#              settle(cancel, readiness)
#     WAKE     set(): claim the waiter's generation EXACTLY ONCE ->
#              enqueue (data log) -> WAITING->RUNNABLE (A0-T12)
#
# Covered here (spec A0-T10/T11/T12 + generation discipline):
#   - wake-before-park: SET_READY at VALIDATE -> early wake, no sleep, no
#     generation bump, nothing enqueued (no lost wakeup)
#   - full park cycle: empty schedule parks through COMMIT, a WAKE-boundary
#     delivery resumes the task (WAITING->RUNNANCE-free: RUNNABLE), one
#     enqueue logged at the fresh generation
#   - duplicate set(): three deliveries in one epoch -> ONE accepted edge,
#     enqueue-log length 1 (exactly-once enqueue per generation)
#   - generation increments per park cycle; a STALE-generation wake is
#     ignored (rejected no-op)
#   - cancel-vs-readiness: both fire in one schedule -> cancellation takes
#     precedence (settle policy) and WinnerRecord guarantees EXACTLY ONE
#     winner; deterministic across repeated runs
#
# Pure Mojo: `mojo run -I spike/colorless_runtime` with no dylib.

from cancel import CancelFlag, make_cancel_flag
from event import Event, ParkOutcome, ParkReport, deliver_wake, park_pipeline
from race_hooks import (
    HookAction,
    HookPoint,
    HookScript,
    WinnerRecord,
    make_hook_script,
    script_add,
)
from task import ResultValue, TaskControlBlock


# --- helpers ---------------------------------------------------------------

def expect(cond: Bool, what: String) raises:
    if not cond:
        raise Error("check failed: " + what)


struct UnitResult(ResultValue):
    """Trivial result slot for the TCBs under test."""
    var v: Int

    def __init__(out self):
        self.v = 0


def make_tcb_running(tp: UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin]) raises:
    """Drive NEW -> RUNNABLE -> RUNNING (pipeline precondition)."""
    tp[].transition(TaskControlBlock.RUNNABLE)
    tp[].transition(TaskControlBlock.RUNNING)


def run_cycle(
    mut ev: Event,
    tp: UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin],
    fp: UnsafePointer[CancelFlag, MutAnyOrigin],
    hooks: HookScript,
) raises -> ParkReport:
    """One full park_pipeline invocation over a fresh report.

    `ev` is passed mutably so the pipeline's publishes/latches/wake
    claims land on the CALLER's event (asserted afterwards).
    """
    var rep = ParkReport()
    park_pipeline(ev, tp, fp, hooks, rep)
    return rep


def script_one(point: Int, action: Int) raises -> HookScript:
    var s = make_hook_script()
    script_add(s, point, action)
    return s


# --- test body -------------------------------------------------------------

def main() raises:
    var TASK_ID = 42

    # ------------------------------------------------------------------
    # 1. Fresh Event unit: unset, silent, empty enqueue log.
    # ------------------------------------------------------------------
    var ev0 = Event()
    expect(not ev0.is_set(), "fresh event is not set")
    expect(ev0.set_attempts() == 0, "no set attempts yet")
    expect(ev0.wakes_accepted() == 0, "no wakes accepted yet")
    expect(ev0.enqueue_len() == 0, "enqueue log empty")

    # ------------------------------------------------------------------
    # 2. Latch-only set(): readiness BEFORE any waiter publishes is a pure
    #    latch — recorded attempt, no accepted wake, nothing enqueued.
    # ------------------------------------------------------------------
    var evL = Event()
    var accL = evL.set()
    expect(not accL, "set() with no parked waiter accepts nothing")
    expect(evL.is_set(), "set() latches readiness")
    expect(evL.set_attempts() == 1, "attempt counted")
    expect(evL.enqueue_len() == 0, "nothing enqueued (no waiter armed)")

    # ------------------------------------------------------------------
    # 3. Baseline full park cycle: undisturbed schedule parks through all
    #    boundaries UNDECIDED — the real scheduler would block here; the
    #    TCB rests in WAITING at a bumped generation.
    # ------------------------------------------------------------------
    var tcbB = TaskControlBlock[UnitResult]()
    var tpB = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbB)
    var fpB_ = make_cancel_flag()
    var fpB = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpB_)
    make_tcb_running(tpB)
    var evB = Event()
    var repB = run_cycle(evB, tpB, fpB, make_hook_script())
    expect(repB.outcome == ParkOutcome.UNDECIDED, "baseline undecided")
    expect(repB.slept(), "baseline slept (committed to WAITING)")
    expect(not repB.winners.decided(), "baseline: no winner")
    expect(tpB[].state() == TaskControlBlock.WAITING, "blocked in WAITING")
    expect(
        tpB[].generation() == 2,
        "WAITING entry bumped generation 1 -> 2",
    )
    expect(
        tpB[].wait_node()[].generation() == 2,
        "embedded WaitNode carries the fresh generation",
    )
    expect(
        tpB[].wait_node()[].reason() == 0,
        "PREPARE stamps the wait reason (0 = EVENT)",
    )

    # ------------------------------------------------------------------
    # 4. Wake the blocked baseline task by hand: set() claims the CURRENT
    #    generation exactly once -> enqueue logged -> WAITING -> RUNNABLE.
    # ------------------------------------------------------------------
    var woke = deliver_wake(evB, tpB)
    expect(woke, "deliver_wake accepted at the current generation")
    expect(tpB[].state() == TaskControlBlock.RUNNABLE, "WAKE -> RUNNABLE")
    expect(evB.is_set(), "readiness latched by the manual wake")
    expect(evB.enqueue_len() == 1, "one enqueue logged")
    expect(
        evB.enqueued_gen(0) == 2,
        "enqueue recorded at the fresh generation",
    )
    expect(evB.claimed_gen() == 2, "generation claimed exactly once")

    # Duplicate delivery for the SAME generation: rejected no-op (A0-T12).
    var dup = deliver_wake(evB, tpB)
    expect(not dup, "duplicate wake for the same generation rejected")
    expect(evB.enqueue_len() == 1, "duplicate enqueued NOTHING (log length)")
    expect(evB.set_attempts() == 2, "both deliveries counted as attempts")
    expect(evB.wakes_accepted() == 1, "only one accepted edge")

    # ------------------------------------------------------------------
    # 5. A0-T11 wake-before-park: readiness delivered at VALIDATE time ->
    #    wait returns WITHOUT sleeping; no lost wakeup; no generation bump;
    #    nothing enqueued (the waiter was never committed).
    # ------------------------------------------------------------------
    var tcbE = TaskControlBlock[UnitResult]()
    var tpE = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbE)
    var fpE_ = make_cancel_flag()
    var fpE = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpE_)
    make_tcb_running(tpE)
    var evE = Event()
    var wb = make_hook_script()
    script_add(wb, HookPoint.VALIDATE, HookAction.SET_READY)
    var repE = run_cycle(evE, tpE, fpE, wb)
    expect(repE.outcome == ParkOutcome.EARLY_WAKE, "early wake outcome")
    expect(repE.decided_at == HookPoint.VALIDATE, "decided at VALIDATE")
    expect(not repE.slept(), "never slept (wake-before-park)")
    expect(
        repE.winners.exactly_one_winner(),
        "exactly one winner (READY)",
    )
    expect(repE.winners.winner() == WinnerRecord.READY, "READY won")
    expect(tpE[].state() == TaskControlBlock.RUNNING, "still RUNNING")
    expect(tpE[].generation() == 1, "no generation bump (never WAITING)")
    expect(evE.enqueue_len() == 0, "no enqueue (never parked)")
    expect(evE.wakes_accepted() == 0, "no accepted wake needed")

    # Same defense when readiness arrives even earlier, at PREPARE.
    var tcbE2 = TaskControlBlock[UnitResult]()
    var tpE2 = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbE2)
    var fpE2_ = make_cancel_flag()
    var fpE2 = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpE2_)
    make_tcb_running(tpE2)
    var evE2 = Event()
    var pb = make_hook_script()
    script_add(pb, HookPoint.PREPARE, HookAction.SET_READY)
    var repP = run_cycle(evE2, tpE2, fpE2, pb)
    expect(repP.outcome == ParkOutcome.EARLY_WAKE, "PREPARE-time early wake")
    expect(repP.decided_at == HookPoint.PREPARE, "decided at PREPARE")
    expect(not repP.slept(), "PREPARE-time readiness never sleeps")
    expect(tpE2[].state() == TaskControlBlock.RUNNING, "still RUNNING")

    # ------------------------------------------------------------------
    # 6. Early wake in the COMMIT window: RUNNING->PARKING observed
    #    readiness -> back to RUNNABLE, NEVER reaching WAITING.
    # ------------------------------------------------------------------
    var tcbC = TaskControlBlock[UnitResult]()
    var tpC = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbC)
    var fpC_ = make_cancel_flag()
    var fpC = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpC_)
    make_tcb_running(tpC)
    var evC = Event()
    var cb = make_hook_script()
    script_add(cb, HookPoint.COMMIT, HookAction.SET_READY)
    var repC = run_cycle(evC, tpC, fpC, cb)
    expect(repC.outcome == ParkOutcome.EARLY_WAKE, "COMMIT-window early wake")
    expect(repC.decided_at == HookPoint.COMMIT, "decided at COMMIT")
    expect(not repC.slept(), "did not stay parked")
    expect(tpC[].state() == TaskControlBlock.RUNNABLE, "PARKING -> RUNNABLE")
    expect(tpC[].generation() == 1, "never entered WAITING: no bump")

    # ------------------------------------------------------------------
    # 7. Scripted full cycle: WAKE fired at the WAKE boundary while parked.
    # ------------------------------------------------------------------
    var tcbF = TaskControlBlock[UnitResult]()
    var tpF = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbF)
    var fpF_ = make_cancel_flag()
    var fpF = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpF_)
    make_tcb_running(tpF)
    var evF = Event()
    var fw = script_one(HookPoint.WAKE, HookAction.WAKE)
    var repF = run_cycle(evF, tpF, fpF, fw)
    expect(repF.outcome == ParkOutcome.PARKED_AND_WOKE, "parked then woken")
    expect(repF.decided_at == HookPoint.WAKE, "decided at WAKE")
    expect(repF.slept(), "slept before the wake")
    expect(repF.winners.exactly_one_winner(), "single READY winner")
    expect(tpF[].state() == TaskControlBlock.RUNNABLE, "resumed RUNNABLE")
    expect(tpF[].generation() == 2, "one park epoch: generation 1 -> 2")
    expect(evF.enqueue_len() == 1, "exactly one enqueue")
    expect(evF.enqueued_gen(0) == 2, "enqueue at the fresh generation")

    # ------------------------------------------------------------------
    # 8. A0-T12 duplicate set() INSIDE one park epoch: three deliveries at
    #    COMMIT -> one accepted edge, enqueue-log length 1.
    # ------------------------------------------------------------------
    var tcbD = TaskControlBlock[UnitResult]()
    var tpD = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbD)
    var fpD_ = make_cancel_flag()
    var fpD = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpD_)
    make_tcb_running(tpD)
    var evD = Event()
    var dd = make_hook_script()
    script_add(dd, HookPoint.COMMIT, HookAction.WAKE)
    script_add(dd, HookPoint.COMMIT, HookAction.SET_READY)
    script_add(dd, HookPoint.COMMIT, HookAction.WAKE)
    var repD = run_cycle(evD, tpD, fpD, dd)
    expect(evD.set_attempts() == 3, "all three deliveries counted")
    expect(evD.wakes_accepted() == 1, "guard accepted ONLY the first edge")
    expect(evD.enqueue_len() == 1, "enqueue-log length 1 (A0-T12)")
    expect(repD.winners.exactly_one_winner(), "exactly one winner")
    expect(tpD[].state() == TaskControlBlock.RUNNABLE, "resumed once")

    # ------------------------------------------------------------------
    # 9. Generation discipline across cycles: each park cycle bumps the
    #    generation; a STALE-generation wake is ignored.
    # ------------------------------------------------------------------
    var tcbG = TaskControlBlock[UnitResult]()
    var tpG = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbG)
    var fpG_ = make_cancel_flag()
    var fpG = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpG_)

    # Cycle 1: park (undecided), wake by hand at gen 2.
    make_tcb_running(tpG)
    var evG = Event()
    var r1 = run_cycle(evG, tpG, fpG, make_hook_script())
    expect(r1.outcome == ParkOutcome.UNDECIDED, "cycle 1 parked")
    expect(tpG[].generation() == 2, "cycle 1: gen 1 -> 2")
    _ = deliver_wake(evG, tpG)
    expect(evG.wakes_accepted() == 1, "cycle 1 wake accepted")

    # Cycle 2: resume -> park again; generation bumps 2 -> 3.
    tpG[].transition(TaskControlBlock.RUNNING)
    var r2 = run_cycle(evG, tpG, fpG, make_hook_script())
    expect(r2.outcome == ParkOutcome.UNDECIDED, "cycle 2 parked")
    expect(tpG[].generation() == 3, "cycle 2: gen 2 -> 3")
    expect(
        tpG[].wait_node()[].generation() == 3,
        "node re-stamped with the fresh generation",
    )

    # STALE wake: targeting cycle 1's generation is a rejected no-op.
    var stale = evG.set_at(2)
    expect(not stale, "stale-generation wake ignored")
    expect(evG.set_attempts() >= 2, "stale delivery counted as attempt")
    expect(evG.wakes_accepted() == 1, "stale wake NOT accepted")
    expect(evG.enqueue_len() == 1, "stale wake enqueued nothing")

    # Current-generation wake still works; second becomes a duplicate.
    var cur = deliver_wake(evG, tpG)
    expect(cur, "current-generation wake accepted")
    expect(tpG[].state() == TaskControlBlock.RUNNABLE, "resumed at gen 3")
    expect(evG.enqueue_len() == 2, "second enqueue logged (new epoch)")
    expect(evG.enqueued_gen(1) == 3, "logged at generation 3")
    var dup2 = evG.set_at(3)
    expect(not dup2, "same-generation re-set rejected")
    expect(evG.enqueue_len() == 2, "still two enqueues total")

    # ------------------------------------------------------------------
    # 10. A0-T10 cancel-vs-readiness: BOTH fire in one schedule ->
    #     cancellation precedence (settle policy), EXACTLY ONE winner,
    #     deterministic across repeated identical schedules.
    # ------------------------------------------------------------------
    # Variant A: both at PREPARE (before any park).
    var cvp = make_hook_script()
    script_add(cvp, HookPoint.PREPARE, HookAction.REQUEST_CANCEL)
    script_add(cvp, HookPoint.PREPARE, HookAction.SET_READY)
    var tA = TaskControlBlock[UnitResult]()
    var fA = make_cancel_flag()
    var eA = Event()
    var tpA = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tA)
    var fpA = UnsafePointer[CancelFlag, MutAnyOrigin](to=fA)
    make_tcb_running(tpA)
    var ra1 = run_cycle(eA, tpA, fpA, cvp)

    var tB = TaskControlBlock[UnitResult]()
    var fB = make_cancel_flag()
    var eB = Event()
    var tpB2 = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tB)
    var fpB2 = UnsafePointer[CancelFlag, MutAnyOrigin](to=fB)
    make_tcb_running(tpB2)
    var rb1 = run_cycle(eB, tpB2, fpB2, cvp)
    expect(ra1.outcome == ParkOutcome.CANCELLED, "race at PREPARE: CANCEL")
    expect(ra1.decided_at == HookPoint.PREPARE, "decided at PREPARE")
    expect(ra1.winners.winner() == WinnerRecord.CANCEL, "CANCEL wins")
    expect(ra1.winners.exactly_one_winner(), "exactly one winner")
    expect(ra1.winners.attempts(WinnerRecord.CANCEL) == 1, "cancel fired once")
    expect(ra1.winners.attempts(WinnerRecord.READY) == 1, "ready fired too...")
    expect(rb1.winners.winner() == WinnerRecord.CANCEL, "deterministic repeat")
    expect(not ra1.slept(), "decided before any sleep")

    # Variant B: both at COMMIT (fully parked when they fire).
    var cvc = make_hook_script()
    script_add(cvc, HookPoint.COMMIT, HookAction.REQUEST_CANCEL)
    script_add(cvc, HookPoint.COMMIT, HookAction.SET_READY)
    var tcbH = TaskControlBlock[UnitResult]()
    var tpH = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbH)
    var fpH_ = make_cancel_flag()
    var fpH = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpH_)
    make_tcb_running(tpH)
    var evH = Event()
    var rc = run_cycle(evH, tpH, fpH, cvc)
    expect(rc.outcome == ParkOutcome.CANCELLED, "race at COMMIT: CANCEL")
    expect(rc.winners.winner() == WinnerRecord.CANCEL, "CANCEL wins parked")
    expect(rc.winners.exactly_one_winner(), "exactly one winner parked")
    expect(rc.slept(), "was parked when the race fired")
    expect(
        evH.enqueue_len() == 1,
        "losing READY physically delivered once (join re-checks cancel)",
    )
    # Integration seam (documented): the losing READY delivery resumed the
    # task; the cancellation checkpoint precedence is enforced by settle()
    # on the WINNER ledger, and the cooperative-cancel fold (A0.8) owns the
    # CancellationError surface on resume.

    # ------------------------------------------------------------------
    # 11. Cancel alone during the pipeline: precedence without a race.
    # ------------------------------------------------------------------
    var tcbK = TaskControlBlock[UnitResult]()
    var tpK = UnsafePointer[TaskControlBlock[UnitResult], MutAnyOrigin](to=tcbK)
    var fpK_ = make_cancel_flag()
    var fpK = UnsafePointer[CancelFlag, MutAnyOrigin](to=fpK_)
    make_tcb_running(tpK)
    var evK = Event()
    var kw = script_one(HookPoint.VALIDATE, HookAction.REQUEST_CANCEL)
    var rk = run_cycle(evK, tpK, fpK, kw)
    expect(rk.outcome == ParkOutcome.CANCELLED, "cancel-only: CANCELLED")
    expect(rk.decided_at == HookPoint.VALIDATE, "decided at VALIDATE")
    expect(not rk.slept(), "cancelled before sleeping")
    expect(tpK[].state() == TaskControlBlock.RUNNING, "still RUNNING")
    expect(evK.enqueue_len() == 0, "no enqueue on the cancel path")
    expect(fpK[].is_requested(), "flag visibly requested")

    print("T10 event park/wake protocol: PASS")
