# mojito_async/test/unit/t38_barrier.mojo
#
# A4.5 (issue #59) — task-aware Barrier acceptance, layered over
# sync/barrier.mojo's reuse of the Condvar FIFO/winner-claim mechanics
# (spec Phase A5).
#
# Acceptance:
#   A. N-phase symmetric release: 3 tasks rendezvous at a 3-target barrier;
#      the Nth arrival releases every waiter EXACTLY ONCE; a SECOND phase
#      (same 3 tasks) again requires all 3 arrivals (no stale target/count).
#   B. Single-phase cancel: cancelling one PARKED waiter shrinks the working
#      target in lockstep with the count, so the remaining waiters still
#      pass — no partial release, no duplicate release, and the barrier
#      resets cleanly for the next phase (target restored to base_target).
#   C. Mixed cancel/timeout/success in one phase: a cancelled waiter and a
#      timed-out waiter both leave the FIFO cleanly while the surviving
#      waiters (plus the triggering arrival) still release together.
#
# Verdict: exit 0 + "PASS"; any RED prints and raises (exit 1).
from std.memory import stack_allocation
from mojito_async.cancellation import is_cancellation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import Barrier, WINNER_CANCELLED, WINNER_READY, WINNER_TIMEOUT
from mojito_async.sync.condvar import PHASE_INIT
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T38 barrier: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# ---------------------------------------------------------------------------
# Scenario A — N-phase symmetric release: 3 waiters, two consecutive phases
# ---------------------------------------------------------------------------

struct SceneA(ImplicitlyCopyable, ImplicitlyDeletable):
    var br: UnsafePointer[Barrier, MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var causes0: UnsafePointer[Int, MutAnyOrigin]
    var passed_p1: UnsafePointer[Int, MutAnyOrigin]
    var passed_p2: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.br = UnsafePointer[Barrier, MutAnyOrigin](unsafe_from_address=1)
        self.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.causes0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.passed_p1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.passed_p2 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


# Per-waiter phase codes (driver-owned dispatch phase, distinct from the
# Barrier's own `cause` cell): 0 = arriving phase 1, 1 = parked in phase 1,
# 2 = arriving phase 2, 3 = parked in phase 2, 4 = done.
def a_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneA]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if h.id() == sc[].ids0[k]:
            who = k
    if who == -1:
        red("unknown waiter id in scenario A")
    var ph = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].phs0) + who * 8
    )
    var cause = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].causes0) + who * 8
    )
    claim_running(h)
    while True:
        if ph[] == 0 or ph[] == 2:
            var done = sc[].br[].wait[IntResult](rt, h, cause)
            if not done:
                ph[] = ph[] + 1
                return 1
            if cause[] != WINNER_READY:
                red("fast-path (Nth arriver) release must be WINNER_READY")
            if ph[] == 0:
                sc[].passed_p1[who] = 1
                cause[] = PHASE_INIT
                ph[] = 2
                continue  # attempt this task's OWN phase-2 arrival too
            sc[].passed_p2[who] = 1
            ph[] = 4
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
        if ph[] == 1 or ph[] == 3:
            var done = sc[].br[].wait[IntResult](rt, h, cause)
            if not done:
                return 1
            if cause[] != WINNER_READY:
                red("released waiter must observe WINNER_READY")
            if ph[] == 1:
                sc[].passed_p1[who] = 1
                cause[] = PHASE_INIT
                ph[] = 2
                continue  # attempt this task's OWN phase-2 arrival too
            sc[].passed_p2[who] = 1
            ph[] = 4
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
        return 1


def scenario_a() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var br = Barrier(3)
    var sc = SceneA()
    sc.br = UnsafePointer[Barrier, MutAnyOrigin](to=br)
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.causes0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.passed_p1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    sc.passed_p2 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16 * 8)
    for zi in range(24):
        buf[zi] = 0
    var scp = UnsafePointer[SceneA, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # Reverse-spawn (LIFO local deque): the deque serves w0, w1, w2 in that
    # order, so w0/w1 arrive+park and w2 is the Nth (releasing) arrival.
    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    # Both phases actually settle within this ONE scheduler_loop() call:
    # each waiter's dispatch chains straight from a settled phase-1 arrival
    # into its own phase-2 arrival (see a_dispatch's `continue`), and the
    # scheduler drains the resulting remote-ready wakes in the same drive.
    # This is itself part of the acceptance: the Nth arrival releases every
    # OTHER waiter of that phase exactly once (no partial/duplicate
    # release), and phase 2 only completes once because ALL THREE tasks
    # made a FRESH arrival against the reset target — verified below via
    # the phase counter and the per-phase pass flags.
    var served = scheduler_loop(rt, a_dispatch, ud)
    if served != 7:
        red("expected 7 dispatch slices (3x phase-1 arrivals + 3x phase-2 "
            "arrivals + w2's synchronous phase-1->phase-2 chain), got "
            + String(served))
    if not (h_w0.is_completed() and h_w1.is_completed() and h_w2.is_completed()):
        red("not all three waiters completed both phases")
    if br.waiter_count() != 0:
        red("barrier FIFO not drained")
    if br.target() != 3:
        red("target must reset to base_target after each full release")
    if br.phase() != 2:
        red("phase counter must be 2 after two completed releases, got "
            + String(br.phase()))
    if buf[12] != 1 or buf[13] != 1 or buf[14] != 1:
        red("not all three waiters passed phase 1")
    if buf[16] != 1 or buf[17] != 1 or buf[18] != 1:
        red("not all three waiters passed phase 2 — a later phase must "
            "require everyone again")
    print("T38 barrier scenario A (N-phase symmetric release): PASS")


# ---------------------------------------------------------------------------
# Scenario B — single-phase cancel: a cancelled waiter leaves the barrier
# healthy; the surviving waiters still pass.
# ---------------------------------------------------------------------------

struct SceneBC(ImplicitlyCopyable, ImplicitlyDeletable):
    var br: UnsafePointer[Barrier, MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var causes0: UnsafePointer[Int, MutAnyOrigin]
    var passed0: UnsafePointer[Int, MutAnyOrigin]
    var cancelled0: UnsafePointer[Int, MutAnyOrigin]
    var n: Int

    def __init__(out self):
        self.br = UnsafePointer[Barrier, MutAnyOrigin](unsafe_from_address=1)
        self.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.causes0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.passed0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cancelled0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = 0


def bc_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneBC]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(sc[].n):
        if h.id() == sc[].ids0[k]:
            who = k
    if who == -1:
        red("unknown waiter id in scenario B/C")
    var ph = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].phs0) + who * 8
    )
    var cause = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].causes0) + who * 8
    )
    if ph[] == 0:
        claim_running(h)
        var done = sc[].br[].wait[IntResult](rt, h, cause)
        if done:
            if cause[] != WINNER_READY:
                red("fast-path (Nth arriver) release must be WINNER_READY")
            sc[].passed0[who] = 1
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
        ph[] = 1
        return 1
    claim_running(h)
    try:
        var done = sc[].br[].wait[IntResult](rt, h, cause)
        if not done:
            return 1
        if cause[] == WINNER_READY:
            sc[].passed0[who] = 1
        elif cause[] == WINNER_TIMEOUT:
            sc[].passed0[who] = 1
        else:
            red("wait() settled with an unexpected cause: " + String(cause[]))
    except e:
        if not is_cancellation(e):
            red("cancelled waiter raised the wrong error: " + String(e))
        sc[].cancelled0[who] = 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_b() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var br = Barrier(4)
    var sc = SceneBC()
    sc.br = UnsafePointer[Barrier, MutAnyOrigin](to=br)
    sc.n = 3
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.causes0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.passed0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    sc.cancelled0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16 * 8)
    for zi in range(24):
        buf[zi] = 0
    var scp = UnsafePointer[SceneBC, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # Reverse-spawn: deque serves w0, w1, w2 — all THREE arrive+park (target
    # is 4, so nobody releases yet).
    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    # Cancel the MIDDLE arriver (w1) while w0/w2 stay parked: this shrinks
    # the working target 4 -> 3 in lockstep with the count (3 -> 2), so the
    # "remaining arrivals needed" (target - count) stays 1 both before and
    # after — a fresh 4th arrival (w3, spawned below) then trips the
    # (now-shrunk) target and releases w0/w2 + itself.
    var served = scheduler_loop(rt, bc_dispatch, ud)
    if served != 3:
        red("expected all 3 waiters to arrive+park: served " + String(served))
    if br.waiter_count() != 3:
        red("expected 3 parked waiters before any cancel: " + String(br.waiter_count()))

    var cancelled = br.cancel_waiter[IntResult](rt, h_w1)
    if not cancelled:
        red("cancel_waiter did not find the parked waiter")
    if br.waiter_count() != 2:
        red("cancel_waiter must remove ONLY the cancelled waiter")
    if br.target() != 3:
        red("cancelling a parked waiter must shrink the working target in lockstep")

    _ = scheduler_loop(rt, bc_dispatch, ud)
    if not h_w1.is_completed():
        red("cancelled waiter did not settle")
    if buf[17] != 1:
        red("cancelled waiter did not raise CancellationError")
    # w0 and w2 remain PARKED — the shrunk target (3) was already met by
    # their own two earlier arrivals (both counted before the cancel), so
    # cancel_waiter's release should NOT itself trip a barrier release; a
    # release only ever happens from INSIDE wait() on a NEW arrival.  Since
    # both real parties already arrived, verify they stay parked (no
    # phantom release) — this matches the design: cancel decrements count
    # AND target together, so 2 == 2 would look "already met", but the
    # check only runs on `wait()`'s OWN new arrival, never asynchronously
    # from cancel_waiter.  Confirm that invariant here:
    if h_w0.state() != TaskControlBlock.WAITING or h_w2.state() != TaskControlBlock.WAITING:
        red("cancel_waiter must never itself trigger a release")
    if br.waiter_count() != 2:
        red("w0/w2 must still be parked after the cancel")

    # Only a genuinely NEW arrival can trip the (now-shrunk) target.  Spawn
    # one more party to supply that arrival.
    var tcb_w3 = TB.create()
    var h_w3 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w3), 0)
    buf[3] = h_w3.id()
    sc.n = 4
    for _ in range(4):
        if not (h_w0.is_completed() and h_w2.is_completed() and h_w3.is_completed()):
            _ = scheduler_loop(rt, bc_dispatch, ud)

    if not (h_w0.is_completed() and h_w2.is_completed() and h_w3.is_completed()):
        red("surviving waiters + the new arrival did not all settle")
    if buf[12] != 1 or buf[14] != 1 or buf[15] != 1:
        red("surviving waiters (w0, w2) and the new arrival (w3) must all pass")
    if br.waiter_count() != 0:
        red("barrier FIFO not drained after the shrunk-target release")
    if br.target() != 4:
        red("target must reset to base_target(4) after the release")
    if br.phase() != 1:
        red("phase counter did not advance")
    print("T38 barrier scenario B (single-phase cancel leaves the barrier healthy): PASS")


# ---------------------------------------------------------------------------
# Scenario C — mixed cancel + timeout + success in ONE phase.
# ---------------------------------------------------------------------------

def scenario_c() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var br = Barrier(4)
    var sc = SceneBC()
    sc.br = UnsafePointer[Barrier, MutAnyOrigin](to=br)
    sc.n = 3
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.causes0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.passed0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    sc.cancelled0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16 * 8)
    for zi in range(24):
        buf[zi] = 0
    var scp = UnsafePointer[SceneBC, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # 3 waiters arrive+park against a target-4 barrier (nobody releases yet).
    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    var served = scheduler_loop(rt, bc_dispatch, ud)
    if served != 3:
        red("expected all 3 waiters to arrive+park: served " + String(served))
    if br.waiter_count() != 3 or br.target() != 4:
        red("expected 3 parked waiters against target 4")

    # Cancel w1, then time out w2: target shrinks 4 -> 3 -> 2 in lockstep
    # with count, leaving only w0 parked and target=2 (one more real
    # arrival needed — matches "remaining needed" staying invariant: before
    # any removal, 3 arrived / target 4 => 1 remaining; after both removals,
    # 1 arrived (w0) / target 2 => 1 remaining, unchanged).
    if not br.cancel_waiter[IntResult](rt, h_w1):
        red("cancel_waiter did not find w1")
    if not br.timeout_waiter[IntResult](rt, h_w2):
        red("timeout_waiter did not find w2")
    if br.target() != 2:
        red("target should have shrunk to 2 after two removals: " + String(br.target()))
    if br.waiter_count() != 1:
        red("only w0 should remain parked: " + String(br.waiter_count()))

    _ = scheduler_loop(rt, bc_dispatch, ud)
    if not h_w1.is_completed() or buf[17] != 1:
        red("w1 (cancelled) did not raise+settle")
    if not h_w2.is_completed() or buf[14] != 1:
        red("w2 (timed out) did not settle with WINNER_TIMEOUT")
    if h_w0.state() != TaskControlBlock.WAITING:
        red("w0 must still be parked (the shrunk target(2) needs ONE more arrival)")

    # One more real arrival trips the shrunk target (2): w0's own earlier
    # arrival (count=1) + this new one (count=2) == target(2).
    var tcb_w3 = TB.create()
    var h_w3 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w3), 0)
    buf[3] = h_w3.id()
    sc.n = 4
    for _ in range(4):
        if not (h_w0.is_completed() and h_w3.is_completed()):
            _ = scheduler_loop(rt, bc_dispatch, ud)

    if not (h_w0.is_completed() and h_w3.is_completed()):
        red("w0 and the new arrival w3 did not settle")
    if buf[12] != 1 or buf[15] != 1:
        red("w0 and w3 must both pass with WINNER_READY")
    if br.waiter_count() != 0:
        red("barrier FIFO not drained")
    if br.target() != 4:
        red("target must reset to base_target(4) after the release")
    if br.phase() != 1:
        red("phase counter did not advance")
    print("T38 barrier scenario C (mixed cancel/timeout/success in one phase): PASS")


def main() raises:
    scenario_a()
    scenario_b()
    scenario_c()
