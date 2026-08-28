# mojito_async/test/unit/t21_mutex.mojo
#
# A1.2 (issue #34) — task-aware Mutex[T] acceptance over the A1.1
# single-worker cooperative scheduler.
#
# Acceptance:
#   1. Fast path: an uncontended mutex acquires with NO waiter and NO park.
#   2. Slow path (park + handoff): a holder keeps the lock while parked; a
#      waiter parks on the contended lock; resuming+unlocking the holder
#      hands the lock FIFO to that one waiter (no thundering herd).
#   3. FIFO fairness: three waiters are granted strictly in arrival order,
#      one wake each, no lost wakeup, no duplicate grant.
#
# Verdict: exit 0 + "PASS"; any RED prints and raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import park_current, unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import Mutex
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T21 mutex: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

comptime E_LOCKED = Int(1)
comptime E_PARK = Int(2)
comptime E_UNLOCK = Int(3)
comptime E_GRANTED = Int(4)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# ---------------------------------------------------------------------------
# Scenario A — holder parks while holding; one waiter parks; unlock hands off
# ---------------------------------------------------------------------------

struct SceneA(ImplicitlyCopyable, ImplicitlyDeletable):
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var seq0: UnsafePointer[Int, MutAnyOrigin]
    var seqN: UnsafePointer[Int, MutAnyOrigin]
    var id_hold: UnsafePointer[Int, MutAnyOrigin]
    var id_wait: UnsafePointer[Int, MutAnyOrigin]
    var ph_hold: UnsafePointer[Int, MutAnyOrigin]
    var ph_wait: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.seq0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.seqN = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_hold = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_wait = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_hold = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_wait = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def a_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneA]()
    var h = _handle(tcb_addr, tid)
    if h.id() == sc[].id_hold[]:
        if sc[].ph_hold[] == 0:
            claim_running(h)
            var got = sc[].mtx[].lock(rt, h)
            if not got:
                red("holder fast lock failed")
            sc[].mtx[].value()[0] = 41
            var i = sc[].seqN[]
            sc[].seq0[i] = E_LOCKED
            sc[].seqN[] = i + 1
            sc[].ph_hold[] = 1
            park_current(rt, h)
            return 1
        else:
            claim_running(h)
            var handed = sc[].mtx[].unlock[IntResult](rt)
            if not handed:
                red("holder unlock did not hand off")
            var i = sc[].seqN[]
            sc[].seq0[i] = E_UNLOCK
            sc[].seqN[] = i + 1
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
    if h.id() == sc[].id_wait[]:
        if sc[].ph_wait[] == 0:
            claim_running(h)
            var got = sc[].mtx[].lock(rt, h)
            if got:
                red("waiter must not acquire while holder holds")
            var i = sc[].seqN[]
            sc[].seq0[i] = E_PARK
            sc[].seqN[] = i + 1
            sc[].ph_wait[] = 1
            return 1
        else:
            claim_running(h)
            var got = sc[].mtx[].lock(rt, h)
            if not got:
                red("granted waiter did not acquire")
            var i = sc[].seqN[]
            sc[].seq0[i] = E_GRANTED
            sc[].seqN[] = i + 1
            sc[].mtx[].value()[0] = 42
            var handed = sc[].mtx[].unlock[IntResult](rt)
            if handed:
                red("final unlock handed off with no waiter")
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
    red("unexpected task id in scenario A")
    return 0


def scenario_a() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var mtx = Mutex[Int](0)
    var sc = SceneA()
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.seqN = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.seq0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    sc.id_hold = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.id_wait = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 7 * 8)
    sc.ph_hold = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.ph_wait = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 9 * 8)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[SceneA, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # A2.2 (issue #68): owner pop is LIFO (spawn locality), so register the
    # waiter FIRST and the holder after -> the holder (A) runs, locks and
    # parks before the waiter (B) contends.
    var tcb_b = TB.create()
    var h_b = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    var tcb_a = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    buf[6] = h_a.id()
    buf[7] = h_b.id()

    _ = scheduler_loop(rt, a_dispatch, ud)
    if not mtx.is_locked():
        red("holder did not hold while parked")
    if mtx.waiter_count() != 1:
        red("expected one waiter queued")
    if h_b.state() != TaskControlBlock.WAITING:
        red("waiter did not park")

    unpark_current(rt, h_a)
    _ = scheduler_loop(rt, a_dispatch, ud)

    if not h_a.is_completed():
        red("holder did not complete")
    if not h_b.is_completed():
        red("waiter did not complete after handoff")
    if mtx.is_locked() or mtx.waiter_count() != 0:
        red("mutex not free & drained after handoff")
    if mtx.value()[0] != 42:
        red("protected value wrong")
    if rt.pending() != 0:
        red("queue not quiet after handoff")
    if buf[0] != 4:
        red("expected 4 events, got " + String(buf[0]))
    if buf[1] != E_LOCKED or buf[2] != E_PARK or buf[3] != E_UNLOCK or (
        buf[4] != E_GRANTED
    ):
        red("event order wrong in scenario A")
    print("T21 mutex scenario A (park+handoff): PASS")


# ---------------------------------------------------------------------------
# Scenario B — FIFO fairness among three waiters
# ---------------------------------------------------------------------------

struct SceneB(ImplicitlyCopyable, ImplicitlyDeletable):
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var grant0: UnsafePointer[Int, MutAnyOrigin]
    var npassed: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.grant0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.npassed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def b_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneB]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if h.id() == sc[].ids0[k]:
            who = k
    if who == -1:
        red("unknown waiter id in scenario B")
    var ph = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].phs0) + who * 8
    )
    if ph[] == 0:
        claim_running(h)
        var got = sc[].mtx[].lock[IntResult](rt, h)
        if got:
            red("waiter must not acquire while holder holds")
        ph[] = 1
        return 1
    # granted (handed off by an explicit unlock): claim the lock, record the
    # arrival order, and complete WITHOUT unlocking (main drives each step).
    claim_running(h)
    var got = sc[].mtx[].lock[IntResult](rt, h)
    if not got:
        red("granted waiter did not acquire (handoff broken) who=" + String(who))
    sc[].grant0[who] = sc[].npassed[]
    sc[].npassed[] = sc[].npassed[] + 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_b() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var mtx = Mutex[Int](0)
    # Simulated holder: the lock is acquired directly before waiters spawn.
    if not mtx.try_lock():
        red("pre-acquire failed")
    var sc = SceneB()
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.grant0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.npassed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    for zi in range(24):
        buf[zi] = 0
    var scp = UnsafePointer[SceneB, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # A2.2 (issue #68): owner pop is LIFO, so register the waiters in
    # REVERSE -> the deque serves w0, w1, w2, which park in that order and
    # are granted strictly FIFO by the handoff chain.
    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    # All three waiters contend and park in spawn order; holder holds.
    _ = scheduler_loop(rt, b_dispatch, ud)
    if mtx.waiter_count() != 3:
        red("expected 3 waiters queued: " + String(mtx.waiter_count()))
    if buf[12] != 0:
        red("no waiter should pass while the lock is held")

    # Drive the handoff chain step by step: each unlock hands to the FIFO
    # head, the driven waiter claims it and completes; the lock stays held
    # across each handoff window.
    for k in range(3):
        var handed = mtx.unlock[IntResult](rt)
        if not handed:
            red("handoff " + String(k) + " failed")
        _ = scheduler_loop(rt, b_dispatch, ud)
    # release the still-held lock (no waiters left) to reach UNLOCKED.
    var last = mtx.unlock[IntResult](rt)
    if last:
        red("final release must not hand off")

    if mtx.is_locked() or mtx.waiter_count() != 0:
        red("waiters not drained after handoff chain")
    if buf[12] != 3:
        red("not all three waiters passed: " + String(buf[12]))
    if buf[8] != 0 or buf[9] != 1 or buf[10] != 2:
        red("grant order not FIFO (expected 0,1,2)")
    if not (h_w0.is_completed() and h_w1.is_completed() and h_w2.is_completed()):
        red("not all waiters completed")
    print("T21 mutex scenario B (FIFO fairness): PASS")


def main() raises:
    scenario_a()
