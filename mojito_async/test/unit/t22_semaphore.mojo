# mojito_async/test/unit/t22_semaphore.mojo
#
# A1.2 (issue #34) — task-aware Semaphore + Permit acceptance over the A1.1
# single-worker cooperative scheduler (spec §36).
#
# Acceptance:
#   1. Fast path: try_acquire/acquire succeed immediately when permits are
#      available and no waiter is queued (1 and N permits, no park).
#   2. Slow path (park + FIFO grant): with 1 permit, a second acquirer parks;
#      release hands the permit to the head waiter; Permit RAII returns it.
#   3. FIFO fairness: 3 waiters for a 1-permit semaphore are granted strictly
#      in arrival order, one wake each (no lost wakeup, no duplicate).
#   4. Batch semantics: acquire(n>1) parks until n permits accumulate; a
#      release(n) can satisfy the head directly.
#
# Verdict: exit 0 + "PASS"; any RED prints and raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import park_current, unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import Permit, Semaphore
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T22 semaphore: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# ---------------------------------------------------------------------------
# Scenario A — 1-permit semaphore: fast path, park, release, Permit RAII
# ---------------------------------------------------------------------------

struct SceneA(ImplicitlyCopyable, ImplicitlyDeletable):
    var sem: UnsafePointer[Semaphore, MutAnyOrigin]
    var seq0: UnsafePointer[Int, MutAnyOrigin]
    var seqN: UnsafePointer[Int, MutAnyOrigin]
    var id_a: UnsafePointer[Int, MutAnyOrigin]
    var id_b: UnsafePointer[Int, MutAnyOrigin]
    var ph_a: UnsafePointer[Int, MutAnyOrigin]
    var ph_b: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.sem = UnsafePointer[Semaphore, MutAnyOrigin](unsafe_from_address=1)
        self.seq0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.seqN = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_a = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_b = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_a = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_b = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


comptime S_TAKE = Int(1)
comptime S_PARK = Int(2)
comptime S_REL = Int(3)
comptime S_GRANT = Int(4)


def a_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneA]()
    var h = _handle(tcb_addr, tid)
    if h.id() == sc[].id_a[]:
        if sc[].ph_a[] == 0:
            claim_running(h)
            var got = sc[].sem[].acquire[IntResult](rt, h)
            if not got:
                red("first acquire must take the fast path")
            var i = sc[].seqN[]
            sc[].seq0[i] = S_TAKE
            sc[].seqN[] = i + 1
            # hold the permit while parked (simulated holder)
            sc[].ph_a[] = 1
            park_current(rt, h)
            return 1
        else:
            claim_running(h)
            # return the permit via a Permit handle (RAII)
            var p = Permit(sc[].sem, 1)
            _ = p.release[IntResult](rt)
            var i = sc[].seqN[]
            sc[].seq0[i] = S_REL
            sc[].seqN[] = i + 1
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
    if h.id() == sc[].id_b[]:
        if sc[].ph_b[] == 0:
            claim_running(h)
            var got = sc[].sem[].acquire[IntResult](rt, h)
            if got:
                red("second acquire must park (no permit left)")
            var i = sc[].seqN[]
            sc[].seq0[i] = S_PARK
            sc[].seqN[] = i + 1
            sc[].ph_b[] = 1
            return 1
        else:
            claim_running(h)
            var got = sc[].sem[].acquire[IntResult](rt, h)
            if not got:
                red("granted acquirer did not get the permit")
            var i = sc[].seqN[]
            sc[].seq0[i] = S_GRANT
            sc[].seqN[] = i + 1
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
    red("unexpected task id in scenario A")
    return 0


def scenario_a() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var sem = Semaphore(1)
    var sc = SceneA()
    sc.sem = UnsafePointer[Semaphore, MutAnyOrigin](to=sem)
    sc.seqN = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.seq0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    sc.id_a = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.id_b = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 7 * 8)
    sc.ph_a = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.ph_b = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 9 * 8)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[SceneA, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # A2.2 (issue #68): owner pop is LIFO (spawn locality), so register the
    # parker FIRST and the taker after -> the taker (A) acquires and holds
    # the permit before the parker (B) contends.
    var tcb_b = TB.create()
    var h_b = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    var tcb_a = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    buf[6] = h_a.id()
    buf[7] = h_b.id()

    _ = scheduler_loop(rt, a_dispatch, ud)
    if sem.available() != 0:
        red("permit not consumed by A")
    if sem.waiter_count() != 1:
        red("expected one semaphore waiter")
    if h_b.state() != TaskControlBlock.WAITING:
        red("B did not park on the exhausted semaphore")

    unpark_current(rt, h_a)
    _ = scheduler_loop(rt, a_dispatch, ud)

    if not h_a.is_completed():
        red("A did not complete")
    if not h_b.is_completed():
        red("B did not complete after grant")
    if sem.available() != 0:
        red("permit accounting wrong after grant/release")
    if sem.waiter_count() != 0:
        red("waiter queue not drained")
    if buf[0] != 4:
        red("expected 4 events, got " + String(buf[0]))
    if buf[1] != S_TAKE or buf[2] != S_PARK or buf[3] != S_REL or buf[4] != S_GRANT:
        red("event order wrong in scenario A")
    print("T22 semaphore scenario A (fast/park/Permit): PASS")

# ---------------------------------------------------------------------------
# Scenario B — FIFO fairness: 1 permit, three waiters granted in arrival order
# ---------------------------------------------------------------------------

struct SceneB(ImplicitlyCopyable, ImplicitlyDeletable):
    var sem: UnsafePointer[Semaphore, MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var grant0: UnsafePointer[Int, MutAnyOrigin]
    var npassed: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.sem = UnsafePointer[Semaphore, MutAnyOrigin](unsafe_from_address=1)
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
        var got = sc[].sem[].acquire[IntResult](rt, h)
        if got:
            red("waiter must park when the permit is held")
        ph[] = 1
        return 1
    # granted: re-enter acquire (claims via GRANT marker), record order.
    claim_running(h)
    var got = sc[].sem[].acquire[IntResult](rt, h)
    if not got:
        red("granted acquirer did not get the permit (who=" + String(who) + ")")
    sc[].grant0[who] = sc[].npassed[]
    sc[].npassed[] = sc[].npassed[] + 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_b() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var sem = Semaphore(1)
    # simulated holder: consume the single permit before waiters spawn
    _ = sem.try_acquire()
    var sc = SceneB()
    sc.sem = UnsafePointer[Semaphore, MutAnyOrigin](to=sem)
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
    # are granted strictly FIFO.
    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    _ = scheduler_loop(rt, b_dispatch, ud)
    if sem.waiter_count() != 3:
        red("expected 3 semaphore waiters: " + String(sem.waiter_count()))
    if buf[12] != 0:
        red("no waiter should pass while the permit is held")

    # Drive the grant chain step by step: each release grants the FIFO head.
    for k in range(3):
        var handed = sem.release[IntResult](rt)
        if not handed:
            red("release " + String(k) + " did not hand off")
        _ = scheduler_loop(rt, b_dispatch, ud)

    if sem.available() != 0 or sem.waiter_count() != 0:
        red("permit accounting wrong after grant chain")
    if buf[12] != 3:
        red("not all three waiters passed: " + String(buf[12]))
    if buf[8] != 0 or buf[9] != 1 or buf[10] != 2:
        red("grant order not FIFO (expected 0,1,2)")
    if not (h_w0.is_completed() and h_w1.is_completed() and h_w2.is_completed()):
        red("not all waiters completed")
    print("T22 semaphore scenario B (FIFO fairness): PASS")


# ---------------------------------------------------------------------------
# Scenario C — batch: acquire(2) parks until 2 permits accumulate; try_acquire
# refuses under N; a release(2) satisfies the head directly.
# ---------------------------------------------------------------------------

struct SceneC(ImplicitlyCopyable, ImplicitlyDeletable):
    var sem: UnsafePointer[Semaphore, MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var done: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.sem = UnsafePointer[Semaphore, MutAnyOrigin](unsafe_from_address=1)
        self.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.done = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def c_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneC]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if h.id() == sc[].ids0[k]:
            who = k
    var ph = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].phs0) + who * 8
    )
    if ph[] == 0:
        claim_running(h)
        var got = sc[].sem[].acquire[IntResult](rt, h, 2)
        if got:
            red("acquire(2) must park with <2 permits")
        ph[] = 1
        return 1
    claim_running(h)
    var got = sc[].sem[].acquire[IntResult](rt, h, 2)
    if not got:
        red("batch grant lost (who=" + String(who) + ")")
    sc[].done[] = sc[].done[] + 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_c() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var sem = Semaphore(0)
    if sem.try_acquire(2):
        red("try_acquire(2) must fail on empty semaphore")
    if sem.try_acquire(1):
        red("try_acquire(1) must fail on empty semaphore")
    _ = sem.release[IntResult](rt)  # available = 1
    if sem.try_acquire(2):
        red("try_acquire(2) must fail with only 1 permit free")
    _ = sem.release[IntResult](rt)  # available = 2
    if not sem.try_acquire(2):
        red("try_acquire(2) must succeed with 2 permits free")
    _ = sem.release[IntResult](rt, 2)  # available = 2 again
    # drain back to 0 so the batch waiter (need 2) must park
    if not sem.try_acquire(2):
        red("drain acquire failed")
    if sem.available() != 0:
        red("setup left wrong permit count")

    var sc = SceneC()
    sc.sem = UnsafePointer[Semaphore, MutAnyOrigin](to=sem)
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.done = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[SceneC, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # one waiter asks for 2 permits; semaphore has exactly 1 -> parks
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    _ = scheduler_loop(rt, c_dispatch, ud)
    if sem.waiter_count() != 1:
        red("batch waiter did not park")
    # release(1) leaves need=2 > 1: head still unsatisfied, queue preserved
    var handed = sem.release[IntResult](rt)
    if handed:
        red("release(1) must not grant acquire(2)")
    if sem.waiter_count() != 1:
        red("unsatisfied head must stay queued (FIFO)")
    # release(1) now makes 2 available: the head waiter is granted
    handed = sem.release[IntResult](rt, 1)
    if not handed:
        red("second release(1) must grant the batch waiter")
    _ = scheduler_loop(rt, c_dispatch, ud)
    if buf[8] != 1:
        red("batch waiter did not complete after grant")
    if sem.waiter_count() != 0 or sem.available() != 0:
        red("batch accounting wrong at end")
    print("T22 semaphore scenario C (batch acquire/release): PASS")


def main() raises:
    scenario_a()
    scenario_b()
    scenario_c()