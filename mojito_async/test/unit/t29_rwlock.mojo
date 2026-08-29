# mojito_async/test/unit/t29_rwlock.mojo
#
# A4.8 (issue #66) — task-aware RWLock[T] acceptance over the A1.1
# single-worker cooperative scheduler.
#
# Acceptance:
#   1. Reader coexistence: multiple readers hold concurrently; writer
#      preference blocks a NEW reader once a writer waits; the writer is
#      granted only once every already-holding reader has released.
#   2. Single-writer-at-a-time + FIFO fairness: three writers contend for
#      an exclusive lock and are granted strictly in arrival order, one
#      wake each (no thundering herd, no lost wakeup, no duplicate grant).
#   3. Cancellation: a queued (not yet granted) reader is pulled out of the
#      FIFO via cancel_read_wait — it must never receive a leaked grant,
#      and the remaining waiters are still granted correctly in order.
#   4. Mixed read/write storm: interleaved readers and writers under
#      contention never observe the mutual-exclusion invariant broken
#      (readers held while a writer holds), every task eventually
#      completes (no lost wakeup / no deadlock), and concurrent readers
#      really do overlap (peak concurrency >= 2).
#
# Verdict: exit 0 + "PASS"; any RED prints and raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import park_current, unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import RWLock
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T29 rwlock: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# ---------------------------------------------------------------------------
# Scenario A — reader coexistence, writer preference, drain-to-writer grant
# ---------------------------------------------------------------------------

struct SceneA(ImplicitlyCopyable, ImplicitlyDeletable):
    var rw: UnsafePointer[RWLock[Int], MutAnyOrigin]
    var id_w: UnsafePointer[Int, MutAnyOrigin]
    var ph_w: UnsafePointer[Int, MutAnyOrigin]
    var seq0: UnsafePointer[Int, MutAnyOrigin]
    var seqN: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.rw = UnsafePointer[RWLock[Int], MutAnyOrigin](unsafe_from_address=1)
        self.id_w = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_w = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.seq0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.seqN = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


comptime A_PARK = Int(1)
comptime A_GRANT = Int(2)


def a_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneA]()
    var h = _handle(tcb_addr, tid)
    if h.id() != sc[].id_w[]:
        red("unexpected task id in scenario A")
        return 0
    if sc[].ph_w[] == 0:
        claim_running(h)
        var got = sc[].rw[].write[IntResult](rt, h)
        if got:
            red("writer must not acquire while readers hold")
        var i = sc[].seqN[]
        sc[].seq0[i] = A_PARK
        sc[].seqN[] = i + 1
        sc[].ph_w[] = 1
        return 1
    claim_running(h)
    var got = sc[].rw[].write[IntResult](rt, h)
    if not got:
        red("granted writer did not acquire")
    var i = sc[].seqN[]
    sc[].seq0[i] = A_GRANT
    sc[].seqN[] = i + 1
    sc[].rw[].value()[0] = 99
    var handed = sc[].rw[].unlock_write[IntResult](rt)
    if handed != 0:
        red("final writer unlock must not hand off (no waiters left)")
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_a() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var rw = RWLock[Int](0)

    # two readers acquire uncontended and hold concurrently.
    if not rw.try_read():
        red("first reader fast path failed")
    if not rw.try_read():
        red("second reader fast path failed (readers must coexist)")
    if rw.reader_count() != 2:
        red("expected 2 concurrent readers, got " + String(rw.reader_count()))
    if rw.try_write():
        red("writer must not acquire while readers hold")

    var sc = SceneA()
    sc.rw = UnsafePointer[RWLock[Int], MutAnyOrigin](to=rw)
    sc.id_w = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.ph_w = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    sc.seqN = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 2 * 8)
    sc.seq0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[SceneA, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb_w = TB.create()
    var h_w = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w), 0)
    buf[0] = h_w.id()

    # writer contends while both readers hold: must park (writer-preference
    # setup) and must NOT jump the queue.
    _ = scheduler_loop(rt, a_dispatch, ud)
    if rw.writer_waiter_count() != 1:
        red("expected the writer queued")
    if h_w.state() != TaskControlBlock.WAITING:
        red("writer did not park")

    # a NEW reader must now be refused (writer waits -> writer preference).
    if rw.try_read():
        red("a new reader must not cut in front of a waiting writer")

    # first release: 2 -> 1, no grant yet.
    var handed0 = rw.unlock_read[IntResult](rt)
    if handed0:
        red("release with a remaining reader must not hand off")
    if rw.reader_count() != 1:
        red("reader count wrong after first release")
    if h_w.state() != TaskControlBlock.WAITING:
        red("writer granted too early")

    # second (last) release: 1 -> 0, writer granted.
    var handed1 = rw.unlock_read[IntResult](rt)
    if not handed1:
        red("last release must hand off to the waiting writer")
    _ = scheduler_loop(rt, a_dispatch, ud)

    if not h_w.is_completed():
        red("writer did not complete after grant")
    if rw.is_write_locked() or rw.reader_count() != 0:
        red("lock not free & drained after handoff")
    if rw.writer_waiter_count() != 0:
        red("writer queue not drained")
    if rw.value()[0] != 99:
        red("protected value wrong")
    if buf[2] != 2:
        red("expected 2 events, got " + String(buf[2]))
    if buf[3] != A_PARK or buf[4] != A_GRANT:
        red("event order wrong in scenario A")
    print("T29 rwlock scenario A (reader coexistence + writer preference): PASS")


# ---------------------------------------------------------------------------
# Scenario B — single-writer-at-a-time, FIFO fairness among three writers
# ---------------------------------------------------------------------------

struct SceneB(ImplicitlyCopyable, ImplicitlyDeletable):
    var rw: UnsafePointer[RWLock[Int], MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var grant0: UnsafePointer[Int, MutAnyOrigin]
    var npassed: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.rw = UnsafePointer[RWLock[Int], MutAnyOrigin](unsafe_from_address=1)
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
        var got = sc[].rw[].write[IntResult](rt, h)
        if got:
            red("writer must not acquire while another writer holds")
        ph[] = 1
        return 1
    # granted (handed off by an explicit unlock): claim it, record arrival
    # order, complete WITHOUT unlocking (main drives each step).
    claim_running(h)
    var got = sc[].rw[].write[IntResult](rt, h)
    if not got:
        red("granted writer did not acquire (handoff broken) who=" + String(who))
    sc[].grant0[who] = sc[].npassed[]
    sc[].npassed[] = sc[].npassed[] + 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_b() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var rw = RWLock[Int](0)
    # simulated holder: the lock is acquired directly before waiters spawn.
    if not rw.try_write():
        red("pre-acquire failed")
    var sc = SceneB()
    sc.rw = UnsafePointer[RWLock[Int], MutAnyOrigin](to=rw)
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
    if rw.writer_waiter_count() != 3:
        red("expected 3 waiters queued: " + String(rw.writer_waiter_count()))
    if buf[12] != 0:
        red("no waiter should pass while the lock is held")

    # Drive the handoff chain step by step: each unlock hands to the FIFO
    # head, the driven waiter claims it and completes; single-writer-at-a-
    # time holds across each handoff window (writer_locked never clears
    # between a pop and its grantee's claim).
    for k in range(3):
        var handed = rw.unlock_write[IntResult](rt)
        if handed != 1:
            red("handoff " + String(k) + " failed, expected 1 writer granted")
        if not rw.is_write_locked():
            red("lock must stay held across the handoff window")
        _ = scheduler_loop(rt, b_dispatch, ud)
    # release the still-held lock (no waiters left) to reach unlocked.
    var last = rw.unlock_write[IntResult](rt)
    if last != 0:
        red("final release must not hand off")

    if rw.is_write_locked() or rw.writer_waiter_count() != 0:
        red("waiters not drained after handoff chain")
    if buf[12] != 3:
        red("not all three waiters passed: " + String(buf[12]))
    if buf[8] != 0 or buf[9] != 1 or buf[10] != 2:
        red("grant order not FIFO (expected 0,1,2)")
    if not (h_w0.is_completed() and h_w1.is_completed() and h_w2.is_completed()):
        red("not all waiters completed")
    print("T29 rwlock scenario B (single-writer + FIFO fairness): PASS")


# ---------------------------------------------------------------------------
# Scenario C — cancel-one-reader: no leaked grant, remaining waiters correct
# ---------------------------------------------------------------------------

struct SceneC(ImplicitlyCopyable, ImplicitlyDeletable):
    var rw: UnsafePointer[RWLock[Int], MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var grant0: UnsafePointer[Int, MutAnyOrigin]
    var npassed: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.rw = UnsafePointer[RWLock[Int], MutAnyOrigin](unsafe_from_address=1)
        self.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.grant0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.npassed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def c_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneC]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if h.id() == sc[].ids0[k]:
            who = k
    if who == -1:
        red("unknown waiter id in scenario C")
    var ph = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].phs0) + who * 8
    )
    if ph[] == 0:
        claim_running(h)
        var got = sc[].rw[].read[IntResult](rt, h)
        if got:
            red("reader must not acquire while a writer holds")
        ph[] = 1
        return 1
    claim_running(h)
    var got = sc[].rw[].read[IntResult](rt, h)
    if not got:
        red("granted reader did not acquire (who=" + String(who) + ")")
    sc[].grant0[who] = sc[].npassed[]
    sc[].npassed[] = sc[].npassed[] + 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_c() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var rw = RWLock[Int](0)
    # simulated writer holder so all three readers below must queue.
    if not rw.try_write():
        red("pre-acquire (writer) failed")
    var sc = SceneC()
    sc.rw = UnsafePointer[RWLock[Int], MutAnyOrigin](to=rw)
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.grant0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.npassed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    for zi in range(24):
        buf[zi] = 0
    var scp = UnsafePointer[SceneC, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # register reverse -> FIFO park order r0, r1, r2.
    var tcb_r2 = TB.create()
    var h_r2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_r2), 0)
    var tcb_r1 = TB.create()
    var h_r1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_r1), 0)
    var tcb_r0 = TB.create()
    var h_r0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_r0), 0)
    buf[0] = h_r0.id()
    buf[1] = h_r1.id()
    buf[2] = h_r2.id()

    _ = scheduler_loop(rt, c_dispatch, ud)
    if rw.reader_waiter_count() != 3:
        red("expected 3 reader waiters: " + String(rw.reader_waiter_count()))

    # cancel the MIDDLE waiter (r1) before it is granted.
    var cancelled = rw.cancel_read_wait[IntResult](h_r1)
    if not cancelled:
        red("cancel_read_wait did not find the queued reader")
    if rw.reader_waiter_count() != 2:
        red("cancel did not shrink the reader FIFO")
    var double_cancel = rw.cancel_read_wait[IntResult](h_r1)
    if double_cancel:
        red("cancelling an already-removed waiter must return False")

    # writer releases: no writer waits, so ALL remaining queued readers
    # (r0, r2) are drained and granted; r1 must be skipped entirely.
    var handed = rw.unlock_write[IntResult](rt)
    if handed != 2:
        red("expected 2 readers granted, got " + String(handed))
    _ = scheduler_loop(rt, c_dispatch, ud)

    if not (h_r0.is_completed() and h_r2.is_completed()):
        red("surviving readers did not complete")
    if h_r1.state() != TaskControlBlock.WAITING:
        red("the cancelled reader must remain WAITING forever (no leaked grant)")
    if rw.is_granted[IntResult](h_r1):
        red("the cancelled reader must never carry a GRANT marker")
    if rw.reader_count() != 2:
        red("reader count wrong after the drain (expected r0+r2 only)")
    if rw.reader_waiter_count() != 0:
        red("reader FIFO not fully drained")
    if buf[8] != 0 or buf[10] != 1:
        red("surviving grant order not FIFO (expected r0 then r2)")
    print("T29 rwlock scenario C (cancel-one-reader, no leaked grant): PASS")


# ---------------------------------------------------------------------------
# Scenario D — mixed read/write storm: invariant holds, no lost wakeup,
# real reader concurrency
# ---------------------------------------------------------------------------

comptime KIND_R = Int(0)
comptime KIND_W = Int(1)

struct SceneD(ImplicitlyCopyable, ImplicitlyDeletable):
    var rw: UnsafePointer[RWLock[Int], MutAnyOrigin]
    var kind0: UnsafePointer[Int, MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var peak: UnsafePointer[Int, MutAnyOrigin]
    var ncomplete: UnsafePointer[Int, MutAnyOrigin]
    var bad_overlap: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.rw = UnsafePointer[RWLock[Int], MutAnyOrigin](unsafe_from_address=1)
        self.kind0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.peak = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ncomplete = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.bad_overlap = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def _check_invariant(sc: UnsafePointer[SceneD, MutAnyOrigin]) raises:
    """Mutual-exclusion invariant: readers and a writer are never both
    holding at the same time."""
    if sc[].rw[].reader_count() > 0 and sc[].rw[].is_write_locked():
        sc[].bad_overlap[] = 1
        red("invariant broken: readers held while a writer also held")


def d_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneD]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(6):
        if h.id() == sc[].ids0[k]:
            who = k
    if who == -1:
        red("unknown waiter id in scenario D")
    var ph = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].phs0) + who * 8
    )
    var kind = sc[].kind0[who]
    if ph[] == 0:
        claim_running(h)
        var got: Bool
        if kind == KIND_R:
            got = sc[].rw[].read[IntResult](rt, h)
        else:
            got = sc[].rw[].write[IntResult](rt, h)
        if got:
            # fast-acquired OR just claimed a grant marker: simulate a hold
            # window by self-parking; main decides when to release it.
            if kind == KIND_R and sc[].rw[].reader_count() > sc[].peak[]:
                sc[].peak[] = sc[].rw[].reader_count()
            _check_invariant(sc)
            ph[] = 1
            park_current(rt, h)
        # else: read()/write() already parked us internally on the RWLock's
        # own FIFO; ph stays 0 so the NEXT dispatch (driven by a grant) lands
        # back in this same branch and the marker check inside read/write
        # claims it transparently.
        return 1
    claim_running(h)
    if kind == KIND_R:
        _ = sc[].rw[].unlock_read[IntResult](rt)
    else:
        _ = sc[].rw[].unlock_write[IntResult](rt)
    _check_invariant(sc)
    sc[].ncomplete[] = sc[].ncomplete[] + 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_d() raises:
    var rt = create()
    var buf = stack_allocation[32, Int]()
    var rw = RWLock[Int](0)
    var sc = SceneD()
    sc.rw = UnsafePointer[RWLock[Int], MutAnyOrigin](to=rw)
    sc.kind0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    sc.peak = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 18 * 8)
    sc.ncomplete = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 19 * 8)
    sc.bad_overlap = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 20 * 8)
    for zi in range(32):
        buf[zi] = 0

    # arrival-order script: R0, R1, W0, R2, W1, R3 (index 0..5).
    sc.kind0[0] = KIND_R
    sc.kind0[1] = KIND_R
    sc.kind0[2] = KIND_W
    sc.kind0[3] = KIND_R
    sc.kind0[4] = KIND_W
    sc.kind0[5] = KIND_R
    var scp = UnsafePointer[SceneD, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # A2.2 (issue #68): owner pop is LIFO -> spawn in REVERSE arrival order
    # so the FIFO dispatch order matches the script above (R0..R3).
    var tcb5 = TB.create()
    var h5 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb5), 0)
    var tcb4 = TB.create()
    var h4 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb4), 0)
    var tcb3 = TB.create()
    var h3 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb3), 0)
    var tcb2 = TB.create()
    var h2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb2), 0)
    var tcb1 = TB.create()
    var h1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb1), 0)
    var tcb0 = TB.create()
    var h0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb0), 0)
    buf[6] = h0.id()
    buf[7] = h1.id()
    buf[8] = h2.id()
    buf[9] = h3.id()
    buf[10] = h4.id()
    buf[11] = h5.id()

    # ---- initial wave: R0,R1 hold concurrently; W0,R2,W1,R3 all queue ------
    _ = scheduler_loop(rt, d_dispatch, ud)
    if rw.reader_count() != 2:
        red("expected R0+R1 holding concurrently, got " + String(rw.reader_count()))
    if buf[18] < 2:
        red("peak concurrent readers never reached 2, got " + String(buf[18]))
    if rw.writer_waiter_count() != 2:
        red("expected W0,W1 queued, got " + String(rw.writer_waiter_count()))
    if rw.reader_waiter_count() != 2:
        red("expected R2,R3 queued (blocked by writer preference), got " + String(rw.reader_waiter_count()))

    # ---- release R0: still 1 reader holding, no grant yet ------------------
    unpark_current(rt, h0)
    _ = scheduler_loop(rt, d_dispatch, ud)
    if not h0.is_completed():
        red("R0 did not complete")
    if rw.reader_count() != 1:
        red("expected 1 reader remaining after R0 release")

    # ---- release R1: drops to 0 readers -> W0 granted (and self-parks) ----
    unpark_current(rt, h1)
    _ = scheduler_loop(rt, d_dispatch, ud)
    if not h1.is_completed():
        red("R1 did not complete")
    if not rw.is_write_locked():
        red("W0 should now hold exclusively")
    if rw.reader_count() != 0:
        red("no reader may hold while a writer holds")

    # ---- release W0: hands to FIFO-head writer W1 (preference) ------------
    unpark_current(rt, h2)
    _ = scheduler_loop(rt, d_dispatch, ud)
    if not h2.is_completed():
        red("W0 did not complete")
    if not rw.is_write_locked():
        red("W1 should now hold exclusively")
    if rw.writer_waiter_count() != 0:
        red("writer queue should be empty (W1 was the last writer)")
    if rw.reader_waiter_count() != 2:
        red("R2,R3 must still be queued behind W1")

    # ---- release W1: no writer waits -> drains BOTH R2 and R3 together ----
    unpark_current(rt, h4)
    _ = scheduler_loop(rt, d_dispatch, ud)
    if not h4.is_completed():
        red("W1 did not complete")
    if rw.is_write_locked():
        red("lock must be free of writers after the drain")
    if rw.reader_count() != 2:
        red("expected R2+R3 both granted together, got " + String(rw.reader_count()))
    if buf[18] < 2:
        red("peak concurrency regressed below 2 on the second reader wave")

    # ---- release R2 then R3: drains to fully idle -------------------------
    unpark_current(rt, h3)
    _ = scheduler_loop(rt, d_dispatch, ud)
    unpark_current(rt, h5)
    _ = scheduler_loop(rt, d_dispatch, ud)

    if buf[20] != 0:
        red("mutual-exclusion invariant was broken during the storm")
    if buf[19] != 6:
        red("not all 6 tasks completed (lost wakeup): " + String(buf[19]))
    if rw.reader_count() != 0 or rw.is_write_locked():
        red("lock not fully idle at the end of the storm")
    if rw.reader_waiter_count() != 0 or rw.writer_waiter_count() != 0:
        red("waiter queues not drained at the end of the storm")
    print("T29 rwlock scenario D (mixed read/write storm): PASS")


def main() raises:
    scenario_a()
    scenario_b()
    scenario_c()
    scenario_d()
