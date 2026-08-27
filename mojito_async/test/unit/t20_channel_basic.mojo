# mojito_async/test/unit/t20_channel_basic.mojo
#
# A1.3 channel (issue #35) — acceptance: bounded-channel fast paths and
# Sender/Receiver split (spec §39-41, §40.1 fast receive, §40.2).
#
# Cases:
#   1. Ring/bound semantics: len/is_empty/is_full move with capacity; try_send
#      on a full channel returns False and consumes NOTHING; try_recv on an
#      empty channel returns None.
#   2. SPSC fast path: capacity-4 channel carries 4 values FIFO with NO park,
#      no waiter registration, no runnable leftovers.
#   3. MPSC (3 producers x 4 values, capacity 64 — no backpressure): a single
#      consumer drains all 12; the SUM matches; each producer's values arrive
#      in FIFO subsequence order; results settle and are joined (no leaks).
#   4. Sender/Receiver split: slot counts track the handles; try_* never
#      register waiters.
#
# Composition: spawn + scheduler_loop drive + channel (the MPSC case runs
# through the A1.1 runtime).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.channel import Channel, make_channel
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, execute, spawn


def red(what: String) raises -> None:
    print("T20 channel basic: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """MPSC scene: channel pointer + value log + task ids.

    buf layout (Int slots):
      [0..11]   received values (12 slots)
      [12]      n_received
      [13]      p1 id, [14] p2 id, [15] p3 id, [16] c1 id
    """

    var chan: UnsafePointer[Channel[Int], MutAnyOrigin]
    var vals: UnsafePointer[Int, MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var p1: UnsafePointer[Int, MutAnyOrigin]
    var p2: UnsafePointer[Int, MutAnyOrigin]
    var p3: UnsafePointer[Int, MutAnyOrigin]
    var c1: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.vals = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p2 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p3 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.c1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def prod_body(ud: BytePtr, tag: Int) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    for k in range(4):
        if not sc[].chan[].try_send(tag + k):
            raise Error("T20 MPSC: producer " + String(tag) + " send failed")
    return IntResult(tag)


def prod_1(ud: BytePtr) raises -> IntResult:
    return prod_body(ud, 100)


def prod_2(ud: BytePtr) raises -> IntResult:
    return prod_body(ud, 200)


def prod_3(ud: BytePtr) raises -> IntResult:
    return prod_body(ud, 300)


def cons_body(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    var total = 0
    for i in range(12):
        var v = sc[].chan[].try_recv()
        if not v:
            raise Error("T20 MPSC: consumer ran dry at " + String(i))
        sc[].vals[i] = v.value()
        total += v.value()
    sc[].n[] = 12
    return IntResult(total)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    if tid == sc[].p1[]:
        _ = execute(h, prod_1, ud)
        return 1
    if tid == sc[].p2[]:
        _ = execute(h, prod_2, ud)
        return 1
    if tid == sc[].p3[]:
        _ = execute(h, prod_3, ud)
        return 1
    if tid == sc[].c1[]:
        _ = execute(h, cons_body, ud)
        return 1
    raise Error("T20: unknown task id " + String(tid))


def main() raises:
    # ---- 1. ring/bound semantics + try_* fast paths --------------------------
    var ch = make_channel[Int](4)
    if ch.capacity() != 4:
        red("capacity not 4")
    if not ch.is_empty() or ch.is_full():
        red("fresh channel should be empty and not full")
    if not ch.try_send(11):
        red("try_send on empty channel failed")
    if ch.is_empty() or ch.len() != 1:
        red("len must reflect one buffered value")
    if not ch.try_send(12) or not ch.try_send(13) or not ch.try_send(14):
        red("fast-path sends failed")
    if not ch.is_full():
        red("4/4 should be full")
    # full: try_send must fail WITHOUT consuming the item
    var got = ch.try_send(99)
    if got:
        red("try_send on a full channel must return False")
    if ch.len() != 4:
        red("rejected item must not be buffered")
    # try_recv on empty must return None
    var drained = make_channel[Int](2)
    var nothing = drained.try_recv()
    if nothing:
        red("try_recv on an empty channel must return None")
    # FIFO drain
    if ch.is_send_closed() or ch.is_recv_closed() or ch.is_closed():
        red("fresh channel must be open")
    var v1 = ch.try_recv()
    var v2 = ch.try_recv()
    var v3 = ch.try_recv()
    var v4 = ch.try_recv()
    if not v1 or not v2 or not v3 or not v4:
        red("buffered values not all receivable")
    if (
        v1.value() != 11
        or v2.value() != 12
        or v3.value() != 13
        or v4.value() != 14
    ):
        red("FIFO order violated on drain")
    if not ch.is_empty():
        red("channel must be empty after full drain")
    if ch.send_waiters_len() != 0 or ch.recv_waiters_len() != 0:
        red("fast paths must never register waiters")

    # ---- 2. Sender/Receiver split + slot counts ------------------------------
    var keep = make_channel[Int](2)
    var tx = keep.sender()
    var rx = keep.receiver()
    if keep.sender_count() != 1 or keep.receiver_count() != 1:
        red("split counts wrong")
    if not tx.try_send(7):
        red("sender try_send failed")
    var got7 = rx.try_recv()
    if not got7 or got7.value() != 7:
        red("receiver try_recv failed")
    if tx.is_closed() or rx.is_closed():
        red("fresh handles must report open")

    # ---- 3. MPSC through the scheduler (spawn + drive + channel) -------------
    var rt = create()
    var buf = stack_allocation[32, Int]()
    var sc = Scene()
    sc.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=keep)
    sc.vals = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    sc.p1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 13 * 8)
    sc.p2 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 14 * 8)
    sc.p3 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 15 * 8)
    sc.c1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16 * 8)
    var mpsc = make_channel[Int](64)
    sc.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=mpsc)
    var sp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = sp.bitcast[Byte]()

    var t1 = TB.create()
    var t2 = TB.create()
    var t3 = TB.create()
    var t4 = TB.create()
    var h1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t1), 0)
    var h2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t2), 0)
    var h3 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t3), 0)
    var h4 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t4), 0)
    buf[13] = h1.id()
    buf[14] = h2.id()
    buf[15] = h3.id()
    buf[16] = h4.id()
    var served = scheduler_loop(rt, dispatch, ud)
    if served != 4:
        red("MPSC scheduler served " + String(served) + ", expected 4")
    if not (h1.is_completed() and h2.is_completed() and h3.is_completed() and h4.is_completed()):
        red("not every MPSC task completed")
    if rt.pending() != 0:
        red("runnable queue not quiet after MPSC drive")
    if mpsc.len() != 0:
        red("MPSC channel must be fully drained")
    if mpsc.send_waiters_len() != 0 or mpsc.recv_waiters_len() != 0:
        red("MPSC no-park run must leave no waiters")
    if buf[12] != 12:
        red("consumer did not receive all 12 values")
    var total = 0
    for i in range(12):
        total += buf[i]
    if total != 2418:
        red("MPSC sum " + String(total) + " != 2418")
    # per-producer FIFO subsequence: tags 100/200/300 increment by 1 each
    for p in range(3):
        var tag = (p + 1) * 100
        var expect = tag
        var found = 0
        for i in range(12):
            if buf[i] == expect:
                expect += 1
                found += 1
        if found != 4:
            red("producer " + String(tag) + " values not FIFO")
    # join settles the results (no result leaks)
    var r1 = h1.join()
    var r2 = h2.join()
    var r3 = h3.join()
    var r4 = h4.join()
    if r1.v != 100 or r2.v != 200 or r3.v != 300:
        red("producer results wrong")
    if r4.v != 2418:
        red("consumer result wrong: " + String(r4.v))


    print("T20 channel basic: PASS")