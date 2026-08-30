# mojito_async/test/unit/t23_unbounded_channel.mojo
#
# A5.2 unbounded channel (issue #90) — acceptance: unbounded-channel fast
# paths, park/resume choreography, and close semantics (spec §39-41).
#
# Cases:
#   1. Unbounded fast path: try_send succeeds far beyond any fixed capacity
#      (200 values) with NO park, no waiter registration; try_recv drains
#      FIFO; try_recv on empty returns None.
#   2. Sender/Receiver split: slot counts track the handles; try_* never
#      register waiters.
#   3. Park/resume: a receiver parked on an empty channel is woken by the
#      next send() and consumes the value on re-entry (exact event order
#      asserted); zero leftovers afterward.
#   4. S1 close-last-sender drains — two senders; closing the FIRST leaves
#      the send side open; closing the SECOND closes it: the receiver
#      drains the buffered values, then try_recv returns None; try_send
#      returns False; double-close is a no-op; splitting a new sender on a
#      closed side raises.
#   5. S2 close-last-sender wakes parked receivers — two receivers parked on
#      an empty channel; the last sender's close moves BOTH waiters to the
#      deferred wake list (FIFO), the driver resumes them, and each
#      receiver observes the closed channel and exits without consuming.
#   6. S3 close-last-receiver drops buffered values and fails sends — one
#      buffered value; the last receiver's close drops it (there is no
#      blocked-sender wake step: a sender never parks on an unbounded
#      channel); the resumed/subsequent send RAISES "ChannelError".
#
# Every scenario ends with ZERO leftovers: no waiters, no deferred wakes,
# no pending runnables, no unconsumed results.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.channel import (
    RecvOutcome,
    SendOutcome,
    UnboundedChannel,
    UnboundedReceiver,
    UnboundedSender,
    make_unbounded,
)
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T23 unbounded channel: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

comptime EV_C_PARK = Int(1)
comptime EV_C_RECV = Int(2)
comptime EV_C_DONE = Int(3)
comptime EV_P_SENT = Int(4)
comptime EV_P_DONE = Int(5)
comptime EV_C_CLOSED = Int(6)

comptime S_N = Int(64)
comptime S_ID_A = Int(72)
comptime S_ID_B = Int(80)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Shared driver scene: channel pointer + one Receiver handle (every
    Receiver handle over the same channel is interchangeable for recv() —
    the state lives in the channel, not the handle) + event log + up to two
    routed task ids."""

    var chan: UnsafePointer[UnboundedChannel[Int], MutAnyOrigin]
    var tx: UnboundedSender[Int]
    var rx: UnboundedReceiver[Int]
    var seq: UnsafePointer[Int, MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var id_a: UnsafePointer[Int, MutAnyOrigin]
    var id_b: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan = UnsafePointer[UnboundedChannel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.tx = UnboundedSender[Int](UnsafePointer[UnboundedChannel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.rx = UnboundedReceiver[Int](UnsafePointer[UnboundedChannel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_a = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_b = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def wire(buf: UnsafePointer[Int, MutAnyOrigin], sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    sc[].seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc[].n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_N * 8)
    sc[].id_a = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_ID_A * 8)
    sc[].id_b = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_ID_B * 8)
    buf[S_N] = 0


def rec(sc: UnsafePointer[Scene, MutAnyOrigin], ev: Int):
    var i = sc[].n[]
    sc[].seq[i] = ev
    sc[].n[] = i + 1


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _complete(h: JoinHandle[IntResult], res: Int) raises:
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    h.tcb()[].mark_result(IntResult(res))


def producer_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    claim_running(h)
    var outcome = sc[].tx.send(rt, h, 42)
    if outcome.is_parked():
        red("producer_slice: unbounded send must never park")
    rec(sc, EV_P_SENT)
    _complete(h, 42)
    rec(sc, EV_P_DONE)


def receiver_once_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    """One-shot receive: called once per dispatch slice; on park the SAME
    call re-enters on resume and takes the fast path or observes close."""
    claim_running(h)
    var v = sc[].rx.recv(rt, h)
    if v.is_parked():
        rec(sc, EV_C_PARK)
        return
    if v.is_value():
        rec(sc, EV_C_RECV)
        _complete(h, v.value())
        rec(sc, EV_C_DONE)
        return
    rec(sc, EV_C_CLOSED)
    _complete(h, -1)
    rec(sc, EV_C_DONE)


def drain(mut rt: Runtime, sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """Driver-side deferred-wake drain: a plain, concrete function — resumes
    parked waiters through the canonical wake path (result type known here)."""
    while sc[].chan[].to_wake_len() > 0:
        var wr = sc[].chan[].pop_to_wake()
        if wr.task_id == 0:
            break
        var hp = _handle(wr.tcb_addr, wr.task_id)
        unpark_current(rt, hp)


def dispatch_prod_recv(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Case 3 dispatch: id_a is the producer, id_b the receiver."""
    var sc = ud.bitcast[Scene]()
    var h = _handle(tcb_addr, tid)
    if tid == sc[].id_a[]:
        producer_slice(rt, h, sc)
        drain(rt, sc)
        return 1
    if tid == sc[].id_b[]:
        receiver_once_slice(rt, h, sc)
        drain(rt, sc)
        return 1
    raise Error("T23: unknown task id " + String(tid))


def dispatch_two_recv(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """S2 dispatch: id_a and id_b are both receivers over the shared
    channel (any Receiver handle works for recv() — the state lives in the
    channel, not the handle)."""
    var sc = ud.bitcast[Scene]()
    var h = _handle(tcb_addr, tid)
    if tid == sc[].id_a[] or tid == sc[].id_b[]:
        receiver_once_slice(rt, h, sc)
        drain(rt, sc)
        return 1
    raise Error("T23 S2: unknown task id " + String(tid))


def seq_find(sc: UnsafePointer[Scene, MutAnyOrigin], ev: Int) -> Int:
    for i in range(sc[].n[]):
        if sc[].seq[i] == ev:
            return i
    return -1


def count_ev(sc: UnsafePointer[Scene, MutAnyOrigin], ev: Int) -> Int:
    var k = 0
    for i in range(sc[].n[]):
        if sc[].seq[i] == ev:
            k += 1
    return k


def main() raises:
    # ---- 1. unbounded fast path: far beyond any fixed capacity ---------------
    var ch = make_unbounded[Int]()
    if not ch.is_empty():
        red("fresh channel should be empty")
    for i in range(200):
        if not ch.try_send(i):
            red("try_send " + String(i) + " must never fail on an open channel")
    if ch.is_empty() or ch.len() != 200:
        red("len must reflect 200 buffered values (no capacity bound)")
    if ch.send_waiters_len() != 0 or ch.recv_waiters_len() != 0:
        red("fast-path sends must never register waiters")
    var total = 0
    for i in range(200):
        var v = ch.try_recv()
        if not v:
            red("try_recv ran dry at " + String(i))
        if v.value() != i:
            red("FIFO order violated on drain at " + String(i))
        total += v.value()
    if total != 19900:
        red("drain sum " + String(total) + " != 19900")
    if not ch.is_empty():
        red("channel must be empty after full drain")
    var nothing = ch.try_recv()
    if nothing:
        red("try_recv on an empty channel must return None")
    if ch.is_send_closed() or ch.is_recv_closed() or ch.is_closed():
        red("fresh channel must be open")

    # ---- 2. Sender/Receiver split + slot counts ------------------------------
    var keep = make_unbounded[Int]()
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

    # ---- 3. park/resume: receiver parks on empty, producer's send wakes it ---
    var rt3 = create()
    var buf3 = stack_allocation[256, Int]()
    var ch3 = make_unbounded[Int]()
    var tx3 = ch3.sender()
    var rx3 = ch3.receiver()
    var sc3 = Scene()
    sc3.chan = UnsafePointer[UnboundedChannel[Int], MutAnyOrigin](to=ch3)
    sc3.tx = tx3
    sc3.rx = rx3
    var scp3 = UnsafePointer[Scene, MutAnyOrigin](to=sc3)
    wire(buf3, scp3)
    var ud3 = scp3.bitcast[Byte]()
    var t_p3 = TB.create()
    var t_c3 = TB.create()
    # A2.2 (issue #68): owner pop is LIFO, so spawn the producer FIRST and
    # the receiver SECOND -> the receiver (spawned last) pops first and
    # parks on the empty channel before the producer ever runs.
    var h_p3 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t_p3), 0)
    var h_c3 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t_c3), 0)
    scp3[].id_a[] = h_p3.id()
    scp3[].id_b[] = h_c3.id()
    var served3 = scheduler_loop(rt3, dispatch_prod_recv, ud3)
    if served3 != 3:
        red("park/resume served " + String(served3) + ", expected 3 slices")
    if not (h_p3.is_completed() and h_c3.is_completed()):
        red("park/resume: not both tasks completed")
    if buf3[S_N] != 5:
        red("park/resume: expected exactly 5 events, got " + String(buf3[S_N]))
    if buf3[0] != EV_C_PARK or buf3[1] != EV_P_SENT or buf3[2] != EV_P_DONE or buf3[3] != EV_C_RECV or buf3[4] != EV_C_DONE:
        red("park/resume: wrong event order")
    if h_p3.join().v != 42:
        red("park/resume: producer result wrong")
    if h_c3.join().v != 42:
        red("park/resume: consumer must receive the producer's item")
    if not ch3.is_empty():
        red("park/resume: channel must be drained")
    if ch3.send_waiters_len() != 0 or ch3.recv_waiters_len() != 0 or ch3.to_wake_len() != 0:
        red("park/resume: leftover waiters/wakes")
    if rt3.pending() != 0:
        red("park/resume: runnable queue not quiet")
    print("case 3 ok")

    # ---- S1: close-last-sender drains; receive side observes close -----------
    var s1 = make_unbounded[Int]()
    var sx1 = s1.sender()
    var sx2 = s1.sender()
    var rx1 = s1.receiver()
    if s1.sender_count() != 2 or s1.receiver_count() != 1:
        red("S1: split counts wrong")
    if not sx1.try_send(21) or not sx2.try_send(22):
        red("S1: prefill sends failed")
    sx1.close()
    if s1.is_send_closed() or s1.is_closed():
        red("S1: closing the FIRST sender must not close the send side")
    if not sx2.try_send(23):
        red("S1: remaining sender must still send after the first close")
    sx2.close()
    if not s1.is_send_closed():
        red("S1: send side must close when the LAST sender closes")
    if s1.is_recv_closed():
        red("S1: the receive side must stay open")
    var t23 = sx2.try_send(99)
    if t23:
        red("S1: try_send on the closed side must return False")
    var a = rx1.try_recv()
    var b = rx1.try_recv()
    var c = rx1.try_recv()
    var d = rx1.try_recv()
    if not a or not b or not c or d:
        red("S1: receiver must drain 3 values then hit closed")
    if a.value() != 21 or b.value() != 22 or c.value() != 23:
        red("S1: drain order wrong")
    if s1.send_waiters_len() != 0 or s1.recv_waiters_len() != 0 or s1.to_wake_len() != 0:
        red("S1: close with zero waiters must wake nobody")
    try:
        sx2.close()  # idempotent
    except Error:
        red("S1: double-close must be a no-op")
    var split = False
    try:
        _ = s1.sender()
    except Error:
        split = True
    if not split:
        red("S1: splitting a sender on a closed side must raise")
    print("S1 ok")

    # ---- S2: close-last-sender wakes parked receivers (N=2) -------------------
    var rt2 = create()
    var buf2 = stack_allocation[256, Int]()
    var ch2 = make_unbounded[Int]()
    var tx2 = ch2.sender()
    var rx2a = ch2.receiver()
    var rx2b = ch2.receiver()
    var sc2 = Scene()
    sc2.chan = UnsafePointer[UnboundedChannel[Int], MutAnyOrigin](to=ch2)
    sc2.rx = rx2a
    var scp2 = UnsafePointer[Scene, MutAnyOrigin](to=sc2)
    wire(buf2, scp2)
    var ud2 = scp2.bitcast[Byte]()
    var ta = TB.create()
    var tb = TB.create()
    var h_a = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=ta), 0)
    var h_b = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=tb), 0)
    scp2[].id_a[] = h_a.id()
    scp2[].id_b[] = h_b.id()
    if rx2b.is_closed():
        red("S2: second receiver handle must be open before use")
    var served_a = scheduler_loop(rt2, dispatch_two_recv, ud2)
    if served_a != 2:
        red("S2: first drive served " + String(served_a) + ", expected 2 (both park)")
    if not (h_a.state() == TaskControlBlock.WAITING and h_b.state() == TaskControlBlock.WAITING):
        red("S2: receivers must be parked before close")
    if ch2.recv_waiters_len() != 2:
        red("S2: two receiver waiters must be registered")
    tx2.close()
    if not ch2.is_send_closed():
        red("S2: send side must close on last sender close")
    if ch2.recv_waiters_len() != 0 or ch2.to_wake_len() != 2:
        red("S2: close must move BOTH parked receivers to the wake list")
    drain(rt2, scp2)
    var served_b = scheduler_loop(rt2, dispatch_two_recv, ud2)
    if served_b != 2:
        red("S2: second drive served " + String(served_b) + ", expected 2 (resumed receivers)")
    if not (h_a.is_completed() and h_b.is_completed()):
        red("S2: receivers must complete after observing close")
    if count_ev(scp2, EV_C_CLOSED) != 2:
        red("S2: both receivers must observe close, not consume")
    if h_a.join().v != -1 or h_b.join().v != -1:
        red("S2: receivers must exit with the closed marker (-1)")
    if rt2.pending() != 0 or ch2.to_wake_len() != 0:
        red("S2: leftovers")
    print("S2 ok")

    # ---- S3: close-last-receiver drops buffered values; sends fail -----------
    var ch4 = make_unbounded[Int]()
    var tx4 = ch4.sender()
    var rx4 = ch4.receiver()
    if not tx4.try_send(5):
        red("S3: prefill failed")
    if ch4.len() != 1:
        red("S3: prefill must be buffered")
    rx4.close()
    if not ch4.is_recv_closed():
        red("S3: receive side must close on last receiver close")
    if ch4.len() != 0:
        red("S3: closing the last receiver must drop the buffered value")
    # unbounded channels never park a sender, so there is no blocked-sender
    # wake step: close leaves zero waiters/wakes immediately.
    if ch4.send_waiters_len() != 0 or ch4.recv_waiters_len() != 0 or ch4.to_wake_len() != 0:
        red("S3: close must leave zero waiters/wakes (no sender ever parks)")
    if tx4.try_send(9):
        red("S3: try_send after close must fail")
    var rt4 = create()
    var t_x4 = TB.create()
    var h_x4 = spawn(rt4, UnsafePointer[TB, MutAnyOrigin](to=t_x4), 0)
    var raised = False
    try:
        _ = tx4.send(rt4, h_x4, 9)
    except Error:
        raised = True
    if not raised:
        red("S3: send on a closed receive side must raise")
    if rt4.pending() != 1:
        red("S3: the never-parked probe task stays queued (not dispatched)")
    print("S3 ok")

    print("T23 unbounded channel: PASS")
