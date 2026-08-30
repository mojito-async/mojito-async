# mojito_async/test/unit/t22_channel_close.mojo
#
# A1.3 channel (issue #35) — acceptance: close semantics (spec §41, B19).
#
# Cases:
#   S1 close-last-sender drains — two senders; closing the FIRST leaves the
#        send side open; closing the SECOND closes it: receivers drain the
#        buffered values, then receive returns None (closed observable);
#        try_send returns False; double-close is a no-op; splitting a new
#        sender on a closed side raises.
#   S2 close-last-sender wakes parked receivers — two receivers parked on an
#        empty channel; the last sender's close moves BOTH waiters to the
#        deferred wake list (FIFO), the driver resumes them, and each
#        receiver observes the closed channel and exits without consuming.
#   S3 close-last-receiver wakes a blocked sender — one producer parked on a
#        full buffer; the last receiver's close drops the buffer, moves the
#        sender waiter to the wake list, and the resumed send RAISES
#        "ChannelError" (subsequent sends fail).
#
# Every scenario ends with ZERO leftovers: no waiters, no pending runnables,
# no deferred wakes, no unconsumed results.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.channel import (
    Channel,
    Receiver,
    RecvOutcome,
    SendOutcome,
    Sender,
    make_channel,
    make_receiver,
    make_sender,
)
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T22 channel close: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

comptime EV_R_PARK = Int(1)
comptime EV_R_CLOSED = Int(2)
comptime EV_R_DONE = Int(3)
comptime EV_P_PARK = Int(4)
comptime EV_P_RAISED = Int(5)
comptime EV_P_DONE = Int(6)
comptime EV_R_RECV = Int(7)
comptime EV_P_SENT = Int(8)

# Scene buf slot layout (event log owns slots [0, EVENTS)).
comptime S_N = Int(160)
comptime S_R1 = Int(168)
comptime S_R2 = Int(176)
comptime S_P = Int(184)
comptime S_R1_PHASE = Int(192)
comptime S_R2_PHASE = Int(200)
comptime S_P_PHASE = Int(208)
comptime S_RAISED = Int(216)
comptime EVENTS = Int(160)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan: UnsafePointer[Channel[Int], MutAnyOrigin]
    var tx: Sender[Int]
    var rx: Receiver[Int]
    var seq: UnsafePointer[Int, MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var r1: UnsafePointer[Int, MutAnyOrigin]
    var r2: UnsafePointer[Int, MutAnyOrigin]
    var p: UnsafePointer[Int, MutAnyOrigin]
    var r1_phase: UnsafePointer[Int, MutAnyOrigin]
    var r2_phase: UnsafePointer[Int, MutAnyOrigin]
    var p_phase: UnsafePointer[Int, MutAnyOrigin]
    var raised: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.tx = Sender[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.rx = Receiver[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.r1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.r2 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.r1_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.r2_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.raised = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
def wire(buf: UnsafePointer[Int, MutAnyOrigin], sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    sc[].seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc[].n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_N * 8)
    sc[].r1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_R1 * 8)
    sc[].r2 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_R2 * 8)
    sc[].p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_P * 8)
    sc[].r1_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_R1_PHASE * 8)
    sc[].r2_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_R2_PHASE * 8)
    sc[].p_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_P_PHASE * 8)
    sc[].raised = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_RAISED * 8)
    buf[S_N] = 0
    buf[S_R1_PHASE] = 0
    buf[S_R2_PHASE] = 0
    buf[S_P_PHASE] = 0
    buf[S_RAISED] = 0


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


def receiver_slice(
    mut rt: Runtime,
    h: JoinHandle[IntResult],
    sc: UnsafePointer[Scene, MutAnyOrigin],
    phase_slot: Int,
    tid: Int,
) raises:
    claim_running(h)
    var v = sc[].rx.recv(rt, h)
    if v.is_parked():
        rec(sc, EV_R_PARK)
        if phase_slot == S_R1_PHASE:
            sc[].r1_phase[] = 1
        else:
            sc[].r2_phase[] = 1
        return
    if v.is_value():
        rec(sc, EV_R_RECV)
        _complete(h, v.value())
        rec(sc, EV_R_DONE)
        return
    # closed + drained: this receiver observes close and exits
    rec(sc, EV_R_CLOSED)
    _complete(h, -1)
    rec(sc, EV_R_DONE)


def producer_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    claim_running(h)
    if sc[].p_phase[] == 0:
        sc[].p_phase[] = 1
    try:
        var outcome = sc[].tx.send(rt, h, 77)
        if outcome.is_parked():
            rec(sc, EV_P_PARK)
            return
    except e:
        sc[].raised[] = 1
        rec(sc, EV_P_RAISED)
        _complete(h, -1)
        rec(sc, EV_P_DONE)
        return
    rec(sc, EV_P_SENT)
    _complete(h, 77)
    rec(sc, EV_P_DONE)


def drain(mut rt: Runtime, sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    while sc[].chan[].to_wake_len() > 0:
        var wr = sc[].chan[].pop_to_wake()
        if wr.task_id == 0:
            break
        var hp = _handle(wr.tcb_addr, wr.task_id)
        unpark_current(rt, hp)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = _handle(tcb_addr, tid)
    if tid == sc[].r1[]:
        receiver_slice(rt, h, sc, S_R1_PHASE, tid)
        drain(rt, sc)
        return 1
    if tid == sc[].r2[]:
        receiver_slice(rt, h, sc, S_R2_PHASE, tid)
        drain(rt, sc)
        return 1
    if tid == sc[].p[]:
        producer_slice(rt, h, sc)
        drain(rt, sc)
        return 1
    raise Error("T22: unknown task id " + String(tid))


def seq_find(sc: UnsafePointer[Scene, MutAnyOrigin], ev: Int) -> Int:
    for i in range(sc[].n[]):
        if sc[].seq[i] == ev:
            return i
    return -1


def main() raises:
    # ---- S1: close-last-sender drains; receive side observes close -------------
    var s1 = make_channel[Int](3)
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
        red("S1: receivers must drain 3 values then hit closed")
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
    var ch2 = make_channel[Int](4)
    var tx2 = ch2.sender()
    var rx2a = ch2.receiver()
    var rx2b = ch2.receiver()
    var sc2 = Scene()
    sc2.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=ch2)
    sc2.tx = tx2
    sc2.rx = rx2a
    var scp2 = UnsafePointer[Scene, MutAnyOrigin](to=sc2)
    wire(buf2, scp2)
    var ud2 = scp2.bitcast[Byte]()
    var ta = TB.create()
    var tb = TB.create()
    var h_a = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=ta), 0)
    var h_b = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=tb), 0)
    scp2[].r1[] = h_a.id()
    scp2[].r2[] = h_b.id()
    var served_a = scheduler_loop(rt2, dispatch, ud2)
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
    var served_b = scheduler_loop(rt2, dispatch, ud2)
    if served_b != 2:
        red("S2: second drive served " + String(served_b) + ", expected 2 (resumed receivers)")
    if not (h_a.is_completed() and h_b.is_completed()):
        red("S2: receivers must complete after observing close")
    var i_park_a = seq_find(scp2, EV_R_PARK)
    var i_park_b = -1
    for i in range(i_park_a + 1, buf2[S_N]):
        if buf2[i] == EV_R_PARK:
            i_park_b = i
            break
    var i_closed_a = seq_find(scp2, EV_R_CLOSED)
    if i_park_a < 0 or i_park_b < 0 or i_closed_a < 0:
        red("S2: missing park/close events")
    if i_closed_a < i_park_b:
        red("S2: wakes must follow BOTH parks (no wake before park)")
    if h_a.join().v != -1 or h_b.join().v != -1:
        red("S2: receivers must exit with the closed marker (-1)")
    if rt2.pending() != 0 or ch2.to_wake_len() != 0:
        red("S2: leftovers")
    print("S2 ok")

    # ---- S3: close-last-receiver wakes a blocked sender; subsequent send fails ---
    var rt3 = create()
    var buf3 = stack_allocation[256, Int]()
    var ch3 = make_channel[Int](1)
    if not ch3.try_send(5):
        red("S3: prefill failed")
    var tx3 = ch3.sender()
    var rx3 = ch3.receiver()
    var sc3 = Scene()
    sc3.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=ch3)
    sc3.tx = tx3
    sc3.rx = rx3
    var scp3 = UnsafePointer[Scene, MutAnyOrigin](to=sc3)
    wire(buf3, scp3)
    var ud3 = scp3.bitcast[Byte]()
    var tp = TB.create()
    var h_p = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=tp), 0)
    scp3[].p[] = h_p.id()
    var served_a3 = scheduler_loop(rt3, dispatch, ud3)
    if served_a3 != 1:
        red("S3: producer must park in one slice")
    if h_p.state() != TaskControlBlock.WAITING:
        red("S3: producer must be WAITING (parked on full buffer)")
    if ch3.send_waiters_len() != 1:
        red("S3: one sender waiter must be registered")
    rx3.close()
    if not ch3.is_recv_closed():
        red("S3: receive side must close on last receiver close")
    if ch3.len() != 0:
        red("S3: closing the last receiver must drop the buffered value")
    if ch3.send_waiters_len() != 0 or ch3.to_wake_len() != 1:
        red("S3: close must move the blocked sender to the wake list")
    drain(rt3, scp3)
    var served_b3 = scheduler_loop(rt3, dispatch, ud3)
    if served_b3 != 1:
        red("S3: resumed producer slice missing")
    if buf3[S_RAISED] != 1:
        red("S3: the resumed send must raise on the closed receive side")
    if seq_find(scp3, EV_P_RAISED) < 0:
        red("S3: no P_RAISED event")
    if not h_p.is_completed():
        red("S3: producer must settle after the raise")
    if tx3.try_send(9):
        red("S3: try_send after close must fail")
    if h_p.join().v != -1:
        red("S3: producer result must be the failed marker")
    if rt3.pending() != 0 or ch3.to_wake_len() != 0 or ch3.send_waiters_len() != 0:
        red("S3: leftovers")
    print("S3 ok")

    print("T22 channel close: PASS")