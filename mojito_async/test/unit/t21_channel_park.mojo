# mojito_async/test/unit/t21_channel_park.mojo
#
# A1.3 channel (issue #35) — acceptance: slow paths, park/wake choreography,
# deterministic lost-wakeup discipline (spec §40.2, B18 backpressure).
#
# The A1.1 colorless runtime has no fibers: a task parks BETWEEN dispatcher
# slices.  The channel's send/recv attempt once, register a waiter, and
# suspend via the canonical A1.1 park (`park_current`); the embedding
# driver re-enters the task on resume, and deferred wakes (the channel's
# `_to_wake`) are drained by the driver via `unpark_current` (the canonical
# wake — single source, issue #39).
#
# Modes (capacity-1 channels, deterministic FIFO schedules):
#   mode 0 BACPRE   — backpressure parks sender: buffer prefilled; the
#              producer's send parks while the buffer is full; the consumer
#              drains the prefill, wakes the producer, and the producer's
#              item then flows through.  Exact order asserted.
#   mode 1 RECVPARK — consumer parks on the empty channel; the producer's
#              send wakes it (handoff), the consumer resumes and receives.
#   mode 2 CHAIN    — M=3 items through a capacity-1 channel; exact park
#              counts (producer 2, consumer 3), FIFO values, full drain.
#   mode 3 PINGPONG — M=64 items ping-ponged across one scheduler drive;
#              asserts full drain, sum 2080, and ZERO leftovers: no waiters,
#              no pending runnables, no unconsumed results (no leaks).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.channel import Channel, Receiver, Sender, make_channel
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop, yield_now
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T21 channel park: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

comptime EV_P_START = Int(1)
comptime EV_P_PARK = Int(2)
comptime EV_P_SENT = Int(3)
comptime EV_P_DONE = Int(4)
comptime EV_C_START = Int(5)
comptime EV_C_PARK = Int(6)
comptime EV_C_RECV = Int(7)
comptime EV_C_DONE = Int(8)

comptime MODE_BACPRE = Int(0)
comptime MODE_RECVPARK = Int(1)
comptime MODE_CHAIN = Int(2)
comptime MODE_PINGPONG = Int(3)

# ~265 events: four slices per item — send, park, recv, park), so the two
# regions can never collide.
comptime EVENTS = Int(512)
comptime EVENTS_BASE = Int(512)
comptime S_N = Int(160)
comptime S_P_ID = Int(168)
comptime S_C_ID = Int(176)
comptime S_P_PHASE = Int(184)
comptime S_P_ITEM = Int(192)
comptime S_P_IDX = Int(200)
comptime S_C_PHASE = Int(208)
comptime S_C_COUNT = Int(216)
comptime S_MODE = Int(224)
comptime S_M = Int(232)
comptime S_C_TOTAL = Int(240)
comptime S_C_TARGET = Int(248)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan: UnsafePointer[Channel[Int], MutAnyOrigin]
    var tx: Sender[Int]
    var rx: Receiver[Int]
    var p_tcb: UnsafePointer[TB, MutAnyOrigin]
    var c_tcb: UnsafePointer[TB, MutAnyOrigin]
    var seq: UnsafePointer[Int, MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var p_id: UnsafePointer[Int, MutAnyOrigin]
    var c_id: UnsafePointer[Int, MutAnyOrigin]
    var p_phase: UnsafePointer[Int, MutAnyOrigin]
    var p_item: UnsafePointer[Int, MutAnyOrigin]
    var p_idx: UnsafePointer[Int, MutAnyOrigin]
    var c_phase: UnsafePointer[Int, MutAnyOrigin]
    var c_count: UnsafePointer[Int, MutAnyOrigin]
    var mode: UnsafePointer[Int, MutAnyOrigin]
    var M: UnsafePointer[Int, MutAnyOrigin]
    var c_total: UnsafePointer[Int, MutAnyOrigin]
    var c_target: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.tx = Sender[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.rx = Receiver[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.p_tcb = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=1)
        self.c_tcb = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=1)
        self.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.c_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p_item = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.p_idx = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.c_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.c_count = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.mode = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.M = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.c_total = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.c_target = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def wire(buf: UnsafePointer[Int, MutAnyOrigin], sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """Point the Scene's slots into the caller's buf."""
    sc[].seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + EVENTS_BASE * 8)
    sc[].n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_N * 8)
    sc[].p_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_P_ID * 8)
    sc[].c_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_C_ID * 8)
    sc[].p_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_P_PHASE * 8)
    sc[].p_item = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_P_ITEM * 8)
    sc[].p_idx = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_P_IDX * 8)
    sc[].c_phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_C_PHASE * 8)
    sc[].c_count = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_C_COUNT * 8)
    sc[].mode = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_MODE * 8)
    sc[].M = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_M * 8)
    sc[].c_total = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_C_TOTAL * 8)
    sc[].c_target = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + S_C_TARGET * 8)
    buf[S_N] = 0
    buf[S_P_PHASE] = 0
    buf[S_P_ITEM] = 1
    buf[S_P_IDX] = 0
    buf[S_C_PHASE] = 0
    buf[S_C_COUNT] = 0
    buf[S_C_TOTAL] = 0
    buf[S_C_TARGET] = 0


def rec(sc: UnsafePointer[Scene, MutAnyOrigin], ev: Int):
    var i = sc[].n[]
    sc[].seq[i] = ev
    sc[].n[] = i + 1


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _complete_ok(h: JoinHandle[IntResult], res: Int) raises:
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    h.tcb()[].mark_result(IntResult(res))


def producer_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    claim_running(h)
    if sc[].p_phase[] == 0:
        rec(sc, EV_P_START)
        sc[].p_phase[] = 1
    while sc[].p_idx[] < sc[].M[]:
        var item = sc[].p_item[]
        sc[].tx.send(rt, h, item)
        if h.state() == TaskControlBlock.WAITING:
            rec(sc, EV_P_PARK)
            return  # parked; re-entered on resume with the pending p_item
        rec(sc, EV_P_SENT)
        sc[].p_idx[] += 1
        sc[].p_item[] = 1 + sc[].p_idx[]
        if sc[].mode[] == MODE_PINGPONG:
            yield_now(rt, h)
            return
    _complete_ok(h, sc[].M[])
    rec(sc, EV_P_DONE)


def consumer_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    claim_running(h)
    if sc[].c_phase[] == 0:
        rec(sc, EV_C_START)
        sc[].c_phase[] = 1
    while sc[].c_count[] < sc[].c_target[]:
        var v = sc[].rx.recv(rt, h)
        if h.state() == TaskControlBlock.WAITING:
            rec(sc, EV_C_PARK)
            return
        if not v:
            _complete_ok(h, -1)
            rec(sc, EV_C_DONE)
            return
        sc[].c_total[] += v.value()
        rec(sc, EV_C_RECV)
        sc[].c_count[] += 1
        if sc[].mode[] == MODE_PINGPONG:
            yield_now(rt, h)
            return
    _complete_ok(h, sc[].c_total[])
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


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h: JoinHandle[IntResult]
    if tid == sc[].p_id[]:
        h = JoinHandle[IntResult](sc[].p_tcb, tid)
        producer_slice(rt, h, sc)
        drain(rt, sc)
        return 1
    if tid == sc[].c_id[]:
        h = JoinHandle[IntResult](sc[].c_tcb, tid)
        consumer_slice(rt, h, sc)
        drain(rt, sc)
        return 1
    raise Error("T21: unknown task id " + String(tid))


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
    # ---- mode 0: backpressure parks the sender (prefilled cap-1) -------------
    var rt0 = create()
    var buf0 = stack_allocation[1024, Int]()
    var ch0 = make_channel[Int](1)
    if not ch0.try_send(0):
        red("mode0: prefill failed")
    var tx0 = ch0.sender()
    var rx0 = ch0.receiver()
    var sc0 = Scene()
    sc0.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=ch0)
    sc0.tx = tx0
    sc0.rx = rx0
    var scp0 = UnsafePointer[Scene, MutAnyOrigin](to=sc0)
    wire(buf0, scp0)
    buf0[S_MODE] = MODE_BACPRE
    buf0[S_M] = 1
    buf0[S_C_TARGET] = 2
    var ud0 = scp0.bitcast[Byte]()
    var t_p0 = TB.create()
    var t_c0 = TB.create()
    # A2.2 (issue #68): owner pop is LIFO, so register the consumer FIRST
    # and the producer after -> the producer pops first and parks on the
    # full buffer (its first attempt).
    var h_c0 = spawn(rt0, UnsafePointer[TB, MutAnyOrigin](to=t_c0), 0)
    var h_p0 = spawn(rt0, UnsafePointer[TB, MutAnyOrigin](to=t_p0), 0)
    scp0[].p_tcb = UnsafePointer[TB, MutAnyOrigin](to=t_p0)
    scp0[].c_tcb = UnsafePointer[TB, MutAnyOrigin](to=t_c0)
    scp0[].p_id[] = h_p0.id()
    scp0[].c_id[] = h_c0.id()
    var served0 = scheduler_loop(rt0, dispatch, ud0)
    if served0 != 4:
        red("mode0 served " + String(served0) + ", expected 4 slices")
    if not (h_p0.is_completed() and h_c0.is_completed()):
        red("mode0: not both tasks completed")
    if not ch0.is_empty():
        red("mode0: channel not fully drained")
    if ch0.send_waiters_len() != 0 or ch0.recv_waiters_len() != 0:
        red("mode0: leftover waiters")
    if rt0.pending() != 0 or ch0.to_wake_len() != 0:
        red("mode0: leftover runnables/wakes")
    if buf0[EVENTS_BASE + 0] != EV_P_START or buf0[EVENTS_BASE + 1] != EV_P_PARK:
        red("mode0: producer must park on the full buffer as the 2nd event")
    if seq_find(scp0, EV_P_PARK) > seq_find(scp0, EV_C_RECV):
        red("mode0: park must precede any consumption")
    if count_ev(scp0, EV_P_PARK) != 1:
        red("mode0: producer must park exactly once")
    if buf0[S_C_COUNT] != 2 or buf0[S_C_TOTAL] != 1:
        red("mode0: consumer counts wrong (want 2 recvs, total 1)")
    if h_p0.join().v != 1 or h_c0.join().v != 1:
        red("mode0: result values wrong")
    print("mode0 ok")

    # ---- mode 1: receiver parks on empty; sender handoff wakes it ------------
    var rt1 = create()
    var buf1 = stack_allocation[1024, Int]()
    var ch1 = make_channel[Int](1)
    var tx1 = ch1.sender()
    var rx1 = ch1.receiver()
    var sc1 = Scene()
    sc1.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=ch1)
    sc1.tx = tx1
    sc1.rx = rx1
    var scp1 = UnsafePointer[Scene, MutAnyOrigin](to=sc1)
    wire(buf1, scp1)
    buf1[S_MODE] = MODE_RECVPARK
    buf1[S_M] = 1
    buf1[S_C_TARGET] = 1
    var ud1 = scp1.bitcast[Byte]()
    var t_p1 = TB.create()
    var t_c1 = TB.create()
    # A2.2 (issue #68): owner pop is LIFO, so register the producer FIRST
    # and the consumer after -> the consumer pops first and parks on the
    # empty channel before the producer runs.
    var h_p1 = spawn(rt1, UnsafePointer[TB, MutAnyOrigin](to=t_p1), 0)
    var h_c1 = spawn(rt1, UnsafePointer[TB, MutAnyOrigin](to=t_c1), 0)
    scp1[].p_tcb = UnsafePointer[TB, MutAnyOrigin](to=t_p1)
    scp1[].c_tcb = UnsafePointer[TB, MutAnyOrigin](to=t_c1)
    scp1[].p_id[] = h_p1.id()
    scp1[].c_id[] = h_c1.id()
    var served1 = scheduler_loop(rt1, dispatch, ud1)
    if served1 != 3:
        red("mode1 served " + String(served1) + ", expected 3 slices")
    if not (h_p1.is_completed() and h_c1.is_completed()):
        red("mode1: not both tasks completed")
    if buf1[EVENTS_BASE + 0] != EV_C_START or buf1[EVENTS_BASE + 1] != EV_C_PARK:
        red("mode1: consumer must park on the empty channel first")
    if seq_find(scp1, EV_C_PARK) >= seq_find(scp1, EV_P_SENT):
        red("mode1: park after send (lost-wakeup shape)")
    if buf1[S_C_COUNT] != 1 or buf1[S_C_TOTAL] != 1:
        red("mode1: consumer counts wrong")
    if ch1.send_waiters_len() != 0 or ch1.recv_waiters_len() != 0:
        red("mode1: leftover waiters")
    if rt1.pending() != 0 or ch1.to_wake_len() != 0:
        red("mode1: leftover runnables/wakes")
    _ = h_p1.join()
    _ = h_c1.join()
    print("mode1 ok")

    # ---- mode 2: CHAIN — exact park counts over M=3 items (capacity 1) ------
    var rt2 = create()
    var buf2 = stack_allocation[1024, Int]()
    var ch2 = make_channel[Int](1)
    var tx2 = ch2.sender()
    var rx2 = ch2.receiver()
    var sc2 = Scene()
    sc2.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=ch2)
    sc2.tx = tx2
    sc2.rx = rx2
    var scp2 = UnsafePointer[Scene, MutAnyOrigin](to=sc2)
    wire(buf2, scp2)
    buf2[S_MODE] = MODE_CHAIN
    buf2[S_M] = 3
    buf2[S_C_TARGET] = 3
    var ud2 = scp2.bitcast[Byte]()
    var t_p2 = TB.create()
    var t_c2 = TB.create()
    # LIFO owner pop (issue #68): producer registered first, consumer after
    # -> consumer parks on the empty channel first, then the chain pumps.
    var h_p2 = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=t_p2), 0)
    var h_c2 = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=t_c2), 0)
    scp2[].p_tcb = UnsafePointer[TB, MutAnyOrigin](to=t_p2)
    scp2[].c_tcb = UnsafePointer[TB, MutAnyOrigin](to=t_c2)
    scp2[].p_id[] = h_p2.id()
    scp2[].c_id[] = h_c2.id()
    var served2 = scheduler_loop(rt2, dispatch, ud2)
    if served2 < 6:
        red("mode2 served " + String(served2) + ", expected >= 6 slices")
    if not (h_p2.is_completed() and h_c2.is_completed()):
        red("mode2: not both tasks completed")
    if buf2[S_C_TOTAL] != 6:
        red("mode2: consumer total " + String(buf2[S_C_TOTAL]) + " != 6")
    if buf2[S_C_COUNT] != 3:
        red("mode2: consumer did not receive all 3 items")
    if count_ev(scp2, EV_P_PARK) != 2:
        red("mode2: producer parks != 2")
    if count_ev(scp2, EV_C_PARK) != 3:
        red("mode2: consumer parks != 3")
    if ch2.send_waiters_len() != 0 or ch2.recv_waiters_len() != 0:
        red("mode2: leftover waiters after chain")
    if rt2.pending() != 0 or ch2.to_wake_len() != 0:
        red("mode2: leftover runnables/wakes")
    _ = h_p2.join()
    _ = h_c2.join()
    print("mode2 ok")

    # ---- mode 3: PINGPONG — M=64 items, capacity 1, one scheduler drive ------
    var rt3 = create()
    var buf3 = stack_allocation[1024, Int]()
    var ch3 = make_channel[Int](1)
    var tx3 = ch3.sender()
    var rx3 = ch3.receiver()
    var sc3 = Scene()
    sc3.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=ch3)
    sc3.tx = tx3
    sc3.rx = rx3
    var scp3 = UnsafePointer[Scene, MutAnyOrigin](to=sc3)
    wire(buf3, scp3)
    buf3[S_MODE] = MODE_PINGPONG
    buf3[S_M] = 64
    buf3[S_C_TARGET] = 64
    var ud3 = scp3.bitcast[Byte]()
    var t_p3 = TB.create()
    var t_c3 = TB.create()
    # LIFO owner pop (issue #68): producer registered first, consumer after
    # -> consumer parks on the empty channel first, then the ping-pong.
    var h_p3 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t_p3), 0)
    var h_c3 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t_c3), 0)
    scp3[].p_tcb = UnsafePointer[TB, MutAnyOrigin](to=t_p3)
    scp3[].c_tcb = UnsafePointer[TB, MutAnyOrigin](to=t_c3)
    scp3[].p_id[] = h_p3.id()
    scp3[].c_id[] = h_c3.id()
    var served3 = scheduler_loop(rt3, dispatch, ud3)
    if served3 < 128:
        red("mode3 served " + String(served3) + " slices, expected >= 128")
    if not (h_p3.is_completed() and h_c3.is_completed()):
        red("mode3: not both tasks completed after ping-pong")
    if buf3[S_C_TOTAL] != 2080:
        red("mode3: total " + String(buf3[S_C_TOTAL]) + " != 2080")
    if count_ev(scp3, EV_P_SENT) != 64 or count_ev(scp3, EV_C_RECV) != 64:
        red("mode3: not every item sent and received")
    if ch3.send_waiters_len() != 0 or ch3.recv_waiters_len() != 0:
        red("mode3: leftover waiters")
    if rt3.pending() != 0 or ch3.to_wake_len() != 0:
        red("mode3: leftovers (pending/to_wake)")
    var r_p = h_p3.join()
    var r_c = h_c3.join()
    if r_p.v != 64 or r_c.v != 2080:
        red("mode3: result values wrong")
    print("mode3 ok")

    print("T21 channel park: PASS")