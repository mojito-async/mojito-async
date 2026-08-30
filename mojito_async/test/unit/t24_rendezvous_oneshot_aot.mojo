# mojito_async/test/unit/t24_rendezvous_oneshot.mojo
#
# A5.1 rendezvous + oneshot channels (issue #89) — acceptance.
#
# Cases:
#   R1  try_send/try_recv never buffer: both fail on a fresh channel with
#       nobody parked, and never register a waiter.
#   R2  try_send matches an already-parked receiver (Case A, non-blocking):
#       direct deposit into `_slot`, receiver woken, resumes with the value.
#   R3  try_recv matches an already-parked sender (Case B, non-blocking):
#       the parked sender's item is returned directly; its own resumed
#       send() short-circuits to success WITHOUT re-matching/re-parking.
#   R4  full blocking SPSC, receiver parks first (Case A): one
#       scheduler_loop drive fully drains the handoff, exactly once.
#   R5  full blocking SPSC, sender parks first (Case B): symmetric to R4.
#   R6  slot-guard + _try_advance: two receivers park; a non-blocking send
#       matches the OLDEST and fills `_slot`; a second send while `_slot`
#       is still occupied must NOT clobber it; a blocking sender queues
#       instead; once the first handoff is collected, `_try_advance`
#       immediately pairs the still-queued sender with the still-parked
#       second receiver (no lost wakeup / deadlock).
#   R7  close-last-sender wakes a parked receiver to observe close (None).
#   R8  close-last-receiver wakes a parked sender; its resumed send RAISES
#       "ChannelError: send on closed channel".
#   O1  Oneshot try_send/try_recv: single delivery, non-blocking.
#   O2  Oneshot: a second send() raises "already sent".
#   O3  Oneshot: receiver-dropped makes send() raise "receiver dropped".
#   O4  Oneshot: blocking recv() parks, then send() delivers; the resumed
#       receiver returns the value.
#   O5  Oneshot: blocking recv() parks; sender-drop (close, no value ever
#       sent) wakes it to observe close (None).
#
# Every scenario ends with ZERO leftovers: no waiters, no pending
# runnables, no deferred wakes, no unconsumed outcome markers.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from mojito_async.channel import (
    Oneshot,
    OneshotReceiver,
    OneshotSender,
    RecvOutcome,
    RendezvousChannel,
    RendezvousReceiver,
    RendezvousSender,
    SendOutcome,
    make_oneshot,
    make_rendezvous,
)
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T24 rendezvous/oneshot: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _complete_ok(h: JoinHandle[IntResult], res: Int) raises:
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    h.tcb()[].mark_result(IntResult(res))


# ---------------------------------------------------------------------------
# PairScene — one rendezvous sender-task + one receiver-task (R2..R8)
# ---------------------------------------------------------------------------

struct PairScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan: UnsafePointer[RendezvousChannel[Int], MutAnyOrigin]
    var tx: RendezvousSender[Int]
    var rx: RendezvousReceiver[Int]
    var r_id: Int
    var s_id: Int
    var s_item: Int
    var r_parked: Bool
    var s_parked: Bool

    def __init__(out self):
        self.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.tx = RendezvousSender[Int](
            UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](unsafe_from_address=1)
        )
        self.rx = RendezvousReceiver[Int](
            UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](unsafe_from_address=1)
        )
        self.r_id = 0
        self.s_id = 0
        self.s_item = 0
        self.r_parked = False
        self.s_parked = False


def pair_receiver_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[PairScene, MutAnyOrigin]
) raises:
    claim_running(h)
    var v = sc[].rx.recv(rt, h)
    if v.is_parked():
        sc[].r_parked = True
        return
    if v.is_value():
        _complete_ok(h, v.value())
        return
    _complete_ok(h, -1)  # closed-and-unset observed


def pair_sender_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[PairScene, MutAnyOrigin]
) raises:
    claim_running(h)
    try:
        var outcome = sc[].tx.send(rt, h, sc[].s_item)
        if outcome.is_parked():
            sc[].s_parked = True
            return
    except e:
        _complete_ok(h, -1)  # raised: closed
        return
    _complete_ok(h, sc[].s_item)


def pair_drain(mut rt: Runtime, sc: UnsafePointer[PairScene, MutAnyOrigin]) raises:
    while sc[].chan[].to_wake_len() > 0:
        var wr = sc[].chan[].pop_to_wake()
        if wr.task_id == 0:
            break
        var hp = _handle(wr.tcb_addr, wr.task_id)
        unpark_current(rt, hp)


def pair_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[PairScene]()
    var h = _handle(tcb_addr, tid)
    if tid == sc[].r_id:
        pair_receiver_slice(rt, h, sc)
        pair_drain(rt, sc)
        return 1
    if tid == sc[].s_id:
        pair_sender_slice(rt, h, sc)
        pair_drain(rt, sc)
        return 1
    raise Error("T24: unknown task id " + String(tid))


def pair_zero_leftovers(chan: UnsafePointer[RendezvousChannel[Int], MutAnyOrigin]) -> Bool:
    return (
        chan[].send_waiters_len() == 0
        and chan[].recv_waiters_len() == 0
        and chan[].to_wake_len() == 0
        and chan[].send_done_len() == 0
        and chan[].send_failed_len() == 0
        and not chan[].is_slot_filled()
    )


# ---------------------------------------------------------------------------
# AdvScene — two receiver-tasks + one sender-task (R6, _try_advance)
# ---------------------------------------------------------------------------

struct AdvScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan: UnsafePointer[RendezvousChannel[Int], MutAnyOrigin]
    var tx: RendezvousSender[Int]
    var rx1: RendezvousReceiver[Int]
    var rx2: RendezvousReceiver[Int]
    var r1_id: Int
    var r2_id: Int
    var s_id: Int
    var s_item: Int

    def __init__(out self):
        self.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.tx = RendezvousSender[Int](
            UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](unsafe_from_address=1)
        )
        self.rx1 = RendezvousReceiver[Int](
            UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](unsafe_from_address=1)
        )
        self.rx2 = RendezvousReceiver[Int](
            UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](unsafe_from_address=1)
        )
        self.r1_id = 0
        self.r2_id = 0
        self.s_id = 0
        self.s_item = 0


def adv_r1_slice(mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[AdvScene, MutAnyOrigin]) raises:
    claim_running(h)
    var v = sc[].rx1.recv(rt, h)
    if v.is_parked():
        return
    if v.is_value():
        _complete_ok(h, v.value())
        return
    _complete_ok(h, -1)


def adv_r2_slice(mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[AdvScene, MutAnyOrigin]) raises:
    claim_running(h)
    var v = sc[].rx2.recv(rt, h)
    if v.is_parked():
        return
    if v.is_value():
        _complete_ok(h, v.value())
        return
    _complete_ok(h, -1)


def adv_s_slice(mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[AdvScene, MutAnyOrigin]) raises:
    claim_running(h)
    try:
        var outcome = sc[].tx.send(rt, h, sc[].s_item)
        if outcome.is_parked():
            return
    except e:
        _complete_ok(h, -1)
        return
    _complete_ok(h, sc[].s_item)


def adv_drain(mut rt: Runtime, sc: UnsafePointer[AdvScene, MutAnyOrigin]) raises:
    while sc[].chan[].to_wake_len() > 0:
        var wr = sc[].chan[].pop_to_wake()
        if wr.task_id == 0:
            break
        var hp = _handle(wr.tcb_addr, wr.task_id)
        unpark_current(rt, hp)


def adv_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[AdvScene]()
    var h = _handle(tcb_addr, tid)
    if tid == sc[].r1_id:
        adv_r1_slice(rt, h, sc)
        adv_drain(rt, sc)
        return 1
    if tid == sc[].r2_id:
        adv_r2_slice(rt, h, sc)
        adv_drain(rt, sc)
        return 1
    if tid == sc[].s_id:
        adv_s_slice(rt, h, sc)
        adv_drain(rt, sc)
        return 1
    raise Error("T24: unknown adv task id " + String(tid))


# ---------------------------------------------------------------------------
# OScene — one oneshot receiver-task (O4, O5); send() never parks so the
# sender side is driven directly from main(), no task needed for it.
# ---------------------------------------------------------------------------

struct OScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan: UnsafePointer[Oneshot[Int], MutAnyOrigin]
    var rx: OneshotReceiver[Int]
    var r_id: Int

    def __init__(out self):
        self.chan = UnsafePointer[Oneshot[Int], MutAnyOrigin](unsafe_from_address=1)
        self.rx = OneshotReceiver[Int](
            UnsafePointer[Oneshot[Int], MutAnyOrigin](unsafe_from_address=1)
        )
        self.r_id = 0


def o_receiver_slice(mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[OScene, MutAnyOrigin]) raises:
    claim_running(h)
    var v = sc[].rx.recv(rt, h)
    if v.is_parked():
        return
    if v.is_value():
        _complete_ok(h, v.value())
        return
    _complete_ok(h, -1)


def o_drain(mut rt: Runtime, sc: UnsafePointer[OScene, MutAnyOrigin]) raises:
    while sc[].chan[].to_wake_len() > 0:
        var wr = sc[].chan[].pop_to_wake()
        if wr.task_id == 0:
            break
        var hp = _handle(wr.tcb_addr, wr.task_id)
        unpark_current(rt, hp)


def o_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[OScene]()
    var h = _handle(tcb_addr, tid)
    if tid == sc[].r_id:
        o_receiver_slice(rt, h, sc)
        o_drain(rt, sc)
        return 1
    raise Error("T24: unknown oneshot task id " + String(tid))


def main() raises:
    # ==== R1: try_send/try_recv never buffer =================================
    var rc1 = make_rendezvous[Int]()
    var v1 = rc1.try_recv()
    if v1:
        red("R1: try_recv on a fresh channel must return None")
    if rc1.try_send(1):
        red("R1: try_send with no parked receiver must return False (no buffering)")
    if rc1.send_waiters_len() != 0 or rc1.recv_waiters_len() != 0:
        red("R1: try_* must never register a waiter")
    print("R1 ok")

    # ==== R2: try_send matches an already-parked receiver (Case A) ===========
    var rt2 = create()
    var chan2 = make_rendezvous[Int]()
    var sc2 = PairScene()
    sc2.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](to=chan2)
    sc2.rx = chan2.receiver()
    var scp2 = UnsafePointer[PairScene, MutAnyOrigin](to=sc2)
    var ud2 = scp2.bitcast[Byte]()
    var t_r2 = TB.create()
    var h_r2 = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=t_r2), 0)
    scp2[].r_id = h_r2.id()
    var served2a = scheduler_loop(rt2, pair_dispatch, ud2)
    if served2a != 1:
        red("R2: first drive served " + String(served2a) + ", expected 1 (receiver parks)")
    if not sc2.r_parked or h_r2.state() != TaskControlBlock.WAITING:
        red("R2: receiver must park on an empty rendezvous")
    if chan2.recv_waiters_len() != 1:
        red("R2: receiver must be registered as a waiter")
    var tx2 = chan2.sender()
    if not tx2.try_send(42):
        red("R2: try_send must match the already-parked receiver")
    if chan2.recv_waiters_len() != 0 or chan2.to_wake_len() != 1 or not chan2.is_slot_filled():
        red("R2: try_send must deposit into _slot and wake the receiver")
    pair_drain(rt2, scp2)
    var served2b = scheduler_loop(rt2, pair_dispatch, ud2)
    if served2b != 1:
        red("R2: second drive served " + String(served2b) + ", expected 1 (receiver resumes)")
    if not h_r2.is_completed() or h_r2.join().v != 42:
        red("R2: receiver must resume with the deposited value")
    if not pair_zero_leftovers(scp2[].chan):
        red("R2: leftovers after the handoff")
    print("R2 ok")

    # ==== R3: try_recv matches an already-parked sender (Case B) =============
    var rt3 = create()
    var chan3 = make_rendezvous[Int]()
    var sc3 = PairScene()
    sc3.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](to=chan3)
    sc3.tx = chan3.sender()
    sc3.s_item = 7
    var scp3 = UnsafePointer[PairScene, MutAnyOrigin](to=sc3)
    var ud3 = scp3.bitcast[Byte]()
    var t_s3 = TB.create()
    var h_s3 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t_s3), 0)
    scp3[].s_id = h_s3.id()
    var served3a = scheduler_loop(rt3, pair_dispatch, ud3)
    if served3a != 1:
        red("R3: first drive served " + String(served3a) + ", expected 1 (sender parks)")
    if not sc3.s_parked or h_s3.state() != TaskControlBlock.WAITING:
        red("R3: sender must park with no receiver ready")
    if chan3.send_waiters_len() != 1 or chan3.is_slot_filled():
        red("R3: sender must queue its item inline, not through _slot")
    var rx3 = chan3.receiver()
    var got3 = rx3.try_recv()
    if not got3 or got3.value() != 7:
        red("R3: try_recv must match the already-parked sender's item directly")
    if chan3.send_waiters_len() != 0 or chan3.send_done_len() != 1 or chan3.to_wake_len() != 1:
        red("R3: match must mark the sender delivered and wake it")
    pair_drain(rt3, scp3)
    var served3b = scheduler_loop(rt3, pair_dispatch, ud3)
    if served3b != 1:
        red("R3: second drive served " + String(served3b) + ", expected 1 (sender resumes)")
    if not h_s3.is_completed() or h_s3.join().v != 7:
        red("R3: resumed sender must short-circuit to success WITHOUT re-matching")
    if not pair_zero_leftovers(scp3[].chan):
        red("R3: leftovers after the handoff")
    print("R3 ok")

    # ==== R4: full blocking SPSC, receiver parks first (Case A) ==============
    var rt4 = create()
    var chan4 = make_rendezvous[Int]()
    var sc4 = PairScene()
    sc4.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](to=chan4)
    sc4.tx = chan4.sender()
    sc4.rx = chan4.receiver()
    sc4.s_item = 99
    var scp4 = UnsafePointer[PairScene, MutAnyOrigin](to=sc4)
    var ud4 = scp4.bitcast[Byte]()
    var t_s4 = TB.create()
    var t_r4 = TB.create()
    # LIFO owner pop (issue #68): register the SENDER first and the
    # RECEIVER after -> the receiver pops first and parks before the
    # sender's Case-A match runs.
    var h_s4 = spawn(rt4, UnsafePointer[TB, MutAnyOrigin](to=t_s4), 0)
    var h_r4 = spawn(rt4, UnsafePointer[TB, MutAnyOrigin](to=t_r4), 0)
    scp4[].s_id = h_s4.id()
    scp4[].r_id = h_r4.id()
    var served4 = scheduler_loop(rt4, pair_dispatch, ud4)
    if served4 != 3:
        red("R4 served " + String(served4) + ", expected 3 (r park, s match, r resume)")
    if not sc4.r_parked:
        red("R4: receiver must have parked first")
    if sc4.s_parked:
        red("R4: sender must match directly (Case A), never park")
    if not (h_s4.is_completed() and h_r4.is_completed()):
        red("R4: both tasks must complete")
    if h_s4.join().v != 99 or h_r4.join().v != 99:
        red("R4: value must transit exactly once, unmodified")
    if rt4.pending() != 0:
        red("R4: leftover runnables")
    if not pair_zero_leftovers(scp4[].chan):
        red("R4: leftovers after the handoff")
    print("R4 ok")

    # ==== R5: full blocking SPSC, sender parks first (Case B) ================
    var rt5 = create()
    var chan5 = make_rendezvous[Int]()
    var sc5 = PairScene()
    sc5.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](to=chan5)
    sc5.tx = chan5.sender()
    sc5.rx = chan5.receiver()
    sc5.s_item = 55
    var scp5 = UnsafePointer[PairScene, MutAnyOrigin](to=sc5)
    var ud5 = scp5.bitcast[Byte]()
    var t_r5 = TB.create()
    var t_s5 = TB.create()
    # LIFO owner pop: register the RECEIVER first and the SENDER after ->
    # the sender pops first and parks (queues its item) before the
    # receiver's Case-B match runs.
    var h_r5 = spawn(rt5, UnsafePointer[TB, MutAnyOrigin](to=t_r5), 0)
    var h_s5 = spawn(rt5, UnsafePointer[TB, MutAnyOrigin](to=t_s5), 0)
    scp5[].r_id = h_r5.id()
    scp5[].s_id = h_s5.id()
    var served5 = scheduler_loop(rt5, pair_dispatch, ud5)
    if served5 != 3:
        red("R5 served " + String(served5) + ", expected 3 (s park, r match, s resume)")
    if not sc5.s_parked:
        red("R5: sender must have parked first")
    if sc5.r_parked:
        red("R5: receiver must match directly (Case B), never park")
    if not (h_s5.is_completed() and h_r5.is_completed()):
        red("R5: both tasks must complete")
    if h_s5.join().v != 55 or h_r5.join().v != 55:
        red("R5: value must transit exactly once, unmodified")
    if rt5.pending() != 0:
        red("R5: leftover runnables")
    if not pair_zero_leftovers(scp5[].chan):
        red("R5: leftovers after the handoff")
    print("R5 ok")

    # ==== R6: slot-guard + _try_advance =======================================
    var rt6 = create()
    var chan6 = make_rendezvous[Int]()
    var sc6 = AdvScene()
    sc6.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](to=chan6)
    sc6.tx = chan6.sender()
    sc6.rx1 = chan6.receiver()
    sc6.rx2 = chan6.receiver()
    sc6.s_item = 200
    var scp6 = UnsafePointer[AdvScene, MutAnyOrigin](to=sc6)
    var ud6 = scp6.bitcast[Byte]()
    var t_r6a = TB.create()
    var t_r6b = TB.create()
    # LIFO owner pop: register r2 first, r1 after -> r1 pops (and parks)
    # first, then r2 parks second -> FIFO order in _recv_waiters is [r1, r2].
    var h_r6b = spawn(rt6, UnsafePointer[TB, MutAnyOrigin](to=t_r6b), 0)
    var h_r6a = spawn(rt6, UnsafePointer[TB, MutAnyOrigin](to=t_r6a), 0)
    scp6[].r2_id = h_r6b.id()
    scp6[].r1_id = h_r6a.id()
    var served6a = scheduler_loop(rt6, adv_dispatch, ud6)
    if served6a != 2:
        red("R6: first drive served " + String(served6a) + ", expected 2 (both receivers park)")
    if chan6.recv_waiters_len() != 2:
        red("R6: both receivers must be queued")
    if not chan6.try_send(100):
        red("R6: first send must match the OLDEST parked receiver")
    if chan6.recv_waiters_len() != 1 or not chan6.is_slot_filled():
        red("R6: exactly one receiver must remain queued; _slot must hold 100")
    if chan6.try_send(999):
        red("R6: a second send while _slot is occupied must NOT clobber it")
    if not chan6.is_slot_filled():
        red("R6: the in-flight handoff must survive a rejected second send")
    var t_s6 = TB.create()
    var h_s6 = spawn(rt6, UnsafePointer[TB, MutAnyOrigin](to=t_s6), 0)
    scp6[].s_id = h_s6.id()
    # One drive now resolves the WHOLE cascade: the producer's own slice
    # ends by draining the still-undrained r1 wake (from the try_send(100)
    # above) in the SAME dispatch cycle, so r1's resume — and the
    # `_try_advance` it triggers when it collects `_slot` — happens before
    # this scheduler_loop call returns: producer parks (slot busy) [1],
    # r1 resumes and advances (matches producer<->r2) [2], producer
    # resumes and short-circuits to success [3], r2 resumes and collects
    # the advanced value [4].
    var served6b = scheduler_loop(rt6, adv_dispatch, ud6)
    if served6b != 4:
        red("R6: drive served " + String(served6b) + ", expected 4 (s park, r1 resume+advance, s resume, r2 resume)")
    if not (h_r6a.is_completed() and h_r6b.is_completed() and h_s6.is_completed()):
        red("R6: all three tasks must complete")
    if h_r6a.join().v != 100:
        red("R6: r1 must receive the FIRST deposited value")
    if h_s6.join().v != 200:
        red("R6: producer must complete with its own item")
    if h_r6b.join().v != 200:
        red("R6: r2 must receive the SECOND (advanced) value, not lost")
    if rt6.pending() != 0:
        red("R6: leftover runnables")
    if not pair_zero_leftovers(scp6[].chan):
        red("R6: leftovers after the multi-pair advance")
    print("R6 ok")

    # ==== R7: close-last-sender wakes a parked receiver to observe close =====
    var rt7 = create()
    var chan7 = make_rendezvous[Int]()
    var tx7 = chan7.sender()
    var sc7 = PairScene()
    sc7.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](to=chan7)
    sc7.rx = chan7.receiver()
    var scp7 = UnsafePointer[PairScene, MutAnyOrigin](to=sc7)
    var ud7 = scp7.bitcast[Byte]()
    var t_r7 = TB.create()
    var h_r7 = spawn(rt7, UnsafePointer[TB, MutAnyOrigin](to=t_r7), 0)
    scp7[].r_id = h_r7.id()
    var served7a = scheduler_loop(rt7, pair_dispatch, ud7)
    if served7a != 1 or not sc7.r_parked:
        red("R7: receiver must park on an empty, open rendezvous")
    tx7.close()
    if not chan7.is_send_closed():
        red("R7: send side must close on last sender close")
    if chan7.recv_waiters_len() != 0 or chan7.to_wake_len() != 1:
        red("R7: close must move the parked receiver to the wake list")
    pair_drain(rt7, scp7)
    var served7b = scheduler_loop(rt7, pair_dispatch, ud7)
    if served7b != 1:
        red("R7: receiver must resume exactly once")
    if not h_r7.is_completed() or h_r7.join().v != -1:
        red("R7: resumed receiver must observe close (None), not a value")
    if not pair_zero_leftovers(scp7[].chan):
        red("R7: leftovers after close")
    print("R7 ok")

    # ==== R8: close-last-receiver wakes a parked sender; resumed send RAISES =
    var rt8 = create()
    var chan8 = make_rendezvous[Int]()
    var rx8 = chan8.receiver()
    var sc8 = PairScene()
    sc8.chan = UnsafePointer[RendezvousChannel[Int], MutAnyOrigin](to=chan8)
    sc8.tx = chan8.sender()
    sc8.s_item = 33
    var scp8 = UnsafePointer[PairScene, MutAnyOrigin](to=sc8)
    var ud8 = scp8.bitcast[Byte]()
    var t_s8 = TB.create()
    var h_s8 = spawn(rt8, UnsafePointer[TB, MutAnyOrigin](to=t_s8), 0)
    scp8[].s_id = h_s8.id()
    var served8a = scheduler_loop(rt8, pair_dispatch, ud8)
    if served8a != 1 or not sc8.s_parked:
        red("R8: sender must park with no receiver ready")
    rx8.close()
    if not chan8.is_recv_closed():
        red("R8: receive side must close on last receiver close")
    if chan8.send_waiters_len() != 0 or chan8.to_wake_len() != 1 or chan8.send_failed_len() != 1:
        red("R8: close must fail the parked sender (moved + marked failed)")
    pair_drain(rt8, scp8)
    var served8b = scheduler_loop(rt8, pair_dispatch, ud8)
    if served8b != 1:
        red("R8: sender must resume exactly once")
    if not h_s8.is_completed() or h_s8.join().v != -1:
        red("R8: resumed send() must RAISE (caught -> -1 marker), not succeed")
    if not pair_zero_leftovers(scp8[].chan):
        red("R8: leftovers after close")
    print("R8 ok")

    # ==== O1: Oneshot try_send/try_recv, single delivery =====================
    var os1 = make_oneshot[Int]()
    if os1.is_filled() or os1.is_closed():
        red("O1: fresh oneshot must be empty and open")
    var nothing1 = os1.try_recv()
    if nothing1:
        red("O1: try_recv before any send must return None")
    var tx1o = os1.sender()
    if not tx1o.try_send(11):
        red("O1: first try_send must succeed")
    if not os1.is_filled() or not os1.is_send_closed():
        red("O1: a successful send fills the slot and closes the send side")
    if tx1o.try_send(12):
        red("O1: a second try_send must fail (already sent)")
    var rx1o = os1.receiver()
    var got1o = rx1o.try_recv()
    if not got1o or got1o.value() != 11:
        red("O1: try_recv must deliver the sent value exactly once")
    if os1.is_filled():
        red("O1: the slot must be empty after collection")
    if not os1.is_recv_closed():
        red("O1: collecting the value closes the receive side")
    var again1o = rx1o.try_recv()
    if again1o:
        red("O1: a second try_recv must return None")
    print("O1 ok")

    # ==== O2: a second send() raises "already sent" ===========================
    var rt2o = create()
    var os2 = make_oneshot[Int]()
    var tx2o = os2.sender()
    var t_h2o = TB.create()
    var h2o = spawn(rt2o, UnsafePointer[TB, MutAnyOrigin](to=t_h2o), 0)
    claim_running(h2o)
    _ = tx2o.send(rt2o, h2o, 21)  # h2o is unused by Oneshot.send() (never parks)
    if not os2.is_filled():
        red("O2: first send() must fill the slot")
    var raised2 = False
    try:
        _ = tx2o.send(rt2o, h2o, 22)
    except e:
        raised2 = "already sent" in String(e)
    if not raised2:
        red("O2: a second send() must raise 'already sent'")
    print("O2 ok")

    # ==== O3: receiver-dropped makes send() raise "receiver dropped" =========
    var rt3o = create()
    var os3 = make_oneshot[Int]()
    var tx3o = os3.sender()
    var rx3o = os3.receiver()
    rx3o.close()
    if not os3.is_recv_closed():
        red("O3: closing the last receiver must close the receive side")
    var t_h3o = TB.create()
    var h3o = spawn(rt3o, UnsafePointer[TB, MutAnyOrigin](to=t_h3o), 0)
    claim_running(h3o)
    var raised3 = False
    try:
        _ = tx3o.send(rt3o, h3o, 31)
    except e:
        raised3 = "receiver dropped" in String(e)
    if not raised3:
        red("O3: send() on a receiver-dropped oneshot must raise 'receiver dropped'")
    if os3.is_filled():
        red("O3: the failed send must not fill the slot")
    print("O3 ok")

    # ==== O4: blocking recv() parks, then send() delivers =====================
    var rt4o = create()
    var os4 = make_oneshot[Int]()
    var sc4o = OScene()
    sc4o.chan = UnsafePointer[Oneshot[Int], MutAnyOrigin](to=os4)
    sc4o.rx = os4.receiver()
    var scp4o = UnsafePointer[OScene, MutAnyOrigin](to=sc4o)
    var ud4o = scp4o.bitcast[Byte]()
    var t_r4o = TB.create()
    var h_r4o = spawn(rt4o, UnsafePointer[TB, MutAnyOrigin](to=t_r4o), 0)
    scp4o[].r_id = h_r4o.id()
    var served4oa = scheduler_loop(rt4o, o_dispatch, ud4o)
    if served4oa != 1 or h_r4o.state() != TaskControlBlock.WAITING:
        red("O4: receiver must park on an unfilled oneshot")
    if os4.recv_waiters_len() != 1:
        red("O4: receiver must be registered as a waiter")
    var tx4o = os4.sender()
    _ = tx4o.send(rt4o, h_r4o, 77)  # h_r4o unused by send(); never parks
    if not os4.is_filled() or os4.recv_waiters_len() != 0 or os4.to_wake_len() != 1:
        red("O4: send() must fill the slot and wake the parked receiver")
    o_drain(rt4o, scp4o)
    var served4ob = scheduler_loop(rt4o, o_dispatch, ud4o)
    if served4ob != 1:
        red("O4: receiver must resume exactly once")
    if not h_r4o.is_completed() or h_r4o.join().v != 77:
        red("O4: resumed receiver must collect the sent value")
    if os4.recv_waiters_len() != 0 or os4.to_wake_len() != 0:
        red("O4: leftovers after delivery")
    print("O4 ok")

    # ==== O5: sender-drop (no send) wakes a parked receiver to observe close =
    var rt5o = create()
    var os5 = make_oneshot[Int]()
    var tx5o = os5.sender()
    var sc5o = OScene()
    sc5o.chan = UnsafePointer[Oneshot[Int], MutAnyOrigin](to=os5)
    sc5o.rx = os5.receiver()
    var scp5o = UnsafePointer[OScene, MutAnyOrigin](to=sc5o)
    var ud5o = scp5o.bitcast[Byte]()
    var t_r5o = TB.create()
    var h_r5o = spawn(rt5o, UnsafePointer[TB, MutAnyOrigin](to=t_r5o), 0)
    scp5o[].r_id = h_r5o.id()
    var served5oa = scheduler_loop(rt5o, o_dispatch, ud5o)
    if served5oa != 1 or h_r5o.state() != TaskControlBlock.WAITING:
        red("O5: receiver must park on an unfilled oneshot")
    tx5o.close()
    if not os5.is_send_closed():
        red("O5: closing the last sender without a send must close the send side")
    if os5.recv_waiters_len() != 0 or os5.to_wake_len() != 1:
        red("O5: sender-drop must wake the parked receiver")
    o_drain(rt5o, scp5o)
    var served5ob = scheduler_loop(rt5o, o_dispatch, ud5o)
    if served5ob != 1:
        red("O5: receiver must resume exactly once")
    if not h_r5o.is_completed() or h_r5o.join().v != -1:
        red("O5: resumed receiver must observe close (None), not a value")
    if os5.recv_waiters_len() != 0 or os5.to_wake_len() != 0:
        red("O5: leftovers after sender-drop close")
    print("O5 ok")

    print("T24 rendezvous/oneshot: PASS")
