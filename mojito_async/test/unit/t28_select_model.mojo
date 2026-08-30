# mojito_async/test/unit/t28_select_model.mojo
#
# A5.6 select model/race acceptance battery (issue #94) — deterministic
# schedule-model driver (spec §74.4 "select winner"): enumerates the four
# abstract interleavings a select() over channel + deadline branches can
# resolve to, driving the single cooperative worker slice-by-slice with an
# EXPLICIT arrival order (the instrumentable-scheduling-point substitute a
# single deterministic worker gives us — spec §74.1's "waiter publication /
# waiter claim" points ARE the register-then-park / rescan-then-claim steps
# below, driven by hand instead of by a generic explorer).
#
#   M1  data-A-arrives-first  — two RECV branches, A's producer fires first.
#   M2  data-B-arrives-first  — mirror of M1, B's producer fires first.
#   M3  timeout-first         — RECV + DEADLINE branches, the deadline
#                               elapses before any producer fires.
#   M4  close-first           — two RECV branches, B's last sender closes
#                               before either channel ever gets data.
#
# Each scenario asserts the "select winner" subject in full: exactly ONE
# winner, the LOSING branch is logically cancelled (unregistered), no
# STALE wake reaches the (already-completed) selector afterward, and every
# invariant table entry (heap/wait-queue/wake/runnable) reads ZERO once the
# scenario settles.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from std.memory import stack_allocation
from mojito_async.channel import Channel, make_channel
from mojito_async.channel.select import (
    SelectBranch,
    SelectState,
    deadline_branch,
    recv_branch,
    select,
)
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Deadline
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.time.timer_service import service_timers


def red(what: String) raises -> None:
    print("T28 select model: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _complete(h: JoinHandle[IntResult], res: Int) raises:
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    h.tcb()[].mark_result(IntResult(res))


# ---------------------------------------------------------------------------
# Scene — one selector task over TWO channel branches (RECV/RECV, matching
# M1/M2/M4) plus an optional DEADLINE branch used for M3 alone.
# ---------------------------------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan_0: UnsafePointer[Channel[Int], MutAnyOrigin]
    var chan_1: UnsafePointer[Channel[Int], MutAnyOrigin]
    var branches: UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin]
    var state: UnsafePointer[SelectState, MutAnyOrigin]
    var sel_id: Int
    var run_count: UnsafePointer[Int, MutAnyOrigin]  # body-entry count (stale-wake guard)
    var winner: UnsafePointer[Int, MutAnyOrigin]
    var timed_out: UnsafePointer[Int, MutAnyOrigin]
    var closed: UnsafePointer[Int, MutAnyOrigin]
    var value: UnsafePointer[Int, MutAnyOrigin]
    var now_ticks: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](unsafe_from_address=1)
        self.state = UnsafePointer[SelectState, MutAnyOrigin](unsafe_from_address=1)
        self.sel_id = 0
        self.run_count = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.winner = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.timed_out = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.closed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.value = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.now_ticks = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def selector_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    claim_running(h)
    sc[].run_count[] = sc[].run_count[] + 1
    var out = select[Int, IntResult](
        rt, h, sc[].branches[], sc[].state[], sc[].now_ticks[]
    )
    if h.state() == TaskControlBlock.WAITING:
        return  # parked; the embedding driver re-enters on resume
    sc[].winner[] = out.index
    sc[].timed_out[] = 1 if out.is_timeout() else 0
    sc[].closed[] = 1 if out.is_closed() else 0
    if out.has_value():
        sc[].value[] = out.recv_value()
    else:
        sc[].value[] = -999
    _complete(h, out.index)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid != sc[].sel_id:
        raise Error("T28: unknown task id " + String(tid))
    var h = _handle(tcb_addr, tid)
    selector_slice(rt, h, sc)
    while sc[].chan_0[].to_wake_len() > 0:
        var wr = sc[].chan_0[].pop_to_wake()
        if wr.task_id == 0:
            break
        unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))
    while sc[].chan_1[].to_wake_len() > 0:
        var wr = sc[].chan_1[].pop_to_wake()
        if wr.task_id == 0:
            break
        unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))
    return 1


def check_invariants(
    label: String,
    winner: Int,
    want_winner: Int,
    run_count: Int,
    chan_0: UnsafePointer[Channel[Int], MutAnyOrigin],
    chan_1: UnsafePointer[Channel[Int], MutAnyOrigin],
    heap: UnsafePointer[TimerHeap, MutAnyOrigin],
    rt: UnsafePointer[Runtime, MutAnyOrigin],
) raises:
    """The 'select winner' invariant table (spec §74.4): exactly one winner,
    zero leftover registrations/wakes/heap entries/runnables, exactly one
    body completion (no stale re-wake)."""
    if winner != want_winner:
        red(label + ": expected winner " + String(want_winner) + ", got " + String(winner))
    if run_count != 2:
        red(label + ": selector body must run exactly twice (park + resume), got "
            + String(run_count))
    if chan_0[].recv_waiters_len() != 0 or chan_1[].recv_waiters_len() != 0:
        red(label + ": leftover channel waiters")
    if chan_0[].to_wake_len() != 0 or chan_1[].to_wake_len() != 0:
        red(label + ": leftover deferred wakes")
    if not heap[].is_empty():
        red(label + ": leftover heap entries")
    if rt[].pending() != 0:
        red(label + ": leftover pending runnables")


def main() raises:
    var heap_unused = TimerHeap()
    var hp_unused = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap_unused)

    # ---- M1: data-A-arrives-first ------------------------------------------
    var rt1 = create()
    var chan0_1 = make_channel[Int](2)
    var chan1_1 = make_channel[Int](2)
    var branches1 = List[SelectBranch[Int]]()
    branches1.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_1)))
    branches1.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_1)))
    var state1 = SelectState()
    var sc1 = Scene()
    sc1.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_1)
    sc1.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_1)
    sc1.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=branches1)
    sc1.state = UnsafePointer[SelectState, MutAnyOrigin](to=state1)
    var run1 = Int(0)
    var winner1 = Int(-1)
    var timed_out1 = Int(0)
    var closed1 = Int(0)
    var value1 = Int(-1)
    var now1 = Int(0)
    sc1.run_count = UnsafePointer[Int, MutAnyOrigin](to=run1)
    sc1.winner = UnsafePointer[Int, MutAnyOrigin](to=winner1)
    sc1.timed_out = UnsafePointer[Int, MutAnyOrigin](to=timed_out1)
    sc1.closed = UnsafePointer[Int, MutAnyOrigin](to=closed1)
    sc1.value = UnsafePointer[Int, MutAnyOrigin](to=value1)
    sc1.now_ticks = UnsafePointer[Int, MutAnyOrigin](to=now1)
    var t1 = TB.create()
    var h1 = spawn(rt1, UnsafePointer[TB, MutAnyOrigin](to=t1), 0)
    sc1.sel_id = h1.id()
    var scp1 = UnsafePointer[Scene, MutAnyOrigin](to=sc1)
    var ud1 = scp1.bitcast[Byte]()
    _ = scheduler_loop(rt1, dispatch, ud1)
    if h1.state() != TaskControlBlock.WAITING:
        red("M1: selector must park with both branches empty")
    # arrival order: A (branch 0) fires first.
    if not chan0_1.try_send(11):
        red("M1: producer A send failed")
    var wr1 = chan0_1.pop_to_wake()
    unpark_current(rt1, _handle(wr1.tcb_addr, wr1.task_id))
    _ = scheduler_loop(rt1, dispatch, ud1)
    if not h1.is_completed():
        red("M1: selector must complete")
    if value1 != 11:
        red("M1: wrong delivered value")
    # no-stale-wake proof: the LOSER (branch 1) is unregistered; a later
    # send there must never touch a waiter for the (already-completed) task.
    if not chan1_1.try_send(99):
        red("M1: post-hoc send into the loser channel failed")
    if chan1_1.to_wake_len() != 0:
        red("M1: a send on the unregistered loser must never enqueue a wake")
    check_invariants(
        "M1", winner1, 0, run1,
        UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_1),
        UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_1),
        hp_unused, UnsafePointer[Runtime, MutAnyOrigin](to=rt1),
    )
    print("M1 ok")

    # ---- M2: data-B-arrives-first (mirror of M1) ---------------------------
    var rt2 = create()
    var chan0_2 = make_channel[Int](2)
    var chan1_2 = make_channel[Int](2)
    var branches2 = List[SelectBranch[Int]]()
    branches2.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_2)))
    branches2.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_2)))
    var state2 = SelectState()
    var sc2 = Scene()
    sc2.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_2)
    sc2.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_2)
    sc2.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=branches2)
    sc2.state = UnsafePointer[SelectState, MutAnyOrigin](to=state2)
    var run2 = Int(0)
    var winner2 = Int(-1)
    var timed_out2 = Int(0)
    var closed2 = Int(0)
    var value2 = Int(-1)
    var now2 = Int(0)
    sc2.run_count = UnsafePointer[Int, MutAnyOrigin](to=run2)
    sc2.winner = UnsafePointer[Int, MutAnyOrigin](to=winner2)
    sc2.timed_out = UnsafePointer[Int, MutAnyOrigin](to=timed_out2)
    sc2.closed = UnsafePointer[Int, MutAnyOrigin](to=closed2)
    sc2.value = UnsafePointer[Int, MutAnyOrigin](to=value2)
    sc2.now_ticks = UnsafePointer[Int, MutAnyOrigin](to=now2)
    var t2 = TB.create()
    var h2 = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=t2), 0)
    sc2.sel_id = h2.id()
    var scp2 = UnsafePointer[Scene, MutAnyOrigin](to=sc2)
    var ud2 = scp2.bitcast[Byte]()
    _ = scheduler_loop(rt2, dispatch, ud2)
    if h2.state() != TaskControlBlock.WAITING:
        red("M2: selector must park with both branches empty")
    # arrival order: B (branch 1) fires first.
    if not chan1_2.try_send(22):
        red("M2: producer B send failed")
    var wr2 = chan1_2.pop_to_wake()
    unpark_current(rt2, _handle(wr2.tcb_addr, wr2.task_id))
    _ = scheduler_loop(rt2, dispatch, ud2)
    if not h2.is_completed():
        red("M2: selector must complete")
    if value2 != 22:
        red("M2: wrong delivered value")
    if not chan0_2.try_send(88):
        red("M2: post-hoc send into the loser channel failed")
    if chan0_2.to_wake_len() != 0:
        red("M2: a send on the unregistered loser must never enqueue a wake")
    check_invariants(
        "M2", winner2, 1, run2,
        UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_2),
        UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_2),
        hp_unused, UnsafePointer[Runtime, MutAnyOrigin](to=rt2),
    )
    print("M2 ok")

    # ---- M3: timeout-first --------------------------------------------------
    var heap3 = TimerHeap()
    var hp3 = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap3)
    var clock_cell3 = stack_allocation[1, UInt64]()
    clock_cell3[0] = 0
    var clock3 = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell3[0]))
    var rt3 = create()
    var chan0_3 = make_channel[Int](2)   # never gets data this scenario
    var chan1_3 = make_channel[Int](2)   # unused second slot (dispatch drains both)
    var branches3 = List[SelectBranch[Int]]()
    branches3.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_3)))
    branches3.append(deadline_branch[Int](hp3, clock3, Deadline(1)))  # 1ms
    var state3 = SelectState()
    var sc3 = Scene()
    sc3.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_3)
    sc3.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_3)
    sc3.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=branches3)
    sc3.state = UnsafePointer[SelectState, MutAnyOrigin](to=state3)
    var run3 = Int(0)
    var winner3 = Int(-1)
    var timed_out3 = Int(0)
    var closed3 = Int(0)
    var value3 = Int(-1)
    var now3 = Int(0)
    sc3.run_count = UnsafePointer[Int, MutAnyOrigin](to=run3)
    sc3.winner = UnsafePointer[Int, MutAnyOrigin](to=winner3)
    sc3.timed_out = UnsafePointer[Int, MutAnyOrigin](to=timed_out3)
    sc3.closed = UnsafePointer[Int, MutAnyOrigin](to=closed3)
    sc3.value = UnsafePointer[Int, MutAnyOrigin](to=value3)
    sc3.now_ticks = UnsafePointer[Int, MutAnyOrigin](to=now3)
    var t3 = TB.create()
    var h3 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t3), 0)
    sc3.sel_id = h3.id()
    var scp3 = UnsafePointer[Scene, MutAnyOrigin](to=sc3)
    var ud3 = scp3.bitcast[Byte]()
    _ = scheduler_loop(rt3, dispatch, ud3)
    if h3.state() != TaskControlBlock.WAITING:
        red("M3: selector must park with the deadline still future")
    # arrival order: the deadline fires before any producer.
    clock3.advance(UInt64(2_000_000))
    var woke3 = service_timers[IntResult](rt3, heap3, clock3.now())
    if woke3 != 1:
        red("M3: service_timers must wake the selector")
    now3 = Int(clock3.now())
    _ = scheduler_loop(rt3, dispatch, ud3)
    if not h3.is_completed():
        red("M3: selector must complete")
    if timed_out3 != 1:
        red("M3: outcome must report is_timeout()")
    # no-stale-wake proof: a LATE send on the loser RECV branch must never
    # touch a waiter for the (already-completed) task.
    if not chan0_3.try_send(77):
        red("M3: post-hoc send into the loser channel failed")
    if chan0_3.to_wake_len() != 0:
        red("M3: a send on the unregistered loser must never enqueue a wake")
    check_invariants(
        "M3", winner3, 1, run3,
        UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_3),
        UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_3),
        hp3, UnsafePointer[Runtime, MutAnyOrigin](to=rt3),
    )
    print("M3 ok")

    # ---- M4: close-first ------------------------------------------------------
    var rt4 = create()
    var chan0_4 = make_channel[Int](2)
    var chan1_4 = make_channel[Int](2)
    var tx1_4 = chan1_4.sender()
    var branches4 = List[SelectBranch[Int]]()
    branches4.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_4)))
    branches4.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_4)))
    var state4 = SelectState()
    var sc4 = Scene()
    sc4.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_4)
    sc4.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_4)
    sc4.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=branches4)
    sc4.state = UnsafePointer[SelectState, MutAnyOrigin](to=state4)
    var run4 = Int(0)
    var winner4 = Int(-1)
    var timed_out4 = Int(0)
    var closed4 = Int(0)
    var value4 = Int(-1)
    var now4 = Int(0)
    sc4.run_count = UnsafePointer[Int, MutAnyOrigin](to=run4)
    sc4.winner = UnsafePointer[Int, MutAnyOrigin](to=winner4)
    sc4.timed_out = UnsafePointer[Int, MutAnyOrigin](to=timed_out4)
    sc4.closed = UnsafePointer[Int, MutAnyOrigin](to=closed4)
    sc4.value = UnsafePointer[Int, MutAnyOrigin](to=value4)
    sc4.now_ticks = UnsafePointer[Int, MutAnyOrigin](to=now4)
    var t4 = TB.create()
    var h4 = spawn(rt4, UnsafePointer[TB, MutAnyOrigin](to=t4), 0)
    sc4.sel_id = h4.id()
    var scp4 = UnsafePointer[Scene, MutAnyOrigin](to=sc4)
    var ud4 = scp4.bitcast[Byte]()
    _ = scheduler_loop(rt4, dispatch, ud4)
    if h4.state() != TaskControlBlock.WAITING:
        red("M4: selector must park with both branches empty")
    if chan0_4.recv_waiters_len() != 1 or chan1_4.recv_waiters_len() != 1:
        red("M4: selector must register on BOTH branches before close")
    # arrival order: channel B's last sender closes before any data.
    tx1_4.close()
    if not chan1_4.is_closed():
        red("M4: channel B must report closed")
    if chan1_4.recv_waiters_len() != 0 or chan1_4.to_wake_len() != 1:
        red("M4: close must move the parked selector to the deferred wake list")
    var wr4 = chan1_4.pop_to_wake()
    unpark_current(rt4, _handle(wr4.tcb_addr, wr4.task_id))
    _ = scheduler_loop(rt4, dispatch, ud4)
    if not h4.is_completed():
        red("M4: selector must complete")
    if closed4 != 1:
        red("M4: outcome must report is_closed()")
    if value4 != -999:
        red("M4: a closed outcome must carry no value")
    check_invariants(
        "M4", winner4, 1, run4,
        UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0_4),
        UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1_4),
        hp_unused, UnsafePointer[Runtime, MutAnyOrigin](to=rt4),
    )
    print("M4 ok")

    print("T28 select model: PASS")
