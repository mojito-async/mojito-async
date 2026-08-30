# mojito_async/test/unit/t26_select_deadline.mojo
#
# A5.3 deadline/timeout integration into select (issue #91) — acceptance:
#
#   S1  an already-due deadline wins select_fast WITHOUT parking and
#       leaves ZERO heap entries (the fast path never touches the heap).
#   S2  data-ready-before-deadline: select() parks (arming the heap for the
#       BLOCKED deadline branch), an external send wins the RECV branch
#       first, and the still-armed timer is cancelled — heap ends empty,
#       zero leftover registrations.
#   S3  deadline-before-data: select() parks (arming the heap), the clock
#       advances past the deadline, `service_timers` wakes the selector,
#       and the TIMEOUT outcome wins — every channel registration is
#       cancelled (zero leftovers on the channel), heap already empty (the
#       timer service popped its own entry).
#   S4  timeout-vs-data exactly-at: both branches are simultaneously ready
#       in ONE rescan; the winner is deterministic by registration order
#       (whichever branch is first in the `branches` list wins), proven
#       both ways (RECV-first and DEADLINE-first).
#
# Every scenario ends with ZERO leftovers: no heap entries, no channel
# waiters, no deferred wakes, no pending runnables (matching every other
# A1/A5 driver's discipline).
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
    select_fast,
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
    print("T26 select deadline: RED (" + what + ")")
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
# Scene — a select-driving task over one RECV channel + one DEADLINE branch
# (matches the t25/t21_timer_cancel Scene pattern: plain UnsafePointer
# fields into caller-owned storage keep the struct ImplicitlyCopyable).
# ---------------------------------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan: UnsafePointer[Channel[Int], MutAnyOrigin]
    var branches: UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin]
    var state: UnsafePointer[SelectState, MutAnyOrigin]
    var sel_id: Int
    var winner: UnsafePointer[Int, MutAnyOrigin]
    var timed_out: UnsafePointer[Int, MutAnyOrigin]
    var value: UnsafePointer[Int, MutAnyOrigin]
    var now_ticks: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](unsafe_from_address=1)
        self.state = UnsafePointer[SelectState, MutAnyOrigin](unsafe_from_address=1)
        self.sel_id = 0
        self.winner = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.timed_out = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.value = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.now_ticks = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def selector_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    claim_running(h)
    var out = select[Int, IntResult](
        rt, h, sc[].branches[], sc[].state[], sc[].now_ticks[]
    )
    if h.state() == TaskControlBlock.WAITING:
        return  # parked; the embedding driver re-enters on resume
    sc[].winner[] = out.index
    sc[].timed_out[] = 1 if out.is_timeout() else 0
    if out.has_value():
        sc[].value[] = out.recv_value()
    else:
        sc[].value[] = -999
    _complete(h, out.index)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid != sc[].sel_id:
        raise Error("T26: unknown task id " + String(tid))
    var h = _handle(tcb_addr, tid)
    selector_slice(rt, h, sc)
    while sc[].chan[].to_wake_len() > 0:
        var wr = sc[].chan[].pop_to_wake()
        if wr.task_id == 0:
            break
        unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))
    return 1


def main() raises:
    # ---- S1: an already-due deadline wins select_fast, never parks, -------
    # ----     and never touches the heap (zero heap entries) ---------------
    var heap1 = TimerHeap()
    var hp1 = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap1)
    var clock_cell1 = stack_allocation[1, UInt64]()
    clock_cell1[0] = 1000
    var clock1 = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell1[0]))
    var chan1 = make_channel[Int](2)  # stays empty: the deadline must win
    var branches1 = List[SelectBranch[Int]]()
    branches1.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1)))
    branches1.append(deadline_branch[Int](hp1, clock1, Deadline(0)))  # already past clock1 (1000)
    var st1 = SelectState()
    var rt1 = create()
    var t1 = TB.create()
    var h1 = spawn(rt1, UnsafePointer[TB, MutAnyOrigin](to=t1), 0)
    var out1 = select_fast[Int, IntResult](rt1, h1, branches1, st1, Int(clock1.now()))
    if out1.is_pending():
        red("S1: expected an immediate timeout winner")
    if not out1.is_timeout():
        red("S1: winner must report is_timeout()")
    if out1.index != 1:
        red("S1: winner must be branch 1 (the DEADLINE), got " + String(out1.index))
    if chan1.recv_waiters_len() != 0:
        red("S1: select_fast must never register a waiter")
    if not heap1.is_empty():
        red("S1: select_fast must never touch the heap (expected empty)")
    print("S1 ok")

    # ---- S2: data-ready-before-deadline -- data wins, timer cancelled -----
    var heap2 = TimerHeap()
    var hp2 = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap2)
    var clock_cell2 = stack_allocation[1, UInt64]()
    clock_cell2[0] = 0
    var clock2 = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell2[0]))
    var chan2 = make_channel[Int](2)  # starts empty -> RECV branch BLOCKED
    var branches2 = List[SelectBranch[Int]]()
    branches2.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan2)))
    branches2.append(deadline_branch[Int](hp2, clock2, Deadline(1000)))  # far future -> BLOCKED
    var state2 = SelectState()
    var rt2 = create()
    var sc2 = Scene()
    sc2.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan2)
    sc2.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=branches2)
    sc2.state = UnsafePointer[SelectState, MutAnyOrigin](to=state2)
    var winner2 = Int(-1)
    var timed_out2 = Int(-1)
    var value2 = Int(-1)
    var now2 = Int(0)
    sc2.winner = UnsafePointer[Int, MutAnyOrigin](to=winner2)
    sc2.timed_out = UnsafePointer[Int, MutAnyOrigin](to=timed_out2)
    sc2.value = UnsafePointer[Int, MutAnyOrigin](to=value2)
    sc2.now_ticks = UnsafePointer[Int, MutAnyOrigin](to=now2)
    var t2 = TB.create()
    var h2 = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=t2), 0)
    sc2.sel_id = h2.id()
    var scp2 = UnsafePointer[Scene, MutAnyOrigin](to=sc2)
    var ud2 = scp2.bitcast[Byte]()
    var served2a = scheduler_loop(rt2, dispatch, ud2)
    if served2a != 1:
        red("S2: first drive must serve exactly 1 slice (the parking attempt)")
    if h2.state() != TaskControlBlock.WAITING:
        red("S2: selector must park when RECV is empty and deadline is future")
    if chan2.recv_waiters_len() != 1:
        red("S2: selector must register on the RECV branch")
    if heap2.is_empty():
        red("S2: select() must arm the heap for the BLOCKED deadline branch")
    if heap2.size() != 1:
        red("S2: expected exactly one armed timer")
    if not state2.timer_armed():
        red("S2: SelectState must record the timer as armed")
    # A producer sends BEFORE the deadline elapses: the clock barely moves.
    clock2.advance(UInt64(10))
    if not chan2.try_send(42):
        red("S2: producer send failed")
    if chan2.recv_waiters_len() != 0 or chan2.to_wake_len() != 1:
        red("S2: send must wake the parked selector (deferred)")
    var wr2 = chan2.pop_to_wake()
    unpark_current(rt2, _handle(wr2.tcb_addr, wr2.task_id))
    if h2.state() != TaskControlBlock.RUNNABLE:
        red("S2: selector must be RUNNABLE after unpark")
    now2 = Int(clock2.now())  # driver refreshes now_ticks before re-driving
    var served2b = scheduler_loop(rt2, dispatch, ud2)
    if served2b != 1:
        red("S2: second drive must serve exactly 1 slice (the winning re-entry)")
    if not h2.is_completed():
        red("S2: selector must complete after claiming the RECV winner")
    if winner2 != 0:
        red("S2: winner must be branch 0 (RECV), got " + String(winner2))
    if timed_out2 != 0:
        red("S2: outcome must NOT report is_timeout()")
    if value2 != 42:
        red("S2: wrong delivered value, got " + String(value2))
    if not heap2.is_empty():
        red("S2: the still-armed timer must be cancelled; heap must end empty")
    if state2.timer_armed():
        red("S2: SelectState must clear the armed-timer flag once cancelled")
    if chan2.recv_waiters_len() != 0 or chan2.to_wake_len() != 0:
        red("S2: leftover channel waiters/wakes")
    if rt2.pending() != 0:
        red("S2: leftover runnables")
    print("S2 ok")

    # ---- S3: deadline-before-data -- TIMEOUT wins, channel is cancelled ---
    var heap3 = TimerHeap()
    var hp3 = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap3)
    var clock_cell3 = stack_allocation[1, UInt64]()
    clock_cell3[0] = 0
    var clock3 = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell3[0]))
    var chan3 = make_channel[Int](2)  # stays empty for the whole scenario
    var branches3 = List[SelectBranch[Int]]()
    branches3.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan3)))
    branches3.append(deadline_branch[Int](hp3, clock3, Deadline(1)))  # 1ms = 1_000_000 ticks
    var state3 = SelectState()
    var rt3 = create()
    var sc3 = Scene()
    sc3.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan3)
    sc3.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=branches3)
    sc3.state = UnsafePointer[SelectState, MutAnyOrigin](to=state3)
    var winner3 = Int(-1)
    var timed_out3 = Int(-1)
    var value3 = Int(-1)
    var now3 = Int(0)
    sc3.winner = UnsafePointer[Int, MutAnyOrigin](to=winner3)
    sc3.timed_out = UnsafePointer[Int, MutAnyOrigin](to=timed_out3)
    sc3.value = UnsafePointer[Int, MutAnyOrigin](to=value3)
    sc3.now_ticks = UnsafePointer[Int, MutAnyOrigin](to=now3)
    var t3 = TB.create()
    var h3 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t3), 0)
    sc3.sel_id = h3.id()
    var scp3 = UnsafePointer[Scene, MutAnyOrigin](to=sc3)
    var ud3 = scp3.bitcast[Byte]()
    var served3a = scheduler_loop(rt3, dispatch, ud3)
    if served3a != 1:
        red("S3: first drive must serve exactly 1 slice")
    if h3.state() != TaskControlBlock.WAITING:
        red("S3: selector must park when both branches are BLOCKED")
    if heap3.size() != 1:
        red("S3: select() must arm exactly one timer")
    if chan3.recv_waiters_len() != 1:
        red("S3: selector must register on the RECV branch too")
    # advance the clock PAST the deadline and run the timer service pass.
    clock3.advance(UInt64(2_000_000))
    var woke3 = service_timers[IntResult](rt3, heap3, clock3.now())
    if woke3 != 1:
        red("S3: service_timers must wake the selector exactly once")
    if not heap3.is_empty():
        red("S3: service_timers must pop its own due entry (heap must end empty)")
    if h3.state() != TaskControlBlock.RUNNABLE:
        red("S3: selector must be RUNNABLE after the timer service wakes it")
    now3 = Int(clock3.now())
    var served3b = scheduler_loop(rt3, dispatch, ud3)
    if served3b != 1:
        red("S3: second drive must serve exactly 1 slice (the timeout re-entry)")
    if not h3.is_completed():
        red("S3: selector must complete after claiming the TIMEOUT winner")
    if winner3 != 1:
        red("S3: winner must be branch 1 (the DEADLINE), got " + String(winner3))
    if timed_out3 != 1:
        red("S3: outcome must report is_timeout()")
    if chan3.recv_waiters_len() != 0:
        red("S3: the losing RECV branch must be unregistered (logical cancellation)")
    if chan3.to_wake_len() != 0:
        red("S3: leftover deferred wakes on the channel")
    if not heap3.is_empty():
        red("S3: heap must stay empty after the timeout claim")
    if rt3.pending() != 0:
        red("S3: leftover runnables")
    print("S3 ok")

    # ---- S4: timeout-vs-data exactly-at -- deterministic by scan order ----
    var heap4a = TimerHeap()
    var hp4a = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap4a)
    var clock_cell4a = stack_allocation[1, UInt64]()
    clock_cell4a[0] = 9_000_000
    var clock4a = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell4a[0]))
    var chan4a = make_channel[Int](2)
    if not chan4a.try_send(7):
        red("S4a: prefill failed")
    var branches4a = List[SelectBranch[Int]]()
    branches4a.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan4a)))
    branches4a.append(deadline_branch[Int](hp4a, clock4a, Deadline(9)))  # 9ms == clock4a exactly
    var st4a = SelectState()
    var rt4 = create()
    var t4 = TB.create()
    var h4 = spawn(rt4, UnsafePointer[TB, MutAnyOrigin](to=t4), 0)
    var out4a = select_fast[Int, IntResult](rt4, h4, branches4a, st4a, Int(clock4a.now()))
    if out4a.index != 0 or not out4a.has_value() or out4a.recv_value() != 7:
        red("S4a: RECV (index 0) must beat an exactly-at deadline (index 1)")
    if out4a.is_timeout():
        red("S4a: outcome must not report is_timeout() when RECV wins the tie")
    print("S4a ok")

    var heap4b = TimerHeap()
    var hp4b = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap4b)
    var clock_cell4b = stack_allocation[1, UInt64]()
    clock_cell4b[0] = 9_000_000
    var clock4b = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell4b[0]))
    var chan4b = make_channel[Int](2)
    if not chan4b.try_send(8):
        red("S4b: prefill failed")
    var branches4b = List[SelectBranch[Int]]()
    branches4b.append(deadline_branch[Int](hp4b, clock4b, Deadline(9)))  # 9ms == clock4b exactly
    branches4b.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan4b)))
    var st4b = SelectState()
    var out4b = select_fast[Int, IntResult](rt4, h4, branches4b, st4b, Int(clock4b.now()))
    if out4b.index != 0 or not out4b.is_timeout():
        red("S4b: DEADLINE (index 0) must beat an exactly-at RECV (index 1)")
    print("S4b ok")

    print("T26 select deadline: PASS")
