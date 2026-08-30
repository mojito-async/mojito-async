# mojito_async/test/unit/t37_select_timer_gaps.mojo
#
# A6.2 (issue #85) — timer deadline branches in the channel select: the
# GENUINE GAPS left over EPIC #5's own deadline/timeout integration (#91,
# t26_select_deadline.mojo).  #91 already exercises the shared machinery
# this issue also needs (deadline_branch/timeout_branch, arm-before-park,
# cancel symmetry, single-winner exactly-at races) — re-reading select.mojo
# before writing this driver confirmed that; duplicating those scenarios
# here would just be the same test twice.  What #91's driver never touches:
#
#   S1  `never[Int]()` (issue #85 deliverable): a DEADLINE branch that can never
#       resolve.  Alongside a channel branch that starts BLOCKED and later
#       receives data, the selector parks (never[Int]() contributes nothing to
#       the heap — heap_addr stays 0), a producer sends, and the select
#       completes via the CHANNEL branch — never[Int]() never wins, the select
#       still completes via another branch (issue #85 exit criterion).
#   S2  `never[Int]()` alongside an ALREADY-CLOSED channel: select_fast claims
#       the close immediately; never[Int]() never touches select_fast's fast
#       path at all (heap_addr == 0 means it is never even inspected past
#       classify_branch's BLOCKED verdict).
#   S3  CLOSE-WHILE-TIMER-PENDING (issue #85 "close + timer semantics", NOT
#       covered by #91/t26): a RECV branch and a real timer-integrated
#       DEADLINE branch are both BLOCKED; select() parks and arms the heap.
#       The channel closes (last sender) BEFORE the deadline elapses — the
#       close wakes the parked selector (Channel's own close-drains-
#       waiters path, spec §41) and the RECV branch wins by observing
#       close.  Exit criteria: the CHANNEL branch alone fails/completes
#       with `is_closed()`; the TIMER IS NOT CANCELLED — the heap still
#       holds its own entry ("the timer keeps its own lifetime", no
#       implicit cancellation just because the channel closed, spec
#       §38/39) — proving the select.mojo `_cancel_armed_timer` call sites
#       correctly special-case a closed-recv win (this driver was RED
#       against the plain #91 implementation, which cancelled unconditionally
#       on every claim).  Advancing the clock past the still-armed deadline
#       and running `service_timers` then wakes NOBODY (no duplicate wake:
#       the selector already COMPLETED via the close winner; a stale timer
#       firing against a COMPLETED task is a documented service_timers
#       no-op, `time/timer_service.mojo`).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from std.memory import stack_allocation
from mojito_async.channel import Channel, make_channel
from mojito_async.channel.select import (
    SelectBranch,
    SelectState,
    deadline_branch,
    never,
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
    print("T39 select timer gaps: RED (" + what + ")")
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
# Scene — a select-driving task (matches t26's Scene pattern exactly).
# ---------------------------------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan: UnsafePointer[Channel[Int], MutAnyOrigin]
    var branches: UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin]
    var state: UnsafePointer[SelectState, MutAnyOrigin]
    var sel_id: Int
    var winner: UnsafePointer[Int, MutAnyOrigin]
    var timed_out: UnsafePointer[Int, MutAnyOrigin]
    var closed: UnsafePointer[Int, MutAnyOrigin]
    var value: UnsafePointer[Int, MutAnyOrigin]
    var now_ticks: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](unsafe_from_address=1)
        self.state = UnsafePointer[SelectState, MutAnyOrigin](unsafe_from_address=1)
        self.sel_id = 0
        self.winner = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.timed_out = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.closed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
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
    sc[].closed[] = 1 if out.is_closed() else 0
    if out.has_value():
        sc[].value[] = out.recv_value()
    else:
        sc[].value[] = -999
    _complete(h, out.index)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid != sc[].sel_id:
        raise Error("T37: unknown task id " + String(tid))
    var h = _handle(tcb_addr, tid)
    selector_slice(rt, h, sc)
    while sc[].chan[].to_wake_len() > 0:
        var wr = sc[].chan[].pop_to_wake()
        if wr.task_id == 0:
            break
        unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))
    return 1


def main() raises:
    # ---- S1: never[Int]() + a channel that later receives data -----------------
    # ----     select still completes via the CHANNEL, never[Int]() never wins ---
    var chan1 = make_channel[Int](2)  # empty -> RECV branch BLOCKED
    var branches1 = List[SelectBranch[Int]]()
    branches1.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1)))
    branches1.append(never[Int]())
    var state1 = SelectState()
    var rt1 = create()
    var sc1 = Scene()
    sc1.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1)
    sc1.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=branches1)
    sc1.state = UnsafePointer[SelectState, MutAnyOrigin](to=state1)
    var winner1 = Int(-1)
    var timed_out1 = Int(-1)
    var closed1 = Int(-1)
    var value1 = Int(-1)
    var now1 = Int(0)
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
    var served1a = scheduler_loop(rt1, dispatch, ud1)
    if served1a != 1:
        red("S1: first drive must serve exactly 1 slice (the parking attempt)")
    if h1.state() != TaskControlBlock.WAITING:
        red("S1: selector must park (RECV empty, never[Int]() unresolvable)")
    if chan1.recv_waiters_len() != 1:
        red("S1: selector must register on the RECV branch")
    if not chan1.try_send(9):
        red("S1: producer send failed")
    if chan1.to_wake_len() != 1:
        red("S1: send must wake the parked selector (deferred)")
    var wr1 = chan1.pop_to_wake()
    unpark_current(rt1, _handle(wr1.tcb_addr, wr1.task_id))
    var served1b = scheduler_loop(rt1, dispatch, ud1)
    if served1b != 1:
        red("S1: second drive must serve exactly 1 slice (the winning re-entry)")
    if not h1.is_completed():
        red("S1: selector must complete via the channel branch")
    if winner1 != 0:
        red("S1: winner must be branch 0 (RECV), never[Int]() (branch 1) must never win")
    if timed_out1 != 0:
        red("S1: outcome must NOT report is_timeout() (never[Int]() cannot fire)")
    if value1 != 9:
        red("S1: wrong delivered value, got " + String(value1))
    print("S1 ok")

    # ---- S2: never[Int]() + an ALREADY-CLOSED channel -- select_fast claims ----
    # ----     the close immediately; never[Int]() is never even a candidate -----
    var chan2 = make_channel[Int](2)
    var tx2 = chan2.sender()
    tx2.close()
    var branches2 = List[SelectBranch[Int]]()
    branches2.append(never[Int]())
    branches2.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan2)))
    var state2 = SelectState()
    var rt2 = create()
    var t2 = TB.create()
    var h2 = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=t2), 0)
    var out2 = select_fast[Int, IntResult](rt2, h2, branches2, state2, 0)
    if out2.is_pending():
        red("S2: the closed-and-empty RECV branch must be selectable")
    if out2.index != 1:
        red("S2: winner must be branch 1 (RECV/closed), got " + String(out2.index))
    if not out2.is_closed():
        red("S2: winning outcome must report is_closed()")
    print("S2 ok")

    # ---- S3: CLOSE-WHILE-TIMER-PENDING -- close wins, timer NOT cancelled -
    var heap3 = TimerHeap()
    var hp3 = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap3)
    var clock_cell3 = stack_allocation[1, UInt64]()
    clock_cell3[0] = 0
    var clock3 = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell3[0]))
    var chan3 = make_channel[Int](2)  # empty -> RECV branch BLOCKED
    var branches3 = List[SelectBranch[Int]]()
    branches3.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan3)))
    branches3.append(deadline_branch[Int](hp3, clock3, Deadline(1)))  # 1ms far future
    var state3 = SelectState()
    var rt3 = create()
    var sc3 = Scene()
    sc3.chan = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan3)
    sc3.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=branches3)
    sc3.state = UnsafePointer[SelectState, MutAnyOrigin](to=state3)
    var winner3 = Int(-1)
    var timed_out3 = Int(-1)
    var closed3 = Int(-1)
    var value3 = Int(-1)
    var now3 = Int(0)
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
    var served3a = scheduler_loop(rt3, dispatch, ud3)
    if served3a != 1:
        red("S3: first drive must serve exactly 1 slice")
    if h3.state() != TaskControlBlock.WAITING:
        red("S3: selector must park (both branches BLOCKED)")
    if heap3.size() != 1:
        red("S3: select() must arm exactly one timer for the BLOCKED deadline")
    if chan3.recv_waiters_len() != 1:
        red("S3: selector must register on the RECV branch too")
    # Close the LAST sender BEFORE the deadline elapses -- the channel's own
    # close-drains-waiters path (spec §41) wakes the parked selector.
    var tx3 = chan3.sender()
    tx3.close()
    if not chan3.is_closed():
        red("S3: channel must report closed after last-sender close")
    if chan3.to_wake_len() != 1:
        red("S3: close must move the parked selector into the deferred wake list")
    var wr3 = chan3.pop_to_wake()
    unpark_current(rt3, _handle(wr3.tcb_addr, wr3.task_id))
    if h3.state() != TaskControlBlock.RUNNABLE:
        red("S3: selector must be RUNNABLE after the close wake")
    var served3b = scheduler_loop(rt3, dispatch, ud3)
    if served3b != 1:
        red("S3: second drive must serve exactly 1 slice (the close re-entry)")
    if not h3.is_completed():
        red("S3: selector must complete after claiming the close winner")
    if winner3 != 0:
        red("S3: winner must be branch 0 (RECV/closed), got " + String(winner3))
    if closed3 != 1:
        red("S3: outcome must report is_closed()")
    if timed_out3 != 0:
        red("S3: a close win must NOT also report is_timeout()")
    # THE GAP: the still-armed timer must survive the close win untouched.
    if heap3.is_empty():
        red(
            "S3: closing the channel must NOT cancel the pending timer -- "
            "the timer keeps its own lifetime (issue #85); heap ended empty"
        )
    if heap3.size() != 1:
        red("S3: expected exactly the original timer entry to survive, got size="
            + String(heap3.size()))
    if not state3.timer_armed():
        red("S3: SelectState must still report the timer as armed after a close win")
    # Advancing past the deadline now must wake NOBODY (no duplicate wake):
    # the selector already COMPLETED via the close winner.
    clock3.advance(UInt64(2_000_000))
    var woke3 = service_timers[IntResult](rt3, heap3, clock3.now())
    if woke3 != 0:
        red("S3: a stale timer against a COMPLETED task must wake nobody, woke "
            + String(woke3))
    if not heap3.is_empty():
        red("S3: service_timers must still pop its own due (stale) entry")
    if rt3.pending() != 0:
        red("S3: leftover runnables after the stale timer pass")
    print("S3 ok")

    print("T39 select timer gaps: PASS")
