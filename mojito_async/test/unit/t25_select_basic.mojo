# mojito_async/test/unit/t25_select_basic.mojo
#
# A5.4 select core (issue #92) — acceptance: single-winner delivery over
# multiple RECV branches, losing-branch logical cancellation, select_fast
# never parks, select's register-once/park-once/resume choreography over
# the canonical park/wake kernel (#39), and a mixed RECV+SEND scene proving
# unregister_sender fires on the losing branch too.
#
# Scenes:
#   S1  select_fast claims the only ready branch (2 RECV, one has data);
#       never registers a waiter (never parks).
#   S2  select_fast tiebreak: BOTH RECV branches ready -> lowest index wins
#       first (registration-order FIFO tiebreak, cursor starts at 0).
#   S3  select() parks when both RECV branches are empty (registers on
#       BOTH); an external send on branch 1 wakes it; on re-entry select()
#       claims branch 1 and unregisters the losing branch 0 (logical
#       cancellation) — zero leftovers afterward.
#   S4  mixed RECV(C)/SEND(D) select: both start BLOCKED (C empty, D full);
#       selector parks on both; an external send on C wakes it; on
#       re-entry select() claims the RECV branch and unregisters the losing
#       SEND branch's send-waiter registration on D.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.channel import Channel, make_channel
from mojito_async.channel.select import (
    SelectBranch,
    SelectState,
    recv_branch,
    send_branch,
    select,
    select_fast,
)
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T25 select basic: RED (" + what + ")")
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
# Scene — a select-driving task over two channel slots (RECV or SEND either
# one), matching the neighboring t21/t22 Scene pattern: fields are plain
# UnsafePointers into caller-owned storage so the struct stays
# ImplicitlyCopyable (a List field, as scope.mojo notes, would forbid that).
# ---------------------------------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan_0: UnsafePointer[Channel[Int], MutAnyOrigin]
    var chan_1: UnsafePointer[Channel[Int], MutAnyOrigin]
    var branches: UnsafePointer[List[SelectBranch], MutAnyOrigin]
    var state: UnsafePointer[SelectState, MutAnyOrigin]
    var sel_id: Int
    var winner: UnsafePointer[Int, MutAnyOrigin]
    var value: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.branches = UnsafePointer[List[SelectBranch], MutAnyOrigin](unsafe_from_address=1)
        self.state = UnsafePointer[SelectState, MutAnyOrigin](unsafe_from_address=1)
        self.sel_id = 0
        self.winner = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.value = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def selector_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    claim_running(h)
    var out = select[Int, IntResult](rt, h, sc[].branches[], sc[].state[])
    if h.state() == TaskControlBlock.WAITING:
        return  # parked; the embedding driver re-enters on resume
    sc[].winner[] = out.index
    if out.has_value():
        sc[].value[] = out.recv_value()
    else:
        sc[].value[] = -999
    _complete(h, out.index)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid != sc[].sel_id:
        raise Error("T25: unknown task id " + String(tid))
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


def main() raises:
    # ---- S1: select_fast claims the only ready branch, zero leftovers -----
    var chA1 = make_channel[Int](2)
    var chB1 = make_channel[Int](2)
    if not chB1.try_send(42):
        red("S1: prefill failed")
    var branches1 = List[SelectBranch]()
    branches1.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chA1)))
    branches1.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chB1)))
    var st1 = SelectState()
    var rt1 = create()
    var t1 = TB.create()
    var h1 = spawn(rt1, UnsafePointer[TB, MutAnyOrigin](to=t1), 0)
    var out1 = select_fast[Int, IntResult](rt1, h1, branches1, st1)
    if out1.is_pending():
        red("S1: expected an immediate winner")
    if out1.index != 1:
        red("S1: winner must be branch 1 (the ready channel), got " + String(out1.index))
    if not out1.has_value() or out1.recv_value() != 42:
        red("S1: wrong recv value")
    if chA1.recv_waiters_len() != 0 or chB1.recv_waiters_len() != 0:
        red("S1: select_fast must never register a waiter")
    if chA1.to_wake_len() != 0 or chB1.to_wake_len() != 0:
        red("S1: leftover deferred wakes")
    print("S1 ok")

    # ---- S2: tiebreak — both ready, lowest index wins first ---------------
    var chA2 = make_channel[Int](2)
    var chB2 = make_channel[Int](2)
    if not chA2.try_send(10) or not chB2.try_send(20):
        red("S2: prefill failed")
    var branches2 = List[SelectBranch]()
    branches2.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chA2)))
    branches2.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chB2)))
    var st2 = SelectState()
    var out2 = select_fast[Int, IntResult](rt1, h1, branches2, st2)
    if out2.index != 0:
        red("S2: fresh cursor must prefer the lower index, got " + String(out2.index))
    if not out2.has_value() or out2.recv_value() != 10:
        red("S2: wrong recv value")
    if st2.cursor() != 1:
        red("S2: cursor must advance past the claimed index, got " + String(st2.cursor()))
    print("S2 ok")

    # ---- S3: select() parks on two empty RECV branches, resumes on B ------
    var rt3 = create()
    var chA3 = make_channel[Int](2)
    var chB3 = make_channel[Int](2)
    var branches3 = List[SelectBranch]()
    branches3.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chA3)))
    branches3.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chB3)))
    var state3 = SelectState()
    var sc3 = Scene()
    sc3.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chA3)
    sc3.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chB3)
    sc3.branches = UnsafePointer[List[SelectBranch], MutAnyOrigin](to=branches3)
    sc3.state = UnsafePointer[SelectState, MutAnyOrigin](to=state3)
    var winner3 = Int(-1)
    var value3 = Int(-1)
    sc3.winner = UnsafePointer[Int, MutAnyOrigin](to=winner3)
    sc3.value = UnsafePointer[Int, MutAnyOrigin](to=value3)
    var t3 = TB.create()
    var h3 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t3), 0)
    sc3.sel_id = h3.id()
    var scp3 = UnsafePointer[Scene, MutAnyOrigin](to=sc3)
    var ud3 = scp3.bitcast[Byte]()
    var served3a = scheduler_loop(rt3, dispatch, ud3)
    if served3a != 1:
        red("S3: first drive must serve exactly 1 slice (the parking attempt)")
    if h3.state() != TaskControlBlock.WAITING:
        red("S3: selector must park when both branches are empty")
    if chA3.recv_waiters_len() != 1 or chB3.recv_waiters_len() != 1:
        red("S3: selector must register on BOTH empty branches")
    if not chB3.try_send(99):
        red("S3: producer send into B failed")
    if chB3.recv_waiters_len() != 0 or chB3.to_wake_len() != 1:
        red("S3: send on B must wake the parked selector (deferred)")
    var wr3 = chB3.pop_to_wake()
    if wr3.task_id != h3.id():
        red("S3: wrong task woken from B")
    unpark_current(rt3, _handle(wr3.tcb_addr, wr3.task_id))
    if h3.state() != TaskControlBlock.RUNNABLE:
        red("S3: selector must be RUNNABLE after unpark")
    var served3b = scheduler_loop(rt3, dispatch, ud3)
    if served3b != 1:
        red("S3: second drive must serve exactly 1 slice (the winning re-entry)")
    if not h3.is_completed():
        red("S3: selector must complete after claiming a winner")
    if winner3 != 1:
        red("S3: winner must be branch 1 (channel B), got " + String(winner3))
    if value3 != 99:
        red("S3: wrong delivered value, got " + String(value3))
    if chA3.recv_waiters_len() != 0:
        red("S3: losing branch A must be unregistered (logical cancellation)")
    if chB3.recv_waiters_len() != 0:
        red("S3: winning branch must leave no leftover registration")
    if chA3.to_wake_len() != 0 or chB3.to_wake_len() != 0:
        red("S3: leftover deferred wakes")
    if rt3.pending() != 0:
        red("S3: leftover runnables")
    print("S3 ok")

    # ---- S4: mixed RECV(C)/SEND(D); losing SEND branch is unregistered ----
    var rt4 = create()
    var chC4 = make_channel[Int](2)
    var chD4 = make_channel[Int](1)
    if not chD4.try_send(0):
        red("S4: prefill of D failed")  # D starts FULL -> SEND branch BLOCKED
    var item4 = Int(55)
    var branches4 = List[SelectBranch]()
    branches4.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chC4)))
    branches4.append(
        send_branch[Int](
            UnsafePointer[Channel[Int], MutAnyOrigin](to=chD4),
            UnsafePointer[Int, MutAnyOrigin](to=item4),
        )
    )
    var state4 = SelectState()
    var sc4 = Scene()
    sc4.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chC4)
    sc4.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chD4)
    sc4.branches = UnsafePointer[List[SelectBranch], MutAnyOrigin](to=branches4)
    sc4.state = UnsafePointer[SelectState, MutAnyOrigin](to=state4)
    var winner4 = Int(-1)
    var value4 = Int(-1)
    sc4.winner = UnsafePointer[Int, MutAnyOrigin](to=winner4)
    sc4.value = UnsafePointer[Int, MutAnyOrigin](to=value4)
    var t4 = TB.create()
    var h4 = spawn(rt4, UnsafePointer[TB, MutAnyOrigin](to=t4), 0)
    sc4.sel_id = h4.id()
    var scp4 = UnsafePointer[Scene, MutAnyOrigin](to=sc4)
    var ud4 = scp4.bitcast[Byte]()
    var served4a = scheduler_loop(rt4, dispatch, ud4)
    if served4a != 1:
        red("S4: first drive must serve exactly 1 slice")
    if h4.state() != TaskControlBlock.WAITING:
        red("S4: selector must park when RECV is empty and SEND is full")
    if chC4.recv_waiters_len() != 1:
        red("S4: selector must register as a receiver on C")
    if chD4.send_waiters_len() != 1:
        red("S4: selector must register as a sender on D")
    if not chC4.try_send(77):
        red("S4: producer send into C failed")
    if chC4.recv_waiters_len() != 0 or chC4.to_wake_len() != 1:
        red("S4: send on C must wake the parked selector (deferred)")
    var wr4 = chC4.pop_to_wake()
    unpark_current(rt4, _handle(wr4.tcb_addr, wr4.task_id))
    var served4b = scheduler_loop(rt4, dispatch, ud4)
    if served4b != 1:
        red("S4: second drive must serve exactly 1 slice (the winning re-entry)")
    if not h4.is_completed():
        red("S4: selector must complete after claiming the RECV winner")
    if winner4 != 0:
        red("S4: winner must be branch 0 (RECV on C), got " + String(winner4))
    if value4 != 77:
        red("S4: wrong delivered value, got " + String(value4))
    if chD4.send_waiters_len() != 0:
        red("S4: losing SEND branch on D must be unregistered")
    if chC4.recv_waiters_len() != 0:
        red("S4: winning branch must leave no leftover registration")
    if chC4.to_wake_len() != 0 or chD4.to_wake_len() != 0:
        red("S4: leftover deferred wakes")
    if rt4.pending() != 0:
        red("S4: leftover runnables")
    print("S4 ok")

    print("T25 select basic: PASS")
