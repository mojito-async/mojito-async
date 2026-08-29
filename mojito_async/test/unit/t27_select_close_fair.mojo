# mojito_async/test/unit/t27_select_close_fair.mojo
#
# A5.5 close/select behavior + try-scan fairness (issue #93) — acceptance:
# a closed-and-empty RECV branch is selectable (never blocks select); a SEND
# branch on a closed channel is never chosen; ready data beats a closed
# branch at equal rescan (deterministic precedence); select_fast's
# `_rescan_cursor` rotation serves two perpetually-ready branches ~N/2 each
# with no starvation; select_fast never parks when any branch is READY.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.channel import Channel, make_channel
from mojito_async.channel.select import (
    SelectBranch,
    SelectState,
    branch_ready,
    recv_branch,
    send_branch,
    select_fast,
)
from mojito_async.integration.sys import IntResult
from mojito_async.runtime.runtime import create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, spawn


def red(what: String) raises -> None:
    print("T27 select close/fair: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def main() raises:
    var rt = create()
    var t0 = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t0), 0)

    # ---- S1: closed-and-empty RECV is selectable, never blocks ------------
    var ch1 = make_channel[Int](2)
    var tx1 = ch1.sender()
    tx1.close()  # last (only) sender -> send side closes; ring stays empty
    if not ch1.is_closed():
        red("S1: channel must report closed after last-sender close")
    var branches1 = List[SelectBranch]()
    branches1.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=ch1)))
    var st1 = SelectState()
    var out1 = select_fast[Int, IntResult](rt, h, branches1, st1)
    if out1.is_pending():
        red("S1: a closed-and-empty RECV branch must be selectable, not pending")
    if not out1.is_closed():
        red("S1: winning outcome must report is_closed()")
    if out1.has_value():
        red("S1: a closed outcome must carry no value")
    if ch1.recv_waiters_len() != 0:
        red("S1: select_fast must never register/park on a closed branch")
    print("S1 ok")

    # ---- S2: SEND on a closed channel is never chosen ----------------------
    var ch2 = make_channel[Int](2)
    var rx2 = ch2.receiver()
    rx2.close()  # last (only) receiver -> recv side closes -> is_closed()
    if not ch2.is_closed():
        red("S2: channel must report closed after last-receiver close")
    var item2 = Int(1)
    var chOpen2 = make_channel[Int](2)  # open, empty -> genuinely BLOCKED
    var branches2 = List[SelectBranch]()
    branches2.append(
        send_branch[Int](
            UnsafePointer[Channel[Int], MutAnyOrigin](to=ch2),
            UnsafePointer[Int, MutAnyOrigin](to=item2),
        )
    )
    branches2.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chOpen2)))
    var st2 = SelectState()
    var out2 = select_fast[Int, IntResult](rt, h, branches2, st2)
    if not out2.is_pending():
        red("S2: a SEND-on-closed branch must never be chosen; expected nothing claimable")
    print("S2 ok")

    # ---- S3: data-before-close precedence (both orderings) ----------------
    var chClosed3 = make_channel[Int](2)
    var txClosed3 = chClosed3.sender()
    txClosed3.close()
    var chData3 = make_channel[Int](2)
    if not chData3.try_send(7):
        red("S3: prefill failed")
    var branchesA = List[SelectBranch]()
    branchesA.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chClosed3)))
    branchesA.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chData3)))
    var stA = SelectState()
    var outA = select_fast[Int, IntResult](rt, h, branchesA, stA)
    if outA.index != 1 or not outA.has_value() or outA.recv_value() != 7:
        red("S3a: data (index 1) must beat close (index 0) at equal rescan")

    var chData3b = make_channel[Int](2)
    if not chData3b.try_send(8):
        red("S3: prefill (b) failed")
    var chClosed3b = make_channel[Int](2)
    var txClosed3b = chClosed3b.sender()
    txClosed3b.close()
    var branchesB = List[SelectBranch]()
    branchesB.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chData3b)))
    branchesB.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chClosed3b)))
    var stB = SelectState()
    var outB = select_fast[Int, IntResult](rt, h, branchesB, stB)
    if outB.index != 0 or not outB.has_value() or outB.recv_value() != 8:
        red("S3b: data (index 0) must beat close (index 1) at equal rescan")
    print("S3 ok")

    # ---- S4: fairness rotation — two perpetually-ready branches, N/2 each -
    var chR = make_channel[Int](4)
    var chL = make_channel[Int](4)
    if not chR.try_send(100) or not chL.try_send(200):
        red("S4: prefill failed")
    var branches4 = List[SelectBranch]()
    branches4.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chR)))
    branches4.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chL)))
    var st4 = SelectState()
    var wins0 = 0
    var wins1 = 0
    comptime N = Int(20)
    for i in range(N):
        if not branch_ready[Int](branches4, 0) or not branch_ready[Int](branches4, 1):
            red("S4: both branches must probe READY before iteration " + String(i))
        var out4 = select_fast[Int, IntResult](rt, h, branches4, st4)
        if out4.is_pending():
            red("S4: select_fast must never park when a branch is READY")
        if out4.index == 0:
            wins0 += 1
            if not chR.try_send(100):
                red("S4: replenish R failed")
        elif out4.index == 1:
            wins1 += 1
            if not chL.try_send(200):
                red("S4: replenish L failed")
        else:
            red("S4: unexpected winner index " + String(out4.index))
    if wins0 != N // 2 or wins1 != N // 2:
        red(
            "S4: fairness rotation must split "
            + String(N)
            + " selects evenly, got "
            + String(wins0)
            + "/"
            + String(wins1)
        )
    print("S4 ok")

    print("T27 select close/fair: PASS")
