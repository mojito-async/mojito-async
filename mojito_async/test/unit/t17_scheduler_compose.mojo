# mojito_async/test/unit/t17_scheduler_compose.mojo
#
# A1.1 (issue #33) — composition: spawn children into a Scope, drive them
# via scheduler_loop with a real dispatcher, and close the scope (join-
# integrated).  This is the scheduler+scope composition acceptance (spec
# §112 Epic B / C7 end-to-end at unit level).
#
# Acceptance:
#   - every spawned child is registered in the scope (register() stamps
#     scope_handle/parent_id);
#   - scheduler_loop drives each child's body to COMPLETED via the dispatcher
#     (bodies increment a shared counter);
#   - scope.close(rt) JOINS the settled children (consume-once take_result —
#     no result left pending) and empties the registry;
#   - rt.pending() == 0 after the drive (no stragglers, no hidden blocking).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.scope import Scope, make_scope
from mojito_async.task import JoinHandle, execute, spawn


def red(what: String) raises -> None:
    print("T17 scheduler compose: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


struct Channel(ImplicitlyCopyable, ImplicitlyDeletable):
    """counter@0, n_children@1, c1@2, c2@3, c3@4 (id slots)."""

    var counter: UnsafePointer[Int, MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var c1: UnsafePointer[Int, MutAnyOrigin]
    var c2: UnsafePointer[Int, MutAnyOrigin]
    var c3: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.counter = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = self.counter
        self.c1 = self.counter
        self.c2 = self.counter
        self.c3 = self.counter


def body_1(ud: BytePtr) raises -> IntResult:
    var ch = ud.bitcast[Channel]()
    ch[].counter[] = ch[].counter[] + 1
    return IntResult(10)


def body_2(ud: BytePtr) raises -> IntResult:
    var ch = ud.bitcast[Channel]()
    ch[].counter[] = ch[].counter[] + 10
    return IntResult(20)


def body_3(ud: BytePtr) raises -> IntResult:
    var ch = ud.bitcast[Channel]()
    ch[].counter[] = ch[].counter[] + 100
    return IntResult(30)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var ch = ud.bitcast[Channel]()
    if tid == ch[].c1[]:
        var h1 = _handle(tcb_addr, tid)
        _ = execute(h1, body_1, ud)
        return 1
    if tid == ch[].c2[]:
        var h2 = _handle(tcb_addr, tid)
        _ = execute(h2, body_2, ud)
        return 1
    if tid == ch[].c3[]:
        var h3 = _handle(tcb_addr, tid)
        _ = execute(h3, body_3, ud)
        return 1
    raise Error("unexpected task id in dispatcher")


def main() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var ch = Channel()
    ch.counter = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0)
    ch.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8)
    ch.c1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16)
    ch.c2 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 24)
    ch.c3 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 32)
    buf[0] = 0
    var chp = UnsafePointer[Channel, MutAnyOrigin](to=ch)
    var ud = chp.bitcast[Byte]()

    # scope owns the three children
    var order = List[Int]()
    var order_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=order)
    var s = make_scope(17, order_ptr, False)
    var sp = UnsafePointer[Scope, MutAnyOrigin](to=s)

    var t1 = TB.create()
    var t2 = TB.create()
    var t3 = TB.create()
    var h1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t1), 0)
    var h2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t2), 0)
    var h3 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t3), 0)
    _ = sp[].register[IntResult](UnsafePointer[TB, MutAnyOrigin](to=t1), h1.id(), 0)
    _ = sp[].register[IntResult](UnsafePointer[TB, MutAnyOrigin](to=t2), h2.id(), 0)
    _ = sp[].register[IntResult](UnsafePointer[TB, MutAnyOrigin](to=t3), h3.id(), 0)
    buf[2] = h1.id()
    buf[3] = h2.id()
    buf[4] = h3.id()
    if sp[].live_child_count() != 3:
        red("scope does not own all 3 children")

    # drive to quiet: each child body runs once, counter = 1+10+100 = 111.
    var served = scheduler_loop(rt, dispatch, ud)
    if served != 3:
        red("scheduler_loop served " + String(served) + " records, expected 3")
    if buf[0] != 111:
        red("bodies did not all run (counter " + String(buf[0]) + ", want 111)")
    if not (h1.is_completed() and h2.is_completed() and h3.is_completed()):
        red("not all children COMPLETED after drive")
    if rt.pending() != 0:
        red("runnable queue not quiet after drive")

    # join-integrated close: joins settled children, consumes their results.
    sp[].close_typed[IntResult](rt)
    if sp[].is_open():
        red("scope still open after close")
    if sp[].live_child_count() != 0:
        red("scope registry not empty after close")
    if t1.has_result_pending() or t2.has_result_pending() or t3.has_result_pending():
        red("close did not consume (join) the settled child results")

    print("T17 scheduler compose: PASS")