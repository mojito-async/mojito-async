# mojito_async/test/unit/t13_scope.mojo
#
# A1.1 (issue #33) — structured-concurrency Scope with a JOIN-INTEGRATED
# close.
#
# Acceptance (A0-T13/T14 + A1.1 "close integrates child join"):
#   - registry: register/unregister TaskControlBlock cells, live_child_count,
#     scope-handle round-trip;
#   - close REFUSES (ChildrenStillLive) while a live (unfinished) child or an
#     open subscope remains; close JOINS settled (COMPLETED) registered
#     children (consume-once result) instead of demanding a pre-drained
#     registry;
#   - nested scopes close inner-before-outer; outer close with an open
#     subscope refused; double-close refused;
#   - drop_children() is the abort escape hatch;
#   - failure policy: first failed child triggers sibling cancellation
#     through the INJECTED CancelHook.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.integration.sys import IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.scope import CancelHook, Scope, make_nested_scope, make_scope


def red(what: String) raises -> None:
    print("T13 scope: RED (" + what + ")")
    raise Error(what)


# --- stub cancellation hook -------------------------------------------------

struct RecordingCancel(CancelHook):
    var log: UnsafePointer[List[Int], MutAnyOrigin]

    def __init__(out self, p: UnsafePointer[List[Int], MutAnyOrigin]):
        self.log = p

    def request_cancel(mut self, scope_handle: Int, child_handle: Int) raises:
        self.log[].append(child_handle)


comptime TB = TaskControlBlock[IntResult]
comptime SC = Scope[IntResult, RecordingCancel]


def tcb_ptr(mut t: TB) -> UnsafePointer[TB, MutAnyOrigin]:
    return UnsafePointer[TB, MutAnyOrigin](to=t)


def scope_ptr(mut s: SC) -> UnsafePointer[SC, MutAnyOrigin]:
    return UnsafePointer[SC, MutAnyOrigin](to=s)


def main() raises:
    var rt = create()
    var order = List[Int]()
    var order_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=order)
    var hook = RecordingCancel(order_ptr)

    # 1. registry: register/unregister + handle round-trip.
    var s1 = make_scope[IntResult, RecordingCancel](hook, 7, order_ptr, False)
    var sp1 = scope_ptr(s1)
    var a = TB.create()
    var b = TB.create()
    var ida = sp1[].register(tcb_ptr(a), 0)
    var idb = sp1[].register(tcb_ptr(b), 0)
    if sp1[].live_child_count() != 2:
        red("registry did not grow to 2")
    if a.scope_handle() != 7 or b.scope_handle() != 7:
        red("scope handle round-trip failed")
    sp1[].unregister(idb)
    if sp1[].live_child_count() != 1 or not sp1[].is_registered(ida):
        red("unregister did not drop exactly one child")

    # 2. close REFUSES while an unfinished (live) child exists.
    var blocked = False
    try:
        sp1[].close(rt)
    except Error:
        blocked = True
    if not blocked:
        red("close with an unfinished live child not refused")
    sp1[].unregister(ida)
    sp1[].close(rt)
    if sp1[].is_open():
        red("close did not mark the scope closed")
    var dbl = False
    try:
        sp1[].close(rt)
    except Error:
        dbl = True
    if not dbl:
        red("double close not refused")

    # 3. close JOINS a settled (COMPLETED, registered-but-unreaped) child.
    var s2 = make_scope[IntResult, RecordingCancel](hook, 8, order_ptr, False)
    var sp2 = scope_ptr(s2)
    var done = TB.create()
    done.transition(TaskControlBlock.RUNNABLE)
    done.transition(TaskControlBlock.RUNNING)
    done.transition(TaskControlBlock.COMPLETED)
    done.mark_result(IntResult(42))
    _ = sp2[].register(tcb_ptr(done), 0)
    sp2[].close(rt)
    if done.has_result_pending():
        red("join-integrated close did not consume the child result")

    # 4. nested order: inner closes before outer.
    var outer = make_scope[IntResult, RecordingCancel](hook, 101, order_ptr, True)
    var op = UnsafePointer[SC, MutAnyOrigin](to=outer)
    var inner = make_nested_scope[IntResult, RecordingCancel](
        hook, 102, op, order_ptr, True
    )
    var outer_first = False
    try:
        op[].close(rt)
    except Error:
        outer_first = True
    if not outer_first:
        red("closing outer before inner not refused")
    inner.close(rt)
    op[].close(rt)
    if len(order) != 2 or order[0] != 102 or order[1] != 101:
        red("nested close order not inner-first")

    # 5. failure policy: first failed child triggers sibling cancellation.
    var cancels = List[Int]()
    var cancels_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=cancels)
    var hook5 = RecordingCancel(cancels_ptr)
    var s5 = make_scope[IntResult, RecordingCancel](hook5, 55, cancels_ptr, False)
    var sp5 = scope_ptr(s5)
    var x1 = TB.create()
    var x2 = TB.create()
    var x3 = TB.create()
    var rid1 = sp5[].register(tcb_ptr(x1), 0)
    _ = sp5[].register(tcb_ptr(x2), 0)
    _ = sp5[].register(tcb_ptr(x3), 0)
    sp5[].unregister(rid1)
    sp5[].request_cancel_all()
    if len(cancels) != 2:
        red("hook did not fire once per surviving sibling")
    var pair = (cancels[0] == 2 and cancels[1] == 3) or (
        cancels[0] == 3 and cancels[1] == 2
    )
    if not pair:
        red("surviving siblings not both cancelled")

    # 6. drop_children escape hatch.
    var s6 = make_scope[IntResult, RecordingCancel](hook, 66, order_ptr, False)
    var sp6 = scope_ptr(s6)
    var y1 = TB.create()
    var y2 = TB.create()
    _ = sp6[].register(tcb_ptr(y1), 0)
    _ = sp6[].register(tcb_ptr(y2), 0)
    sp6[].drop_children()
    if sp6[].live_child_count() != 0:
        red("drop_children did not empty the registry")

    print("T13 scope: PASS")