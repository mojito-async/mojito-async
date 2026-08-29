# mojito_async/test/unit/t13_scope.mojo
#
# A1.1 (issue #33) — structured-concurrency Scope with a JOIN-INTEGRATED
# close.
#
# A3.2 (#42 decision, issue #61): migrated onto the NON-GENERIC Scope —
# `make_scope(handle, order_log, has_log)` (no type parameters),
# `register[T](child, task_id, parent_task_id)`, `scope.spawn[T](rt, tcb,
# parent_id)`.  The injected CancelHook is RETIRED: request_cancel_all()
# now drives the ERASED TCB_Prefix directly (RUNNING -> CANCELLED, WAITING
# woken) — section 5 below asserts sibling state transitions instead of a
# hook-recorded log (the hook-injection seam is decoupled from Scope by the
# #42 decision; full flag-tree wiring is lane #54).
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
#   - failure policy: request_cancel_all() cancels every surviving,
#     currently-RUNNING sibling through the erased prefix.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.integration.sys import IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.scope import Scope, make_nested_scope, make_scope


def red(what: String) raises -> None:
    print("T13 scope: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def tcb_ptr(mut t: TB) -> UnsafePointer[TB, MutAnyOrigin]:
    return UnsafePointer[TB, MutAnyOrigin](to=t)


def scope_ptr(mut s: Scope) -> UnsafePointer[Scope, MutAnyOrigin]:
    return UnsafePointer[Scope, MutAnyOrigin](to=s)


def main() raises:
    var rt = create()
    var order = List[Int]()
    var order_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=order)

    # 1. registry: register/unregister + handle round-trip.
    var s1 = make_scope(7, order_ptr, False)
    var sp1 = scope_ptr(s1)
    var a = TB.create()
    var b = TB.create()
    var ida = sp1[].register[IntResult](tcb_ptr(a), 1, 0)
    var idb = sp1[].register[IntResult](tcb_ptr(b), 2, 0)
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

    # 3. close_typed[T] JOINS a settled (COMPLETED, registered-but-unreaped)
    # child (#42 pt 4: plain close() is erased VALIDATE-ONLY and never
    # consumes a result; close_typed[T] is the homogeneous typed reap).
    var s2 = make_scope(8, order_ptr, False)
    var sp2 = scope_ptr(s2)
    var done = TB.create()
    done.transition(TaskControlBlock.RUNNABLE)
    done.transition(TaskControlBlock.RUNNING)
    done.transition(TaskControlBlock.COMPLETED)
    done.mark_result(IntResult(42))
    _ = sp2[].register[IntResult](tcb_ptr(done), 3, 0)
    sp2[].close_typed[IntResult](rt)
    if done.has_result_pending():
        red("join-integrated close_typed did not consume the child result")
    if sp2[].is_open():
        red("close_typed did not close the scope")

    # 4. nested order: inner closes before outer.
    var outer = make_scope(101, order_ptr, True)
    var op = UnsafePointer[Scope, MutAnyOrigin](to=outer)
    var inner = make_nested_scope(102, op, order_ptr, True)
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

    # 5. failure policy: request_cancel_all cancels every surviving, RUNNING
    # sibling through the erased TCB_Prefix (the CancelHook injection seam
    # is retired by the #42 decision — Scope drives the prefix directly).
    var s5 = make_scope(55, order_ptr, False)
    var sp5 = scope_ptr(s5)
    var x1 = TB.create()
    var x2 = TB.create()
    var x3 = TB.create()
    var rid1 = sp5[].register[IntResult](tcb_ptr(x1), 4, 0)
    _ = sp5[].register[IntResult](tcb_ptr(x2), 5, 0)
    _ = sp5[].register[IntResult](tcb_ptr(x3), 6, 0)
    sp5[].unregister(rid1)
    x2.transition(TaskControlBlock.RUNNABLE)
    x2.transition(TaskControlBlock.RUNNING)
    x3.transition(TaskControlBlock.RUNNABLE)
    x3.transition(TaskControlBlock.RUNNING)
    sp5[].request_cancel_all()
    if x2.state() != TaskControlBlock.CANCELLED or x3.state() != TaskControlBlock.CANCELLED:
        red("surviving siblings not both cancelled")
    if x1.state() == TaskControlBlock.CANCELLED:
        red("unregistered child must not be reached by request_cancel_all")

    # 7. SCOPE-AWARE spawn auto-registers the child (INV-3, issue #40).
    var rt7 = create()
    var s7 = make_scope(70, order_ptr, False)
    var sp7a = scope_ptr(s7)
    var z1 = TB.create()
    var h1 = sp7a[].spawn[IntResult](rt7, tcb_ptr(z1), 0)
    if sp7a[].live_child_count() != 1:
        red("scoped spawn did not auto-register the child")
    if z1.scope_handle() != 70:
        red("scoped spawn did not stamp the scope handle")
    if not sp7a[].is_registered(h1.id()):
        red("registry does not know the spawned child by task id")
    if z1.state() != TaskControlBlock.RUNNABLE:
        red("spawned child not RUNNABLE after scoped spawn")
    if rt7.pending() != 1:
        red("spawned child not enqueued exactly once")
    # join-integrated close consumes the spawned child's settled result
    # (z1 is already RUNNABLE from the scoped spawn).
    z1.transition(TaskControlBlock.RUNNING)
    z1.transition(TaskControlBlock.COMPLETED)
    z1.mark_result(IntResult(9))
    sp7a[].close_typed[IntResult](rt7)
    if z1.has_result_pending():
        red("close_typed did not join+consume the scoped spawn's result")
    # a spawn into a CLOSED scope is refused.
    var denied = False
    var z8 = TB.create()
    try:
        _ = sp7a[].spawn[IntResult](rt7, tcb_ptr(z8), 0)
    except Error:
        denied = True
    if not denied:
        red("spawn into a closed scope did not refuse")
    # a child that already names a DIFFERENT scope is refused (no
    # cross-scope aliasing).
    var s9 = make_scope(92, order_ptr, False)
    var sp9 = scope_ptr(s9)
    var c2 = TB.create()
    _ = sp9[].register[IntResult](tcb_ptr(c2), 7, 0)
    var cross = False
    try:
        _ = sp7a[].spawn[IntResult](rt7, tcb_ptr(c2), 0)
    except Error:
        cross = True
    if not cross:
        red("spawn did not refuse a child already scoped elsewhere")

    print("T13 scope: PASS")
