# spike/colorless_runtime/tests/t13_scope.mojo
#
# A0.9 (issue #18) — pure-Mojo scope containment + nested scopes tests
# (TDD: written RED against scope.mojo, goes GREEN once the implementation
# lands).
#
# Covers spec A0-T13/T14 (unit level; fibers NOT required):
#   - scope child registry: register/unregister TaskControlBlock handles,
#     set_scope_handle/set_parent_id round-trip, live_child_count()
#   - structured exit: close() requires zero live children, else raises
#     ChildrenStillLive; registry drops all references on close;
#     double-close raises
#   - structured order: nested scopes close inner-before-outer (recorded
#     close-order list); closing an outer scope while an inner subscope is
#     still open raises ChildrenStillLive
#   - failure policy: cancellation of siblings flows through an INJECTED
#     cancellation callback (CancelHook trait slot) — tested with a stub
#     hook recording calls; real cancel.mojo integration lands later (this
#     test deliberately does NOT import cancel.mojo or fiber.mojo)
#
# Pure Mojo: `mojo run -I spike/colorless_runtime` with no dylib.

from std.collections import List
from task import ResultValue, TaskControlBlock
from scope import CancelHook, Scope, make_nested_scope, make_scope

# Result type for the generic TCB slot (see t3_state_machine.mojo).
struct TInt(ResultValue):
    var v: Int
    def __init__(out self):
        self.v = 0

comptime TB = TaskControlBlock[TInt]


# --- stub cancellation hook -------------------------------------------------

# Records every request_cancel(scope, child) call into a caller-owned log so
# the test can assert exactly which siblings were cancelled, in order.
struct RecordingCancel(CancelHook):
    var log: UnsafePointer[List[Int], MutAnyOrigin]

    def __init__(out self, p: UnsafePointer[List[Int], MutAnyOrigin]):
        self.log = p

    def request_cancel(mut self, scope_handle: Int, child_handle: Int) raises:
        self.log[].append(child_handle)


# --- helpers ---------------------------------------------------------------

def expect(cond: Bool, what: String) raises:
    if not cond:
        raise Error("check failed: " + what)


def fresh_tcb() -> TB:
    return TB.create()

def main() raises:
    # Shared close-order log (nested-order assertions).
    var order = List[Int]()
    var order_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=order)

    # 1. Child registry: register/unregister + handle round-trip.
    var hook1 = RecordingCancel(order_ptr)  # not exercised in this section
    var s1 = make_scope[TInt, RecordingCancel](hook1, 7, order_ptr, False)
    var sp1 = UnsafePointer[Scope[TInt, RecordingCancel], MutAnyOrigin](to=s1)

    var a = fresh_tcb()
    var b = fresh_tcb()
    var c = fresh_tcb()

    var ida = sp1[0].register(ptr(a), 0)
    var idb = sp1[0].register(ptr(b), 0)
    var idc = sp1[0].register(ptr(c), 0)
    expect(ida == 1 and idb == 2 and idc == 3, "child handles assigned 1,2,3")
    expect(sp1[0].live_child_count() == 3, "three live children registered")

    # Round-trip: the scope stamped its own handle on every child.
    expect(a.scope_handle() == 7, "child A scope_handle round-trips")
    expect(b.scope_handle() == 7, "child B scope_handle round-trips")
    expect(c.scope_handle() == 7, "child C scope_handle round-trips")

    # Unregister drops exactly the requested child.
    sp1[0].unregister(idb)
    expect(sp1[0].live_child_count() == 2, "unregister drops exactly one child")
    expect(
        sp1[0].is_registered(ida) and sp1[0].is_registered(idc),
        "remaining children still registered",
    )
    expect(not sp1[0].is_registered(idb), "unregistered child gone from registry")

    # Unknown handle: unregister raises.
    var unknown_raised = False
    try:
        sp1[0].unregister(999)
    except Error:
        unknown_raised = True
    expect(unknown_raised, "unregister of unknown child handle raises")

    # 2. Parent-task-id round-trip through registration.
    var p = fresh_tcb()
    var idp = sp1[0].register(ptr(p), 42)
    expect(p.parent_id() == 42, "set_parent_id round-trips via register")
    sp1[0].unregister(idp)

    # 3. Structured exit: close() requires a drained registry.
    var close_blocked = False
    try:
        sp1[0].close()
    except Error:
        close_blocked = True
    expect(close_blocked, "close with live children raises ChildrenStillLive")
    expect(sp1[0].live_child_count() == 2, "registry intact after refused close")

    # Drain, then close succeeds.
    sp1[0].unregister(ida)
    sp1[0].unregister(idc)
    expect(sp1[0].live_child_count() == 0, "registry drained")
    sp1[0].close()
    expect(sp1[0].live_child_count() == 0, "close leaves empty registry")
    expect(not sp1[0].is_open(), "closed scope reports closed")

    var dbl_raised = False
    try:
        sp1[0].close()
    except Error:
        dbl_raised = True
    expect(dbl_raised, "double close raises")

    # 4. Structured order: nested scopes close inner-before-outer.
    #    Outer (handle 101) hosts inner (handle 102) as a subscope; both
    #    record their close order into the shared log.
    var hook4 = RecordingCancel(order_ptr)
    var outer = make_scope[TInt, RecordingCancel](hook4, 101, order_ptr, True)
    var op = UnsafePointer[Scope[TInt, RecordingCancel], MutAnyOrigin](to=outer)
    var inner = make_nested_scope[TInt, RecordingCancel](
        hook4, 102, op, order_ptr, True
    )
    _ = UnsafePointer[Scope[TInt, RecordingCancel], MutAnyOrigin](to=inner)

    # Closing the OUTER scope while the inner subscope is open is refused.
    var outer_first = False
    try:
        op[0].close()
    except Error:
        outer_first = True
    expect(outer_first, "closing outer before inner raises ChildrenStillLive")

    inner.close()
    op[0].close()
    expect(len(order) == 2, "both closes recorded")
    expect(order[0] == 102 and order[1] == 101, "inner closes before outer")

    # 5. Failure policy: first failed child triggers sibling cancellation
    #    through the INJECTED hook (stub records calls; real cancel.mojo
    #    integration lands later — NOT imported here).
    var cancels = List[Int]()
    var cancels_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=cancels)
    var hook5 = RecordingCancel(cancels_ptr)
    var s5 = make_scope[TInt, RecordingCancel](hook5, 55, cancels_ptr, False)
    var sp5 = UnsafePointer[Scope[TInt, RecordingCancel], MutAnyOrigin](to=s5)

    var x1 = fresh_tcb()
    var x2 = fresh_tcb()
    var x3 = fresh_tcb()
    var rid1 = sp5[0].register(ptr(x1), 0)
    _ = sp5[0].register(ptr(x2), 0)
    _ = sp5[0].register(ptr(x3), 0)

    # First child fails: it leaves the registry (its failure is terminal)
    # and the scope cancels every REMAINING sibling via the injected hook.
    sp5[0].unregister(rid1)
    sp5[0].request_cancel_all()
    expect(len(cancels) == 2, "hook fired once per surviving sibling")
    expect(cancels[0] == 2 and cancels[1] == 3, "siblings 2,3 cancelled in order")

    # Second pass fires only for still-live children.
    sp5[0].unregister(2)
    sp5[0].request_cancel_all()
    expect(len(cancels) == 3 and cancels[2] == 3, "second pass cancels last sibling")

    # 6. No child outlives scope storage: containment drops every reference
    #    even when callers forget to unregister individually.
    var hook6 = RecordingCancel(order_ptr)
    var s6 = make_scope[TInt, RecordingCancel](hook6, 66, order_ptr, False)
    var sp6 = UnsafePointer[Scope[TInt, RecordingCancel], MutAnyOrigin](to=s6)
    var y1 = fresh_tcb()
    var y2 = fresh_tcb()
    _ = sp6[0].register(ptr(y1), 0)
    _ = sp6[0].register(ptr(y2), 0)
    sp6[0].drop_children()
    expect(sp6[0].live_child_count() == 0, "drop_children empties the registry")
    sp6[0].close()
    expect(not sp6[0].is_registered(1), "no child reference survives close")

    print("T13 scope containment: PASS")
