# mojito_async/test/unit/t29_cancel_tree_aot.mojo
#
# A3.1 (issue #54) — scope cancellation TREE: recursive descend through
# child scopes (spec §29.1), layered on the #42/#61 non-generic Scope.
#
# The tree driver exercises, END TO END, the §29.1 shape:
#
#     root scope (handle 30)
#      |
#      +-- task A            (task id 1)
#      +-- task B             (task id 2)
#      +-- child scope A1          (scope 31)
#      |    +-- task A1.1               (task id 3)
#      |    `-- task A1.2               (task id 4)
#      |
#      `-- child scope B1          (scope 32)
#           `-- task B1.1              (task id 5)
#
# Acceptance (issue #54 + the quotable body):
#   - request_cancel on the ROOT cancels tasks at depth 1, child scopes at
#     depth 2, their tasks at depth 3 (three-deep tree driver);
#   - cancel-state propagation child -> parent: cancelling a LEAF child
#     scope marks its parent's cancel state (`is_cancelled()`), per §29.1;
#   - a scope that is itself cancelled refuses new register()/spawns AND
#     refuses nesting a new child scope under it (ScopeCancelled);
#   - unregister before a cancel leaves the tree consistent: the walk never
#     re-touches an unregistered child, and still reaches the remaining
#     registered child scope;
#   - request_cancel_all is IDEMPOTENT at every node (root or leaf): a
#     repeat request is a no-op — no double-cancel, no re-drive;
#   - closing a child scope drops its cancellation-tree edge (no dangling
#     walk into a closed child on a later ancestor cancel).
#
# Observability: the non-generic Scope's request_cancel_all() (#42/#61)
# drives the erased TCB_Prefix DIRECTLY (RUNNING -> CANCELLED, WAITING
# woken) — there is no more injected CancelHook seam to observe through a
# caller-owned flag registry.  This driver follows the same convention the
# #61 landing established in t13_scope.mojo section 5: tasks are
# transitioned to RUNNING before a cancel so the state transition is
# directly observable via `TaskControlBlock.state()`; scope-level state is
# observed via `Scope.is_cancelled()`.
#
# The tree is pure Mojo (no fiber/stack externs), so this driver MAY run JIT
# or AOT; it is shipped as *_aot.mojo (built + executed) to match the
# cancel-adjacent driver convention (t24_cancel_adapter is JIT; this one is
# the tree lane's AOT proof).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.integration.sys import IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.scope import Scope, make_nested_scope, make_scope


def red(what: String) raises -> None:
    print("T29 cancel-tree: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def tcb_ptr(mut t: TB) -> UnsafePointer[TB, MutAnyOrigin]:
    return UnsafePointer[TB, MutAnyOrigin](to=t)


def scope_ptr(mut s: Scope) -> UnsafePointer[Scope, MutAnyOrigin]:
    return UnsafePointer[Scope, MutAnyOrigin](to=s)


def running(mut t: TB) raises:
    """Drive a fresh TCB to RUNNING so request_cancel_all's erased-prefix
    drive (RUNNING -> CANCELLED) is directly observable (matches the
    convention t13_scope.mojo section 5 established for the #61 landing)."""
    t.transition(TaskControlBlock.RUNNABLE)
    t.transition(TaskControlBlock.RUNNING)


def main() raises:
    # =====================================================================
    # 1. THREE-DEEP TREE, ROOT CANCEL (acceptance 1):
    #    request_cancel on the root reaches depth-1 tasks, depth-2 child
    #    scopes, AND depth-3 tasks (recursive descend).
    # =====================================================================
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)

    var root = make_scope(30, logp, True)
    var rootp = scope_ptr(root)
    var s_a1 = make_nested_scope(31, rootp, logp, True)
    var sa1p = scope_ptr(s_a1)
    var s_b1 = make_nested_scope(32, rootp, logp, True)
    var sb1p = scope_ptr(s_b1)

    var t_a = TB.create()
    var t_b = TB.create()
    var t_11 = TB.create()
    var t_12 = TB.create()
    var t_21 = TB.create()
    running(t_a)
    running(t_b)
    running(t_11)
    running(t_12)
    running(t_21)
    _ = rootp[].register[IntResult](tcb_ptr(t_a), 1, 0)
    _ = rootp[].register[IntResult](tcb_ptr(t_b), 2, 0)
    _ = sa1p[].register[IntResult](tcb_ptr(t_11), 3, 0)
    _ = sa1p[].register[IntResult](tcb_ptr(t_12), 4, 0)
    _ = sb1p[].register[IntResult](tcb_ptr(t_21), 5, 0)

    # ---- THE ROOT CANCEL --------------------------------------------------
    rootp[].request_cancel_all()
    # depth-1 tasks cancelled
    if t_a.state() != TaskControlBlock.CANCELLED or t_b.state() != TaskControlBlock.CANCELLED:
        red("root cancel did not reach depth-1 task state")
    # depth-2 child scopes cancelled
    if not sa1p[].is_cancelled() or not sb1p[].is_cancelled():
        red("root cancel did not reach depth-2 child-scope state")
    # root itself is cancelled
    if not rootp[].is_cancelled():
        red("root cancel did not mark the root's own state")
    # depth-3 tasks cancelled
    if t_11.state() != TaskControlBlock.CANCELLED or t_12.state() != TaskControlBlock.CANCELLED:
        red("root cancel did not reach depth-3 task state in A1")
    if t_21.state() != TaskControlBlock.CANCELLED:
        red("root cancel did not reach depth-3 task state in B1")
    # a repeat root cancel is a NO-OP (no double-cancel, no re-drive) — the
    # registries are unchanged and everything stays cancelled.
    rootp[].request_cancel_all()
    if t_11.state() != TaskControlBlock.CANCELLED or t_12.state() != TaskControlBlock.CANCELLED:
        red("second root cancel lost an already-cancelled descendant")
    if rootp[].live_child_count() != 2 or rootp[].open_subscopes() != 2:
        red("repeat cancel mutated the scope registries")

    # =====================================================================
    # 2. LEAF CANCEL MARKS THE PARENT (acceptance 2, child -> parent):
    #    cancelling leaf scope 41 (C1) marks ROOT2's cancel state.
    # =====================================================================
    var log2 = List[Int]()
    var log2p = UnsafePointer[List[Int], MutAnyOrigin](to=log2)
    var root2 = make_scope(40, log2p, True)
    var root2p = scope_ptr(root2)
    var s_c1 = make_nested_scope(41, root2p, log2p, True)
    var sc1p = scope_ptr(s_c1)
    var t_c21 = TB.create()
    running(t_c21)
    _ = sc1p[].register[IntResult](tcb_ptr(t_c21), 6, 0)
    # cancel the LEAF scope only.
    sc1p[].request_cancel_all()
    if t_c21.state() != TaskControlBlock.CANCELLED:
        red("leaf cancel did not reach the leaf's own task")
    if not sc1p[].is_cancelled():
        red("leaf scope's own state not set by its own cancel")
    if not root2p[].is_cancelled():
        red("cancelled leaf scope did not mark its parent's cancel state")

    # =====================================================================
    # 3. CANCELLED SCOPE REFUSES NEW SPAWNS (acceptance 3):
    #    register() into a cancelled (still open) scope refuses; nesting a
    #    new scope under a cancelled parent refuses too.  A repeat cancel
    #    of an already-cancelled scope (root or leaf) is a no-op.
    # =====================================================================
    var root3 = make_scope(50, log2p, True)
    var root3p = scope_ptr(root3)
    # a CLEAN scope accepts registration.
    var t_z = TB.create()
    _ = root3p[].register[IntResult](tcb_ptr(t_z), 7, 0)
    root3p[].request_cancel_all()
    # register() into a CANCELLED (still open) scope refuses, tagged
    # ScopeCancelled.
    var refused2 = False
    try:
        var t_z2 = TB.create()
        _ = root3p[].register[IntResult](tcb_ptr(t_z2), 8, 0)
    except Error:
        refused2 = True
    if not refused2:
        red("register into a cancelled scope did not refuse")
    var nested_refused = False
    try:
        var s_bad = make_nested_scope(52, root3p, log2p, True)
    except Error:
        nested_refused = True
    if not nested_refused:
        red("nesting a scope under a cancelled parent did not refuse")

    # repeat cancel of the section-2 leaf is a no-op: the task stays
    # cancelled, the scope stays cancelled.
    sc1p[].request_cancel_all()
    if t_c21.state() != TaskControlBlock.CANCELLED:
        red("repeat leaf cancel lost an already-cancelled task state")
    if not sc1p[].is_cancelled():
        red("repeat leaf cancel lost the leaf's own cancel state")

    # =====================================================================
    # 4. UNREGISTER CONSISTENCY (acceptance 4):
    #    unregister a task child BEFORE the cancel walk; the walk stays
    #    sound, reaches the still-registered child scope, and does NOT
    #    touch the unregistered task.
    # =====================================================================
    var log4 = List[Int]()
    var log4p = UnsafePointer[List[Int], MutAnyOrigin](to=log4)
    var root4 = make_scope(60, log4p, True)
    var root4p = scope_ptr(root4)
    var s_e1 = make_nested_scope(61, root4p, log4p, True)
    var se1p = scope_ptr(s_e1)
    var t_u = TB.create()
    running(t_u)
    var t_e11 = TB.create()
    running(t_e11)
    var id_u = root4p[].register[IntResult](tcb_ptr(t_u), 9, 0)
    _ = se1p[].register[IntResult](tcb_ptr(t_e11), 10, 0)
    # unregister the task child, then cancel.
    root4p[].unregister(id_u)
    root4p[].request_cancel_all()
    if t_u.state() == TaskControlBlock.CANCELLED:
        red("unregistered child was re-cancelled by the tree walk")
    if t_e11.state() != TaskControlBlock.CANCELLED:
        red("cancel walk did not reach the still-registered child scope")
    if not se1p[].is_cancelled():
        red("cancel walk did not mark the still-registered child scope")
    if not root4p[].is_cancelled():
        red("cancel walk did not mark the root scope's own state")
    # a repeat cancel afterward is a no-op (already cancelled).
    root4p[].request_cancel_all()
    if not se1p[].is_cancelled():
        red("repeat cancel after unregister lost the child scope's state")

    # =====================================================================
    # 5. CLOSE DROPS THE TREE EDGE (acceptance 5):
    #    closing the child scope drops its tree edge (`open_subscopes` -> 0,
    #    no child-scope edge in the parent); a LATER ancestor cancel does
    #    not dangle on / re-descend into the closed child.
    # =====================================================================
    var rt5 = create()
    var log5 = List[Int]()
    var log5p = UnsafePointer[List[Int], MutAnyOrigin](to=log5)
    var root5 = make_scope(70, log5p, True)
    var root5p = scope_ptr(root5)
    var s_f1 = make_nested_scope(71, root5p, log5p, True)
    # close the child scope (no live children in it); the tree edge must
    # drop (open_subscopes -> 0).
    s_f1.close(rt5)
    if root5p[].open_subscopes() != 0:
        red("child close did not decrement the parent's open-subscope count")
    if root5p[].has_child_scope(71):
        red("child close did not drop the parent's child-scope tree edge")
    # a ROOT cancel after the child closed must not dangle on the closed
    # child (no crash, no exception) and must still mark the root cancelled.
    root5p[].request_cancel_all()
    if not root5p[].is_cancelled():
        red("root cancel after child close did not mark the root state")

    print("T29 cancel-tree: PASS")
