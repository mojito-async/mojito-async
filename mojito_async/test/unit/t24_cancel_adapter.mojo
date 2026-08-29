# mojito_async/test/unit/t24_cancel_adapter.mojo
#
# A1.2-cancel (issue #41) — handle<->flag adapter for a scope's failure
# policy seam.
#
# A3.2 (#42 decision, issue #61): the non-generic Scope's
# request_cancel_all() now drives the ERASED TCB_Prefix directly and has no
# injected-hook seam (CancelFlagHook no longer implements a Scope trait —
# see cancellation_adapter.mojo header).  This driver exercises the adapter
# STANDALONE: a caller-owned CancelFlagRegistry maps scope handle -> scope
# flag and (scope, child) -> per-child flag; CancelFlagHook resolves the
# child handle and requests its flag, with each child flag read through to
# the scope flag (make_child_flag propagation).  Section 4 feeds the
# adapter the REAL task ids a non-generic Scope hands out from
# register[T](), so the (scope, child) handle shape stays proven against
# genuine Scope ids even though Scope no longer invokes the hook itself
# (full flag-tree wiring into Scope is lane #54, decision pt 5).
#
# Acceptance (issue #41):
#   - register scope + child; resolve BOTH directions (scope flag, child
#     flag);
#   - the hook fires the RIGHT child's flag (a sibling under the same scope is
#     untouched);
#   - child flag read-through: cancelling the scope flag cancels the child
#     checkpoint (register_child links the child under the scope flag);
#   - the adapter hook driven with a real (non-generic) Scope's register[T]
#     child ids reaches the flag tree (the handle-shape contract Scope's
#     ids satisfy, decoupled from Scope's own cancellation seam);
#   - unknown scope / unknown child refuse deterministically (documented
#     message prefixes);
#   - double-register / unregister / unregister-of-unknown refuse
#     deterministically and never leave a stale mapping behind.
#   - symmetric unregister: unregister_child / unregister_scope severs the
#     child's parent link (clear_parent), so a later scope request no longer
#     propagates into an unregistered child flag.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.cancellation import CancelFlag, make_cancel_flag
from mojito_async.cancellation_adapter import (
    CancelFlagHook,
    CancelFlagRegistry,
    make_cancel_flag_hook,
    make_cancel_flag_registry,
)
from mojito_async.integration.sys import IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.scope import Scope, make_scope


def red(what: String) raises -> None:
    print("T24 cancel-adapter: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def main() raises:
    var rt = create()

    # ---- flag cells (caller-owned; the registry never allocates cells) ----
    var scope_flag = make_cancel_flag()
    var sfp = UnsafePointer[CancelFlag, MutAnyOrigin](to=scope_flag)

    var child_a = make_cancel_flag()
    var child_b = make_cancel_flag()
    var cap = UnsafePointer[CancelFlag, MutAnyOrigin](to=child_a)
    var cbp = UnsafePointer[CancelFlag, MutAnyOrigin](to=child_b)

    # ---- registry: caller-owned handle<->flag mapping ----
    var reg = make_cancel_flag_registry()
    var rp = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=reg)

    # 1. register scope + children; resolve BOTH directions.
    rp[].register_scope(7, sfp)
    rp[].register_child(7, 10, cap)
    rp[].register_child(7, 11, cbp)

    if not rp[].has_scope(7) or not rp[].has_child(7, 10):
        red("register scope+child did not take")
    var got_scope = rp[].scope_flag_ptr(7)
    var got_child = rp[].child_flag_ptr(7, 11)
    if got_scope != sfp:
        red("scope handle did not resolve to the registered scope flag")
    if got_child != cbp:
        red("child handle did not resolve to the registered child flag")

    # 2. the hook fires the RIGHT child flag; a sibling is untouched.
    var hook = make_cancel_flag_hook(rp)
    var hptr = UnsafePointer[CancelFlagHook, MutAnyOrigin](to=hook)
    hptr[].request_cancel(7, 10)
    if not child_a.is_requested():
        red("adapter did not request the targeted child flag")
    if child_b.is_requested():
        red("adapter fired the sibling under the same scope")

    # 3. child flag read-through: cancelling the scope flag cancels the child
    # checkpoint (register_child links the child under the scope flag).
    var s2 = make_cancel_flag()
    var s2p = UnsafePointer[CancelFlag, MutAnyOrigin](to=s2)
    var c2 = make_cancel_flag()
    var c2p = UnsafePointer[CancelFlag, MutAnyOrigin](to=c2)
    var reg2 = make_cancel_flag_registry()
    var r2p = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=reg2)
    r2p[].register_scope(8, s2p)
    r2p[].register_child(8, 20, c2p)
    s2.request()
    var cp_raised = False
    try:
        c2.checkpoint()
    except Error:
        cp_raised = True
    if not cp_raised:
        red("cancelling the scope flag did not cancel the child checkpoint via read-through")

    # 4. adapter driven with the REAL task ids a non-generic Scope hands out
    # from register[T]() (#42: Scope drives request_cancel_all() through the
    # erased TCB_Prefix directly and no longer carries an injected hook —
    # this proves the adapter's (scope, child) handle contract against
    # genuine Scope ids, not a Scope-internal wiring).
    var reg3 = make_cancel_flag_registry()
    var r3p = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=reg3)
    var sf3 = make_cancel_flag()
    var sf3p = UnsafePointer[CancelFlag, MutAnyOrigin](to=sf3)
    var f1 = make_cancel_flag()
    var f1p = UnsafePointer[CancelFlag, MutAnyOrigin](to=f1)
    var f2 = make_cancel_flag()
    var f2p = UnsafePointer[CancelFlag, MutAnyOrigin](to=f2)
    r3p[].register_scope(55, sf3p)
    var hook3 = make_cancel_flag_hook(r3p)
    var hook3p = UnsafePointer[CancelFlagHook, MutAnyOrigin](to=hook3)
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var scp = make_scope(55, logp, True)
    var scpp = UnsafePointer[Scope, MutAnyOrigin](to=scp)
    var t1 = TB.create()
    var t2 = TB.create()
    var id1 = scpp[].register[IntResult](UnsafePointer[TB, MutAnyOrigin](to=t1), 1, 0)
    var id2 = scpp[].register[IntResult](UnsafePointer[TB, MutAnyOrigin](to=t2), 2, 0)
    # map the scope's real child handles to per-child flags
    r3p[].register_child(55, id1, f1p)
    r3p[].register_child(55, id2, f2p)
    hook3p[].request_cancel(55, id1)
    hook3p[].request_cancel(55, id2)
    if not f1.is_requested() or not f2.is_requested():
        red("adapter hook did not reach both child flags via the scope's real child ids")

    # 5. unknown scope / unknown child refuse deterministically.
    var scope_refused = False
    try:
        hptr[].request_cancel(999, 10)
    except Error:
        scope_refused = True
    if not scope_refused:
        red("unknown scope handle did not refuse")
    var child_refused = False
    try:
        hptr[].request_cancel(7, 999)
    except Error:
        child_refused = True
    if not child_refused:
        red("unknown child handle did not refuse")

    # 6. unregister drops the child mapping; re-unregister of a removed child
    # refuses; an untouched sibling still resolves; re-register of an already
    # mapped id refuses.
    rp[].unregister_child(7, 10)
    if rp[].has_child(7, 10):
        red("unregister did not drop the child mapping")
    var double_unreg = False
    try:
        rp[].unregister_child(7, 10)
    except Error:
        double_unreg = True
    if not double_unreg:
        red("unregister of an already-removed child did not refuse")
    if not rp[].has_child(7, 11):
        red("unregister of one child removed a sibling")
    var re_reg = False
    try:
        rp[].register_child(7, 11, cap)
    except Error:
        re_reg = True
    if not re_reg:
        red("re-register of an already-mapped child did not refuse")

    # 7. symmetric unregister: unregister_child severs the child's parent link
    # (clear_parent symmetry with register_child's set_parent), so a later
    # scope request no longer propagates into the unregistered child flag.
    var sym_scope = make_cancel_flag()
    var sym_sp = UnsafePointer[CancelFlag, MutAnyOrigin](to=sym_scope)
    var sym_child = make_cancel_flag()
    var sym_cp = UnsafePointer[CancelFlag, MutAnyOrigin](to=sym_child)
    var sym_reg = make_cancel_flag_registry()
    var sym_rp = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=sym_reg)
    sym_rp[].register_scope(77, sym_sp)
    sym_rp[].register_child(77, 771, sym_cp)  # links child under scope
    sym_scope.request()
    if not sym_child.is_requested():
        red("registered child did not read through the requested scope flag")
    sym_rp[].unregister_child(77, 771)  # severs the parent link (clear_parent)
    # Re-verify the real contract with a FRESH child: register, then
    # unregister, then request the scope — the unregistered child must NOT
    # read through any more.
    var sym2_scope = make_cancel_flag()
    var sym2_sp = UnsafePointer[CancelFlag, MutAnyOrigin](to=sym2_scope)
    var sym2_child = make_cancel_flag()
    var sym2_cp = UnsafePointer[CancelFlag, MutAnyOrigin](to=sym2_child)
    var sym2_reg = make_cancel_flag_registry()
    var sym2_rp = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=sym2_reg)
    sym2_rp[].register_scope(88, sym2_sp)
    sym2_rp[].register_child(88, 881, sym2_cp)
    sym2_rp[].unregister_child(88, 881)
    sym2_scope.request()
    if sym2_child.is_requested():
        red("unregistered child still read through the scope flag (clear_parent broken)")

    # 8. duplicate scope registration refuses; unregister of an unknown scope
    # refuses; a fresh registry starts empty.
    var fresh = make_cancel_flag_registry()
    var frp = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=fresh)
    if frp[].has_scope(1):
        red("fresh registry must have no scopes")
    var dup_scope = False
    try:
        frp[].register_scope(9, sfp)
        frp[].register_scope(9, sfp)
    except Error:
        dup_scope = True
    if not dup_scope:
        red("duplicate scope registration did not refuse")
    var unknown_scope_unreg = False
    try:
        frp[].unregister_scope(12345)
    except Error:
        unknown_scope_unreg = True
    if not unknown_scope_unreg:
        red("unregister of an unknown scope did not refuse")

    print("T24 cancel-adapter: PASS")