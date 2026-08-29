# mojito_async/test/unit/t30_join_group.mojo
#
# A3.4 (issue #63) — grouped joins over a scope's HETEROGENEOUS children:
# `Scope.join_all()` and `Scope.first_error(rt)`, under the #42 DECISION
# (non-generic Scope, typed per-child spawn[T], erased registry,
# comptime-tagged boundary checks; decision42.md) landed by #61.
#
# RED: the driver imports the to-be-landed grouped-join surface
# (Scope.join_all, Scope.first_error) — neither exists on Scope yet.
#
# Acceptance (issue #63):
#   - join_all() over N heterogeneous children (different R per child, §96
#     fan-out shape / §113 join-in-loop): it is a WAIT-BARRIER (b2 cannot
#     express a heterogeneous collection call — §96's `profile.join()`,
#     `posts.join()`, `permissions.join()` side by side IS the grouped
#     ergonomic), never a heterogeneous collector; every child still joins
#     with its OWN static type at its own `handle.join()` call site.
#     join_all() raises ChildrenStillLive (unconsumed on refusal) while any
#     registered child has not reached COMPLETED, and is silent once every
#     child has.
#   - first_error(rt) implements the §8.2 default failure policy directly
#     on the Scope (no with_scope body wrapper required): with K children
#     where child 3 fails, it raises child 3's PRESERVED error, cancellation-
#     requests the other K-1 (asserted via their CANCELLED flags), and NEVER
#     consumes the failed child's typed result — the owning JoinHandle's own
#     join() still re-raises the identical error afterward (no double-join).
#     With no failed child it is a silent no-op (a non-failing poll).
#   - typed safety: a mismatched boundary cast (lookup[T]/close_typed[T])
#     still raises the deterministic ScopeTagMismatch AFTER join_all() has
#     been used as a barrier — the grouped ops never bypass the #42 tag
#     check (the negative test).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async import Scope
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.scope import (
    ScopeChild,
    is_children_still_live,
    is_scope_tag_mismatch,
    make_scope,
)
from mojito_async.task import execute


def red(what: String) raises -> None:
    print("T30 join-group: RED (" + what + ")")
    raise Error(what)


# ---------------------------------------------------------------------------
# Heterogeneous result types (comptime tags for the erased registry —
# decision #42 pt 3: boundary casts are comptime-tag-checked).
# ---------------------------------------------------------------------------

struct Metric(ScopeChild):
    """A join-group child outcome slot (tag 1)."""

    var v: Int

    def __init__(out self):
        self.v = 0

    def __init__(out self, x: Int):
        self.v = x

    comptime TAG = Int(1)


struct Event(ScopeChild):
    """A second, distinct-typed join-group child outcome slot (tag 2)."""

    var v: Int

    def __init__(out self):
        self.v = 0

    def __init__(out self, x: Int):
        self.v = x

    comptime TAG = Int(2)


comptime TB_M = TaskControlBlock[Metric]
comptime TB_E = TaskControlBlock[Event]


def body_metric(ud: BytePtr) raises -> Metric:
    var id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(ud))[]
    return Metric(id * 10)


def body_event(ud: BytePtr) raises -> Event:
    var id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(ud))[]
    return Event(id * 100)


def body_metric_boom(ud: BytePtr) raises -> Metric:
    raise Error("join-group boom C")


def _mp(mut tcb: TB_M) -> UnsafePointer[TB_M, MutAnyOrigin]:
    return UnsafePointer[TB_M, MutAnyOrigin](to=tcb)


def _ep(mut tcb: TB_E) -> UnsafePointer[TB_E, MutAnyOrigin]:
    return UnsafePointer[TB_E, MutAnyOrigin](to=tcb)


def main() raises:
    var rt = create()
    var idc = 0
    var idp = UnsafePointer[Int, MutAnyOrigin](to=idc).bitcast[Byte]()
    # has_log=False everywhere below: no scope in this driver needs the
    # shared close-order log, so one throwaway backing List is reused for
    # every make_scope() call (the pointer is stored but never read).
    var _log = List[Int]()
    var _logp = UnsafePointer[List[Int], MutAnyOrigin](to=_log)

    # ---- A. join_all() over N=3 heterogeneous children (§96/§113 shape) ---
    var s_a = make_scope(70, _logp, False)
    var sp_a = UnsafePointer[Scope, MutAnyOrigin](to=s_a)
    var m1 = TB_M.create()
    var e1 = TB_E.create()
    var m2 = TB_M.create()
    var hm1 = sp_a[].spawn[Metric](rt, _mp(m1), 0)
    var he1 = sp_a[].spawn[Event](rt, _ep(e1), 0)
    var hm2 = sp_a[].spawn[Metric](rt, _mp(m2), 0)
    idc = 3
    _ = execute(hm1, body_metric, idp)
    idc = 5
    _ = execute(he1, body_event, idp)
    idc = 7
    _ = execute(hm2, body_metric, idp)
    # join_all() is a silent barrier once every registered child is COMPLETED:
    sp_a[].join_all()
    if sp_a[].live_child_count() != 3:
        red("join_all() touched the registry (barrier must be read-only)")
    # each child still joins with its OWN static type (§96 fan-out shape):
    var vm1 = hm1.join()
    var ve1 = he1.join()
    var vm2 = hm2.join()
    if vm1.v != 30 or ve1.v != 500 or vm2.v != 70:
        red(
            "join_all mixed values wrong: "
            + String(vm1.v) + "/" + String(ve1.v) + "/" + String(vm2.v)
        )
    sp_a[].close(rt)
    if sp_a[].is_open():
        red("close() after join_all did not close the scope")

    # ---- B. join_all() refuses while a registered child is unfinished -----
    var s_b = make_scope(71, _logp, False)
    var sp_b = UnsafePointer[Scope, MutAnyOrigin](to=s_b)
    var m_b = TB_M.create()
    _ = sp_b[].spawn[Metric](rt, _mp(m_b), 0)
    var refused = False
    try:
        sp_b[].join_all()
    except e:
        refused = is_children_still_live(e)
    if not refused:
        red("join_all() did not refuse an unfinished registered child")
    if not sp_b[].is_open() or sp_b[].live_child_count() != 1:
        red("join_all() refusal consumed or closed the registry")
    # drive it to completion and close for a clean teardown:
    idc = 1
    var h_b = sp_b[].spawn[Metric](rt, _mp(m_b), 0)
    _ = execute(h_b, body_metric, idp)
    sp_b[].join_all()
    _ = h_b.join()
    sp_b[].close(rt)

    # ---- C. first_error(rt): K=4 children, child 3 fails ------------------
    var s_c = make_scope(72, _logp, False)
    var sp_c = UnsafePointer[Scope, MutAnyOrigin](to=s_c)
    var c0 = TB_M.create()
    var c1 = TB_E.create()
    var c2 = TB_M.create()
    var c3 = TB_E.create()
    var h0 = sp_c[].spawn[Metric](rt, _mp(c0), 0)
    var h1 = sp_c[].spawn[Event](rt, _ep(c1), 0)
    var h2 = sp_c[].spawn[Metric](rt, _mp(c2), 0)
    var h3 = sp_c[].spawn[Event](rt, _ep(c3), 0)
    # child 3 (the third spawned, h2) fails; siblings are mid-flight
    # (RUNNING) — modeled directly, same as t29's service_first_error, since
    # b2 has no in-library scheduler drive to pause a real execute() mid-way.
    idc = 6
    _ = execute(h2, body_metric_boom, idp)
    c0.transition(TaskControlBlock.RUNNING)
    c1.transition(TaskControlBlock.RUNNING)
    c3.transition(TaskControlBlock.RUNNING)
    var first_raised = False
    var first_msg = ""
    try:
        sp_c[].first_error(rt)
    except e:
        first_raised = True
        first_msg = String(e)
    if not first_raised:
        red("first_error(rt) did not raise child 3's error")
    if "join-group boom C" not in first_msg:
        red("first_error(rt) lost the primary error: " + first_msg)
    # the other K-1 siblings were cancellation-requested (asserted via flags):
    if c0.state() != TaskControlBlock.CANCELLED:
        red("sibling 0 not cancelled (state " + String(c0.state()) + ")")
    if c1.state() != TaskControlBlock.CANCELLED:
        red("sibling 1 not cancelled (state " + String(c1.state()) + ")")
    if c3.state() != TaskControlBlock.CANCELLED:
        red("sibling 3 not cancelled (state " + String(c3.state()) + ")")
    # all four settled (COMPLETED or CANCELLED) — nothing left dangling in a
    # live pre-terminal state; the refused close() fell back to
    # drop_children() (the with_scope abort escape hatch), so the scope
    # stays open with an EMPTY registry (never masking the primary error):
    if not sp_c[].is_open():
        red("first_error(rt) fully closed the scope on a refused join-all")
    if sp_c[].live_child_count() != 0:
        red("first_error(rt) did not drop the registry on a refused close")
    # no double-join: the failed child's OWN handle still re-raises the
    # IDENTICAL preserved error — first_error() never consumed it:
    var second_raised = False
    var second_msg = ""
    try:
        _ = h2.join()
    except e:
        second_raised = True
        second_msg = String(e)
    if not second_raised:
        red("h2.join() after first_error() did not re-raise")
    if "join-group boom C" not in second_msg:
        red("h2.join() after first_error() lost the error: " + second_msg)
    if "double join" in second_msg:
        red("first_error() already consumed h2 (double-join detected)")

    # ---- C2. first_error(rt) is a silent no-op with no failed child -------
    var s_c2 = make_scope(73, _logp, False)
    var sp_c2 = UnsafePointer[Scope, MutAnyOrigin](to=s_c2)
    var d0 = TB_M.create()
    var d1 = TB_E.create()
    var hd0 = sp_c2[].spawn[Metric](rt, _mp(d0), 0)
    var hd1 = sp_c2[].spawn[Event](rt, _ep(d1), 0)
    idc = 2
    _ = execute(hd0, body_metric, idp)
    idc = 4
    _ = execute(hd1, body_event, idp)
    sp_c2[].first_error(rt)
    if sp_c2[].live_child_count() != 2:
        red("first_error(rt) touched the registry with no failed child")
    sp_c2[].join_all()
    var vd0 = hd0.join()
    var vd1 = hd1.join()
    if vd0.v != 20 or vd1.v != 400:
        red("post first_error() values wrong: " + String(vd0.v) + "/" + String(vd1.v))
    sp_c2[].close(rt)

    # ---- D. typed safety: mismatched cast still raises after join_all() ---
    var s_d = make_scope(74, _logp, False)
    var sp_d = UnsafePointer[Scope, MutAnyOrigin](to=s_d)
    var g_m = TB_M.create()
    var g_e = TB_E.create()
    var hg_m = sp_d[].spawn[Metric](rt, _mp(g_m), 0)
    var hg_e = sp_d[].spawn[Event](rt, _ep(g_e), 0)
    idc = 8
    _ = execute(hg_m, body_metric, idp)
    _ = execute(hg_e, body_event, idp)
    sp_d[].join_all()
    var lookup_raised = False
    try:
        _ = sp_d[].lookup[Event](hg_m.id())
    except e:
        lookup_raised = is_scope_tag_mismatch(e)
    if not lookup_raised:
        red("lookup through the wrong child type after join_all did not raise ScopeTagMismatch")
    var reap_raised = False
    try:
        sp_d[].close_typed[Metric](rt)
    except e:
        reap_raised = is_scope_tag_mismatch(e)
    if not reap_raised:
        red("close_typed over a mixed scope after join_all did not raise ScopeTagMismatch")
    if not sp_d[].is_open() or sp_d[].live_child_count() != 2:
        red("tag-mismatch refusal consumed or closed the scope")
    sp_d[].close(rt)
    var vg_m = hg_m.join()
    var vg_e = hg_e.join()
    if vg_m.v != 80 or vg_e.v != 800:
        red("grouped mixed reap-by-handle wrong: " + String(vg_m.v) + "/" + String(vg_e.v))

    print("T30 join-group: PASS")
