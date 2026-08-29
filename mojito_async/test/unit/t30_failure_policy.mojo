# mojito_async/test/unit/t30_failure_policy.mojo
#
# A3.5 (issue #64) — scope failure policy: record primary, cancel siblings,
# join, raise (spec §8.2 first-error semantics, made deterministic and
# exactly-once).
#
# A3.2 (#42 decision, issue #61): migrated onto the NON-GENERIC Scope —
# `make_scope(handle, order_log, has_log)` (no type parameters),
# `register[T](child, task_id, parent_task_id)`.  The failure-policy fields
# (`_primary_handle`/`_primary_msg`/`_suppressed`/`_failed`/`_raised`) live
# directly on the non-generic Scope; `record_failure[H: CancelHook]` stays a
# PER-CALL generic method (mirroring register[T]/spawn[T]) rather than a
# stored generic hook field — the STRUCT stays non-generic (#42 pt 1), only
# the method is parametrically polymorphic over the caller-supplied hook.
#
# Acceptance:
#   (a) first-RECORDED wins: the primary is the error of the FIRST child
#       whose failure the scope machinery RECORDS — NOT the first child to
#       reach COMPLETED (documented ordering: first-RECORDED, not
#       first-finished).  The primary survives the join and is raised at the
#       boundary;
#   (b) cancel-on-failure: a failing child triggers sibling cancellation
#       (through the injected CancelHook — the cancel-tree #54-ready seam);
#       the failed child itself is not re-cancelled; the sibling's
#       cancellation flag is OBSERVED before the sibling completes;
#   (c) exactly-once raise: close() / raise_primary() surface the primary
#       exactly once (consumed on raise); a second boundary never re-raises
#       the primary;
#   (d) suppressed count: later failures are recorded-but-not-primary and
#       stay observable as counters.
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
from mojito_async.scope import (
    CancelHook,
    Scope,
    make_nested_scope,
    make_scope,
)


def red(what: String) raises -> None:
    print("T30 failure-policy: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


# --- recording cancellation hook (asserts the exact cancel set) ------------

struct RecordingCancel(CancelHook):
    var log: UnsafePointer[List[Int], MutAnyOrigin]

    def __init__(out self, p: UnsafePointer[List[Int], MutAnyOrigin]):
        self.log = p

    def request_cancel(mut self, scope_handle: Int, child_handle: Int) raises:
        self.log[].append(child_handle)


def tcb_ptr(mut t: TB) -> UnsafePointer[TB, MutAnyOrigin]:
    return UnsafePointer[TB, MutAnyOrigin](to=t)


def scope_ptr(mut s: Scope) -> UnsafePointer[Scope, MutAnyOrigin]:
    return UnsafePointer[Scope, MutAnyOrigin](to=s)


# --- choreography helpers (deterministic TCB walks) ------------------------
# These mirror what the engine does for real (task.mojo execute / cancel
# paths): a failed child reaches COMPLETED with no stored result; a cancelled
# child walks RUNNING -> CANCELLED -> COMPLETED and never completes normally.

def complete_failed(mut t: TB) raises:
    """The execute() failure path: the child body raised, so the TCB reaches
    COMPLETED without a stored result (the error lives on the handle)."""
    t.transition(TaskControlBlock.RUNNABLE)
    t.transition(TaskControlBlock.RUNNING)
    t.transition(TaskControlBlock.COMPLETED)


def complete_clean(mut t: TB, v: Int) raises:
    """A child that finishes normally: COMPLETED with a stored result."""
    t.transition(TaskControlBlock.RUNNABLE)
    t.transition(TaskControlBlock.RUNNING)
    t.transition(TaskControlBlock.COMPLETED)
    t.mark_result(IntResult(v))


def cancelled_then_completed(mut t: TB) raises:
    """A sibling that observed cancellation before finishing: RUNNING ->
    CANCELLED -> COMPLETED (never completes normally)."""
    t.transition(TaskControlBlock.RUNNABLE)
    t.transition(TaskControlBlock.RUNNING)
    t.transition(TaskControlBlock.CANCELLED)
    t.transition(TaskControlBlock.COMPLETED)


def main() raises:
    var rt = create()

    # ------------------------------------------------------------------
    # (a) first-RECORDED wins; primary survives the join; sibling-targeted
    #     cancel; suppressed count for a later failure; exactly-once close.
    # ------------------------------------------------------------------
    var log = List[Int]()
    var log_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var hook = RecordingCancel(log_ptr)
    var s1 = make_scope(501, log_ptr, False)
    var sp1 = scope_ptr(s1)
    var a = TB.create()
    var b = TB.create()
    var ida = sp1[].register[IntResult](tcb_ptr(a), 1, 0)
    var idb = sp1[].register[IntResult](tcb_ptr(b), 2, 0)

    # B fails-and-finishes FIRST; A's failure is then RECORDED first.  The
    # documented ordering is first-RECORDED, not first-finished, so the
    # primary is A's error even though B reached COMPLETED earlier.
    complete_failed(b)
    complete_failed(a)
    sp1[].record_failure(ida, "A boom", hook)
    if sp1[].has_primary_error() != True:
        red("primary not recorded on first failure")
    if sp1[].failed_count() != 1:
        red("failed_count must count the primary")
    # cancel fired exactly once: the sibling B, never the failed child A.
    if len(log) != 1 or log[0] != idb:
        red("first failure must cancel the sibling exactly once, not the failed child")
    # a later failure is recorded-but-not-primary.
    sp1[].record_failure(idb, "B boom", hook)
    if sp1[].suppressed_count() != 1:
        red("later failure did not bump the suppressed count")
    if sp1[].failed_count() != 2:
        red("failed_count must count every recorded failure (no error is lost)")
    if len(log) != 1:
        red("suppressed failure must not re-cancel siblings")
    # boundary: close() JOINS everything (both COMPLETED), then raises A's
    # error exactly once.
    var raised_a = ""
    try:
        sp1[].close(rt)
    except e:
        raised_a = String(e)
    if raised_a != "A boom":
        red("close must raise the primary error once; got: '" + raised_a + "'")
    if sp1[].is_open():
        red("close did not complete after raising the primary")
    # exactly-once: a second close does NOT re-raise the primary.
    var raised2 = ""
    try:
        sp1[].close(rt)
    except e:
        raised2 = String(e)
    if "A boom" in raised2:
        red("second close re-raised the primary (must be exactly-once)")
    if not ("DoubleClose" in raised2):
        red("second close must refuse (not re-raise the primary); got: '" + raised2 + "'")
    if sp1[].suppressed_count() != 1:
        red("suppressed count must stay observable after the boundary")

    # ------------------------------------------------------------------
    # (b) cancel-on-failure stops siblings: the sibling's cancellation flag
    #     is OBSERVED (requested) before it completes, through the real
    #     shipped CancelFlagHook adapter.
    # ------------------------------------------------------------------
    var reg = make_cancel_flag_registry()
    var rp = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=reg)
    var sf = make_cancel_flag()
    var sfp = UnsafePointer[CancelFlag, MutAnyOrigin](to=sf)
    var fa = make_cancel_flag()
    var fap = UnsafePointer[CancelFlag, MutAnyOrigin](to=fa)
    var fb = make_cancel_flag()
    var fbp = UnsafePointer[CancelFlag, MutAnyOrigin](to=fb)
    rp[].register_scope(502, sfp)
    var hookb = make_cancel_flag_hook(rp)
    var log2 = List[Int]()
    var log2_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=log2)
    var s2 = make_scope(502, log2_ptr, False)
    var sp2 = scope_ptr(s2)
    var a2 = TB.create()
    var b2 = TB.create()
    var ida2 = sp2[].register[IntResult](tcb_ptr(a2), 3, 0)
    var idb2 = sp2[].register[IntResult](tcb_ptr(b2), 4, 0)
    rp[].register_child(502, ida2, fap)
    rp[].register_child(502, idb2, fbp)
    sp2[].record_failure(ida2, "fail A", hookb)
    if not fb.is_requested():
        red("sibling flag not requested on primary failure (cancel-on-failure)")
    if fa.is_requested():
        red("failed child's own flag must not be re-cancelled")
    # B observes the flag: a cooperative checkpoint now raises.
    var cp = False
    try:
        fb.checkpoint()
    except Error:
        cp = True
    if not cp:
        red("sibling checkpoint did not observe the cancellation")
    # B is stopped by cancellation, never completing normally.
    cancelled_then_completed(b2)
    complete_failed(a2)
    var raised_b = ""
    try:
        sp2[].close(rt)
    except e:
        raised_b = String(e)
    if raised_b != "fail A":
        red("close must raise the primary once (flag scene); got: '" + raised_b + "'")
    if sp2[].suppressed_count() != 0:
        red("cleanly-cancelled sibling must not count as suppressed")

    # ------------------------------------------------------------------
    # (c) exactly-once raise on the DEFERRED surface (raise_primary): the
    #     first_error-style boundary may raise at the caller's chosen point;
    #     a second raise is a no-op and the later close never re-raises.
    # ------------------------------------------------------------------
    var log3 = List[Int]()
    var log3_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=log3)
    var hook3 = RecordingCancel(log3_ptr)
    var s3 = make_scope(503, log3_ptr, False)
    var sp3 = scope_ptr(s3)
    var c1 = TB.create()
    var c2 = TB.create()
    var idc1 = sp3[].register[IntResult](tcb_ptr(c1), 5, 0)
    var idc2 = sp3[].register[IntResult](tcb_ptr(c2), 6, 0)
    complete_failed(c1)
    complete_clean(c2, 1)
    sp3[].record_failure(idc1, "deferred boom", hook3)
    if sp3[].has_primary_error() != True:
        red("primary missing before deferred raise")
    var r1 = ""
    try:
        sp3[].raise_primary()
    except e:
        r1 = String(e)
    if r1 != "deferred boom":
        red("raise_primary did not surface the primary once; got: '" + r1 + "'")
    if sp3[].has_primary_error() != False:
        red("raise_primary must consume the primary (exactly-once)")
    var r2 = ""
    try:
        sp3[].raise_primary()
    except e:
        r2 = String(e)
    if r2 != "":
        red("second raise_primary must be a no-op; got: '" + r2 + "'")
    if sp3[].suppressed_count() != 0:
        red("primary-only failure must leave suppressed at 0")
    # the boundary then completes WITHOUT re-raising (primary already
    # consumed).
    var closed_after_raise = True
    try:
        sp3[].close(rt)
    except Error:
        closed_after_raise = False
    if not closed_after_raise:
        red("close after consumed primary must complete without re-raising")
    # a scope with no primary raises nothing on close.
    var log4 = List[Int]()
    var log4_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=log4)
    var hook4 = RecordingCancel(log4_ptr)
    var s4 = make_scope(504, log4_ptr, False)
    var sp4 = scope_ptr(s4)
    var d1 = TB.create()
    _ = sp4[].register[IntResult](tcb_ptr(d1), 7, 0)
    complete_clean(d1, 5)
    var closed_clean = True
    try:
        sp4[].close(rt)
    except Error:
        closed_clean = False
    if not closed_clean:
        red("close with no recorded failure raised")

    # ------------------------------------------------------------------
    # (d) suppressed count with several failures; deterministic target set;
    #     unknown-child record refuses; the boundary surfaces ONLY the
    #     first-recorded error.
    # ------------------------------------------------------------------
    var log5 = List[Int]()
    var log5_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=log5)
    var hook5 = RecordingCancel(log5_ptr)
    var s5 = make_scope(505, log5_ptr, False)
    var sp5 = scope_ptr(s5)
    var e1 = TB.create()
    var e2 = TB.create()
    var e3 = TB.create()
    var ide1 = sp5[].register[IntResult](tcb_ptr(e1), 8, 0)
    var ide2 = sp5[].register[IntResult](tcb_ptr(e2), 9, 0)
    var ide3 = sp5[].register[IntResult](tcb_ptr(e3), 10, 0)
    # unknown-child record refuses.
    var refused = False
    try:
        sp5[].record_failure(9999, "ghost", hook5)
    except Error:
        refused = True
    if not refused:
        red("record_failure of an unknown child must refuse")
    complete_failed(e1)
    complete_failed(e2)
    complete_failed(e3)
    sp5[].record_failure(ide2, "E2 first", hook5)
    sp5[].record_failure(ide1, "E1 second", hook5)
    sp5[].record_failure(ide3, "E3 third", hook5)
    if sp5[].failed_count() != 3:
        red("failed_count must total every recorded failure")
    if sp5[].suppressed_count() != 2:
        red("two later failures must be suppressed")
    # first record (E2) cancelled the OTHER two siblings (E1, E3), never E2
    # itself; later records do not re-cancel.
    if len(log5) != 2:
        red("only the FIRST record_failure may cancel siblings")
    if ide2 in log5:
        red("the failed child that RECORDED first must not cancel itself")
    var raised5 = ""
    try:
        sp5[].close(rt)
    except e:
        raised5 = String(e)
    if raised5 != "E2 first":
        red("boundary must surface ONLY the first-recorded error; got: '" + raised5 + "'")

    # ------------------------------------------------------------------
    # (e) M4 regression (A3 consensus review): record_failure's
    #     cooperative cancel must reach the FULL #54 tree recursively,
    #     not just direct siblings -- a nested subscope beneath the
    #     failed child's OWN sibling scope must also observe the
    #     request.  Also exercises M3: close() on a scope with a
    #     (now-drained) nested child still surfaces the recorded
    #     primary rather than losing it in the drop_children() abort
    #     path.
    # ------------------------------------------------------------------
    var log6 = List[Int]()
    var log6_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=log6)
    var hook6 = RecordingCancel(log6_ptr)
    var s6 = make_scope(601, log6_ptr, False)
    var sp6 = scope_ptr(s6)
    var f1 = TB.create()  # the child that fails
    var g1 = TB.create()  # a direct sibling task of f1
    var s6n = make_nested_scope(602, sp6, log6_ptr, False)
    var sp6n = scope_ptr(s6n)
    var h1 = TB.create()  # nested TWO levels below f1: inside f1's sibling scope
    var idf1 = sp6[].register[IntResult](tcb_ptr(f1), 20, 0)
    var idg1 = sp6[].register[IntResult](tcb_ptr(g1), 21, 0)
    var idh1 = sp6n[].register[IntResult](tcb_ptr(h1), 22, 0)
    complete_failed(f1)
    sp6[].record_failure(idf1, "nested boom", hook6)
    if idh1 not in log6:
        red("record_failure must recursively reach a nested child scope's"
            " tasks (M4)")
    if idg1 not in log6:
        red("record_failure must still cancel the direct sibling task"
            " (M4 regression guard)")
    if idf1 in log6:
        red("record_failure must never cancel the failed child itself")
    complete_clean(g1, 0)
    complete_clean(h1, 0)
    sp6n[].close(rt)
    var raised6 = ""
    try:
        sp6[].close(rt)
    except e:
        raised6 = String(e)
    if raised6 != "nested boom":
        red("boundary must surface the recorded primary through a scope"
            " with a (drained) nested child (M3/M4); got: '" + raised6 + "'")

    print("T30 failure-policy: PASS")
