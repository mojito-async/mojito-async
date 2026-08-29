# mojito_async/test/unit/t29_with_scope.mojo
#
# A3.2 (issue #61) — with-Scope root ergonomics + §113 prototype acceptance
# driver, under the #42 DECISION (non-generic Scope + typed per-child spawn,
# erased registry, comptime-tagged boundary checks; decision42.md).
#
# The spec §113 prototype ("with Scope() as scope: ... scope.spawn ...") is
# transcribed onto the b2 surface: b2 has no `with` context managers and no
# lambda/closure capture, so the ROOT ergonomic wrapper
#   with_scope(rt, body, ud)
# creates the root scope, runs `body(rt, scope, ud)`, and joins it —
# spec §13/§113 mandate the same shape; the driver's `service_113` body
# mirrors the §113 example body-for-body (spawn-in-loop, join-in-loop).
#
# Acceptance (issue #61 + #42):
#   - with_scope creates the root scope, runs the body, closes (joins) it on
#     normal return;
#   - a body error propagates as the FIRST error; mid-flight siblings are
#     cancellation-requested (erased-prefix request_cancel_all: RUNNING ->
#     CANCELLED; WAITING woken) before close; the primary error is NEVER
#     masked by teardown errors (refused close -> drop_children abort);
#   - MIXED-TYPE children (Profile + Activity) live in ONE non-generic Scope
#     and join with their STATIC types (scope.spawn[T] -> JoinHandle[T]);
#   - homogeneous typed reap: close_typed[T] tag-checks every child and
#     consume-once takes each settled result (decision #42 pt 4);
#   - any WRONG-TYPE boundary cast raises a deterministic ScopeTagMismatch
#     (the negative test; prefix decode via is_scope_tag_mismatch);
#   - the TCB's R-free prefix (state/generation/wait/failure stamp/parent/
#     scope) is readable ERASED through the registry (uniform-prefix layout,
#     decision #42 pt 6) — the erased-prefix access is what lets a
#     non-generic Scope cancel/validate without knowing child types.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async import Scope, JoinHandle, with_scope
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.scope import (
    ScopeChild,
    is_scope_tag_mismatch,
    make_nested_scope,
    make_scope,
)
from mojito_async.task import execute


def red(what: String) raises -> None:
    print("T29 with-scope: RED (" + what + ")")
    raise Error(what)


# ---------------------------------------------------------------------------
# §113 result types (mixed typed children; comptime tags for the erased
# registry — decision #42 pt 3: boundary casts are comptime-tag-checked).
# ---------------------------------------------------------------------------

struct Profile(ScopeChild):
    """A §113 'fetch' outcome slot (tag 1)."""

    var v: Int

    def __init__(out self):
        self.v = 0

    def __init__(out self, x: Int):
        self.v = x

    comptime TAG = Int(1)


struct Activity(ScopeChild):
    """A second mixed-type child outcome slot (tag 2)."""

    var v: Int

    def __init__(out self):
        self.v = 0

    def __init__(out self, x: Int):
        self.v = x

    comptime TAG = Int(2)


comptime TB_P = TaskControlBlock[Profile]
comptime TB_A = TaskControlBlock[Activity]


# ---------------------------------------------------------------------------
# §113 task bodies (all plain `def`, no async/await/Future — ordinary
# compiled Mojo; id flows through the caller-allocated userdata cell).
# ---------------------------------------------------------------------------

def body_fetch(ud: BytePtr) raises -> Profile:
    """Transcription of §113 `fetch(id)`: reads its id from the ud cell."""
    var id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(ud))[]
    return Profile(id * 10)


def body_track(ud: BytePtr) raises -> Activity:
    """Second mixed child body: reads its id from the ud cell."""
    var id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(ud))[]
    return Activity(id * 100)


def body_boom(ud: BytePtr) raises -> Profile:
    """Failing child body: settles COMPLETED with a preserved error (the
    execute() failure path); join() re-raises it."""
    raise Error("proto boom")


# ---------------------------------------------------------------------------
# Scene — caller-allocated choreography cells (the b2 userdata pattern;
# mirrors t13_stress's Scene).  `cells` owns per-child TCB cells so the
# with_scope bodies can spawn/join through the scope + drive via execute;
# `args` are the per-child id cells consumed by the §113 bodies.
# ---------------------------------------------------------------------------

struct Scene(Movable, ImplicitlyDeletable):
    var cells: UnsafePointer[List[TB_P], MutAnyOrigin]
    var act: UnsafePointer[TB_A, MutAnyOrigin]
    var args: UnsafePointer[List[Int], MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var total: UnsafePointer[Int, MutAnyOrigin]

    def __init__(
        out self,
        cells: UnsafePointer[List[TB_P], MutAnyOrigin],
        act: UnsafePointer[TB_A, MutAnyOrigin],
        args: UnsafePointer[List[Int], MutAnyOrigin],
        n: UnsafePointer[Int, MutAnyOrigin],
        total: UnsafePointer[Int, MutAnyOrigin],
    ):
        self.cells = cells
        self.act = act
        self.args = args
        self.n = n
        self.total = total


# ---------------------------------------------------------------------------
# §113-shaped body: spawn-in-loop -> drive-in-loop -> join-in-loop.
# ---------------------------------------------------------------------------

def service_113(mut rt: Runtime, sp: UnsafePointer[Scope, MutAnyOrigin], ud: BytePtr) raises:
    """Transcribes §113's example body-for-body:
    `for id in ids: handles.append(scope.spawn(...)); ... results.append(
    handle^.join())` — spawn-in-loop, join-in-loop, results summed."""
    var sc = UnsafePointer[Scene, MutAnyOrigin](unsafe_from_address=Int(ud))
    var ids = List[Int]()
    ids.append(3)
    ids.append(7)
    for i in range(len(ids)):
        sc[].args[][i] = ids[i]

    var handles = List[JoinHandle[Profile]]()
    for i in range(len(sc[].cells[])):
        handles.append(
            sp[].spawn[Profile](
                rt, UnsafePointer[TB_P, MutAnyOrigin](to=sc[].cells[][i]), 0
            )
        )

    for i in range(len(handles)):
        var h = handles[i]
        var id_cell = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(sc[].args[].unsafe_ptr()) + i * 8
        )
        _ = execute(h, body_fetch, id_cell.bitcast[Byte]())

    var total = 0
    for i in range(len(handles)):
        var res = handles[i].join()
        total += res.v
    sc[].total[] = total
    sc[].n[] = len(handles)


# ---------------------------------------------------------------------------
# with_scope normal-return body: MIXED children join with static types.
# ---------------------------------------------------------------------------

def service_mixed(mut rt: Runtime, sp: UnsafePointer[Scope, MutAnyOrigin], ud: BytePtr) raises:
    var sc = UnsafePointer[Scene, MutAnyOrigin](unsafe_from_address=Int(ud))
    var id_cell = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].args[].unsafe_ptr()) + 0 * 8
    )
    id_cell[] = 5
    var hp = sp[].spawn[Profile](
        rt, UnsafePointer[TB_P, MutAnyOrigin](to=sc[].cells[][0]), 0
    )
    var ha = sp[].spawn[Activity](
        rt, UnsafePointer[TB_A, MutAnyOrigin](to=sc[].act[]), 0
    )
    _ = execute(hp, body_fetch, id_cell.bitcast[Byte]())
    _ = execute(ha, body_track, id_cell.bitcast[Byte]())
    # typed joins: Profile + Activity static types side by side (acceptance).
    var pv = hp.join()
    var av = ha.join()
    if pv.v != 50 or av.v != 500:
        red("mixed join values wrong: " + String(pv.v) + "/" + String(av.v))
    sc[].total[] = pv.v + av.v


# ---------------------------------------------------------------------------
# with_scope first-error body: a failing child is joined first -> the body
# raises the FIRST error; the sibling is mid-flight (RUNNING) and must be
# cancellation-requested before the teardown re-raises the primary error.
# ---------------------------------------------------------------------------

def service_first_error(
    mut rt: Runtime, sp: UnsafePointer[Scope, MutAnyOrigin], ud: BytePtr
) raises:
    var sc = UnsafePointer[Scene, MutAnyOrigin](unsafe_from_address=Int(ud))
    var hp = sp[].spawn[Profile](
        rt, UnsafePointer[TB_P, MutAnyOrigin](to=sc[].cells[][0]), 0
    )
    var ha = sp[].spawn[Activity](
        rt, UnsafePointer[TB_A, MutAnyOrigin](to=sc[].act[]), 0
    )
    _ = execute(hp, body_boom, UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x33))
    # the Activity sibling is mid-flight on the cooperative worker: model its
    # RUNNING frame directly (it would otherwise keep running in the
    # embedding scheduler loop; on b2 the RUNNING record is the observable).
    sc[].act[].transition(TaskControlBlock.RUNNING)
    # the body's join of the FIRST child raises the primary error:
    _ = hp.join()


def main() raises:
    var rt = create()

    # ---- A. §113 prototype: spawn-in-loop + join-in-loop, normal return ---
    var cells_a = List[TB_P]()
    cells_a.append(TB_P.create())
    cells_a.append(TB_P.create())
    var act_a = TB_A.create()
    var args_a = List[Int]()
    args_a.append(0)
    args_a.append(0)
    var n_a = 0
    var total_a = 0
    var scene_a = Scene(
        UnsafePointer[List[TB_P], MutAnyOrigin](to=cells_a),
        UnsafePointer[TB_A, MutAnyOrigin](to=act_a),
        UnsafePointer[List[Int], MutAnyOrigin](to=args_a),
        UnsafePointer[Int, MutAnyOrigin](to=n_a),
        UnsafePointer[Int, MutAnyOrigin](to=total_a),
    )
    var ud_a = UnsafePointer[Scene, MutAnyOrigin](to=scene_a).bitcast[Byte]()
    with_scope(rt, service_113, ud_a)
    if total_a != 30 + 70:
        red("§113 body total wrong: " + String(total_a))
    if n_a != 2:
        red("§113 body did not join exactly 2 children")

    # ---- B. with_scope on a closed runtime refuses --------------------------
    var rt2 = create()
    rt2.shutdown()
    var closed_raised = False
    try:
        with_scope(rt2, service_113, ud_a)
    except Error:
        closed_raised = True
    if not closed_raised:
        red("with_scope on a shut-down runtime did not refuse")

    # ---- C. FIRST ERROR propagation + sibling cancellation-request ---------
    var cells_c = List[TB_P]()
    cells_c.append(TB_P.create())
    var act_c = TB_A.create()
    var args_c = List[Int]()
    args_c.append(0)
    var n_c = 0
    var total_c = 0
    var scene_c = Scene(
        UnsafePointer[List[TB_P], MutAnyOrigin](to=cells_c),
        UnsafePointer[TB_A, MutAnyOrigin](to=act_c),
        UnsafePointer[List[Int], MutAnyOrigin](to=args_c),
        UnsafePointer[Int, MutAnyOrigin](to=n_c),
        UnsafePointer[Int, MutAnyOrigin](to=total_c),
    )
    var ud_c = UnsafePointer[Scene, MutAnyOrigin](to=scene_c).bitcast[Byte]()
    var first_raised = False
    var first_msg = ""
    try:
        with_scope(rt, service_first_error, ud_c)
    except e:
        first_raised = True
        first_msg = String(e)
    if not first_raised:
        red("with_scope did not propagate the body's first error")
    if "proto boom" not in first_msg:
        red("with_scope lost the primary error: " + first_msg)
    # the mid-flight sibling was cancellation-requested through the erased
    # prefix (RUNNING -> CANCELLED) before the primary error re-raised:
    if act_c.state() != TaskControlBlock.CANCELLED:
        red(
            "sibling not cancellation-requested (state "
            + String(act_c.state())
            + ")"
        )

    # ---- D. mixed children in ONE scope: typed spawn + typed join ---------
    var cells_d = List[TB_P]()
    cells_d.append(TB_P.create())
    var act_d = TB_A.create()
    var args_d = List[Int]()
    args_d.append(0)
    var n_d = 0
    var total_d = 0
    var scene_d = Scene(
        UnsafePointer[List[TB_P], MutAnyOrigin](to=cells_d),
        UnsafePointer[TB_A, MutAnyOrigin](to=act_d),
        UnsafePointer[List[Int], MutAnyOrigin](to=args_d),
        UnsafePointer[Int, MutAnyOrigin](to=n_d),
        UnsafePointer[Int, MutAnyOrigin](to=total_d),
    )
    var ud_d = UnsafePointer[Scene, MutAnyOrigin](to=scene_d).bitcast[Byte]()
    with_scope(rt, service_mixed, ud_d)
    if total_d != 50 + 500:
        red("mixed with_scope total wrong: " + String(total_d))

    # ---- E. homogeneous typed reap: close_typed[T] consumes settled results
    var args_e = List[Int]()
    args_e.append(0)
    var s_h = make_scope(61, UnsafePointer[List[Int], MutAnyOrigin](to=args_e), False)
    var sp_h = UnsafePointer[Scope, MutAnyOrigin](to=s_h)
    var cell_p1 = TB_P.create()
    var cell_p2 = TB_P.create()
    var h1 = sp_h[].spawn[Profile](rt, UnsafePointer[TB_P, MutAnyOrigin](to=cell_p1), 0)
    var h2 = sp_h[].spawn[Profile](rt, UnsafePointer[TB_P, MutAnyOrigin](to=cell_p2), 0)
    var id_e = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(args_e.unsafe_ptr()) + 0 * 8
    )
    id_e[] = 2
    _ = execute(h1, body_fetch, id_e.bitcast[Byte]())
    _ = execute(h2, body_fetch, id_e.bitcast[Byte]())
    sp_h[].close_typed[Profile](rt)
    if cell_p1.has_result_pending() or cell_p2.has_result_pending():
        red("close_typed did not join+consume the settled results")
    if sp_h[].is_open() or sp_h[].live_child_count() != 0:
        red("close_typed did not close the scope")

    # ---- F. negative: wrong-type boundary cast -> deterministic tag mismatch
    var args_f = List[Int]()
    args_f.append(0)
    var id_f = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(args_f.unsafe_ptr()) + 0 * 8
    )
    var s_m = make_scope(62, UnsafePointer[List[Int], MutAnyOrigin](to=args_f), False)
    var sp_m = UnsafePointer[Scope, MutAnyOrigin](to=s_m)
    var cell_m1 = TB_P.create()
    var cell_m2 = TB_A.create()
    var hm1 = sp_m[].spawn[Profile](rt, UnsafePointer[TB_P, MutAnyOrigin](to=cell_m1), 0)
    var hm2 = sp_m[].spawn[Activity](rt, UnsafePointer[TB_A, MutAnyOrigin](to=cell_m2), 0)
    id_f[] = 6
    _ = execute(hm1, body_fetch, id_f.bitcast[Byte]())
    id_f[] = 6
    _ = execute(hm2, body_track, id_f.bitcast[Byte]())
    var tag_raised = False
    try:
        _ = sp_m[].lookup[Profile](hm2.id())
    except e:
        tag_raised = is_scope_tag_mismatch(e)
    if not tag_raised:
        red("lookup through the wrong child type did not raise ScopeTagMismatch")
    var reap_raised = False
    try:
        sp_m[].close_typed[Profile](rt)
    except e:
        reap_raised = is_scope_tag_mismatch(e)
    if not reap_raised:
        red("typed reap over a mixed scope did not raise ScopeTagMismatch")
    # the refusal must not have consumed or closed anything:
    if not sp_m[].is_open():
        red("tag-mismatch refusal closed the scope")
    if not cell_m1.has_result_pending():
        red("tag-mismatch refusal consumed a settled child result")
    # clean teardown of the mixed scope: validate-only close (children all
    # COMPLETED), then reap the settled children by their typed handles:
    sp_m[].close(rt)
    var pv = hm1.join()
    if pv.v != 60:
        red("mixed reap-by-handle value wrong: " + String(pv.v))
    _ = hm2.join()

    # ---- G. nested-scope ergonomics stay on the non-generic surface --------
    var args_g = List[Int]()
    args_g.append(0)
    var argsp = UnsafePointer[List[Int], MutAnyOrigin](to=args_g)
    var s_o = make_scope(63, argsp, True)
    var sp_o = UnsafePointer[Scope, MutAnyOrigin](to=s_o)
    var s_i = make_nested_scope(64, sp_o, argsp, True)
    var sp_i = UnsafePointer[Scope, MutAnyOrigin](to=s_i)
    var outer_first = False
    try:
        sp_o[].close(rt)
    except Error:
        outer_first = True
    if not outer_first:
        red("outer close with an open subscope not refused")
    sp_i[].close(rt)
    sp_o[].close(rt)

    print("T29 with-scope: PASS")