# mojito_async/test/stress/t18_scope_storm_aot.mojo
#
# A3.3 (issue #62) — NESTED-SCOPE + CANCELLATION/FAILURE STORM stress suite
# (EPIC #3 A3 exit criteria).
#
# Three deterministic scenarios over the scope machinery, no sleep-based
# waits, every counter exact:
#
#   S1 (deep tree, mixed outcomes): N_ROUNDS rounds of a K_DEPTH-deep
#     nested-scope tree, M_TASKS tasks per scope, each round a fixed
#     mixed population per scope (1 failer, 1 cancelee, M-2 completers).
#     Per round: every task reaches exactly ONE terminal state (COMPLETED;
#     its dispatcher slice count is exact), cancels are observed exactly
#     once per task (flag stamp + exactly one CancellationError re-raised
#     at join), every scope REGISTRY DRAINS to empty after close (no
#     leaks), close order is inner-before-outer (shared order log), the
#     failer's primary raises exactly once per scope, double-join is
#     refused, and the runtime counters (enqueued / served / skipped /
#     pending) are exact.
#
#   S2 (cancellation storm, wide tree): 1 root x 64 child scopes x 16
#     tasks, every task parked mid-flight, then ONE root-level cancel
#     (scope.request_cancel_all — the #54 seam: a scope cancel MUST descend
#     into child scopes per spec §29.1 "cancelling a scope recursively
#     requests cancellation of descendants").  Asserts every task's flag is
#     requested (exactly once each), every task observes the cancellation
#     exactly once, the root's close returns only after every descendant
#     settled, and every registry drains.
#
#     ON CURRENT MAIN THIS IS THE RED (the #54 hole): today
#     request_cancel_all fires the CancelHook only for the scope's
#     REGISTERED direct children, and subscopes are not registered children
#     — a root-level cancel reaches zero task flags and every parked
#     descendant would run un-cancelled (missed == 1024).  The known-red
#     row points at issue #54; the driver is written against the A1.1
#     machinery so the SAME file compiles and passes once the
#     recursive-descend semantics land.
#
#   S3 (failure storm): children raise errors through the scopes under the
#     CURRENT failure contract ("first error recorded, error propagated at
#     join" — scope-level aggregation is #64's policy seam): per scope
#     exactly ONE failing child.  Asserts exactly-one primary error per
#     scope (its message raises exactly once), NO double-raise (a future
#     close()-raise policy must still surface the primary exactly once
#     overall — the driver counts join + close combined), all siblings
#     settle clean, registries drain, counters exact.
#
# Determinism: the single-worker scheduler (scheduler_loop) is driven to
# quiescence at every choreography point; parked tasks are woken only by
# explicit unpark_current; request() fires per flag exactly once per round.
#
# AOT (*_aot.mojo): extern-free (pure Mojo), compiled+run as one binary
# (modular/modular#6971 discipline: any future extern stays driver-local);
# runtime bounded like t11's 100k-lifecycle pattern (~66k tasks total).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.cancellation import (
    CancelFlag,
    CancellationToken,
    is_cancellation,
    make_cancel_flag,
)
from mojito_async.cancellation_adapter import (
    CancelFlagHook,
    CancelFlagRegistry,
    make_cancel_flag_hook,
    make_cancel_flag_registry,
)
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import park_current, unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.scope import Scope, make_nested_scope, make_scope
from mojito_async.task import JoinHandle, claim_running, execute, spawn


def red(what: String) raises -> None:
    print("T18 scope storm: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]
comptime SC = Scope[IntResult, CancelFlagHook]

# task roles (per-scope role assignment is deterministic by index)
comptime RoleComplete = Int(0)
comptime RoleCancel = Int(1)
comptime RoleFail = Int(2)

# scenario sizes (the A3.3 acceptance shape; bounded like t11's 100k)
comptime N_ROUNDS = Int(500)    # deep-tree rounds (S1)
comptime K_DEPTH = Int(16)      # nested-scope depth (S1)
comptime M_TASKS = Int(8)       # tasks per scope (S1)
comptime S1_TASKS = Int(K_DEPTH * M_TASKS)

comptime S2_CHILD = Int(64)     # wide-tree child scopes (S2)
comptime S2_PER = Int(16)       # tasks per child scope (S2)
comptime S2_TASKS = Int(S2_CHILD * S2_PER)

comptime S3_CHILD = Int(8)      # failure-storm child scopes (S3)
comptime S3_PER = Int(8)        # tasks per child scope (S3)
comptime S3_TASKS = Int(S3_CHILD * S3_PER)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# ---------------------------------------------------------------------------
# StormScene — pointer-backed shared choreography (b2 by-value params share
# the pointees).  Counter base:
#   parked@0, completed@1, observed@2, primary@3, scenario@4, phase@5,
#   round@6, cur@7
#   tid_map    = base + 16          (tid -> task index, N+1 entries)
#   counts     = base + 16 + N      (per-task slice counts, N entries)
#   roles      = base + 16 + 2N     (per-task role, N entries)
#   tokens     = List of per-task CancelFlag pointers (stable: the List is
#                fully preloaded before any address is captured).
# ---------------------------------------------------------------------------

struct StormScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var tokens: UnsafePointer[
        UnsafePointer[CancelFlag, MutAnyOrigin], MutAnyOrigin
    ]
    var base: UnsafePointer[Int, MutAnyOrigin]
    var tid_map: UnsafePointer[Int, MutAnyOrigin]
    var counts: UnsafePointer[Int, MutAnyOrigin]
    var roles: UnsafePointer[Int, MutAnyOrigin]
    # Owner handles: execute() runs with the OWNER handle so preserved child
    # errors land on the handle the driver later joins.
    var owners: UnsafePointer[List[JoinHandle[IntResult]], MutAnyOrigin]

    def __init__(out self):
        self.tokens = UnsafePointer[
            UnsafePointer[CancelFlag, MutAnyOrigin], MutAnyOrigin
        ](unsafe_from_address=1)
        self.base = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.tid_map = self.base
        self.counts = self.base
        self.roles = self.base
        self.owners = UnsafePointer[
            List[JoinHandle[IntResult]], MutAnyOrigin
        ](unsafe_from_address=1)

    def parked(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 0 * 8
        )

    def completed(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 1 * 8
        )

    def observed(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 2 * 8
        )

    def primary(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 3 * 8
        )

    def scenario(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 4 * 8
        )

    def phase(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 5 * 8
        )

    def round_no(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 6 * 8
        )

    def cur(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 7 * 8
        )


# ---------------------------------------------------------------------------
# Task body — one generic body over the role cell (t13's pattern): a
# Fail-role task raises its deterministic primary message; a Cancel-role
# task checkpoints (raising CancellationError exactly when its flag is
# requested); a Complete-role task returns its index.
# ---------------------------------------------------------------------------

def body_task(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[StormScene]()
    var idx = sc[].cur()[]
    var r = sc[].roles[idx]
    if r == RoleFail:
        raise Error(
            "t18-prime:" + String(sc[].round_no()[]) + ":" + String(idx)
        )
    if r == RoleCancel:
        var tok = CancellationToken(sc[].tokens[idx])
        tok.checkpoint()  # raises CancellationError; stamps the flag
    return IntResult(idx)


# ---------------------------------------------------------------------------
# The single-worker dispatcher (t13's model): a Cancel-role task parks on
# its FIRST slice (mid-flight), then checkpoints (raising CancellationError)
# on its second slice; Fail/Complete tasks execute their body on the first
# slice.  The park/body split is the deterministic mid-flight choreography.
# ---------------------------------------------------------------------------

def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[StormScene]()
    var idx = sc[].tid_map[tid]
    sc[].cur()[] = idx
    var transient = _handle(tcb_addr, tid)
    var r = sc[].roles[idx]
    if r == RoleCancel and sc[].counts[idx] == 0:
        claim_running(transient)
        park_current(rt, transient)
        sc[].counts[idx] = 1
        sc[].parked()[] = sc[].parked()[] + 1
        return 1
    # execute with the OWNER handle (single-owner convention): preserved
    # child errors land on the handle the driver later joins.
    _ = execute(sc[].owners[][idx], body_task, ud)
    sc[].counts[idx] = sc[].counts[idx] + 1
    sc[].completed()[] = sc[].completed()[] + 1
    return 1


# ---------------------------------------------------------------------------
# S1 helpers — one deep-tree round over fresh per-round cells.
# ---------------------------------------------------------------------------

def build_deep_tree(
    hook: CancelFlagHook,
    round_no: Int,
    n: Int,
    mut scopes: List[SC],
    logp: UnsafePointer[List[Int], MutAnyOrigin],
) raises:
    """k-deep nested scope tree in ONE preloaded List (b2: List element
    addresses are stable once fully preloaded, so the parent pointers taken
    during index-assignment stay valid).  Handles: round_no*100 + level; the
    close-order log pointer is captured in MAIN (a `mut` List param would
    capture the param copy's stack storage, not the caller's list object —
    b2 trap)."""
    for _ in range(n):
        scopes.append(make_scope[IntResult, CancelFlagHook](
            hook, 1, logp, False
        ))
    scopes[0] = make_scope[IntResult, CancelFlagHook](
        hook, round_no * 100, logp, True
    )
    for i in range(1, n):
        var parent = UnsafePointer[SC, MutAnyOrigin](to=scopes[i - 1])
        scopes[i] = make_nested_scope[IntResult, CancelFlagHook](
            hook, round_no * 100 + i, parent, logp, True
        )


def run_deep_round(
    mut rt: Runtime,
    round_no: Int,
    mut scopes: List[SC],
    ud: BytePtr,
) raises:
    """Drive one S1 round to quiescence: wave 1 parks cancelees and settles
    failers/completers, the per-task cancel requests fire exactly once,
    wave 2 settles the parked cancelees."""
    var sc = ud.bitcast[StormScene]()
    sc[].round_no()[] = round_no

    # ---- wave 1: every task runs to its first checkpoint ------------------
    var n = len(scopes) * M_TASKS
    var served1 = scheduler_loop(rt, dispatch, ud)
    if served1 != n:
        red("S1 r" + String(round_no) + " wave1 served " + String(served1)
            + " != " + String(n))
    if sc[].parked()[] != len(scopes):
        red("S1 r" + String(round_no) + " parked " + String(sc[].parked()[])
            + " != " + String(len(scopes)))
    if rt.pending() != 0:
        red("S1 r" + String(round_no) + " queue not quiet after wave 1")
    if rt.skipped() != 0:
        red("S1 r" + String(round_no) + " stale records skipped in wave 1")

    # ---- per-task cancel requests (exactly one request per cancelee) ------
    for i in range(n):
        if sc[].roles[i] == RoleCancel:
            sc[].tokens[i][].request()

    # ---- wake every parked cancelee, then wave 2 --------------------------
    for i in range(n):
        if sc[].roles[i] == RoleCancel:
            unpark_current(rt, sc[].owners[][i])
    var served2 = scheduler_loop(rt, dispatch, ud)
    if served2 != len(scopes):
        red("S1 r" + String(round_no) + " wave2 served " + String(served2)
            + " != " + String(len(scopes)))


def assert_round_drains(
    mut rt: Runtime,
    round_no: Int,
    mut scopes: List[SC],
    logp: UnsafePointer[List[Int], MutAnyOrigin],
    ud: BytePtr,
) raises:
    """Per-round S1 assertions: exactly one terminal state per task, cancels
    observed exactly once, exactly-one primary per scope, every scope
    registry drains after close, inner-before-outer close order, no
    double-join, exact counters."""
    var sc = ud.bitcast[StormScene]()
    var n = len(scopes) * M_TASKS

    # ---- exactly one terminal state per task ------------------------------
    for i in range(n):
        if not sc[].owners[][i].is_completed():
            red("S1 r" + String(round_no) + " task " + String(i)
                + " not COMPLETED (terminal state missing)")
        var runs = sc[].counts[i]
        var want_runs = 2 if sc[].roles[i] == RoleCancel else 1
        if runs != want_runs:
            red("S1 r" + String(round_no) + " task " + String(i)
                + " ran " + String(runs) + " slices != " + String(want_runs))

    # ---- cancels observed exactly once per task ---------------------------
    for i in range(n):
        if sc[].roles[i] == RoleCancel:
            if not sc[].tokens[i][].is_requested():
                red("S1 r" + String(round_no) + " cancelee " + String(i)
                    + " flag not requested")
            if not sc[].tokens[i][].observed():
                red("S1 r" + String(round_no) + " cancelee " + String(i)
                    + " never observed cancellation")
        else:
            if sc[].tokens[i][].observed():
                red("S1 r" + String(round_no) + " non-cancelee " + String(i)
                    + " observed a cancellation")

    # ---- joins: cancels re-raise CancellationError exactly once; completers
    # clean; failers raise their primary exactly once -----------------------
    for i in range(n):
        var r = sc[].roles[i]
        var raised = False
        try:
            var res = sc[].owners[][i].join()
            if r != RoleComplete:
                red("S1 r" + String(round_no) + " role-" + String(r)
                    + " task " + String(i) + " join did not raise")
            if res.v != i:
                red("S1 r" + String(round_no) + " completer " + String(i)
                    + " result " + String(res.v))
        except e:
            raised = True
            if r == RoleComplete:
                red("S1 r" + String(round_no) + " completer " + String(i)
                    + " join raised: " + String(e))
            elif r == RoleCancel:
                if not is_cancellation(e):
                    red("S1 r" + String(round_no) + " cancelee " + String(i)
                        + " join raised a non-cancellation error: "
                        + String(e))
            else:  # RoleFail
                var tag = "t18-prime:" + String(round_no) + ":" + String(i)
                if not (tag in String(e)):
                    red("S1 r" + String(round_no) + " failer " + String(i)
                        + " join raised the wrong primary: " + String(e))
        if r != RoleComplete and not raised:
            red("S1 r" + String(round_no) + " role-" + String(r)
                + " task " + String(i) + " join raised nothing")

    # ---- scope close: inner-before-outer (order log), tolerant close ------
    var k = len(scopes)
    for i in range(k - 1, -1, -1):
        var tag = "t18-prime:" + String(round_no) + ":" + String(i * M_TASKS)
        try:
            scopes[i].close(rt)
        except e:
            if not (tag in String(e)):
                red("S1 r" + String(round_no) + " scope " + String(i)
                    + " close raised a non-primary error: " + String(e))
            sc[].primary()[] = sc[].primary()[] + 1
        if scopes[i].is_open():
            red("S1 r" + String(round_no) + " scope " + String(i)
                + " still open after close")
        if scopes[i].live_child_count() != 0:
            red("S1 r" + String(round_no) + " scope " + String(i)
                + " registry NOT drained (leak)")
    # ---- inner-before-outer close order (shared order log) ----------------
    if logp[].__len__() != k:
        red("S1 r" + String(round_no) + " order log recorded "
            + String(logp[].__len__()) + " closes != " + String(k))
    for i in range(k):
        if logp[][i] != round_no * 100 + (k - 1 - i):
            red("S1 r" + String(round_no) + " close order violated at "
                + String(i) + ": recorded " + String(logp[][i]) + " expected "
                + String(round_no * 100 + (k - 1 - i)))
    if sc[].primary()[] != 0:
        red("S1 r" + String(round_no) + " the primary error was raised "
            + String(sc[].primary()[]) + " extra time(s) by close — the "
            + "failure policy must surface it exactly once overall (no "
            + "double-raise)")

    # ---- no orphaned results, queue quiet, exact counters -----------------
    var orphaned = 0
    for i in range(n):
        if sc[].owners[][i].tcb()[].has_result_pending():
            orphaned += 1
    if orphaned != 0:
        red("S1 r" + String(round_no) + " " + String(orphaned)
            + " orphaned results")
    if rt.pending() != 0:
        red("S1 r" + String(round_no) + " runnable queue not quiet")
    if rt.skipped() != 0:
        red("S1 r" + String(round_no) + " stale records skipped")
    var expect_enq = n + k  # spawns + one wake per cancelee
    if rt.enqueued() != expect_enq:
        red("S1 r" + String(round_no) + " enqueued " + String(rt.enqueued())
            + " != " + String(expect_enq))

    # ---- double-join refused (sample: first 8 handles per round) ----------
    var sample = 8
    if sample > n:
        sample = n
    for i in range(sample):
        var doubled = False
        try:
            _ = sc[].owners[][i].join()
        except Error:
            doubled = True
        if not doubled:
            red("S1 r" + String(round_no) + " double join of " + String(i)
                + " was not refused")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() raises:
    # =========================================================================
    # S1: N_ROUNDS x K_DEPTH-deep trees, M_TASKS per scope, mixed outcomes.
    # =========================================================================
    for round_no in range(N_ROUNDS):
        var rt = create()
        var tmp_reg = make_cancel_flag_registry()
        var tmp_regp = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=tmp_reg)
        var hook = make_cancel_flag_hook(tmp_regp)
        var scopes = List[SC]()
        var log = List[Int]()
        var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
        build_deep_tree(hook, round_no, K_DEPTH, scopes, logp)

        # Stable per-round cells: TCB pool, flags (fresh: observed flags
        # cannot reset), token pointers, roles/counts, owner handles.
        var cells = List[TB]()
        for _ in range(S1_TASKS):
            cells.append(TB.create())
        var flags = List[CancelFlag]()
        for _ in range(S1_TASKS):
            flags.append(make_cancel_flag())
        var token_ptrs = List[UnsafePointer[CancelFlag, MutAnyOrigin]]()
        for i in range(S1_TASKS):
            token_ptrs.append(
                UnsafePointer[CancelFlag, MutAnyOrigin](to=flags[i])
            )
        var buf = List[Int]()
        for _ in range(16 + 3 * S1_TASKS):
            buf.append(0)
        var scene = StormScene()
        scene.tokens = UnsafePointer[
            UnsafePointer[CancelFlag, MutAnyOrigin], MutAnyOrigin
        ](unsafe_from_address=Int(token_ptrs.unsafe_ptr()))
        scene.base = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(buf.unsafe_ptr()) + 0 * 8
        )
        scene.tid_map = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(buf.unsafe_ptr()) + 16 * 8
        )
        scene.counts = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(buf.unsafe_ptr()) + (17 + S1_TASKS) * 8
        )
        scene.roles = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(buf.unsafe_ptr())
            + (17 + 2 * S1_TASKS) * 8
        )
        buf[4] = 1  # scenario S1
        buf[5] = 0  # phase: wave 1
        var handles = List[JoinHandle[IntResult]]()
        for i in range(S1_TASKS):
            scene.roles[i] = RoleComplete
            if i % M_TASKS == 0:
                scene.roles[i] = RoleFail
            elif i % M_TASKS == 1:
                scene.roles[i] = RoleCancel
            var level = i / M_TASKS
            var scope_cell = UnsafePointer[SC, MutAnyOrigin](to=scopes[level])
            var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=cells[i]), 0)
            handles.append(h)
            _ = scope_cell[].register(
                UnsafePointer[TB, MutAnyOrigin](to=cells[i]), h.id()
            )
            buf[16 + h.id()] = i  # tid -> task index
        scene.owners = UnsafePointer[List[JoinHandle[IntResult]], MutAnyOrigin](
            to=handles
        )
        var sp_scene = UnsafePointer[StormScene, MutAnyOrigin](to=scene)
        var ud = sp_scene.bitcast[Byte]()

        run_deep_round(rt, round_no, scopes, ud)
        assert_round_drains(rt, round_no, scopes, logp, ud)

    print("T18 scope storm: S1 PASS (500 rounds x 16-depth x 8 tasks, "
          + "mixed complete/cancel/fail)")

    # =========================================================================
    # S2: CANCELLATION STORM — 1 root x 64 child scopes x 16 tasks, all
    # parked mid-flight, ONE root-level cancel (the #54 seam); every task's
    # flag must be requested by the root cancel, every task must observe the
    # cancellation exactly once, the root close returns only after every
    # descendant settled.
    # =========================================================================
    var rt2 = create()
    var reg2 = make_cancel_flag_registry()
    var reg2p = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=reg2)
    var hook2 = make_cancel_flag_hook(reg2p)
    var log2 = List[Int]()
    var log2p = UnsafePointer[List[Int], MutAnyOrigin](to=log2)
    var s2_scopes = List[SC]()
    for _ in range(S2_CHILD + 1):
        s2_scopes.append(make_scope[IntResult, CancelFlagHook](
            hook2, 1, log2p, False
        ))
    s2_scopes[0] = make_scope[IntResult, CancelFlagHook](
        hook2, 20000, log2p, True
    )
    var root_sp = UnsafePointer[SC, MutAnyOrigin](to=s2_scopes[0])
    for c in range(S2_CHILD):
        s2_scopes[c + 1] = make_nested_scope[IntResult, CancelFlagHook](
            hook2, 20100 + c, root_sp, log2p, True
        )

    # the adapter registry mirrors the real tree: every scope handle -> its
    # flag, every (scope, child) -> the task flag (linked under the scope
    # flag).  The root-level cancel must reach ALL of them via the hook.
    var s2_scope_flags = List[CancelFlag]()
    for _ in range(S2_CHILD + 1):
        s2_scope_flags.append(make_cancel_flag())
    var s2_scope_flag_ptrs = List[UnsafePointer[CancelFlag, MutAnyOrigin]]()
    for i in range(S2_CHILD + 1):
        s2_scope_flag_ptrs.append(
            UnsafePointer[CancelFlag, MutAnyOrigin](to=s2_scope_flags[i])
        )
    reg2p[].register_scope(20000, s2_scope_flag_ptrs[0])
    for c in range(S2_CHILD):
        reg2p[].register_scope(20100 + c, s2_scope_flag_ptrs[c + 1])
    var s2_task_flags = List[CancelFlag]()
    for _ in range(S2_TASKS):
        s2_task_flags.append(make_cancel_flag())
    var s2_task_ptrs = List[UnsafePointer[CancelFlag, MutAnyOrigin]]()
    for i in range(S2_TASKS):
        s2_task_ptrs.append(
            UnsafePointer[CancelFlag, MutAnyOrigin](to=s2_task_flags[i])
        )

    var s2_cells = List[TB]()
    for _ in range(S2_TASKS):
        s2_cells.append(TB.create())
    var s2_handles = List[JoinHandle[IntResult]]()
    var s2_cids = List[Int]()
    for _ in range(S2_TASKS):
        s2_cids.append(0)
    var s2_buf = List[Int]()
    for _ in range(16 + 3 * S2_TASKS):
        s2_buf.append(0)
    var s2_scene = StormScene()
    s2_scene.tokens = UnsafePointer[
        UnsafePointer[CancelFlag, MutAnyOrigin], MutAnyOrigin
    ](unsafe_from_address=Int(s2_task_ptrs.unsafe_ptr()))
    s2_scene.base = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(s2_buf.unsafe_ptr()) + 0 * 8
    )
    s2_scene.tid_map = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(s2_buf.unsafe_ptr()) + 16 * 8
    )
    s2_scene.counts = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(s2_buf.unsafe_ptr()) + (17 + S2_TASKS) * 8
    )
    s2_scene.roles = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(s2_buf.unsafe_ptr())
        + (17 + 2 * S2_TASKS) * 8
    )
    s2_buf[4] = 2  # scenario S2
    s2_buf[5] = 0  # phase: wave 1 (park)
    for i in range(S2_TASKS):
        s2_scene.roles[i] = RoleCancel
        var sc_idx = 1 + i / S2_PER
        var s2_cell = UnsafePointer[SC, MutAnyOrigin](to=s2_scopes[sc_idx])
        var h = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=s2_cells[i]), 0)
        s2_handles.append(h)
        var cid = s2_cell[].register(
            UnsafePointer[TB, MutAnyOrigin](to=s2_cells[i]), h.id()
        )
        s2_cids[i] = cid
        reg2p[].register_child(20100 + sc_idx - 1, cid, s2_task_ptrs[i])
        s2_buf[16 + h.id()] = i
    s2_scene.owners = UnsafePointer[
        List[JoinHandle[IntResult]], MutAnyOrigin
    ](to=s2_handles)
    var s2_sp = UnsafePointer[StormScene, MutAnyOrigin](to=s2_scene)
    var ud2 = s2_sp.bitcast[Byte]()

    # ---- wave 1: every task parks mid-flight ------------------------------
    var s2_served1 = scheduler_loop(rt2, dispatch, ud2)
    if s2_served1 != S2_TASKS:
        red("S2 wave1 served " + String(s2_served1) + " != "
            + String(S2_TASKS))
    if s2_scene.parked()[] != S2_TASKS:
        red("S2 parked " + String(s2_scene.parked()[]) + " != "
            + String(S2_TASKS))
    for i in range(S2_TASKS):
        if not s2_handles[i].tcb()[].is_waiting():
            red("S2 task " + String(i) + " not WAITING after wave 1")
    if rt2.pending() != 0:
        red("S2 queue not quiet after wave 1")
    if rt2.enqueued() != S2_TASKS:
        red("S2 enqueued " + String(rt2.enqueued()) + " != "
            + String(S2_TASKS))

    # ---- THE STORM: ONE root-level cancel (the #54 seam) ------------------
    s2_scopes[0].request_cancel_all()

    # every task's flag must be requested (exactly once each): the root-level
    # cancel must descend into every child scope.
    var missed = 0
    for i in range(S2_TASKS):
        if not s2_task_flags[i].is_requested():
            missed += 1
    if missed != 0:
        red("S2 " + String(missed) + "/" + String(S2_TASKS)
            + " task flags NOT requested by the root-level cancel — a scope "
            + "cancel must descend through child scopes (recursive "
            + "cancellation tree, issue #54)")

    # ---- wake every parked descendant, then wave 2 ------------------------
    for i in range(S2_TASKS):
        unpark_current(rt2, s2_handles[i])
    var s2_served2 = scheduler_loop(rt2, dispatch, ud2)
    if s2_served2 != S2_TASKS:
        red("S2 wave2 served " + String(s2_served2) + " != "
            + String(S2_TASKS))

    # ---- every task observed the cancellation exactly once ----------------
    for i in range(S2_TASKS):
        if not s2_task_flags[i].observed():
            red("S2 task " + String(i) + " never observed its cancellation")
        if not s2_handles[i].is_completed():
            red("S2 task " + String(i) + " not COMPLETED after the storm")
        var raised = False
        try:
            _ = s2_handles[i].join()
        except e:
            raised = True
            if not is_cancellation(e):
                red("S2 task " + String(i)
                    + " join raised a non-cancellation error: " + String(e))
        if not raised:
            red("S2 task " + String(i) + " join did not re-raise its cancel")

    # ---- close EVERY child scope before the root (inner-before-outer);
    # the root's close returning proves all descendants settled ------------
    for c in range(S2_CHILD):
        s2_scopes[c + 1].close(rt2)
        if s2_scopes[c + 1].live_child_count() != 0:
            red("S2 child scope " + String(c)
                + " registry NOT drained (leak)")
    root_sp[].close(rt2)
    if root_sp[].is_open():
        red("S2 root scope still open after close")
    if root_sp[].live_child_count() != 0:
        red("S2 root registry NOT drained (leak)")

    # ---- storms leave no strays: queue quiet, exact counters, no orphans --
    var s2_orphans = 0
    for i in range(S2_TASKS):
        if s2_cells[i].has_result_pending():
            s2_orphans += 1
    if s2_orphans != 0:
        red("S2 " + String(s2_orphans) + " orphaned results")
    if rt2.pending() != 0 or rt2.skipped() != 0:
        red("S2 runnable queue not quiet / stale records after the storm")
    if rt2.enqueued() != 2 * S2_TASKS:
        red("S2 enqueued " + String(rt2.enqueued()) + " != "
            + String(2 * S2_TASKS))

    # ---- adapter teardown: symmetric unregister (severs parent links) -----
    for i in range(S2_TASKS):
        reg2p[].unregister_child(20100 + i / S2_PER, s2_cids[i])
    for c in range(S2_CHILD):
        reg2p[].unregister_scope(20100 + c)
    reg2p[].unregister_scope(20000)
    if reg2p[].has_scope(20000) or reg2p[].has_scope(20100):
        red("S2 adapter registry not drained after unregister")

    print("T18 scope storm: S2 PASS (1 root x 64 child scopes x 16 tasks, "
          + "root-level cancel, 1024/1024 flags requested, 1024 observed)")

    # =========================================================================
    # S3: FAILURE STORM — children raise errors through the scopes; current
    # contract: first error recorded per failing child, propagated exactly
    # once at join.  Asserts exactly-one primary per scope, no double-raise
    # (join + close combined), all siblings settle clean, registries drain.
    # =========================================================================
    var rt3 = create()
    var tmp_reg3 = make_cancel_flag_registry()
    var tmp_reg3p = UnsafePointer[CancelFlagRegistry, MutAnyOrigin](to=tmp_reg3)
    var hook3 = make_cancel_flag_hook(tmp_reg3p)
    var log3 = List[Int]()
    var log3p = UnsafePointer[List[Int], MutAnyOrigin](to=log3)
    var s3_scopes = List[SC]()
    for _ in range(S3_CHILD + 1):
        s3_scopes.append(make_scope[IntResult, CancelFlagHook](
            hook3, 1, log3p, False
        ))
    s3_scopes[0] = make_scope[IntResult, CancelFlagHook](
        hook3, 30000, log3p, True
    )
    var root3_sp = UnsafePointer[SC, MutAnyOrigin](to=s3_scopes[0])
    for c in range(S3_CHILD):
        s3_scopes[c + 1] = make_nested_scope[IntResult, CancelFlagHook](
            hook3, 30100 + c, root3_sp, log3p, True
        )

    var s3_cells = List[TB]()
    for _ in range(S3_TASKS):
        s3_cells.append(TB.create())
    var s3_handles = List[JoinHandle[IntResult]]()
    var s3_buf = List[Int]()
    for _ in range(16 + 3 * S3_TASKS):
        s3_buf.append(0)
    var s3_scene = StormScene()
    s3_scene.base = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(s3_buf.unsafe_ptr()) + 0 * 8
    )
    s3_scene.tid_map = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(s3_buf.unsafe_ptr()) + 16 * 8
    )
    s3_scene.counts = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(s3_buf.unsafe_ptr()) + (17 + S3_TASKS) * 8
    )
    s3_scene.roles = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(s3_buf.unsafe_ptr())
        + (17 + 2 * S3_TASKS) * 8
    )
    s3_buf[4] = 3  # scenario S3
    s3_buf[5] = 0
    s3_scene.round_no()[] = 45000  # tag base for S3 failers
    for i in range(S3_TASKS):
        s3_scene.roles[i] = RoleComplete
        if i % S3_PER == 0:
            s3_scene.roles[i] = RoleFail
        var sc_idx = 1 + i / S3_PER
        var s3_cell = UnsafePointer[SC, MutAnyOrigin](to=s3_scopes[sc_idx])
        var h = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=s3_cells[i]), 0)
        s3_handles.append(h)
        _ = s3_cell[].register(
            UnsafePointer[TB, MutAnyOrigin](to=s3_cells[i]), h.id()
        )
        s3_buf[16 + h.id()] = i
    s3_scene.owners = UnsafePointer[
        List[JoinHandle[IntResult]], MutAnyOrigin
    ](to=s3_handles)
    var s3_sp = UnsafePointer[StormScene, MutAnyOrigin](to=s3_scene)
    var ud3 = s3_sp.bitcast[Byte]()

    # ---- wave: every child settles; failers record their primary ----------
    var s3_served = scheduler_loop(rt3, dispatch, ud3)
    if s3_served != S3_TASKS:
        red("S3 served " + String(s3_served) + " != " + String(S3_TASKS))
    if s3_scene.completed()[] != S3_TASKS:
        red("S3 completed " + String(s3_scene.completed()[]) + " != "
            + String(S3_TASKS))

    # ---- exactly one terminal state per task ------------------------------
    for i in range(S3_TASKS):
        if not s3_handles[i].is_completed():
            red("S3 task " + String(i) + " not COMPLETED")

    # ---- joins: failers raise their primary exactly once; siblings clean -
    var s3_primary_raises = 0
    for i in range(S3_TASKS):
        var raised = False
        try:
            var res = s3_handles[i].join()
            if s3_scene.roles[i] == RoleFail:
                red("S3 failer " + String(i) + " join did not raise")
            if res.v != i:
                red("S3 completer " + String(i) + " result wrong")
        except e:
            raised = True
            if s3_scene.roles[i] == RoleComplete:
                red("S3 completer " + String(i) + " join raised: " + String(e))
            else:
                var tag = "t18-prime:45000:" + String(i)
                if not (tag in String(e)):
                    red("S3 failer " + String(i)
                        + " join raised the wrong primary: " + String(e))
                s3_primary_raises += 1
    if s3_primary_raises != S3_CHILD:
        red("S3 primary raises " + String(s3_primary_raises) + " != "
            + String(S3_CHILD) + " (one per scope)")

    # ---- close the tree (tolerant close: a future failure policy raising
    # the primary at close must never surface an already-observed error) ----
    for c in range(S3_CHILD - 1, -1, -1):
        var tag = "t18-prime:45000:" + String(c * S3_PER)
        try:
            s3_scopes[c + 1].close(rt3)
        except e:
            if not (tag in String(e)):
                red("S3 child scope " + String(c)
                    + " close raised a non-primary error: " + String(e))
            red("S3 child scope " + String(c)
                + " close re-raised the primary after the joiner already "
                + "observed it (no double-raise violated)")
        if s3_scopes[c + 1].live_child_count() != 0:
            red("S3 child scope " + String(c) + " registry NOT drained")
    var root3_raised = False
    try:
        root3_sp[].close(rt3)
    except Error:
        root3_raised = True
    if root3_raised:
        red("S3 root close raised (no child error belongs to the root)")
    if root3_sp[].live_child_count() != 0:
        red("S3 root registry NOT drained")

    # ---- no strays --------------------------------------------------------
    var s3_orphans = 0
    for i in range(S3_TASKS):
        if s3_cells[i].has_result_pending():
            s3_orphans += 1
    if s3_orphans != 0:
        red("S3 " + String(s3_orphans) + " orphaned results")
    if rt3.pending() != 0 or rt3.skipped() != 0:
        red("S3 runnable queue not quiet / stale records")
    if rt3.enqueued() != S3_TASKS:
        red("S3 enqueued " + String(rt3.enqueued()) + " != "
            + String(S3_TASKS))

    print("T18 scope storm: S3 PASS (8 child scopes x 8 tasks, 8/8 primaries "
          + "raised exactly once, siblings settled)")
    print("T18 scope storm: PASS")