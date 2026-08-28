# mojito_async/test/stress/t13_stress.mojo
#
# A1.5 stress (issue #37) — exit criterion 3: CANCELLATION STORM.
#
# N concurrent cancellation requests must leave ZERO live children and ZERO
# orphaned results, with every cancelled child ending COMPLETED (failure
# recorded with the preserved CancellationError message) and the runnable
# queue quiet.
#
# On the A1.1 single cooperative worker "N concurrent cancels" is modeled
# as a DETERMINISTIC storm: all N requests fire back-to-back with no
# scheduling in between (the interleaving N simultaneous arrivals present
# to a single worker), and every child observes its own request at its next
# checkpoint and settles COMPLETED.
#
# Scenario A (storm on parked): 10,000 children spawn into a Scope, each
# parks via park_current with its own CancellationToken; the storm
# requests all 10,000 tokens, every child is woken, each checkpoint raises
# CancellationError, execute() settles COMPLETED; scope.close(rt) joins all
# settled children (no orphaned results); sample joins re-raise the
# preserved CancellationError.
#
# Scenario B (storm before park): 1,000 children spawn with tokens ALREADY
# requested; each child's first slice checkpoints immediately and settles
# COMPLETED at the PRE-PARK checkpoint — zero children ever park, zero live
# children remain.
#
# Pure Mojo (`mojo run -I repo`), extern-free, def-only, deterministic.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.cancellation import (
    CancelFlag,
    CancellationToken,
    is_cancellation,
    make_cancel_flag,
)
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import park_current, unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.scope import CancelHook, Scope, make_scope
from mojito_async.task import JoinHandle, claim_running, execute, spawn


def red(what: String) raises -> None:
    print("T13 stress (cancellation storm): RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# Stub cancellation hook for the Scope (A1.1 injects the failure policy;
# real tree propagation is a later lane).
struct NoopCancel(CancelHook):
    def __init__(out self):
        pass

    def request_cancel(mut self, scope_handle: Int, child_handle: Int) raises:
        pass


# ---------------------------------------------------------------------------
# Scene (pointer-backed; b2 by-value params share the pointees):
#   counters base: parked@0, completed@1, ran_ok@2, phase@3, cur@4
#   tid_map    = base + 16          (tid -> child index, N entries; ids are
#                                    sequential 1..N on a fresh runtime)
#   counts     = base + 16 + N      (per-child slice counters, N entries)
#   tokens     = List of per-child CancelFlag pointers (stable: the List is
#                fully preloaded before any address is captured).
# ---------------------------------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var tokens: UnsafePointer[UnsafePointer[CancelFlag, MutAnyOrigin], MutAnyOrigin]
    var base: UnsafePointer[Int, MutAnyOrigin]
    var tid_map: UnsafePointer[Int, MutAnyOrigin]
    var counts: UnsafePointer[Int, MutAnyOrigin]
    # Owner handles: the A1.1 single-owner convention — execute() runs with
    # the OWNER handle so the preserved child error lands on the handle the
    # driver later joins (b2 `mut` def-params are inout-like, List[i] is an
    # inout lvalue; the failure flags stick on the SHARED element).
    var owners: UnsafePointer[List[JoinHandle[IntResult]], MutAnyOrigin]

    def __init__(out self):
        self.tokens = UnsafePointer[
            UnsafePointer[CancelFlag, MutAnyOrigin], MutAnyOrigin
        ](unsafe_from_address=1)
        self.base = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.tid_map = self.base
        self.counts = self.base
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

    def ran_ok(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 2 * 8
        )

    def phase(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 3 * 8
        )

    def cur(mut self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(self.base) + 4 * 8
        )


def body_checkpoint(ud: BytePtr) raises -> IntResult:
    """Child's checkpoint slice: raises the preserved CancellationError when
    this child's token was requested; never reached with an unrequested
    token in these storms (else ran_ok would trip)."""
    var sc = ud.bitcast[Scene]()
    var idx = sc[].cur()[]
    var tok = CancellationToken(sc[].tokens[idx])
    tok.checkpoint()
    sc[].ran_ok()[] = sc[].ran_ok()[] + 1
    return IntResult(1)


def dispatch_storm(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Scenario-aware dispatcher over ONE generic body:
      phase 0 (storm on parked): slice 1 parks the child (claim RUNNING,
        park_current); slice 2 executes body_checkpoint (raises ->
        COMPLETED).
      phase 1 (storm before park): every slice executes body_checkpoint
        (raises -> COMPLETED at the very first checkpoint)."""
    var sc = ud.bitcast[Scene]()
    var idx = sc[].tid_map[tid]
    sc[].cur()[] = idx
    var transient = _handle(tcb_addr, tid)
    if sc[].phase()[] == 0:
        if sc[].counts[idx] == 0:
            claim_running(transient)
            park_current(rt, transient)
            sc[].counts[idx] = 1
            sc[].parked()[] = sc[].parked()[] + 1
            return 1
        # execute with the OWNER handle (inout through the List element):
        # the preserved child failure must land on the handle the driver
        # later joins (single-owner convention).
        _ = execute(sc[].owners[][idx], body_checkpoint, ud)
        sc[].completed()[] = sc[].completed()[] + 1
        return 1
    _ = execute(sc[].owners[][idx], body_checkpoint, ud)
    sc[].completed()[] = sc[].completed()[] + 1
    return 1


def main() raises:
    # =========================================================================
    # Scenario A: storm on 10,000 parked children.
    # =========================================================================
    comptime NA = Int(10000)
    var rt = create()
    var order = List[Int]()
    var order_ptr = UnsafePointer[List[Int], MutAnyOrigin](to=order)
    var hook = NoopCancel()
    var scope = make_scope[IntResult, NoopCancel](hook, 77, order_ptr, False)
    var sp = UnsafePointer[Scope[IntResult, NoopCancel], MutAnyOrigin](to=scope)

    # Stable flag cells + per-child token pointers (fully preloaded first).
    var flags = List[CancelFlag]()
    for _ in range(NA):
        flags.append(make_cancel_flag())
    var token_ptrs = List[UnsafePointer[CancelFlag, MutAnyOrigin]]()
    for i in range(NA):
        token_ptrs.append(UnsafePointer[CancelFlag, MutAnyOrigin](to=flags[i]))
    var tok_base = UnsafePointer[
        UnsafePointer[CancelFlag, MutAnyOrigin], MutAnyOrigin
    ](unsafe_from_address=Int(token_ptrs.unsafe_ptr()))

    var buf = List[Int]()
    for _ in range(16 + 2 * NA + 4):
        buf.append(0)
    var scene = Scene()
    scene.tokens = tok_base
    scene.base = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 0 * 8
    )
    scene.tid_map = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 16 * 8
    )
    # counts region starts one slot PAST tid_map's last index (16 + NA).
    scene.counts = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + (17 + NA) * 8
    )
    buf[3] = 0  # phase: scenario A
    var scenep = UnsafePointer[Scene, MutAnyOrigin](to=scene)
    var ud = scenep.bitcast[Byte]()

    # Preload the full TCB pool FIRST (List growth would otherwise move
    # earlier cells and dangle the addresses captured at spawn time), then
    # spawn and register.
    var cells = List[TB]()
    for _ in range(NA):
        cells.append(TB.create())
    var handles = List[JoinHandle[IntResult]]()
    for i in range(NA):
        var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=cells[i]), 0)
        handles.append(h)
        _ = sp[].register(UnsafePointer[TB, MutAnyOrigin](to=cells[i]), h.id())
        buf[16 + h.id()] = i  # tid -> child index
    scene.owners = UnsafePointer[List[JoinHandle[IntResult]], MutAnyOrigin](
        to=handles
    )

    # ---- wave 1: every child parks ------------------------------------------
    var served1 = scheduler_loop(rt, dispatch_storm, ud)
    if served1 != NA:
        red("A wave1 served " + String(served1) + " != " + String(NA))
    if buf[0] != NA:
        red("A: parked " + String(buf[0]) + " != " + String(NA))
    for i in range(NA):
        if not handles[i].tcb()[].is_waiting():
            red("A: child " + String(i) + " not WAITING after wave 1")
    if rt.pending() != 0:
        red("A: queue not quiet after wave 1")

    # ---- THE STORM: N concurrent requests, back-to-back ---------------------
    for i in range(NA):
        flags[i].request()

    # ---- wake every parked child (the cancel-resume edge) -------------------
    for i in range(NA):
        unpark_current(rt, handles[i])

    # ---- wave 2: every child checkpoints -> CancellationError -> COMPLETED -
    var served2 = scheduler_loop(rt, dispatch_storm, ud)
    if served2 != NA:
        red("A wave2 served " + String(served2) + " != " + String(NA))

    # ---- zero live children, zero uncancelled runs --------------------------
    var live = 0
    for i in range(NA):
        if not handles[i].is_completed():
            live += 1
    if live != 0:
        red("A: " + String(live) + " live children after the storm")
    if buf[2] != 0:
        red("A: " + String(buf[2]) + " children ran WITHOUT cancellation")
    for i in range(NA):
        if handles[i].state() != TaskControlBlock.COMPLETED:
            red("A: child " + String(i) + " not COMPLETED")

    # ---- scope close: join-integrated consume; no orphaned results ----------
    sp[].close(rt)
    if sp[].is_open():
        red("A: scope did not close")
    if sp[].live_child_count() != 0:
        red("A: scope registry not empty after close")
    var orphaned = 0
    for i in range(NA):
        if cells[i].has_result_pending():
            orphaned += 1
    if orphaned != 0:
        red("A: " + String(orphaned) + " orphaned results")
    if rt.pending() != 0:
        red("A: runnable queue not quiet after the storm")
    if rt.skipped() != 0:
        red("A: stale records skipped during the storm")
    if rt.enqueued() != 2 * NA:
        red("A enqueued " + String(rt.enqueued()) + " != " + String(2 * NA))

    # ---- sample joins re-raise the PRESERVED CancellationError ---------------
    for i in range(64):
        var raised = False
        try:
            _ = handles[i].join()
        except e:
            raised = True
            if not is_cancellation(e):
                red("A: join re-raised a non-cancellation error: " + String(e))
        if not raised:
            red("A: join of a cancelled child did not re-raise")

    # =========================================================================
    # Scenario B: storm BEFORE park — 1,000 children, tokens requested before
    # any child runs; every child settles COMPLETED at its FIRST checkpoint,
    # never parks, zero live children.
    # =========================================================================
    comptime NB = Int(1000)
    var rt2 = create()
    var flags2 = List[CancelFlag]()
    for _ in range(NB):
        flags2.append(make_cancel_flag())
    for i in range(NB):
        flags2[i].request()  # the storm lands before any child runs

    var token_ptrs2 = List[UnsafePointer[CancelFlag, MutAnyOrigin]]()
    for i in range(NB):
        token_ptrs2.append(UnsafePointer[CancelFlag, MutAnyOrigin](to=flags2[i]))
    var tok_base2 = UnsafePointer[
        UnsafePointer[CancelFlag, MutAnyOrigin], MutAnyOrigin
    ](unsafe_from_address=Int(token_ptrs2.unsafe_ptr()))

    var buf2 = List[Int]()
    for _ in range(16 + 2 * NB + 4):
        buf2.append(0)
    var scene2 = Scene()
    scene2.tokens = tok_base2
    scene2.base = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf2.unsafe_ptr()) + 0 * 8
    )
    scene2.tid_map = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf2.unsafe_ptr()) + 16 * 8
    )
    scene2.counts = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf2.unsafe_ptr()) + (17 + NB) * 8
    )
    buf2[3] = 1  # phase: scenario B
    var scenep2 = UnsafePointer[Scene, MutAnyOrigin](to=scene2)
    var ud2 = scenep2.bitcast[Byte]()

    var order2 = List[Int]()
    var order_ptr2 = UnsafePointer[List[Int], MutAnyOrigin](to=order2)
    var hook2 = NoopCancel()
    var scope2 = make_scope[IntResult, NoopCancel](hook2, 88, order_ptr2, False)
    var sp2 = UnsafePointer[Scope[IntResult, NoopCancel], MutAnyOrigin](to=scope2)

    var cells2 = List[TB]()
    for _ in range(NB):
        cells2.append(TB.create())
    var handles2 = List[JoinHandle[IntResult]]()
    for i in range(NB):
        var h = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=cells2[i]), 0)
        handles2.append(h)
        _ = sp2[].register(UnsafePointer[TB, MutAnyOrigin](to=cells2[i]), h.id())
        buf2[16 + h.id()] = i
    scene2.owners = UnsafePointer[List[JoinHandle[IntResult]], MutAnyOrigin](
        to=handles2
    )

    var servedB = scheduler_loop(rt2, dispatch_storm, ud2)
    if servedB != NB:
        red("B served " + String(servedB) + " != " + String(NB))
    if buf2[0] != 0:
        red("B: " + String(buf2[0]) + " children PARKED (storm must preempt park)")
    if buf2[2] != 0:
        red("B: " + String(buf2[2]) + " children ran WITHOUT cancellation")
    var liveB = 0
    for i in range(NB):
        if not handles2[i].is_completed():
            liveB += 1
    if liveB != 0:
        red("B: " + String(liveB) + " live children")
    sp2[].close(rt2)
    var orphanedB = 0
    for i in range(NB):
        if cells2[i].has_result_pending():
            orphanedB += 1
    if orphanedB != 0:
        red("B: " + String(orphanedB) + " orphaned results")
    if rt2.pending() != 0 or rt2.skipped() != 0:
        red("B: queue not quiet / stale records")
    if rt2.enqueued() != NB:
        red("B enqueued " + String(rt2.enqueued()) + " != " + String(NB))

    print("T13 stress (cancellation storm): PASS")