# mojito_async/test/unit/t26_scheduler_fiber_aot.mojo
#
# A1.5 (issue #53) — fiber integration into the A1 scheduler seam.
#
# TDD acceptance driver: RED first (the driver imports
# mojito_async.runtime.fiber_seam, which lands in the implementation
# commit), then green.  Proves the in-scheduler interleave the A0.4 seam
# closed (spec §21/§27/§60):
#
#   - A parks MID-FRAME on its fiber stack (the frame physically leaves the
#     worker's native context via the fiber->caller switch in the body);
#   - while A is parked (WAITING, generation-bumped), B runs to completion
#     on the SAME worker's native context — inside scheduler_loop, which
#     serves B's record between A's two slices — and Y runs a fiber-backed
#     yield (fiber park + immediate FIFO re-enqueue, no wait epoch);
#   - A is woken via the fiber-backed resume_current, and scheduler_loop
#     re-enters its fiber at the EXACT saved point: the stack-local marker
#     (recorded at first entry) is byte-identical, the payload survives,
#     and the frame still sits inside A's fiber-region stack;
#   - the worker's NATIVE stack pointer changes across the suspend/resume
#     (markers captured at different call depths; the deep loop drives the
#     resume with 4 live nested native frames) while A's frame-local stays
#     identical — the fiber, not the native frame, held the task;
#   - every park/wake state transition is legal in the A0.5 machine
#     (RUNNING->PARKING->WAITING generation bump for A; the yield early-wake
#     edge PARKING->RUNNABLE with NO wait epoch for Y);
#   - resume of an already-terminal fiber is a LOUD error (never a silent
#     frame walk);
#   - fast path preserved: a run of non-parking tasks takes ZERO fiber
#     switches (cheap-path counter flat; only parking/yielding tasks count).
#
# EXTERN DISCIPLINE (modular/modular#6971): the driver imports the
# extern-bearing fiber seam (ms_stack_alloc/ms_ctx_* reachable through
# mojito_async.runtime.fiber_seam -> fiber.mojo -> vendor firewall), so it
# MUST be AOT (`mojo build` + execute; *_aot.mojo pattern — picked up by
# test/run.sh) exactly like t23_fiber_aot / t25_fiber_affinity_aot.  The
# JIT unit drivers (t11..t18/t20..t22) keep importing the extern-free
# scheduler.mojo/park.mojo and are untouched.
#
# Verdict: exit 0 + "PASS"; any failure prints FAIL + raises (exit 1).
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    c_free,
    c_malloc,
    entry_pointer,
    ms_page_size,
    ms_stack_alloc,
)
from mojito_async.fiber.fiber import FiberFrame
from mojito_async.runtime.fiber_seam import (
    SeamSlot,
    fiber_resume_current,
    fiber_suspend_current,
    fiber_yield_now,
    seam_park_switch,
    seam_slot_stride,
)
from mojito_async.integration.sys import IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn
from std.memory import stack_allocation


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]


def red(what: String) raises -> None:
    print("T26 scheduler fiber: RED (" + what + ")")
    raise Error(what)


# --- event log -------------------------------------------------------------
# Interleave proof: B (and Y) run ENTIRELY inside A's park window.
comptime EV_A_ENTER = Int(1)
comptime EV_A_PARK = Int(2)
comptime EV_B_ENTER = Int(3)
comptime EV_B_DONE = Int(4)
comptime EV_Y_ENTER = Int(5)
comptime EV_Y_PARK = Int(6)
comptime EV_Y_RESUMED = Int(7)
comptime EV_Y_DONE = Int(8)
comptime EV_A_RESUMED = Int(9)
comptime EV_A_DONE = Int(10)
comptime N_EVENTS = Int(10)

# A's payload sentinel (survives the whole park/resume round trip).
comptime PAY_A = Int(0xA15)

# --- scene (heap-backed; t25 lesson: escapees must not be stack-carved) ----

struct T26Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Shared scene: event log + per-task state, all cells heap-backed.

    log@0, n@1, tid cells, markers, phases, ok flags, payloads; the three
    SeamSlot POINTERS (heap slots) the dispatcher drives."""

    var log: UnsafePointer[Int, MutUntrackedOrigin]
    var n: UnsafePointer[Int, MutUntrackedOrigin]
    var a_tid: UnsafePointer[Int, MutUntrackedOrigin]
    var b_tid: UnsafePointer[Int, MutUntrackedOrigin]
    var y_tid: UnsafePointer[Int, MutUntrackedOrigin]
    var marker_a: UnsafePointer[Int, MutUntrackedOrigin]
    var marker_b: UnsafePointer[Int, MutUntrackedOrigin]
    var marker_y: UnsafePointer[Int, MutUntrackedOrigin]
    var ok_a: UnsafePointer[Int, MutUntrackedOrigin]
    var ok_y: UnsafePointer[Int, MutUntrackedOrigin]
    var pay_a: UnsafePointer[Int, MutUntrackedOrigin]
    var slot_a: UnsafePointer[SeamSlot, MutAnyOrigin]
    var slot_b: UnsafePointer[SeamSlot, MutAnyOrigin]
    var slot_y: UnsafePointer[SeamSlot, MutAnyOrigin]

    def __init__(out self):
        self.log = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.n = self.log
        self.a_tid = self.log
        self.b_tid = self.log
        self.y_tid = self.log
        self.marker_a = self.log
        self.marker_b = self.log
        self.marker_y = self.log
        self.ok_a = self.log
        self.ok_y = self.log
        self.pay_a = self.log
        self.slot_a = UnsafePointer[SeamSlot, MutAnyOrigin](unsafe_from_address=1)
        self.slot_b = self.slot_a
        self.slot_y = self.slot_a


def _log(mut sc: T26Scene, ev: Int):
    var i = sc.n[]
    sc.log[i] = ev
    sc.n[] = i + 1


def _expect_ev(i: Int) -> Int:
    """Expected event at log slot i (the interleave proof)."""
    if i == 0:
        return EV_A_ENTER
    if i == 1:
        return EV_A_PARK
    if i == 2:
        return EV_B_ENTER
    if i == 3:
        return EV_B_DONE
    if i == 4:
        return EV_Y_ENTER
    if i == 5:
        return EV_Y_PARK
    if i == 6:
        return EV_Y_RESUMED
    if i == 7:
        return EV_Y_DONE
    if i == 8:
        return EV_A_RESUMED
    return EV_A_DONE


# --- task bodies (fiber entry trampolines; abi("C"), never raise) ----------

# Task A: enters, parks MID-FRAME on its synthetic stack, resumes at the
# exact point (stack-local marker + payload sentinel), completes.
@export("t26_entry_a")
def t26_entry_a(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var sc = fr[].user.bitcast[T26Scene]()

    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)
    if sc[].ok_a[] == 0:
        sc[].marker_a[] = Int(local_p)
        sc[].ok_a[] = 1
        _log(sc[], EV_A_ENTER)
        _log(sc[], EV_A_PARK)
        # ---- FIBER PARK: migrate this RUNNING frame off the worker's
        # ---- native context (fiber -> caller); the worker serves B/Y.
        seam_park_switch(fr)

        # ---- EXACT RESUME POINT (spec §14 one-shot continuation): -------
        if sc[].marker_a[] != Int(local_p):
            sc[].ok_a[] = 0
        if sc[].pay_a[] != PAY_A:
            sc[].ok_a[] = 0
        _log(sc[], EV_A_RESUMED)
    _log(sc[], EV_A_DONE)


# Task B: runs to completion on the SAME worker inside A's park window.
@export("t26_entry_b")
def t26_entry_b(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var sc = fr[].user.bitcast[T26Scene]()
    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)
    sc[].marker_b[] = Int(local_p)
    _log(sc[], EV_B_ENTER)
    _log(sc[], EV_B_DONE)


# Task Y: fiber-backed yield — parks (fiber switch) then completes on the
# immediate FIFO re-entry (the early-wake edge; no WAITING, no gen bump).
@export("t26_entry_y")
def t26_entry_y(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var sc = fr[].user.bitcast[T26Scene]()

    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)
    if sc[].ok_y[] == 0:
        sc[].marker_y[] = Int(local_p)
        sc[].ok_y[] = 1
        _log(sc[], EV_Y_ENTER)
        _log(sc[], EV_Y_PARK)
        seam_park_switch(fr)

        if sc[].marker_y[] != Int(local_p):
            sc[].ok_y[] = 0
        _log(sc[], EV_Y_RESUMED)
    _log(sc[], EV_Y_DONE)


# --- driver-side helpers ---------------------------------------------------

def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _native_marker() -> Int:
    """Address of a frame-local in THIS frame (worker native stack)."""
    var slot: Int = 0
    return Int(UnsafePointer[Int, MutAnyOrigin](to=slot))


# The fiber-backed dispatcher (generic scheduler_loop bound): the fiber
# handle is threaded through the driver value (design decision, issue #53:
# NEVER dynamic dispatch).  Each slice: claim RUNNING, drive the task's
# fiber (entry or exact re-entry), then settle:
#   - body unwound (slot.finished)  -> RUNNING -> COMPLETED + result;
#   - body parked mid-frame         -> fiber_suspend_current (A: WAITING
#     + generation bump) or fiber_yield_now (Y: PARKING -> RUNNABLE + FIFO
#     re-enqueue, no wait epoch).
def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[T26Scene]()
    if tid == sc[].a_tid[]:
        var ha = _handle(tcb_addr, tid)
        claim_running(ha)
        seam_drive(rt, sc[].slot_a)
        if sc[].slot_a[].finished:
            ha.tcb()[].transition(TaskControlBlock.COMPLETED)
            ha.tcb()[].mark_result(IntResult(211))
        else:
            fiber_suspend_current(rt, ha, Int(3))  # SuspendReason.PARK
        return 1
    if tid == sc[].b_tid[]:
        var hb = _handle(tcb_addr, tid)
        claim_running(hb)
        seam_drive(rt, sc[].slot_b)
        hb.tcb()[].transition(TaskControlBlock.COMPLETED)
        hb.tcb()[].mark_result(IntResult(111))
        return 1
    if tid == sc[].y_tid[]:
        var hy = _handle(tcb_addr, tid)
        claim_running(hy)
        seam_drive(rt, sc[].slot_y)
        if sc[].slot_y[].finished:
            hy.tcb()[].transition(TaskControlBlock.COMPLETED)
            hy.tcb()[].mark_result(IntResult(311))
        else:
            fiber_yield_now(rt, hy)
        return 1
    raise Error("unexpected task id in t26 dispatcher")


# Drive scheduler_loop from `depth` nested LIVE native frames (recursion
# cannot be inlined away), recording the native marker at the innermost
# point.  Two depths => GUARANTEED distinct native SPs across the
# suspend/resume (t25 carry-over; ADR-007 evidence).
def _loop_marked[F: def(mut Runtime, Int, Int, BytePtr) raises -> Int](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
    depth: Int,
    cell: UnsafePointer[Int, MutUntrackedOrigin],
) raises -> Int:
    if depth == 0:
        cell[0] = _native_marker()
        return scheduler_loop(rt, dispatcher, ud)
    return _loop_marked(rt, dispatcher, ud, depth - 1, cell)


comptime NATIVE_DEPTH_SHALLOW = Int(0)
comptime NATIVE_DEPTH_DEEP = Int(4)


# --- cheap-path dispatcher (fast-path guard; plain execute, no fiber) ------

def cheap_body(ud: BytePtr) raises -> IntResult:
    return IntResult(7)


def dispatch_cheap(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var h = _handle(tcb_addr, tid)
    _ = execute(h, cheap_body, ud)
    return 1


def main() raises:
    var failures = List[String]()
    var ps = Int(ms_page_size())
    if ps <= 0:
        failures.append("ms_page_size non-positive")
    var stack_bytes = 4 * ps

    # --- heap block layout (single malloc of raw cells) --------------------
    var block = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(14 * 8))
    )
    for i in range(14):
        block[i] = 0
    var sc = T26Scene()
    sc.log = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 0)
    sc.n = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 1 * 8)
    sc.a_tid = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 2 * 8)
    sc.b_tid = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 3 * 8)
    sc.y_tid = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 4 * 8)
    sc.marker_a = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 5 * 8)
    sc.marker_b = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 6 * 8)
    sc.marker_y = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 7 * 8)
    sc.ok_a = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 8 * 8)
    sc.ok_y = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 9 * 8)
    sc.pay_a = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(block) + 10 * 8)
    sc.pay_a[] = PAY_A

    # --- three SeamSlots in a stable heap block (fibers never relocate) ----
    var slots_block = c_malloc(3 * seam_slot_stride())
    if Int(slots_block) == 0:
        red("slot block allocation failed")
    var slot_a = UnsafePointer[SeamSlot, MutAnyOrigin](
        unsafe_from_address=Int(slots_block)
    )
    var slot_b = UnsafePointer[SeamSlot, MutAnyOrigin](
        unsafe_from_address=Int(slots_block) + seam_slot_stride()
    )
    var slot_y = UnsafePointer[SeamSlot, MutAnyOrigin](
        unsafe_from_address=Int(slots_block) + 2 * seam_slot_stride()
    )
    slot_a[0] = make_seam_slot()
    slot_b[0] = make_seam_slot()
    slot_y[0] = make_seam_slot()
    sc.slot_a = slot_a
    sc.slot_b = slot_b
    sc.slot_y = slot_y

    # --- acquire three synthetic stacks (fresh #49 bindings) ---------------
    var slots_a = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(16))
    )
    var slots_b = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(16))
    )
    var slots_y = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(16))
    )
    if ms_stack_alloc(stack_bytes, slots_a, slots_a + 1) != 0:
        failures.append("ms_stack_alloc failed for A")
    if ms_stack_alloc(stack_bytes, slots_b, slots_b + 1) != 0:
        failures.append("ms_stack_alloc failed for B")
    if ms_stack_alloc(stack_bytes, slots_y, slots_y + 1) != 0:
        failures.append("ms_stack_alloc failed for Y")
    var ns_a = NativeStack(slots_a[0], (slots_a + 1)[])
    var ns_b = NativeStack(slots_b[0], (slots_b + 1)[])
    var ns_y = NativeStack(slots_y[0], (slots_y + 1)[])
    var base_a = Int(ns_a.base)
    var top_a = Int(ns_a.top)
    var base_b = Int(ns_b.base)
    var top_b = Int(ns_b.top)
    var base_y = Int(ns_y.base)
    var top_y = Int(ns_y.top)

    var scp = UnsafePointer[T26Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # --- bind the task fibers (entry + payload side channel) ---------------
    seam_bind_slot(slot_a, ns_a, entry_pointer["t26_entry_a"](), ud)
    seam_bind_slot(slot_b, ns_b, entry_pointer["t26_entry_b"](), ud)
    seam_bind_slot(slot_y, ns_y, entry_pointer["t26_entry_y"](), ud)

    # --- spawn A, B, Y into the single worker's runnable FIFO --------------
    var rt = create()
    var tcb_a = TB.create()
    var tcb_b = TB.create()
    var tcb_y = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    var h_b = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    var h_y = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_y), 0)
    sc.a_tid[] = h_a.id()
    sc.b_tid[] = h_b.id()
    sc.y_tid[] = h_y.id()

    # --- drive 1 (shallow): A parks, B completes, Y yields + completes -----
    var npre = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(8))
    )
    var ndeep = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(8))
    )
    var served1 = _loop_marked(rt, dispatch, ud, NATIVE_DEPTH_SHALLOW, npre)
    if served1 != 4:
        failures.append("drive1 served " + String(served1) + ", expected 4")
    if not h_b.is_completed():
        failures.append("B did not complete inside A's park window")
    if not h_y.is_completed():
        failures.append("Y did not complete after its yield")
    if h_a.state() != TaskControlBlock.WAITING:
        failures.append("A must be WAITING after the fiber park (state "
                        + String(h_a.state()) + ")")
    if rt.pending() != 0:
        failures.append("queue not quiet after drive1 (pending "
                        + String(rt.pending()) + ")")
    # yield took the early-wake edge: NO WAITING, NO generation bump.
    if tcb_y.generation() != 1:
        failures.append("yield bumped the wait generation (early-wake edge "
                        "must not; gen " + String(tcb_y.generation()) + ")")
    # A's park committed a fresh wait epoch (generation-bumped).
    if tcb_a.generation() != 2:
        failures.append("A's park did not generation-bump (gen "
                        + String(tcb_a.generation()) + ")")
    if sc.ok_a[] != 1:
        failures.append("A entry did not record its marker")

    # --- wake A via the fiber-backed resume_current ------------------------
    fiber_resume_current(rt, h_a)
    if h_a.state() != TaskControlBlock.RUNNABLE:
        failures.append("resume_current did not make A RUNNABLE (state "
                        + String(h_a.state()) + ")")
    if rt.pending() != 1:
        failures.append("resume_current did not re-enqueue A exactly once")

    # --- drive 2 (deep): A resumes at the EXACT saved point, in-scheduler --
    var served2 = _loop_marked(rt, dispatch, ud, NATIVE_DEPTH_DEEP, ndeep)
    if served2 != 1:
        failures.append("drive2 served " + String(served2) + ", expected 1")
    if not h_a.is_completed():
        failures.append("resumed A did not reach COMPLETED")
    if rt.pending() != 0:
        failures.append("queue not quiet after drive2")

    # --- the interleave proof (event order) --------------------------------
    if sc.n[] != N_EVENTS:
        failures.append("expected " + String(N_EVENTS) + " events, got "
                        + String(sc.n[]))
    else:
        for i in range(N_EVENTS):
            if sc.log[i] != _expect_ev(i):
                failures.append("event order mismatch at " + String(i)
                                + ": got " + String(sc.log[i]) + " want "
                                + String(_expect_ev(i)))

    # --- exact resume + fiber-region locality (ADR-007) --------------------
    if sc.ok_a[] != 1:
        failures.append("A lost its exact resume point (marker/payload)")
    if sc.ok_y[] != 1:
        failures.append("Y lost its exact resume point (marker)")
    if sc.marker_a[] < base_a or sc.marker_a[] >= top_a:
        failures.append("A's frame-local is NOT inside A's fiber-region stack")
    if sc.marker_b[] < base_b or sc.marker_b[] >= top_b:
        failures.append("B's frame-local is NOT inside B's fiber-region stack")
    if sc.marker_y[] < base_y or sc.marker_y[] >= top_y:
        failures.append("Y's frame-local is NOT inside Y's fiber-region stack")
    if sc.pay_a[] != PAY_A:
        failures.append("A's payload sentinel was clobbered across the park")
    if npre[0] == ndeep[0]:
        failures.append("worker NATIVE stack pointer did not change across "
                        "the suspend/resume (frame never left the native "
                        "caller)")

    # --- cheap-path counter: only parking/yielding tasks switch ------------
    # A: 2 slices, B: 1, Y: 2 -> 5 fiber drives -> 10 ms_ctx_switch calls.
    if rt.fiber_drives() != 5:
        failures.append("fiber drives " + String(rt.fiber_drives())
                        + " != 5")
    if rt.fiber_switches() != 10:
        failures.append("fiber switches " + String(rt.fiber_switches())
                        + " != 10")

    # --- harden the frame contract: terminal resume is a LOUD error --------
    var before = rt.fiber_switches()
    var term_err = False
    try:
        seam_drive(rt, slot_a)  # A is terminal (finished); must NOT walk frames
    except e:
        var msg = String(e)
        if "already-terminal" in msg:
            term_err = True
    if not term_err:
        failures.append("resume of an already-terminal fiber did NOT raise")
    if rt.fiber_switches() != before:
        failures.append("terminal-resume error performed a frame walk "
                        "(counters moved)")

    # --- fast path: non-parking tasks take ZERO fiber switches -------------
    var rt2 = create()
    var tcb_pool = List[TB]()
    var cheap_ud = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x33)
    for i in range(16):
        tcb_pool.append(TB.create())
        _ = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=tcb_pool[i]), 0)
    var served3 = scheduler_loop(rt2, dispatch_cheap, cheap_ud)
    if served3 != 16:
        failures.append("cheap drive served " + String(served3) + ", expected 16")
    if rt2.fiber_drives() != 0 or rt2.fiber_switches() != 0:
        failures.append("fast path broke: cheap tasks took "
                        + String(rt2.fiber_switches()) + " fiber switches "
                        + "(must stay 0)")
    for i in range(16):
        if not tcb_pool[i].is_completed():
            failures.append("cheap task " + String(i) + " did not complete")

    # --- teardown: fibers own their reservations; destroy releases --------
    seam_destroy_slot(slot_a)
    seam_destroy_slot(slot_b)
    seam_destroy_slot(slot_y)
    if slot_a[].fiber.alive() or slot_b[].fiber.alive() or slot_y[].fiber.alive():
        failures.append("destroy left a fiber alive")

    c_free(slots_block)
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(block)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(npre)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(ndeep)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(slots_a)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(slots_b)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(slots_y)))

    if len(failures) == 0:
        print("T26 scheduler fiber: PASS")
    else:
        print("T26 scheduler fiber: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)