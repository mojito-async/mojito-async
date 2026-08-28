# mojito_async/test/stress/t16_fiber_seam_stress_aot.mojo
#
# A1.5 (issue #53) — 100k park/resume lifecycle over the fiber-backed
# scheduler seam (issue #33 exit criterion retained).
#
# One worker drives 30 live tasks; EVERY task parks PARKS=3334 times on its
# own fiber (a frame-migrating park: the body's seam_park_switch moves the
# RUNNING frame off the worker's native context, the dispatcher commits
# fiber_suspend_current [RUNNING->PARKING->WAITING, generation-bumped], the
# driver wakes each parked task exactly once per episode via
# fiber_resume_current, and the next scheduler slice re-enters the fiber at
# the exact saved point).  Totals:
#
#     parks   = N_WAVE * PARKS                  = 100_020
#     drives  = N_WAVE * (PARKS + 1)            = 100_050  (dispatch slices)
#     switches= 2 * drives                      = 200_100  (ms_ctx_switch)
#
# LIVE-FIBER BOUND (removed in a2/00, issue #101): the old vendored
# mojito-sys bookkeeping kept a FIXED 64-row resume table with no eviction
# (aarch64_switch.S: _ms_resume_tab), capping the process at 32 live fibers.
# The rework replaced that table/globals with an O(1) in-ctx return link, so
# there is NO live-fiber cap (see t27_concurrency_aot, which drives 40).
# 30 live waves is kept here as the fixed geometry of the 100k exact-counter
# proof (drives==100050, switches==200100).
#
# Assertions:
#   - every task COMPLETED; every task parked exactly PARKS times; exact-
#     resume markers intact (ADR-007 stack locality); runnable queue quiet;
#     ZERO stale/skipped records — all states legal in the park/wake machine;
#   - the Runtime fiber-path toggle is EXACT (drives == 100050, switches ==
#     200100) — the deterministic register of the fiber path;
#   - #52 stack-cache policy holds under the same placement pressure: a
#     capacity-16 StackCache sustains 100k acquire->release warm cycles with
#     FLAT committed bytes after warm-up (warm reuse performs no fresh
#     ms_stack_alloc) — the #52 evidence this lane carries.
#
# EXTERN DISCIPLINE (modular/modular#6971): this driver imports the
# extern-bearing fiber seam (ms_* through fiber.mojo + vendor firewall) and
# the stack pool, so it MUST be AOT (*_aot.mojo — picked up by test/run.sh)
# exactly like t15_stack_cache_stress_aot.
#
# Verdict: exit 0 + "PASS"; any failure prints RED/raises (exit 1).
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    c_free,
    c_malloc,
    entry_pointer,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_total_size,
)
from mojito_async.fiber.fiber import FiberFrame
from mojito_async.fiber.stack_pool import DEFAULT_STACK_BYTES, make_stack_cache
from mojito_async.runtime.fiber_seam import (
    SeamSlot,
    fiber_resume_current,
    fiber_suspend_current,
    make_seam_slot,
    seam_bind_slot,
    seam_destroy_slot,
    seam_drive,
    seam_mark_completed,
    seam_park_switch,
    seam_slot_stride,
)
from mojito_async.integration.sys import IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn
from std.memory import stack_allocation


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]
comptime SUSPEND_PARK = Int(3)  # SuspendReason.PARK

comptime N_WAVE = Int(30)       # live waves in the 100k exact-counter proof
comptime PARKS = Int(3334)      # park episodes per task -> 100,020 parks
comptime N_PARK_TOTAL = Int(100020)


def red(what: String) raises -> None:
    print("T16 fiber seam stress: RED (" + what + ")")
    raise Error(what)


# --- per-task cell (mirrors the TCB/task state the dispatcher drives) --------

struct T16Cell(ImplicitlyCopyable, ImplicitlyDeletable):
    """Per-task state, all cells heap-backed (t25 lesson)."""

    var slices: UnsafePointer[Int, MutUntrackedOrigin]
    var parks: UnsafePointer[Int, MutUntrackedOrigin]
    var marker: UnsafePointer[Int, MutUntrackedOrigin]
    var ok: UnsafePointer[Int, MutUntrackedOrigin]
    var finish: UnsafePointer[Int, MutUntrackedOrigin]
    var tid: UnsafePointer[Int, MutUntrackedOrigin]
    var slot: UnsafePointer[SeamSlot, MutAnyOrigin]

    def __init__(out self):
        self.slices = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.parks = self.slices
        self.marker = self.slices
        self.ok = self.slices
        self.finish = self.slices
        self.tid = self.slices
        self.slot = UnsafePointer[SeamSlot, MutAnyOrigin](unsafe_from_address=1)


struct T16Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Shared scene: the cell array base + the tid->cell-index map."""

    var cells: UnsafePointer[T16Cell, MutAnyOrigin]
    var kof: UnsafePointer[Int, MutUntrackedOrigin]

    def __init__(out self):
        self.cells = UnsafePointer[T16Cell, MutAnyOrigin](unsafe_from_address=1)
        self.kof = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)


def _cell_stride() -> Int:
    var one = stack_allocation[1, T16Cell]()
    return Int(one + 1) - Int(one)


# --- the single shared task trampoline (every task) -------------------------
# Park loop: the FIRST entry records the stack-local marker; every re-entry
# verifies it (exact resume / ADR-007).  Each pass parks mid-frame; on the
# completing slice the dispatcher pre-set finish, so the body unwinds (the
# completion trampoline switches back to the worker).
@export("t16_entry")
def t16_entry(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var cell = fr[].user.bitcast[T16Cell]()
    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)
    if cell[].marker[] == 0:
        cell[].marker[] = Int(local_p)
    while True:
        if cell[].marker[] != Int(local_p):
            cell[].ok[] = 0
        if cell[].finish[] == 1:
            return  # unwound: the completing slice settles COMPLETED
        cell[].parks[] = cell[].parks[] + 1
        seam_park_switch(fr)
        # -- exact resume point (ADR-007): verified at loop top ----------


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# The fiber-backed dispatcher: per task k, slice counting decides the final
# (completing) slice; every other slice parks via fiber_suspend_current.
def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[T16Scene]()
    var k = sc[].kof[tid]
    var cell = sc[].cells + k  # element arithmetic (ptr + n = n cells)
    var h = _handle(tcb_addr, tid)
    cell[].slices[] = cell[].slices[] + 1
    if cell[].slices[] > PARKS:
        cell[].finish[] = 1
    claim_running(h)
    var v = seam_drive(rt, cell[].slot)  # T3 frame-reported verdict
    if not v.is_parked():
        # body unwound (finish was pre-set): terminal slice
        seam_mark_completed(cell[].slot)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        h.tcb()[].mark_result(IntResult(k))
    else:
        fiber_suspend_current(rt, h, SUSPEND_PARK)
    return 1


# --- destroy-while-suspended negative driver (T6, issue #53) ------------------
# A dedicated single-task fiber that parks ONCE; the slot is then destroyed
# while its frame is live: seam_destroy_slot MUST raise loudly (never free a
# live reservation).  After the raise the task is woken and completes, and a
# second destroy succeeds (terminal/inert).
@export("t16_neg_entry")
def t16_neg_entry(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    seam_park_switch(fr)  # park mid-frame once (suspended); unwind on resume
    return


def dispatch_neg(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var slot = UnsafePointer[SeamSlot, MutAnyOrigin](
        unsafe_from_address=Int(ud)
    )
    var h = _handle(tcb_addr, tid)
    claim_running(h)
    var v = seam_drive(rt, slot)  # T3 verdict
    if not v.is_parked():
        seam_mark_completed(slot)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        h.tcb()[].mark_result(IntResult(0))
    else:
        fiber_suspend_current(rt, h, SUSPEND_PARK)
    return 1


# --- per-task cell layout inside one Int scratch block ----------------------
# CELL_INTS = 6: slices, parks, marker, ok, finish AND tid occupy one row;
# capping at 5 wrote tid (+5) past the row into the next task's first cell —
# a real 8-byte heap OOB at task 29 (fold fix, issue #53).
comptime CELL_INTS = Int(6)


def main() raises:
    var failures = List[String]()
    var ps = Int(ms_page_size())
    if ps <= 0:
        failures.append("ms_page_size non-positive")
    var stack_bytes = 4 * ps

    # --- cell array + int scratch + tid map (stable heap, never moves) -----
    var cstride = _cell_stride()
    if cstride <= 0:
        failures.append("cell stride non-positive")
    var cells = UnsafePointer[T16Cell, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(N_WAVE * cstride))
    )
    var ibuf = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(N_WAVE * CELL_INTS * 8))
    )
    for i in range(N_WAVE * CELL_INTS):
        ibuf[i] = 0
    var kof = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc((N_WAVE + 64) * 8))
    )
    for i in range(N_WAVE + 64):
        kof[i] = 0
    var slots_block = c_malloc(N_WAVE * seam_slot_stride())
    var slots = UnsafePointer[SeamSlot, MutAnyOrigin](
        unsafe_from_address=Int(slots_block)
    )
    for k in range(N_WAVE):
        (slots + k)[0] = make_seam_slot()

    var sc = T16Scene()
    sc.cells = cells
    sc.kof = kof
    var scp = UnsafePointer[T16Scene, MutAnyOrigin](to=sc)
    var scene_ud = scp.bitcast[Byte]()

    # --- wire the cells once (pointers never change; values reset) ---------
    for k in range(N_WAVE):
        (cells + k)[0] = T16Cell()
        var c = cells + k
        c[].slices = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(ibuf) + (k * CELL_INTS + 0) * 8)
        c[].parks = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(ibuf) + (k * CELL_INTS + 1) * 8)
        c[].marker = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(ibuf) + (k * CELL_INTS + 2) * 8)
        c[].ok = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(ibuf) + (k * CELL_INTS + 3) * 8)
        c[].finish = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(ibuf) + (k * CELL_INTS + 4) * 8)
        c[].tid = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=Int(ibuf) + (k * CELL_INTS + 5) * 8)
        c[].slot = slots + k

    var rt = create()

    # One shared BytePtr-slot buffer for stack acquisition (addresses are
    # COPIED into the fibers at bind; the buffer is dead scratch afterwards —
    # the t26/t25 proven ms_stack_alloc shape).
    var sbuf = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(N_WAVE * 2 * 8))
    )

    # ---- build the 30 live tasks: TCB pool (settle), fibers, spawn --------
    var tcb_list = List[TB]()
    for _ in range(N_WAVE):
        tcb_list.append(TB.create())
    var handles = List[JoinHandle[IntResult]]()
    for k in range(N_WAVE):
        var c = cells + k
        c[].slices[] = 0
        c[].parks[] = 0
        c[].marker[] = 0
        c[].ok[] = 1
        c[].finish[] = 0
        var scell = sbuf + 2 * k
        if ms_stack_alloc(stack_bytes, scell, scell + 1) != 0:
            failures.append("task " + String(k) + " stack alloc failed")
        var ns = NativeStack(scell[0], (scell + 1)[0])
        seam_bind_slot(
            c[].slot, ns, entry_pointer["t16_entry"](),
            (c).bitcast[Byte](),
        )
        var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_list[k]), 0)
        handles.append(h)
        c[].tid[] = h.id()
        kof[h.id()] = k

    # ---- the park/resume lifecycle: PARKS episodes ------------------------
    for ep in range(PARKS):
        var served = scheduler_loop(rt, dispatch, scene_ud)
        if served != N_WAVE:
            failures.append("ep " + String(ep) + " served "
                            + String(served) + " != " + String(N_WAVE))
        for k in range(N_WAVE):
            fiber_resume_current(rt, handles[k])
        if rt.pending() != N_WAVE:
            failures.append("ep " + String(ep) + " pending "
                            + String(rt.pending()) + " != " + String(N_WAVE))

    # ---- final drive: every task completes on its PARKS+1 slice -----------
    var served_f = scheduler_loop(rt, dispatch, scene_ud)
    if served_f != N_WAVE:
        failures.append("final served " + String(served_f)
                        + " != " + String(N_WAVE))

    # ---- verdicts ----------------------------------------------------------
    for k in range(N_WAVE):
        var c = cells + k
        if not handles[k].is_completed():
            failures.append("task " + String(k) + " not COMPLETED")
        if c[].parks[] != PARKS:
            failures.append("task " + String(k) + " parked " + String(c[].parks[])
                            + " != " + String(PARKS))
        if c[].ok[] != 1:
            failures.append("task " + String(k) + " lost its exact resume marker")
    if rt.pending() != 0:
        failures.append("queue not quiet after the lifecycle")
    if rt.skipped() != 0:
        failures.append("stale records skipped (" + String(rt.skipped()) + ")")

    # ---- deterministic fiber-path register --------------------------------
    if rt.fiber_drives() != N_WAVE * (PARKS + 1):
        failures.append("fiber drives " + String(rt.fiber_drives())
                        + " != 100050")
    if rt.fiber_switches() != 2 * N_WAVE * (PARKS + 1):
        failures.append("fiber switches " + String(rt.fiber_switches())
                        + " != 200100")
    var parks_total = 0
    for k in range(N_WAVE):
        parks_total += (cells + k)[].parks[]
    if parks_total != N_PARK_TOTAL:
        failures.append("total parks " + String(parks_total)
                        + " != 100020")

    # ---- #52 stack-cache policy under the same placement pressure --------
    var cache = make_stack_cache(16, DEFAULT_STACK_BYTES)
    # warm-up: fill the free set with cold allocations, then release all.
    var warm = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(16 * 8))
    )
    for i in range(16):
        warm[i] = Int(cache.acquire())
    for i in range(16):
        cache.release(
            UnsafePointer[NativeStack, MutAnyOrigin](unsafe_from_address=warm[i])
        )
    var committed_warm = Int(ms_stack_total_size())
    for i in range(100000):
        var acquired = cache.acquire()
        cache.release(acquired)
    var committed_now = Int(ms_stack_total_size())
    if committed_now != committed_warm:
        failures.append("cache committed bytes moved on warm reuse ("
                        + String(committed_now) + " != " + String(committed_warm)
                        + ")")
    if cache.live() != 0 or cache.cached() != 16:
        failures.append("cache free-set not intact after 100k cycles")
    cache.drain()

    # ---- teardown: fibers own their reservations; destroy releases --------
    for k in range(N_WAVE):
        seam_destroy_slot(slots + k)
    c_free(slots_block)
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(sbuf)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(cells)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(ibuf)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(kof)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(warm)))

    # ---- destroy-while-suspended negative (T6, issue #53) -----------------
    var slot_neg_block = c_malloc(seam_slot_stride())
    var slot_neg = UnsafePointer[SeamSlot, MutAnyOrigin](
        unsafe_from_address=Int(slot_neg_block)
    )
    slot_neg[0] = make_seam_slot()
    var neg_ud = Int(slot_neg)
    var nsbuf = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(2 * 8))
    )
    if ms_stack_alloc(stack_bytes, nsbuf, nsbuf + 1) != 0:
        failures.append("negative driver stack alloc failed")
    var ns_neg = NativeStack(nsbuf[0], (nsbuf + 1)[0])
    seam_bind_slot(
        slot_neg, ns_neg, entry_pointer["t16_neg_entry"](),
        UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x99),
    )
    var rt3 = create()
    var tcb_neg = TB.create()
    var h_neg = spawn(
        rt3, UnsafePointer[TB, MutAnyOrigin](to=tcb_neg), 0
    )
    var ud_neg = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=neg_ud)
    var ud_neg_bp = ud_neg.bitcast[Byte]()
    var served_neg = scheduler_loop(rt3, dispatch_neg, ud_neg_bp)
    if served_neg != 1:
        failures.append("negative drive served " + String(served_neg))
    if h_neg.state() != TaskControlBlock.WAITING:
        failures.append("negative fiber not WAITING after drive")
    # destroy-while-suspended MUST raise loudly
    var destroyed_live = False
    try:
        seam_destroy_slot(slot_neg)
    except e:
        if "parked/suspended" in String(e):
            destroyed_live = True
    if not destroyed_live:
        failures.append("destroy-while-suspended did NOT raise loudly")
    # wake, complete, then destroy succeeds (terminal/inert)
    fiber_resume_current(rt3, h_neg)
    if h_neg.state() != TaskControlBlock.RUNNABLE:
        failures.append("negative wake not RUNNABLE")
    var served_neg2 = scheduler_loop(rt3, dispatch_neg, ud_neg_bp)
    if served_neg2 != 1:
        failures.append("negative final drive served " + String(served_neg2))
    if not h_neg.is_completed():
        failures.append("negative fiber did not complete after wake")
    seam_destroy_slot(slot_neg)  # inert now: must NOT raise
    if slot_neg[].fiber.alive():
        failures.append("negative slot fiber still alive after destroy")
    c_free(slot_neg_block)
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(nsbuf)))

    if len(failures) == 0:
        print("T16 fiber seam stress: parks=100020 drives="
              + String(rt.fiber_drives()) + " switches="
              + String(rt.fiber_switches()) + " PASS")
    else:
        print("T16 fiber seam stress: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)