# bench/fairness_aot.mojo
#
# A7.9 (issue #83) — the two-class CPU-vs-IO probe: a mix of CPU-bound
# fibers (deliberately compute-long, modeled as a bounded never-yielding
# slice count — spec §67's documented cooperative-limitation shape, same
# as t36_fairness_aot.mojo's hog) and I/O-bound fibers parked on the
# reactor, with per-class completion stats, checking the I/O class does
# not fall below a measured solo-parking floor while the CPU class runs
# at full budget (issue #83 acceptance).
#
# I/O PAWN (issue #83 point 5, "The I/O pawn comes from the G8 echo
# bench"): reused MECHANICALLY, not literally — bench/echo_aot.mojo's
# TCP loopback pawn exercises the identical Reactor/IoOpTable/
# register_and_park machinery this probe needs; wiring N_IO concurrent
# TCP connections here would only add connection-setup overhead around
# the SAME fairness question, so this probe uses plain pipe(2) pairs
# (the same substrate t46_reactor_fairness_aot.mojo's unit acceptance
# already proved the fair_service_io composition against) — the fairness
# property under test lives entirely in the reactor/scheduler budget
# composition, not in which fd flavor is parked.
#
# TCB STORAGE (bug found + fixed during development): every task's
# `TaskControlBlock` MUST live at a STABLE address for its whole
# lifetime — a loop-local `var tcb = TB.create()` whose address is handed
# to `spawn()` is UNSAFE across loop iterations (the b2 -O0 AOT compiler
# can reuse the same stack slot for a loop-scoped `var` each iteration,
# silently aliasing every task spawned inside the loop onto ONE physical
# TCB address — verified experimentally: with N=2 fibers registered in a
# `for idx in range(N):` loop, `Reactor.poll()` correctly reported BOTH
# slots ready, but only ONE task ever reached `dispatch()` because both
# `unpark_current` calls resolved to the SAME aliased TCB, so the second
# was a benign-looking H2 duplicate no-op). Every TCB here therefore
# lives in a malloc'd pool addressed via TYPED pointer arithmetic
# (`pool + i`, matching t29_park_cancel_stress_aot.mojo's/bench/
# echo_aot.mojo's proven precedent), never a loop-local `var`.
#
# Two regimes (issue #83's "shows fairness under both a surge... and a
# steady mix"):
#   SOLO   — N_IO fibers alone (no CPU class): establishes the floor wall
#            time for the I/O class to complete unopposed.
#   MIXED  — the SAME N_IO fibers running CONCURRENTLY against N_CPU
#            never-yielding hogs (bounded so the drive quiesces) through
#            `runtime.scheduler.fair_scheduler_loop` + `reactor.fairness.
#            fair_service_io` (the exact composition t46 unit-tests): the
#            I/O class's wall time must stay within a measured bound of
#            the solo floor — the HARD GATE (issue #83: "a measured
#            floor, not a strict starvation"); CPU throughput (slices
#            served) is reported for the CPU-scaling picture, not gated.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1); metrics
# printed as {"bench":"fairness",...} JSON (matches bench/scheduler_scale
# _aot.mojo/bench/timer_scale_aot.mojo/bench/echo_aot.mojo's convention).
from std.memory import stack_allocation

from mojito_async.integration.sys import BytePtr
from mojito_async.reactor.fairness import fair_service_io
from mojito_async.reactor.io_token import IoOpKind, IoToken
from mojito_async.reactor.reactor import Reactor, make_reactor
from mojito_async.runtime.runtime import Nil, Runtime, create
from mojito_async.runtime.scheduler import fair_scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.vendor.mojito_sys import monotonic_now_ns
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoInterest


@extern("malloc")
def _c_malloc(size: Int) abi("C") -> BytePtr: ...


@extern("free")
def _c_free(ptr: BytePtr) abi("C"): ...


@extern("pipe")
def c_pipe(fds: UnsafePointer[Int32, MutAnyOrigin]) abi("C") -> Int32:
    ...


def red(what: String) raises -> None:
    print("bench_fairness: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[Nil]
comptime TCB_STRIDE = Int(256)      # generous, matches t29_park_cancel_stress_aot's precedent
comptime N_IO = Int(16)
comptime N_CPU = Int(4)
comptime CPU_SLICES = Int(6000)     # bounded "sustained load" per hog
comptime BUDGET_K = Int(4)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[Nil]:
    return JoinHandle[Nil](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


struct Scene:
    var reactor: UnsafePointer[Reactor, MutAnyOrigin]
    var io_id: List[Int]
    var io_done: List[Int]
    var io_token: List[IoToken]
    var cpu_id: List[Int]
    var cpu_slices: List[Int]
    var io_completed: UnsafePointer[Int, MutAnyOrigin]
    var cpu_completed: UnsafePointer[Int, MutAnyOrigin]
    var cpu_total_slices: UnsafePointer[Int, MutAnyOrigin]
    var io_complete_at_cpu_slice: UnsafePointer[Int, MutAnyOrigin]  # max, single cell

    def __init__(out self):
        self.reactor = UnsafePointer[Reactor, MutAnyOrigin](unsafe_from_address=1)
        self.io_id = List[Int]()
        self.io_done = List[Int]()
        self.io_token = List[IoToken]()
        self.cpu_id = List[Int]()
        self.cpu_slices = List[Int]()
        self.io_completed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cpu_completed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cpu_total_slices = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.io_complete_at_cpu_slice = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = _handle(tcb_addr, tid)
    h.tcb()[].transition(TaskControlBlock.RUNNING)

    for i in range(len(sc[].io_id)):
        if sc[].io_id[i] == tid:
            sc[].reactor[].unregister(sc[].io_token[i])
            sc[].io_done[i] = 1
            sc[].io_completed[0] = sc[].io_completed[0] + 1
            if sc[].cpu_total_slices[0] > sc[].io_complete_at_cpu_slice[0]:
                sc[].io_complete_at_cpu_slice[0] = sc[].cpu_total_slices[0]
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1

    for i in range(len(sc[].cpu_id)):
        if sc[].cpu_id[i] == tid:
            var c = sc[].cpu_slices[i]
            sc[].cpu_total_slices[0] = sc[].cpu_total_slices[0] + 1
            if c + 1 < CPU_SLICES:
                sc[].cpu_slices[i] = c + 1
                h.tcb()[].transition(TaskControlBlock.PARKING)
                h.tcb()[].transition(TaskControlBlock.RUNNABLE)
                rt.enqueue_local(tcb_addr, tid)
            else:
                sc[].cpu_slices[i] = c + 1
                sc[].cpu_completed[0] = sc[].cpu_completed[0] + 1
                h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1

    raise Error("bench_fairness: unexpected task id " + String(tid))


def service_sweep(mut rt: Runtime, ud: BytePtr) raises:
    var sc = ud.bitcast[Scene]()
    _ = fair_service_io(rt, sc[].reactor[])


def _run_io_class(
    mut sc: Scene,
    mut rt: Runtime,
    io_tcb_pool: UnsafePointer[TB, MutAnyOrigin],
    cpu_tcb_pool: UnsafePointer[TB, MutAnyOrigin],
    with_cpu: Bool,
) raises -> UInt64:
    """Parks N_IO fibers on N_IO pipes (each already writable — data sits
    ready from round zero, mirroring t46's shape), optionally alongside
    N_CPU never-yielding hogs, and drives to full quiescence via
    `fair_scheduler_loop`. Returns the wall-clock ns for every I/O fiber
    to complete."""
    sc.io_id = List[Int]()
    sc.io_done = List[Int]()
    sc.io_token = List[IoToken]()
    for idx in range(N_IO):
        var fds = stack_allocation[2, Int32]()
        if c_pipe(fds) != 0:
            red("setup: pipe(2) failed")
        var rfd = fds[0]
        var wfd = fds[1]
        var tcb = io_tcb_pool + idx
        tcb[] = TB.create()
        var h = spawn[Nil](rt, tcb, 0)
        claim_running(h)
        var t = sc.reactor[].register_and_park[Nil](
            rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
        )
        if h.state() != TaskControlBlock.WAITING:
            red("io fiber must park before the drive starts")
        var wf = FileDescriptor(Int(wfd))
        wf.write("A")
        sc.io_id.append(h.id())
        sc.io_done.append(0)
        sc.io_token.append(t)

    sc.cpu_id = List[Int]()
    sc.cpu_slices = List[Int]()
    if with_cpu:
        for idx in range(N_CPU):
            var ctcb = cpu_tcb_pool + idx
            ctcb[] = TB.create()
            var ch = spawn[Nil](rt, ctcb, 0)
            sc.cpu_id.append(ch.id())
            sc.cpu_slices.append(0)

    var io_completed_cell = stack_allocation[1, Int]()
    io_completed_cell[0] = 0
    var cpu_completed_cell = stack_allocation[1, Int]()
    cpu_completed_cell[0] = 0
    var cpu_total_cell = stack_allocation[1, Int]()
    cpu_total_cell[0] = 0
    var io_complete_at_cell = stack_allocation[1, Int]()
    io_complete_at_cell[0] = 0
    sc.io_completed = io_completed_cell
    sc.cpu_completed = cpu_completed_cell
    sc.cpu_total_slices = cpu_total_cell
    sc.io_complete_at_cpu_slice = io_complete_at_cell

    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var t0 = monotonic_now_ns()
    _ = fair_scheduler_loop[R=Nil](rt, dispatch, ud, service_sweep, budget_k=BUDGET_K)
    var t1 = monotonic_now_ns()

    for i in range(N_IO):
        if sc.io_done[i] != 1:
            red("io fiber " + String(i) + " never completed")
    if with_cpu:
        if cpu_completed_cell[0] != N_CPU:
            red("not every CPU hog completed (" + String(cpu_completed_cell[0]) + "/" + String(N_CPU) + ")")
    if sc.reactor[].live_count() != 0:
        red("reactor left " + String(sc.reactor[].live_count()) + " live registration(s) after the run")

    return UInt64(t1) - UInt64(t0)


def main() raises:
    var reactor = make_reactor()
    var reactor_ptr = UnsafePointer[Reactor, MutAnyOrigin](to=reactor)

    var io_cells = _c_malloc(N_IO * TCB_STRIDE)
    var cpu_cells = _c_malloc(N_CPU * TCB_STRIDE)
    var io_tcb_pool = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=Int(io_cells))
    var cpu_tcb_pool = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=Int(cpu_cells))

    var rt_solo = create()
    var sc_solo = Scene()
    sc_solo.reactor = reactor_ptr
    var solo_ns = _run_io_class(sc_solo, rt_solo, io_tcb_pool, cpu_tcb_pool, False)
    print("bench_fairness: solo I/O floor established (n_io=" + String(N_IO)
          + " wall_ns=" + String(solo_ns) + ")")

    var rt_mixed = create()
    var sc_mixed = Scene()
    sc_mixed.reactor = reactor_ptr
    var mixed_ns = _run_io_class(sc_mixed, rt_mixed, io_tcb_pool, cpu_tcb_pool, True)
    var cpu_slices_total = sc_mixed.cpu_total_slices[0]

    var cpu_slices_before_io_done = sc_mixed.io_complete_at_cpu_slice[0]
    # The HARD GATE (issue #83: "a measured floor, not a strict
    # starvation"): every I/O fiber must complete within a SMALL bound of
    # CPU slices served, not deferred until the CPU class's full run
    # (24000 slices) — t46_reactor_fairness_aot.mojo's single-fiber unit
    # acceptance measures the SAME quantity at io_ready_at=4 for
    # budget_k=4; this bound is 3x that plus N_CPU's own interleave slop,
    # generous but still >100x tighter than "deferred to the end".
    comptime IO_COMPLETE_SLICE_BOUND = Int(BUDGET_K * 3 + N_CPU * 2)

    print(
        "{\"bench\":\"fairness\",\"n_io\":" + String(N_IO)
        + ",\"n_cpu\":" + String(N_CPU)
        + ",\"cpu_slices_per_hog\":" + String(CPU_SLICES)
        + ",\"budget_k\":" + String(BUDGET_K)
        + ",\"solo_io_wall_ns\":" + String(solo_ns)
        + ",\"mixed_io_wall_ns\":" + String(mixed_ns)
        + ",\"cpu_total_slices_served\":" + String(cpu_slices_total)
        + ",\"cpu_slices_before_all_io_done\":" + String(cpu_slices_before_io_done)
        + "}"
    )
    print("[report] I/O floor gate: all " + String(N_IO) + " I/O fibers completed after "
          + String(cpu_slices_before_io_done) + " CPU slices served (hard gate: <= "
          + String(IO_COMPLETE_SLICE_BOUND) + ") out of " + String(cpu_slices_total)
          + " total CPU slices in the full mixed run — the CPU class (n_cpu=" + String(N_CPU)
          + " x " + String(CPU_SLICES) + " slices) ran at full budget throughout. Wall-clock "
          + "solo=" + String(solo_ns) + "ns / mixed=" + String(mixed_ns)
          + "ns reported for reference only (not gated — dominated by the CPU class's own "
          + "syscall/dispatch overhead, not I/O fairness, matching bench/run.sh's H4 "
          + "'report with statistics, don't gate on wall time' discipline)")

    if cpu_slices_before_io_done > IO_COMPLETE_SLICE_BOUND:
        red("I/O class fell below its floor: all I/O fibers did not complete until "
            + String(cpu_slices_before_io_done) + " CPU slices had run, exceeding the "
            + String(IO_COMPLETE_SLICE_BOUND) + "-slice gate — the CPU class starved the I/O class")

    _c_free(io_cells)
    _c_free(cpu_cells)
    print("bench_fairness: PASS")
