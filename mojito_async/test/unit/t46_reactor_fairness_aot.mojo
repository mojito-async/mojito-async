# mojito_async/test/unit/t46_reactor_fairness_aot.mojo
#
# A7.9 (issue #83) — scheduler fairness CPU-vs-IO acceptance driver: proves
# a CPU-bound fiber does not starve an I/O-bound fiber parked on the
# reactor, by composing the ALREADY-MERGED `fair_scheduler_loop` (A2.7,
# issue #73, t36_fairness_aot.mojo) with `reactor/fairness.mojo`'s
# `fair_service_io` (issue #83) as the budget's service callback — exactly
# the composition fairness.mojo's module docblock documents.
#
# Scene (mirrors t36_fairness_aot.mojo's event-log style, simplified to
# the one scenario issue #83 asks for): a NEVER-YIELDING CPU hog re-queues
# itself every slice (bounded so the drive quiesces); a real pipe already
# has a byte sitting in it before the drive starts (readiness exists from
# slice 0, exactly like a real I/O-bound fiber that is ready to make
# progress the instant anyone polls) and an I/O task is parked
# (register_and_park) on that pipe's read side. Budget_k=4: the acceptance
# is that the I/O task's readiness gets serviced WITHIN THE FIRST BUDGET
# WINDOW (by the 4th-5th hog slice), never deferred to the end of the
# hog's run — proving the reactor is NOT starved by continuous local CPU
# work, matching issue #83's "the I/O class completes within a bound of
# its solo-parking rate... even while the CPU class runs at full budget."
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation

from mojito_async.integration.sys import BytePtr
from mojito_async.reactor.fairness import fair_service_io
from mojito_async.reactor.io_token import IoOpKind
from mojito_async.reactor.reactor import Reactor, make_reactor
from mojito_async.runtime.runtime import Nil, Runtime, create
from mojito_async.runtime.scheduler import fair_scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoInterest


@extern("pipe")
def c_pipe(fds: UnsafePointer[Int32, MutAnyOrigin]) abi("C") -> Int32:
    ...


def red(what: String) raises -> None:
    print("T46 reactor fairness: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[Nil]
comptime HOG_LIMIT = Int(40)  # bounded "endless" hog; drive quiesces after this
comptime BUDGET_K = Int(4)

comptime EV_HOG = Int(1)
comptime EV_IO_READY = Int(2)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var reactor: UnsafePointer[Reactor, MutAnyOrigin]
    var log: UnsafePointer[Int, MutAnyOrigin]      # ring of EV_* codes
    var log_n: UnsafePointer[Int, MutAnyOrigin]
    var hog_count: UnsafePointer[Int, MutAnyOrigin]
    var hog_id: Int
    var io_id: Int

    def __init__(out self):
        self.reactor = UnsafePointer[Reactor, MutAnyOrigin](unsafe_from_address=1)
        self.log = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.log_n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.hog_count = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.hog_id = 0
        self.io_id = 0


def _rec(mut sc: Scene, ev: Int):
    var i = sc.log_n[0]
    sc.log[i] = ev
    sc.log_n[0] = i + 1


def _first(mut sc: Scene, ev: Int) -> Int:
    for i in range(sc.log_n[0]):
        if sc.log[i] == ev:
            return i
    return -1


def _last(mut sc: Scene, ev: Int) -> Int:
    var found = -1
    for i in range(sc.log_n[0]):
        if sc.log[i] == ev:
            found = i
    return found


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[Nil]:
    return JoinHandle[Nil](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """The hog re-queues its own RUNNABLE record every slice (never parks,
    never yields — the §67/§71 documented cooperative-limitation shape,
    same as t36's dispatch_yield_hog's sibling `body_hog`/hog branch); the
    io task is dispatched exactly ONCE (its own park happens OUTSIDE this
    dispatcher, before the drive starts) to record that it was woken and
    complete."""
    var sc = ud.bitcast[Scene]()
    var h = _handle(tcb_addr, tid)
    h.tcb()[].transition(TaskControlBlock.RUNNING)
    if tid == sc[].hog_id:
        var c = sc[].hog_count[0]
        _rec(sc[], EV_HOG)
        if c + 1 < HOG_LIMIT:
            sc[].hog_count[0] = c + 1
            h.tcb()[].transition(TaskControlBlock.PARKING)
            h.tcb()[].transition(TaskControlBlock.RUNNABLE)
            rt.enqueue_local(tcb_addr, tid)
        else:
            sc[].hog_count[0] = c + 1
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
        return 1
    if tid == sc[].io_id:
        _rec(sc[], EV_IO_READY)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        return 1
    raise Error("t46: unexpected task id " + String(tid))


def service_sweep(mut rt: Runtime, ud: BytePtr) raises:
    """The budget's service callback (issue #83's documented composition):
    a NONBLOCKING reactor sweep every BUDGET_K local slices — exactly
    `fair_scheduler_loop`'s seam, fed by `fairness.fair_service_io`."""
    var sc = ud.bitcast[Scene]()
    _ = fair_service_io(rt, sc[].reactor[])


def main() raises:
    var fds = stack_allocation[2, Int32]()
    if c_pipe(fds) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds[0]
    var wfd = fds[1]

    var rt = create()
    var reactor = make_reactor()

    var log_buf = stack_allocation[256, Int]()
    for i in range(256):
        log_buf[i] = 0
    var log_n_cell = stack_allocation[1, Int]()
    log_n_cell[0] = 0
    var hog_count_cell = stack_allocation[1, Int]()
    hog_count_cell[0] = 0

    var sc = Scene()
    sc.reactor = UnsafePointer[Reactor, MutAnyOrigin](to=reactor)
    sc.log = log_buf
    sc.log_n = log_n_cell
    sc.hog_count = hog_count_cell

    # The I/O task parks FIRST (register_and_park), then the pipe becomes
    # readable — readiness exists from the very first budget sweep onward,
    # exactly like a real I/O-bound fiber ready to make progress the
    # instant anyone polls.
    var io_tcb = TB.create()
    var io_h = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=io_tcb), 0)
    claim_running(io_h)
    var token = reactor.register_and_park[Nil](
        rt, io_h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if io_h.state() != TaskControlBlock.WAITING:
        red("io task must park before the hog ever runs")
    sc.io_id = io_h.id()
    var wf = FileDescriptor(Int(wfd))
    wf.write("A")

    var hog_tcb = TB.create()
    var hog_h = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=hog_tcb), 0)
    sc.hog_id = hog_h.id()

    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    _ = fair_scheduler_loop[R=Nil](rt, dispatch, ud, service_sweep, budget_k=BUDGET_K)

    if not io_h.is_completed():
        red("io task never completed — the reactor was starved by the CPU hog")
    if not hog_h.is_completed():
        red("hog task never completed (drive did not quiesce)")
    reactor.unregister(token)
    if reactor.live_count() != 0:
        red("reactor left a live registration after the run")

    var io_at = _first(sc, EV_IO_READY)
    var hog_last = _last(sc, EV_HOG)
    if io_at < 0:
        red("io readiness was never observed in the event log")
    # The acceptance: the I/O wake must land WELL BEFORE the hog's final
    # slice — bounded by roughly one budget window (K + a small margin),
    # never deferred to "after the hog finally quiesces" (which is what
    # PLAIN, non-fair scheduling would do: local work always drains before
    # a reactor poll is ever attempted).
    var margin = BUDGET_K + 2
    if io_at > margin:
        red("io readiness deferred past the first budget window: observed at log index "
            + String(io_at) + ", expected <= " + String(margin)
            + " (hog's last slice was at " + String(hog_last) + ") — the CPU hog starved the reactor")

    print("T46 fairness ok (io_ready_at=" + String(io_at) + " hog_last_at=" + String(hog_last)
          + " budget_k=" + String(BUDGET_K) + " hog_slices=" + String(hog_count_cell[0]) + ")")
    print("T46 reactor fairness: PASS")
