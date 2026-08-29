# mojito_async/test/stress/t38_mutex_cross_worker_aot.mojo
#
# A4.1 (issue #55) — multi-worker Mutex[T] producer/consumer handoff stress:
# zero lost wakeups, no duplicate resumes, across TWO REAL worker OS
# threads driving the REAL Mutex[Int] + scheduler_loop machinery end to
# end (t34/t34b/t34c already prove the raw park.mojo kernel in isolation;
# this exercises the actual sync-primitive CONSUMER of that kernel).
#
# Background: Mutex.lock()'s slow path used the SINGLE-PHASE `park_current`
# through A2.5; on the A2 M:N scheduler a foreign unlock() racing into the
# PARKING window (between the PARKING transition and the WAITING commit)
# was silently dropped — `park_current` never consults the early-wake
# latch — a genuine lost wakeup that parks the granted waiter FOREVER
# (unlock() already popped it off the FIFO and stamped its GRANT marker;
# nothing else will ever re-drive it).  The fix switches lock()/acquire()
# to the two-phase `park_prepare`/`park_validate`/`park_commit` kernel
# (A2.5, issue #71) park.mojo's cross-worker wake routing already used.
#
# Scene: TWO REAL worker OS threads (w0, w1) are SYMMETRIC contenders on
# ONE shared `Mutex[Int]` counter — no artificial synchronization gate
# beyond the mutex itself, so the interleavings (including a release
# landing inside the other side's PARKING window) are whatever REAL OS
# thread scheduling produces across ROUNDS_PER_WORKER contended rounds per
# side.  Each round: acquire (fast or contended-slow), increment the
# shared counter by exactly 1, release, repeat.  A lost wakeup would park
# a waiter FOREVER (caught by this driver's bounded watchdog instead of
# hanging CI); a duplicate resume or a torn handoff would corrupt the
# final counter (it would not equal exactly 2 * ROUNDS_PER_WORKER) or leave
# the mutex un-drained.
#
# Deadlock/hang detection: every drive loop is BOUNDED — a lost wakeup
# would hang the owning worker thread on this driver's very own bug (the
# a4.1 defense exists precisely so it doesn't); a bound trip prints RED
# and forces process exit instead of hanging CI.
#
# Verdict: exit 0 + "PASS"; any hang/mismatch prints RED and forces exit 1.
# AOT-only (pthread externs; modular/modular#6971).
from std.time import sleep
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import Worker, make_worker
from mojito_async.sync import Mutex
from mojito_async.task import JoinHandle, claim_running
from mojito_async.vendor.mojito_sys import c_malloc, entry_pointer


@extern("pthread_create")
def _pthread_create(
    thread: UnsafePointer[Int, MutAnyOrigin],
    attr: Int,
    start_routine: UnsafePointer[Byte, MutAnyOrigin],
    arg: BytePtr,
) abi("C") -> Int32: ...


@extern("pthread_join")
def _pthread_join(thread: UInt, retval: UInt) abi("C") -> Int32: ...


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]

comptime ROUNDS_PER_WORKER = Int(500)
comptime SPIN_BUDGET = Int(4000000)

comptime W0_ID = Int(1)
comptime W1_ID = Int(2)


def _fail(failures: UnsafePointer[Int, MutAnyOrigin], msg: String):
    failures[0] = failures[0] + 1
    print("  - " + msg)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var w0: UnsafePointer[Worker, MutAnyOrigin]
    var w1: UnsafePointer[Worker, MutAnyOrigin]
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var failures: UnsafePointer[Int, MutAnyOrigin]
    var thread_err: UnsafePointer[Int, MutAnyOrigin]   # 2-cell array
    var rounds0: UnsafePointer[Int, MutAnyOrigin]       # worker0 completed rounds
    var rounds1: UnsafePointer[Int, MutAnyOrigin]       # worker1 completed rounds

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.w1 = self.w0
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.thread_err = self.failures
        self.rounds0 = self.failures
        self.rounds1 = self.failures


# ---------------------------------------------------------------------------
# Per-worker dispatch: acquire (fast or contended-slow), bump the shared
# counter, release, count the completed round.
# ---------------------------------------------------------------------------

def dispatch_w0(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    if sc[].mtx[].lock(rt, h):
        sc[].mtx[].value()[0] += 1
        _ = sc[].mtx[].unlock[IntResult](rt)
        sc[].rounds0[] += 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def dispatch_w1(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    if sc[].mtx[].lock(rt, h):
        sc[].mtx[].value()[0] += 1
        _ = sc[].mtx[].unlock[IntResult](rt)
        sc[].rounds1[] += 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def _drive_one_round[
    F: def(mut Runtime, Int, Int, BytePtr) raises -> Int
](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
    worker_id: Int,
    failures: UnsafePointer[Int, MutAnyOrigin],
) raises -> Bool:
    """Spawn one fresh one-shot task and drive it to COMPLETED.  Bounded
    spin+sleep watchdog: a lost wakeup would otherwise hang this thread
    forever (the exact bug this driver defends against)."""
    var tcb = TB.create()
    var tcbp = UnsafePointer[TB, MutAnyOrigin](to=tcb)
    tcb.transition(TaskControlBlock.RUNNABLE)
    rt.enqueue_local(Int(tcbp), 1)
    var h = JoinHandle[IntResult](tcbp, 1)

    var spins = 0
    while not h.is_completed():
        _ = scheduler_loop(rt, dispatcher, ud, worker_id=worker_id)
        spins += 1
        if spins > SPIN_BUDGET:
            _fail(failures, "worker " + String(worker_id)
                  + ": round did not complete (LOST WAKEUP or hang)")
            return False
        sleep(0.00001)
    return True


def serve_worker0(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rt = sc.w0[].runtime()
    for r in range(ROUNDS_PER_WORKER):
        if not _drive_one_round(rt[], dispatch_w0, scp.bitcast[Byte](), W0_ID, sc.failures):
            return


def serve_worker1(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rt = sc.w1[].runtime()
    for r in range(ROUNDS_PER_WORKER):
        if not _drive_one_round(rt[], dispatch_w1, scp.bitcast[Byte](), W1_ID, sc.failures):
            return


@export("t38_worker0")
def t38_worker0(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker0(sc)
    except e:
        sc[].thread_err[0] = 1


@export("t38_worker1")
def t38_worker1(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker1(sc)
    except e:
        sc[].thread_err[1] = 1


def run_scenario(failures: UnsafePointer[Int, MutAnyOrigin]) raises:
    var w0 = make_worker()
    var w1 = make_worker()
    var w0p = UnsafePointer[Worker, MutAnyOrigin](to=w0)
    var w1p = UnsafePointer[Worker, MutAnyOrigin](to=w1)
    var mtx = Mutex[Int](0)
    var mtxp = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].w0 = w0p
    sc[].w1 = w1p
    sc[].mtx = mtxp
    var cells = Int(c_malloc(8 * 8))
    var p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    for i in range(8):
        p[i] = 0
    sc[].failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 0 * 8)
    sc[].thread_err = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 1 * 8)
    sc[].rounds0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 3 * 8)
    sc[].rounds1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 4 * 8)

    var t0: Int = 0
    var t1: Int = 0
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t0), 0,
        entry_pointer["t38_worker0"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t1), 0,
        entry_pointer["t38_worker1"](), sc.bitcast[Byte](),
    )
    _ = _pthread_join(UInt(t0), 0)
    _ = _pthread_join(UInt(t1), 0)

    if sc[].thread_err[0] != 0 or sc[].thread_err[1] != 0:
        _fail(failures, "a worker thread raised (see prints above)")
    if sc[].rounds0[] != ROUNDS_PER_WORKER:
        _fail(failures, "worker0 completed " + String(sc[].rounds0[]) + "/"
              + String(ROUNDS_PER_WORKER) + " rounds")
    if sc[].rounds1[] != ROUNDS_PER_WORKER:
        _fail(failures, "worker1 completed " + String(sc[].rounds1[]) + "/"
              + String(ROUNDS_PER_WORKER) + " rounds")
    var expected = 2 * ROUNDS_PER_WORKER
    if mtx.value()[0] != expected:
        _fail(failures, "shared counter = " + String(mtx.value()[0]) + ", expected "
              + String(expected) + " (lost increment / torn handoff / duplicate resume)")
    if mtx.is_locked() or mtx.waiter_count() != 0:
        _fail(failures, "mutex not drained after the run")
    print("T38 mutex cross-worker: rounds0=" + String(sc[].rounds0[])
          + " rounds1=" + String(sc[].rounds1[]) + " counter=" + String(mtx.value()[0]))


def main() raises:
    var failures = 0
    var fp = UnsafePointer[Int, MutAnyOrigin](to=failures)
    try:
        run_scenario(fp)
    except e:
        print("T38 mutex cross-worker: RED (exception " + String(e) + ")")
        _iso_exit(1)
    if fp[] == 0:
        print("T38 mutex cross-worker: PASS")
    else:
        print("T38 mutex cross-worker: RED (" + String(fp[]) + " failure(s))")
        _iso_exit(1)
