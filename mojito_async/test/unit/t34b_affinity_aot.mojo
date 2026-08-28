# mojito_async/test/unit/t34b_affinity_aot.mojo
#
# A2.5 (issue #71) — STARTED-FIBER WORKER AFFINITY: TDD red->green driver.
#
# Proves the issue #71 affinity exit criteria on TWO REAL worker OS threads
# (the owner driven through the REAL scheduler_loop, worker_id=1):
#
#   - FIRST DISPATCH STAMPS: the task's first dispatch sets STARTED (the
#     TCB latch fires at the first RUNNABLE -> RUNNING) and owner_worker =
#     the current worker (1) + owner_runtime = the owner's Runtime address
#     (scheduler_loop's first-run stamping).
#   - NO OFF-OWNER EXECUTION: across a FORCED cross-worker wake (worker 1,
#     the foreign waker, delivers the wake through ITS OWN Runtime), the
#     started task is NEVER observed executing on the non-owner worker —
#     the wake record lands on the OWNER's REMOTE-ready queue and only the
#     owner pops it.  The body itself asserts the executing worker is the
#     owner on EVERY entry (a migrated run fails the driver immediately).
#   - OWNER IMMUTABLE: after the first run, owner_worker never changes —
#     asserted by the dispatcher on the resume slice, and by the waker
#     (two reads, equal, and equal to 1).
#   - STARTED LATCH: is_started() is latched (the waker observes it True
#     while the task is WAITING — a started task never becomes stealable,
#     spec §19.2 / ADR-006).
#   - WAKE ROUTING: the waker's unpark_current claims the generation once
#     and pushes the record onto the OWNER's remote-ready queue — the waker
#     itself enqueues NOTHING locally (its pending() stays 0; the record is
#     never a steal candidate, never injection).
#   - GENERATION: exactly one WAITING commit bump (generation 1 -> 2);
#     duplicate + stale wakes are no-ops (A0-T12, asserted via the owner's
#     skipped() == 0 and the final quiet queues).
#
# Choreography: the task starts on worker 0 (owner, worker_id=1), parks
# two-phase; worker 1 (the waker) delivers the wake; worker 0 resumes it to
# COMPLETED.  Every body entry records the executing worker id (stamped by
# the dispatcher right before the body) and the driver asserts all entries
# equal the owner.  Raw pthread harness (t33 pattern; AOT only —
# modular/modular#6971).
#
# Verdict: exit 0 + "PASS"; any failure prints FAIL + _iso_exit(1).
#
# BUILD REQUIREMENT (H4-partial/M10, PR #109): this driver MUST be built at
# `mojo build -O 0`.  Its cross-thread handshake cells (phase, parked_*,
# body_entries, running_worker) are PLAIN Ints published with release/
# acquire fences; a higher optimization level can hoist the plain handshake
# reads and deadlock or desynchronize the driver.  mojito_async/test/run.sh
# carries the -O 0 build flag for this driver.
from std.atomic import Atomic, fence, Ordering
from std.time import sleep
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.park import (
    park_commit,
    park_prepare,
    park_validate,
    unpark_current,
)
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import Worker, make_worker
from mojito_async.task import JoinHandle, claim_running
from mojito_async.vendor.mojito_sys import c_free, c_malloc, entry_pointer


@extern("pthread_create")
def _pthread_create(
    thread: UnsafePointer[Int, MutAnyOrigin],
    attr: Int,
    start_routine: UnsafePointer[Byte, MutAnyOrigin],
    arg: BytePtr,
) abi("C") -> Int32: ...

@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]
comptime TCB_STRIDE = Int(256)

comptime PH_RUN = Int(0)
comptime PH_PARKED = Int(1)    # parker committed WAITING
comptime PH_WOKE = Int(2)      # waker delivered (claims + dup + stale done)
comptime PH_DONE = Int(3)
comptime PH_ERR = Int(4)

comptime W0_ID = Int(1)  # owner worker identity (driver-threaded)
comptime W1_ID = Int(2)  # waker worker identity


def _fail(failures: UnsafePointer[Int, MutAnyOrigin], msg: String):
    failures[0] = failures[0] + 1
    print("  - " + msg)


def set_phase(sc: UnsafePointer[Int, MutAnyOrigin], v: Int):
    sc[0] = v
    sc[1] = v


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var w0: UnsafePointer[Worker, MutAnyOrigin]
    var w1: UnsafePointer[Worker, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]       # 2-cell array
    var failures: UnsafePointer[Int, MutAnyOrigin]
    var thread_err: UnsafePointer[Int, MutAnyOrigin]  # 2-cell array
    var progress: UnsafePointer[Atomic[DType.int64], MutAnyOrigin]
    var parked_tcb: UnsafePointer[Int, MutAnyOrigin]
    var parked_id: UnsafePointer[Int, MutAnyOrigin]
    var parked_gen: UnsafePointer[Int, MutAnyOrigin]
    var body_entries: UnsafePointer[Int, MutAnyOrigin]
    var running_worker: UnsafePointer[Int, MutAnyOrigin]  # stamped pre-body
    var owner_seen: UnsafePointer[Int, MutAnyOrigin]      # waker's reads
    var started_latched: UnsafePointer[Int, MutAnyOrigin]
    var w1_observed: UnsafePointer[Int, MutAnyOrigin]
    var w1_pending: UnsafePointer[Int, MutAnyOrigin]      # waker's pending

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.w1 = self.w0
        self.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.failures = self.phase
        self.thread_err = self.phase
        self.progress = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
            unsafe_from_address=1
        )
        self.parked_tcb = self.phase
        self.parked_id = self.phase
        self.parked_gen = self.phase
        self.body_entries = self.phase
        self.running_worker = self.phase
        self.owner_seen = self.phase
        self.started_latched = self.phase
        self.w1_observed = self.phase
        self.w1_pending = self.phase


def parked_body(ud: BytePtr) raises -> IntResult:
    """Every execution asserts the executing worker IS the owner (stamped
    by the dispatcher just before this runs).  An off-owner run fails the
    driver immediately — no started fiber may ever migrate (issue #71)."""
    var sc = ud.bitcast[Scene]()
    sc[].body_entries[] += 1
    if sc[].running_worker[] != W0_ID:
        _fail(sc[].failures, "AFFINITY: body executed on worker "
              + String(sc[].running_worker[]) + " (owner is " + String(W0_ID)
              + ") — a started fiber migrated!")
    return IntResult(7)


def dispatch_w0(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Worker 0's dispatcher (driven by the REAL scheduler_loop, which
    stamps owner at the first run): slice 1 runs the body + parks; slice 2
    (the forced cross-worker wake resume) re-asserts the immutable owner
    and completes."""
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    if not h.tcb()[].is_started():
        # FIRST dispatch: the loop stamped owner before calling us.
        if h.tcb()[].owner_worker() != W0_ID:
            _fail(sc[].failures, "owner_worker not stamped at first run")
        claim_running(h)
        sc[].running_worker[] = W0_ID
        _ = parked_body(ud)
        park_prepare(h)
        var rdy = park_validate(h)
        if rdy:
            _fail(sc[].failures, "VALIDATE READY with no wake delivered")
        park_commit(h, 3)
        if h.state() != TaskControlBlock.WAITING:
            _fail(sc[].failures, "park_commit did not reach WAITING")
        sc[].parked_tcb[] = tcb_addr
        sc[].parked_id[] = tid
        sc[].parked_gen[] = h.tcb()[].generation()
        set_phase(sc[].phase, PH_PARKED)
        return 1
    # RESUME (post-wake): the owner must be immutable and THIS must be the
    # owner (the off-owner assert in scheduler_loop + the routing keep it
    # so; the body re-asserts the executing worker).
    if h.tcb()[].owner_worker() != W0_ID:
        _fail(sc[].failures, "owner_worker CHANGED after first run ("
              + String(h.tcb()[].owner_worker()) + ") — immutable violated")
    claim_running(h)
    sc[].running_worker[] = W0_ID
    _ = parked_body(ud)
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    h.tcb()[].mark_result(IntResult(7))
    set_phase(sc[].phase, PH_DONE)
    return 1


def serve_worker0(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rt = sc.w0[].runtime()
    var spins = 0
    while True:
        _ = sc.progress[0].fetch_add[ordering=Ordering.RELAXED](Int64(1))
        var ph = sc.phase[0]
        if ph == PH_DONE or ph == PH_ERR:
            return
        _ = scheduler_loop(rt[], dispatch_w0, scp.bitcast[Byte](), worker_id=W0_ID)
        fence[Ordering.RELEASE]()
        spins += 1
        if spins > 2000000:
            _fail(sc.failures, "w0: serve loop did not quiesce")
            set_phase(sc.phase, PH_ERR)
            return
        sleep(0.0001)


def serve_worker1(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """The foreign waker: observes the WAITING park, audits the owner +
    STARTED latch, delivers the wake through ITS OWN Runtime (claims the
    generation), runs the duplicate + stale probes, then waits for DONE."""
    var sc = scp[]
    var spins = 0
    while True:
        _ = sc.progress[1].fetch_add[ordering=Ordering.RELAXED](Int64(1))
        var ph = sc.phase[1]
        if ph == PH_ERR or ph == PH_DONE:
            return
        if ph == PH_PARKED and sc.w1_observed[] == 0:
            var h = JoinHandle[IntResult](
                UnsafePointer[TB, MutAnyOrigin](
                    unsafe_from_address=sc.parked_tcb[]
                ),
                sc.parked_id[],
            )
            if h.state() != TaskControlBlock.WAITING:
                _fail(sc.failures, "waker: parker not WAITING")
                set_phase(sc.phase, PH_ERR)
                return
            # owner audit: two reads, stable, == the owner id; and the
            # STARTED latch must be set (a started task is never stealable).
            var ow1 = h.tcb()[].owner_worker()
            var ow2 = h.tcb()[].owner_worker()
            if ow1 != ow2 or ow1 != W0_ID:
                _fail(sc.failures, "owner_worker unstable across reads ("
                      + String(ow1) + "," + String(ow2) + ")")
            sc.owner_seen[] = ow1
            if not h.tcb()[].is_started():
                _fail(sc.failures, "STARTED latch not set on a running task")
            sc.started_latched[] = 1
            sc.w1_observed[] = 1
            # THE FORCED CROSS-WORKER WAKE (waker's own Runtime): must claim
            # the generation once and route to the OWNER's remote queue.
            unpark_current(sc.w1[].runtime()[], h, required_gen=sc.parked_gen[])
            # duplicate wake of the same epoch: no-op (already RUNNABLE).
            unpark_current(sc.w1[].runtime()[], h, required_gen=sc.parked_gen[])
            # stale wake (previous epoch): rejected, nothing enqueued.
            unpark_current(
                sc.w1[].runtime()[], h, required_gen=sc.parked_gen[] - 1
            )
            sc.w1_pending[] = sc.w1[].runtime()[].pending()
            set_phase(sc.phase, PH_WOKE)
            fence[Ordering.RELEASE]()
        fence[Ordering.RELEASE]()
        spins += 1
        if spins > 2000000:
            _fail(sc.failures, "w1: never observed the parked task")
            set_phase(sc.phase, PH_ERR)
            return
        sleep(0.0001)


@export("t34b_worker0")
def t34b_worker0(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker0(sc)
    except e:
        sc[].thread_err[0] = 1
        set_phase(sc[].phase, PH_ERR)
    _park_forever(sc, 0)


@export("t34b_worker1")
def t34b_worker1(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker1(sc)
    except e:
        sc[].thread_err[1] = 1
        set_phase(sc[].phase, PH_ERR)
    _park_forever(sc, 1)


def _park_forever(sc: UnsafePointer[Scene, MutAnyOrigin], me: Int) -> None:
    while True:
        _ = sc[].progress[me].fetch_add[ordering=Ordering.RELAXED](Int64(1))


def run_scenario(failures: UnsafePointer[Int, MutAnyOrigin]) raises:
    var w0 = make_worker()
    var w1 = make_worker()
    var w0p = UnsafePointer[Worker, MutAnyOrigin](to=w0)
    var w1p = UnsafePointer[Worker, MutAnyOrigin](to=w1)

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].w0 = w0p
    sc[].w1 = w1p
    var cells = Int(c_malloc(16 * 8 + 2 * 64))
    var p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    for i in range(16):
        p[i] = 0
    sc[].phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 0)
    sc[].failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 2 * 8)
    sc[].thread_err = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 3 * 8
    )
    sc[].parked_tcb = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 5 * 8
    )
    sc[].parked_id = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 6 * 8
    )
    sc[].parked_gen = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 7 * 8
    )
    sc[].body_entries = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 8 * 8
    )
    sc[].running_worker = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 9 * 8
    )
    sc[].owner_seen = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 10 * 8
    )
    sc[].started_latched = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 11 * 8
    )
    sc[].w1_observed = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 12 * 8
    )
    sc[].w1_pending = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 13 * 8
    )
    sc[].progress = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
        unsafe_from_address=cells + 16 * 8
    )
    set_phase(sc[].phase, PH_RUN)

    var tcb = UnsafePointer[TB, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(TCB_STRIDE))
    )
    tcb[0] = TB.create()
    tcb[].transition(TaskControlBlock.RUNNABLE)
    var w0rt = w0p[].runtime()
    var tid = w0rt[].next_id()
    w0rt[].enqueue_local(Int(tcb), tid)

    var scp = sc
    var arg = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(scp))
    var t0 = Int(0)
    var t1 = Int(0)
    var t0p = UnsafePointer[Int, MutAnyOrigin](to=t0)
    var t1p = UnsafePointer[Int, MutAnyOrigin](to=t1)
    var rc0 = _pthread_create(t0p, 0, entry_pointer["t34b_worker0"](), arg)
    var rc1 = _pthread_create(t1p, 0, entry_pointer["t34b_worker1"](), arg)
    if rc0 != 0 or rc1 != 0:
        _fail(failures, "pthread_create failed (" + String(rc0) + ", "
                        + String(rc1) + ")")
        c_free(scp.bitcast[Byte]())
        c_free(tcb.bitcast[Byte]())
        return
    var budget = 600_000_000
    while budget > 0:
        var s0 = sc[].phase[budget & 1]
        var s1 = sc[].phase[1 - (budget & 1)]
        if s0 == PH_DONE or s0 == PH_ERR or s1 == PH_ERR:
            break
        budget -= 1
    fence[Ordering.ACQUIRE]()
    if budget == 0:
        _fail(failures, "workers did not quiesce (phase "
                        + String(sc[].phase[0]) + " err "
                        + String(sc[].thread_err[0]) + ","
                        + String(sc[].thread_err[1]) + ")")

    if failures[0] == 0:
        if sc[].thread_err[0] != 0 or sc[].thread_err[1] != 0:
            _fail(failures, "a worker thread raised in its serve loop")
        if sc[].owner_seen[] != W0_ID:
            _fail(failures, "waker observed owner_worker "
                  + String(sc[].owner_seen[]) + " (want " + String(W0_ID) + ")")
        if sc[].started_latched[] != 1:
            _fail(failures, "waker did not observe the STARTED latch")
        if sc[].body_entries[] != 2:
            _fail(failures, "body ran " + String(sc[].body_entries[])
                  + " times (want exactly 2: park + owner resume)")
        if not tcb[].is_completed():
            _fail(failures, "parker not COMPLETED")
        if tcb[].owner_worker() != W0_ID:
            _fail(failures, "owner_worker = " + String(tcb[].owner_worker())
                  + " (want " + String(W0_ID) + ", immutable)")
        if tcb[].generation() != 2:
            _fail(failures, "generation = " + String(tcb[].generation())
                  + " (want 2: one WAITING bump)")
        if sc[].w1_pending[] != 0:
            _fail(failures, "waker enqueued the wake locally (pending "
                  + String(sc[].w1_pending[])
                  + " — the wake must route to the OWNER remote queue)")
        var w0pending = w0p[].runtime()[].pending()
        if w0pending != 0:
            _fail(failures, "w0 not quiet after the resume (pending "
                  + String(w0pending) + ")")
        var skipped = w0p[].runtime()[].skipped()
        if skipped != 0:
            _fail(failures, String(skipped)
                  + " stale records skipped (dup/stale wakes must enqueue "
                  + "NOTHING)")

    c_free(scp.bitcast[Byte]())
    c_free(tcb.bitcast[Byte]())


def main() raises:
    var failures = Int(0)
    var failuresp = UnsafePointer[Int, MutAnyOrigin](to=failures)
    run_scenario(failuresp)
    if failures == 0:
        print("T34b affinity (issue #71): PASS")
        _iso_exit(0)
    print("T34b affinity (issue #71): FAIL (" + String(failures) + ")")
    _iso_exit(1)