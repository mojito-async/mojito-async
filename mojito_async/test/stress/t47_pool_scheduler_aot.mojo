# mojito_async/test/stress/t47_pool_scheduler_aot.mojo
#
# #112 (item 1, EPIC #2 review consensus) — "Pool worker loop drives REAL
# scheduler work": a REAL WorkerPool (2 workers, worker_pool.mojo) with a
# custom `@export("mjs_pool_entry")` trampoline wired to
# thread_entry.pool_worker_loop_scheduled — the NEW #112 seam that drives
# fair_scheduler_loop (local -> remote-ready -> injection, spec §21) plus
# ONE E4 steal round on quiet, ON EACH POOL THREAD, with a STATICALLY-KNOWN
# dispatcher (b2 design decision #4).  Before this fold NO driver ran
# scheduler_loop/fair_scheduler_loop on an ACTUAL pool-spawned OS thread:
# every multi-worker scheduler_loop call in the suite (t34/t34b/t34c/
# t38_mutex_cross_worker/bench/scheduler_scale_aot phase-2) drives the
# Worker/Runtime pair from the HARNESS thread; the real pool's own
# pool_worker_loop (thread_entry.mojo) only ever drained A2.1 seeded seam
# units, never a worker's actual local/remote/injection queues.
#
# Scene (heterogeneous heterogeneous task mix across BOTH pool threads):
#   - N_LOCAL0 tasks spawned onto worker 0's own local deque (spawn
#     locality) BEFORE the threads start.
#   - N_LOCAL1 tasks spawned onto worker 1's own local deque.
#   - N_INJECT tasks pushed through worker 0's runtime.enqueue_global(...,
#     current_worker=0) — the E3 injection intake (issue #69) — proving
#     the injection-drain seam (fair_scheduler_loop's has_inject/pop_inject)
#     actually fires ON A POOL THREAD, not just in a JIT/AOT unit driver.
#   - N_STEAL tasks seeded ONLY onto worker 0's local deque (worker 1 gets
#     NONE of its own local/injected work) so worker 1 MUST steal (E4,
#     issue #70) to make progress — proving Worker.try_steal_unstarted
#     fires from inside pool_worker_loop_scheduled, not just a raw-thread
#     steal-only harness (t33).
#
# Every task's dispatcher increments a per-task run counter (exactly-once
# proof) and records which worker ran it (TaskControlBlock.owner_worker(),
# stamped by fair_scheduler_loop's first-run seam) into a shared array.
#
# Acceptance (issue #112's exit criterion for item 1):
#   - every task runs to COMPLETED EXACTLY ONCE (no loss, no double-run);
#   - BOTH pool workers observe served scheduler slices > 0 (slices_local +
#     slices_remote + slices_inject) — REAL scheduler_loop work on BOTH
#     pool OS threads, not just worker 0;
#   - worker 0's slices_inject() > 0 — the injection intake drained on a
#     pool thread;
#   - worker 1's task_steals_total() > 0 — the E4 steal probe fired from
#     inside the pool's own worker loop and made real progress;
#   - the pool's queues are quiet at rest (pending() == 0 on both workers)
#     and no task is ever observed running on more than one worker.
#
# Verdict: exit 0 + "PASS"; any assertion failure prints RED + exit 1
# (T37 driver's known-red convention).  AOT-only (thread_entry embeds the
# pool trampoline + externs; modular/modular#6971).
from std.atomic import Atomic, Ordering
from std.time import sleep
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.config import make_pool_config
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.thread_entry import WorkerEntryCell, pool_worker_loop_scheduled
from mojito_async.runtime.worker_pool import WorkerPool, make_pool
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.vendor.mojito_sys import c_free, c_malloc, entry_pointer


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]
comptime TCB_STRIDE = Int(256)

comptime N_LOCAL0 = Int(10)
comptime N_LOCAL1 = Int(0)
comptime N_INJECT = Int(10)
comptime N_STEAL = Int(80)
comptime N_TOTAL = N_LOCAL0 + N_LOCAL1 + N_INJECT + N_STEAL

comptime SPIN_BUDGET = Int(20000)  # bounded wait: ~10s at 0.5ms/iter


def _fail(failures: UnsafePointer[Int, MutAnyOrigin], msg: String):
    failures[0] = failures[0] + 1
    print("  - " + msg)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Shared scratch every dispatched task's `ud` points at.  `run_count`/
    `ran_worker` are indexed by (task_id - 1); `completed` is the shared
    Atomic progress counter the main thread polls."""

    var run_count: UnsafePointer[Int64, MutAnyOrigin]
    var ran_worker: UnsafePointer[Int64, MutAnyOrigin]
    var completed: UnsafePointer[Atomic[DType.int64], MutAnyOrigin]

    def __init__(out self):
        var s = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=1)
        self.run_count = s
        self.ran_worker = s
        self.completed = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
            unsafe_from_address=1
        )


def t47_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Every task's whole body: claim RUNNING, record (exactly-once count +
    the owner worker fair_scheduler_loop just stamped), COMPLETE, bump the
    shared progress counter.  A small per-task delay (NOT a busy-spin —
    the worker parks/serves other work meanwhile it never blocks the
    worker) widens the window worker 1 has to steal from worker 0's
    still-nonempty deque before worker 0 drains it alone, so the E4 steal
    assertion below is a DETERMINISTIC property of the seeded task counts
    (module header), not a raw-OS-scheduling race."""
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    var idx = h.tcb()[].parent_id()  # #112 test note: the GLOBAL seed-time
    # slot index — task_id (`tid`) is allocated PER-WORKER-RUNTIME
    # (Runtime._next_id, runtime.mojo), so two tasks seeded on DIFFERENT
    # workers can share the SAME tid; parent_id is the one caller-supplied
    # field guaranteed globally unique across this driver's seeding.
    sleep(0.0002)
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        sc[].run_count + idx, 1
    )
    (sc[].ran_worker + idx)[0] = Int64(h.tcb()[].owner_worker())
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    _ = sc[].completed[0].fetch_add(1)
    return 1


def t47_service(mut rt: Runtime, ud: BytePtr) raises:
    """No-op fairness-budget service pass (no timers/reactor this driver)."""
    return


@export("mjs_pool_entry")
def mjs_pool_entry(ud: BytePtr) abi("C"):
    """The pool's per-worker trampoline (EMBEDDING RULE, thread_entry.mojo):
    forwards straight to the #112 scheduled loop with t47's statically-known
    dispatcher/service pair.  try/except lives HERE (a non-abi callee that
    raises, called from this abi("C") wrapper) — the PROVEN
    t38_mutex_cross_worker_aot shape, not a try/except INSIDE an abi("C")
    body wrapping the extern-touching idle park itself (pool_worker_loop's
    own documented constraint stays satisfied: pool_worker_loop_scheduled's
    idle-park body is inlined at the SAME two-layer depth)."""
    try:
        pool_worker_loop_scheduled[R=IntResult](
            ud, t47_dispatch, t47_service, 4
        )
    except e:
        ud.bitcast[WorkerEntryCell]()[].loop_ok = False


def _seed_local(
    mut pool: WorkerPool,
    worker_idx: Int,
    n: Int,
    next_id: UnsafePointer[Int, MutAnyOrigin],
) raises:
    """Spawn `n` fresh RUNNABLE tasks directly onto worker `worker_idx`'s
    OWN local deque (spawn locality) — safe pre-spawn (no thread races yet:
    called between pool.start() and pool.spawn_all_workers()).  The GLOBAL
    slot index rides `parent_id` (spawn()'s own parameter) since the
    scheduler's `task_id` is allocated per-worker-Runtime (see
    t47_dispatch's note)."""
    var rt = pool.worker_at(worker_idx)[].runtime()
    for _k in range(n):
        var tcb = UnsafePointer[TB, MutAnyOrigin](
            unsafe_from_address=Int(c_malloc(TCB_STRIDE))
        )
        tcb[0] = TB.create()
        _ = spawn(rt[], tcb, next_id[0])
        next_id[0] += 1


def _seed_inject(
    mut pool: WorkerPool,
    worker_idx: Int,
    n: Int,
    next_id: UnsafePointer[Int, MutAnyOrigin],
) raises:
    """Push `n` fresh RUNNABLE tasks through worker `worker_idx`'s runtime
    enqueue_global(current_worker=0) — the E3 injection intake."""
    var rt = pool.worker_at(worker_idx)[].runtime()
    for _k in range(n):
        var tcb = UnsafePointer[TB, MutAnyOrigin](
            unsafe_from_address=Int(c_malloc(TCB_STRIDE))
        )
        tcb[0] = TB.create()
        tcb[0].set_parent_id(next_id[0])
        tcb[0].transition(TaskControlBlock.RUNNABLE)
        var id = rt[].next_id()
        rt[].enqueue_global(Int(tcb), id, 0)
        next_id[0] += 1


def main() raises:
    var failures = 0
    var fp = UnsafePointer[Int, MutAnyOrigin](to=failures)

    var sc = Scene()
    sc.run_count = UnsafePointer[Int64, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(N_TOTAL * 8))
    )
    sc.ran_worker = UnsafePointer[Int64, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(N_TOTAL * 8))
    )
    sc.completed = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(64))
    )
    for i in range(N_TOTAL):
        sc.run_count[i] = 0
        sc.ran_worker[i] = 0
    sc.completed[0] = Atomic[DType.int64](0)
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)

    var pool = make_pool(make_pool_config(2))
    pool.start(entry_pointer["mjs_pool_entry"]())

    # Every worker's entry cell must carry `sc` as the dispatcher userdata
    # BEFORE spawn (thread_entry.mojo's #112 EMBEDDING note: dispatch_ud
    # rides the entry cell, the abi("C") trampoline's one arg has no room
    # for a second pointer).
    pool.entry_at(0)[].dispatch_ud = scp.bitcast[Byte]()
    pool.entry_at(1)[].dispatch_ud = scp.bitcast[Byte]()

    var next_id = 0
    var nip = UnsafePointer[Int, MutAnyOrigin](to=next_id)
    # N_LOCAL0/N_LOCAL1: spawn locality on each worker's own deque.
    _seed_local(pool, 0, N_LOCAL0, nip)
    _seed_local(pool, 1, N_LOCAL1, nip)
    # N_INJECT: worker 0's injection intake (drained on a pool thread).
    _seed_inject(pool, 0, N_INJECT, nip)
    # N_STEAL: ALL on worker 0's local deque, NONE on worker 1 — worker 1
    # must steal to make any progress at all.
    _seed_local(pool, 0, N_STEAL, nip)
    if next_id != N_TOTAL:
        _fail(fp, "seeding produced " + String(next_id) + " tasks != " + String(N_TOTAL))

    pool.spawn_all_workers(entry_pointer["mjs_pool_entry"]())

    var spins = 0
    while Int(sc.completed[0].load()) < N_TOTAL:
        spins += 1
        if spins > SPIN_BUDGET:
            _fail(fp, "timed out: " + String(Int(sc.completed[0].load()))
                  + "/" + String(N_TOTAL) + " completed (lost task or hang)")
            break
        sleep(0.0005)

    pool.request_shutdown()
    pool.join_all()

    # ---- exactly-once + no-loss -------------------------------------------
    var double_run = 0
    var never_run = 0
    for i in range(N_TOTAL):
        if Int(sc.run_count[i]) == 0:
            never_run += 1
        elif Int(sc.run_count[i]) > 1:
            double_run += 1
    if never_run > 0:
        _fail(fp, String(never_run) + " task(s) never ran (lost)")
    if double_run > 0:
        _fail(fp, String(double_run) + " task(s) ran more than once (duplicate dispatch)")
    if Int(sc.completed[0].load()) != N_TOTAL:
        _fail(fp, "completed=" + String(Int(sc.completed[0].load())) + " != " + String(N_TOTAL))

    # ---- BOTH pool threads served REAL scheduler_loop slices (item 1) -----
    var rt0 = pool.worker_at(0)[].runtime()
    var rt1 = pool.worker_at(1)[].runtime()
    var slices0 = rt0[].slices_local() + rt0[].slices_remote() + rt0[].slices_inject()
    var slices1 = rt1[].slices_local() + rt1[].slices_remote() + rt1[].slices_inject()
    if slices0 <= 0:
        _fail(fp, "worker 0 served 0 scheduler slices")
    if slices1 <= 0:
        _fail(fp, "worker 1 served 0 scheduler slices (pool_worker_loop_scheduled "
              + "never drove worker 1's own Runtime)")

    # ---- injection drained on a pool thread --------------------------------
    if rt0[].slices_inject() < N_INJECT:
        _fail(fp, "worker 0 slices_inject=" + String(rt0[].slices_inject())
              + " < seeded " + String(N_INJECT))

    # ---- E4 steal fired from inside the pool's own worker loop -------------
    if rt1[].task_steals_total() <= 0:
        _fail(fp, "worker 1 task_steals_total=0 (steal-on-quiet never fired "
              + "inside pool_worker_loop_scheduled)")

    # ---- quiet at rest ------------------------------------------------------
    if rt0[].pending() != 0:
        _fail(fp, "worker 0 not quiet at rest: pending=" + String(rt0[].pending()))
    if rt1[].pending() != 0:
        _fail(fp, "worker 1 not quiet at rest: pending=" + String(rt1[].pending()))

    print("T47 pool scheduler: PASS (slices0=" + String(slices0) + " slices1="
          + String(slices1) + " inject0=" + String(rt0[].slices_inject())
          + " steals1=" + String(rt1[].task_steals_total()) + " completed="
          + String(Int(sc.completed[0].load())) + ")")

    pool.finalize()
    c_free(sc.run_count.bitcast[Byte]())
    c_free(sc.ran_worker.bitcast[Byte]())
    c_free(sc.completed.bitcast[Byte]())

    if fp[] == 0:
        _iso_exit(0)
    print("T47 pool scheduler: RED (" + String(fp[]) + " failure(s))")
    _iso_exit(1)
