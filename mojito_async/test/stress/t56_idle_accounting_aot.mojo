# mojito_async/test/stress/t56_idle_accounting_aot.mojo
#
# RED driver for issue #150 — idle accounting never completes a unit, so
# parking is structurally dead and pooled workers busy-poll.
#
# `runtime/runtime.mojo:331-343` — `_announce_and_wake` fires
# `announce_work(acct, 1)` on EVERY `enqueue_local` / `push_remote` /
# `enqueue_global`, including every `yield_now` re-enqueue.  Grep the tree
# for the counterpart: no caller of `complete_work` exists on the scheduled
# path at all.  The only callers are the A2.1 seam-unit drain in
# thread_entry.mojo and the manual `WorkerPool.complete_work`, and neither
# runs inside `fair_scheduler_loop` or `pool_worker_loop_scheduled`.
#
# `runtime/idle.mojo:47-48` documents the exact failure it now has:
#
#     NOT   announce without a matching drain: the pending counter leaks and
#           the workers' pre-park re-check finds phantom work forever.
#
# and `thread_entry.mojo:585-591` is that pre-park re-check:
#
#     var have_work = _idle_pending(acctp) > 0
#     if have_work:
#         ...
#         sleep(0.0002)
#         continue                 # <- skips the OS park entirely
#
# 0.0002s is 5000 iterations a second, per worker, on a pool with nothing to
# do.  Because the park branch is never reached, `idle_parks` is never bumped
# and `park_total` never moves, so the E6/#72 invariant that workers park
# rather than spin is dead in the real pool — and `idle_parked` stays 0, so
# `wake_one` never signals and `spurious_total`/`wake_total` are meaningless.
#
# THE SCENE.  A real 2-worker `WorkerPool` driving
# `pool_worker_loop_scheduled` — the #112 seam that runs
# `fair_scheduler_loop` on actual pool threads — seeded with a batch of
# tasks, drained to completion, then left completely idle for a beat.
#
# ORACLES, all exact counters rather than timings:
#
#   1. `pending_work()` must be 0 once every seeded task has completed and
#      the queues are quiet.  Announce and complete are a documented MUST
#      pair; this is that pair's own invariant.
#   2. `park_total()` must be > 0 after an idle beat, because a worker with
#      no work is supposed to park.
#   3. every worker's `idle_parks(i)` must be > 0, read post-join from the
#      entry cell — the per-worker split of the same event.
#
# Process CPU time across the idle window is reported as telemetry, not
# gated: it is the visible cost, but it is a timing measurement and this
# lane's verdict should not rest on one.
#
# NOT COVERED, deliberately.  The issue's second half — one shared
# breadth-one `NativeEvent` for the whole pool while remote-queue work is
# owner-affine, so a wake for a task owned by A is consumed by parked worker
# B while A sleeps to its ~2s backstop — cannot be observed today, because
# workers never park at all.  The issue says so itself: "Today this is masked
# because workers never park. Fixing the accounting exposes it." So there is
# nothing here to make red yet, and I have not pretended otherwise.
#
# BUILD LEVEL: `-O 0`, matching t47_pool_scheduler_aot, which hits the
# default-optimization compiler crash once the full WorkerPool dependency
# graph is compiled alongside a driver.
#
# Verdict: exit 0 + "PASS"; any failure prints RED and exits 1.
# AOT-only (thread_entry embeds the pool trampoline + externs;
# modular/modular#6971).
from std.atomic import Atomic, Ordering
from std.time import sleep
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.config import make_pool_config
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.thread_entry import WorkerEntryCell, pool_worker_loop_scheduled
from mojito_async.runtime.worker_pool import WorkerPool, make_pool
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.vendor.mojito_sys import c_malloc, entry_pointer


@extern("clock")
def _c_clock() abi("C") -> Int: ...


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]
comptime TCB_STRIDE = Int(256)

comptime N_WORKERS = Int(2)
comptime N_TASKS = Int(60)
comptime SPIN_BUDGET = Int(20000)      # bounded wait: ~10s at 0.5ms/iter
comptime IDLE_BEAT_S = Float64(0.5)    # how long the pool is left idle
comptime CLOCKS_PER_SEC = Int(1000000) # macOS/BSD and glibc both use 1e6


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var completed: UnsafePointer[Atomic[DType.int64], MutAnyOrigin]

    def __init__(out self):
        self.completed = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
            unsafe_from_address=1
        )


def t56_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    _ = sc[].completed[0].fetch_add(1)
    return 1


def t56_service(mut rt: Runtime, ud: BytePtr) raises:
    return


@export("mjs_pool_entry")
def mjs_pool_entry(ud: BytePtr) abi("C"):
    try:
        pool_worker_loop_scheduled[R=IntResult](ud, t56_dispatch, t56_service, 4)
    except e:
        ud.bitcast[WorkerEntryCell]()[].loop_ok = False


def _seed(mut pool: WorkerPool, worker_idx: Int, n: Int) raises:
    var rt = pool.worker_at(worker_idx)[].runtime()
    for k in range(n):
        var tcb = UnsafePointer[TB, MutAnyOrigin](
            unsafe_from_address=Int(c_malloc(TCB_STRIDE))
        )
        tcb[0] = TB.create()
        _ = spawn(rt[], tcb, k)


def main() raises:
    var failures = List[String]()

    var sc = Scene()
    sc.completed = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(64))
    )
    sc.completed[0] = Atomic[DType.int64](0)
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)

    var pool = make_pool(make_pool_config(N_WORKERS))
    pool.start(entry_pointer["mjs_pool_entry"]())
    for i in range(N_WORKERS):
        pool.entry_at(i)[].dispatch_ud = scp.bitcast[Byte]()

    _seed(pool, 0, N_TASKS // 2)
    _seed(pool, 1, N_TASKS - N_TASKS // 2)

    pool.spawn_all_workers(entry_pointer["mjs_pool_entry"]())

    var spins = 0
    while Int(sc.completed[0].load()) < N_TASKS:
        spins += 1
        if spins > SPIN_BUDGET:
            failures.append("timed out: " + String(Int(sc.completed[0].load()))
                            + "/" + String(N_TASKS) + " tasks completed")
            break
        sleep(0.0005)

    var announced_after_drain = pool.pending_work()

    # --- the idle beat: nothing left to do, nothing being produced --------
    var cpu0 = _c_clock()
    var park0 = pool.park_total()
    sleep(IDLE_BEAT_S)
    var park1 = pool.park_total()
    var cpu1 = _c_clock()
    var parked_now = pool.idle_parked()
    var pending_now = pool.pending_work()

    pool.request_shutdown()
    pool.join_all()

    var total_idle_parks = 0
    for i in range(N_WORKERS):
        total_idle_parks += pool.idle_parks(i)

    var cpu_us = cpu1 - cpu0
    var idle_us = Int(IDLE_BEAT_S * Float64(CLOCKS_PER_SEC))
    var cpu_pct_of_one_core = 0
    if idle_us > 0:
        cpu_pct_of_one_core = (cpu_us * 100) // idle_us

    print("T56 idle accounting: tasks=" + String(Int(sc.completed[0].load()))
          + "/" + String(N_TASKS)
          + " pending_work(after drain)=" + String(announced_after_drain)
          + " pending_work(after idle beat)=" + String(pending_now)
          + " park_total=" + String(park0) + "->" + String(park1)
          + " idle_parked=" + String(parked_now)
          + " idle_parks(sum)=" + String(total_idle_parks))
    print("  telemetry (not gated): " + String(cpu_pct_of_one_core)
          + "% of one core burned across a " + String(IDLE_BEAT_S)
          + "s window in which the pool had nothing to do")

    if pending_now != 0:
        failures.append(
            "PENDING LEAK — pending_work() is " + String(pending_now)
            + " on a pool that has drained every task and is idle."
            + " _announce_and_wake announces one unit on every enqueue and"
            + " nothing on the scheduled path ever calls complete_work, so"
            + " the counter only grows. idle.mojo:47-48 names the"
            + " consequence: 'the pending counter leaks and the workers'"
            + " pre-park re-check finds phantom work forever.'"
        )

    if park1 <= park0:
        failures.append(
            "NO PARKS — park_total did not move across a "
            + String(IDLE_BEAT_S) + "s idle window (" + String(park0)
            + " -> " + String(park1) + "). The pre-park re-check at"
            + " thread_entry.mojo:585-591 sees phantom pending work and takes"
            + " the sleep(0.0002) branch, which is 5000 iterations a second"
            + " per worker instead of an OS park."
        )

    if total_idle_parks == 0:
        failures.append(
            "NO PER-WORKER PARKS — every worker's idle_parks() is 0 after"
            + " join, so no worker ever reached the OS park at all. The"
            + " E6/#72 invariant that workers park rather than spin is dead"
            + " in the real pool, and the 'no busy-spin' proof counter proves"
            + " nothing."
        )

    if len(failures) == 0:
        print("T56 idle accounting: PASS")
    else:
        print("T56 idle accounting: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)
