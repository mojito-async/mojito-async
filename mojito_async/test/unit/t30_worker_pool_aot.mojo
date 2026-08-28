# mojito_async/test/unit/t30_worker_pool_aot.mojo
#
# A2.1 (issue #67) — WORKER POOL OVER NativeThread: acceptance driver.
#
# Proves the issue #67 exit criteria end to end on a REAL pool:
#   - run() with N>1 workers starts N native threads and all runnable work
#     completes: start a 4-worker pool, seed K=400 seam tasks (the E2-OWNED
#     acceptance surface of this lane), wait until every task recorded its
#     current_worker TLS observation, then shutdown+join;
#   - run() with default worker_count uses cpu_logical_count(): the default
#     RuntimeConfig asserts worker_count == cpu_logical_count(), and a
#     pool of 1 (A1 single-worker parity at N=1) runs, shuts down, joins;
#   - request_shutdown() observed by every worker loop, join_all() returns,
#     no thread leaks: per-worker `exited` flags (the trampoline only sets
#     them after the loop saw the latch), threads_joined() == worker_count
#     for both pools, and the process exits having reaped every thread;
#   - each worker reports a distinct id and its own Runtime: per-worker id()
#     getter is i and each worker's Runtime cell is at a distinct address;
#   - current_worker TLS reads back its own worker id at entry: entry_ok
#     (the trampoline round-trips the TLS slot immediately after the entry
#     write) plus every seam task observed ONE stable worker id (task j
#     recorded the id of the worker that ran it — obs slice [i*PER,..)
#     all equal i — proving a task never sees two ids mid-stream).
#
# The pool threads run a per-unit current_worker read (thread_entry.seam_
# run_unit); the RESULT SLICES are the lane's observable "K tasks".
#
# EMBEDDING RULE (AOT): this driver declares the @export("mjs_pool_entry")
# trampoline (thread_entry.mjs_pool_entry_main is the shared body) and feeds
# entry_pointer["mjs_pool_entry"]() into pool.start().  THE POOL SPAWNS:
# pool.start(entry) arms the pool (validate + TLS keys + entry cells +
# latch) and pool.spawn_all_workers(entry) spawns every worker AND performs
# the mark_started + TLS-key wiring inside the pool (H3 fold, PR #104) — the
# old footgun path (a driver-side spawn_native_thread loop calling
# wptr[].mark_started by hand) is GONE, and this driver asserts the wiring
# happened: immediately after spawn_all_workers returns, every worker is
# already started() — mark_started completed BEFORE pthread_create returned,
# so no thread can run user code before its Worker/TLS wiring is visible
# (happens-before via pthread_create).
#
# FOLD GUARDS (M5/M6, PR #104), proven here on the real pool:
#   - RuntimeConfig.validate() runs FIRST in start(): degenerate
#     worker_count (0 and above the 1024 sane bound) raises loudly, as does
#     stack_initial_commit_bytes > stack_reserve_bytes;
#   - start() after finalize() raises loudly (no address-1 sentinel reuse).
#
# AOT only (modular/modular#6971: the JIT cannot materialize @export symbols
# from imported modules; the run.sh _aot glob drives this).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.time import sleep
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.config import make_pool_config
from mojito_async.runtime.thread_entry import mjs_pool_entry_main
from mojito_async.runtime.worker_pool import WorkerPool, make_pool
from mojito_async.vendor.mojito_sys import (
    cpu_logical_count,
    entry_pointer,
    c_free,
    c_malloc,
)


@export("mjs_pool_entry")
def mjs_pool_entry(ud: BytePtr) abi("C"):
    """Embedding-binary trampoline symbol (see thread_entry.mojo's EMBEDDING
    RULE): forwards to the shared entry body."""
    mjs_pool_entry_main(ud)


comptime POOL_N = Int(4)
comptime PER = Int(100)  # seam tasks per worker
comptime K = POOL_N * PER  # total tasks


def red(what: String) raises -> None:
    print("T30 worker pool: RED (" + what + ")")
    raise Error(what)


def wait_drained(mut pool: WorkerPool, what: String) raises:
    var spins = 0
    while not pool.poll_done():
        sleep(0.001)
        spins += 1
        if spins > 30000:
            red(what + ": pool did not drain in 30s (spins=" + String(spins) + ")")


def expect_start_raise(mut pool: WorkerPool, entry: BytePtr, what: String) raises:
    """M5/M6 fold guard: pool.start(entry) MUST raise (validate / finalize);
    anything else is a RED.  Always finalize()s the pool afterwards so the
    negative pools release their heap."""
    var threw = False
    try:
        pool.start(entry)
    except:
        threw = True
    if not threw:
        red(what)
    pool.finalize()


def assert_pool_audit(mut pool: WorkerPool, n: Int, per: Int) raises:
    """Post-join audit of every worker: distinct ids, own Runtime, TLS entry
    round-trip, stable units, loop completion, latch observation (exited),
    and the exact per-worker observation count."""
    var rt_first = Int(pool.worker_at(0)[].handle())
    for i in range(n):
        if pool.worker_at(i)[].id() != i:
            red(
                "worker " + String(i) + " reports id "
                + String(pool.worker_at(i)[].id()) + " (want " + String(i) + ")"
            )
        if i > 0 and Int(pool.worker_at(i)[].handle()) == rt_first:
            red("worker " + String(i) + " shares one Runtime with worker 0")
        if not pool.entry_ok(i):
            red("worker " + String(i) + ": current_worker TLS did not round-trip at entry")
        if not pool.unit_ok(i):
            red("worker " + String(i) + ": a seam task saw an unstable current_worker")
        if not pool.loop_ok(i):
            red("worker " + String(i) + ": worker loop did not complete cleanly")
        if not pool.exited(i):
            red("worker " + String(i) + " never observed request_shutdown (leak)")
        if pool.obs_done(i) != per:
            red(
                "worker " + String(i) + " ran " + String(pool.obs_done(i))
                + " tasks (want " + String(per) + ")"
            )
    if pool.threads_joined() != n:
        red(
            "threads_joined() == " + String(pool.threads_joined())
            + " (want " + String(n) + ")"
        )


def main() raises:
    # ---- 1. config defaults (spec §89 + cpu_logical_count) ----------------
    var cfg = make_pool_config()
    var want_cpu = cpu_logical_count()
    if cfg.worker_count != want_cpu:
        red(
            "default worker_count = " + String(cfg.worker_count)
            + " (want cpu_logical_count() = " + String(want_cpu) + ")"
        )
    if cfg.stack_reserve_bytes != 1048576:
        red("default stack_reserve_bytes = " + String(cfg.stack_reserve_bytes))
    if cfg.stack_initial_commit_bytes != 0:
        red("default stack_initial_commit_bytes != 0")
    if cfg.enable_tracing:
        red("default enable_tracing must be false")

    # ---- 2. N=4 pool: K tasks, all ids appear, stable per task ------------
    var entry = entry_pointer["mjs_pool_entry"]()
    var obs = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(K * 8))
    )
    var pool = make_pool(make_pool_config(POOL_N))
    pool.seed_seam_units(PER, obs, K)
    pool.start(entry)
    pool.spawn_all_workers(entry)

    # H3 fold guard: spawn_all_workers completed mark_started for EVERY
    # worker BEFORE any thread could run user code (happens-before via
    # pthread_create).  The old footgun — a driver-side spawn_native_thread
    # loop calling wptr[].mark_started(t, key) by hand — no longer exists:
    # this driver never references spawn_native_thread or mark_started, and
    # the getters below prove the POOL did the wiring.
    for i in range(POOL_N):
        if not pool.worker_at(i)[].started():
            red(
                "worker " + String(i)
                + " not marked started by spawn_all_workers (wiring footgun)"
            )
    wait_drained(pool, "4-worker pool")

    # per-worker slices: task j must record worker j/PER exactly (one stable
    # worker id per task; all POOL_N ids appear since every worker seeded).
    for j in range(K):
        var v = obs[j]
        var want = j // PER
        if v != want:
            red(
                "task " + String(j) + " observed worker " + String(v)
                + " (want " + String(want) + ") - id not stable per task"
            )

    pool.request_shutdown()
    pool.join_all()
    assert_pool_audit(pool, POOL_N, PER)
    pool.finalize()
    c_free(obs.bitcast[Byte]())

    # ---- 2.5 fold guards (M5/M6, PR #104) ---------------------------------
    # A finalized pool must refuse start() LOUDLY — never re-arm through the
    # address-1 sentinel latch into freed heap.
    expect_start_raise(pool, entry, "start() after finalize() must raise")
    # Degenerate configs must raise in start() via RuntimeConfig.validate():
    # worker_count below 1, worker_count above the sane bound, and
    # stack_initial_commit_bytes above stack_reserve_bytes.
    var bad0 = make_pool(make_pool_config(0))
    expect_start_raise(bad0, entry, "worker_count=0 must raise in start()")
    var badbig = make_pool(make_pool_config(1025))
    expect_start_raise(
        badbig, entry, "worker_count above the sane bound must raise in start()"
    )
    var badcommit = make_pool(make_pool_config(1, 1048576, 2097152))
    expect_start_raise(
        badcommit, entry, "commit > reserve must raise in start()"
    )

    # ---- 3. N=1 pool (A1 single-worker parity at count 1) ------------------
    comptime ONE_PER = Int(5)
    var obs1 = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(ONE_PER * 8))
    )
    var pool1 = make_pool(make_pool_config(1))
    pool1.seed_seam_units(ONE_PER, obs1, ONE_PER)
    pool1.start(entry)
    pool1.spawn_all_workers(entry)
    if not pool1.worker_at(0)[].started():
        red("N=1 worker not marked started by spawn_all_workers (wiring footgun)")
    wait_drained(pool1, "1-worker pool")
    for j in range(ONE_PER):
        if obs1[j] != 0:
            red(
                "N=1 pool task " + String(j) + " observed worker "
                + String(obs1[j])
            )
    pool1.request_shutdown()
    pool1.join_all()
    assert_pool_audit(pool1, 1, ONE_PER)
    pool1.finalize()
    c_free(obs1.bitcast[Byte]())

    print("T30 worker pool: PASS")