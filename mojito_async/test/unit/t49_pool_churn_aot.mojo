# mojito_async/test/unit/t49_pool_churn_aot.mojo
#
# #112 (item 5, EPIC #2 review consensus) — "Pool lifecycle hardening": 50+
# create/start/spawn/run/shutdown/join/finalize cycles over FRESH WorkerPool
# instances must complete without EAGAIN (macOS PTHREAD_KEY_MAX exhaustion:
# each pool cycle creates 3 pthread TLS keys via start(); a leak would hit
# the ceiling around ~43 cycles on the documented macOS default), without a
# sentinel (address-1) dereference, and without a poison-recycle abort (a
# later cycle's fresh pool must run REAL seeded work correctly — a pool
# that "completes" while silently corrupting its own heap/TLS state is not
# a pass).
#
# Distinct from t30_worker_pool_aot (issue #67, ONE pool's lifecycle proven
# once) and from worker_pool.mojo's own re-arm test (start() after a
# previous start/join cycle on the SAME pool object): this driver churns
# MANY DISTINCT pool OBJECTS (finalize() -> the object is DONE, a fresh
# make_pool() is a NEW heap allocation + NEW TLS keys) — the scenario the
# issue's "50-pool churn" acceptance criterion names, and the TLS-key-leak
# class of bug only a genuinely fresh WorkerPool per cycle can surface (a
# single object's start()/finalize() re-arm path already deletes+recreates
# its OWN keys every cycle, which t30 does not repeat 50 times).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).  AOT-only
# (modular/modular#6971: @export symbols need the same binary).
from std.time import sleep
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.config import make_pool_config
from mojito_async.runtime.thread_entry import mjs_pool_entry_main
from mojito_async.runtime.worker_pool import WorkerPool, make_pool
from mojito_async.vendor.mojito_sys import entry_pointer, c_free, c_malloc


@export("mjs_pool_entry")
def mjs_pool_entry(ud: BytePtr) abi("C"):
    """Embedding-binary trampoline symbol (thread_entry.mojo EMBEDDING
    RULE): forwards to the shared entry body."""
    mjs_pool_entry_main(ud)


comptime N_POOLS = Int(60)   # > the documented ~43-cycle EAGAIN threshold
comptime POOL_N = Int(2)     # workers per cycle (keep each cycle cheap)
comptime PER = Int(20)       # seam tasks per worker per cycle
comptime K = POOL_N * PER    # total tasks per cycle


def red(what: String) raises -> None:
    print("T49 pool churn: RED (" + what + ")")
    raise Error(what)


def wait_drained(mut pool: WorkerPool, cycle: Int) raises:
    var spins = 0
    while not pool.poll_done():
        sleep(0.0005)
        spins += 1
        if spins > 20000:
            red("cycle " + String(cycle) + ": pool did not drain in 10s")


def run_one_cycle(cycle: Int) raises:
    """One FULL create -> seed -> start -> spawn -> drain -> shutdown ->
    join -> audit -> finalize lifecycle on a FRESH WorkerPool object.  Every
    heap allocation (worker/entry cells, latch, acct block, peers array)
    and every pthread TLS key this cycle creates MUST be released by
    finalize() before the NEXT cycle allocates its own — a leak on ANY of
    these surfaces as either an EAGAIN raise (TLS keys) or an unbounded
    process RSS climb (heap) that later cycles in the SAME run would
    eventually trip."""
    var entry = entry_pointer["mjs_pool_entry"]()
    var obs = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(K * 8))
    )
    var pool = make_pool(make_pool_config(POOL_N))
    pool.seed_seam_units(PER, obs, K)
    pool.start(entry)
    pool.spawn_all_workers(entry)
    for i in range(POOL_N):
        if not pool.worker_at(i)[].started():
            red("cycle " + String(cycle) + " worker " + String(i) + " not started")
    wait_drained(pool, cycle)

    # POISON-RECYCLE GUARD: a fresh pool that silently reused a PRIOR
    # cycle's freed/corrupted heap would show up here as wrong worker ids,
    # unstable current_worker observations, or a task recorded on the
    # wrong slice — the exact audit t30 runs once, repeated every cycle.
    for j in range(K):
        var want = j // PER
        if obs[j] != want:
            red(
                "cycle " + String(cycle) + " task " + String(j)
                + " observed worker " + String(obs[j]) + " (want "
                + String(want) + ") — poisoned/stale state from a prior cycle"
            )
    for i in range(POOL_N):
        if pool.worker_at(i)[].id() != i:
            red("cycle " + String(cycle) + " worker " + String(i)
                + " reports id " + String(pool.worker_at(i)[].id()))
        if not pool.entry_ok(i):
            red("cycle " + String(cycle) + " worker " + String(i)
                + " TLS entry round-trip failed")
        if not pool.unit_ok(i):
            red("cycle " + String(cycle) + " worker " + String(i)
                + " saw an unstable current_worker")
        if pool.obs_done(i) != PER:
            red("cycle " + String(cycle) + " worker " + String(i)
                + " ran " + String(pool.obs_done(i)) + " tasks (want "
                + String(PER) + ")")

    # loop_ok/exited are only set by a worker AFTER it observes the
    # shutdown latch and exits (thread_entry.mjs_pool_entry_main) — MUST
    # be checked AFTER request_shutdown()+join_all(), never before (a
    # worker parked in its idle loop pre-shutdown has neither set yet).
    pool.request_shutdown()
    pool.join_all()
    if pool.threads_joined() != POOL_N:
        red("cycle " + String(cycle) + " threads_joined="
            + String(pool.threads_joined()) + " != " + String(POOL_N))
    for i in range(POOL_N):
        if not pool.loop_ok(i):
            red("cycle " + String(cycle) + " worker " + String(i)
                + " loop did not complete cleanly")
        if not pool.exited(i):
            red("cycle " + String(cycle) + " worker " + String(i)
                + " never observed shutdown (thread leak)")
    # finalize() releases: the 3 pthread TLS keys (the EAGAIN surface,
    # macOS PTHREAD_KEY_MAX ceiling), the worker/entry-cell/latch/acct/
    # peers heap blocks, and the pool NativeEvent.  A SUBSEQUENT start()
    # on this SAME (now-finalized) object must raise loudly (no address-1
    # sentinel reuse) — proven once per cycle here, exactly like t30's
    # single-object fold guard, but on EVERY one of the N_POOLS objects.
    pool.finalize()
    var threw = False
    try:
        pool.start(entry)
    except:
        threw = True
    if not threw:
        red("cycle " + String(cycle) + ": start() after finalize() must raise")
    c_free(obs.bitcast[Byte]())


def main() raises:
    for cycle in range(N_POOLS):
        run_one_cycle(cycle)
    print("T49 pool churn: PASS (" + String(N_POOLS) + " pool cycles, "
          + String(N_POOLS * POOL_N) + " workers spawned+joined, no EAGAIN, "
          + "no sentinel deref, no poison-recycle)")
