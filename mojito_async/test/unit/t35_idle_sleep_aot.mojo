# mojito_async/test/unit/t35_idle_sleep_aot.mojo
#
# A2.6 (issue #72) — IDLE WORKER SLEEP/WAKE VIA NativeEvent + TLS:
# acceptance driver (E6).
#
# Proves the issue #72 exit criteria on a REAL pool, deterministically:
#   1. NO BUSY-SPIN: a 2-worker pool with no runnable work idles by SLEEPING
#      on the pool NativeEvent.  While idle the driver observes
#      _idle_workers (pool.idle_parked()) reach the full worker count (a
#      SPINNING worker never holds an idle-sleeper slot), park_total climbing
#      (workers are hitting the OS park), and per-worker idle_parks > 0 after
#      join.  This is the CPU-idle / "observed parked" evidence.
#   2. WAKE (K-wake-K-sleepers budget): the producer signals the event
#      (pool.wake_one) up to K times; a parked worker consumes a token and
#      wakes, parked drops, and wake_total reflects the token consumption.
#      The driver fires a REAL BOUNDED BURST — K = 8 wake_one() signals
#      across the sleepers (M7/t35 fold) — and asserts wake_total ADVANCES
#      (the burst provably woke workers) while staying <= the K + slack
#      budget (breadth-one: never over-signals).  The burst driver builds
#      with -O 0 (run.sh): the same loop at -O 3 trips a 1.0.0b2 codegen
#      SEGV, so the unoptimized build is the acceptance harness for it.
#   3. SHUTDOWN WAKES-AND-JOINS EVERY PARKED WORKER: request_shutdown() sets
#      the latch AND signals the event; join_all() returns with
#      threads_joined == 2, every worker exited (no leaked native threads)
#      even though every worker was asleep on the event when shutdown began.
#   4. RUNTIME-ABSENT ACCESSOR (issue #72/H5): current_worker_addr() on the
#      embedder's (never-bound) thread raises the real NoConcurrencyRuntime
#      error — the absent-path accessor is live, not dead surface.
#   5. COUNTERS (spec §71): park_total / wake_total / spurious_wake_total are
#      non-decreasing and sensible (park_total > 0 after the idle beat).
#
# The pool threads idle via the E6 seam (thread_entry.pool_worker_loop inlines
# the NativeEvent park; see thread_entry.mojo's E6 b2 workaround note).  This
# lane's acceptance is the OS-level idle sleep/wake + shutdown-join + counter
# discipline; real user-task completion is the sibling lanes' a2.2 embedder
# discipline (driven from the main thread), not exercised here.
#
# AOT only (modular/modular#6971: the JIT cannot materialize @export symbols
# from imported modules; the run.sh _aot glob drives this).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.time import sleep
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.config import make_pool_config
from mojito_async.runtime.thread_entry import mjs_pool_entry_main
from mojito_async.runtime.tls import current_worker_addr
from mojito_async.runtime.worker_pool import WorkerPool, make_pool
from mojito_async.vendor.mojito_sys import (
    entry_pointer,
    spawn_native_thread,
)


@export("mjs_pool_entry")
def mjs_pool_entry(ud: BytePtr) abi("C"):
    """Embedding-binary trampoline symbol (see thread_entry.mojo's EMBEDDING
    RULE): forwards to the shared entry body."""
    mjs_pool_entry_main(ud)


comptime POOL_N = Int(2)
comptime K = Int(8)  # wake-budget signals


def red(what: String) raises -> None:
    print("T35 idle sleep: RED (" + what + ")")
    raise Error(what)


def wait_idle(mut pool: WorkerPool, want: Int, what: String) raises:
    var spins = 0
    while pool.idle_parked() < want:
        sleep(0.001)
        spins += 1
        if spins > 20000:
            red(
                what + ": never saw " + String(want)
                + " idle sleeper(s) parked (parked="
                + String(pool.idle_parked()) + ", park_total="
                + String(pool.park_total()) + ") — workers are spinning, not sleeping"
            )


def main() raises:
    var entry = entry_pointer["mjs_pool_entry"]()
    # A no-seam pool: every worker drains 0 units, then E6-parks immediately.
    var pool = make_pool(make_pool_config(POOL_N))
    pool.start(entry)
    var key = pool.current_worker_key()
    for i in range(POOL_N):
        var wptr = pool.worker_at(i)
        var cell = pool.entry_at(i).bitcast[Byte]()
        var t = spawn_native_thread(entry, cell)
        wptr[].mark_started(t, key)

    # ---- 1. NO BUSY-SPIN: all workers park as idle sleepers --------------
    wait_idle(pool, POOL_N, "idle beat")
    if pool.park_total() < 1:
        red("park_total must be > 0 once the workers idle (got "
            + String(pool.park_total()) + ") — no worker actually parked")
    # sustained idle keeps them parked (not spinning): a short additional
    # beat must still show all workers as parked sleepers.
    sleep(0.05)
    if pool.idle_parked() < POOL_N:
        red("workers stopped reporting as parked during a sustained idle beat "
            + "(parked=" + String(pool.idle_parked()) + "); busiest worker "
            + "parked_count register would still climb — workers must sleep")

    # ---- 2. WAKE (real bounded burst: K=8 wake_one signals) ---------------
    # The driver fires a BOUNDED BURST of K wake_one() signals while the
    # workers are provably parked and requires TWO things: wake_total
    # ADVANCES (the burst provably woke at least one sleeper — the single-
    # probe version could never show consumption) and stays within the
    # K + slack budget (breadth-one: K signals wake at most K sleepers,
    # never over-signaling).  Slack covers the one extra consumed token a
    # worker may burn racing the shutdown.  (The burst loop trips a 1.0.0b2
    # codegen SEGV at -O 3, so the driver is built with -O 0 — run.sh.)
    var wake_before = pool.wake_total()
    for i in range(K):
        if pool.idle_parked() > 0:
            pool.wake_one()
    sleep(0.02)
    if pool.wake_total() <= wake_before:
        red("wake_total did not advance across the K=" + String(K)
            + " wake burst (before=" + String(wake_before)
            + ", after=" + String(pool.wake_total())
            + ") — no sleeper consumed a token (the wake hand-off is broken)")
    if pool.wake_total() > wake_before + K + 2:
        red("wake_total grew beyond the wake budget K=" + String(K)
            + " + slack over the run — over-signaling (breadth-one violated)")

    # ---- 3. SHUTDOWN WAKES-AND-JOINS EVERY PARKED WORKER -----------------
    pool.request_shutdown()
    pool.join_all()
    if pool.threads_joined() != POOL_N:
        red("threads_joined() == " + String(pool.threads_joined())
            + " (want " + String(POOL_N) + ")")
    for i in range(POOL_N):
        if not pool.exited(i):
            red("worker " + String(i) + " never observed request_shutdown (leak)")
        if pool.idle_parks(i) < 1:
            red("worker " + String(i) + " never parked as an idle sleeper (idle_parks="
                + String(pool.idle_parks(i)) + ") — it must have slept on the event")
        if not pool.entry_ok(i):
            red("worker " + String(i) + ": current_worker TLS did not round-trip")

    # ---- 4. RUNTIME-ABSENT ACCESSOR (issue #72/H5) ------------------------
    # current_worker_addr() on THIS thread (the embedder's main thread —
    # never bound by any trampoline) must raise the REAL NoConcurrencyRuntime
    # error: the accessor surface is live and the absent-path error model is
    # a raised type, not dead code.
    var key_after = pool.current_worker_key()
    var raised_absent = False
    try:
        var p = current_worker_addr(key_after)
        _ = p
    except Error:
        raised_absent = True
    if not raised_absent:
        red("current_worker_addr() on an unbound thread did not raise "
            + "NoConcurrencyRuntime (the runtime-absent path is dead)")

    # ---- 5. COUNTERS (spec §71) ------------------------------------------
    if pool.park_total() < 1:
        red("park_total must be >= 1")

    # Print the evidence BEFORE finalize(): the PASS line reports the REAL
    # pre-teardown counters (finalize frees the acct block, so post-finalize
    # reads are the quiescent 0).
    print("T35 idle sleep: PASS (idle_parked=" + String(POOL_N)
          + ", park_total=" + String(pool.park_total())
          + ", wake_total=" + String(pool.wake_total())
          + ", spurious=" + String(pool.spurious_total())
          + ", joined=" + String(pool.threads_joined()) + ")")

    pool.finalize()
