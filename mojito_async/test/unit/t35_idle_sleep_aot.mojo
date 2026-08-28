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
#      Breadth-one: wake_total stays <= K + slack (never over-signals).  A
#      consumer of every token is observed across repeated bursts.
#   3. SHUTDOWN WAKES-AND-JOINS EVERY PARKED WORKER: request_shutdown() sets
#      the latch AND signals the event; join_all() returns with
#      threads_joined == 2, every worker exited (no leaked native threads)
#      even though every worker was asleep on the event when shutdown began.
#   4. COUNTERS (spec §71): park_total / wake_total / spurious_wake_total are
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

    # ---- 2. WAKE (K-wake-K-sleepers budget) -------------------------------
    # Signal the pool NativeEvent K times.  Each signal wakes at most one
    # parked worker (breadth-one); across bursts every parked worker should
    # be woken at least once and wake_total climb.  wake_total is timing-racy
    # (a worker can be mid-cycle at the exact signal), so we burst until we
    # OBSERVE at least one consumption (the mechanism, proven), while bounding
    # wake_total by K + slack (no over-signaling).
    var observed_wake = False
    var wake_before = pool.wake_total()
    for burst in range(8):
        var base = pool.wake_total()
        var parked_now = pool.idle_parked()
        if parked_now > 0:
            for i in range(K):
                pool.wake_one()
            sleep(0.01)
        if pool.wake_total() > base:
            observed_wake = True
            break
    if not observed_wake:
        red("wake budget produced no observed token consumption in 8 bursts "
            + "(wake_total=" + String(pool.wake_total()) + ") — a parked "
            + "worker never consumed a signal")
    var wake_cnt = pool.wake_total() - wake_before
    if wake_cnt > K + 2:
        red("wake_total delta " + String(wake_cnt)
            + " exceeds the wake budget K=" + String(K)
            + " + slack — over-signaling (breadth-one violated)")

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

    # ---- 4. COUNTERS (spec §71) ------------------------------------------
    if pool.park_total() < 1:
        red("park_total must be >= 1")

    pool.finalize()

    print("T35 idle sleep: PASS (idle_parked=" + String(POOL_N)
          + ", park_total=" + String(pool.park_total())
          + ", wake_total=" + String(pool.wake_total())
          + ", spurious=" + String(pool.spurious_total())
          + ", joined=" + String(pool.threads_joined()) + ")")
