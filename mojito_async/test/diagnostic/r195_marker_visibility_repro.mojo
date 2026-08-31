# mojito_async/test/diagnostic/r195_marker_visibility_repro.mojo
#
# STANDALONE diagnostic for issue #195, round 5 — NOT a regression test of
# this codebase's own logic, and deliberately NOT wired into
# mojito_async/test/run.sh or precommit/gate.sh.  See
# mojito_async/test/diagnostic/run_r195_repro.sh for how to invoke it and
# why it lives in its own directory instead of test/unit or test/stress
# (both of those are glob-picked into the gate automatically; this is not
# meant to be).
#
# WHAT THIS ISOLATES
#
# Round 4 of issue #195 got a live-instrumented capture of
# t60_barrier_cross_worker_aot's real failure and found something very
# specific: sync/condvar.mojo's `resolve_winner` reads WaitNode._next as 0
# when the trace proves the notifying thread's `notify_marker` stamped it
# to 1 moments earlier via `set_next(reason)`.  Four hypotheses for the
# mechanism were tested and ALL came back negative (read-side pointer
# indirection, write-side pointer indirection, dead-store elimination via a
# same-thread readback check, and cross-function inlining).  What was left,
# quoting round 4's own writeup:
#
#   "a store that provably executes (same-thread readback proves it), uses
#    the strongest ordering this codebase's atomics offer on both ends,
#    crosses a SEQUENTIAL SpinLock two or three separate times between the
#    write and the read, and still isn't observed."
#
# That is specific enough to be a real modular/modular (Mojo compiler) bug
# candidate, but it was only ever observed embedded inside the full t60
# driver (real scheduler, real TaskControlBlock, real park/wake kernel).
# This driver strips ALL of that away and keeps only the bare mechanism
# round 4's finding actually implicates:
#
#   - TWO REAL OS THREADS (pthreads, this codebase's own extern bindings —
#     no scheduler, no Runtime, no Worker, no TaskControlBlock, no
#     park.mojo anywhere in this file's import list).
#   - ONE shared plain Int64 cell (`marker`), accessed exclusively through
#     `Atomic[DType.int64].load[ordering=Ordering.ACQUIRE]` /
#     `.store[ordering=Ordering.RELEASE]` over its raw address — the EXACT
#     access pattern task_control_block.mojo's WaitNode.next()/set_next()
#     use for `_next`, copied verbatim (see task_control_block.mojo:135-147).
#   - ONE instance of THIS codebase's own `SpinLock`
#     (runtime/queue.mojo) — the same type RemoteReadyQueue embeds, whose
#     compare_exchange/store already default to Ordering.SEQUENTIAL (round
#     3's own disassembly confirmed this compiles to `casal`/`stlr` on
#     arm64, the strongest ordering this codebase's atomics offer).
#
# THE MIRRORED CROSSING PATTERN
#
# In the real code, `notify_marker` (condvar.mojo) writes `_next` with a
# plain unguarded call to `set_next(reason)` — BEFORE calling
# `unpark_current` at all.  `unpark_current` (park.mojo) then:
#   1. locks/unlocks `owner[].remote_queue()[]._guard` for the claim
#      section (reads/writes TCB state under the guard);
#   2. on a successful claim, calls `owner[].push_remote(...)`, which locks
#      /unlocks the SAME `_guard` a second time (RemoteReadyQueue.push).
# On the OTHER worker's thread, `scheduler_loop` later:
#   3. locks/unlocks the SAME `_guard` via `has_remote()` (is_empty());
#   4. locks/unlocks it AGAIN via `pop_remote()` (pop()).
# Only THEN does `resolve_winner` read `_next` back — and round 4 caught it
# reading 0.
#
# This driver reproduces that exact shape with ONE producer thread and ONE
# consumer thread, single-flight (one marker value in flight per round, so
# a miss can only mean THIS round's write was missed — never a genuine
# future overwrite racing a slow reader):
#
#   PRODUCER (per round r, r = 1..ROUNDS):
#     a. UNGUARDED atomic RELEASE store: marker := r.
#     b. Same-thread READBACK (round 4's clean negative-dead-store-
#        elimination test): atomic ACQUIRE load of marker, on the SAME
#        thread, before touching the lock at all.  Any mismatch here would
#        mean the store itself never executed — tracked separately from
#        the cross-thread test below.
#     c. CROSSING #1 — guard.lock(); touch a guarded dummy counter (some
#        guarded work happens in the real claim section too);
#        guard.unlock().  Mirrors unpark_current's claim-section crossing.
#     d. CROSSING #2 — guard.lock(); produced := 1; guard.unlock().
#        Mirrors push_remote's own push() crossing.
#     e. Spin-wait (bounded) for the consumer to clear `produced` before
#        producing round r+1 — pure pacing, keeps exactly one round's
#        marker value in flight.
#
#   CONSUMER (per round r):
#     a. Spin-wait (bounded) via CROSSING #3 — guard.lock(); read
#        `produced`; guard.unlock() — until it sees 1.  Mirrors
#        scheduler_loop's has_remote() poll.
#     b. CROSSING #4 — guard.lock(); produced := 0; guard.unlock().
#        Mirrors pop_remote's own pop() crossing.
#     c. THE READ UNDER TEST — atomic ACQUIRE load of marker.  Mirrors
#        resolve_winner's h.tcb()[].wait_node()[].next().  If it is not
#        exactly r, that is round 4's exact anomaly, reproduced outside the
#        whole mojito-async stack.
#
# Four real lock/unlock crossings on ONE SpinLock instance sit between the
# producer's write (step a) and the consumer's read (step c) — matching
# round 4's "two or three separate lock/unlock cycles" (claim section,
# push_remote, has_remote/pop_remote) crossing count.
#
# VERDICT: this driver does not print PASS/RED in the gate sense — see
# run_r195_repro.sh for the aggregate verdict across many process
# invocations.  This binary itself prints a per-run summary line and exits
# 0 always (a miss is DATA for this investigation, not a driver bug) unless
# an internal pacing invariant is violated (a genuine hang in this driver's
# OWN bookkeeping, distinct from the visibility question under test), in
# which case it exits 1 with HANG in the summary.
from std.atomic import Atomic, Ordering
from mojito_async.runtime.queue import SpinLock
from mojito_async.vendor.mojito_sys import c_malloc, entry_pointer

comptime BytePtr = UnsafePointer[Byte, MutAnyOrigin]


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


# Rounds per process invocation.  run_r195_repro.sh repeats the WHOLE
# PROCESS (fresh ASLR base, fresh thread scheduling) many times on top of
# this — see that script for the outer repeat count.  This count alone
# already dwarfs round 1's original 2,000,000-iteration retry-loop probe
# across a handful of repro runs.
comptime ROUNDS = Int(5000000)

# Pure pacing bound — NOT part of the crossing count under test, just a
# watchdog against a bug in this driver's OWN handshake hanging the run.
comptime SPIN_BUDGET = Int(20000000)

# --- results cell layout (plain Int cells; each field is written by
# exactly one of the two threads during the run, read by the parent only
# after both are joined) -----------------------------------------------
comptime R_ERR0 = Int(0)              # producer thread raised
comptime R_ERR1 = Int(1)              # consumer thread raised
comptime R_ROUNDS_DONE = Int(2)       # rounds the consumer fully resolved
comptime R_READBACK_FAIL = Int(3)     # producer same-thread readback misses
comptime R_VIS_FAIL = Int(4)          # consumer cross-thread visibility misses
comptime R_FIRST_FAIL_ROUND = Int(5)  # round of the first visibility miss (0 = none)
comptime R_FIRST_FAIL_SEEN = Int(6)   # marker value the consumer actually saw
comptime R_HANG = Int(7)              # 1 if either side hit SPIN_BUDGET
comptime R_HANG_LANE = Int(8)         # 1 = producer, 2 = consumer
comptime R_CELLS = Int(16)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var guard: UnsafePointer[SpinLock, MutAnyOrigin]
    # `markers` is ONE Int64 cell PER ROUND (an array of length ROUNDS), NOT
    # one shared cell reused every round.  This matters: the real driver
    # allocates a FRESH TaskControlBlock (and therefore a fresh WaitNode
    # `_next`) per round (t60's `_drive_round` calls `TB.create()` every
    # time), so there is never a possibility of a round r+1 write aliasing
    # round r's still-unread cell.  An earlier version of this file reused
    # ONE cell across rounds and got a deterministic false positive: every
    # "miss" read back exactly round+1's value, never 0 and never anything
    # else, which is the fingerprint of the producer's own pacing wait
    # unblocking (and starting round r+1's write) a few cycles before the
    # consumer's read of round r's cell completed — a race in THIS
    # driver's own single-cell reuse, not the phenomenon under test.  With
    # per-round cells that race is structurally impossible: whatever the
    # consumer reads out of markers[r], it is EITHER the producer's stamped
    # value for round r, OR 0 (that cell's zero-initialized value, never
    # written by anyone else) — nothing else can land there.
    var markers: UnsafePointer[Int64, MutAnyOrigin]
    var produced: UnsafePointer[Int64, MutAnyOrigin]  # guarded readiness flag
    var claim_dummy: UnsafePointer[Int64, MutAnyOrigin]  # guarded dummy work
    var r: UnsafePointer[Int, MutAnyOrigin]           # results block (R_CELLS)

    def __init__(out self):
        self.guard = UnsafePointer[SpinLock, MutAnyOrigin](unsafe_from_address=1)
        self.markers = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=1)
        self.produced = self.markers
        self.claim_dummy = self.markers
        self.r = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def serve_producer(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var g = sc.guard
    var mk_base = sc.markers
    var prod = sc.produced
    var dummy = sc.claim_dummy
    var r_ = sc.r

    for round_i in range(ROUNDS):
        var rv = Int64(round_i + 1)
        var mk = mk_base + round_i

        # (a) UNGUARDED atomic RELEASE store — mirrors notify_marker's
        # set_next(reason), issued before unpark_current touches any lock.
        Atomic[DType.int64].store[ordering=Ordering.RELEASE](mk, rv)

        # (b) Same-thread readback (round 4's clean test #3).
        var back = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](mk)
        if back != rv:
            r_[R_READBACK_FAIL] = r_[R_READBACK_FAIL] + 1

        # (c) CROSSING #1 — mirrors unpark_current's claim-section guard.
        g[].lock()
        dummy[0] = dummy[0] + 1
        g[].unlock()

        # (d) CROSSING #2 — mirrors push_remote's own push() guard.
        g[].lock()
        prod[0] = Int64(1)
        g[].unlock()

        # (e) Pacing: wait for the consumer to clear `produced`.
        var spins = 0
        while True:
            g[].lock()
            var still = prod[0]
            g[].unlock()
            if still == 0:
                break
            spins += 1
            if spins > SPIN_BUDGET:
                r_[R_HANG] = 1
                r_[R_HANG_LANE] = 1
                return


def serve_consumer(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var g = sc.guard
    var mk_base = sc.markers
    var prod = sc.produced
    var r_ = sc.r

    for round_i in range(ROUNDS):
        var expect = Int64(round_i + 1)
        var mk = mk_base + round_i

        # (a) CROSSING #3 — mirrors scheduler_loop's has_remote() poll.
        var spins = 0
        while True:
            g[].lock()
            var avail = prod[0]
            g[].unlock()
            if avail != 0:
                break
            spins += 1
            if spins > SPIN_BUDGET:
                r_[R_HANG] = 1
                r_[R_HANG_LANE] = 2
                return

        # (b) CROSSING #4 — mirrors pop_remote's own pop() guard.
        g[].lock()
        prod[0] = Int64(0)
        g[].unlock()

        # (c) THE READ UNDER TEST — mirrors resolve_winner's
        # h.tcb()[].wait_node()[].next().
        var seen = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](mk)
        if seen != expect:
            r_[R_VIS_FAIL] = r_[R_VIS_FAIL] + 1
            if r_[R_FIRST_FAIL_ROUND] == 0:
                r_[R_FIRST_FAIL_ROUND] = round_i + 1
                r_[R_FIRST_FAIL_SEEN] = Int(seen)

        r_[R_ROUNDS_DONE] = round_i + 1


@export("r195_producer")
def r195_producer(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_producer(sc)
    except e:
        sc[].r[R_ERR0] = 1
        print("  producer raised: " + String(e))


@export("r195_consumer")
def r195_consumer(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_consumer(sc)
    except e:
        sc[].r[R_ERR1] = 1
        print("  consumer raised: " + String(e))


def run_scenario() raises -> Int:
    var guard = SpinLock()
    var guardp = UnsafePointer[SpinLock, MutAnyOrigin](to=guard)

    # One Int64 cell PER ROUND (see the Scene.markers comment above for why
    # this is not one shared cell) plus two small guarded scalars.
    var markersp = UnsafePointer[Int64, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(ROUNDS * 8))
    )
    for i in range(ROUNDS):
        markersp[i] = Int64(0)

    var scalars = Int(c_malloc(2 * 8))
    var producedp = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=scalars)
    var dummyp = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=scalars + 8)
    producedp[0] = Int64(0)
    dummyp[0] = Int64(0)

    var rcells = Int(c_malloc(R_CELLS * 8))
    var rp = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=rcells)
    for i in range(R_CELLS):
        rp[i] = 0

    var sc = UnsafePointer[Scene, MutAnyOrigin](unsafe_from_address=Int(c_malloc(256)))
    sc[0] = Scene()
    sc[].guard = guardp
    sc[].markers = markersp
    sc[].produced = producedp
    sc[].claim_dummy = dummyp
    sc[].r = rp

    var t0: Int = 0
    var t1: Int = 0
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t0), 0,
        entry_pointer["r195_producer"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t1), 0,
        entry_pointer["r195_consumer"](), sc.bitcast[Byte](),
    )
    _ = _pthread_join(UInt(t0), 0)
    _ = _pthread_join(UInt(t1), 0)

    var r_ = sc[].r
    print("R195 marker-visibility repro (issue #195 round 5)")
    print("  rounds_done=" + String(r_[R_ROUNDS_DONE]) + "/" + String(ROUNDS)
          + " readback_fail=" + String(r_[R_READBACK_FAIL])
          + " visibility_fail=" + String(r_[R_VIS_FAIL]))

    if r_[R_ERR0] != 0 or r_[R_ERR1] != 0:
        print("  - a lane raised (see above)")
        return 2

    if r_[R_HANG] != 0:
        print("  - HANG: lane " + String(r_[R_HANG_LANE])
              + " exceeded its pacing spin budget (" + String(SPIN_BUDGET)
              + " iterations). This is this DRIVER's own handshake stalling,"
              + " not (necessarily) the visibility question under test.")
        return 1

    if r_[R_READBACK_FAIL] != 0:
        print("  - same-thread readback failed " + String(r_[R_READBACK_FAIL])
              + " time(s): the producer's OWN next load after its OWN store"
              + " did not observe it. This would be a materially DIFFERENT"
              + " (and more severe) finding than round 4's — dead-store"
              + " elimination or a genuinely broken atomic, not a cross-"
              + " thread visibility gap.")

    if r_[R_VIS_FAIL] != 0:
        print("  - VISIBILITY MISS x" + String(r_[R_VIS_FAIL])
              + ": the consumer crossed the SAME SpinLock 4 times after the"
              + " producer's atomic RELEASE store (which the producer's own"
              + " same-thread readback confirmed executed) and still read a"
              + " stale marker at least once.")
        print("    first miss: round " + String(r_[R_FIRST_FAIL_ROUND])
              + ", expected that round's stamped value, saw "
              + String(r_[R_FIRST_FAIL_SEEN]) + " instead.")
        print("R195: REPRODUCED")
        return 0

    print("R195: no visibility miss this run"
          + " (" + String(ROUNDS) + " rounds, 4 lock crossings each)")
    return 0


def main() raises:
    var rc: Int
    try:
        rc = run_scenario()
    except e:
        print("R195 marker-visibility repro: driver raised " + String(e))
        rc = 2
    _iso_exit(Int32(rc))
