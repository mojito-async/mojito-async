# mojito_async/test/diagnostic/r195_cluster_visibility_repro.mojo
#
# STANDALONE diagnostic for issue #195, round 6 — NOT a regression test of
# this codebase's own logic, deliberately NOT wired into
# mojito_async/test/run.sh or precommit/gate.sh.  Sibling of round 5's
# r195_marker_visibility_repro.mojo (kept alongside it, not merged into it,
# so round 5's already-merged, already-run repro stays byte-for-byte
# reproducible on its own).  See run_r195_cluster_repro.sh for invocation.
#
# WHAT THIS TESTS — round 5's candidate 6, option 1
#
# Round 4 caught sync/condvar.mojo's `resolve_winner` reading
# WaitNode._next as 0 moments after notify_marker's `set_next(reason)`
# demonstrably stamped it (same-thread readback proved the store executed).
# Round 5 built a minimal repro isolating EXACTLY that one field (one
# Atomic[DType.int64] cell, one SpinLock, the same 4-lock-crossing count)
# and ran it 560 million times with ZERO reproductions — strong evidence
# that the bare primitive (one atomic field, crossed by itself) is not the
# mechanism.  Round 5's own ranked follow-up list put this first:
#
#   "The real TCB's field CLUSTER, not a single isolated atomic. ... Worth
#    trying a 'fatter' version of this same repro with several more atomic
#    fields laid out and touched the same way, to test whether the cluster
#    itself (register pressure, more stores queued around the same
#    barrier, something about how many stores land right before a lock
#    crossing) is a necessary ingredient."
#
# This file is that fatter version.  Same two-pthread, one-SpinLock shape
# as round 5; the only change is WHAT happens around the crossings.
#
# THE REAL FIELD CLUSTER AND ITS REAL TOUCH ORDER
#
# `FatTCB` below lays out the real TCB_Prefix + WaitNode's ATOMIC fields in
# their real declared order (task_control_block.mojo:122-129, 182-266):
# `_state`, `_generation`, WaitNode's `_reason` then `_next`, `_started`,
# `_owner_runtime`, `_early`, `_claim_epoch`.  The non-atomic fields between
# them (`_has_result`/`_failed`/`_err`/`_parent`/`_scope`/`_owner_worker`)
# are represented as plain filler cells of comparable size in the same
# relative positions, so the atomic fields sit at the same STRUCTURAL
# distances apart as the real struct — except `_err`, which is a `String`
# (heap-backed) in the real struct.  This driver allocates FatTCB cells
# directly out of raw c_malloc'd memory with no proper Mojo-managed
# construction/destruction (matching round 5's own approach, and required
# here since the process ends via `_exit` without running destructors) —
# placing a real `String` in that memory would be its own unrelated hazard
# to get right, not something this investigation is testing, so `_err` is
# represented by a same-sized `Int64` filler and that simplification is
# flagged here rather than silently glossed over.
#
# The producer thread reproduces `notify_marker` -> `unpark_current` ->
# `wake_claim` -> `push_remote`'s EXACT real sequence of touches to this
# cluster (park.mojo:78-172, task_control_block.mojo's wake_claim/_apply/
# clear_early_readiness), in the real order, relative to the SAME `_next`
# write and the SAME lock crossings round 5 already modeled:
#
#   PRODUCER (per round r):
#     a. UNGUARDED atomic RELEASE store: _next := r.
#        (notify_marker's set_next(reason), issued before touching any lock)
#     b. Same-thread ACQUIRE readback of _next (round 4/5's clean
#        dead-store-elimination check, kept unchanged).
#     c. UNGUARDED ACQUIRE load of _state.
#        (unpark_current's own top-of-function `h.state() == RUNNABLE`
#        fast-return check, park.mojo:133)
#     d. UNGUARDED ACQUIRE load of _owner_runtime.
#        (_owner_rt's owner_runtime() call, park.mojo:135/219)
#     e. CROSSING #1 lock.  (owner[].remote_queue()[]._guard.lock(),
#        park.mojo:139 — the claim section)
#     f. GUARDED ACQUIRE load of _state.
#        (unpark_current's own state check under the lock, park.mojo:141)
#     g. GUARDED ACQUIRE load of _state AGAIN.
#        (wake_claim's OWN `self.state()` re-check,
#        task_control_block.mojo:402 — yes, a second read of the same
#        field under the same lock a few lines later; reproduced verbatim,
#        not simplified away, since that is part of what "the real
#        sequence" means)
#     h. GUARDED ACQUIRE load of _generation.
#        (wake_claim's `var gen = self.generation()`,
#        task_control_block.mojo:404)
#     i. GUARDED RELEASE store of _state (-> RUNNABLE-equivalent).
#        (wake_claim's `self._apply(RUNNABLE)` ->
#        `_apply`'s atomic store, task_control_block.mojo:316-317)
#     j. GUARDED RELEASE store of _claim_epoch (:= gen).
#        (wake_claim's H2 duplicate-claim stamp,
#        task_control_block.mojo:408-410)
#     k. GUARDED RELEASE store of _early (:= 0).
#        (unpark_current's `clear_early_readiness()` on a successful claim,
#        park.mojo:145)
#     l. CROSSING #1 unlock.
#     m. CROSSING #2 lock; produced := 1; CROSSING #2 unlock.
#        (push_remote's own RemoteReadyQueue.push() crossing,
#        runtime.mojo:311-333 / queue.mojo's `push`)
#     n. Pacing spin-wait for the consumer to clear `produced` (bookkeeping
#        only, not part of the crossing count under test — identical to
#        round 5's step (e)).
#
#   CONSUMER (per round r):
#     a. CROSSING #3 poll loop; guard.lock(); read `produced`; guard.unlock()
#        — until it sees 1.  (scheduler_loop's `rt.has_remote()` poll,
#        scheduler.mojo:185)
#     b. CROSSING #4: guard.lock(); produced := 0; guard.unlock().
#        (scheduler_loop's `rt.pop_remote()`, scheduler.mojo:186)
#     c. UNGUARDED ACQUIRE load of _state.
#        (scheduler_loop's own very next line, `checker[].state() !=
#        TaskControlBlock.RUNNABLE`, scheduler.mojo:193 — read
#        cross-thread, UNGUARDED, immediately after popping the record and
#        BEFORE the dispatcher/task body would run and eventually reach
#        resolve_winner; recorded as a secondary visibility check below,
#        since a stale read here would produce a DIFFERENT, also-plausible
#        failure shape — the scheduler wrongly treating a freshly-claimed
#        task as stale and skipping it — worth distinguishing from a stale
#        _next read specifically)
#     d. THE READ UNDER TEST — UNGUARDED ACQUIRE load of _next.
#        (resolve_winner's h.tcb()[].wait_node()[].next(), condvar.mojo:108)
#
# Same 4 real lock/unlock crossings as round 5 between the _next write and
# the _next read.  What is NEW here: 5 more atomic fields (_state x2 reads
# + 1 write, _generation x1 read, _owner_runtime x1 read, _claim_epoch x1
# write, _early x1 write — 8 additional atomic operations total) all
# landing in the SAME producer-side critical section as _next's neighbor
# operations, in the SAME relative order the real code performs them, plus
# one more cross-thread read (_state) on the consumer side immediately
# before the read under test — exactly the register-pressure / clustered-
# stores shape candidate 6 option 1 named.
#
# VERDICT: same convention as round 5 — this binary always exits 0 (a miss
# is DATA, not a driver bug) unless the driver's OWN pacing handshake hangs
# (exit 1, HANG) or a lane raises (exit 2).  See run_r195_cluster_repro.sh
# for the aggregate verdict across many process invocations.
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


@extern("sched_yield")
def _sched_yield() abi("C") -> Int32: ...


# Rounds per process invocation.  run_r195_cluster_repro.sh repeats the
# WHOLE PROCESS many times on top of this, same convention as round 5.
comptime ROUNDS = Int(5000000)

# Pure pacing bound — NOT part of the crossing count under test.
comptime SPIN_BUDGET = Int(20000000)

# Backoff cadence for the pacing spin loops ONLY (producer's wait-for-
# consumer-to-clear loop, consumer's has_remote-equivalent poll loop) --
# NOT the SpinLock's own internal compare_exchange retry, and NOT any of
# the 4 real crossings under test.  Calling sched_yield() every
# BACKOFF_INTERVAL unsuccessful poll iterations is a standard busy-wait
# courtesy so the strict single-flight producer/consumer alternation
# does not needlessly hammer the shared cache line while waiting; it does
# not touch SpinLock.lock() itself or any of the 4 crossings' own retry
# behavior.  (An earlier version of this file had a genuine heap buffer
# overflow in its FatTCB array allocation -- see the comment beside
# fat_stride in run_scenario -- that looked like a severe throughput
# collapse / near-hang during development; that was memory corruption,
# not a contention effect, and is fixed now.  This backoff is kept as
# ordinary good practice for a busy-poll loop, not as a fix for that bug.)
comptime BACKOFF_INTERVAL = Int(64)

# --- FatTCB: mirrors TCB_Prefix + WaitNode's real field order ----------
# ATOMIC fields (touched, in the real code's own order) carry a comment
# naming the real field they stand in for.  Non-atomic filler fields are
# present, in the same relative position, purely to keep the atomic
# fields' structural spacing comparable to the real struct; the driver
# never touches them except to zero-initialize.
struct FatTCB(ImplicitlyCopyable, ImplicitlyDeletable):
    var _state: Int64           # atomic — TCB_Prefix._state
    var _generation: Int64      # atomic — TCB_Prefix._generation
    var _wait_generation: Int   # filler — WaitNode._generation (plain, untouched)
    var _reason: Int64          # filler — WaitNode._reason (atomic in reality,
                                 #   untouched on THIS call path: notify_marker's
                                 #   win_reason is UNCHANGED for every current
                                 #   sync-primitive caller, task_control_block
                                 #   comment at park.mojo:107-119)
    var _next: Int64            # atomic — WaitNode._next -- FIELD UNDER TEST
    var _has_result: Bool       # filler — TCB_Prefix._has_result
    var _failed: Bool           # filler — TCB_Prefix._failed
    var _err_filler: Int64      # filler — TCB_Prefix._err (String in reality;
                                 #   see file header for why this is not mirrored
                                 #   as a real String)
    var _parent: Int            # filler — TCB_Prefix._parent
    var _scope: Int             # filler — TCB_Prefix._scope
    var _started: UInt8         # atomic — TCB_Prefix._started (untouched on
                                 #   this call path: only _apply(RUNNING) writes
                                 #   it, and this wake path applies RUNNABLE)
    var _owner_worker: Int      # filler — TCB_Prefix._owner_worker
    var _owner_runtime: Int64   # atomic — TCB_Prefix._owner_runtime
    var _early: UInt8           # atomic — TCB_Prefix._early
    var _claim_epoch: Int64     # atomic — TCB_Prefix._claim_epoch

    def __init__(out self):
        self._state = Int64(0)
        self._generation = Int64(1)
        self._wait_generation = 0
        self._reason = Int64(0)
        self._next = Int64(0)
        self._has_result = False
        self._failed = False
        self._err_filler = Int64(0)
        self._parent = 0
        self._scope = 0
        self._started = UInt8(0)
        self._owner_worker = 0
        self._owner_runtime = Int64(0)
        self._early = UInt8(0)
        self._claim_epoch = Int64(0)


comptime STATE_RUNNABLE = Int64(1)  # TCB_Prefix.RUNNABLE

# --- results cell layout (plain Int cells) ------------------------------
comptime R_ERR0 = Int(0)
comptime R_ERR1 = Int(1)
comptime R_ROUNDS_DONE = Int(2)
comptime R_READBACK_FAIL = Int(3)
comptime R_VIS_FAIL = Int(4)          # _next visibility misses (the primary test)
comptime R_FIRST_FAIL_ROUND = Int(5)
comptime R_FIRST_FAIL_SEEN = Int(6)
comptime R_HANG = Int(7)
comptime R_HANG_LANE = Int(8)
comptime R_STATE_VIS_FAIL = Int(9)    # _state visibility misses (secondary check)
comptime R_CELLS = Int(16)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var guard: UnsafePointer[SpinLock, MutAnyOrigin]
    # One FatTCB PER ROUND (array of length ROUNDS) — same reasoning as
    # round 5's per-round `markers` cells: the real driver allocates a
    # fresh TaskControlBlock per round, so a round r+1 write can never
    # alias round r's still-unread cell.  See round 5's file for the false
    # positive this avoided when a single cell was reused instead.
    var tcbs: UnsafePointer[FatTCB, MutAnyOrigin]
    var produced: UnsafePointer[Int64, MutAnyOrigin]  # guarded readiness flag
    var r: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.guard = UnsafePointer[SpinLock, MutAnyOrigin](unsafe_from_address=1)
        self.tcbs = UnsafePointer[FatTCB, MutAnyOrigin](unsafe_from_address=1)
        self.produced = self.tcbs.bitcast[Int64]()
        self.r = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def serve_producer(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var g = sc.guard
    var tcb_base = sc.tcbs
    var prod = sc.produced
    var r_ = sc.r

    for round_i in range(ROUNDS):
        var rv = Int64(round_i + 1)
        var t = tcb_base + round_i

        # (a) UNGUARDED atomic RELEASE store — notify_marker's
        # set_next(reason), before touching any lock.
        Atomic[DType.int64].store[ordering=Ordering.RELEASE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._next), rv)

        # (b) Same-thread readback (round 4/5's clean test).
        var back = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._next))
        if back != rv:
            r_[R_READBACK_FAIL] = r_[R_READBACK_FAIL] + 1

        # (c) UNGUARDED ACQUIRE load of _state — unpark_current's top-of-
        # function fast-return check.
        _ = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._state))

        # (d) UNGUARDED ACQUIRE load of _owner_runtime — _owner_rt.
        _ = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._owner_runtime))

        # (e) CROSSING #1 lock — the claim section.
        g[].lock()

        # (f) GUARDED ACQUIRE load of _state — unpark_current's own check.
        var st1 = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._state))
        _ = st1

        # (g) GUARDED ACQUIRE load of _state AGAIN — wake_claim's own
        # self.state() re-check, verbatim (not deduplicated).
        var st2 = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._state))
        _ = st2

        # (h) GUARDED ACQUIRE load of _generation — wake_claim's gen.
        var gen = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._generation))

        # (i) GUARDED RELEASE store of _state -> RUNNABLE-equivalent —
        # wake_claim's _apply(RUNNABLE).
        Atomic[DType.int64].store[ordering=Ordering.RELEASE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._state), STATE_RUNNABLE)

        # (j) GUARDED RELEASE store of _claim_epoch := gen — wake_claim's
        # H2 duplicate-claim stamp.
        Atomic[DType.int64].store[ordering=Ordering.RELEASE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._claim_epoch), gen)

        # (k) GUARDED RELEASE store of _early := 0 —
        # clear_early_readiness() on a successful claim.
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](
            UnsafePointer[UInt8, MutAnyOrigin](to=t[]._early), UInt8(0))

        # (l) CROSSING #1 unlock.
        g[].unlock()

        # (m) CROSSING #2 — push_remote's own RemoteReadyQueue.push().
        g[].lock()
        prod[0] = Int64(1)
        g[].unlock()

        # (n) Pacing: wait for the consumer to clear `produced`.
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
            if spins % BACKOFF_INTERVAL == 0:
                _ = _sched_yield()


def serve_consumer(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var g = sc.guard
    var tcb_base = sc.tcbs
    var prod = sc.produced
    var r_ = sc.r

    for round_i in range(ROUNDS):
        var expect = Int64(round_i + 1)
        var t = tcb_base + round_i

        # (a) CROSSING #3 — scheduler_loop's has_remote() poll.
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
            if spins % BACKOFF_INTERVAL == 0:
                _ = _sched_yield()

        # (b) CROSSING #4 — scheduler_loop's pop_remote().
        g[].lock()
        prod[0] = Int64(0)
        g[].unlock()

        # (c) UNGUARDED ACQUIRE load of _state — scheduler_loop's very next
        # line (checker[].state() != RUNNABLE), read cross-thread,
        # immediately after popping, before the dispatcher would run.
        # Secondary check: a stale read here (expected STATE_RUNNABLE) is a
        # DIFFERENT failure shape than the primary _next miss below.
        var seen_state = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._state))
        if seen_state != STATE_RUNNABLE:
            r_[R_STATE_VIS_FAIL] = r_[R_STATE_VIS_FAIL] + 1

        # (d) THE READ UNDER TEST — resolve_winner's
        # h.tcb()[].wait_node()[].next().
        var seen = Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=t[]._next))
        if seen != expect:
            r_[R_VIS_FAIL] = r_[R_VIS_FAIL] + 1
            if r_[R_FIRST_FAIL_ROUND] == 0:
                r_[R_FIRST_FAIL_ROUND] = round_i + 1
                r_[R_FIRST_FAIL_SEEN] = Int(seen)

        r_[R_ROUNDS_DONE] = round_i + 1


@export("r195c_producer")
def r195c_producer(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_producer(sc)
    except e:
        sc[].r[R_ERR0] = 1
        print("  producer raised: " + String(e))


@export("r195c_consumer")
def r195c_consumer(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_consumer(sc)
    except e:
        sc[].r[R_ERR1] = 1
        print("  consumer raised: " + String(e))


def run_scenario() raises -> Int:
    var guard = SpinLock()
    var guardp = UnsafePointer[SpinLock, MutAnyOrigin](to=guard)

    # Compute FatTCB's REAL stride via typed pointer arithmetic rather than
    # a hand-counted byte literal: an earlier version of this file hardcoded
    # 96 (the sum of each field's own size, ignoring alignment padding
    # between the Bool/UInt8 fields and their neighboring 8-byte-aligned
    # Int64/Int fields) and under-allocated this buffer by 16 bytes per
    # element (real stride is 112) -- a heap buffer overflow that corrupted
    # whatever c_malloc handed back next (the produced-flag/results cells
    # allocated right after this one), which surfaced as an apparent
    # deadlock in the producer/consumer pacing handshake, not as an
    # obviously-a-crash.  Caught via an isolated single-instance repro
    # (no array, so no allocation-sizing question) that ran clean, which
    # narrowed it to the array allocation specifically.
    var _probe_a = UnsafePointer[FatTCB, MutAnyOrigin](unsafe_from_address=4096)
    var _probe_b = _probe_a + 1
    var fat_stride = Int(_probe_b) - Int(_probe_a)
    var tcbs_bytes = Int(c_malloc(ROUNDS * fat_stride))
    var tcbsp = UnsafePointer[FatTCB, MutAnyOrigin](unsafe_from_address=tcbs_bytes)
    for i in range(ROUNDS):
        tcbsp[i] = FatTCB()

    var scalars = Int(c_malloc(1 * 8))
    var producedp = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=scalars)
    producedp[0] = Int64(0)

    var rcells = Int(c_malloc(R_CELLS * 8))
    var rp = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=rcells)
    for i in range(R_CELLS):
        rp[i] = 0

    var sc = UnsafePointer[Scene, MutAnyOrigin](unsafe_from_address=Int(c_malloc(256)))
    sc[0] = Scene()
    sc[].guard = guardp
    sc[].tcbs = tcbsp
    sc[].produced = producedp
    sc[].r = rp

    var t0: Int = 0
    var t1: Int = 0
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t0), 0,
        entry_pointer["r195c_producer"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t1), 0,
        entry_pointer["r195c_consumer"](), sc.bitcast[Byte](),
    )
    _ = _pthread_join(UInt(t0), 0)
    _ = _pthread_join(UInt(t1), 0)

    var r_ = sc[].r
    print("R195 CLUSTER visibility repro (issue #195 round 6)")
    print("  rounds_done=" + String(r_[R_ROUNDS_DONE]) + "/" + String(ROUNDS)
          + " readback_fail=" + String(r_[R_READBACK_FAIL])
          + " next_visibility_fail=" + String(r_[R_VIS_FAIL])
          + " state_visibility_fail=" + String(r_[R_STATE_VIS_FAIL]))

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
              + " did not observe it.")

    if r_[R_STATE_VIS_FAIL] != 0:
        print("  - SECONDARY _state visibility miss x" + String(r_[R_STATE_VIS_FAIL])
              + ": the consumer's cross-thread _state read (mirroring"
              + " scheduler_loop's post-pop check) did not see the"
              + " producer's guarded RUNNABLE store.")

    if r_[R_VIS_FAIL] != 0:
        print("  - _next VISIBILITY MISS x" + String(r_[R_VIS_FAIL])
              + ": the consumer crossed the SAME SpinLock 4 times after the"
              + " producer's atomic RELEASE store to _next (which the"
              + " producer's own same-thread readback confirmed executed),"
              + " with the full real field-cluster touch sequence"
              + " interleaved around those crossings, and still read a"
              + " stale marker at least once.")
        print("    first miss: round " + String(r_[R_FIRST_FAIL_ROUND])
              + ", expected that round's stamped value, saw "
              + String(r_[R_FIRST_FAIL_SEEN]) + " instead.")
        print("R195C: REPRODUCED")
        return 0

    print("R195C: no visibility miss this run"
          + " (" + String(ROUNDS) + " rounds, 4 lock crossings each,"
          + " full field cluster touched each round)")
    return 0


def main() raises:
    var rc: Int
    try:
        rc = run_scenario()
    except e:
        print("R195 cluster visibility repro: driver raised " + String(e))
        rc = 2
    _iso_exit(Int32(rc))
