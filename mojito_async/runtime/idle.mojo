# mojito_async/runtime/idle.mojo
#
# A2.6 (issue #72) — the idle-sleeper accounting over the pool's NativeEvent
# (S3.5).  The pool's heap `acct` block holds the SHARED lock-free atomics:
# the idle-sleeper count, the announced-work count, and the spec §71 counters
# park_total / wake_total / spurious_wake_total.  Worker-side and producer-
# side touch only these atomics (never each other's queues), so there is no
# data race under the a2.2 discipline (the embedder drives real task queues
# from its own thread; workers drive the park mechanism).
#
# The worker-side OS-level park (idle_park_worker) lives here as the API, but
# the abi("C")-reached worker loop calls it via thread_entry.mojo's same-name
# module function (which inlines the identical body at the trampoline module's
# concrete scope, per the b2 compiler-bug workaround documented in
# thread_entry.mojo — deep struct-method / cross-module extern chains from an
# abi("C") def mis-lower on 1.0.0b2).  Both share these ACCT_* offsets and the
# acct_* reader/adder helpers, so the accounting is single-sourced.
#
# CANONICAL COUNTER NAMES (spec §71; M8 — ONE public name per counter, no
# naming maze).  The WorkerPool accessors in worker_pool.mojo are THE public
# names; the acct_* readers below are the raw accounting-block aliases that
# single-source them:
#
#     counter                     WorkerPool accessor   raw reader (here)
#     --------------------------  --------------------  ------------------
#     parked idle sleepers (now)  idle_parked()         acct_parked()
#     announced, undrained units  pending_work()        acct_pending()
#     cumulative OS parks         park_total()          acct_park_total()
#     cumulative token wakes      wake_total()          acct_wake_total()
#     cumulative spurious wakes   spurious_total()      acct_spurious_total()
#
# There is NO other public name for any of these counters.  (The per-worker
# park count is `WorkerPool.idle_parks(i)`, read post-join from the entry
# cell; it counts the SAME park events as park_total, split per worker.)
#
# PRODUCER WAKE PROTOCOL (M7 — MUST/NOT).  The embedder asks for K work units
# to be drained with exactly this handshake:
#
#     MUST  preallocate the target worker queue(s) BEFORE announcing, so the
#           wake path performs NO allocation (a deque growth would allocate
#           in the middle of the signal hand-off; issue #72/M9).
#     MUST  call announce_work(K) exactly once per K units injected.
#     MUST  call wake_one() at most K times after announcing (K units wake at
#           most K sleepers; breadth-one — never more).
#     MUST  call complete_work(K) exactly once when the K units are drained
#           (the pending counter must return to its pre-announce value).
#     NOT   announce without a matching drain: the pending counter leaks and
#           the workers' pre-park re-check finds phantom work forever.
#     NOT   complete more than announced: in debug builds the underflow
#           raises (pair-mismatch detection, below); in release it silently
#           underflows the signed counter and must be caught by the caller.
#     NOT   signal the event when nobody is parked — wake_one() already
#           guards on acct_parked() > 0; wake_one_force() is for shutdown /
#           teardown ONLY.
#
# PAIR-MISMATCH DETECTION (M7): the pending counter is SIGNED, and
# complete_work() checks in debug builds that the completion does not push
# it below the announced floor (old_value - delta >= 0).  A completion that
# would underflow means the embedder drained MORE units than it announced —
# a call-site bookkeeping bug — and the debug build raises instead of
# corrupting the accounting silently.  Release builds skip the check (the
# counter still subtracts; the bug surfaces as a negative pending).
#
# LOCK-FREE DISCIPLINE: every counter is a SEQUENTIALLY-CONSISTENT int64 on
# the pool-owned heap block (acct).  CACHE-LINE SPLIT (M9, issue #72): the
# two HOT counters (idle sleepers + pending work — touched by every park and
# every wake) share one 64-byte line; the three OBSERVABILITY counters
# (park_total / wake_total / spurious_wake_total — read only by drivers/
# benches) live on a second line, so observability reads never false-share
# with the wake path.
#
# Mojo 1.0.0b2 (def-only): no globals, no static methods; externs stay at
# vendor module scope (this module is extern-free except through the vendor
# wrappers).
from std.atomic import Atomic, Ordering
from mojito_async.integration.sys import BytePtr
from mojito_async.vendor.mojito_sys import (
    monotonic_now_ns,
    native_event_wait_until,
)


# --- acct block layout (pool-owned heap; 5 x int64 on 2 cache lines) -------
comptime ACCT_IDLE_OFF = Int(0)      # _idle_workers (seq-cst int64)  [line 1]
comptime ACCT_PENDING_OFF = Int(8)   # _pending_work (announced units) [line 1]
comptime ACCT_PARK_OFF = Int(64)     # park_total (spec §71)          [line 2]
comptime ACCT_WAKE_OFF = Int(72)     # wake_total (spec §71)          [line 2]
comptime ACCT_SPUR_OFF = Int(80)     # spurious_wake_total (spec §71) [line 2]
comptime ACCT_BYTES = Int(128)

# Debug-build pair-mismatch detection (M7): True in debug/CI builds, False in
# release.  When True, complete_work() below the announced floor raises.
comptime IDLE_PAIR_ASSERT = True

def _acct_cell(acct: BytePtr, off: Int) -> UnsafePointer[Int64, MutAnyOrigin]:
    """Address of the int64 atomic at `off` bytes into the acct block."""
    return UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=Int(acct) + off)


# --- readers (pool + embedder side) ----------------------------------------

def _acct_guarded(acct: BytePtr, off: Int) -> Int:
    """Read one acct counter; an unarmed pool (the address-1 sentinel — the
    accounting block is allocated at construction now) reports a quiescent 0
    instead of dereferencing the sentinel."""
    if Int(acct) <= 1:
        return 0
    return Int(Atomic[DType.int64].load[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, off)
    ))


def acct_parked(acct: BytePtr) -> Int:
    """Raw reader for the CANONICAL `idle_parked` counter: number of workers
    currently parked as sleepers (sequentially the pool's idle-worker count;
    a spinning worker never holds one of these)."""
    return _acct_guarded(acct, ACCT_IDLE_OFF)


def acct_pending(acct: BytePtr) -> Int:
    """Raw reader for the CANONICAL `pending_work` counter: announced
    (injected but not yet drained) work units."""
    return _acct_guarded(acct, ACCT_PENDING_OFF)


def acct_park_total(acct: BytePtr) -> Int:
    """Raw reader for the CANONICAL `park_total` counter (spec §71)."""
    return _acct_guarded(acct, ACCT_PARK_OFF)


def acct_wake_total(acct: BytePtr) -> Int:
    """Raw reader for the CANONICAL `wake_total` counter (spec §71)."""
    return _acct_guarded(acct, ACCT_WAKE_OFF)


def acct_spurious_total(acct: BytePtr) -> Int:
    """Raw reader for the CANONICAL `spurious_total` counter (spec §71)."""
    return _acct_guarded(acct, ACCT_SPUR_OFF)


def acct_reset(acct: BytePtr):
    """Zero all five counters (each pool start() re-arms).  Owns the layout:
    the called must NOT iterate the block linearly — the observability
    counters live on a second cache line (M9)."""
    Atomic[DType.int64].store[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_IDLE_OFF), 0
    )
    Atomic[DType.int64].store[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_PENDING_OFF), 0
    )
    Atomic[DType.int64].store[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_PARK_OFF), 0
    )
    Atomic[DType.int64].store[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_WAKE_OFF), 0
    )
    Atomic[DType.int64].store[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_SPUR_OFF), 0
    )


# --- producers / embedder side ---------------------------------------------

def announce_work(acct: BytePtr, delta: Int):
    """MUST pair with complete_work + at most `delta` wake_one() calls (the
    producer wake protocol, module header).  A producer injected `delta`
    work units: bump the announced-work count (the worker wake re-check /
    spurious classification reads this)."""
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_PENDING_OFF), delta)

def complete_work(acct: BytePtr, delta: Int) raises:
    """MUST pair with a prior announce_work (the producer wake protocol,
    module header): the embedder drained `delta` units (real tasks
    completed), so the pending counter drops by `delta`.

    PAIR-MISMATCH DETECTION (debug, comptime IDLE_PAIR_ASSERT): the signed
    pending counter must never drop below the announced floor — completing
    more units than were announced is a call-site bookkeeping bug and RAISES
    in debug builds instead of underflowing silently.  Release builds skip
    the check (the subtraction still runs)."""
    var old = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_PENDING_OFF), -delta
    )
    if IDLE_PAIR_ASSERT and old - delta < 0:
        raise Error(
            "idle.complete_work: pair mismatch — completed "
            + String(delta) + " unit(s) below the announced floor (pending "
            + "would drop under 0); every complete_work must pair with an "
            + "earlier announce_work"
        )


# --- worker-side counters (module helpers, extern-free) ---------------------

def idle_join(acct: BytePtr) -> Int:
    """Commit THIS worker as an idle sleeper (returns the prior sleeper
    count).  SEQ_CST so a producer's announce_work + wake ordering holds."""
    return Int(Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_IDLE_OFF), 1))

def idle_leave(acct: BytePtr):
    """This worker is no longer a parked sleeper (woken / withdrew)."""
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_IDLE_OFF), -1)

def note_park(acct: BytePtr):
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_PARK_OFF), 1)

def note_wake(acct: BytePtr):
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_WAKE_OFF), 1)

def note_spurious(acct: BytePtr):
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_SPUR_OFF), 1)


def idle_park_worker(acct: BytePtr, event: Int, deadline_ns: UInt) -> Bool:
    """The OS-level idle park of one worker (issue #72 step 2/step 5):

      1. commit as a sleeper (idle_join);
      2. RE-CHECK for announced work immediately before sleeping — the
         lost-wakeup guard (work landed between the last pop and here); if
         present, withdraw and report True (the caller does not sleep and
         loops — the embedder drains the real tasks);
      3. park on the pool NativeEvent with an absolute CLOCK_MONOTONIC
         deadline slice (wait_until consumes a token; the C predicate loop
         means ok ONLY on a real token — no fake spurious ready, spec §17);
      4. leave the sleeper set; on a consumed token classify it: productive
         when announced work remains, spurious otherwise (spurious_wake_total).

    Returns True when the caller should NOT sleep (announced work found at
    the pre-park re-check); False after a real park (woken by token or the
    deadline slice elapsed — the caller re-checks the latch and re-parks).
    """
    idle_join(acct)
    if acct_pending(acct) > 0:
        idle_leave(acct)
        return True
    note_park(acct)
    var consumed = native_event_wait_until(event, deadline_ns)
    idle_leave(acct)
    if consumed:
        note_wake(acct)
        if acct_pending(acct) == 0:
            note_spurious(acct)
    return False
