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
# Lock-free discipline: every counter is a SEQUENTIALLY-CONSISTENT int64 on
# the pool-owned heap block (acct).  The wake-budget contract (issue #72
# step 3) is enforced by the pool: a producer that injects K units calls
# announce_work(K) then wake_one() up to K times; wake_one signals the event
# ONLY IF acct_parked > 0 (never burns a signal into nobody); breadth-one +
# sticky + coalescing come from the C NativeEvent.
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


# --- acct block layout (pool-owned heap; 5 x int64) ------------------------
comptime ACCT_IDLE_OFF = Int(0)      # _idle_workers (seq-cst int64)
comptime ACCT_PENDING_OFF = Int(8)   # _pending_work (announced work units)
comptime ACCT_PARK_OFF = Int(16)     # park_total      (spec §71)
comptime ACCT_WAKE_OFF = Int(24)     # wake_total      (spec §71)
comptime ACCT_SPUR_OFF = Int(32)     # spurious_wake_total (spec §71)
comptime ACCT_BYTES = Int(40)


def _acct_cell(acct: BytePtr, off: Int) -> UnsafePointer[Int64, MutAnyOrigin]:
    """Address of the int64 atomic at `off` bytes into the acct block."""
    return UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=Int(acct) + off)


# --- readers (pool + embedder side) ----------------------------------------

def acct_parked(acct: BytePtr) -> Int:
    """Number of workers currently parked as sleepers (sequentially the
    pool's idle-worker count; a spinning worker never holds one of these)."""
    return Int(Atomic[DType.int64].load[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_IDLE_OFF)))

def acct_pending(acct: BytePtr) -> Int:
    """Announced (injected but not yet drained) work units."""
    return Int(Atomic[DType.int64].load[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_PENDING_OFF)))

def acct_park_total(acct: BytePtr) -> Int:
    return Int(Atomic[DType.int64].load[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_PARK_OFF)))

def acct_wake_total(acct: BytePtr) -> Int:
    return Int(Atomic[DType.int64].load[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_WAKE_OFF)))

def acct_spurious_total(acct: BytePtr) -> Int:
    return Int(Atomic[DType.int64].load[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_SPUR_OFF)))


# --- producers / embedder side ---------------------------------------------

def announce_work(acct: BytePtr, delta: Int):
    """A producer injected `delta` work units: bump the announced-work count
    (the worker wake re-check / spurious classification reads this)."""
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_PENDING_OFF), delta)

def complete_work(acct: BytePtr, delta: Int):
    """The embedder drained `delta` units (real tasks completed)."""
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, ACCT_PENDING_OFF), -delta)


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
