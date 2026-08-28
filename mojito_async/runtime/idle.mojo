# mojito_async/runtime/idle.mojo
#
# A2.3 M2-partial + M7-partial (PR #106 fold; issues #69/#72/#112) — the
# idle-accounting ("acct") block seam behind the PRODUCER-SIDE WAKE BUDGET.
# The E6 lane (issue #72) allocates the 40-byte acct block on the pool heap
# and arms each worker's runtime with it (Runtime.arm_acct); THIS lane
# establishes the seam's producer side — announce_work / complete_work (the
# submit()/complete() pair) — plus the guarded readers the E6 worker loop
# consumes, the MUST/NOT protocol table, and the debug pair-mismatch
# detection (M7).  The M2-partial fold wires the announce into
# enqueue_global per ACCEPTED record; the SIGNAL — wake_one — lands in
# #112 (every wired call site carries the `# #112-OWNED: wake_one call`
# mark).
#
# BLOCK LAYOUT (pool-owned heap; 5 x int64 — MUST match the E6 lane's
# offsets exactly):
#   +0   idle_workers         — workers currently parked as sleepers
#   +8   pending_work         — ANNOUNCED work units (announce+ / complete-)
#   +16  park_total           — spec §71 park counter (E6 lane)
#   +24  wake_total           — spec §71 wake counter (E6 lane)
#   +32  spurious_wake_total  — spec §71 spurious-wake counter (E6 lane)
#
# Lock-free discipline: every counter is a SEQUENTIALLY-CONSISTENT int64 on
# the pool-owned heap block; producers and workers touch only atomics
# (never each other's queues), so there is no data race.
#
# ---------------------------------------------------------------------------
# MUST/NOT PROTOCOL TABLE (M7 fold) — the announce/complete balance contract
# ---------------------------------------------------------------------------
#   MUST     announce_work(acct, K) for every K ACCEPTED units, BEFORE any
#            wake signal for them (the E6 pre-park re-check reads pending:
#            a unit that was announced but never signalled still keeps a
#            parked worker from sleeping — safe direction).
#   MUST     complete_work(acct, K) for every accepted announce_work, once
#            per unit, when the drain side actually completed those units
#            (a missing complete leaves pending > 0 forever and wedges E6
#            idle workers in the sleep-yield loop — M7's failure mode).
#   MUST     announce ONLY accepted units — a rejected push (injection
#            capacity, ADR-009) announces NOTHING (per-accepted-record
#            wake budget is never over-spent).
#   MUST     balance the pair exactly: net pending_work == 0 when the
#            queues are quiet and every unit completed.  The counter is
#            SIGNED; a negative value is a pair mismatch.
#   MUST NOT complete_work past the announced balance (drives pending
#            negative — caught in debug builds, see ACCT_PAIR_CHECK).
#   MUST NOT call any acct_* writer with an UNARMED acct (the address-1
#            sentinel, <= 1): readers report quiescent 0; writers on a
#            sentinel are caller misuse (only arm_acct's guard may test it).
#   SHOULD   wake_one at most once per announced unit (the bounded wake
#            budget), and only when acct_parked > 0 — the SIGNAL itself
#            is #112-OWNED.
# ---------------------------------------------------------------------------
#
# Debug pair-mismatch detection (M7): ACCT_PAIR_CHECK (on in debug/JIT
# builds) makes complete_work assert the SIGNED pending counter never goes
# negative — an over-complete (a missing announce or a double complete)
# raises instead of silently unbalancing the budget.  Release embeds that
# accept the one-atomic-load cost may leave it on; embeds that flip it off
# trade the check for the smallest possible complete path.
#
# Mojo 1.0.0b2 (def-only): extern-free; no globals; no static methods.
from std.atomic import Atomic, Ordering
from mojito_async.integration.sys import BytePtr


# --- acct block layout (pool-owned heap; 5 x int64) ------------------------
comptime ACCT_IDLE_OFF = Int(0)      # _idle_workers (seq-cst int64)
comptime ACCT_PENDING_OFF = Int(8)   # _pending_work (announced work units)
comptime ACCT_PARK_OFF = Int(16)     # park_total      (spec §71)
comptime ACCT_WAKE_OFF = Int(24)     # wake_total      (spec §71)
comptime ACCT_SPUR_OFF = Int(32)     # spurious_wake_total (spec §71)
comptime ACCT_BYTES = Int(40)

# M7: debug pair-mismatch detection (signed pending counter assert).
comptime ACCT_PAIR_CHECK = Bool(True)


def _acct_cell(acct: BytePtr, off: Int) -> UnsafePointer[Int64, MutAnyOrigin]:
    """Address of the int64 atomic at `off` bytes into the acct block."""
    return UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=Int(acct) + off)


# --- guarded readers (the acct readers' pattern) ---------------------------

def _acct_guarded(acct: BytePtr, off: Int) -> Int:
    """Read one acct counter; an UNARMED runtime (the address-1 sentinel,
    `BytePtr(unsafe_from_address=1)`) reports a quiescent 0 instead of
    dereferencing the sentinel."""
    if Int(acct) <= 1:
        return 0
    return Int(
        Atomic[DType.int64].load[ordering=Ordering.SEQUENTIAL](_acct_cell(acct, off))
    )


def acct_parked(acct: BytePtr) -> Int:
    """Workers currently parked as sleepers (a spinning worker never holds
    one of these).  wake_one gates on this: never signal into nobody."""
    return _acct_guarded(acct, ACCT_IDLE_OFF)


def acct_pending(acct: BytePtr) -> Int:
    """Announced (accepted/buffered but not yet drained) work units.  The
    E6 pre-park re-check reads this: pending > 0 means a producer announced
    work the drain side has not completed yet."""
    return _acct_guarded(acct, ACCT_PENDING_OFF)


# --- producers / drain side (the submit()/complete() pair) -----------------

def announce_work(acct: BytePtr, delta: Int):
    """submit(): a producer injected `delta` ACCEPTED work units — bump the
    announced-work count.  The caller then signals wake_one up to `delta`
    times (the bounded wake budget); the SIGNAL itself is #112-OWNED — this
    fold only wires the announce (every call site is guarded by the
    runtime's optional acct pointer, address-1 sentinel)."""
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_PENDING_OFF), Int64(delta)
    )


def complete_work(acct: BytePtr, delta: Int) raises:
    """complete(): the drain side completed `delta` announced units —
    release the announced budget.  MUST balance every accepted
    announce_work exactly once per unit (MUST/NOT table above; a missing
    complete wedges E6 idle workers).  Debug pair-mismatch detection (M7):
    under ACCT_PAIR_CHECK the SIGNED pending counter is asserted after the
    release — an over-complete (prev < delta, i.e. pending went negative)
    or a complete on an unarmed acct raises."""
    if ACCT_PAIR_CHECK and Int(acct) <= 1:
        raise Error(
            "idle.complete_work: pair mismatch — complete on an unarmed acct"
        )
    var prev = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_PENDING_OFF), -Int64(delta)
    )
    if ACCT_PAIR_CHECK and prev < Int64(delta):
        raise Error(
            "idle.complete_work: pair mismatch — pending went negative "
            "(complete past the announced balance)"
        )