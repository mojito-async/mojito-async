# mojito_async/runtime/idle.mojo
#
# A2.2 M2-partial (PR #107 fold; issues #68/#112) — the idle-accounting
# ("acct") block seam behind the PRODUCER-SIDE WAKE BUDGET.  The E6 lane
# (issue #72) allocates the 40-byte acct block on the pool heap and arms
# each worker's runtime with it (Runtime.arm_acct); THIS lane establishes
# the seam's producer side — announce_work / complete_work — plus the
# guarded readers the E6 worker loop consumes.  The M2-partial fold wires
# the announce calls into the runtime enqueue paths (enqueue_local /
# push_remote); the SIGNAL — wake_one — lands in #112 (every wired call
# site carries the `# #112-OWNED: wake_one call` mark).
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
# WAKE-BUDGET CONTRACT (producer side; the OS event is the E6 lane's):
#   announce_work(acct, K)  — a producer injected K ACCEPTED units;
#   wake_one(acct)          — up to K signals, each fired ONLY IF
#                             acct_parked > 0 (never burn a signal into
#                             nobody) — the signal itself is #112-OWNED;
#   complete_work(acct, K)  — the drain side completed K announced units.
#   An announce without its matching complete keeps pending_work > 0 and
#   wedges E6 idle workers (the pre-park re-check always reports work).
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


# --- producers / drain side ------------------------------------------------

def announce_work(acct: BytePtr, delta: Int):
    """A producer injected `delta` ACCEPTED work units (submit): bump the
    announced-work count.  The caller then signals wake_one up to `delta`
    times — the signal itself is #112-OWNED; this fold only wires the
    announce (every call site is guarded by the runtime's optional acct
    pointer, address-1 sentinel)."""
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_PENDING_OFF), Int64(delta)
    )


def complete_work(acct: BytePtr, delta: Int):
    """The drain side completed `delta` announced units (complete): release
    the announced budget.  MUST balance every accepted announce_work — a
    missing complete keeps pending > 0 forever and wedges E6 idle workers."""
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        _acct_cell(acct, ACCT_PENDING_OFF), -Int64(delta)
    )