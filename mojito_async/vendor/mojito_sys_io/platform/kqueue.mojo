# mojito_async/vendor/mojito_sys_io/platform/kqueue.mojo
#
# A7.1 reactor lane (issue #75) — adapted from mojito-sys
# `mojito_sys/io/platform/kqueue.mojo` @ commit
# 0bfb5291b698c833b302feb35c5724d44961f6d5 (see
# vendor/mojito-sys/VENDORED_AT_S6.txt).  Two deliberate edits from
# upstream:
#   1. The `ReadinessPoller` trait conformance is DROPPED (and the trait
#      itself is not vendored): readiness.mojo's own doc block records a
#      b2 SIGSEGV when a `[T: ReadinessPoller]`-constrained generic call is
#      lowered, so this codebase's house rule (no generic dispatch, `def`
#      switch on a concrete kind instead — see reactor/poller.mojo's
#      NativePoller) never needs the trait; KqueuePoller keeps the exact
#      same METHOD SHAPE the trait documents, just without the conformance
#      declaration.
#   2. `wait()`'s `timeout` parameter is a plain `Optional[UInt64]`
#      (nanoseconds) instead of `mojito_sys.time.duration.Duration`: this
#      codebase already has ONE Duration type (`mojito_async.time.
#      deadline.Duration`, A1.4/#36) and the "second convention beside an
#      existing one" duplication this vendoring would otherwise introduce
#      is avoided by staying in raw ns at this low FFI-adjacent layer;
#      reactor/poller.mojo converts at the call boundary.
# Every other line (the marshaling discipline, the exact SIGSEGV
# workarounds) is carried forward VERBATIM — this is proven, working b2 FFI
# code and re-deriving it independently risks re-discovering the same
# lowering bugs from scratch.
#
# ReadinessPoller-shaped implementation over the frozen mjs_poller_* C ABI
# (vendor/mojito-sys/include/mojito_sys.h, s6-poller block;
# vendor/mojito-sys/mjs_poller.c).
#
# DOCUMENTED b2 ADAPTATIONS (mirroring s6-socket / s2-thread lanes,
# verbatim from upstream):
#   - def-only members; construction via @staticmethod create() +
#     _adopt(), because the extern-reaching path must stay non-raising
#     until each rc is decoded;
#   - wait()/recv-style shape (proven b2 form): carve scratch OUTSIDE,
#     ONE straight-line probe call, decode strictly AFTER the call
#     returns. Delivery is a single memcpy of the raw kevent results —
#     the frozen mjs_poll_event layout (token@0, fd@8, events@12) is
#     bit-identical to IoEvent {UInt64, Int32, UInt32} and C writes only
#     the four defined flag bits. Pointer-computing loops or shift|OR
#     load merges sharing a frame with the probe SIGSEGV b2 1.0.0b2's
#     lowering (both reproduced minimized in this lane);
#   - interests cross the FFI as Int32 (bit-preserving; UInt32 scalars
#     are byval-poisoned in b2);
#   - the Optional[UInt64] branch is confined to _timeout_slot, a helper
#     with no extern call (atomic_wait precedent).
#
# EINTR doctrine (§38.11): wait surfaces raw -EINTR as a DECODED raise
# (callers catch-and-retry one layer up); every other failure raises
# too. classify_wait_rc exposes the mapping core for conformance
# simulation. Timeout expiry is success-with-zero events. Wake
# deliveries never occupy event slots but DO end a wait early.
#
# Blocking (SYS-5): ONLY wait() parks its OS thread (bounded by its
# timeout); register/modify/unregister/wake/close never block.
# Allocation (SYS-4): comptime-sized stack scratch per call; nothing
# retained. Task-aware: no — OS-thread granularity per spec §14.

from std.memory import Span, UnsafePointer, stack_allocation, memcpy

import mojito_async.vendor.mojito_sys_io.externs as _externs
from mojito_async.vendor.mojito_sys_io.errors import raise_errno
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoEvent, IoInterest

# Deterministic consumed/misuse codes (frozen ABI: -errno). EINVAL spells
# 22 on darwin AND Linux; EINTR is 4 on both.
comptime EINVAL_RC = Int32(-22)
comptime EINTR_RC = Int32(-4)

# One wait's C-side batch ceiling (contract: beyond-cap deliveries are
# dropped; callers loop). COMPTIME-sized so the wait path allocates only
# stack scratch.
comptime MAX_BATCH = 256

# Wait-status classification of the raw C rc (mapping core made visible
# for §38.7 interrupt/retry conformance).
comptime WAIT_OK = UInt8(0)
comptime WAIT_INTERRUPTED = UInt8(1)
comptime WAIT_ERROR = UInt8(2)


def classify_wait_rc(rc: Int32) -> UInt8:
    """Classify a raw mjs_poller_wait rc: OK (count valid), INTERRUPTED
    (-EINTR: caller retries), or ERROR. Pure function; never blocks."""
    if rc == 0:
        return WAIT_OK
    if rc == EINTR_RC:
        return WAIT_INTERRUPTED
    return WAIT_ERROR


def _decode_rc(rc: Int32) raises:
    # Straight-line raise through the shared helper (H6 doctrine): no
    # String-valued branches anywhere on the path to `raise`.
    if rc != 0:
        raise_errno(rc)


def _timeout_slot(
    timeout_ns: Optional[UInt64],
    cell: UnsafePointer[UInt64, MutAnyOrigin],
) -> _externs.TimeoutSlot:
    # The Optional branch lives HERE, in a helper with no extern call
    # (atomic_wait precedent). NULL slot = infinite wait.
    var zero = 0
    var slot = _externs.TimeoutSlot(unsafe_from_address=zero)
    var dl = timeout_ns
    if dl:
        cell[] = dl.take()
        slot = cell
    return slot


struct KqueuePoller(Movable):
    """ReadinessPoller-shaped adapter over kqueue/kevent (spec §29),
    macOS/BSD only.

    The poller is NON-OWNING of descriptors: registered fds are borrowed
    (spec §25); close() retires the kqueue, never the fds. Tokens ride
    through kevent.udata verbatim (§31 MUST preserve accurately).

    Blocking (SYS-5): wait() parks its caller bounded by the timeout;
    every other member never blocks. Allocation (SYS-4): stack scratch
    per call. Task-aware: no.
    """

    var ptr: _externs.PollerPtr
    var closed: Bool

    def __init__(out self):
        var zero = 0
        self.ptr = _externs.PollerPtr(unsafe_from_address=zero)
        self.closed = True


    @staticmethod
    def create() raises -> KqueuePoller:
        """Create a kqueue-backed poller (wake source pre-registered).
        Raises the decoded errno on failure (-ENOSYS without kqueue)."""
        var slot = stack_allocation[1, _externs.PollerPtr]()
        var rc = _externs.probe_poller_create(slot)
        _decode_rc(rc)
        return KqueuePoller._adopt(slot[0])

    @staticmethod
    def _adopt(handle: _externs.PollerPtr) -> KqueuePoller:
        var p = KqueuePoller()
        p.ptr = handle
        p.closed = False
        return p^

    # The raw opaque C handle (fixture/test plumbing).
    def handle_ptr(self) -> _externs.PollerPtr:
        return self.ptr

    def is_closed(self) -> Bool:
        return self.closed

    # ---- readiness-poller surface (spec §27.1) ---------------------------

    def register(
        mut self, handle: NativeIoHandle, interests: IoInterest, token: UInt64
    ) raises:
        """Start watching `handle`; deliveries carry `token` EXACTLY.
        Re-registration is an upsert (last interests+token win)."""
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_poller_register(
            self.ptr, handle.get(), Int32(interests.bits), token
        )
        _decode_rc(rc)

    def modify(
        mut self, handle: NativeIoHandle, interests: IoInterest, token: UInt64
    ) raises:
        """Change interests/token of an existing registration."""
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_poller_modify(
            self.ptr, handle.get(), Int32(interests.bits), token
        )
        _decode_rc(rc)

    def unregister(mut self, handle: NativeIoHandle) raises:
        """Stop watching `handle`; NEVER closes it. Not-registered
        descriptors degrade to success (no-op), per the frozen header."""
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_poller_unregister(self.ptr, handle.get())
        _decode_rc(rc)

    def wake(mut self) raises:
        """Make at most ONE blocked wait return promptly with zero
        events; sticks for one later wait when idle. Never blocks."""
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_poller_wake(self.ptr)
        _decode_rc(rc)

    # Non-raising variant for bulk teardown paths.
    def unregister_raw(mut self, handle: NativeIoHandle) -> Int32:
        if self.closed:
            return EINVAL_RC
        return _externs.probe_poller_unregister(self.ptr, handle.get())

    def close(mut self) raises:
        """Consume the poller handle; double close raises -EINVAL.
        Registered descriptors are NOT closed (non-owning, §25)."""
        if self.closed:
            raise_errno(EINVAL_RC)
        var slot = stack_allocation[1, _externs.PollerPtr]()
        slot[0] = self.ptr
        var rc = _externs.probe_poller_close(slot)
        _decode_rc(rc)
        var zero = 0
        self.ptr = _externs.PollerPtr(unsafe_from_address=zero)
        self.closed = True

    def wait(
        mut self,
        events: Span[IoEvent, MutAnyOrigin],
        timeout_ns: Optional[UInt64],
    ) raises -> Int:
        """Fill `events` with ready registrations; return how many.

        Blocks THIS OS THREAD only, up to `timeout_ns` (None = until an
        event or wake; Some(0) = non-blocking poll). Timeout expiry is
        SUCCESS with zero. Raw -EINTR raises decoded (caller retries,
        §38.11); any other C failure raises too.
        """
        if self.closed:
            raise_errno(EINVAL_RC)
        var n = len(events)
        if n == 0:
            raise_errno(EINVAL_RC)
        var cap = n if (n < MAX_BATCH) else MAX_BATCH
        var ncell = stack_allocation[1, UInt32]()
        var dl_cell = stack_allocation[1, UInt64]()
        var tslot = _timeout_slot(timeout_ns, dl_cell)
        var buf = stack_allocation[MAX_BATCH * 2, Int64]()
        var bp = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(buf))
        var rc = _externs.probe_poller_wait(
            self.ptr, bp, Int32(cap), tslot, ncell
        )
        if rc != 0:
            raise_errno(rc)
        var count = Int(ncell[])
        if count > 0:
            # Bit-identical layouts: deliver with ONE memcpy (scalar-loop
            # decode shares a frame with the probe and SIGSEGVs b2).
            var dst = UnsafePointer[IoEvent, MutAnyOrigin](
                unsafe_from_address=Int(events.unsafe_ptr())
            )
            var srcp = UnsafePointer[IoEvent, MutAnyOrigin](
                unsafe_from_address=Int(buf)
            )
            memcpy(dest=dst, src=srcp, count=count)
        return count
