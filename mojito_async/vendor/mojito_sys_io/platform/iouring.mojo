# mojito_async/vendor/mojito_sys_io/platform/iouring.mojo
#
# A7.1 reactor lane (issue #75) — the EXPERIMENTAL completion-poller
# adapter (ADR-SYS-009 readiness-vs-completion normalization: readiness
# is the shipped path, completion (isotonic io_uring) stays a flagged
# experimental adapter behind the same public reactor surface and the
# propagation flag `MOJITO_IO_URING`).
#
# This file is NEW (not vendored) but follows the EXACT same b2 marshaling
# discipline as the sibling platform/kqueue.mojo and platform/epoll.mojo
# adapters (leaf @extern probes in externs.mojo, decode/raise only after
# each call returns, wait() batches via one straight-line probe + memcpy)
# over the s6-ioring bindings vendor/mojito_sys_io/externs.mojo already
# carries (mirroring mojito-sys upstream's own mjs_iouring_* leaf shims,
# which cover this backend too).
#
# Capability gate (spec §28, ADR-SYS-009): a ring is instantiated ONLY when
# the host kernel supports io_uring AND the explicit environment flag
# MOJITO_IO_URING=1 is set — mjs_iouring_available() (vendor/mojito-sys/
# mjs_iouring.c) is the authoritative predicate and mjs_iouring_create()
# itself returns -ENOSYS whenever it is false, so this Mojo wrapper does
# NOT re-implement the flag check: create() calls straight through and
# decodes whatever the C layer reports. On every host WITHOUT __linux__
# (this Darwin build host included) native/posix/mjs_iouring.c's
# detect-and-exclude stub returns -ENOSYS unconditionally regardless of
# the flag, which is exactly the acceptance contract reactor/poller.mojo's
# create_completion_poller() exercises: "unreachable without the flag,
# raises a decoded error."
#
# Blocking (SYS-5): ONLY wait() parks its OS thread (bounded by its
# timeout); probe/available/register/modify/unregister/wake/close never
# block. Allocation (SYS-4): comptime-sized stack scratch per call.

from std.memory import Span, UnsafePointer, stack_allocation, memcpy

import mojito_async.vendor.mojito_sys_io.externs as _externs
from mojito_async.vendor.mojito_sys_io.errors import raise_errno
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoEvent, IoInterest

comptime EINVAL_RC = Int32(-22)
comptime EINTR_RC = Int32(-4)

comptime MAX_BATCH = 256

comptime WAIT_OK = UInt8(0)
comptime WAIT_INTERRUPTED = UInt8(1)
comptime WAIT_ERROR = UInt8(2)


def classify_wait_rc(rc: Int32) -> UInt8:
    """Classify a raw mjs_iouring_wait rc: OK (count valid), INTERRUPTED
    (-EINTR: caller retries), or ERROR. Pure function; never blocks."""
    if rc == 0:
        return WAIT_OK
    if rc == EINTR_RC:
        return WAIT_INTERRUPTED
    return WAIT_ERROR


def _decode_rc(rc: Int32) raises:
    if rc != 0:
        raise_errno(rc)


def _timeout_slot(
    timeout_ns: Optional[UInt64],
    cell: UnsafePointer[UInt64, MutAnyOrigin],
) -> _externs.TimeoutSlot:
    var zero = 0
    var slot = _externs.TimeoutSlot(unsafe_from_address=zero)
    var dl = timeout_ns
    if dl:
        cell[] = dl.take()
        slot = cell
    return slot


def iouring_probe() -> Bool:
    """True when the host KERNEL supports io_uring at all (independent of
    the MOJITO_IO_URING flag).  Never blocks, never raises."""
    return _externs.probe_uring_probe() != 0


def iouring_available() -> Bool:
    """The AUTHORITATIVE capability predicate (ADR-SYS-009): True only
    when the host supports io_uring AND MOJITO_IO_URING=1 is set.  Mirrors
    exactly what create() will succeed/fail with — callers that want to
    know "will create() work" without paying for a ring should call this
    first. Never blocks, never raises."""
    return _externs.probe_uring_available() != 0


struct IoUringPoller(Movable):
    """The experimental completion-backend adapter (ADR-SYS-009), same
    ReadinessPoller-shaped method surface as KqueuePoller/EpollPoller so
    reactor/poller.mojo's NativePoller can dispatch to it identically.

    NON-OWNING of descriptors (spec §25); close() retires the ring only.
    Tokens ride through the ring's user_data verbatim (§31).

    Blocking (SYS-5): wait() parks its caller bounded by the timeout;
    every other member never blocks. Allocation (SYS-4): stack scratch
    per call plus the ring's own fixed pages (owned by the C layer).
    """

    var ptr: _externs.UringPtr
    var closed: Bool

    def __init__(out self):
        var zero = 0
        self.ptr = _externs.UringPtr(unsafe_from_address=zero)
        self.closed = True


    @staticmethod
    def create() raises -> IoUringPoller:
        """Create an io_uring-backed completion poller.  Raises a decoded
        -ENOSYS (via raise_errno) whenever the host lacks io_uring OR the
        MOJITO_IO_URING=1 capability flag is unset — the C layer
        (mjs_iouring_create) is the single source of truth for that
        decision; this wrapper never re-implements the gate."""
        var slot = stack_allocation[1, _externs.UringPtr]()
        var rc = _externs.probe_uring_create(slot)
        _decode_rc(rc)
        return IoUringPoller._adopt(slot[0])

    @staticmethod
    def _adopt(handle: _externs.UringPtr) -> IoUringPoller:
        var p = IoUringPoller()
        p.ptr = handle
        p.closed = False
        return p^

    def handle_ptr(self) -> _externs.UringPtr:
        return self.ptr

    def is_closed(self) -> Bool:
        return self.closed

    # ---- readiness-poller-shaped surface ----------------------------------

    def register(
        mut self, handle: NativeIoHandle, interests: IoInterest, token: UInt64
    ) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_uring_register(
            self.ptr, handle.get(), Int32(interests.bits), token
        )
        _decode_rc(rc)

    def modify(
        mut self, handle: NativeIoHandle, interests: IoInterest, token: UInt64
    ) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_uring_modify(
            self.ptr, handle.get(), Int32(interests.bits), token
        )
        _decode_rc(rc)

    def unregister(mut self, handle: NativeIoHandle) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_uring_unregister(self.ptr, handle.get())
        _decode_rc(rc)

    def wake(mut self) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_uring_wake(self.ptr)
        _decode_rc(rc)

    def unregister_raw(mut self, handle: NativeIoHandle) -> Int32:
        if self.closed:
            return EINVAL_RC
        return _externs.probe_uring_unregister(self.ptr, handle.get())

    def close(mut self) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var slot = stack_allocation[1, _externs.UringPtr]()
        slot[0] = self.ptr
        var rc = _externs.probe_uring_close(slot)
        _decode_rc(rc)
        var zero = 0
        self.ptr = _externs.UringPtr(unsafe_from_address=zero)
        self.closed = True

    def wait(
        mut self,
        events: Span[IoEvent, MutAnyOrigin],
        timeout_ns: Optional[UInt64],
    ) raises -> Int:
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
        var rc = _externs.probe_uring_wait(
            self.ptr, bp, Int32(cap), tslot, ncell
        )
        if rc != 0:
            raise_errno(rc)
        var count = Int(ncell[])
        if count > 0:
            var dst = UnsafePointer[IoEvent, MutAnyOrigin](
                unsafe_from_address=Int(events.unsafe_ptr())
            )
            var srcp = UnsafePointer[IoEvent, MutAnyOrigin](
                unsafe_from_address=Int(buf)
            )
            memcpy(dest=dst, src=srcp, count=count)
        return count
