# mojito_async/vendor/mojito_sys_io/platform/epoll.mojo
#
# A7.1 reactor lane (issue #75) — adapted from mojito-sys
# `mojito_sys/io/platform/epoll.mojo` @ commit
# 0bfb5291b698c833b302feb35c5724d44961f6d5 (see
# vendor/mojito-sys/VENDORED_AT_S6.txt).  Same two deliberate edits as
# platform/kqueue.mojo in this package: the `ReadinessPoller` trait
# conformance is dropped (house rule: no generic dispatch, `def` switch on
# a concrete kind instead — reactor/poller.mojo's NativePoller), and
# `wait()`'s timeout crosses as `Optional[UInt64]` nanoseconds instead of
# `mojito_sys.time.duration.Duration` (this codebase's one Duration type is
# `mojito_async.time.deadline.Duration`; reactor/poller.mojo converts at
# the call boundary).  UNTESTED on this (Darwin) build host — mjs_epoll.c
# is a detect-and-exclude -ENOSYS stub here (see vendor/mojito-sys/
# mjs_epoll.c); create() below also raises an explicit unsupported-
# platform error before ever reaching the C layer, matching upstream.
#
# PLATFORM GATE: epoll is LINUX-ONLY (spec §28). On a non-Linux host
# (macOS/BSD, where the kqueue lane's KqueuePoller is the backend)
# create() raises an EXPLICIT unsupported-platform error.
#
# ReadinessPoller-shaped implementation over the frozen mjs_epoll_* C ABI
# (vendor/mojito-sys/include/mojito_sys.h, s6-epoll block;
# vendor/mojito-sys/mjs_epoll.c).  See platform/kqueue.mojo's header for
# the full b2 marshaling-discipline rationale (verbatim here too).
#
# Blocking (SYS-5): wait() parks its caller bounded by the timeout;
# every other member never blocks. Allocation (SYS-4): stack scratch
# per call. Task-aware: no.

from std.memory import Span, UnsafePointer, stack_allocation, memcpy
from std.sys import CompilationTarget

import mojito_async.vendor.mojito_sys_io.externs as _externs
from mojito_async.vendor.mojito_sys_io.errors import raise_errno
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoEvent, IoInterest

# Deterministic consumed/misuse codes (frozen ABI: -errno). EINVAL spells
# 22 on darwin AND Linux; EINTR is 4 on both.
comptime EINVAL_RC = Int32(-22)
comptime EINTR_RC = Int32(-4)
comptime ENOSYS_DARWIN = Int32(-78)
comptime ENOSYS_LINUX = Int32(-38)

# One wait's C-side batch ceiling (contract: beyond-cap deliveries are
# dropped; callers loop). COMPTIME-sized so the wait path allocates only
# stack scratch.
comptime MAX_BATCH = 256

comptime WAIT_OK = UInt8(0)
comptime WAIT_INTERRUPTED = UInt8(1)
comptime WAIT_ERROR = UInt8(2)


def classify_wait_rc(rc: Int32) -> UInt8:
    """Classify a raw mjs_epoll_wait rc: OK (count valid), INTERRUPTED
    (-EINTR: caller retries), or ERROR. Pure function; never blocks."""
    if rc == 0:
        return WAIT_OK
    if rc == EINTR_RC:
        return WAIT_INTERRUPTED
    return WAIT_ERROR


def _enotsys_rc() -> Int32:
    """-ENOSYS for the CURRENT host (78 darwin / 38 Linux)."""
    if CompilationTarget().is_linux():
        return ENOSYS_LINUX
    return ENOSYS_DARWIN


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


struct EpollPoller(Movable):
    """ReadinessPoller-shaped adapter over epoll (spec §28), Linux only.

    The poller is NON-OWNING of descriptors: registered fds are borrowed
    (spec §25); close() retires the epoll, never the fds. Tokens ride
    through epoll_event.data.u64 verbatim (§31 MUST preserve accurately).

    Trigger doctrine (§38.7 epoll-specific): LEVEL-triggered default — a
    ready fd reports on every wait until drained.

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
    def create() raises -> EpollPoller:
        """Create an epoll-backed poller (wake eventfd pre-registered).
        On a non-Linux host raises an EXPLICIT unsupported-platform
        error (decoded -ENOSYS) before ever reaching the C layer."""
        if not CompilationTarget().is_linux():
            raise_errno(_enotsys_rc())
        var slot = stack_allocation[1, _externs.PollerPtr]()
        var rc = _externs.probe_epoll_create(slot)
        _decode_rc(rc)
        return EpollPoller._adopt(slot[0])

    @staticmethod
    def _adopt(handle: _externs.PollerPtr) -> EpollPoller:
        var p = EpollPoller()
        p.ptr = handle
        p.closed = False
        return p^

    def handle_ptr(self) -> _externs.PollerPtr:
        return self.ptr

    def is_closed(self) -> Bool:
        return self.closed

    # ---- readiness-poller surface (spec §27.1) ---------------------------

    def register(
        mut self, handle: NativeIoHandle, interests: IoInterest, token: UInt64
    ) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_epoll_register(
            self.ptr, handle.get(), Int32(interests.bits), token
        )
        _decode_rc(rc)

    def modify(
        mut self, handle: NativeIoHandle, interests: IoInterest, token: UInt64
    ) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_epoll_modify(
            self.ptr, handle.get(), Int32(interests.bits), token
        )
        _decode_rc(rc)

    def unregister(mut self, handle: NativeIoHandle) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_epoll_unregister(self.ptr, handle.get())
        _decode_rc(rc)

    def wake(mut self) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_epoll_wake(self.ptr)
        _decode_rc(rc)

    def unregister_raw(mut self, handle: NativeIoHandle) -> Int32:
        if self.closed:
            return EINVAL_RC
        return _externs.probe_epoll_unregister(self.ptr, handle.get())

    def close(mut self) raises:
        if self.closed:
            raise_errno(EINVAL_RC)
        var slot = stack_allocation[1, _externs.PollerPtr]()
        slot[0] = self.ptr
        var rc = _externs.probe_epoll_close(slot)
        _decode_rc(rc)
        var zero = 0
        self.ptr = _externs.PollerPtr(unsafe_from_address=zero)
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
        var rc = _externs.probe_epoll_wait(
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
