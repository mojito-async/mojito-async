# mojito_async/reactor/poller.mojo
#
# A7.1 reactor lane (issue #75) — the platform-pick factories
# (`create_poller`/`create_completion_poller`) and the normalized
# `drain_ready` that maps a filled `IoEvent` slice back to op-table
# entries.
#
# Design decision (documented, matches open question #12; ADR-SYS-009):
# ONE normalized operation state machine (`reactor.reactor.Reactor` +
# `io_op_table.IoOpTable`), TWO adapter shapes underneath.  The reactor and
# every caller above this module see only `IoEvent`/`IoToken`; the
# readiness lane (kqueue today, epoll next) is the shipped path, and the
# completion lane (`create_completion_poller`, isotonic io_uring) is a
# flagged EXPERIMENTAL adapter behind the SAME public reactor surface — the
# `NativePoller` wrapper below exposes an IDENTICAL method set for both, so
# `reactor.reactor.Reactor` never branches on which one it holds.  The
# propagation flag is `MOJITO_IO_URING`, enforced entirely by the vendored
# C layer (vendor/mojito-sys/mjs_iouring.c) and NOT re-implemented here —
# `create_completion_poller()` simply surfaces whatever decoded error
# `IoUringPoller.create()` raises when the flag/host support is absent.
#
# `NativePoller` dispatches every member through a `def` switch on a
# concrete `_kind` tag rather than a `[T: ReadinessPoller]`-constrained
# generic: mojito-sys's own readiness.mojo documents a b2 1.0.0b2 SIGSEGV
# when that exact generic dispatch shape is lowered (the `(Span,
# Optional[Duration])` mut-self `wait` signature is the trigger), and this
# codebase's house rule is "no function-typed struct fields, `def` switch
# on an enum/int kind instead" (see mojito_async/runtime/scheduler.mojo's
# dispatcher-by-value precedent) — so this wrapper sidesteps the bug by
# construction rather than working around it per call site.
#
# Extern-free (b2 discipline, mojito_async/vendor/mojito_sys.mojo's
# header): every @extern call lives in vendor/mojito_sys_io/externs.mojo;
# this module only calls the KqueuePoller/EpollPoller/IoUringPoller wrapper
# methods.
from std.memory import Span
from std.sys import CompilationTarget

from mojito_async.reactor.io_op_table import IO_OP_FREE, IoOpTable
from mojito_async.reactor.io_token import IoToken, decode_token
from mojito_async.time.deadline import Duration
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoEvent, IoInterest
from mojito_async.vendor.mojito_sys_io.platform.epoll import EpollPoller
from mojito_async.vendor.mojito_sys_io.platform.iouring import IoUringPoller
from mojito_async.vendor.mojito_sys_io.platform.kqueue import KqueuePoller

comptime POLLER_KIND_KQUEUE = Int(1)
comptime POLLER_KIND_EPOLL = Int(2)
comptime POLLER_KIND_IOURING = Int(3)


def _duration_to_ns(timeout: Optional[Duration]) -> Optional[UInt64]:
    """Convert this codebase's ONE Duration type
    (`mojito_async.time.deadline.Duration`, A1.4/#36) to the raw
    nanosecond `Optional[UInt64]` the vendored backends take — the FFI-
    adjacent layer stays in raw ns (see vendor/mojito_sys_io/platform/
    kqueue.mojo's header) so no second Duration type is introduced for
    this lane."""
    if timeout:
        return Optional[UInt64](timeout.value().ticks())
    return Optional[UInt64]()


struct NativePoller(Movable):
    """Platform-normalized readiness/completion poller.  Exactly ONE of
    `_kq`/`_ep`/`_ur` is live (selected by `_kind`); the other two sit in
    their default (closed) state and are never touched. register/modify/
    unregister/wake/close/wait all dispatch through the `_kind` switch."""

    var _kind: Int
    var _kq: KqueuePoller
    var _ep: EpollPoller
    var _ur: IoUringPoller

    def __init__(
        out self,
        kind: Int,
        var kq: KqueuePoller,
        var ep: EpollPoller,
        var ur: IoUringPoller,
    ):
        self._kind = kind
        self._kq = kq^
        self._ep = ep^
        self._ur = ur^


    def kind(self) -> Int:
        return self._kind

    def register(
        mut self, handle: NativeIoHandle, interests: IoInterest, token: UInt64
    ) raises:
        if self._kind == POLLER_KIND_KQUEUE:
            self._kq.register(handle, interests, token)
        elif self._kind == POLLER_KIND_EPOLL:
            self._ep.register(handle, interests, token)
        else:
            self._ur.register(handle, interests, token)

    def modify(
        mut self, handle: NativeIoHandle, interests: IoInterest, token: UInt64
    ) raises:
        if self._kind == POLLER_KIND_KQUEUE:
            self._kq.modify(handle, interests, token)
        elif self._kind == POLLER_KIND_EPOLL:
            self._ep.modify(handle, interests, token)
        else:
            self._ur.modify(handle, interests, token)

    def unregister(mut self, handle: NativeIoHandle) raises:
        if self._kind == POLLER_KIND_KQUEUE:
            self._kq.unregister(handle)
        elif self._kind == POLLER_KIND_EPOLL:
            self._ep.unregister(handle)
        else:
            self._ur.unregister(handle)

    def wake(mut self) raises:
        if self._kind == POLLER_KIND_KQUEUE:
            self._kq.wake()
        elif self._kind == POLLER_KIND_EPOLL:
            self._ep.wake()
        else:
            self._ur.wake()

    def close(mut self) raises:
        if self._kind == POLLER_KIND_KQUEUE:
            self._kq.close()
        elif self._kind == POLLER_KIND_EPOLL:
            self._ep.close()
        else:
            self._ur.close()

    def wait(
        mut self, events: Span[IoEvent, MutAnyOrigin], timeout: Optional[Duration]
    ) raises -> Int:
        """Fill `events`; return the count filled (0 on timeout/wake).
        Blocks ONLY this OS thread, bounded by `timeout` (None = block
        indefinitely)."""
        var ns = _duration_to_ns(timeout)
        if self._kind == POLLER_KIND_KQUEUE:
            return self._kq.wait(events, ns)
        elif self._kind == POLLER_KIND_EPOLL:
            return self._ep.wait(events, ns)
        else:
            return self._ur.wait(events, ns)


# ---------------------------------------------------------------------------
# Platform-pick factories (spec §27.1/§28, mirrors A1's CompilationTarget
# platform branch).
# ---------------------------------------------------------------------------


def create_poller() raises -> NativePoller:
    """The SHIPPED readiness-poller factory: kqueue on macOS/BSD, epoll on
    Linux (spec §27.1/§28).  Construction of the losing-platform backends
    uses their default (unopened, `closed`) constructor and they are never
    dispatched to; only the WINNING backend's `.create()` opens a real OS
    handle and can raise (propagated straight through, `raises`)."""
    if CompilationTarget().is_linux():
        return NativePoller(
            POLLER_KIND_EPOLL, KqueuePoller(), EpollPoller.create(), IoUringPoller()
        )
    return NativePoller(
        POLLER_KIND_KQUEUE, KqueuePoller.create(), EpollPoller(), IoUringPoller()
    )


def create_completion_poller() raises -> NativePoller:
    """The EXPERIMENTAL completion-poller factory (ADR-SYS-009): binds
    io_uring behind the `MOJITO_IO_URING` capability flag.  Raises the
    decoded error `IoUringPoller.create()` reports whenever the host lacks
    io_uring OR the flag is unset (vendor/mojito-sys/mjs_iouring.c is the
    single source of truth for that gate) — this is the acceptance
    contract: "Completion backend is unreachable without the flag and
    raises a decoded error" (issue #75)."""
    return NativePoller(
        POLLER_KIND_IOURING, KqueuePoller(), EpollPoller(), IoUringPoller.create()
    )


# ---------------------------------------------------------------------------
# drain_ready — map a filled IoEvent slice back to op-table entries.
# ---------------------------------------------------------------------------


def drain_ready(events: Span[IoEvent, MutAnyOrigin], mut table: IoOpTable) -> List[IoToken]:
    """Map each delivered `IoEvent` (already filled by `NativePoller.wait`)
    back to its op-table slot: decode the wire token, validate it against
    the table (bounds + generation + not-FREE), and transition a LIVE
    match to `IO_OP_READY`.  Returns the tokens (with `op_kind` resolved
    from the table entry, unlike the bare `decode_token` result) for every
    entry that actually became ready.

    A STALE delivery — the decoded slot is out of range, FREE, or its
    generation no longer matches (the slot was released and reused, or
    never allocated at all, since the last time this exact wire token was
    minted) — is PROVABLY DROPPED here: nothing is returned for it, the
    table is untouched, and `reactor.reactor.Reactor.poll` therefore never
    attempts a wake for it (issue #76 acceptance: "a late delivery is
    provably dropped — no enqueue, no wake").  This function is pure
    table-mutation + event mapping: it knows nothing about tasks, parking,
    or wake routing — that is `reactor.reactor.Reactor`'s job, one layer
    up, over the tokens this returns."""
    var ready = List[IoToken]()
    for i in range(len(events)):
        var decoded = decode_token(events[i].token)
        if decoded.slot < 0 or decoded.slot >= IoOpTable.CAPACITY:
            continue
        var e = table.get(decoded.slot)
        if e.state == IO_OP_FREE or e.generation != decoded.generation:
            continue  # stale: slot free, or reused under a newer generation
        e.state = IO_OP_READY
        table.set(decoded.slot, e)
        ready.append(IoToken(decoded.slot, decoded.generation, e.op_kind))
    return ready^
