# mojito_async/net/tcp_listener.mojo
#
# A7.4 (issue #78) — direct-style `TcpListener.accept()` over the same
# `register_and_park` shape as `TcpStream.connect` (net/tcp_stream.mojo,
# issue #77; spec §48). On `accept_nonblocking` WouldBlock the accepting
# fiber registers READABLE interest over the A7.1/A7.2 reactor (issues
# #75/#76) and parks; re-running `accept_nonblocking` on wake — so an
# idle listener parks instead of tying down a scheduler worker.
#
# Task-body redrive: `accept_current` is a single attempt per call, driven
# from the caller's dispatcher exactly like `connect_current`
# (tcp_stream.mojo) — see that module's docblock for the full house
# pattern. Unlike connect, accept's syscall (`accept_nonblocking`) is
# naturally idempotent/retryable on every call (first attempt or redrive
# after wake): there is no separate "decode the outcome" step, so
# `accept_current` does not need a stream-shaped state marker — it always
# runs unregister-if-pending -> try -> {ready | would_block -> register +
# park | error}. A spurious/overflow wake that re-observes WouldBlock
# naturally falls through to re-register + re-park (issue #78 plan step 3)
# with no special-casing and no exception storm.
#
# b2 note: `accept_current` fills a caller-owned `mut out_stream:
# TcpStream` cell rather than returning `Optional[TcpStream]` — b2 1.0.0b2
# cannot lower a generic parameterized over a Movable (non-copyable)
# payload (documented in vendor/mojito_sys_io/socket.mojo's IoAttempt
# docblock: the same reason `IoAttempt` ships as one concrete carrier
# instead of a parameterized type). Returns Bool instead; True means
# `out_stream` now holds the accepted, connected TcpStream.
#
# Listener-close-while-pending (issue #78 point 5 / acceptance: "on
# listener drop the pending accept fires with a cancellation/closed error
# and no fd leaks"): `cancellation.mojo`'s CancellationToken is
# COOPERATIVE (a parked task only observes it at its own checkpoint, which
# a WAITING task never reaches), so it cannot by itself wake a parked
# acceptor. `close_current` instead unregisters the pending op and calls
# `unpark_current` DIRECTLY on the stored waiter — the same raw wake
# primitive every other primitive in this codebase uses (mirrors
# `Mutex.unlock`'s FIFO handoff, single-waiter here since one listener has
# one accepting task looping `accept_current`). The woken redrive observes
# `_closed` and raises a decoded "listener closed" error before touching
# the now-dead descriptor — never a leaked registration, never a stranded
# waiter.
from mojito_async.reactor import IoOpKind, IoToken, Reactor
from mojito_async.runtime.join_handle import JoinHandle
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.vendor.mojito_sys_io.errors import raise_errno
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoInterest
from mojito_async.vendor.mojito_sys_io.socket import NativeSocket, SocketAddress
from mojito_async.net.tcp_stream import TcpStream


struct TcpListener(Movable):
    """A listening, non-blocking TCP socket (spec §48). Owns exactly one
    descriptor; move (`^`) transfers it; `close()`/`close_current()`
    release it exactly once (spec §25 ownership family)."""

    var _sock: NativeSocket
    # A7.4 pending-accept state, carried across the park/redrive task-body
    # phases (module docblock). `_token`/`_waiter_tcb`/`_waiter_id` are
    # only meaningful while `_pending` is True.
    var _pending: Bool
    var _token: IoToken
    var _waiter_tcb: Int
    var _waiter_id: Int
    # Set by close_current(); a redrive after a close-while-pending wake
    # raises instead of touching the dead descriptor.
    var _closed: Bool

    def __init__(out self, var sock: NativeSocket):
        self._sock = sock^
        self._pending = False
        self._token = IoToken(0, 0, IoOpKind.NONE)
        self._waiter_tcb = 0
        self._waiter_id = 0
        self._closed = False

    def __moveinit__(mut self, mut existing: Self):
        self._sock = existing._sock^
        self._pending = existing._pending
        self._token = existing._token
        self._waiter_tcb = existing._waiter_tcb
        self._waiter_id = existing._waiter_id
        self._closed = existing._closed

    def fd(self) -> Int32:
        return self._sock.get()

    def as_handle(self) -> NativeIoHandle:
        """Non-owning view for reactor registration (spec §25 borrow rule:
        the reactor never closes a registered descriptor)."""
        return NativeIoHandle(self._sock.get())

    def is_pending(self) -> Bool:
        """Diagnostics: True while an accept() slow path is parked awaiting
        read-readiness (the issue #78 park-count acceptance assertion keys
        on this rather than reaching into the reactor/TCB)."""
        return self._pending

    def close(mut self) raises:
        """Close with no accept pending. Raises if an accept IS pending —
        a bare close() has no reactor/task context to wake the parked
        waiter and would otherwise strand it forever; call
        close_current(rt, reactor) instead (module docblock)."""
        if self._pending:
            raise Error(
                "TcpListener.close: an accept is pending; call "
                "close_current(rt, reactor) so the parked waiter is woken"
            )
        self._closed = True
        self._sock.close()

    def close_current[R: ResultValue](
        mut self, mut rt: Runtime, mut reactor: Reactor
    ) raises:
        """Close the listener, waking any parked accept with a decoded
        "listener closed" error instead of leaking the registration or
        stranding the waiter (issue #78 point 5)."""
        self._closed = True
        if self._pending:
            self._pending = False
            reactor.unregister(self._token)
            var waiter = _listener_waiter_handle[R](
                self._waiter_tcb, self._waiter_id
            )
            unpark_current(rt, waiter)
        self._sock.close()


def _listener_waiter_handle[R: ResultValue](
    tcb_addr: Int, tid: Int
) -> JoinHandle[R]:
    """Reconstruct the parked acceptor's one-shot handle from the stored
    (tcb_addr, id) — same shape as sync/mutex.mojo's `_waiter_handle`."""
    return JoinHandle[R](
        UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=tcb_addr
        ),
        tid,
    )


def bind_and_listen(address: SocketAddress, backlog: Int) raises -> TcpListener:
    """Module factory (b2 forbids `@staticmethod`-as-primary-construction;
    matches `create_tcp_stream` (net/tcp_stream.mojo), `scope.mojo`'s
    `make_nested_scope`): a listening, non-blocking TCP/IPv4 socket bound
    to `address` with SO_REUSEADDR set (issue #78 deliverable — the A7.4
    mjs_socket_set_reuseaddr deviation, mojito_async/vendor/mojito-sys/
    mjs_socket.c) so a restarted listener can rebind immediately.
    `backlog` pending connections queue in the kernel; `accept_current`
    drains them one at a time."""
    var sock = NativeSocket.tcp_v4()
    sock.set_nonblocking(True)
    sock.set_reuseaddr(True)
    sock.bind(address)
    sock.listen(backlog)
    return TcpListener(sock^)


def accept_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    mut reactor: Reactor,
    mut listener: TcpListener,
    mut out_stream: TcpStream,
) raises -> Bool:
    """Park-integrated accept (issue #78). ONE attempt per call — see the
    module docblock for the task-body redrive contract and the b2 rationale
    for the `out_stream` out-param.

    Returns True and fills `out_stream` with the accepted, connected
    `TcpStream` once a peer is available (first-call immediate success —
    the kernel already queued a connection — or a redrive after wake).
    Returns False after registering read-interest and parking; the
    caller's dispatcher is free to drive other tasks meanwhile — no
    scheduler worker ever blocks on an idle listener (spec L1371 / issue
    #78 acceptance)."""
    if listener._pending:
        listener._pending = False
        reactor.unregister(listener._token)
    if listener._closed:
        raise Error(
            "TcpListener.accept: listener closed while accept was pending"
        )
    var attempt = listener._sock.accept_nonblocking()
    while attempt.is_interrupted():
        # -EINTR (spec §38.11): nothing happened, retry the syscall
        # immediately in THIS call — never a readiness change, never a
        # park.
        attempt = listener._sock.accept_nonblocking()
    if attempt.is_ready():
        var child_fd = attempt.take_ready_fd()
        var child = NativeSocket._adopt(child_fd)
        child.set_nonblocking(True)
        out_stream = TcpStream(child^)
        return True
    if attempt.is_would_block():
        # First attempt WouldBlock, or a spurious/overflow wake that
        # re-observed WouldBlock: register (again) and park (again) — no
        # special-casing, no exception storm (issue #78 plan step 3).
        var token = reactor.register_and_park(
            rt, h, listener.as_handle(), IoInterest.READABLE, IoOpKind.ACCEPT
        )
        listener._token = token
        if h.state() == TaskControlBlock.WAITING:
            listener._pending = True
            listener._waiter_tcb = Int(h.tcb())
            listener._waiter_id = h.id()
            return False
        # Early wake (concurrent cancellation) — see tcp_stream.mojo's
        # connect_current for the identical rationale.
        reactor.unregister(token)
        raise Error(
            "accept_current: an early wake preempted the pending accept "
            "before it parked (concurrent cancellation?)"
        )
    raise_errno(attempt.errno_code())
    # Unreachable: raise_errno always raises; b2 still demands a return on
    # every path of a result-bearing def.
    return False
