# mojito_async/net/tcp_stream.mojo
#
# A7.3 (issue #77) — direct-style `TcpStream.connect()` over the reactor's
# recommended `register_and_park` entry point (reactor/reactor.mojo,
# issues #75/#76; spec §43/§47/§48).
#
# `connect(address)` itself has no scheduler context to park on — b2 has
# no global current task (the same constraint documented in
# time/sleep.mojo's `sleep()`/`sleep_until()` stubs) — so the bare surface
# raises a precise context error directing callers to the driven variant,
# `connect_current`, invoked from a dispatcher frame exactly like
# `sleep_current` (time/sleep.mojo) and `Mutex.lock` (sync/mutex.mojo).
#
# Task-body redrive (the b2 dispatcher pattern every park-based primitive
# in this codebase uses): `connect_current` is a single ATTEMPT per call.
# The caller's dispatcher re-invokes it over the SAME `TcpStream` after a
# wake; the stream's own `_connecting` flag distinguishes a fresh attempt
# from a redrive (mirrors `Mutex.lock`'s WAITER_GRANTED marker, except the
# state is operation-specific so it lives on the stream rather than the
# embedded WaitNode).
#
# Slow-path recipe (reactor.mojo's documented CALL CONTRACT):
#   token = reactor.register_and_park(rt, h, handle, interests, kind)
#   # register_and_park composes register_op + the two-phase park kernel
#   # + attach_waiter; it never loses a readiness that races the park.
# On wake, `service_io` (mirroring time/timer_service.service_timers)
# claims the waiter's generation via `unpark_current`; the caller's
# dispatcher redrives `connect_current`, which unregisters the op and
# decodes the outcome via `NativeSocket.connect_error()` — getsockopt
# (SO_ERROR), the A7.3 mjs_socket_connect_error deviation
# (mojito_async/vendor/mojito-sys/mjs_socket.c) the frozen ABI otherwise
# has no way to express.
#
# Ownership (spec §25): the moved `NativeSocket` becomes the `TcpStream`'s
# `_sock`; ONE close (explicit `close()` or the destructor) releases the
# descriptor. `TcpListener.accept_current` (issue #78,
# net/tcp_listener.mojo) constructs a `TcpStream` the SAME way over an
# adopted child descriptor.
from mojito_async.reactor import IoOpKind, IoToken, Reactor
from mojito_async.runtime.join_handle import JoinHandle
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoInterest
from mojito_async.vendor.mojito_sys_io.socket import NativeSocket, SocketAddress


struct TcpStream(Movable):
    """A connected, non-blocking TCP socket (spec §48). Owns exactly one
    descriptor; move (`^`) transfers it; `close()`/the destructor release
    it exactly once (spec §25 ownership family)."""

    var _sock: NativeSocket
    # A7.3 pending-connect state, carried across the park/redrive task-body
    # phases (module docblock). `_token` is only meaningful while
    # `_connecting` is True.
    var _connecting: Bool
    var _token: IoToken

    def __init__(out self, var sock: NativeSocket):
        self._sock = sock^
        self._connecting = False
        self._token = IoToken(0, 0, IoOpKind.NONE)

    def __moveinit__(mut self, mut existing: Self):
        self._sock = existing._sock^
        self._connecting = existing._connecting
        self._token = existing._token

    def fd(self) -> Int32:
        return self._sock.get()

    def as_handle(self) -> NativeIoHandle:
        """Non-owning view for reactor registration (spec §25 borrow rule:
        the reactor never closes a registered descriptor)."""
        return NativeIoHandle(self._sock.get())

    def is_connecting(self) -> Bool:
        """Diagnostics: True while a connect() slow path is parked awaiting
        write-readiness (the issue #77 park-count acceptance assertion
        keys on this rather than reaching into the reactor/TCB)."""
        return self._connecting

    def close(mut self) raises:
        self._sock.close()


def create_tcp_stream() raises -> TcpStream:
    """Module factory (b2 forbids `@staticmethod`-as-primary-construction
    for the house's own higher-level types; matches `bind_and_listen`
    (net/tcp_listener.mojo), `scope.mojo`'s `make_nested_scope`,
    `time/sleep.mojo`'s module-level factories): a fresh non-blocking
    TCP/IPv4 socket wrapped as a `TcpStream`, ready for `connect_current`."""
    var sock = NativeSocket.tcp_v4()
    sock.set_nonblocking(True)
    return TcpStream(sock^)


def connect(address: SocketAddress) raises -> TcpStream:
    """spec §48 direct-style surface — stable signature. A bare call has no
    runtime to park (b2: no global current task; the same constraint as
    `time/sleep.sleep()`), so it raises a precise context error. Use
    `connect_current(rt, h, reactor, stream, address)` from a driven
    dispatcher frame, over a `TcpStream` from `create_tcp_stream()`."""
    raise Error(
        "connect: A7.3 direct-style connect requires a driven scheduler "
        "frame (b2 has no global current task); call "
        "connect_current(rt, h, reactor, stream, address) inside the "
        "dispatcher over a TcpStream from create_tcp_stream()"
    )


def connect_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    mut reactor: Reactor,
    mut stream: TcpStream,
    address: SocketAddress,
) raises -> Bool:
    """Park-integrated connect (issue #77). ONE attempt per call — see the
    module docblock for the task-body redrive contract.

    Returns True once ESTABLISHED (first-call immediate success, or a
    redrive that decoded a successful handshake). Returns False after
    registering write-interest and parking (via `register_and_park`); the
    caller's dispatcher is free to drive other tasks meanwhile — no
    scheduler worker ever blocks on the handshake (spec L1371 / issue #77
    acceptance). Redriving this call over the SAME `stream` after the wake
    decodes the outcome and either returns True or raises the decoded
    errno (e.g. -ECONNREFUSED for a refused connect) for a failed
    handshake."""
    if stream._connecting:
        # Redrive after wake: the pending attempt settled one way or the
        # other. Unregister FIRST (spec §25: never leave a stale op table
        # entry behind) then decode via getsockopt(SO_ERROR).
        stream._connecting = False
        reactor.unregister(stream._token)
        return stream._sock.connect_error()
    if stream._sock.connect(address):
        return True
    # Pending (-EINPROGRESS): register write-interest and park through the
    # reactor's recommended two-phase entry point.
    var token = reactor.register_and_park(
        rt, h, stream.as_handle(), IoInterest.WRITABLE, IoOpKind.CONNECT
    )
    stream._token = token
    if h.state() == TaskControlBlock.WAITING:
        stream._connecting = True
        return False
    # An early wake (e.g. a concurrent cancellation) landed in the
    # PARKING window before this task genuinely parked (register_and_park's
    # own documented branch): the connect attempt itself made no progress,
    # so unregister and surface a decoded error rather than leaving the
    # caller in an inconsistent (RUNNING but "pending") state.
    reactor.unregister(token)
    raise Error(
        "connect_current: an early wake preempted the pending connect "
        "before it parked (concurrent cancellation?); retry connect_current "
        "over a fresh TcpStream"
    )
