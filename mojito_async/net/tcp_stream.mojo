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
#
# A7.5/A7.6 (issues #79/#80) — read/write/write_all/shutdown + cancellation
# and deadline integration, EXTENDING this same struct (the issue #79
# deliverable list literally says "extending G3").  `read`/`write` follow
# the IDENTICAL bare-surface-raises / `_current`-does-the-work split as
# `connect`/`connect_current` above (b2 has no global current task, so a
# park-based op can never be truly colorless at the call-site boundary —
# this is the SAME documented deviation, not a new one).  `_reading`/
# `_writing` are `_connecting`'s siblings: independent pending-op flags
# (a full-duplex stream can have a read AND a write parked at once, unlike
# connect which is one-shot) that distinguish a fresh attempt from a
# redrive.  `_read_waiter_tcb`/`_read_waiter_id` (and the write twins)
# mirror `TcpListener`'s `_waiter_tcb`/`_waiter_id` exactly: they let an
# EXTERNAL caller (a scope's cancellation descent, `close_current` below)
# reach a parked read/write waiter without needing a live `JoinHandle` —
# `reactor/cancel.mojo`'s `cancel_op`/`cancel_and_close` take a raw
# `(tcb_addr, task_id)` pair for exactly this reason.
#
# read/write loop semantics (issue #79): `IoAttempt.Ready(count)` returns
# the partial count (never loops internally — partial reads/writes are
# normal, callers loop `write_all`-style); `WouldBlock` registers+parks
# via `wait_readable`/`wait_writable`; `Interrupted` retries the syscall
# immediately in the SAME attempt (no readiness change, no park);
# `Closed` (recv EOF only) returns 0 — `IoAttempt`'s own contract already
# guarantees a genuine `Ready(0)` never happens (socket.mojo: "recv EOF
# maps to Closed — never Ready(0)"), so 0 is an UNAMBIGUOUS EOF marker,
# exactly like a POSIX `read()` returning 0; `Error(errno)` raises
# decoded.  `read_current`/`write_current` return -1 as the "parked,
# redrive me" sentinel (never a legal byte count) instead of the
# `connect_current`/`accept_current` family's `Bool` — issue #79's own
# signature is `-> Int`, and a scalar sentinel needs no out-param.
#
# Cancellation/deadline (issue #79 points 4/5, issue #80): mirrors the
# EXISTING split every sync primitive in this codebase already uses
# (`Mutex.lock` vs `lock_cancellable`, `Semaphore.acquire` vs
# `acquire_cancellable`) rather than growing the plain `read_current`/
# `write_current` signature: `read_current_cancellable`/
# `write_current_cancellable` below take an optional `Duration` deadline
# (armed on the SAME timer heap id/tcb as the I/O park, so
# `reactor.cancel.service_io_deadlines` and `Reactor.poll`/`service_io`
# race for the C6 exactly-one-winner claim on the SAME wake — see
# reactor/cancel.mojo's module docblock) and an optional
# `CancellationToken` (checked at the SAME two checkpoints
# `park_cancellable`/`raise_if_cancel_wake` already establish: a pre-park
# `token.checkpoint()` and a post-resume `raise_if_cancel_wake` decode).
# The token's PRE-park check is cooperative (as everywhere else in this
# codebase); an actual MID-wait push-cancel is delivered by an EXTERNAL
# caller invoking `cancel_op` directly against the stream's stored
# `_read_waiter_tcb`/`_read_waiter_id` — this module exposes exactly the
# state a scope's cancellation descent (issue #80 point 4, a later lane)
# needs to do that; it is not this module's job to invent that descent.
from std.memory import Span
from mojito_async.cancellation import CancellationToken
from mojito_async.reactor import IoOpKind, IoToken, Reactor
from mojito_async.reactor.cancel import (
    cancel_and_close,
    is_closed_wake,
    is_timeout_wake,
    raise_if_closed_wake,
    raise_if_timeout_wake,
)
from mojito_async.runtime.join_handle import JoinHandle
from mojito_async.runtime.park import raise_if_cancel_wake
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Duration
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.vendor.mojito_sys_io.errors import raise_errno
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
    # A7.5 (issue #79) pending-read/pending-write state — `_connecting`'s
    # siblings, independent because a full-duplex stream can have BOTH a
    # read and a write parked at once.  `_read_waiter_tcb`/`_read_waiter_id`
    # (and the write twins) are only meaningful while `_reading`/`_writing`
    # is True; they mirror `TcpListener`'s `_waiter_tcb`/`_waiter_id` so an
    # external `close_current`/scope cancel-descent can reach the waiter
    # (module docblock).
    var _reading: Bool
    var _read_token: IoToken
    var _read_waiter_tcb: Int
    var _read_waiter_id: Int
    var _writing: Bool
    var _write_token: IoToken
    var _write_waiter_tcb: Int
    var _write_waiter_id: Int
    # A7.6 (issue #80): set by close_current(); a redrive after a
    # close-while-pending wake raises instead of touching the dead
    # descriptor (mirrors TcpListener's `_closed`).
    var _closed: Bool

    def __init__(out self, var sock: NativeSocket):
        self._sock = sock^
        self._connecting = False
        self._token = IoToken(0, 0, IoOpKind.NONE)
        self._reading = False
        self._read_token = IoToken(0, 0, IoOpKind.NONE)
        self._read_waiter_tcb = 0
        self._read_waiter_id = 0
        self._writing = False
        self._write_token = IoToken(0, 0, IoOpKind.NONE)
        self._write_waiter_tcb = 0
        self._write_waiter_id = 0
        self._closed = False

    def __moveinit__(mut self, mut existing: Self):
        self._sock = existing._sock^
        self._connecting = existing._connecting
        self._token = existing._token
        self._reading = existing._reading
        self._read_token = existing._read_token
        self._read_waiter_tcb = existing._read_waiter_tcb
        self._read_waiter_id = existing._read_waiter_id
        self._writing = existing._writing
        self._write_token = existing._write_token
        self._write_waiter_tcb = existing._write_waiter_tcb
        self._write_waiter_id = existing._write_waiter_id
        self._closed = existing._closed

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
        """Close with no read/write pending. Raises if a read OR a write
        IS pending — a bare close() has no reactor/task context to wake
        the parked waiter(s) and would otherwise strand them forever
        (mirrors TcpListener.close()); call close_current(rt, reactor)
        instead."""
        if self._reading or self._writing:
            raise Error(
                "TcpStream.close: a read or write is pending; call "
                "close_current(rt, reactor) so the parked waiter(s) are woken"
            )
        self._closed = True
        self._sock.close()

    def close_current[R: ResultValue](
        mut self, mut rt: Runtime, mut reactor: Reactor
    ) raises:
        """Close the stream, waking any parked read AND/OR write with a
        decoded ClosedError instead of leaking the registration or
        stranding the waiter (issue #80 deliverable 3: "the reactor's
        disposal hook so TcpStream drops funnels to unregister + close").
        Composes reactor/cancel.mojo's cancel_and_close for each pending
        half independently — a stream can have both a read AND a write
        parked at once (module docblock)."""
        self._closed = True
        if self._reading:
            self._reading = False
            _ = cancel_and_close(
                reactor, rt, self._read_token, self._read_waiter_tcb, self._read_waiter_id
            )
        if self._writing:
            self._writing = False
            _ = cancel_and_close(
                reactor, rt, self._write_token, self._write_waiter_tcb, self._write_waiter_id
            )
        self._sock.close()

    def read(mut self, buffer: Span[UInt8, _]) raises -> Int:
        """spec §48 direct-style surface — stable signature. A bare call
        has no runtime to park (b2: no global current task; the same
        constraint as connect()/sleep()), so it raises a precise context
        error. Use read_current(rt, h, reactor, stream, buffer) from a
        driven dispatcher frame."""
        raise Error(
            "TcpStream.read: A7.5 direct-style read requires a driven "
            "scheduler frame (b2 has no global current task); call "
            "read_current(rt, h, reactor, stream, buffer) inside the "
            "dispatcher"
        )

    def write(mut self, data: Span[UInt8, _]) raises -> Int:
        """Same context note as read() above. Use write_current(rt, h,
        reactor, stream, data)."""
        raise Error(
            "TcpStream.write: A7.5 direct-style write requires a driven "
            "scheduler frame (b2 has no global current task); call "
            "write_current(rt, h, reactor, stream, data) inside the "
            "dispatcher"
        )

    def write_all(mut self, data: Span[UInt8, _]) raises:
        """Same context note as read()/write() above. Use
        write_all_current(rt, h, reactor, stream, data, written)."""
        raise Error(
            "TcpStream.write_all: A7.5 direct-style write_all requires a "
            "driven scheduler frame (b2 has no global current task); call "
            "write_all_current(rt, h, reactor, stream, data, written) "
            "inside the dispatcher"
        )

    def shutdown(mut self, how: Int32) raises:
        """Shut down one/both transfer halves (SHUT_READ/SHUT_WRITE/
        SHUT_BOTH from vendor/mojito_sys_io/socket.mojo). Never parks
        (SYS-5): a plain shutdown(2) syscall, unlike read/write/connect —
        no `_current` variant needed."""
        self._sock.shutdown(how)


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


# ---------------------------------------------------------------------------
# A7.5 (issue #79) — wait_readable / wait_writable: the shared
# register-and-park-once-per-wait helpers read_current/write_current call
# on WouldBlock.  Threaded with full (rt, h, reactor) context per the
# module docblock's documented deviation from the issue's bare `(mut
# self)` pseudocode.
# ---------------------------------------------------------------------------


def wait_readable[R: ResultValue](
    mut rt: Runtime, h: JoinHandle[R], mut reactor: Reactor, mut stream: TcpStream
) raises -> Bool:
    """Register read-interest and park once (reactor.register_and_park).
    Returns True when genuinely parked (stream._reading set, caller
    returns -1 to its own caller and awaits a redrive); False when an
    early wake (concurrent cancel/timeout/close) preempted the park —
    the token is already unregistered in that case, mirroring connect_
    current's early-wake branch."""
    var token = reactor.register_and_park(
        rt, h, stream.as_handle(), IoInterest.READABLE, IoOpKind.READ
    )
    stream._read_token = token
    if h.state() == TaskControlBlock.WAITING:
        stream._reading = True
        stream._read_waiter_tcb = Int(h.tcb())
        stream._read_waiter_id = h.id()
        return True
    reactor.unregister(token)
    return False


def wait_writable[R: ResultValue](
    mut rt: Runtime, h: JoinHandle[R], mut reactor: Reactor, mut stream: TcpStream
) raises -> Bool:
    """Write-interest twin of wait_readable."""
    var token = reactor.register_and_park(
        rt, h, stream.as_handle(), IoInterest.WRITABLE, IoOpKind.WRITE
    )
    stream._write_token = token
    if h.state() == TaskControlBlock.WAITING:
        stream._writing = True
        stream._write_waiter_tcb = Int(h.tcb())
        stream._write_waiter_id = h.id()
        return True
    reactor.unregister(token)
    return False


# ---------------------------------------------------------------------------
# A7.5 (issue #79) — read_current / write_current / write_all_current: the
# park-integrated core.  ONE attempt per call (the house redrive
# contract, see connect_current above); -1 is the "parked, redrive me"
# sentinel (never a legal byte count — IoAttempt's own contract rules out
# a genuine Ready(0), see module docblock).
# ---------------------------------------------------------------------------


def read_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    mut reactor: Reactor,
    mut stream: TcpStream,
    buffer: Span[UInt8, _],
) raises -> Int:
    """Park-integrated read (issue #79). Returns the byte count actually
    received (0 == EOF/Closed) once Ready/Closed; returns -1 after
    registering read-interest and parking — the caller's dispatcher
    redrives this call over the SAME `stream`/`buffer` once woken."""
    if stream._reading:
        stream._reading = False
        reactor.unregister(stream._read_token)
    if stream._closed:
        raise Error(
            "ClosedError: TcpStream closed (close_current) while the read "
            "was pending"
        )
    while True:
        var attempt = stream._sock.recv_nonblocking(buffer)
        if attempt.is_ready():
            return attempt.ready_count()
        if attempt.is_closed():
            return 0
        if attempt.is_interrupted():
            continue
        if attempt.is_would_block():
            if wait_readable(rt, h, reactor, stream):
                return -1
            continue  # early wake resolved without ever parking: retry now
        raise_errno(attempt.errno_code())
    return 0


def write_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    mut reactor: Reactor,
    mut stream: TcpStream,
    data: Span[UInt8, _],
) raises -> Int:
    """Park-integrated write (issue #79). Write twin of read_current: -1
    is the "parked, redrive me" sentinel; otherwise the count the kernel
    ACCEPTED this attempt (possibly a partial prefix of `data` —
    write_all_current below loops over the remainder)."""
    if stream._writing:
        stream._writing = False
        reactor.unregister(stream._write_token)
    if stream._closed:
        raise Error(
            "ClosedError: TcpStream closed (close_current) while the write "
            "was pending"
        )
    while True:
        var attempt = stream._sock.send_nonblocking(data)
        if attempt.is_ready():
            return attempt.ready_count()
        if attempt.is_interrupted():
            continue
        if attempt.is_would_block():
            if wait_writable(rt, h, reactor, stream):
                return -1
            continue
        raise_errno(attempt.errno_code())
    return 0


def write_all_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    mut reactor: Reactor,
    mut stream: TcpStream,
    data: Span[UInt8, _],
    mut written: Int,
) raises -> Bool:
    """write_all (issue #79): folds write_current over `data` until every
    byte is sent. `written` is an IN/OUT redrive cursor — the caller's
    dispatcher passes back the SAME `written` value this call last left
    it at, so a WouldBlock after a partial transfer never re-sends bytes
    the kernel already accepted. Returns True once `written == len(data)`;
    False after a park (redrive with the same `written`)."""
    while written < len(data):
        var n = write_current(rt, h, reactor, stream, data[written : len(data)])
        if n < 0:
            return False
        written += n
    return True


# ---------------------------------------------------------------------------
# A7.5/A7.6 (issues #79 points 4/5, #80) — cancellable + deadline-aware
# entry points, mirroring the Mutex.lock/lock_cancellable split (module
# docblock) rather than growing read_current/write_current's own
# signature.
# ---------------------------------------------------------------------------


def read_current_cancellable[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    mut reactor: Reactor,
    mut stream: TcpStream,
    buffer: Span[UInt8, _],
    mut heap: TimerHeap,
    clock: MonotonicClock,
    deadline: Optional[Duration] = None,
    token: Optional[CancellationToken] = None,
) raises -> Int:
    """Deadline+cancellation-integrated read (issue #79 points 4/5, issue
    #80). Same -1 "parked, redrive me" / >=0 byte-count contract as
    read_current. On EVERY re-entry: a mid-wait CANCEL/TIMEOUT/CLOSED
    winner (stamped by an EXTERNAL cancel_op/service_io_deadlines/
    cancel_and_close call — see module docblock) is decoded FIRST and
    raised (CancellationError/TimeoutError/ClosedError); a pre-park
    already-requested token also raises before ever registering the op
    (park_cancellable's exact contract). A `deadline` is armed on the
    SAME timer-heap id/tcb as the I/O park immediately before parking (the
    sleep_current arm-then-park ordering) so reactor.cancel.
    service_io_deadlines and Reactor.poll/service_io race for the C6
    exactly-one-winner claim on the SAME wake; the still-armed heap entry
    is cleared via heap.cancel(h.id()) on every exit that does NOT park
    (readiness won, or a raise) so a stale deadline can never fire after
    the fact."""
    if stream._reading:
        stream._reading = False
        reactor.unregister(stream._read_token)
        raise_if_cancel_wake(h)
        raise_if_timeout_wake(h)
        raise_if_closed_wake(h)
        # readiness won: fall through to retry the syscall below.
    if stream._closed:
        raise Error(
            "ClosedError: TcpStream closed (close_current) while the read "
            "was pending"
        )
    if token:
        var t = token.value()
        t.checkpoint()
    while True:
        var attempt = stream._sock.recv_nonblocking(buffer)
        if attempt.is_ready():
            _ = heap.cancel(h.id())
            return attempt.ready_count()
        if attempt.is_closed():
            _ = heap.cancel(h.id())
            return 0
        if attempt.is_interrupted():
            continue
        if attempt.is_would_block():
            if deadline:
                _ = heap.arm(h.id(), Int(h.tcb()), clock.now() + deadline.value().ticks())
            if wait_readable(rt, h, reactor, stream):
                return -1
            _ = heap.cancel(h.id())
            raise_if_cancel_wake(h)
            raise_if_timeout_wake(h)
            raise_if_closed_wake(h)
            continue
        _ = heap.cancel(h.id())
        raise_errno(attempt.errno_code())
    return 0


def write_current_cancellable[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    mut reactor: Reactor,
    mut stream: TcpStream,
    data: Span[UInt8, _],
    mut heap: TimerHeap,
    clock: MonotonicClock,
    deadline: Optional[Duration] = None,
    token: Optional[CancellationToken] = None,
) raises -> Int:
    """Write twin of read_current_cancellable — identical contract."""
    if stream._writing:
        stream._writing = False
        reactor.unregister(stream._write_token)
        raise_if_cancel_wake(h)
        raise_if_timeout_wake(h)
        raise_if_closed_wake(h)
    if stream._closed:
        raise Error(
            "ClosedError: TcpStream closed (close_current) while the write "
            "was pending"
        )
    if token:
        var t2 = token.value()
        t2.checkpoint()
    while True:
        var attempt = stream._sock.send_nonblocking(data)
        if attempt.is_ready():
            _ = heap.cancel(h.id())
            return attempt.ready_count()
        if attempt.is_interrupted():
            continue
        if attempt.is_would_block():
            if deadline:
                _ = heap.arm(h.id(), Int(h.tcb()), clock.now() + deadline.value().ticks())
            if wait_writable(rt, h, reactor, stream):
                return -1
            _ = heap.cancel(h.id())
            raise_if_cancel_wake(h)
            raise_if_timeout_wake(h)
            raise_if_closed_wake(h)
            continue
        _ = heap.cancel(h.id())
        raise_errno(attempt.errno_code())
    return 0
