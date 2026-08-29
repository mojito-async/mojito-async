# mojito_async/test/unit/t44_tcp_read_write_aot.mojo
#
# A7.5/A7.6 (issues #79/#80) — TcpStream.read/write/write_all/shutdown +
# cancellation/deadline integration acceptance driver, over a REAL TCP
# loopback pair (bind_and_listen + a raw client NativeSocket + a real
# kqueue-backed Reactor) — no fakes.
#
# Scenarios (issue #79 acceptance / #80 acceptance):
#   A. a client writes N bytes while the server-side TcpStream reads
#      (park-based: the read genuinely parks on an empty socket first,
#      WAITING is observed, THEN the client writes and a redrive completes
#      it byte-exact).
#   B. write_all succeeds across a large payload that forces at least one
#      real partial-write/WouldBlock cycle over loopback (kernel send
#      buffer is finite); the client drains and reassembles byte-exact.
#   C. an EOF read (client shuts its write half) is delivered as the
#      Closed condition — read_current returns 0, never hangs.
#   D. read_current_cancellable unblocks a blocked read on a deadline with
#      a decoded TimeoutError; the op-table slot is not leaked afterward.
#   E. an EXTERNAL cancel_op call against the stream's stored waiter
#      unblocks a parked read_current_cancellable with a decoded
#      CancellationError (issue #80 point 4's "a scope exit... cancels
#      every registered op it owns" primitive, exercised directly).
#   F. close_current wakes a pending read with a decoded ClosedError and
#      releases the op-table registration (issue #80 deliverable 3).
#
# AOT (imports the socket/poller/fiber dylib seams — modular/modular#6971).
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation

from mojito_async.cancellation import CancelFlag, CancellationToken, is_cancellation, make_cancel_flag
from mojito_async.integration.sys import BytePtr
from mojito_async.net.tcp_listener import TcpListener, accept_current, bind_and_listen
from mojito_async.net.tcp_stream import (
    TcpStream,
    create_tcp_stream,
    read_current,
    read_current_cancellable,
    write_all_current,
    write_current,
)
from mojito_async.reactor import Reactor, make_reactor
from mojito_async.reactor.cancel import cancel_op, service_io_deadlines
from mojito_async.runtime.runtime import Nil, Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Duration, from_millis
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.vendor.mojito_sys import monotonic_now_ns
from mojito_async.vendor.mojito_sys_io.socket import NativeSocket, SocketAddress, SHUT_WRITE


def red(what: String) raises -> None:
    print("T44 tcp read/write: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[Nil]
comptime MAX_POLL_RETRIES = 4000
comptime PORT_A = Int32(61601)
comptime PORT_B = Int32(61602)
comptime PORT_C = Int32(61603)
comptime PORT_D = Int32(61604)
comptime PORT_E = Int32(61605)
comptime PORT_F = Int32(61606)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[Nil]:
    return JoinHandle[Nil](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _connect_client(port: Int32) raises -> NativeSocket:
    var client = NativeSocket.tcp_v4()
    client.set_nonblocking(True)
    _ = client.connect(SocketAddress.ipv4(127, 0, 0, 1, port))
    return client^


# ---------------------------------------------------------------------------
# Shared accept-side setup: bind, connect a raw client, drive accept_current
# to a real server-side TcpStream (mirrors t42_tcp_accept_aot.mojo).
# ---------------------------------------------------------------------------


struct AcceptScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var reactor: UnsafePointer[Reactor, MutAnyOrigin]
    var listener: UnsafePointer[TcpListener, MutAnyOrigin]
    var out_stream: UnsafePointer[TcpStream, MutAnyOrigin]
    var accepted: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.reactor = UnsafePointer[Reactor, MutAnyOrigin](unsafe_from_address=1)
        self.listener = UnsafePointer[TcpListener, MutAnyOrigin](unsafe_from_address=1)
        self.out_stream = UnsafePointer[TcpStream, MutAnyOrigin](unsafe_from_address=1)
        self.accepted = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def accept_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[AcceptScene]()
    var h = _handle(tcb_addr, tid)
    h.tcb()[].transition(TaskControlBlock.RUNNING)
    var ok = accept_current(rt, h, sc[].reactor[], sc[].listener[], sc[].out_stream[])
    if ok:
        sc[].accepted[] = 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def _accept_pair(
    mut rt: Runtime,
    reactor_ptr: UnsafePointer[Reactor, MutAnyOrigin],
    port: Int32,
    mut out_stream: TcpStream,
) raises -> NativeSocket:
    """Bind+listen on `port`, connect a raw client, drive accept_current to
    completion, and fill `out_stream` with the accepted server-side
    TcpStream. Returns the raw client NativeSocket (the other peer)."""
    var listener = bind_and_listen(SocketAddress.ipv4(127, 0, 0, 1, port), 4)
    var client = _connect_client(port)

    var sc = AcceptScene()
    sc.reactor = reactor_ptr
    sc.listener = UnsafePointer[TcpListener, MutAnyOrigin](to=listener)
    sc.out_stream = UnsafePointer[TcpStream, MutAnyOrigin](to=out_stream)
    var accepted_cell = stack_allocation[1, Int]()
    accepted_cell[0] = 0
    sc.accepted = UnsafePointer[Int, MutAnyOrigin](to=accepted_cell[0])
    var scp = UnsafePointer[AcceptScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    _ = scheduler_loop(rt, accept_dispatch, ud)
    var tries = 0
    while h.state() == TaskControlBlock.WAITING and tries < MAX_POLL_RETRIES:
        _ = reactor_ptr[].poll(rt, Optional[Duration](from_millis(50)))
        tries += 1
    if not h.is_completed():
        _ = scheduler_loop(rt, accept_dispatch, ud)
    if not h.is_completed() or accepted_cell[0] != 1:
        red("setup: accept_current never completed the loopback handshake")
    listener.close()
    return client^


# ---------------------------------------------------------------------------
# Scenario A — park-based read, byte-exact round-trip.
# ---------------------------------------------------------------------------


struct ReadScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var reactor: UnsafePointer[Reactor, MutAnyOrigin]
    var stream: UnsafePointer[TcpStream, MutAnyOrigin]
    var buf: UnsafePointer[List[UInt8], MutAnyOrigin]
    var result: UnsafePointer[Int, MutAnyOrigin]  # -2 = not run, -1 = parked, >=0 = done

    def __init__(out self):
        self.reactor = UnsafePointer[Reactor, MutAnyOrigin](unsafe_from_address=1)
        self.stream = UnsafePointer[TcpStream, MutAnyOrigin](unsafe_from_address=1)
        self.buf = UnsafePointer[List[UInt8], MutAnyOrigin](unsafe_from_address=1)
        self.result = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def read_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[ReadScene]()
    var h = _handle(tcb_addr, tid)
    h.tcb()[].transition(TaskControlBlock.RUNNING)
    var n = read_current(rt, h, sc[].reactor[], sc[].stream[], Span[UInt8, MutAnyOrigin](sc[].buf[]))
    sc[].result[] = n
    if n >= 0:
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_read_write(mut rt: Runtime, reactor_ptr: UnsafePointer[Reactor, MutAnyOrigin]) raises:
    var out_stream = create_tcp_stream()
    var client = _accept_pair(rt, reactor_ptr, PORT_A, out_stream)

    var buf = List[UInt8]()
    for _ in range(64):
        buf.append(UInt8(0))
    var sc = ReadScene()
    sc.reactor = reactor_ptr
    sc.stream = UnsafePointer[TcpStream, MutAnyOrigin](to=out_stream)
    sc.buf = UnsafePointer[List[UInt8], MutAnyOrigin](to=buf)
    var result_cell = stack_allocation[1, Int]()
    result_cell[0] = -2
    sc.result = UnsafePointer[Int, MutAnyOrigin](to=result_cell[0])
    var scp = UnsafePointer[ReadScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    _ = scheduler_loop(rt, read_dispatch, ud)
    if result_cell[0] != -1:
        red("A: first read attempt did not park on an empty socket (got " + String(result_cell[0]) + ")")
    if h.state() != TaskControlBlock.WAITING:
        red("A: reading task not WAITING while the socket is empty (park assertion)")

    # the client writes N known bytes now.
    var payload = List[UInt8]()
    for i in range(37):
        payload.append(UInt8(i + 1))
    var sent = 0
    while sent < len(payload):
        var attempt = client.send_nonblocking(Span[UInt8, MutAnyOrigin](payload)[sent : len(payload)])
        if attempt.is_ready():
            sent += attempt.ready_count()
        elif attempt.is_would_block() or attempt.is_interrupted():
            continue
        else:
            red("A: client send failed: errno " + String(attempt.errno_code()))

    var tries = 0
    while h.state() == TaskControlBlock.WAITING and tries < MAX_POLL_RETRIES:
        _ = reactor_ptr[].poll(rt, Optional[Duration](from_millis(50)))
        tries += 1
    if h.state() != TaskControlBlock.RUNNABLE:
        red("A: read never became RUNNABLE after the client wrote")
    _ = scheduler_loop(rt, read_dispatch, ud)
    if not h.is_completed():
        red("A: redriven read did not complete")
    if result_cell[0] != len(payload):
        red("A: byte count mismatch: got " + String(result_cell[0]) + " expected " + String(len(payload)))
    for i in range(len(payload)):
        if buf[i] != payload[i]:
            red("A: byte " + String(i) + " mismatch: got " + String(buf[i]) + " expected " + String(payload[i]))

    client.close()
    out_stream.close()
    print("T44 scenario A (park-based read, byte-exact): PASS")

# ---------------------------------------------------------------------------
# Scenario B — write_all across a large payload (forces a real partial
# write/WouldBlock cycle over loopback); client drains concurrently.
# ---------------------------------------------------------------------------


struct WriteAllScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var reactor: UnsafePointer[Reactor, MutAnyOrigin]
    var stream: UnsafePointer[TcpStream, MutAnyOrigin]
    var data: UnsafePointer[List[UInt8], MutAnyOrigin]
    var written: UnsafePointer[Int, MutAnyOrigin]
    var done: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.reactor = UnsafePointer[Reactor, MutAnyOrigin](unsafe_from_address=1)
        self.stream = UnsafePointer[TcpStream, MutAnyOrigin](unsafe_from_address=1)
        self.data = UnsafePointer[List[UInt8], MutAnyOrigin](unsafe_from_address=1)
        self.written = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.done = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def write_all_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[WriteAllScene]()
    var h = _handle(tcb_addr, tid)
    h.tcb()[].transition(TaskControlBlock.RUNNING)
    var ok = write_all_current(
        rt, h, sc[].reactor[], sc[].stream[], Span[UInt8, MutAnyOrigin](sc[].data[]), sc[].written[]
    )
    if ok:
        sc[].done[] = 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_write_all(mut rt: Runtime, reactor_ptr: UnsafePointer[Reactor, MutAnyOrigin]) raises:
    var out_stream = create_tcp_stream()
    var client = _accept_pair(rt, reactor_ptr, PORT_B, out_stream)

    comptime N = 512 * 1024
    var payload = List[UInt8]()
    for i in range(N):
        payload.append(UInt8((i * 37 + 11) & 0xFF))

    var sc = WriteAllScene()
    sc.reactor = reactor_ptr
    sc.stream = UnsafePointer[TcpStream, MutAnyOrigin](to=out_stream)
    sc.data = UnsafePointer[List[UInt8], MutAnyOrigin](to=payload)
    var written_cell = stack_allocation[1, Int]()
    written_cell[0] = 0
    sc.written = UnsafePointer[Int, MutAnyOrigin](to=written_cell[0])
    var done_cell = stack_allocation[1, Int]()
    done_cell[0] = 0
    sc.done = UnsafePointer[Int, MutAnyOrigin](to=done_cell[0])
    var scp = UnsafePointer[WriteAllScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)

    var received = List[UInt8]()
    var recv_buf = List[UInt8]()
    for _ in range(65536):
        recv_buf.append(UInt8(0))

    var saw_park = False
    var rounds = 0
    while done_cell[0] == 0 and rounds < MAX_POLL_RETRIES:
        _ = scheduler_loop(rt, write_all_dispatch, ud)
        rounds += 1
        if h.state() == TaskControlBlock.WAITING:
            saw_park = True
        while True:
            var attempt = client.recv_nonblocking(Span[UInt8, MutAnyOrigin](recv_buf))
            if attempt.is_ready():
                var n = attempt.ready_count()
                for i in range(n):
                    received.append(recv_buf[i])
            elif attempt.is_interrupted():
                continue
            else:
                break
        if h.state() == TaskControlBlock.WAITING:
            _ = reactor_ptr[].poll(rt, Optional[Duration](from_millis(20)))

    if done_cell[0] != 1:
        red("B: write_all_current never completed within the retry budget")
    if not saw_park:
        red("B: a 512KB write_all over loopback never actually parked (WouldBlock never exercised)")

    var drain_tries = 0
    while len(received) < N and drain_tries < MAX_POLL_RETRIES:
        var attempt = client.recv_nonblocking(Span[UInt8, MutAnyOrigin](recv_buf))
        if attempt.is_ready():
            var n = attempt.ready_count()
            for i in range(n):
                received.append(recv_buf[i])
        elif attempt.is_interrupted():
            continue
        else:
            drain_tries += 1

    if len(received) != N:
        red("B: byte count mismatch: received " + String(len(received)) + " expected " + String(N))
    for i in range(N):
        if received[i] != payload[i]:
            red("B: byte " + String(i) + " mismatch")

    client.close()
    out_stream.close()
    print("T44 scenario B (write_all, partial transfer over 512KB): PASS")


# ---------------------------------------------------------------------------
# Scenario C — EOF read delivered as Closed (returns 0).
# ---------------------------------------------------------------------------


def scenario_eof(mut rt: Runtime, reactor_ptr: UnsafePointer[Reactor, MutAnyOrigin]) raises:
    var out_stream = create_tcp_stream()
    var client = _accept_pair(rt, reactor_ptr, PORT_C, out_stream)

    var buf = List[UInt8]()
    for _ in range(16):
        buf.append(UInt8(0))
    var sc = ReadScene()
    sc.reactor = reactor_ptr
    sc.stream = UnsafePointer[TcpStream, MutAnyOrigin](to=out_stream)
    sc.buf = UnsafePointer[List[UInt8], MutAnyOrigin](to=buf)
    var result_cell = stack_allocation[1, Int]()
    result_cell[0] = -2
    sc.result = UnsafePointer[Int, MutAnyOrigin](to=result_cell[0])
    var scp = UnsafePointer[ReadScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    _ = scheduler_loop(rt, read_dispatch, ud)
    if result_cell[0] != -1:
        red("C: first read attempt did not park on an empty socket")

    client.shutdown(SHUT_WRITE)

    var tries = 0
    while h.state() == TaskControlBlock.WAITING and tries < MAX_POLL_RETRIES:
        _ = reactor_ptr[].poll(rt, Optional[Duration](from_millis(50)))
        tries += 1
    if h.state() != TaskControlBlock.RUNNABLE:
        red("C: read never became RUNNABLE after the client shut its write half")
    _ = scheduler_loop(rt, read_dispatch, ud)
    if not h.is_completed():
        red("C: redriven read did not complete on EOF")
    if result_cell[0] != 0:
        red("C: EOF read did not return the 0 marker, got " + String(result_cell[0]))

    client.close()
    out_stream.close()
    print("T44 scenario C (EOF -> 0): PASS")


# ---------------------------------------------------------------------------
# Scenarios D/E/F — deadline timeout, external cancel, close-while-pending.
# ---------------------------------------------------------------------------


def _classify_raise(e: Error) -> Int:
    """1 = TimeoutError, 2 = CancellationError, 3 = ClosedError, 4 = other."""
    if "TimeoutError" in String(e):
        return 1
    if is_cancellation(e):
        return 2
    if "ClosedError" in String(e):
        return 3
    return 4


struct CancellableReadScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var reactor: UnsafePointer[Reactor, MutAnyOrigin]
    var stream: UnsafePointer[TcpStream, MutAnyOrigin]
    var buf: UnsafePointer[List[UInt8], MutAnyOrigin]
    var heap: UnsafePointer[TimerHeap, MutAnyOrigin]
    var clock_cell: UnsafePointer[UInt64, MutAnyOrigin]
    var deadline_ms: Int  # 0 = no deadline
    var result: UnsafePointer[Int, MutAnyOrigin]  # -2 not run, -1 parked, >=0 done
    var raised_kind: UnsafePointer[Int, MutAnyOrigin]  # 0 = none (see _classify_raise)

    def __init__(out self):
        self.reactor = UnsafePointer[Reactor, MutAnyOrigin](unsafe_from_address=1)
        self.stream = UnsafePointer[TcpStream, MutAnyOrigin](unsafe_from_address=1)
        self.buf = UnsafePointer[List[UInt8], MutAnyOrigin](unsafe_from_address=1)
        self.heap = UnsafePointer[TimerHeap, MutAnyOrigin](unsafe_from_address=1)
        self.clock_cell = UnsafePointer[UInt64, MutAnyOrigin](unsafe_from_address=1)
        self.deadline_ms = 0
        self.result = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.raised_kind = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def cancellable_read_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[CancellableReadScene]()
    var h = _handle(tcb_addr, tid)
    h.tcb()[].transition(TaskControlBlock.RUNNING)
    var clock = MonotonicClock(sc[].clock_cell)
    var dl = Optional[Duration]()
    if sc[].deadline_ms > 0:
        dl = Optional[Duration](from_millis(sc[].deadline_ms))
    try:
        var n = read_current_cancellable(
            rt, h, sc[].reactor[], sc[].stream[], Span[UInt8, MutAnyOrigin](sc[].buf[]),
            sc[].heap[], clock, dl, Optional[CancellationToken](),
        )
        sc[].result[] = n
        if n >= 0:
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
    except e:
        sc[].raised_kind[] = _classify_raise(e)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_timeout(mut rt: Runtime, reactor_ptr: UnsafePointer[Reactor, MutAnyOrigin]) raises:
    var out_stream = create_tcp_stream()
    var client = _accept_pair(rt, reactor_ptr, PORT_D, out_stream)

    var buf = List[UInt8]()
    for _ in range(16):
        buf.append(UInt8(0))
    var heap = TimerHeap()
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = monotonic_now_ns()

    var sc = CancellableReadScene()
    sc.reactor = reactor_ptr
    sc.stream = UnsafePointer[TcpStream, MutAnyOrigin](to=out_stream)
    sc.buf = UnsafePointer[List[UInt8], MutAnyOrigin](to=buf)
    sc.heap = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)
    sc.clock_cell = clock_cell
    sc.deadline_ms = 50
    var result_cell = stack_allocation[1, Int]()
    result_cell[0] = -2
    sc.result = UnsafePointer[Int, MutAnyOrigin](to=result_cell[0])
    var raised_cell = stack_allocation[1, Int]()
    raised_cell[0] = 0
    sc.raised_kind = UnsafePointer[Int, MutAnyOrigin](to=raised_cell[0])
    var scp = UnsafePointer[CancellableReadScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    _ = scheduler_loop(rt, cancellable_read_dispatch, ud)
    if result_cell[0] != -1:
        red("D: first read attempt did not park")
    if reactor_ptr[].live_count() != 1:
        red("D: expected exactly one live op-table registration while parked")

    var tries = 0
    while h.state() == TaskControlBlock.WAITING and tries < MAX_POLL_RETRIES:
        _ = reactor_ptr[].poll(rt, Optional[Duration](from_millis(5)))
        clock_cell[0] = monotonic_now_ns()
        _ = service_io_deadlines[Nil](rt, heap, clock_cell[0])
        tries += 1
    if h.state() != TaskControlBlock.RUNNABLE:
        red("D: the deadline never fired (task still WAITING after the retry budget)")
    _ = scheduler_loop(rt, cancellable_read_dispatch, ud)
    if not h.is_completed():
        red("D: redriven read did not settle")
    if raised_cell[0] != 1:
        red("D: expected a TimeoutError (kind 1), got kind " + String(raised_cell[0]))
    if reactor_ptr[].live_count() != 0:
        red("D: the op-table slot leaked after the timeout redrive")

    client.close()
    out_stream.close()
    print("T44 scenario D (deadline -> TimeoutError, no leak): PASS")


def scenario_external_cancel(mut rt: Runtime, reactor_ptr: UnsafePointer[Reactor, MutAnyOrigin]) raises:
    var out_stream = create_tcp_stream()
    var client = _accept_pair(rt, reactor_ptr, PORT_E, out_stream)

    var buf = List[UInt8]()
    for _ in range(16):
        buf.append(UInt8(0))
    var heap = TimerHeap()
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = monotonic_now_ns()

    var sc = CancellableReadScene()
    sc.reactor = reactor_ptr
    sc.stream = UnsafePointer[TcpStream, MutAnyOrigin](to=out_stream)
    sc.buf = UnsafePointer[List[UInt8], MutAnyOrigin](to=buf)
    sc.heap = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)
    sc.clock_cell = clock_cell
    sc.deadline_ms = 0  # no deadline: only an EXTERNAL cancel_op can end this
    var result_cell = stack_allocation[1, Int]()
    result_cell[0] = -2
    sc.result = UnsafePointer[Int, MutAnyOrigin](to=result_cell[0])
    var raised_cell = stack_allocation[1, Int]()
    raised_cell[0] = 0
    sc.raised_kind = UnsafePointer[Int, MutAnyOrigin](to=raised_cell[0])
    var scp = UnsafePointer[CancellableReadScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    _ = scheduler_loop(rt, cancellable_read_dispatch, ud)
    if result_cell[0] != -1:
        red("E: first read attempt did not park")
    if h.state() != TaskControlBlock.WAITING:
        red("E: reading task not WAITING before the external cancel")

    # Exactly the primitive a scope's cancellation descent would call
    # (issue #80 point 4) — exercised directly here.
    var won = cancel_op(
        reactor_ptr[], rt, out_stream._read_token, out_stream._read_waiter_tcb, out_stream._read_waiter_id
    )
    if not won:
        red("E: cancel_op did not win against a still-WAITING waiter")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("E: cancel_op did not deliver the wake")
    if reactor_ptr[].live_count() != 0:
        red("E: cancel_op did not release the op-table slot")

    _ = scheduler_loop(rt, cancellable_read_dispatch, ud)
    if not h.is_completed():
        red("E: redriven read did not settle")
    if raised_cell[0] != 2:
        red("E: expected a CancellationError (kind 2), got kind " + String(raised_cell[0]))

    client.close()
    out_stream.close()
    print("T44 scenario E (external cancel_op -> CancellationError): PASS")


def scenario_close_current(mut rt: Runtime, reactor_ptr: UnsafePointer[Reactor, MutAnyOrigin]) raises:
    var out_stream = create_tcp_stream()
    var client = _accept_pair(rt, reactor_ptr, PORT_F, out_stream)

    var buf = List[UInt8]()
    for _ in range(16):
        buf.append(UInt8(0))
    var heap = TimerHeap()
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = monotonic_now_ns()

    var sc = CancellableReadScene()
    sc.reactor = reactor_ptr
    sc.stream = UnsafePointer[TcpStream, MutAnyOrigin](to=out_stream)
    sc.buf = UnsafePointer[List[UInt8], MutAnyOrigin](to=buf)
    sc.heap = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)
    sc.clock_cell = clock_cell
    sc.deadline_ms = 0
    var result_cell = stack_allocation[1, Int]()
    result_cell[0] = -2
    sc.result = UnsafePointer[Int, MutAnyOrigin](to=result_cell[0])
    var raised_cell = stack_allocation[1, Int]()
    raised_cell[0] = 0
    sc.raised_kind = UnsafePointer[Int, MutAnyOrigin](to=raised_cell[0])
    var scp = UnsafePointer[CancellableReadScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    _ = scheduler_loop(rt, cancellable_read_dispatch, ud)
    if result_cell[0] != -1:
        red("F: first read attempt did not park")
    if reactor_ptr[].live_count() != 1:
        red("F: expected exactly one live op-table registration while parked")

    out_stream.close_current[Nil](rt, reactor_ptr[])
    if h.state() != TaskControlBlock.RUNNABLE:
        red("F: close_current did not wake the parked read")
    if reactor_ptr[].live_count() != 0:
        red("F: close_current leaked the op-table registration")

    _ = scheduler_loop(rt, cancellable_read_dispatch, ud)
    if not h.is_completed():
        red("F: redriven read did not settle")
    if raised_cell[0] != 3:
        red("F: expected a ClosedError (kind 3), got kind " + String(raised_cell[0]))

    client.close()
    print("T44 scenario F (close_current -> ClosedError, no leak): PASS")



def main() raises:
    var rt = create()
    var reactor = make_reactor()
    var reactor_ptr = UnsafePointer[Reactor, MutAnyOrigin](to=reactor)

    scenario_read_write(rt, reactor_ptr)
    scenario_write_all(rt, reactor_ptr)
    scenario_eof(rt, reactor_ptr)
    scenario_timeout(rt, reactor_ptr)
    scenario_external_cancel(rt, reactor_ptr)
    scenario_close_current(rt, reactor_ptr)

    print("T44 tcp read/write: PASS")
