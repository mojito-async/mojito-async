# mojito_async/test/unit/t41_tcp_connect_aot.mojo
#
# A7.3 (issue #77) — TcpStream.connect() acceptance over the reactor's
# `register_and_park` entry point, resolved via kqueue write-readiness +
# the mjs_socket_connect_error deviation (getsockopt(SO_ERROR)).
#
# Scenarios:
#   1. established — connect to a real loopback listener that accepts:
#      the connecting task parks (WAITING, `_connecting=True`) on write-
#      readiness with no OS thread blocked (this whole driver runs
#      cooperatively on ONE OS thread — the park is proven by the task
#      state staying WAITING while this driver keeps executing plain
#      code), then resumes RUNNABLE once `reactor.poll` observes
#      readiness and redrives to a decoded established (True) outcome.
#   2. refused — connect to a closed local port raises a decoded errno,
#      whether the raw connect() call fails synchronously or is decoded
#      later from SO_ERROR after a pending write-readiness wake.
#
# AOT (imports the socket/poller/fiber dylib seams — modular/modular#6971).
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation

from mojito_async.integration.sys import BytePtr
from mojito_async.net.tcp_stream import (
    TcpStream,
    connect_current,
    create_tcp_stream,
)
from mojito_async.reactor import Reactor, make_reactor
from mojito_async.runtime.runtime import Nil, Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, spawn
from mojito_async.time.deadline import Duration, from_millis
from mojito_async.vendor.mojito_sys_io.socket import NativeSocket, SocketAddress


def red(what: String) raises -> None:
    print("T41 tcp connect: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[Nil]
comptime ESTABLISHED_PORT = Int32(61391)
comptime REFUSED_PORT = Int32(61392)
comptime MAX_POLL_RETRIES = 2000


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var reactor: UnsafePointer[Reactor, MutAnyOrigin]
    var stream: UnsafePointer[TcpStream, MutAnyOrigin]
    var addr: UnsafePointer[SocketAddress, MutAnyOrigin]
    var established: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.reactor = UnsafePointer[Reactor, MutAnyOrigin](unsafe_from_address=1)
        self.stream = UnsafePointer[TcpStream, MutAnyOrigin](unsafe_from_address=1)
        self.addr = UnsafePointer[SocketAddress, MutAnyOrigin](unsafe_from_address=1)
        self.established = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[Nil]:
    return JoinHandle[Nil](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Drives the ONE connecting task on every call — `connect_current` is
    self-redriving (its own `_connecting` flag distinguishes a fresh
    attempt from a redrive), so no phase field is needed here."""
    var sc = ud.bitcast[Scene]()
    var ha = _handle(tcb_addr, tid)
    ha.tcb()[].transition(TaskControlBlock.RUNNING)
    var ok = connect_current(rt, ha, sc[].reactor[], sc[].stream[], sc[].addr[])
    if ok:
        sc[].established[] = 1
        ha.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def _drain_reactor_until_runnable(
    mut rt: Runtime, mut reactor: Reactor, h: JoinHandle[Nil]
) raises:
    var tries = 0
    while h.state() == TaskControlBlock.WAITING and tries < MAX_POLL_RETRIES:
        _ = reactor.poll(rt, Optional[Duration](from_millis(50)))
        tries += 1
    if h.state() == TaskControlBlock.WAITING:
        red("reactor.poll never observed readiness within the retry budget")


def main() raises:
    var rt = create()
    var reactor = make_reactor()

    # ---- scenario 1: established via readiness -----------------------------
    var listener = NativeSocket.tcp_v4()
    listener.set_nonblocking(True)
    listener.set_reuseaddr(True)
    listener.bind(SocketAddress.ipv4(127, 0, 0, 1, ESTABLISHED_PORT))
    listener.listen(1)

    var sc = Scene()
    var established_cell = stack_allocation[1, Int]()
    established_cell[0] = 0
    sc.established = UnsafePointer[Int, MutAnyOrigin](to=established_cell[0])
    var reactor_ptr = UnsafePointer[Reactor, MutAnyOrigin](to=reactor)
    sc.reactor = reactor_ptr
    var stream = create_tcp_stream()
    sc.stream = UnsafePointer[TcpStream, MutAnyOrigin](to=stream)
    var addr = SocketAddress.ipv4(127, 0, 0, 1, ESTABLISHED_PORT)
    sc.addr = UnsafePointer[SocketAddress, MutAnyOrigin](to=addr)

    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)

    var served1 = scheduler_loop(rt, dispatch, ud)
    if served1 != 1:
        red("phase-1 drive served " + String(served1) + ", expected 1")

    if not h.is_completed():
        if h.state() != TaskControlBlock.WAITING:
            red("connecting task not WAITING after a pending connect")
        if not stream.is_connecting():
            red("stream.is_connecting() False while a connect is pending")

        # Drain the listener's backlog: the kernel completes the TCP
        # handshake before accept() is ever called, but this driver still
        # calls accept() to behave like "a real loopback listener that
        # accepts" per the issue #77 acceptance wording.
        var accepted = False
        var tries = 0
        while (not accepted) and tries < MAX_POLL_RETRIES:
            var attempt = listener.accept_nonblocking()
            if attempt.is_ready():
                var child_fd = attempt.take_ready_fd()
                var child = NativeSocket._adopt(child_fd)
                child.close()
                accepted = True
            elif attempt.is_would_block():
                tries += 1
            else:
                red(
                    "listener.accept_nonblocking unexpected error "
                    + String(attempt.errno_code())
                )
        if not accepted:
            red("listener never observed the queued connection")

        _drain_reactor_until_runnable(rt, reactor, h)
        if h.state() != TaskControlBlock.RUNNABLE:
            red("connecting task not RUNNABLE after readiness")

        var served2 = scheduler_loop(rt, dispatch, ud)
        if served2 != 1:
            red("redrive served " + String(served2) + ", expected 1")

    if established_cell[0] != 1:
        red("established flag never set")
    if not h.is_completed():
        red("connecting task did not complete once established")
    listener.close()

    # ---- scenario 2: refused ------------------------------------------------
    var sc2 = Scene()
    var established_cell2 = stack_allocation[1, Int]()
    established_cell2[0] = 0
    sc2.established = UnsafePointer[Int, MutAnyOrigin](to=established_cell2[0])
    sc2.reactor = reactor_ptr
    var stream2 = create_tcp_stream()
    sc2.stream = UnsafePointer[TcpStream, MutAnyOrigin](to=stream2)
    var addr2 = SocketAddress.ipv4(127, 0, 0, 1, REFUSED_PORT)
    sc2.addr = UnsafePointer[SocketAddress, MutAnyOrigin](to=addr2)
    var scp2 = UnsafePointer[Scene, MutAnyOrigin](to=sc2)
    var ud2 = scp2.bitcast[Byte]()

    var tcb2 = TB.create()
    var h2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb2), 0)

    var refused = False
    var refused_msg = String("")
    try:
        _ = scheduler_loop(rt, dispatch, ud2)
    except e:
        refused = True
        refused_msg = String(e)

    if (not refused) and h2.state() == TaskControlBlock.WAITING:
        # Pending path: kqueue reports EVFILT_WRITE for a FAILED connect
        # too (POSIX semantics) — poll, then redrive to observe the
        # decoded failure.
        var tries = 0
        while h2.state() == TaskControlBlock.WAITING and tries < MAX_POLL_RETRIES:
            _ = reactor.poll(rt, Optional[Duration](from_millis(50)))
            tries += 1
        if h2.state() == TaskControlBlock.WAITING:
            red("refused-connect reactor.poll never observed readiness")
        try:
            _ = scheduler_loop(rt, dispatch, ud2)
        except e:
            refused = True
            refused_msg = String(e)

    if not refused:
        red("refused connect neither raised synchronously nor on redrive")
    if "errno" not in refused_msg:
        red("refused-connect error not decoded via raise_errno: " + refused_msg)

    print("T41 tcp connect: PASS")
