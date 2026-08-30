# mojito_async/test/unit/t57_park_return_ambiguity.mojo
#
# GREEN driver for issue #152 — park-return ambiguity resolved.
#
# Before the fix recv() returned Optional[T](), indistinguishable from
# "channel closed", when it parked.  send() returned normally, identical
# to "item sent".  The only way to know which had happened was an out-of-
# band `h.state() == WAITING` check — an invisible caller invariant the
# project's own tests (t21_channel_park.mojo:169,191) carry explicitly.
#
# After the fix:
#   - recv() returns RecvOutcome[T] with kind VALUE | CLOSED | PARKED.
#   - send() returns SendOutcome with kind SENT | PARKED.
#
# This driver verifies:
#   scenario_recv — recv() on an empty open channel returns PARKED,
#     never CLOSED; the caller can distinguish the two without any
#     out-of-band state inspection.
#   scenario_send — send() on a full channel returns PARKED, never SENT;
#     the caller can distinguish the two and avoid miscounting.
#
# Verdict: exit 0 + "PASS"; any failure prints the details and raises.
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.channel import Channel, RecvOutcome, SendOutcome
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.vendor.mojito_sys import c_malloc


comptime TB = TaskControlBlock[IntResult]
comptime CAP = Int(2)
comptime N_ITEMS = Int(5)
comptime TCB_STRIDE = Int(256)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var ch: UnsafePointer[Channel[Int], MutAnyOrigin]
    var c: UnsafePointer[Int, MutAnyOrigin]
    # c[0] items received (VALUE outcomes)
    # c[1] 1 = dispatcher took the CLOSED branch (bug indicator)
    # c[2] 1 = dispatcher correctly identified PARKED
    # c[3] items sent (SENT outcomes)
    # c[4] 1 = dispatcher correctly identified PARKED on send

    def __init__(out self):
        self.ch = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


# ---------------------------------------------------------------------------
# Correct consumer: uses RecvOutcome to distinguish PARKED from CLOSED.
# ---------------------------------------------------------------------------

def dispatch_recv_probe(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    while True:
        var v = sc[].ch[].recv(rt, h)
        if v.is_parked():
            # Correctly identified PARKED — not CLOSED.
            sc[].c[2] = 1
            return 1
        if v.is_closed():
            # Correctly identified CLOSED.
            sc[].c[1] = 1
            return 1
        # VALUE: received an item.
        sc[].c[0] += 1
    return 1


# ---------------------------------------------------------------------------
# Correct producer: uses SendOutcome to distinguish PARKED from SENT.
# ---------------------------------------------------------------------------

def dispatch_send_probe(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    for i in range(N_ITEMS):
        var outcome = sc[].ch[].send(rt, h, i + 1)
        if outcome.is_parked():
            # Correctly identified PARKED — not SENT.
            sc[].c[4] = 1
            return 1
        # SENT: item was buffered.
        sc[].c[3] += 1
    return 1


def scenario_recv(mut failures: List[String]) raises:
    """recv() returns RecvOutcome.PARKED on an empty open channel.
    A caller can distinguish PARKED from CLOSED without any out-of-band
    h.state() check."""
    var rt = create()
    var ch = Channel[Int](CAP)
    var cells = stack_allocation[8, Int]()
    for i in range(8):
        cells[i] = 0
    var sc = Scene()
    sc.ch = UnsafePointer[Channel[Int], MutAnyOrigin](to=ch)
    sc.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(cells))
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcbp = UnsafePointer[TB, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(TCB_STRIDE))
    )
    tcbp[0] = TB.create()
    var h = spawn(rt, tcbp, 0)

    # Channel is OPEN and empty.  The consumer parks on the first recv().
    _ = scheduler_loop(rt, dispatch_recv_probe, ud)

    if cells[1] != 0:
        failures.append(
            "recv: consumer took the CLOSED branch on an open channel"
            + " — RecvOutcome.CLOSED returned when PARKED was expected"
        )
        return
    if cells[2] != 1:
        failures.append(
            "recv: consumer did not identify PARKED"
            + " — RecvOutcome.PARKED not returned by recv() on empty open channel"
        )
        return
    if h.state() != TaskControlBlock.WAITING:
        failures.append(
            "recv: task is not WAITING after identifying PARKED"
            + " (state=" + String(h.state()) + ")"
        )
        return
    if ch.is_closed():
        failures.append("recv: channel must still be OPEN")
        return
    # RecvOutcome correctly distinguished PARKED from CLOSED.


def scenario_send(mut failures: List[String]) raises:
    """send() returns SendOutcome.PARKED when the ring is full.
    A caller can distinguish PARKED from SENT without any out-of-band
    h.state() check."""
    var rt = create()
    var ch = Channel[Int](CAP)
    var cells = stack_allocation[8, Int]()
    for i in range(8):
        cells[i] = 0
    var sc = Scene()
    sc.ch = UnsafePointer[Channel[Int], MutAnyOrigin](to=ch)
    sc.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(cells))
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcbp = UnsafePointer[TB, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(TCB_STRIDE))
    )
    tcbp[0] = TB.create()
    var h = spawn(rt, tcbp, 0)

    # No consumer; the ring fills after CAP items and the next send parks.
    _ = scheduler_loop(rt, dispatch_send_probe, ud)

    var believed_sent = cells[3]
    var actually_in = ch.len()
    if cells[4] != 1:
        failures.append(
            "send: producer did not identify PARKED"
            + " — SendOutcome.PARKED not returned by send() on full channel"
        )
        return
    if believed_sent != actually_in:
        failures.append(
            "send: producer counted " + String(believed_sent)
            + " SENT outcome(s) but " + String(actually_in)
            + " item(s) are in the channel — miscounting despite fix"
        )
        return
    if believed_sent > CAP:
        failures.append(
            "send: producer overcounted: " + String(believed_sent)
            + " SENT outcomes on a capacity-" + String(CAP) + " channel"
        )
        return
    # SendOutcome correctly distinguished PARKED from SENT.


def main() raises:
    var failures = List[String]()
    try:
        scenario_recv(failures)
    except e:
        failures.append("recv: scenario raised: " + String(e))
    try:
        scenario_send(failures)
    except e:
        failures.append("send: scenario raised: " + String(e))
    if len(failures) == 0:
        print("T57 park return ambiguity: PASS")
    else:
        print("T57 park return ambiguity: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        raise Error("T57 park return ambiguity: RED")
