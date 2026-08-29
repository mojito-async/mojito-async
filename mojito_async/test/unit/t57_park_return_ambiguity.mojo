# mojito_async/test/unit/t57_park_return_ambiguity.mojo
#
# RED driver for issue #152 — every blocking call returns a lie when it
# parks, and each family lies differently.
#
#     channel/channel.mojo:374-408   recv  -> Optional[T]()  indistinguishable
#                                            from "closed"
#     channel/channel.mojo:371-372   send  -> returns NORMALLY, indistinguish-
#                                            able from "sent"
#     sync/mutex.mojo:232-233        lock  -> False = "parked", while
#                                            try_lock's False = "contended"
#     net/tcp_stream.mojo:66-69      read  -> -1
#     channel/select.mojo:640-644    select -> "a meaningless pending
#                                            placeholder" (its own words)
#
# The protocol requires the caller to check `h.state() == WAITING` out of
# band after EVERY blocking call, or the sentinel is misread.  The project's
# own tests carry that burden explicitly (`t21_channel_park.mojo:168,190`
# does exactly that after every send and recv), which is the tell.
#
# The misuse the issue names is the loop a competent user writes first:
#
#     var v = rx.recv(rt, h)
#     if not v: break          # "closed"  -> actually terminates on the
#                              #             first backpressure park
#     tx.send(rt, h, item)
#     count += 1               # counts sends that never happened
#
# Both compile, both run, both are quietly wrong.  This driver writes both
# halves and asserts against the truth the channel itself holds.
#
# The scope note from the issue applies: the cooperative no-TLS driver model
# is a real constraint and is not what is under test here.  The AMBIGUITY is,
# and it is a free choice — `SelectOutcome` already demonstrates the
# discriminated-result pattern one module away.
#
# Verdict: exit 0 + "PASS"; any failure prints the details and raises.
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.channel import Channel
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.vendor.mojito_sys import c_malloc


comptime TB = TaskControlBlock[IntResult]
comptime CAP = Int(2)
comptime N_ITEMS = Int(5)
# The TCB is heap cells, not a frame local: the handle outlives the calls
# that park through it and a frame-local cell made state() read garbage.
comptime TCB_STRIDE = Int(256)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var ch: UnsafePointer[Channel[Int], MutAnyOrigin]
    var c: UnsafePointer[Int, MutAnyOrigin]
    # c[0] items the naive consumer believes it received
    # c[1] set when the consumer took the "channel is closed" branch
    # c[2] items the naive producer believes it sent
    # c[3] whether the producer loop finished
    # c[4] set when a later send RAISED because an earlier one had parked

    def __init__(out self):
        self.ch = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


# ---------------------------------------------------------------------------
# The consumer, written the natural way.
# ---------------------------------------------------------------------------

def dispatch_naive_consumer(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    while True:
        var v = sc[].ch[].recv(rt, h)
        if not v:
            # "The channel is closed, we are done."  There is nothing in the
            # return value that says otherwise.
            sc[].c[1] = 1
            break
        sc[].c[0] += 1
    return 1


# ---------------------------------------------------------------------------
# The producer, written the natural way.
# ---------------------------------------------------------------------------

def dispatch_naive_producer(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    for i in range(N_ITEMS):
        try:
            sc[].ch[].send(rt, h, i + 1)
        except e:
            # A previous send() had actually PARKED, and said nothing. This
            # one tries to park a task that is already WAITING.
            sc[].c[4] = 1
            print("    (the naive producer's next send raised: "
                  + String(e) + ")")
            return 1
        # send() returned without raising, so the item went in.
        sc[].c[2] += 1
    sc[].c[3] = 1
    return 1


def scenario_recv(mut failures: List[String]) raises:
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

    # The channel is OPEN and momentarily empty — an ordinary moment in any
    # producer/consumer pair.
    _ = scheduler_loop(rt, dispatch_naive_consumer, ud)

    # The producer's items arrive right afterwards.
    for i in range(N_ITEMS):
        if not ch.try_send(i + 1):
            break

    if cells[1] != 1:
        failures.append("recv: the consumer did not take the closed branch;"
                        + " scenario no longer reproduces as written")
        return
    if ch.is_closed():
        failures.append("recv: the channel must still be OPEN for this to be"
                        + " a lie")
        return
    if cells[0] >= N_ITEMS:
        failures.append("recv: the consumer received everything; nothing lost")
        return

    failures.append(
        "recv: the consumer stopped on 'closed' after receiving "
        + String(cells[0]) + " item(s), while the channel is OPEN and holds "
        + String(ch.len()) + " item(s) with " + String(N_ITEMS)
        + " produced. recv() returned Optional[T]() because it PARKED, which"
        + " is the same value it returns for a closed-and-empty channel"
        + " (channel.mojo:374-408). The caller cannot tell the two apart"
        + " without an out-of-band h.state() == WAITING check."
    )
    # The out-of-band check the API forces on every caller, reported rather
    # than asserted: the point is that the caller has to know to make it.
    print("    (after the consumer broke, the task state is "
          + String(h.state()) + "; TaskControlBlock.WAITING is "
          + String(TaskControlBlock.WAITING)
          + " — the out-of-band check the API forces on every caller)")


def scenario_send(mut failures: List[String]) raises:
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

    # Nobody is consuming, so the ring fills after CAP items and the next
    # send parks.
    _ = scheduler_loop(rt, dispatch_naive_producer, ud)

    var believed = cells[2]
    var actually_in = ch.len()
    if believed <= actually_in:
        failures.append("send: the producer's count matches the channel;"
                        + " scenario no longer reproduces as written")
        return

    if cells[4] != 0:
        failures.append(
            "send: the producer's NEXT send raised IllegalTransitionError"
            + " (WAITING -> PARKING) because the previous one had silently"
            + " parked. The loop is not merely miscounting, it walks the"
            + " task state machine into an illegal transition."
        )
    failures.append(
        "send: the producer counted " + String(believed)
        + " send(s) but only " + String(actually_in)
        + " item(s) are in the channel. send() RETURNED NORMALLY when it"
        + " parked (channel.mojo:371-372), which is byte-for-byte what a"
        + " successful send looks like to the caller — there is no return"
        + " value at all to inspect."
    )
    if cells[3] != 0:
        failures.append(
            "send: and the producer loop ran to completion believing every"
            + " item was delivered"
        )


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
