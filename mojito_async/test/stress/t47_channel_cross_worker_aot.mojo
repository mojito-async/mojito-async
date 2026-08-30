# mojito_async/test/stress/t47_channel_cross_worker_aot.mojo
#
# Issue #128 — multi-worker Channel[Int] producer/consumer cross-worker
# stress: zero lost wakeups, no lost/duplicated items, across TWO REAL
# worker OS threads driving the REAL Channel[Int] send()/recv() +
# scheduler_loop machinery end to end (mirrors t38_mutex_cross_worker_aot
# exactly, adapted to the channel's send()/recv() + deferred-wake
# choreography instead of Mutex's direct lock()/unlock() handoff).
#
# Background: Channel[T]'s send()/recv() used a plain check-then-set on
# `_items`/`_send_waiters`/`_recv_waiters`/the closed flags and the
# SINGLE-PHASE `park_current` (channel.mojo landed before the A2 M:N
# scheduler existed).  On the A2 M:N scheduler two REAL worker OS threads
# calling send()/recv() on the SAME channel concurrently could race on the
# ring/waiter FIFOs (double-acquisition/lost-update, corrupting delivered
# items) and a foreign wake racing into the PARKING window (between the
# PARKING transition and the WAITING commit) was silently dropped —
# `park_current` never consulted the early-wake latch — a genuine lost
# wakeup that parks the waiting task FOREVER.  The issue #128 fix adds a
# SpinLock guard (mirrors Mutex/RWLock's A4.1/A4-batch-review treatment)
# and switches send()/recv() to the two-phase `park_prepare`/
# `park_validate`/`park_commit` kernel.
#
# Scene: TWO REAL worker OS threads — w0 is the SOLE sender side, w1 is
# the SOLE receiver side — contend on ONE shared `Channel[Int]` of
# capacity 1 (the tightest possible ring: every send blocks until the
# previous item is drained and every recv blocks until the next item
# arrives, so BOTH directions park on essentially every round) — no
# artificial synchronization gate beyond the channel itself, so the
# interleavings (including a wake landing inside the other side's PARKING
# window) are whatever REAL OS thread scheduling produces across ROUNDS
# contended rounds.  Each round: w0 spawns a fresh sender task that sends
# the round's 1-indexed sequence number; w1 spawns a fresh receiver task
# that receives one value and adds it to a running total.  A lost wakeup
# would park a waiter FOREVER (caught by this driver's bounded watchdog
# instead of hanging CI); a corrupted ring or a torn handoff would corrupt
# the final count/sum or leave the channel un-drained.
#
# Deadlock/hang detection: every drive loop is BOUNDED — a lost wakeup
# would hang the owning worker thread on this driver's very own bug (the
# issue #128 defense exists precisely so it doesn't); a bound trip prints
# RED and forces process exit instead of hanging CI.
#
# Verdict: exit 0 + "PASS"; any hang/mismatch prints RED and forces exit 1.
# AOT-only (pthread externs; modular/modular#6971).
from std.time import sleep
from mojito_async.channel import Channel, Receiver, RecvOutcome, SendOutcome, Sender, make_channel
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import Worker, make_worker
from mojito_async.task import JoinHandle, claim_running
from mojito_async.vendor.mojito_sys import c_malloc, entry_pointer


@extern("pthread_create")
def _pthread_create(
    thread: UnsafePointer[Int, MutAnyOrigin],
    attr: Int,
    start_routine: UnsafePointer[Byte, MutAnyOrigin],
    arg: BytePtr,
) abi("C") -> Int32: ...


@extern("pthread_join")
def _pthread_join(thread: UInt, retval: UInt) abi("C") -> Int32: ...


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]

comptime ROUNDS = Int(500)
comptime SPIN_BUDGET = Int(4000000)

comptime W0_ID = Int(1)
comptime W1_ID = Int(2)


def _fail(failures: UnsafePointer[Int, MutAnyOrigin], msg: String):
    failures[0] = failures[0] + 1
    print("  - " + msg)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var w0: UnsafePointer[Worker, MutAnyOrigin]
    var w1: UnsafePointer[Worker, MutAnyOrigin]
    var chan: UnsafePointer[Channel[Int], MutAnyOrigin]
    var tx: Sender[Int]
    var rx: Receiver[Int]
    var failures: UnsafePointer[Int, MutAnyOrigin]
    var thread_err: UnsafePointer[Int, MutAnyOrigin]    # 2-cell array
    var next_value: UnsafePointer[Int, MutAnyOrigin]    # w0-owned: this round's item
    var sent: UnsafePointer[Int, MutAnyOrigin]          # sender rounds completed
    var received: UnsafePointer[Int, MutAnyOrigin]      # receiver rounds completed
    var sum_sent: UnsafePointer[Int, MutAnyOrigin]
    var sum_received: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.w1 = self.w0
        self.chan = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.tx = Sender[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.rx = Receiver[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.thread_err = self.failures
        self.next_value = self.failures
        self.sent = self.failures
        self.received = self.failures
        self.sum_sent = self.failures
        self.sum_received = self.failures


# ---------------------------------------------------------------------------
# Per-worker dispatch: one send (w0) / one recv (w1) per round, draining
# the channel's deferred wakes after every attempt (the driver-side wake
# path #35/#128 both rely on) so a cross-worker wake actually gets
# delivered via the canonical unpark_current.
# ---------------------------------------------------------------------------

def _drain(mut rt: Runtime, sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    while sc[].chan[].to_wake_len() > 0:
        var wr = sc[].chan[].pop_to_wake()
        if wr.task_id == 0:
            break
        unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))


def dispatch_w0(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    var outcome = sc[].tx.send(rt, h, sc[].next_value[])
    # Outcome is captured synchronously at send() return (issue #152):
    # the return value records park at call-time, immune to the race where
    # `_drain()` immediately re-wakes this same task between the send()
    # return and a subsequent h.state() read.
    if outcome.is_parked():
        _drain(rt, sc)
        return 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    sc[].sum_sent[] += sc[].next_value[]
    sc[].sent[] += 1
    _drain(rt, sc)
    return 1


def dispatch_w1(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    var v = sc[].rx.recv(rt, h)
    # Same race-safety rationale as dispatch_w0 above.
    if v.is_parked():
        _drain(rt, sc)
        return 1
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    if v.is_value():
        sc[].sum_received[] += v.value()
        sc[].received[] += 1
    else:
        _fail(sc[].failures, "receiver observed close prematurely")
    _drain(rt, sc)
    return 1


def _drive_one_round[
    F: def(mut Runtime, Int, Int, BytePtr) raises -> Int
](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
    worker_id: Int,
    failures: UnsafePointer[Int, MutAnyOrigin],
) raises -> Bool:
    """Spawn one fresh one-shot task and drive it to COMPLETED.  Bounded
    spin+sleep watchdog: a lost wakeup would otherwise hang this thread
    forever (the exact bug issue #128 defends against)."""
    var tcb = TB.create()
    var tcbp = UnsafePointer[TB, MutAnyOrigin](to=tcb)
    tcb.transition(TaskControlBlock.RUNNABLE)
    rt.enqueue_local(Int(tcbp), 1)
    var h = JoinHandle[IntResult](tcbp, 1)

    var spins = 0
    while not h.is_completed():
        _ = scheduler_loop(rt, dispatcher, ud, worker_id=worker_id)
        spins += 1
        if spins > SPIN_BUDGET:
            _fail(failures, "worker " + String(worker_id)
                  + ": round did not complete (LOST WAKEUP or hang)")
            return False
        sleep(0.00001)
    return True


def serve_worker0(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rt = sc.w0[].runtime()
    for r in range(ROUNDS):
        sc.next_value[] = r + 1  # 1-indexed: 0 never mistaken for "unset"
        if not _drive_one_round(rt[], dispatch_w0, scp.bitcast[Byte](), W0_ID, sc.failures):
            return


def serve_worker1(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rt = sc.w1[].runtime()
    for r in range(ROUNDS):
        if not _drive_one_round(rt[], dispatch_w1, scp.bitcast[Byte](), W1_ID, sc.failures):
            return


@export("t47_worker0")
def t47_worker0(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker0(sc)
    except e:
        print("w0 raised: " + String(e))
        sc[].thread_err[0] = 1


@export("t47_worker1")
def t47_worker1(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker1(sc)
    except e:
        print("w1 raised: " + String(e))
        sc[].thread_err[1] = 1


def run_scenario(failures: UnsafePointer[Int, MutAnyOrigin]) raises:
    var w0 = make_worker()
    var w1 = make_worker()
    var w0p = UnsafePointer[Worker, MutAnyOrigin](to=w0)
    var w1p = UnsafePointer[Worker, MutAnyOrigin](to=w1)
    var chan = make_channel[Int](1)
    var chanp = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan)
    var tx = chan.sender()
    var rx = chan.receiver()

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].w0 = w0p
    sc[].w1 = w1p
    sc[].chan = chanp
    sc[].tx = tx
    sc[].rx = rx
    var cells = Int(c_malloc(8 * 8))
    var p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    for i in range(8):
        p[i] = 0
    sc[].failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 0 * 8)
    sc[].thread_err = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 1 * 8)
    # thread_err spans TWO cells (indices 1 and 2); every other field
    # starts at index 3 to avoid aliasing thread_err[1].
    sc[].next_value = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 3 * 8)
    sc[].sent = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 4 * 8)
    sc[].received = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 5 * 8)
    sc[].sum_sent = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 6 * 8)
    sc[].sum_received = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 7 * 8)

    var t0: Int = 0
    var t1: Int = 0
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t0), 0,
        entry_pointer["t47_worker0"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t1), 0,
        entry_pointer["t47_worker1"](), sc.bitcast[Byte](),
    )
    _ = _pthread_join(UInt(t0), 0)
    _ = _pthread_join(UInt(t1), 0)

    if sc[].thread_err[0] != 0 or sc[].thread_err[1] != 0:
        _fail(failures, "a worker thread raised (see prints above)")
    if sc[].sent[] != ROUNDS:
        _fail(failures, "sender completed " + String(sc[].sent[]) + "/"
              + String(ROUNDS) + " rounds")
    if sc[].received[] != ROUNDS:
        _fail(failures, "receiver completed " + String(sc[].received[]) + "/"
              + String(ROUNDS) + " rounds")
    var expected_sum = 0
    for r in range(ROUNDS):
        expected_sum += r + 1
    if sc[].sum_sent[] != expected_sum:
        _fail(failures, "sum sent = " + String(sc[].sum_sent[]) + ", expected "
              + String(expected_sum))
    if sc[].sum_received[] != expected_sum:
        _fail(failures, "sum received = " + String(sc[].sum_received[]) + ", expected "
              + String(expected_sum) + " (lost item / duplicate delivery / torn handoff)")
    if not chan.is_empty():
        _fail(failures, "channel not drained after the run")
    if chan.send_waiters_len() != 0 or chan.recv_waiters_len() != 0:
        _fail(failures, "channel not drained: leftover waiters")
    if chan.to_wake_len() != 0:
        _fail(failures, "channel not drained: leftover deferred wakes")
    print("T47 channel cross-worker: sent=" + String(sc[].sent[])
          + " received=" + String(sc[].received[])
          + " sum_sent=" + String(sc[].sum_sent[])
          + " sum_received=" + String(sc[].sum_received[]))


def main() raises:
    var failures = 0
    var fp = UnsafePointer[Int, MutAnyOrigin](to=failures)
    try:
        run_scenario(fp)
    except e:
        print("T47 channel cross-worker: RED (exception " + String(e) + ")")
        _iso_exit(1)
    if fp[] == 0:
        print("T47 channel cross-worker: PASS")
    else:
        print("T47 channel cross-worker: RED (" + String(fp[]) + " failure(s))")
        _iso_exit(1)
