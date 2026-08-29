# mojito_async/test/unit/t29_cancel_park.mojo
#
# A4.3 (issue #57) — task-aware cancellation in park and sync waits.
#
# Acceptance:
#   - the park kernel itself: park_cancellable raises (never parks) when a
#     token is ALREADY requested; with_cancel returns False instead
#     (deliverable 4, unblocks-without-raise form); a MID-wait wake_cancelled
#     stamps the CANCEL winner that raise_if_cancel_wake/is_cancel_wake
#     decode on the next re-entry; a LATER readiness wake against an
#     already-cancel-woken task is a safe no-op (C6 exactly-one-winner);
#   - Mutex/Semaphore/Channel each grow a token-aware entry point
#     (lock_cancellable / acquire_cancellable / send_cancellable /
#     recv_cancellable) plus a cancel_*_wait that removes the waiter from
#     the primitive's OWN FIFO before waking it — proven against BOTH C6
#     orderings: cancel wins (a mid-queue waiter, NOT the head — the other
#     queued waiters keep their relative order) and readiness wins (a
#     waiter already granted by a normal unlock/release/fast-path; a LATER
#     cancel_*_wait call on it is a no-op, never a double wake / double
#     raise) — leaving the primitive in a coherent state either way (lock
#     not stuck held, permits not leaked, channel ring/slots untouched).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.cancellation import CancelFlag, CancellationToken, is_cancellation, make_cancel_flag
from mojito_async.channel import Channel, Receiver, Sender, make_channel
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import (
    is_cancel_wake,
    park_cancellable,
    park_current,
    raise_if_cancel_wake,
    unpark_current,
    wake_cancelled,
    with_cancel,
)
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import Mutex, Semaphore
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T29 cancel park: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _drain_channel(mut rt: Runtime, mut ch: Channel[Int]) raises:
    """Driver-side deferred-wake drain (mirrors t21_channel_park.mojo): the
    channel's OWN try_send/try_recv/send/recv only APPEND a deferred
    WaitRecord to _to_wake (channel.mojo never reconstructs a handle
    in-method); this driver resumes each through the canonical
    unpark_current."""
    while ch.to_wake_len() > 0:
        var wr = ch.pop_to_wake()
        if wr.task_id == 0:
            break
        var hp = _handle(wr.tcb_addr, wr.task_id)
        unpark_current(rt, hp)


# ---------------------------------------------------------------------------
# Scenario 1 — the park kernel itself: pre-check, with_cancel, mid-wait
# cancel winner, and the C6 "later readiness is a no-op" cross-check.
# ---------------------------------------------------------------------------


def scenario_kernel() raises:
    var rt = create()

    # 1a. ALREADY requested -> park_cancellable raises WITHOUT parking.
    var flag_a = make_cancel_flag()
    var token_a = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=flag_a))
    token_a.request()
    var tcb_a = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    claim_running(h_a)
    var raised_a = False
    try:
        park_cancellable(rt, h_a, token_a)
    except e:
        raised_a = True
        if not is_cancellation(e):
            red("1a: park_cancellable's pre-check did not use the CancellationError naming")
    if not raised_a:
        red("1a: park_cancellable did not raise on an already-requested token")
    if h_a.state() != TaskControlBlock.RUNNING:
        red("1a: an already-cancelled park_cancellable must never park (state "
            + String(h_a.state()) + ")")

    # 1b. with_cancel: same pre-check, UNBLOCKS WITHOUT RAISING (False).
    var flag_b = make_cancel_flag()
    var token_b = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=flag_b))
    token_b.request()
    var tcb_b = TB.create()
    var h_b = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    claim_running(h_b)
    if with_cancel(rt, h_b, token_b):
        red("1b: with_cancel must return False on an already-requested token")
    if h_b.state() != TaskControlBlock.RUNNING:
        red("1b: with_cancel's pre-check must never park either")

    # 1c. fresh token: with_cancel parks (True); a MID-wait wake_cancelled
    # wins the C6 race; raise_if_cancel_wake raises on the NEXT re-entry and
    # clears the stamp (re-stamp safety).
    var flag_c = make_cancel_flag()
    var token_c = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=flag_c))
    var tcb_c = TB.create()
    var h_c = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_c), 0)
    claim_running(h_c)
    if not with_cancel(rt, h_c, token_c):
        red("1c: with_cancel must return True (parked) on a fresh token")
    if h_c.state() != TaskControlBlock.WAITING:
        red("1c: with_cancel did not actually park")
    wake_cancelled(rt, h_c)
    if h_c.state() != TaskControlBlock.RUNNABLE:
        red("1c: wake_cancelled did not deliver the wake")
    if not is_cancel_wake(h_c):
        red("1c: is_cancel_wake did not observe the CANCEL winner stamp")
    claim_running(h_c)
    var raised_c = False
    try:
        raise_if_cancel_wake(h_c)
    except e:
        raised_c = True
        if not is_cancellation(e):
            red("1c: raise_if_cancel_wake used the wrong error naming")
    if not raised_c:
        red("1c: raise_if_cancel_wake did not raise after a cancel-won wake")
    if is_cancel_wake(h_c):
        red("1c: raise_if_cancel_wake did not clear the stamp (re-stamp-safety)")

    # 1d. readiness-wins ordering: park_cancellable (fresh token), then an
    # ORDINARY unpark_current (readiness, not wake_cancelled) — the resumed
    # task must NOT observe a cancel win.
    var flag_d = make_cancel_flag()
    var token_d = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=flag_d))
    var tcb_d = TB.create()
    var h_d = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_d), 0)
    claim_running(h_d)
    park_cancellable(rt, h_d, token_d)
    unpark_current(rt, h_d)
    claim_running(h_d)
    raise_if_cancel_wake(h_d)  # must be a silent no-op
    if h_d.state() != TaskControlBlock.RUNNING:
        red("1d: readiness-wins resume left the task in the wrong state")

    # 1e. C6 exactly-one-winner: once wake_cancelled has already claimed the
    # wake, a LATER readiness-style unpark_current against the SAME task
    # must be a safe enqueue-once no-op (never a double transition, never a
    # second stamp) — inherited for free from unpark_current's existing
    # generation-claim contract.
    var flag_e = make_cancel_flag()
    var token_e = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=flag_e))
    var tcb_e = TB.create()
    var h_e = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_e), 0)
    claim_running(h_e)
    park_cancellable(rt, h_e, token_e)
    wake_cancelled(rt, h_e)
    if h_e.state() != TaskControlBlock.RUNNABLE:
        red("1e: cancel wake did not win")
    var pending_before = rt.pending()
    unpark_current(rt, h_e)  # a losing, racing "readiness" wake: must no-op
    if h_e.state() != TaskControlBlock.RUNNABLE:
        red("1e: a losing readiness wake corrupted the cancel winner's state")
    if rt.pending() != pending_before:
        red("1e: a losing readiness wake double-enqueued")
    if not is_cancel_wake(h_e):
        red("1e: a losing readiness wake overwrote the cancel winner's stamp")

    print("T29 scenario 1 (park kernel C6): PASS")


# ---------------------------------------------------------------------------
# Scenario 2 — Mutex: cancel a MID-queue waiter (order preserved for the
# others); a LATER cancel_lock_wait on an already-granted waiter is a no-op.
# ---------------------------------------------------------------------------


struct MtxScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var ids: UnsafePointer[Int, MutAnyOrigin]  # 3 waiter ids
    var phs: UnsafePointer[Int, MutAnyOrigin]  # 3 phases
    var order: UnsafePointer[Int, MutAnyOrigin]  # completion order slots
    var norder: UnsafePointer[Int, MutAnyOrigin]
    var cancelled: UnsafePointer[Int, MutAnyOrigin]  # 1 if waiter k raised
    var token: UnsafePointer[CancellationToken, MutAnyOrigin]

    def __init__(out self):
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cancelled = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.token = UnsafePointer[CancellationToken, MutAnyOrigin](unsafe_from_address=1)


def mtx_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[MtxScene]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if sc[].ids[k] == tid:
            who = k
    if who == -1:
        red("mtx: unexpected task id")
    var ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(sc[].phs) + who * 8)
    claim_running(h)
    if ph[] == 0:
        ph[] = 1
        try:
            var got = sc[].mtx[].lock_cancellable(rt, h, sc[].token[])
            if got:
                red("mtx: contended waiter must not acquire immediately")
        except e:
            red("mtx: unexpected raise on first attempt: " + String(e))
        return 1
    # re-entry: either the GRANT marker (readiness won) or a raise (cancel won)
    try:
        var got2 = sc[].mtx[].lock_cancellable(rt, h, sc[].token[])
        if not got2:
            red("mtx: granted re-entry did not acquire (who=" + String(who) + ")")
        var i = sc[].norder[]
        sc[].order[i] = who
        sc[].norder[] = i + 1
        _ = sc[].mtx[].unlock[IntResult](rt)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    except e:
        if not is_cancellation(e):
            red("mtx: re-entry raised a non-cancellation error: " + String(e))
        sc[].cancelled[who] = 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_mutex() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var mtx = Mutex[Int](0)
    if not mtx.try_lock():
        red("mtx: pre-acquire failed")
    var mtx_flag = make_cancel_flag()
    var token = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=mtx_flag))
    var sc = MtxScene()
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    sc.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 9 * 8)
    sc.cancelled = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 10 * 8)
    sc.token = UnsafePointer[CancellationToken, MutAnyOrigin](to=token)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[MtxScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # A2.2 (issue #68): owner pop is LIFO, register in reverse so the deque
    # serves w0, w1, w2 in that arrival order (mirrors t21_mutex.mojo).
    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    _ = scheduler_loop(rt, mtx_dispatch, ud)
    if mtx.waiter_count() != 3:
        red("mtx: expected 3 queued waiters, got " + String(mtx.waiter_count()))

    # cancel the MID waiter (w1) — w0 and w2 keep their relative order.
    var won = mtx.cancel_lock_wait(rt, h_w1)
    if not won:
        red("mtx: cancel_lock_wait must win against a still-queued waiter")
    if mtx.waiter_count() != 2:
        red("mtx: cancelled waiter not removed from the FIFO")

    # a second cancel_lock_wait against the SAME (now-gone) waiter is a
    # no-op — never a double wake.
    var pending_before = rt.pending()
    var won_again = mtx.cancel_lock_wait(rt, h_w1)
    if won_again:
        red("mtx: cancel_lock_wait must not win twice against the same waiter")
    if rt.pending() != pending_before:
        red("mtx: a losing repeat cancel double-enqueued")

    _ = scheduler_loop(rt, mtx_dispatch, ud)  # drive w1's cancellation raise
    if buf[11] != 1:
        red("mtx: w1 must observe the cancellation")
    if not h_w1.is_completed():
        red("mtx: cancelled waiter did not reach COMPLETED")

    # a single unlock() hands off to w0 (FIFO head, order preserved); w0's
    # OWN re-entry (mtx_dispatch's granted branch) unlocks again internally
    # once it completes, cascading the handoff straight to w2 within the
    # SAME scheduler_loop drain — no second manual unlock() is needed (or
    # even possible: by the time this driver could call one, the queue is
    # already empty).
    var handed1 = mtx.unlock[IntResult](rt)
    if not handed1:
        red("mtx: first unlock must hand off to the FIFO head (w0)")
    _ = scheduler_loop(rt, mtx_dispatch, ud)
    var last = mtx.unlock[IntResult](rt)
    if last:
        red("mtx: final release must not hand off (queue drained by the cascade)")

    if mtx.is_locked() or mtx.waiter_count() != 0:
        red("mtx: not left coherent (unlocked, empty queue) after the cancel+drain")
    if not (h_w0.is_completed() and h_w2.is_completed()):
        red("mtx: surviving waiters did not complete")
    if buf[9] != 2:
        red("mtx: expected exactly 2 granted completions, got " + String(buf[9]))
    if buf[6] != 0 or buf[7] != 2:
        red("mtx: FIFO order of the SURVIVING waiters broken by the cancel (want w0,w2): "
            + String(buf[6]) + "," + String(buf[7]))

    print("T29 scenario 2 (Mutex cancel mid-queue, order+coherence): PASS")


# ---------------------------------------------------------------------------
# Scenario 3 — Semaphore: same shape as Mutex (1 permit, 3 waiters, cancel
# the mid waiter, verify order + permit accounting stays coherent).
# ---------------------------------------------------------------------------


struct SemScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var sem: UnsafePointer[Semaphore, MutAnyOrigin]
    var ids: UnsafePointer[Int, MutAnyOrigin]
    var phs: UnsafePointer[Int, MutAnyOrigin]
    var order: UnsafePointer[Int, MutAnyOrigin]
    var norder: UnsafePointer[Int, MutAnyOrigin]
    var cancelled: UnsafePointer[Int, MutAnyOrigin]
    var token: UnsafePointer[CancellationToken, MutAnyOrigin]

    def __init__(out self):
        self.sem = UnsafePointer[Semaphore, MutAnyOrigin](unsafe_from_address=1)
        self.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cancelled = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.token = UnsafePointer[CancellationToken, MutAnyOrigin](unsafe_from_address=1)


def sem_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SemScene]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if sc[].ids[k] == tid:
            who = k
    if who == -1:
        red("sem: unexpected task id")
    var ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(sc[].phs) + who * 8)
    claim_running(h)
    if ph[] == 0:
        ph[] = 1
        try:
            var got = sc[].sem[].acquire_cancellable(rt, h, sc[].token[])
            if got:
                red("sem: contended waiter must not acquire immediately")
        except e:
            red("sem: unexpected raise on first attempt: " + String(e))
        return 1
    try:
        var got2 = sc[].sem[].acquire_cancellable(rt, h, sc[].token[])
        if not got2:
            red("sem: granted re-entry did not acquire (who=" + String(who) + ")")
        var i = sc[].norder[]
        sc[].order[i] = who
        sc[].norder[] = i + 1
        _ = sc[].sem[].release[IntResult](rt)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    except e:
        if not is_cancellation(e):
            red("sem: re-entry raised a non-cancellation error: " + String(e))
        sc[].cancelled[who] = 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_semaphore() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var sem = Semaphore(1)
    if not sem.try_acquire():
        red("sem: pre-acquire failed")
    var sem_flag = make_cancel_flag()
    var token = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=sem_flag))
    var sc = SemScene()
    sc.sem = UnsafePointer[Semaphore, MutAnyOrigin](to=sem)
    sc.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    sc.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 9 * 8)
    sc.cancelled = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 10 * 8)
    sc.token = UnsafePointer[CancellationToken, MutAnyOrigin](to=token)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[SemScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    _ = scheduler_loop(rt, sem_dispatch, ud)
    if sem.waiter_count() != 3:
        red("sem: expected 3 queued waiters, got " + String(sem.waiter_count()))

    var won = sem.cancel_acquire_wait(rt, h_w1)
    if not won:
        red("sem: cancel_acquire_wait must win against a still-queued waiter")
    if sem.waiter_count() != 2:
        red("sem: cancelled waiter not removed from the FIFO")
    var won_again = sem.cancel_acquire_wait(rt, h_w1)
    if won_again:
        red("sem: cancel_acquire_wait must not win twice against the same waiter")

    _ = scheduler_loop(rt, sem_dispatch, ud)
    if buf[11] != 1:
        red("sem: w1 must observe the cancellation")

    # a single release() hands off to w0 (FIFO head, order preserved); w0's
    # OWN re-entry (sem_dispatch's granted branch) releases again internally
    # once it completes, cascading the handoff straight to w2 within the
    # SAME scheduler_loop drain (mirrors sync/mutex.mojo's cascade).
    var handed1 = sem.release[IntResult](rt)
    if not handed1:
        red("sem: first release must hand off to the FIFO head (w0)")
    _ = scheduler_loop(rt, sem_dispatch, ud)

    if sem.available() != 1 or sem.waiter_count() != 0:
        red("sem: not left coherent (1 permit free, empty queue) after cancel+drain: "
            + String(sem.available()))
    if not (h_w0.is_completed() and h_w2.is_completed()):
        red("sem: surviving waiters did not complete")
    if buf[6] != 0 or buf[7] != 2:
        red("sem: FIFO order of the SURVIVING waiters broken by the cancel (want w0,w2): "
            + String(buf[6]) + "," + String(buf[7]))

    print("T29 scenario 3 (Semaphore cancel mid-queue, order+coherence): PASS")


# ---------------------------------------------------------------------------
# Scenario 4 — Channel: cancel a mid-queue send waiter (backpressure) and a
# mid-queue recv waiter (empty channel); order + ring/slot coherence.
# ---------------------------------------------------------------------------


struct ChanScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var tx: Sender[Int]
    var rx: Receiver[Int]
    var ids: UnsafePointer[Int, MutAnyOrigin]
    var phs: UnsafePointer[Int, MutAnyOrigin]
    var order: UnsafePointer[Int, MutAnyOrigin]
    var norder: UnsafePointer[Int, MutAnyOrigin]
    var cancelled: UnsafePointer[Int, MutAnyOrigin]
    var token: UnsafePointer[CancellationToken, MutAnyOrigin]

    def __init__(out self):
        self.tx = Sender[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.rx = Receiver[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cancelled = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.token = UnsafePointer[CancellationToken, MutAnyOrigin](unsafe_from_address=1)


def chan_send_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[ChanScene]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if sc[].ids[k] == tid:
            who = k
    if who == -1:
        red("chan-send: unexpected task id")
    var ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(sc[].phs) + who * 8)
    claim_running(h)
    if ph[] == 0:
        ph[] = 1
        try:
            sc[].tx.send_cancellable(rt, h, 100 + who, sc[].token[])
            if h.state() != TaskControlBlock.WAITING:
                red("chan-send: contended send must park")
        except e:
            red("chan-send: unexpected raise on first attempt: " + String(e))
        return 1
    try:
        sc[].tx.send_cancellable(rt, h, 100 + who, sc[].token[])
        var i = sc[].norder[]
        sc[].order[i] = who
        sc[].norder[] = i + 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    except e:
        if not is_cancellation(e):
            red("chan-send: re-entry raised a non-cancellation error: " + String(e))
        sc[].cancelled[who] = 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_channel_send() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var ch = make_channel[Int](1)
    if not ch.try_send(0):
        red("chan-send: prefill failed")
    var tx = ch.sender()
    var rx = ch.receiver()
    var send_flag = make_cancel_flag()
    var token = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=send_flag))
    var sc = ChanScene()
    sc.tx = tx
    sc.rx = rx
    sc.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    sc.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 9 * 8)
    sc.cancelled = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 10 * 8)
    sc.token = UnsafePointer[CancellationToken, MutAnyOrigin](to=token)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[ChanScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    _ = scheduler_loop(rt, chan_send_dispatch, ud)
    if ch.send_waiters_len() != 3:
        red("chan-send: expected 3 queued senders, got " + String(ch.send_waiters_len()))

    var won = sc.tx.cancel_send_wait(rt, h_w1)
    if not won:
        red("chan-send: cancel_send_wait must win against a still-queued waiter")
    if ch.send_waiters_len() != 2:
        red("chan-send: cancelled sender not removed from the FIFO")
    if sc.tx.cancel_send_wait(rt, h_w1):
        red("chan-send: cancel_send_wait must not win twice")

    _ = scheduler_loop(rt, chan_send_dispatch, ud)
    if buf[11] != 1:
        red("chan-send: w1 must observe the cancellation")

    # drain: two recvs free two ring slots, handing off to w0 then w2 FIFO.
    var v1 = sc.rx.try_recv()
    if not v1:
        red("chan-send: first recv must return the prefilled value")
    _drain_channel(rt, ch)
    _ = scheduler_loop(rt, chan_send_dispatch, ud)
    var v2 = sc.rx.try_recv()
    if not v2:
        red("chan-send: second recv must return w0's sent value")
    _drain_channel(rt, ch)
    _ = scheduler_loop(rt, chan_send_dispatch, ud)
    var v3 = sc.rx.try_recv()
    if not v3:
        red("chan-send: third recv must return w2's sent value")

    if ch.send_waiters_len() != 0 or not ch.is_empty():
        red("chan-send: not left coherent (drained ring, empty send queue)")
    if not (h_w0.is_completed() and h_w2.is_completed()):
        red("chan-send: surviving waiters did not complete")
    if buf[6] != 0 or buf[7] != 2:
        red("chan-send: FIFO order of the SURVIVING senders broken by the cancel (want w0,w2): "
            + String(buf[6]) + "," + String(buf[7]))

    print("T29 scenario 4 (Channel cancel mid-queue send, order+coherence): PASS")


def chan_recv_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[ChanScene]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if sc[].ids[k] == tid:
            who = k
    if who == -1:
        red("chan-recv: unexpected task id")
    var ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(sc[].phs) + who * 8)
    claim_running(h)
    if ph[] == 0:
        ph[] = 1
        try:
            var v = sc[].rx.recv_cancellable(rt, h, sc[].token[])
            if h.state() != TaskControlBlock.WAITING:
                red("chan-recv: contended recv must park")
        except e:
            red("chan-recv: unexpected raise on first attempt: " + String(e))
        return 1
    try:
        var v2 = sc[].rx.recv_cancellable(rt, h, sc[].token[])
        if not v2:
            red("chan-recv: granted re-entry must return a value")
        var i = sc[].norder[]
        sc[].order[i] = who
        sc[].norder[] = i + 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    except e:
        if not is_cancellation(e):
            red("chan-recv: re-entry raised a non-cancellation error: " + String(e))
        sc[].cancelled[who] = 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_channel_recv() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var ch = make_channel[Int](1)
    var tx = ch.sender()
    var rx = ch.receiver()
    var recv_flag = make_cancel_flag()
    var token = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=recv_flag))
    var sc = ChanScene()
    sc.tx = tx
    sc.rx = rx
    sc.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.phs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    sc.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 9 * 8)
    sc.cancelled = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 10 * 8)
    sc.token = UnsafePointer[CancellationToken, MutAnyOrigin](to=token)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[ChanScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    _ = scheduler_loop(rt, chan_recv_dispatch, ud)
    if ch.recv_waiters_len() != 3:
        red("chan-recv: expected 3 queued receivers, got " + String(ch.recv_waiters_len()))

    var won = sc.rx.cancel_recv_wait(rt, h_w1)
    if not won:
        red("chan-recv: cancel_recv_wait must win against a still-queued waiter")
    if ch.recv_waiters_len() != 2:
        red("chan-recv: cancelled receiver not removed from the FIFO")
    if sc.rx.cancel_recv_wait(rt, h_w1):
        red("chan-recv: cancel_recv_wait must not win twice")

    _ = scheduler_loop(rt, chan_recv_dispatch, ud)
    if buf[11] != 1:
        red("chan-recv: w1 must observe the cancellation")

    if not sc.tx.try_send(1):
        red("chan-recv: send #1 failed")
    _drain_channel(rt, ch)
    _ = scheduler_loop(rt, chan_recv_dispatch, ud)
    if not sc.tx.try_send(2):
        red("chan-recv: send #2 failed")
    _drain_channel(rt, ch)
    _ = scheduler_loop(rt, chan_recv_dispatch, ud)

    if ch.recv_waiters_len() != 0 or not ch.is_empty():
        red("chan-recv: not left coherent (drained ring, empty recv queue)")
    if not (h_w0.is_completed() and h_w2.is_completed()):
        red("chan-recv: surviving waiters did not complete")
    if buf[6] != 0 or buf[7] != 2:
        red("chan-recv: FIFO order of the SURVIVING receivers broken by the cancel (want w0,w2): "
            + String(buf[6]) + "," + String(buf[7]))

    print("T29 scenario 5 (Channel cancel mid-queue recv, order+coherence): PASS")


def main() raises:
    scenario_kernel()
    scenario_mutex()
    scenario_semaphore()
    scenario_channel_send()
    scenario_channel_recv()
