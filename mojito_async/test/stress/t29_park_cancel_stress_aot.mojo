# mojito_async/test/stress/t29_park_cancel_stress_aot.mojo
#
# A4.7 (issue #65) — park/cancel model+race battery: the cross-cutting
# volume acceptance for #57's token-aware park/sync waits.
#
# Deterministic, bounded-iteration storms (no wall-clock ties, spec D6):
#   - Storm A (Mutex): N waiters queue behind a held lock; a fixed subset
#     is cancelled WHILE STILL QUEUED (cancel_lock_wait), the rest are
#     granted through the normal FIFO unlock chain.  Asserts: every waiter
#     reaches a terminal state (no hangs / lost wakeup), exactly the
#     cancelled subset observes CancellationError (no double-raise, no
#     missed cancellation), the survivors are granted in their ORIGINAL
#     relative arrival order (the cancel never reorders the FIFO), and the
#     mutex ends coherent (unlocked, empty queue).
#   - Storm B (Semaphore): same shape over permits instead of a lock —
#     final permit accounting must be exact (no leaked/duplicated permits).
#   - Storm C (Channel): a capacity-1 channel backs up N senders; a subset
#     is cancelled mid-queue, the rest drain through the ring FIFO — ring
#     and waiter-queue state must end coherent (empty, no stray waiters).
#
# Each storm reuses the SAME cancel_*_wait + *_cancellable primitives the
# t29_cancel_park.mojo unit acceptance exercises pairwise; this battery is
# the VOLUME cross-check (exactly-one-winner and no lost wakeup hold at
# scale, not just for 3 waiters).
#
# RENAMED from t29_park_cancel_stress.mojo to t29_park_cancel_stress_aot.mojo
# (A4 merge, 2026-08-28): each storm's N=600 TCB cells now live in a
# malloc/free heap pool (see the TCB_STRIDE comment below) instead of
# `stack_allocation[N, TB]()` — fixing a b2 stack_allocation oversized-
# array crash the merge onto current main exposed (TaskControlBlock grew
# to 136 bytes once A3's fields landed, pushing this storm's pool past the
# threshold, exactly the t32_injection_aot/t11_stress_aot/t33_steal_aot bug
# class).  The malloc/free `@extern`s are local to this driver, but this
# driver's own storms still crash under `mojo run` regardless (b2 JIT
# instability at this task-storm volume, not the modular/modular#6971
# import-indirection case those other drivers hit) — AOT (`mojo build` +
# execute) is reliably green, matching run.sh's existing `_aot.mojo` glob
# convention for drivers that cannot run under the JIT loop.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.cancellation import CancelFlag, CancellationToken, is_cancellation, make_cancel_flag
from mojito_async.channel import Channel, Receiver, Sender, make_channel
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import Mutex, Semaphore
from mojito_async.task import JoinHandle, claim_running, spawn


# A4 merge fix (2026-08-28): each storm's N=600 TCB cells previously lived
# in ONE `stack_allocation[N, TB]()` pool.  That was safe when this branch
# was authored against an older main where TaskControlBlock[IntResult] was
# 112 bytes (600*112 = ~67KB); merging onto the CURRENT main — where A3's
# owner_worker/owner_runtime/early/claim_epoch fields already grew TB to
# 136 bytes (600*136 = ~82KB) — tripped the SAME b2 stack_allocation
# oversized-array compiler bug already documented/fixed in
# t32_injection_aot.mojo/t11_stress_aot.mojo/t33_steal_aot.mojo (silent
# corruption / SIGSEGV, not a logic bug in this file).  HEAP-backed
# (malloc/free), the t32-proven allocation shape, same as those drivers.
# This driver still crashes under `mojo run` even after the heap-pool fix
# (see the file-header rename note) — the file is AOT-only (`_aot.mojo`
# suffix, run.sh's existing convention), not JIT-safe despite the `@extern`
# being local rather than routed through an imported module.
@extern("malloc")
def _c_malloc(size: Int) abi("C") -> BytePtr: ...


@extern("free")
def _c_free(ptr: BytePtr) abi("C"): ...


def red(what: String) raises -> None:
    print("T29 park/cancel stress: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]
comptime TCB_STRIDE = Int(256)  # generous, >= real TB size (136 currently)

comptime N = Int(600)
comptime CANCEL_EVERY = Int(3)  # every CANCEL_EVERY-th waiter is cancelled


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _should_cancel(i: Int) -> Bool:
    return i % CANCEL_EVERY == 0


# ---------------------------------------------------------------------------
# Storm A — Mutex
# ---------------------------------------------------------------------------


struct MtxStorm(ImplicitlyCopyable, ImplicitlyDeletable):
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var ids: UnsafePointer[Int, MutAnyOrigin]  # N task ids, arrival order
    var ph: UnsafePointer[Int, MutAnyOrigin]  # N phases
    var order: UnsafePointer[Int, MutAnyOrigin]  # granted arrival-index order
    var norder: UnsafePointer[Int, MutAnyOrigin]
    var ncancel: UnsafePointer[Int, MutAnyOrigin]
    var token: UnsafePointer[CancellationToken, MutAnyOrigin]

    def __init__(out self):
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ncancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.token = UnsafePointer[CancellationToken, MutAnyOrigin](unsafe_from_address=1)


def mtx_storm_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[MtxStorm]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(N):
        if sc[].ids[k] == tid:
            who = k
            break
    if who == -1:
        red("mtx-storm: unexpected task id " + String(tid))
    var ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(sc[].ph) + who * 8)
    claim_running(h)
    if ph[] == 0:
        ph[] = 1
        try:
            var got = sc[].mtx[].lock_cancellable(rt, h, sc[].token[])
            if got:
                red("mtx-storm: contended waiter must not acquire immediately")
        except e:
            red("mtx-storm: unexpected raise on first attempt: " + String(e))
        return 1
    try:
        var got2 = sc[].mtx[].lock_cancellable(rt, h, sc[].token[])
        if not got2:
            red("mtx-storm: granted re-entry did not acquire (who=" + String(who) + ")")
        var i = sc[].norder[]
        sc[].order[i] = who
        sc[].norder[] = i + 1
        _ = sc[].mtx[].unlock[IntResult](rt)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    except e:
        if not is_cancellation(e):
            red("mtx-storm: re-entry raised a non-cancellation error: " + String(e))
        sc[].ncancel[] = sc[].ncancel[] + 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def storm_mutex() raises:
    var rt = create()
    var mtx = Mutex[Int](0)
    if not mtx.try_lock():
        red("mtx-storm: pre-acquire failed")
    var flag = make_cancel_flag()
    var token = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=flag))

    var ids = List[Int]()
    var ph = List[Int]()
    var order = List[Int]()
    for _ in range(N):
        ids.append(0)
        ph.append(0)
        order.append(0)
    var norder = 0
    var ncancel = 0

    var sc = MtxStorm()
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.ids = UnsafePointer[Int, MutAnyOrigin](to=ids[0])
    sc.ph = UnsafePointer[Int, MutAnyOrigin](to=ph[0])
    sc.order = UnsafePointer[Int, MutAnyOrigin](to=order[0])
    sc.norder = UnsafePointer[Int, MutAnyOrigin](to=norder)
    sc.ncancel = UnsafePointer[Int, MutAnyOrigin](to=ncancel)
    sc.token = UnsafePointer[CancellationToken, MutAnyOrigin](to=token)
    var scp = UnsafePointer[MtxStorm, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # A2.2 (issue #68): owner pop is LIFO — spawn in REVERSE so the deque
    # serves task 0, 1, ..., N-1 in that arrival order.  TCB cells live in
    # ONE stable stack_allocation pool (N individually-named locals is not
    # feasible at this N; the pool gives every cell a fixed address for the
    # whole storm, matching the caller-allocates-the-TCB discipline every
    # other driver in this suite already follows).
    var cells = _c_malloc(N * TCB_STRIDE)
    var tcb_pool = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=Int(cells))
    var handles = List[JoinHandle[IntResult]]()
    for _ in range(N):
        handles.append(JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=1), 0))
    var i = N - 1
    while i >= 0:
        (tcb_pool + i)[] = TB.create()
        var h = spawn(rt, tcb_pool + i, 0)
        handles[i] = h
        i -= 1
    for k in range(N):
        ids[k] = handles[k].id()

    _ = scheduler_loop(rt, mtx_storm_dispatch, ud)
    if mtx.waiter_count() != N:
        red("mtx-storm: expected " + String(N) + " queued waiters, got " + String(mtx.waiter_count()))

    var expect_cancel = 0
    for k in range(N):
        if _should_cancel(k):
            expect_cancel += 1
            var won = mtx.cancel_lock_wait(rt, handles[k])
            if not won:
                red("mtx-storm: cancel_lock_wait must win against a still-queued waiter " + String(k))
    if mtx.waiter_count() != N - expect_cancel:
        red("mtx-storm: FIFO not drained of exactly the cancelled subset")

    _ = scheduler_loop(rt, mtx_storm_dispatch, ud)  # drive every cancellation raise

    # ONE unlock cascades through every remaining (granted) waiter: each
    # re-entry unlocks again internally, chaining to the next FIFO head.
    var handed = mtx.unlock[IntResult](rt)
    if N - expect_cancel > 0 and not handed:
        red("mtx-storm: first unlock must hand off (survivors remain)")
    _ = scheduler_loop(rt, mtx_storm_dispatch, ud)

    if mtx.is_locked() or mtx.waiter_count() != 0:
        red("mtx-storm: not left coherent after the storm")
    if ncancel != expect_cancel:
        red("mtx-storm: cancelled count mismatch: got " + String(ncancel)
            + ", expected " + String(expect_cancel))
    if norder != N - expect_cancel:
        red("mtx-storm: granted count mismatch: got " + String(norder)
            + ", expected " + String(N - expect_cancel))
    for k in range(N):
        if not handles[k].is_completed():
            red("mtx-storm: waiter " + String(k) + " never reached COMPLETED (lost wakeup)")
    # the surviving waiters' granted order must be their ORIGINAL relative
    # arrival order (skipping the cancelled indices) — the cancel storm must
    # never reorder the FIFO.
    var expect_idx = 0
    for k in range(N):
        if _should_cancel(k):
            continue
        if order[expect_idx] != k:
            red("mtx-storm: FIFO order broken at survivor slot "
                + String(expect_idx) + " (want " + String(k) + ", got "
                + String(order[expect_idx]) + ")")
        expect_idx += 1
    if rt.pending() != 0:
        red("mtx-storm: runnable queue not quiet after the storm")

    _c_free(cells)
    print("T29 storm A (Mutex, N=" + String(N) + "): PASS")


# ---------------------------------------------------------------------------
# Storm B — Semaphore
# ---------------------------------------------------------------------------


struct SemStorm(ImplicitlyCopyable, ImplicitlyDeletable):
    var sem: UnsafePointer[Semaphore, MutAnyOrigin]
    var ids: UnsafePointer[Int, MutAnyOrigin]
    var ph: UnsafePointer[Int, MutAnyOrigin]
    var order: UnsafePointer[Int, MutAnyOrigin]
    var norder: UnsafePointer[Int, MutAnyOrigin]
    var ncancel: UnsafePointer[Int, MutAnyOrigin]
    var token: UnsafePointer[CancellationToken, MutAnyOrigin]

    def __init__(out self):
        self.sem = UnsafePointer[Semaphore, MutAnyOrigin](unsafe_from_address=1)
        self.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ncancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.token = UnsafePointer[CancellationToken, MutAnyOrigin](unsafe_from_address=1)


def sem_storm_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SemStorm]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(N):
        if sc[].ids[k] == tid:
            who = k
            break
    if who == -1:
        red("sem-storm: unexpected task id " + String(tid))
    var ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(sc[].ph) + who * 8)
    claim_running(h)
    if ph[] == 0:
        ph[] = 1
        try:
            var got = sc[].sem[].acquire_cancellable(rt, h, sc[].token[])
            if got:
                red("sem-storm: contended waiter must not acquire immediately")
        except e:
            red("sem-storm: unexpected raise on first attempt: " + String(e))
        return 1
    try:
        var got2 = sc[].sem[].acquire_cancellable(rt, h, sc[].token[])
        if not got2:
            red("sem-storm: granted re-entry did not acquire (who=" + String(who) + ")")
        var i = sc[].norder[]
        sc[].order[i] = who
        sc[].norder[] = i + 1
        _ = sc[].sem[].release[IntResult](rt)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    except e:
        if not is_cancellation(e):
            red("sem-storm: re-entry raised a non-cancellation error: " + String(e))
        sc[].ncancel[] = sc[].ncancel[] + 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def storm_semaphore() raises:
    var rt = create()
    var sem = Semaphore(1)
    if not sem.try_acquire():
        red("sem-storm: pre-acquire failed")
    var flag = make_cancel_flag()
    var token = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=flag))

    var ids = List[Int]()
    var ph = List[Int]()
    var order = List[Int]()
    for _ in range(N):
        ids.append(0)
        ph.append(0)
        order.append(0)
    var norder = 0
    var ncancel = 0

    var sc = SemStorm()
    sc.sem = UnsafePointer[Semaphore, MutAnyOrigin](to=sem)
    sc.ids = UnsafePointer[Int, MutAnyOrigin](to=ids[0])
    sc.ph = UnsafePointer[Int, MutAnyOrigin](to=ph[0])
    sc.order = UnsafePointer[Int, MutAnyOrigin](to=order[0])
    sc.norder = UnsafePointer[Int, MutAnyOrigin](to=norder)
    sc.ncancel = UnsafePointer[Int, MutAnyOrigin](to=ncancel)
    sc.token = UnsafePointer[CancellationToken, MutAnyOrigin](to=token)
    var scp = UnsafePointer[SemStorm, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var cells_b = _c_malloc(N * TCB_STRIDE)
    var tcb_pool = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=Int(cells_b))
    var handles = List[JoinHandle[IntResult]]()
    for _ in range(N):
        handles.append(JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=1), 0))
    var i = N - 1
    while i >= 0:
        (tcb_pool + i)[] = TB.create()
        var h = spawn(rt, tcb_pool + i, 0)
        handles[i] = h
        i -= 1
    for k in range(N):
        ids[k] = handles[k].id()

    _ = scheduler_loop(rt, sem_storm_dispatch, ud)
    if sem.waiter_count() != N:
        red("sem-storm: expected " + String(N) + " queued waiters, got " + String(sem.waiter_count()))

    var expect_cancel = 0
    for k in range(N):
        if _should_cancel(k):
            expect_cancel += 1
            var won = sem.cancel_acquire_wait(rt, handles[k])
            if not won:
                red("sem-storm: cancel_acquire_wait must win against a still-queued waiter " + String(k))

    _ = scheduler_loop(rt, sem_storm_dispatch, ud)  # drive every cancellation raise

    var handed = sem.release[IntResult](rt)
    if N - expect_cancel > 0 and not handed:
        red("sem-storm: first release must hand off (survivors remain)")
    _ = scheduler_loop(rt, sem_storm_dispatch, ud)

    if sem.available() != 1 or sem.waiter_count() != 0:
        red("sem-storm: not left coherent after the storm (available=" + String(sem.available()) + ")")
    if ncancel != expect_cancel:
        red("sem-storm: cancelled count mismatch: got " + String(ncancel) + ", expected " + String(expect_cancel))
    if norder != N - expect_cancel:
        red("sem-storm: granted count mismatch: got " + String(norder) + ", expected " + String(N - expect_cancel))
    for k in range(N):
        if not handles[k].is_completed():
            red("sem-storm: waiter " + String(k) + " never reached COMPLETED (lost wakeup)")
    var expect_idx = 0
    for k in range(N):
        if _should_cancel(k):
            continue
        if order[expect_idx] != k:
            red("sem-storm: FIFO order broken at survivor slot " + String(expect_idx)
                + " (want " + String(k) + ", got " + String(order[expect_idx]) + ")")
        expect_idx += 1
    if rt.pending() != 0:
        red("sem-storm: runnable queue not quiet after the storm")

    _c_free(cells_b)
    print("T29 storm B (Semaphore, N=" + String(N) + "): PASS")


# ---------------------------------------------------------------------------
# Storm C — Channel (capacity-1, N backed-up senders)
# ---------------------------------------------------------------------------


struct ChanStorm(ImplicitlyCopyable, ImplicitlyDeletable):
    var tx: Sender[Int]
    var rx: Receiver[Int]
    var ids: UnsafePointer[Int, MutAnyOrigin]
    var ph: UnsafePointer[Int, MutAnyOrigin]
    var order: UnsafePointer[Int, MutAnyOrigin]
    var norder: UnsafePointer[Int, MutAnyOrigin]
    var ncancel: UnsafePointer[Int, MutAnyOrigin]
    var token: UnsafePointer[CancellationToken, MutAnyOrigin]

    def __init__(out self):
        self.tx = Sender[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.rx = Receiver[Int](UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1))
        self.ids = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.order = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.norder = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ncancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.token = UnsafePointer[CancellationToken, MutAnyOrigin](unsafe_from_address=1)


def _drain_channel_storm(mut rt: Runtime, mut ch: Channel[Int]) raises:
    while ch.to_wake_len() > 0:
        var wr = ch.pop_to_wake()
        if wr.task_id == 0:
            break
        var hp = _handle(wr.tcb_addr, wr.task_id)
        unpark_current(rt, hp)


def chan_storm_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[ChanStorm]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(N):
        if sc[].ids[k] == tid:
            who = k
            break
    if who == -1:
        red("chan-storm: unexpected task id " + String(tid))
    var ph = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(sc[].ph) + who * 8)
    claim_running(h)
    if ph[] == 0:
        ph[] = 1
        try:
            sc[].tx.send_cancellable(rt, h, who, sc[].token[])
            if h.state() != TaskControlBlock.WAITING:
                red("chan-storm: contended send must park")
        except e:
            red("chan-storm: unexpected raise on first attempt: " + String(e))
        return 1
    try:
        sc[].tx.send_cancellable(rt, h, who, sc[].token[])
        var i = sc[].norder[]
        sc[].order[i] = who
        sc[].norder[] = i + 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    except e:
        if not is_cancellation(e):
            red("chan-storm: re-entry raised a non-cancellation error: " + String(e))
        sc[].ncancel[] = sc[].ncancel[] + 1
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def storm_channel() raises:
    var rt = create()
    var ch = make_channel[Int](1)
    if not ch.try_send(-1):
        red("chan-storm: prefill failed")
    var tx = ch.sender()
    var rx = ch.receiver()
    var flag = make_cancel_flag()
    var token = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=flag))

    var ids = List[Int]()
    var ph = List[Int]()
    var order = List[Int]()
    for _ in range(N):
        ids.append(0)
        ph.append(0)
        order.append(0)
    var norder = 0
    var ncancel = 0

    var sc = ChanStorm()
    sc.tx = tx
    sc.rx = rx
    sc.ids = UnsafePointer[Int, MutAnyOrigin](to=ids[0])
    sc.ph = UnsafePointer[Int, MutAnyOrigin](to=ph[0])
    sc.order = UnsafePointer[Int, MutAnyOrigin](to=order[0])
    sc.norder = UnsafePointer[Int, MutAnyOrigin](to=norder)
    sc.ncancel = UnsafePointer[Int, MutAnyOrigin](to=ncancel)
    sc.token = UnsafePointer[CancellationToken, MutAnyOrigin](to=token)
    var scp = UnsafePointer[ChanStorm, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var cells_c = _c_malloc(N * TCB_STRIDE)
    var tcb_pool = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=Int(cells_c))
    var handles = List[JoinHandle[IntResult]]()
    for _ in range(N):
        handles.append(JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=1), 0))
    var i = N - 1
    while i >= 0:
        (tcb_pool + i)[] = TB.create()
        var h = spawn(rt, tcb_pool + i, 0)
        handles[i] = h
        i -= 1
    for k in range(N):
        ids[k] = handles[k].id()

    _ = scheduler_loop(rt, chan_storm_dispatch, ud)
    if ch.send_waiters_len() != N:
        red("chan-storm: expected " + String(N) + " queued senders, got " + String(ch.send_waiters_len()))

    var expect_cancel = 0
    for k in range(N):
        if _should_cancel(k):
            expect_cancel += 1
            var won = sc.tx.cancel_send_wait(rt, handles[k])
            if not won:
                red("chan-storm: cancel_send_wait must win against a still-queued waiter " + String(k))

    _ = scheduler_loop(rt, chan_storm_dispatch, ud)  # drive every cancellation raise

    # drain the ring one recv at a time: each recv frees one slot and defers
    # a wake for the next queued sender (channel.mojo never wakes in-method).
    var drained = 0
    var v0 = rx.try_recv()  # the prefilled sentinel
    if not v0:
        red("chan-storm: prefill drain failed")
    _drain_channel_storm(rt, ch)
    _ = scheduler_loop(rt, chan_storm_dispatch, ud)
    while drained < N - expect_cancel:
        var v = rx.try_recv()
        if not v:
            red("chan-storm: expected a value at drain step " + String(drained))
        drained += 1
        _drain_channel_storm(rt, ch)
        _ = scheduler_loop(rt, chan_storm_dispatch, ud)

    if ch.send_waiters_len() != 0 or not ch.is_empty():
        red("chan-storm: not left coherent after the storm")
    if ncancel != expect_cancel:
        red("chan-storm: cancelled count mismatch: got " + String(ncancel) + ", expected " + String(expect_cancel))
    if norder != N - expect_cancel:
        red("chan-storm: granted count mismatch: got " + String(norder) + ", expected " + String(N - expect_cancel))
    for k in range(N):
        if not handles[k].is_completed():
            red("chan-storm: waiter " + String(k) + " never reached COMPLETED (lost wakeup)")
    var expect_idx = 0
    for k in range(N):
        if _should_cancel(k):
            continue
        if order[expect_idx] != k:
            red("chan-storm: FIFO order broken at survivor slot " + String(expect_idx)
                + " (want " + String(k) + ", got " + String(order[expect_idx]) + ")")
        expect_idx += 1
    if rt.pending() != 0:
        red("chan-storm: runnable queue not quiet after the storm")

    _c_free(cells_c)
    print("T29 storm C (Channel, N=" + String(N) + "): PASS")


def main() raises:
    storm_mutex()
    storm_semaphore()
    storm_channel()
