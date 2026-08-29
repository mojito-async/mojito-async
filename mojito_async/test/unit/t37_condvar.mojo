# mojito_async/test/unit/t37_condvar.mojo
#
# A4.6 (issue #60) — task-aware Condvar acceptance over the A1 park/wake
# kernel, composing with Mutex[T] (spec §A5).
#
# Acceptance:
#   A. Basic wait/notify_one: a waiter releases its held lock, parks; a
#      signaler acquires the (now free) lock, mutates the guarded value,
#      notify_one()s, unlocks; the waiter wakes, re-acquires the lock, sees
#      WINNER_READY and the mutated value.
#   B. FIFO fairness: 3 waiters parked on one condvar are notified strictly
#      in arrival order, one wake each — no lost wakeup, no duplicate grant.
#   C. notify_all wakes every currently-waiting task exactly once.
#   D. Lost-signal safety: notify_one/notify_all on an EMPTY condvar are a
#      safe no-op (False / 0) — never queued for a future waiter.
#   E. cancel_waiter: a mid-wait cancellation re-acquires the lock then
#      raises CancellationError exactly once; the OTHER FIFO waiters are
#      untouched and still notify/complete normally.
#   F. timeout_waiter: a mid-wait timeout re-acquires the lock and returns
#      True with cause==WINNER_TIMEOUT (no raise); other waiters unaffected.
#
# Verdict: exit 0 + "PASS"; any RED prints and raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.cancellation import is_cancellation
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import Condvar, Mutex, WINNER_CANCELLED, WINNER_READY, WINNER_TIMEOUT
from mojito_async.sync.condvar import PHASE_INIT
from mojito_async.task import JoinHandle, claim_running, spawn


def red(what: String) raises -> None:
    print("T37 condvar: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# ---------------------------------------------------------------------------
# Scenario A — single waiter + single signaler, mutex-composed wait/notify
# ---------------------------------------------------------------------------

struct SceneA(ImplicitlyCopyable, ImplicitlyDeletable):
    var cv: UnsafePointer[Condvar, MutAnyOrigin]
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var cause_w: UnsafePointer[Int, MutAnyOrigin]
    var id_wait: UnsafePointer[Int, MutAnyOrigin]
    var id_sig: UnsafePointer[Int, MutAnyOrigin]
    var ph_wait: UnsafePointer[Int, MutAnyOrigin]
    var ph_sig: UnsafePointer[Int, MutAnyOrigin]
    var woke_cause: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.cv = UnsafePointer[Condvar, MutAnyOrigin](unsafe_from_address=1)
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.cause_w = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_wait = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_sig = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_wait = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_sig = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.woke_cause = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def a_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneA]()
    var h = _handle(tcb_addr, tid)
    if h.id() == sc[].id_wait[]:
        if sc[].ph_wait[] == 0:
            claim_running(h)
            var got = sc[].mtx[].lock[IntResult](rt, h)
            if not got:
                red("waiter's initial lock must take the fast path")
            var done = sc[].cv[].wait[IntResult, Int](
                rt, sc[].mtx[], h, sc[].cause_w
            )
            if done:
                red("wait() must park on the first call (no predicate fast path)")
            sc[].ph_wait[] = 1
            return 1
        else:
            claim_running(h)
            var done = sc[].cv[].wait[IntResult, Int](
                rt, sc[].mtx[], h, sc[].cause_w
            )
            if not done:
                red("waiter did not settle after notify")
            sc[].woke_cause[] = sc[].cause_w[]
            if sc[].mtx[].value()[0] != 41:
                red("waiter observed the wrong protected value")
            _ = sc[].mtx[].unlock[IntResult](rt)
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
    if h.id() == sc[].id_sig[]:
        claim_running(h)
        var got = sc[].mtx[].lock[IntResult](rt, h)
        if not got:
            red("signaler lock must take the fast path (waiter already released)")
        sc[].mtx[].value()[0] = 41
        var woke = sc[].cv[].notify_one[IntResult](rt)
        if not woke:
            red("notify_one found no waiter")
        _ = sc[].mtx[].unlock[IntResult](rt)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        return 1
    red("unexpected task id in scenario A")
    return 0


def scenario_a() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var cv = Condvar()
    var mtx = Mutex[Int](0)
    var sc = SceneA()
    sc.cv = UnsafePointer[Condvar, MutAnyOrigin](to=cv)
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.cause_w = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.id_wait = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    sc.id_sig = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 2 * 8)
    sc.ph_wait = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    sc.ph_sig = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.woke_cause = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 5 * 8)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[SceneA, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # LIFO owner pop (A2.2, issue #68): register the signaler FIRST and the
    # waiter after -> the waiter runs first, locks, and parks (releasing the
    # mutex to the signaler) before the signaler contends.
    var tcb_sig = TB.create()
    var h_sig = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_sig), 0)
    var tcb_wait = TB.create()
    var h_wait = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_wait), 0)
    buf[1] = h_wait.id()
    buf[2] = h_sig.id()

    # Single drive: the waiter parks, the signaler runs+notifies (pushing a
    # remote-ready wake), and the same scheduler_loop drains that wake too.
    _ = scheduler_loop(rt, a_dispatch, ud)

    if not h_sig.is_completed():
        red("signaler did not complete")
    if not h_wait.is_completed():
        red("waiter did not complete after notify")
    if buf[5] != WINNER_READY:
        red("waiter did not observe WINNER_READY")
    if mtx.is_locked() or mtx.waiter_count() != 0:
        red("mutex not free & drained after the cv round-trip")
    if cv.waiter_count() != 0:
        red("condvar FIFO not drained")
    print("T37 condvar scenario A (basic wait/notify_one): PASS")


# ---------------------------------------------------------------------------
# Scenario B — FIFO fairness: 3 waiters, notified one at a time in order
# ---------------------------------------------------------------------------

struct SceneB(ImplicitlyCopyable, ImplicitlyDeletable):
    var cv: UnsafePointer[Condvar, MutAnyOrigin]
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var ids0: UnsafePointer[Int, MutAnyOrigin]
    var causes0: UnsafePointer[Int, MutAnyOrigin]
    var phs0: UnsafePointer[Int, MutAnyOrigin]
    var order0: UnsafePointer[Int, MutAnyOrigin]
    var npassed: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.cv = UnsafePointer[Condvar, MutAnyOrigin](unsafe_from_address=1)
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.causes0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.order0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.npassed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def b_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneB]()
    var h = _handle(tcb_addr, tid)
    var who = -1
    for k in range(3):
        if h.id() == sc[].ids0[k]:
            who = k
    if who == -1:
        red("unknown waiter id in scenario B")
    var ph = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].phs0) + who * 8
    )
    var cause = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(sc[].causes0) + who * 8
    )
    if ph[] == 0:
        claim_running(h)
        var got = sc[].mtx[].lock[IntResult](rt, h)
        if not got:
            red("waiter's initial lock must take the fast path (uncontended in turn)")
        var done = sc[].cv[].wait[IntResult, Int](rt, sc[].mtx[], h, cause)
        if done:
            red("wait() must park on the first call")
        ph[] = 1
        return 1
    claim_running(h)
    var done = sc[].cv[].wait[IntResult, Int](rt, sc[].mtx[], h, cause)
    if not done:
        red("granted waiter did not settle (handoff broken) who=" + String(who))
    sc[].order0[who] = sc[].npassed[]
    sc[].npassed[] = sc[].npassed[] + 1
    _ = sc[].mtx[].unlock[IntResult](rt)
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def scenario_b() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var cv = Condvar()
    var mtx = Mutex[Int](0)
    var sc = SceneB()
    sc.cv = UnsafePointer[Condvar, MutAnyOrigin](to=cv)
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.causes0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.order0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    sc.npassed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16 * 8)
    for zi in range(24):
        buf[zi] = 0
    var scp = UnsafePointer[SceneB, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # Reverse-spawn (A2.2, issue #68 LIFO local deque): the deque then
    # SERVES w0, w1, w2 in that order, so they arrive (lock+wait) on the
    # condvar FIFO in that order too.
    var tcb_w2 = TB.create()
    var h_w2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w2), 0)
    var tcb_w1 = TB.create()
    var h_w1 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w1), 0)
    var tcb_w0 = TB.create()
    var h_w0 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_w0), 0)
    buf[0] = h_w0.id()
    buf[1] = h_w1.id()
    buf[2] = h_w2.id()

    _ = scheduler_loop(rt, b_dispatch, ud)
    if cv.waiter_count() != 3:
        red("expected 3 condvar waiters: " + String(cv.waiter_count()))
    if buf[16] != 0:
        red("no waiter should pass before any notify")

    # Drive the notify chain step by step: each notify_one wakes the FIFO
    # head; redrive the scheduler so it settles (re-acquires the lock) and
    # unlocks before the next notify.
    for k in range(3):
        var woke = cv.notify_one[IntResult](rt)
        if not woke:
            red("notify_one " + String(k) + " found no waiter")
        _ = scheduler_loop(rt, b_dispatch, ud)

    if cv.waiter_count() != 0:
        red("condvar FIFO not drained after the notify chain")
    if buf[16] != 3:
        red("not all three waiters passed: " + String(buf[16]))
    if buf[12] != 0 or buf[13] != 1 or buf[14] != 2:
        red("notify order not FIFO (expected 0,1,2)")
    if not (h_w0.is_completed() and h_w1.is_completed() and h_w2.is_completed()):
        red("not all waiters completed")
    print("T37 condvar scenario B (FIFO fairness): PASS")


# ---------------------------------------------------------------------------
# Scenario C — notify_all wakes every currently-waiting task exactly once
# ---------------------------------------------------------------------------

def scenario_c() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var cv = Condvar()
    var mtx = Mutex[Int](0)
    var sc = SceneB()
    sc.cv = UnsafePointer[Condvar, MutAnyOrigin](to=cv)
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.ids0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.causes0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.phs0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.order0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    sc.npassed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16 * 8)
    for zi in range(24):
        buf[zi] = 0
    var scp = UnsafePointer[SceneB, MutAnyOrigin](to=sc)
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

    _ = scheduler_loop(rt, b_dispatch, ud)
    if cv.waiter_count() != 3:
        red("expected 3 condvar waiters: " + String(cv.waiter_count()))

    var n = cv.notify_all[IntResult](rt)
    if n != 3:
        red("notify_all should have woken exactly 3, woke " + String(n))
    if cv.waiter_count() != 0:
        red("condvar FIFO not drained immediately by notify_all")

    # The mutex reacquire may itself chain through the mutex's own FIFO
    # (all 3 were released "simultaneously" by notify_all); drive until
    # every waiter settles — bounded to a small constant, no flakiness.
    for _ in range(4):
        if not (h_w0.is_completed() and h_w1.is_completed() and h_w2.is_completed()):
            _ = scheduler_loop(rt, b_dispatch, ud)

    if not (h_w0.is_completed() and h_w1.is_completed() and h_w2.is_completed()):
        red("not all waiters completed after notify_all")
    if buf[16] != 3:
        red("not all three waiters passed: " + String(buf[16]))
    if mtx.is_locked() or mtx.waiter_count() != 0:
        red("mutex not free & drained after notify_all handoff chain")
    print("T37 condvar scenario C (notify_all wakes every waiter once): PASS")


# ---------------------------------------------------------------------------
# Scenario D — lost-signal safety: notify on an empty condvar is a no-op
# ---------------------------------------------------------------------------

def scenario_d() raises:
    var rt = create()
    var cv = Condvar()
    if cv.notify_one[IntResult](rt):
        red("notify_one on an empty condvar must return False")
    if cv.notify_all[IntResult](rt) != 0:
        red("notify_all on an empty condvar must wake 0")
    print("T37 condvar scenario D (lost-signal safety): PASS")


# ---------------------------------------------------------------------------
# Scenario E — cancel_waiter: mid-wait cancellation raises exactly once;
# the other FIFO waiter is untouched.
# ---------------------------------------------------------------------------

struct SceneE(ImplicitlyCopyable, ImplicitlyDeletable):
    var cv: UnsafePointer[Condvar, MutAnyOrigin]
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var id_ok: UnsafePointer[Int, MutAnyOrigin]
    var id_cancel: UnsafePointer[Int, MutAnyOrigin]
    var ph_ok: UnsafePointer[Int, MutAnyOrigin]
    var ph_cancel: UnsafePointer[Int, MutAnyOrigin]
    var cause_ok: UnsafePointer[Int, MutAnyOrigin]
    var cause_cancel: UnsafePointer[Int, MutAnyOrigin]
    var ok_done: UnsafePointer[Int, MutAnyOrigin]
    var cancel_raised: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.cv = UnsafePointer[Condvar, MutAnyOrigin](unsafe_from_address=1)
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.id_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ph_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cause_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cause_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.ok_done = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cancel_raised = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def e_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneE]()
    var h = _handle(tcb_addr, tid)
    if h.id() == sc[].id_ok[]:
        if sc[].ph_ok[] == 0:
            claim_running(h)
            _ = sc[].mtx[].lock[IntResult](rt, h)
            _ = sc[].cv[].wait[IntResult, Int](rt, sc[].mtx[], h, sc[].cause_ok)
            sc[].ph_ok[] = 1
            return 1
        else:
            claim_running(h)
            var done = sc[].cv[].wait[IntResult, Int](rt, sc[].mtx[], h, sc[].cause_ok)
            if not done:
                return 1
            if sc[].cause_ok[] != WINNER_READY:
                red("uncancelled waiter must observe WINNER_READY")
            sc[].ok_done[] = 1
            _ = sc[].mtx[].unlock[IntResult](rt)
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
    if h.id() == sc[].id_cancel[]:
        if sc[].ph_cancel[] == 0:
            claim_running(h)
            _ = sc[].mtx[].lock[IntResult](rt, h)
            _ = sc[].cv[].wait[IntResult, Int](rt, sc[].mtx[], h, sc[].cause_cancel)
            sc[].ph_cancel[] = 1
            return 1
        else:
            claim_running(h)
            try:
                var done = sc[].cv[].wait[IntResult, Int](
                    rt, sc[].mtx[], h, sc[].cause_cancel
                )
                if not done:
                    return 1
                red("cancelled waiter must raise, not return normally")
            except e:
                if not is_cancellation(e):
                    red("cancelled waiter raised the wrong error: " + String(e))
                sc[].cancel_raised[] = 1
                # The contract re-acquires the lock before surfacing the
                # cancellation (module header): release it here, as a real
                # caller's unwind would.
                _ = sc[].mtx[].unlock[IntResult](rt)
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
    red("unexpected task id in scenario E")
    return 0


def scenario_e() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var cv = Condvar()
    var mtx = Mutex[Int](0)
    var sc = SceneE()
    sc.cv = UnsafePointer[Condvar, MutAnyOrigin](to=cv)
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.id_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.id_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    sc.ph_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 2 * 8)
    sc.ph_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    sc.cause_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.cause_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 5 * 8)
    sc.ok_done = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.cancel_raised = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 7 * 8)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[SceneE, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # Reverse-spawn: "cancel" arrives at the condvar FIRST, "ok" second —
    # cancelling the HEAD waiter while a later one stays healthy is the
    # sharper FIFO-division test (module header: cancel must not corrupt
    # the rest of the FIFO).
    var tcb_ok = TB.create()
    var h_ok = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_ok), 0)
    var tcb_cancel = TB.create()
    var h_cancel = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_cancel), 0)
    buf[0] = h_ok.id()
    buf[1] = h_cancel.id()

    _ = scheduler_loop(rt, e_dispatch, ud)
    if cv.waiter_count() != 2:
        red("expected 2 condvar waiters: " + String(cv.waiter_count()))

    var cancelled = cv.cancel_waiter[IntResult](rt, h_cancel)
    if not cancelled:
        red("cancel_waiter did not find the parked waiter")
    if cv.waiter_count() != 1:
        red("cancel_waiter must remove ONLY the cancelled waiter")
    _ = scheduler_loop(rt, e_dispatch, ud)
    if not h_cancel.is_completed():
        red("cancelled waiter did not settle")
    if buf[7] != 1:
        red("cancelled waiter did not raise CancellationError")

    var woke = cv.notify_one[IntResult](rt)
    if not woke:
        red("the remaining waiter must still be reachable by notify_one")
    _ = scheduler_loop(rt, e_dispatch, ud)
    if not h_ok.is_completed():
        red("the uncancelled waiter did not settle")
    if buf[6] != 1:
        red("the uncancelled waiter did not observe WINNER_READY")
    if cv.waiter_count() != 0:
        red("condvar FIFO not drained")
    print("T37 condvar scenario E (cancel_waiter leaves the FIFO healthy): PASS")


# ---------------------------------------------------------------------------
# Scenario F — timeout_waiter: mid-wait timeout resolves without raising
# ---------------------------------------------------------------------------
def f_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Like e_dispatch, but the second slot (`id_cancel`/`cause_cancel`) is
    driven to a TIMEOUT settlement instead of a cancellation: a real
    condvar wait resolves via timeout_waiter WITHOUT raising, so this
    dispatcher expects a normal `wait()` return, not an exception."""
    var sc = ud.bitcast[SceneE]()
    var h = _handle(tcb_addr, tid)
    if h.id() == sc[].id_ok[]:
        return e_dispatch(rt, tcb_addr, tid, ud)
    if h.id() == sc[].id_cancel[]:
        if sc[].ph_cancel[] == 0:
            claim_running(h)
            _ = sc[].mtx[].lock[IntResult](rt, h)
            _ = sc[].cv[].wait[IntResult, Int](rt, sc[].mtx[], h, sc[].cause_cancel)
            sc[].ph_cancel[] = 1
            return 1
        else:
            claim_running(h)
            var done = sc[].cv[].wait[IntResult, Int](
                rt, sc[].mtx[], h, sc[].cause_cancel
            )
            if not done:
                return 1
            if sc[].cause_cancel[] != WINNER_TIMEOUT:
                red("timed-out waiter must observe WINNER_TIMEOUT")
            _ = sc[].mtx[].unlock[IntResult](rt)
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            return 1
    red("unexpected task id in scenario F")
    return 0



def scenario_f() raises:
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var cv = Condvar()
    var mtx = Mutex[Int](0)
    var sc = SceneE()
    sc.cv = UnsafePointer[Condvar, MutAnyOrigin](to=cv)
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.id_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.id_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    sc.ph_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 2 * 8)
    sc.ph_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    sc.cause_ok = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 4 * 8)
    sc.cause_cancel = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 5 * 8)
    sc.ok_done = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 6 * 8)
    sc.cancel_raised = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 7 * 8)
    for zi in range(16):
        buf[zi] = 0
    var scp = UnsafePointer[SceneE, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb_ok = TB.create()
    var h_ok = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_ok), 0)
    var tcb_to = TB.create()
    var h_to = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_to), 0)
    buf[0] = h_ok.id()
    buf[1] = h_to.id()

    _ = scheduler_loop(rt, f_dispatch, ud)
    if cv.waiter_count() != 2:
        red("expected 2 condvar waiters: " + String(cv.waiter_count()))

    var timed_out = cv.timeout_waiter[IntResult](rt, h_to)
    if not timed_out:
        red("timeout_waiter did not find the parked waiter")
    if cv.waiter_count() != 1:
        red("timeout_waiter must remove ONLY the timed-out waiter")
    _ = scheduler_loop(rt, f_dispatch, ud)
    if not h_to.is_completed():
        red("timed-out waiter did not settle")
    if buf[7] != 0:
        red("timeout must not raise CancellationError")
    if buf[5] != WINNER_TIMEOUT:
        red("timed-out waiter's cause must be WINNER_TIMEOUT")

    var woke = cv.notify_one[IntResult](rt)
    if not woke:
        red("the remaining waiter must still be reachable by notify_one")
    _ = scheduler_loop(rt, f_dispatch, ud)
    if not h_ok.is_completed():
        red("the untimed-out waiter did not settle")
    if buf[6] != 1:
        red("the untimed-out waiter did not observe WINNER_READY")
    print("T37 condvar scenario F (timeout_waiter leaves the FIFO healthy): PASS")


def main() raises:
    scenario_a()
    scenario_b()
    scenario_c()
    scenario_d()
    scenario_e()
    scenario_f()
