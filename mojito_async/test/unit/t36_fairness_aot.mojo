# mojito_async/test/unit/t36_fairness_aot.mojo
#
# A2.7 (issue #73) — SCHEDULER FAIRNESS BUDGET: acceptance driver.
#
# Proves the issue #73 exit criteria end to end on ONE worker (manual
# runtime + queues; the E1 pool integration is the sibling lanes' seam):
#
#   1. TIMER UNDER CPU SATURATION — one worker runs an endless LOCAL task
#      that never yields (its dispatcher re-queues the same RUNNABLE record
#      every slice; no yield_now, no park, no WAITING); a timer_service
#      deadline is armed SHORT (the sleeper parked before the hog was
#      spawned); the fair loop's budget (K locally-sourced slices) forces a
#      service pass — the deadline FIRES within the budget window and the
#      sleeper COMPLETES while the hog is still RUNNABLE (no timer
#      starvation).  The hog accumulates starvation_events (it exceeds K
#      consecutive slices with no cooperative handoff of its own — the
#      documented §67/§71 cooperative limitation).
#   2. WORK-CLASS INTERLEAVING — continuous local CPU work + a delivered
#      remote wake + an injected task (+ a remote wake delivered MID-RUN by
#      the hog itself): every non-local slice runs inside the K-local budget
#      window, never indefinitely deferred.  Work-class slices are accounted
#      (local vs remote vs injection) on the runtime.
#   3. STARVATION WATCH — a NEVER-YIELDING local task crossing the budget K
#      bumps starvation_events; a YIELDING busy task (yield_now every slice)
#      never bumps it (the cooperative reset).
#   4. YIELD PARITY (A1) — yield_now still cooperatively reschedules
#      (RUNNING -> PARKING -> RUNNABLE + re-enqueue; the fair loop serves
#      the yielded record to completion) — the A1.1 surface, unchanged.
#
# The budget drive is `fair_scheduler_loop` (runtime/scheduler.mojo): the
# worker-loop fairness budget (spec §21/§67) — after K locally-sourced
# slices it sweeps remote-ready, the injection intake, and the caller's
# timer/reactor service callback before resuming local work, and it runs a
# FINAL sweep when the drain is quiet (the E6 idle-sleep handoff: sleep
# only after the fair drain is empty — the sibling lane parks the OS thread
# AFTER this loop returns).
#
# Deterministic virtual-clock stepping (spec §76.5): the service callback
# advances the caller-owned MonotonicClock cell past the armed deadline,
# then service_timers pops the due timer and wakes the sleeper through the
# canonical park/wake (unpark_current -> REMOTE-ready queue, issue #68).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.runtime.config import RuntimeConfig
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import (
    fair_scheduler_loop,
    scheduler_loop,
    yield_now,
)
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Duration
from mojito_async.time.sleep import sleep_current
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.time.timer_service import service_timers


def red(what: String) raises -> None:
    print("T36 fairness: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

# Event codes for the interleaving log.
comptime EV_HOG = Int(1)
comptime EV_SLEEPER_PARK = Int(2)
comptime EV_SLEEPER_DONE = Int(3)
comptime EV_SWEEP = Int(4)
comptime EV_REMOTE1 = Int(5)
comptime EV_INJECT = Int(6)
comptime EV_REMOTE2 = Int(7)

# Scene layout inside the driver-owned Int buffer:
#   log region   [0, 192)  — ordered event log (EV_* codes)
#   cell 192 = n            — log length
#   cell 193 = hog_count    — hog slices served so far
#   cell 194 = hog_limit    — hog re-queue bound (bounded "endless" hog)
#   cell 195 = hog_id / 196 = sleeper_id / 197 = remote1_id
#   cell 198 = inject_id / 199 = remote2_id / 200 = remote2_addr
#   cell 201 = sleeper_phase
comptime S_N = Int(192)
comptime S_COUNT = Int(193)
comptime S_LIMIT = Int(194)
comptime S_HOG = Int(195)
comptime S_SLEEP = Int(196)
comptime S_R1 = Int(197)
comptime S_INJ = Int(198)
comptime S_R2 = Int(199)
comptime S_R2ADDR = Int(200)
comptime S_PHASE = Int(201)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var buf: UnsafePointer[Int, MutAnyOrigin]
    var heap: UnsafePointer[TimerHeap, MutAnyOrigin]
    var clock: MonotonicClock

    def __init__(out self):
        self.buf = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.heap = UnsafePointer[TimerHeap, MutAnyOrigin](
            unsafe_from_address=1
        )
        self.clock = MonotonicClock(
            UnsafePointer[UInt64, MutAnyOrigin](unsafe_from_address=1)
        )


def scene_init(mut sc: Scene, buf: UnsafePointer[Int, MutAnyOrigin]):
    sc.buf = buf
    buf[S_N] = 0
    buf[S_COUNT] = 0
    buf[S_PHASE] = 0


def rec(mut sc: Scene, ev: Int):
    var i = sc.buf[S_N]
    sc.buf[i] = ev
    sc.buf[S_N] = i + 1


def log_first(mut sc: Scene, ev: Int) -> Int:
    """Index of the FIRST event `ev`, or -1."""
    var n = sc.buf[S_N]
    for i in range(n):
        if sc.buf[i] == ev:
            return i
    return -1


def log_last(mut sc: Scene, ev: Int) -> Int:
    """Index of the LAST event `ev`, or -1."""
    var n = sc.buf[S_N]
    var found = -1
    for i in range(n):
        if sc.buf[i] == ev:
            found = i
    return found


def _ptr(mut t: TB) -> UnsafePointer[TB, MutAnyOrigin]:
    return UnsafePointer[TB, MutAnyOrigin](to=t)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# --- task bodies -----------------------------------------------------------

def body_result(ud: BytePtr) raises -> IntResult:
    return IntResult(42)


def body_hog(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_HOG)
    return IntResult(4)


def body_sleeper_done(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_SLEEPER_DONE)
    return IntResult(7)


def body_remote1(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_REMOTE1)
    return IntResult(11)


def body_inject(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_INJECT)
    return IntResult(12)


def body_remote2(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_REMOTE2)
    return IntResult(13)


# --- dispatchers -------------------------------------------------------------

def dispatch_exec(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Execute any record to completion (yield-parity + quiet records)."""
    var h = _handle(tcb_addr, tid)
    _ = execute(h, body_result, ud)
    return 1


def dispatch_yield_hog(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    """A YIELDING busy task: claim + yield_now every slice (the cooperative
    escape hatch), complete on the final slice.  Never parks, never blocks —
    pure CPU saturation that cooperatively reschedules each slice."""
    var sc = ud.bitcast[Scene]()
    var h = _handle(tcb_addr, tid)
    var c = sc[].buf[S_COUNT]
    if c + 1 < sc[].buf[S_LIMIT]:
        sc[].buf[S_COUNT] = c + 1
        claim_running(h)
        rec(sc[], EV_HOG)
        yield_now(rt, h)
    else:
        sc[].buf[S_COUNT] = c + 1
        rec(sc[], EV_HOG)
        _ = execute(h, body_hog, ud)
    return 1


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """The A2.7 dispatcher: knows every task tree (b2 static dispatch).

    - hog_id      — a NEVER-YIELDING local task: re-queues its own RUNNABLE
                    record every slice (the task body's infinite loop
                    iteration runs inside the dispatcher; no yield, no
                    park); never completes.  Bounded by hog_limit so the
                    drive quiesces after the acceptance assertions.
    - sleeper_id  — phase 0: claim + sleep_current (arms the timer heap and
                    parks WAITING); phase 1: completes after the timer wake.
    - remote1 / inject / remote2 — RUNNABLE cross-class records dispatched
                    to completion (work-class interleaving).
    """
    var sc = ud.bitcast[Scene]()
    if tid == sc[].buf[S_HOG]:
        rec(sc[], EV_HOG)
        var c = sc[].buf[S_COUNT]
        if c + 1 < sc[].buf[S_LIMIT]:
            sc[].buf[S_COUNT] = c + 1
            if c + 1 == 5:
                # deliver a FRESH remote wake mid-run (test 2): the hog's
                # own work pushes a remote-ready record (guarded: test 1
                # leaves the sentinel -1, so no bogus wake is pushed).
                if sc[].buf[S_R2] >= 0:
                    rt.push_remote(sc[].buf[S_R2ADDR], sc[].buf[S_R2])
            rt.enqueue_local(tcb_addr, tid)
        else:
            sc[].buf[S_COUNT] = c + 1
        return 1
    if tid == sc[].buf[S_SLEEP]:
        var h = _handle(tcb_addr, tid)
        if sc[].buf[S_PHASE] == 0:
            claim_running(h)
            rec(sc[], EV_SLEEPER_PARK)
            sleep_current(rt, h, sc[].heap[], sc[].clock, Duration(UInt64(100)))
            sc[].buf[S_PHASE] = 1
        else:
            rec(sc[], EV_SLEEPER_DONE)
            _ = execute(h, body_sleeper_done, ud)
        return 1
    if tid == sc[].buf[S_R1]:
        var h1 = _handle(tcb_addr, tid)
        _ = execute(h1, body_remote1, ud)
        return 1
    if tid == sc[].buf[S_INJ]:
        var h2 = _handle(tcb_addr, tid)
        _ = execute(h2, body_inject, ud)
        return 1
    if tid == sc[].buf[S_R2]:
        var h3 = _handle(tcb_addr, tid)
        _ = execute(h3, body_remote2, ud)
        return 1
    raise Error("unexpected task id in fairness dispatcher")


# --- timer/reactor service sweep (the budget's service callback) ------------

def service_sweep(mut rt: Runtime, ud: BytePtr) raises:
    """The per-budget service pass: advance the virtual clock past any armed
    deadline, then service due timers (the timer/reactor sweep; a reactor
    nonblocking poll would sit here too).  Wakes land on the REMOTE-ready
    queue; fair_scheduler_loop drains them in the same phase."""
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_SWEEP)
    sc[].clock.advance(UInt64(1000000))  # 1 ms virtual step
    _ = service_timers[IntResult](rt, sc[].heap[], sc[].clock.now())


# --- test 1: timer under CPU saturation (never-yielding local hog) ----------

def test_timer_under_saturation() raises:
    var buf = stack_allocation[256, Int]()
    for zi in range(256):
        buf[zi] = 0
    var sc = Scene()
    scene_init(sc, buf)
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    sc.clock = MonotonicClock(
        UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0])
    )
    var heap = TimerHeap()
    sc.heap = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)

    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var rt = create()
    # The timer SLEEPER parks before the hog floods the deque (deterministic
    # pre-drive; the record is LOCAL, owner LIFO pops it alone).
    var tcb_s = TB.create()
    var h_s = spawn(rt, _ptr(tcb_s), 0)
    buf[S_SLEEP] = h_s.id()
    var pre = scheduler_loop(rt, dispatch, ud)
    if pre != 1:
        red("sleeper pre-drive served " + String(pre) + " != 1")
    if h_s.state() != TaskControlBlock.WAITING:
        red("sleeper did not park WAITING with an armed timer")
    if heap.size() != 1:
        red("sleeper timer not armed (heap size " + String(heap.size()) + ")")

    # The endless LOCAL CPU hog: re-queues every slice, never yields/parks.
    var tcb_hog = TB.create()
    var h_hog = spawn(rt, _ptr(tcb_hog), 0)
    buf[S_HOG] = h_hog.id()
    buf[S_LIMIT] = 60
    buf[S_R1] = -1
    buf[S_INJ] = -1
    buf[S_R2] = -1

    # Fair drive: K=4 locally-sourced slices, then service phase, etc.
    var served = fair_scheduler_loop[type_of(dispatch), type_of(service_sweep), IntResult](
        rt, dispatch, ud, service_sweep, budget_k=4, worker_id=1
    )
    if served != 61:
        red("saturation drive served " + String(served) + " != 61")
    if not h_s.is_completed():
        red("timer sleeper did NOT complete under CPU saturation")
    if not heap.is_empty():
        red("due timer left in the heap (deadline did not fire)")
    if h_hog.state() != TaskControlBlock.RUNNABLE:
        red("hog must still be RUNNABLE (never completed) when the timer fired")
    if rt.starvation_events() < 1:
        red("never-yielding hog exceeded budget K without a starvation event")
    if rt.budget_resets() < 1:
        red("budget window never reset")
    if rt.service_sweeps() < 1:
        red("service sweep never ran")
    if rt.slices_local() != 60:
        red("local slice accounting = " + String(rt.slices_local()) + " != 60")
    if rt.slices_remote() != 1:
        red("remote slice accounting = " + String(rt.slices_remote()) + " != 1")
    if rt.slices_inject() != 0:
        red("injection slice accounting != 0")
    # The deadline fired INSIDE the first budget window (sweep before hog
    # slice 5) and the sleeper ran while the hog was still going:
    var first_sweep = log_first(sc, EV_SWEEP)
    if first_sweep < 1 or first_sweep > 5:
        red("first service sweep index " + String(first_sweep) + " not in window 1")
    var sleep_done = log_first(sc, EV_SLEEPER_DONE)
    var last_hog = log_last(sc, EV_HOG)
    if sleep_done < 0 or last_hog < 0 or sleep_done > last_hog:
        red("sleeper did not complete while the hog was still running")
    # E5 parity: first-run owner stamp still applies inside the fair loop.
    if tcb_hog.owner_worker() != 1:
        red("hog owner_worker not stamped by the fair drive")
    if tcb_s.owner_worker() != 1:
        red("sleeper owner_worker not stamped by the fair drive")
    print("T36 fairness: 1. timer-under-saturation OK "
        + "(starvation_events=" + String(rt.starvation_events())
        + ", budget_resets=" + String(rt.budget_resets()) + ")")


# --- test 2: work-class interleaving within the K-local budget --------------

def test_interleaving() raises:
    var buf = stack_allocation[256, Int]()
    for zi in range(256):
        buf[zi] = 0
    var sc = Scene()
    scene_init(sc, buf)
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    sc.clock = MonotonicClock(
        UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0])
    )
    var heap = TimerHeap()
    sc.heap = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)

    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var rt = create()
    # A remote wake + an injected task are PRESENT before the drive: they
    # must run inside the FIRST K-local window (never deferred indefinitely).
    var tcb_r1 = TB.create()
    tcb_r1.transition(TaskControlBlock.RUNNABLE)
    var tcb_inj = TB.create()
    tcb_inj.transition(TaskControlBlock.RUNNABLE)
    var tcb_r2 = TB.create()
    tcb_r2.transition(TaskControlBlock.RUNNABLE)
    buf[S_R1] = 700
    buf[S_INJ] = 800
    buf[S_R2] = 777
    buf[S_R2ADDR] = Int(_ptr(tcb_r2))
    rt.push_remote(Int(_ptr(tcb_r1)), 700)
    # The E3 injection intake: #69's InjectQueue (the A1 `_ready` FIFO is
    # only the legacy enqueue() path — the fair loop's has_inject/pop_inject
    # drain the real shared bounded intake in the full tree).
    rt.enqueue_global(Int(_ptr(tcb_inj)), 800, 0)

    # The continuous LOCAL CPU hog (bounded; K=3).
    var tcb_hog = TB.create()
    var h_hog = spawn(rt, _ptr(tcb_hog), 0)
    buf[S_HOG] = h_hog.id()
    buf[S_LIMIT] = 30
    buf[S_SLEEP] = -1

    var served = fair_scheduler_loop[type_of(dispatch), type_of(service_sweep), IntResult](
        rt, dispatch, ud, service_sweep, budget_k=3, worker_id=1
    )
    if served != 33:
        red("interleave drive served " + String(served) + " != 33")
    if rt.slices_local() != 30:
        red("interleave local slices = " + String(rt.slices_local()) + " != 30")
    if rt.slices_remote() != 2:
        red("interleave remote slices = " + String(rt.slices_remote()) + " != 2")
    if rt.slices_inject() != 1:
        red("interleave inject slices = " + String(rt.slices_inject()) + " != 1")
    # Order: 3 hogs, then the budget sweep, then the pre-delivered remote,
    # then the injected task — the pre-delivered classes run inside window 1
    # (deferred by at most K local slices).
    if buf[3] != EV_SWEEP:
        red("budget sweep not at window-1 boundary (log[3]="
            + String(buf[3]) + ")")
    if buf[4] != EV_REMOTE1:
        red("remote wake not served at window-1 position (log[4]="
            + String(buf[4]) + ")")
    if buf[5] != EV_INJECT:
        red("injected task not served at window-1 position (log[5]="
            + String(buf[5]) + ")")
    # The MID-RUN remote delivery (hog slice 5, log[7]) is served in the
    # NEXT budget window (sweep at log[9], wake at log[10]) — deferred by
    # at most K local slices.
    if buf[9] != EV_SWEEP:
        red("second budget sweep missing at log[9] (got "
            + String(buf[9]) + ")")
    if buf[10] != EV_REMOTE2:
        red("mid-run remote wake not served in the next window (log[10]="
            + String(buf[10]) + ")")
    if (
        tcb_r1.state() != TaskControlBlock.COMPLETED
        or tcb_inj.state() != TaskControlBlock.COMPLETED
    ):
        red("remote/injected records did not complete")
    print("T36 fairness: 2. interleaving OK "
        + "(local=" + String(rt.slices_local())
        + ", remote=" + String(rt.slices_remote())
        + ", inject=" + String(rt.slices_inject()) + ")")


# --- test 3: starvation watch + yielding task (0 events) ---------------------

def test_starvation_watch() raises:
    var buf = stack_allocation[256, Int]()
    for zi in range(256):
        buf[zi] = 0
    var sc = Scene()
    scene_init(sc, buf)
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    sc.clock = MonotonicClock(
        UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0])
    )
    var heap = TimerHeap()
    sc.heap = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)

    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # (a) A YIELDING busy task never bumps the starve watch: cooperative
    #     reschedule every slice = the reset channel (A1 yield parity).
    var rt_y = create()
    var tcb_y = TB.create()
    var h_y = spawn(rt_y, _ptr(tcb_y), 0)
    buf[S_HOG] = h_y.id()
    buf[S_LIMIT] = 20
    buf[S_SLEEP] = -1
    buf[S_R1] = -1
    buf[S_INJ] = -1
    buf[S_R2] = -1
    var served_y = fair_scheduler_loop[
        type_of(dispatch_yield_hog), type_of(service_sweep), IntResult
    ](
        rt_y, dispatch_yield_hog, ud, service_sweep, budget_k=2, worker_id=0
    )
    if served_y != 20:
        red("yielding hog drive served " + String(served_y) + " != 20")
    if not h_y.is_completed():
        red("yielding hog did not complete")
    if rt_y.starvation_events() != 0:
        red("yielding task must NEVER bump starvation_events "
            + "(got " + String(rt_y.starvation_events()) + ")")
    if rt_y.yields() != 19:
        red("yields() = " + String(rt_y.yields()) + " != 19 (19 yielding slices)")
    if rt_y.budget_resets() < 1:
        red("yielding drive never hit a budget window")

    # (b) A1 yield_now parity, through the fair loop: RUNNING -> PARKING ->
    #     RUNNABLE + re-enqueue, then the fair loop serves it to completion.
    var rt_p = create()
    var tcb_p = TB.create()
    var h_p = spawn(rt_p, _ptr(tcb_p), 0)
    claim_running(h_p)
    _ = rt_p.pop_ready()
    if rt_p.pending() != 0:
        red("expected empty ready queue after pop (yield parity)")
    yield_now(rt_p, h_p)
    if h_p.state() != TaskControlBlock.RUNNABLE:
        red("yield_now did not reschedule to RUNNABLE (A1 parity broken)")
    if rt_p.pending() != 1:
        red("yield_now did not re-enqueue (A1 parity broken)")
    var served_p = fair_scheduler_loop[
        type_of(dispatch_exec), type_of(service_sweep), IntResult
    ](
        rt_p, dispatch_exec, ud, service_sweep, budget_k=2, worker_id=0
    )
    if served_p != 1:
        red("fair loop didn't re-serve the yielded record (got "
            + String(served_p) + ")")
    if not h_p.is_completed():
        red("yielded task did not complete on the fair re-drive")
    if rt_p.starvation_events() != 0:
        red("single yielded record must not bump starvation_events")
    if rt_p.pending() != 0:
        red("runtime not quiet after yield parity drive")
    print("T36 fairness: 3. starvation watch OK "
        + "(never-yield hog events=" + String(0)
        + ", yielding hog events=" + String(rt_y.starvation_events()) + ")")


def test_config_fair_budget_validation() raises:
    """M6 (review fold, issue #73): fair_budget_k is validated in
    RuntimeConfig.validate() — a negative K must raise (a K that would make
    the budget gate fire immediately is a config error); K=0 (budget
    disabled, plain scheduler_loop semantics) and K>=1 must validate."""
    var bad = RuntimeConfig(1, fair_budget_k=-1)
    try:
        bad.validate()
        red("negative fair_budget_k must be rejected by validate()")
    except e:
        if not String(e).startswith("RuntimeConfig.validate: fair_budget_k"):
            red("negative fair_budget_k raised the wrong error: " + String(e))
    var zero = RuntimeConfig(1, fair_budget_k=0)
    zero.validate()  # 0 = disabled, documented — must be accepted
    var ok = RuntimeConfig(1, fair_budget_k=4)
    ok.validate()
    print("T36 fairness: 4. config fair_budget_k validation OK "
        + "(negative raises, 0 = disabled accepted, K=4 accepted)")


def main() raises:
    test_timer_under_saturation()
    test_interleaving()
    test_starvation_watch()
    test_config_fair_budget_validation()
    print("T36 fairness: PASS")
