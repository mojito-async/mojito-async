# mojito_async/test/unit/t20_timer_sleep.mojo
#
# A1.4 (issue #36) — sleep CURRENT task via the timer heap, composed with
# the A1.1 runtime: spawn + scheduler drive + timer sleep + the servicing
# hook wake (issue #39 canonical park/wake).
#
# Scenario on the single cooperative worker (deterministic, virtual clock):
#   - A is spawned and, on its first entry, parks for 100 ns through
#     sleep_current (RUNNING -> PARKING -> WAITING, reason TIMER) after
#     arming the heap — it drops off the runnable queue;
#   - B runs to COMPLETION entirely inside A's sleep window (worker reuse);
#   - the virtual clock has NOT reached A's deadline: the servicing hook
#     wakes nobody (no premature wake, no spin, no OS-thread block);
#   - the driver advances the clock past the deadline; service_timers pops
#     the due timer and wakes A ONCE via the canonical unpark_current
#     (WAITING -> RUNNABLE + enqueue);
#   - A resumes on the next drive and completes; heap drained; runnable
#     queue quiet.
#
# Also asserts the §7.1 `sleep(Duration)` surface still compiles and raises
# its precise context error OUTSIDE a driven frame (no hidden blocking).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Duration
from mojito_async.time.sleep import sleep, sleep_current
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.time.timer_service import drive_step, service_timers


def red(what: String) raises -> None:
    print("T20 timer sleep: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

comptime EV_A_START = Int(1)
comptime EV_A_PARK = Int(2)
comptime EV_B_RUN = Int(3)
comptime EV_A_RESUME = Int(4)
comptime EV_A_DONE = Int(5)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var seq: UnsafePointer[Int, MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var a_id: UnsafePointer[Int, MutAnyOrigin]
    var b_id: UnsafePointer[Int, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]
    var counter: UnsafePointer[Int, MutAnyOrigin]
    var heap: UnsafePointer[TimerHeap, MutAnyOrigin]
    var clock: MonotonicClock

    def __init__(out self):
        self.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = self.seq
        self.a_id = self.seq
        self.b_id = self.seq
        self.phase = self.seq
        self.counter = self.seq
        self.heap = UnsafePointer[TimerHeap, MutAnyOrigin](
            unsafe_from_address=1
        )
        self.clock = MonotonicClock(
            UnsafePointer[UInt64, MutAnyOrigin](unsafe_from_address=1)
        )


def rec(mut sc: Scene, ev: Int):
    var i = sc.n[]
    sc.seq[i] = ev
    sc.n[] = i + 1


def a_finish(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    sc[].counter[] = sc[].counter[] + 1
    rec(sc[], EV_A_DONE)
    return IntResult(2)


def b_run(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_B_RUN)
    return IntResult(100)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid == sc[].b_id[]:
        var hb = _handle(tcb_addr, tid)
        _ = execute(hb, b_run, ud)
        return 1
    if tid == sc[].a_id[]:
        var ha = _handle(tcb_addr, tid)
        if sc[].phase[] == 0:
            claim_running(ha)
            rec(sc[], EV_A_START)
            # park for 100 ns on the timer heap (the real A1.4 sleep).
            sleep_current(rt, ha, sc[].heap[], sc[].clock, Duration(UInt64(100)))
            rec(sc[], EV_A_PARK)
            sc[].phase[] = 1
        else:
            rec(sc[], EV_A_RESUME)
            _ = execute(ha, a_finish, ud)
        return 1
    raise Error("unexpected task id in dispatcher")


def main() raises:
    var buf = stack_allocation[512, Int]()
    var sc = Scene()
    sc.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.a_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 9 * 8)
    sc.b_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 10 * 8)
    sc.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 11 * 8)
    sc.counter = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 12 * 8)
    buf[8] = 0
    buf[11] = 0
    buf[12] = 0

    # caller-owned virtual clock cell + timer heap (addresses stable).
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    sc.clock = MonotonicClock(
        UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0])
    )
    var heap = TimerHeap()
    var hheap_ptr = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)
    sc.heap = hheap_ptr

    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var rt = create()
    var tcb_a = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    var tcb_b = TB.create()
    var h_b = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    buf[9] = h_a.id()
    buf[10] = h_b.id()

    # ---- drive 1: A parks (timer armed), B completes in the window --------
    var served = scheduler_loop(rt, dispatch, ud)
    if served != 2:
        red("first drive served " + String(served) + " records, expected 2")
    if h_a.state() != TaskControlBlock.WAITING:
        red("A should be WAITING after sleep_current")
    if h_b.state() != TaskControlBlock.COMPLETED:
        red("B did not complete inside A's sleep window")
    if rt.pending() != 0:
        red("runnable queue not quiet while A sleeps")
    if heap.size() != 1:
        red("heap should hold exactly A's timer, holds " + String(heap.size()))
    if sc.clock.now() != 0 or heap.min_deadline() <= 0:
        red("deadline should be in the future at now=0")

    # ---- no premature wake before the deadline -----------------------------
    var woke0 = service_timers[IntResult](rt, heap, 50)
    if woke0 != 0:
        red("service woke a sleeper before its deadline")
    if h_a.state() != TaskControlBlock.WAITING:
        red("A woke prematurely")

    # ---- advance virtual time past the deadline; service the hook ----------
    sc.clock.advance(UInt64(200))
    var woke = service_timers[IntResult](rt, heap, sc.clock.now())
    if woke != 1:
        red("service should wake exactly A, woke " + String(woke))
    if h_a.state() != TaskControlBlock.RUNNABLE:
        red("A not RUNNABLE after timer wake")
    if rt.pending() != 1:
        red("wake did not re-enqueue A exactly once")
    if not heap.is_empty():
        red("expired timer left in heap")

    # ---- drive 2: A resumes and completes (via the composed drive_step) ----
    var served2 = drive_step[type_of(dispatch), IntResult](
        rt, dispatch, ud, heap, sc.clock.now()
    )
    if served2 != 1:
        red("second drive served " + String(served2) + " records, expected 1")
    if not h_a.is_completed():
        red("A did not complete after timer resume")
    if buf[12] != 1:
        red("A's finish body did not run")
    if rt.pending() != 0:
        red("runnable queue not quiet at end")

    # ---- event interleaving ------------------------------------------------
    if buf[0] != EV_A_START or buf[1] != EV_A_PARK or buf[2] != EV_B_RUN:
        red("interleave broken (B must run inside A's sleep window)")
    if buf[3] != EV_A_RESUME or buf[4] != EV_A_DONE:
        red("resume/done event order wrong")
    if buf[8] != 5:
        red("expected exactly 5 events, got " + String(buf[8]))

    # ---- §7.1 sleep(Duration) surface: precise context error, no block ----
    var raised = False
    try:
        sleep(Duration(UInt64(100)))
    except e:
        raised = True
        if "A1.4 timer lane" not in String(e):
            red("sleep surface error unexpected: " + String(e))
    if not raised:
        red("bare sleep(Duration) did not raise the context error")

    print("T20 timer sleep: PASS")