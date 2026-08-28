# mojito_async/test/unit/t21_timer_cancel.mojo
#
# A1.4 (issue #36) — timer cancellation semantics end-to-end:
#   - a parked (sleeping) task's timer CAN be removed by cancel(id) /
#     cancel_token(live gen) BEFORE expiry: the heap drains, the servicing
#     hook wakes nobody, and the sleeper stays parked (a cancelled timer
#     never fires);
#   - a STALE generation token (cancel_token with an old gen) does NOT
#     cancel the live timer — only the live-gen token (or whole-id cancel)
#     removes it (generation suppression on the cancel path);
#   - after cancellation the same task can be re-armed and resumed normally
#     (cancellation does not poison the task or the heap).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Duration
from mojito_async.time.sleep import sleep_current
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.time.timer_service import service_timers


def red(what: String) raises -> None:
    print("T21 timer cancel: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var a_id: UnsafePointer[Int, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]
    var count: UnsafePointer[Int, MutAnyOrigin]
    var heap: UnsafePointer[TimerHeap, MutAnyOrigin]
    var clock: MonotonicClock

    def __init__(out self):
        self.a_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phase = self.a_id
        self.count = self.a_id
        self.heap = UnsafePointer[TimerHeap, MutAnyOrigin](
            unsafe_from_address=1
        )
        self.clock = MonotonicClock(
            UnsafePointer[UInt64, MutAnyOrigin](unsafe_from_address=1)
        )


def a_only(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    sc[].count[] = sc[].count[] + 1
    return IntResult(7)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid == sc[].a_id[]:
        var ha = _handle(tcb_addr, tid)
        if sc[].phase[] == 0:
            # first entry: arm timer1 (100 ns) and park.
            claim_running(ha)
            sleep_current(rt, ha, sc[].heap[], sc[].clock, Duration(UInt64(100)))
            sc[].phase[] = 1
        elif sc[].phase[] == 1:
            # resumed AFTER the cancelled timer1 (main resumed us); re-arm a
            # fresh 50 ns timer and park again.
            claim_running(ha)
            sleep_current(rt, ha, sc[].heap[], sc[].clock, Duration(UInt64(50)))
            sc[].phase[] = 2
        else:
            # final resume: run the body to completion.
            _ = execute(ha, a_only, ud)
        return 1
    raise Error("unexpected task id in dispatcher")


def main() raises:
    var buf = stack_allocation[512, Int]()
    var sc = Scene()
    sc.a_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8)

    buf[2] = 0  # count
    sc.count = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16)
    buf[1] = 0  # phase

    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    sc.clock = MonotonicClock(
        UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0])
    )
    var heap = TimerHeap()
    var hp = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)
    sc.heap = hp

    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var rt = create()
    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    buf[0] = h.id()

    # ---- drive 1: A arms a 100 ns timer and parks --------------------------
    var served = scheduler_loop(rt, dispatch, ud)
    if served != 1:
        red("first drive served " + String(served) + " records, expected 1")
    if h.state() != TaskControlBlock.WAITING:
        red("A not WAITING after park")
    if heap.size() != 1:
        red("expected one armed timer")

    # ---- live-keyed cancel removes the timer (generation-aware) ------------
    var live = heap.live_gen(h.id())
    if not heap.cancel_token(h.id(), live):
        red("cancel_token(live gen) failed to remove the timer")
    if heap.size() != 0:
        red("cancelled timer still in heap")
    if heap.live_gen(h.id()) != 0:
        red("cancel did not clear the live registration")

    # ---- stale-token path: an old gen must NOT remove a live timer ---------
    var heap2 = TimerHeap()
    var stale = heap2.arm(99, 0x99, 500)
    if heap2.cancel_token(99, stale - 1):
        red("stale generation token wrongly cancelled a live timer")
    if heap2.size() != 1:
        red("stale token removed the live timer")

    # ---- advance past the (cancelled) deadline: nobody wakes ---------------
    sc.clock.advance(UInt64(10_000))
    var woke = service_timers[IntResult](rt, heap, sc.clock.now())
    if woke != 0:
        red("cancelled timer still woke a task")
    if h.state() != TaskControlBlock.WAITING:
        red("A left WAITING despite the cancelled timer")
    if rt.pending() != 0:
        red("cancelled wake enqueued a record")

    # ---- re-arm the same task: cancellation is not poisoning --------------
    unpark_current(rt, h)
    if h.state() != TaskControlBlock.RUNNABLE:
        red("resume did not schedule A after cancel")
    var served2 = scheduler_loop(rt, dispatch, ud)
    if served2 != 1:
        red("re-arm drive served " + String(served2) + " records, expected 1")
    if h.state() != TaskControlBlock.WAITING:
        red("A did not re-park after re-arm")
    if heap.size() != 1:
        red("re-arm did not register one timer")

    # ---- service the re-armed timer: A wakes and completes -----------------
    sc.clock.advance(UInt64(100))
    var woke2 = service_timers[IntResult](rt, heap, sc.clock.now())
    if woke2 != 1:
        red("re-armed timer did not wake A")
    if heap.size() != 0:
        red("re-armed timer left in heap after expiry")
    var served3 = scheduler_loop(rt, dispatch, ud)
    if served3 != 1:
        red("final drive served " + String(served3) + " records, expected 1")
    if not h.is_completed():
        red("A did not complete after re-arm")
    if buf[2] != 1:
        red("A body ran wrong number of times: " + String(buf[2]))

    print("T21 timer cancel: PASS")