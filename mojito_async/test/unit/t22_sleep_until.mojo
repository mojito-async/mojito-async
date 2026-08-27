# mojito_async/test/unit/t22_sleep_until.mojo
#
# A1.4 (issue #36) — sleep_until: absolute-deadline park on the timer heap.
#
#   - sleep_until_current(deadline) parks the task until an ABSOLUTE
#     monotonic deadline (A1.1 ms Deadline mapped to ns ticks); before the
#     deadline the servicing hook wakes nobody, after it the task resumes;
#   - an ALREADY-EXPIRED deadline parks with an immediately-due timer and the
#     very next service pass wakes the task (no spin, no OS-thread block —
#     the canonical park/wake path).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Deadline
from mojito_async.time.sleep import sleep_until_current
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.time.timer_service import service_timers


def red(what: String) raises -> None:
    print("T22 sleep until: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var a_id: UnsafePointer[Int, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]
    var count: UnsafePointer[Int, MutAnyOrigin]
    var deadline_ms: UnsafePointer[Int, MutAnyOrigin]
    var heap: UnsafePointer[TimerHeap, MutAnyOrigin]
    var clock: MonotonicClock

    def __init__(out self):
        self.a_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phase = self.a_id
        self.count = self.a_id
        self.deadline_ms = self.a_id
        self.heap = UnsafePointer[TimerHeap, MutAnyOrigin](
            unsafe_from_address=1
        )
        self.clock = MonotonicClock(
            UnsafePointer[UInt64, MutAnyOrigin](unsafe_from_address=1)
        )


def a_body(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    sc[].count[] = sc[].count[] + 1
    return IntResult(11)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid == sc[].a_id[]:
        var ha = _handle(tcb_addr, tid)
        if sc[].phase[] == 0:
            claim_running(ha)
            sleep_until_current(
                rt, ha, sc[].heap[], sc[].clock, Deadline(sc[].deadline_ms[])
            )
            sc[].phase[] = 1
        else:
            _ = execute(ha, a_body, ud)
        return 1
    raise Error("unexpected task id in dispatcher")


def main() raises:
    var buf = stack_allocation[512, Int]()
    var sc = Scene()
    sc.a_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8)
    sc.count = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 16)
    buf[2] = 0      # count
    sc.deadline_ms = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf) + 24
    )
    buf[1] = 0      # phase
    buf[3] = 5      # deadline 5 ms (absolute)

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

    # ---- park until absolute deadline 5 ms ---------------------------------
    var served = scheduler_loop(rt, dispatch, ud)
    if served != 1:
        red("drive served " + String(served) + " records, expected 1")
    if h.state() != TaskControlBlock.WAITING:
        red("A not WAITING after sleep_until")
    if heap.min_deadline() != 5000000:
        red("deadline ticks wrong: " + String(heap.min_deadline()))

    # ---- before the deadline: no wake ---------------------------------------
    sc.clock.advance(UInt64(3_000_000))  # now = 3 ms < 5 ms
    var woke_early = service_timers[IntResult](rt, heap, sc.clock.now())
    if woke_early != 0:
        red("woke before the absolute deadline")
    if h.state() != TaskControlBlock.WAITING:
        red("A woken before its deadline")

    # ---- at/after the deadline: wake and complete --------------------------
    sc.clock.advance(UInt64(3_000_000))  # now = 6 ms >= 5 ms
    var woke = service_timers[IntResult](rt, heap, sc.clock.now())
    if woke != 1:
        red("deadline wake did not fire exactly once")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("A not RUNNABLE after deadline wake")
    var served2 = scheduler_loop(rt, dispatch, ud)
    if served2 != 1:
        red("resume drive served " + String(served2) + " records, expected 1")
    if not h.is_completed():
        red("A did not complete after the deadline")
    if buf[2] != 1:
        red("A body did not run")
    if not heap.is_empty():
        red("expired deadline timer left in heap")

    # ---- already-expired deadline: immediately-due park --------------------
    var tcb2 = TB.create()
    var h2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb2), 0)
    # reuse the scene cells for the 2nd task (documented rewire):
    buf[0] = h2.id()
    buf[1] = 0    # phase 0 -> park on entry
    buf[3] = 1    # deadline 1 ms, clock reads 6 ms -> already expired
    var served3 = scheduler_loop(rt, dispatch, ud)
    if served3 != 1:
        red("expired-deadline drive served " + String(served3))
    if h2.state() != TaskControlBlock.WAITING:
        red("h2 not parked on the expired deadline")
    if heap.size() != 1:
        red("expired deadline should arm an immediately-due timer")
    var woke2 = service_timers[IntResult](rt, heap, sc.clock.now())
    if woke2 != 1:
        red("immediately-due timer did not wake h2")
    var served4 = scheduler_loop(rt, dispatch, ud)
    if served4 != 1:
        red("expired-deadline resume drive served " + String(served4))
    if not h2.is_completed():
        red("h2 did not complete after the expired deadline")
    if buf[2] != 2:
        red("second body did not run")

    print("T22 sleep until: PASS")