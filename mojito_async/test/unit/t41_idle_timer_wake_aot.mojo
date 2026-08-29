# mojito_async/test/unit/t39_idle_timer_wake_aot.mojo
#
# A6.3 (issue #86) — idle-worker timer wake: acceptance driver on a REAL OS
# thread (mirrors t35_idle_sleep_aot.mojo's spawn_native_thread/
# join_native_thread convention for the E6 idle-sleep infrastructure itself
# — this is the OS-level companion to t38's deterministic virtual-clock
# cross-worker routing proof).  A single idle worker thread parks on its
# OWN dedicated timer NativeEvent (worker_timer.WorkerTimerHandle); the
# MAIN thread plays the "coordinator on another worker" arming a deadline
# for a task owned by the idle worker (arm_remote, issue #86 deliverable 2).
#
#   1. NO BUSY-SPIN, NO SPURIOUS WAKE: the idle worker parks (parks >= 1)
#      immediately, with NO timer armed yet; it stays parked (no delivery,
#      no service) while the (2s) backstop is far from elapsing.
#   2. TARGETED SIGNAL WAKE: arm_remote arms a ~30ms-out deadline for the
#      idle worker's OWNED (parked) task and delivers exactly ONE signal.
#      The idle worker wakes and services its heap WELL BEFORE the 2s
#      backstop would have elapsed (elapsed < backstop proves the SIGNAL,
#      not the timeout, delivered the wake) -- "wakes that worker at the
#      deadline" (issue #86 acceptance).
#   3. EXACTLY ONE RESUME, NO MIGRATION: the owned task transitions
#      WAITING -> RUNNABLE exactly once; owner_worker() is UNCHANGED
#      (the task never moves worker) -- "the owner resumes the task
#      exactly once"/"no started task moves between workers" (issue #86).
#   4. RETURNS TO SLEEP: after servicing, the idle worker re-parks (a
#      second `parks` increment) instead of spinning -- "services it, and
#      returns to sleep" (issue #86 acceptance).
#   5. Shutdown: a plain flag + one more delivered signal wakes the idle
#      thread promptly so it observes the flag and returns normally (the
#      SAME spawn_native_thread/join_native_thread convention t35 already
#      proves works for a real-thread pool loop that returns cleanly).
#
# AOT only (worker_timer.mojo reaches the vendored NativeEvent C-ABI;
# modular/modular#6971: the JIT cannot resolve dylib symbols through an
# imported module).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.atomic import fence, Ordering
from std.memory import stack_allocation
from std.time import sleep
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.park import park_current
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, SuspendReason, claim_running, execute, spawn
from mojito_async.time.worker_timer import (
    WorkerTimerHandle,
    arm_remote,
    deliver_deadline,
    idle_park_worker_timers,
    make_worker_timer_table,
    service_worker_timers,
)
from mojito_async.vendor.mojito_sys import (
    entry_pointer,
    join_native_thread,
    make_native_event,
    monotonic_now_ns,
    spawn_native_thread,
)


def red(what: String) raises -> None:
    print("T41 idle timer wake: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]
comptime WORKER_ID = Int(1)
# 2s backstop: never elapses in the happy path (the arm is ~30ms out), so a
# fast fired-count observation proves the SIGNAL path woke the worker, not
# the deadline-slice timeout (issue #86 "no spurious wake while every timer
# is in the future" + the reverse: a real, targeted wake IS prompt).
comptime BACKSTOP_NS = Int(2_000_000_000)
comptime ARM_DELAY_NS = Int(30_000_000)  # 30ms


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var wt: UnsafePointer[WorkerTimerHandle, MutAnyOrigin]
    var rt: UnsafePointer[Runtime, MutAnyOrigin]
    var shutdown: UnsafePointer[Int, MutAnyOrigin]
    var fired: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.wt = UnsafePointer[WorkerTimerHandle, MutAnyOrigin](unsafe_from_address=1)
        self.rt = UnsafePointer[Runtime, MutAnyOrigin](unsafe_from_address=1)
        self.shutdown = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.fired = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def _finish(ud: BytePtr) raises -> IntResult:
    return IntResult(1)


def dispatch_task(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    var phase = ud.bitcast[Int]()
    if phase[] == 0:
        claim_running(h)
        park_current(rt, h, SuspendReason.TIMER)
        phase[] = 1
        return 1
    _ = execute(h, _finish, ud)
    return 1


def idle_loop(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """The idle worker's OWN loop: park on its dedicated timer event until
    either a deliver_deadline() signal or its own nearest deadline elapses,
    then service its heap and re-park -- E6's park_os_thread_until_event
    companion (worker_timer.idle_park_worker_timers), scoped to this
    worker's timer channel exactly as issue #86 deliverable 4 describes."""
    var sc = scp[]
    while True:
        fence[Ordering.ACQUIRE]()
        if sc.shutdown[] != 0:
            return
        _ = idle_park_worker_timers(sc.wt[], BACKSTOP_NS)
        var now = UInt64(monotonic_now_ns())
        if sc.wt[].heap.has_due(now):
            var woke = service_worker_timers[IntResult](sc.rt[], sc.wt[], now)
            if woke > 0:
                sc.fired[] += woke
        fence[Ordering.RELEASE]()


@export("t41_idle_entry")
def t41_idle_entry(arg: BytePtr) abi("C"):
    var scp = arg.bitcast[Scene]()
    try:
        idle_loop(scp)
    except e:
        scp[].fired[] = -1000  # sentinel: the loop itself raised
        fence[Ordering.RELEASE]()


def main() raises:
    var rt = create()
    var wt = WorkerTimerHandle(WORKER_ID)
    wt.bind_event(make_native_event())
    var slots = stack_allocation[2, UnsafePointer[WorkerTimerHandle, MutAnyOrigin]]()
    slots[0] = UnsafePointer[WorkerTimerHandle, MutAnyOrigin](to=wt)
    slots[1] = UnsafePointer[WorkerTimerHandle, MutAnyOrigin](to=wt)
    var table = make_worker_timer_table(slots, 2)

    var phase = Int(0)
    var ud = UnsafePointer[Int, MutAnyOrigin](to=phase).bitcast[Byte]()
    var t = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t), 0)
    var served1 = scheduler_loop(rt, dispatch_task, ud, worker_id=WORKER_ID)
    if served1 != 1:
        red("first drive must serve exactly 1 slice (the parking attempt)")
    if h.state() != TaskControlBlock.WAITING:
        red("the owned task must be WAITING before the idle worker starts")

    var shutdown = Int(0)
    var fired = Int(0)
    var sc = Scene()
    sc.wt = UnsafePointer[WorkerTimerHandle, MutAnyOrigin](to=wt)
    sc.rt = UnsafePointer[Runtime, MutAnyOrigin](to=rt)
    sc.shutdown = UnsafePointer[Int, MutAnyOrigin](to=shutdown)
    sc.fired = UnsafePointer[Int, MutAnyOrigin](to=fired)
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var thread = spawn_native_thread(entry_pointer["t41_idle_entry"](), scp.bitcast[Byte]())

    # ---- 1. the idle worker genuinely parks with NO timer armed yet ------
    var spins = 0
    while wt.parks < 1:
        sleep(0.001)
        spins += 1
        if spins > 5000:
            red("idle worker never parked (parks=" + String(wt.parks) + ")")
    if wt.deliveries != 0:
        red("no delivery must have happened before any arm")

    # ---- 2. arm a ~30ms-out deadline; the TARGETED signal wakes it -------
    var t_arm = monotonic_now_ns()
    var deadline_ticks = UInt64(t_arm + ARM_DELAY_NS)
    _ = arm_remote[IntResult](table, h, deadline_ticks)
    if wt.deliveries != 1:
        red("arm_remote must deliver EXACTLY ONE singleton wake, got "
            + String(wt.deliveries))

    var spins2 = 0
    while fired < 1:
        if fired < 0:
            red("the idle loop raised internally (sentinel " + String(fired) + ")")
        sleep(0.002)
        spins2 += 1
        if spins2 > 3000:
            red("idle worker never serviced the armed deadline (fired="
                + String(fired) + ")")
    fence[Ordering.ACQUIRE]()
    var elapsed = monotonic_now_ns() - t_arm
    if elapsed >= BACKSTOP_NS:
        red("the wake took the full backstop -- the SIGNAL path (not the "
            + "timeout) must have delivered it; elapsed=" + String(elapsed))

    # ---- 3. exactly one resume, no migration ------------------------------
    if h.state() != TaskControlBlock.RUNNABLE:
        red("the owned task must be RUNNABLE once the idle worker services it")
    if h.tcb()[].owner_worker() != WORKER_ID:
        red("owner_worker must be UNCHANGED (no started task migrates)")
    if not wt.heap.is_empty():
        red("the idle worker's heap must be empty after popping the due entry")
    if fired != 1:
        red("exactly one wake must have fired, got " + String(fired))

    # ---- 4. returns to sleep (a second park cycle, no spin) ---------------
    var spins3 = 0
    while wt.parks < 2:
        sleep(0.001)
        spins3 += 1
        if spins3 > 5000:
            red("idle worker never re-parked after servicing (parks="
                + String(wt.parks) + ") -- it must return to sleep, not spin")

    # ---- 5. shutdown: flag + one more delivered signal, then join --------
    shutdown = 1
    fence[Ordering.RELEASE]()
    deliver_deadline(wt)
    join_native_thread(thread)

    # ---- the task resumes and completes on the MAIN thread ----------------
    var served2 = scheduler_loop(rt, dispatch_task, ud, worker_id=WORKER_ID)
    if served2 != 1 or not h.is_completed():
        red("the owned task must complete on its resume drive")
    if rt.pending() != 0:
        red("leftover runnables after completion")

    print("T41 idle timer wake: PASS (parks=" + String(wt.parks)
          + ", deliveries=" + String(wt.deliveries)
          + ", elapsed_ns=" + String(elapsed) + ")")
