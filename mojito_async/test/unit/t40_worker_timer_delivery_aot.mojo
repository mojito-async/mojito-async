# mojito_async/test/unit/t38_worker_timer_delivery.mojo
#
# A6.3 (issue #86) — deadline delivery ACROSS WORKERS: acceptance driver.
#
# Two VIRTUAL workers (two Runtime instances + two WorkerTimerHandle cells
# in one WorkerTimerTable), driven deterministically by a caller-owned
# virtual clock cell — no OS threads, no wall-clock wait (spec §76.5;
# matches every other A1.4/A5 timer driver's discipline).  Scenario:
#
#   - task T is spawned + first-dispatched on worker 2 (rtB), stamping
#     owner_worker=2/owner_runtime=addr(rtB) (A2.5, #71); it parks itself
#     (SuspendReason.TIMER) WITHOUT arming any heap itself.
#   - task U is spawned + first-dispatched on worker 1 (rtA) the same way.
#   - `arm_remote` is called for BOTH tasks from a NEUTRAL context (neither
#     "is" worker 1 or 2) — arm_remote resolves each task's OWNER from its
#     OWN TaskControlBlock stamp, so it always lands on the CORRECT
#     per-worker heap regardless of who calls it (issue #86 deliverable 2):
#     T's timer lands ONLY in worker 2's heap, U's ONLY in worker 1's.
#   - each worker's `service_worker_timers` pass touches ONLY its own heap:
#     advancing the clock to T's (earlier) deadline and servicing worker 2
#     wakes T alone (worker 1's heap, still holding U's entry, is provably
#     untouched); a LATER advance to U's deadline wakes U alone off
#     worker 1's heap.  "The total pass is the sum of per-worker passes
#     with no cross-worker duplicate delivery" (issue #86 exit criterion).
#   - NO STARTED TASK MIGRATES: owner_worker()/owner_runtime() are read
#     before arming and re-checked after the cross-worker wake — unchanged
#     both times (the migration invariant "stays zero").
#   - `deliver_deadline`'s counter fires EXACTLY ONCE per arm_remote call
#     (issue #86 "the singleton cross-worker wake").
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.park import park_current
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, SuspendReason, claim_running, execute, spawn
from mojito_async.time.timer_heap import NO_DEADLINE
from mojito_async.time.worker_timer import (
    WorkerTimerHandle,
    WorkerTimerTable,
    arm_remote,
    make_worker_timer_table,
    min_deadline,
    next_park_deadline_ns,
    service_worker_timers,
)


def red(what: String) raises -> None:
    print("T40 worker timer delivery: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]
comptime WORKER_A = Int(1)
comptime WORKER_B = Int(2)


def _finish(ud: BytePtr) raises -> IntResult:
    return IntResult(1)


def dispatch_task(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """First entry parks (SuspendReason.TIMER) without arming any heap
    itself -- the deadline is armed EXTERNALLY via arm_remote below,
    exactly the "coordinator arms a timeout on behalf of a worker-pinned
    task" shape issue #86 describes; the second entry (post cross-worker
    wake) completes."""
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


def main() raises:
    # ---- two virtual workers: two Runtimes + a shared timer table --------
    var rtA = create()
    var rtB = create()
    # Each WorkerTimerHandle is a plain local (stable address for main's
    # whole lifetime, normal Mojo value semantics/destructors -- avoids
    # b2's no-placement-new hazard for a stride-addressed array of a
    # destructor-owning struct); the table is a POD pointer array over
    # them (WorkerTimerTable's own convention, see its docstring).
    var wt0 = WorkerTimerHandle(0)
    var wtA = WorkerTimerHandle(WORKER_A)
    var wtB = WorkerTimerHandle(WORKER_B)
    var slots = stack_allocation[3, UnsafePointer[WorkerTimerHandle, MutAnyOrigin]]()
    slots[0] = UnsafePointer[WorkerTimerHandle, MutAnyOrigin](to=wt0)
    slots[1] = UnsafePointer[WorkerTimerHandle, MutAnyOrigin](to=wtA)
    slots[2] = UnsafePointer[WorkerTimerHandle, MutAnyOrigin](to=wtB)
    var table = make_worker_timer_table(slots, 3)

    # ---- U on worker A: first dispatch stamps owner, then parks -----------
    var phaseA = Int(0)
    var udA = UnsafePointer[Int, MutAnyOrigin](to=phaseA).bitcast[Byte]()
    var tU = TB.create()
    var hU = spawn(rtA, UnsafePointer[TB, MutAnyOrigin](to=tU), 0)
    var servedA1 = scheduler_loop(rtA, dispatch_task, udA, worker_id=WORKER_A)
    if servedA1 != 1:
        red("U: first drive must serve exactly 1 slice")
    if hU.state() != TaskControlBlock.WAITING:
        red("U must be WAITING after its own park")
    if hU.tcb()[].owner_worker() != WORKER_A:
        red("U must be stamped owner_worker == WORKER_A at first dispatch")

    # ---- T on worker B: same shape -----------------------------------------
    var phaseB = Int(0)
    var udB = UnsafePointer[Int, MutAnyOrigin](to=phaseB).bitcast[Byte]()
    var tT = TB.create()
    var hT = spawn(rtB, UnsafePointer[TB, MutAnyOrigin](to=tT), 0)
    var servedB1 = scheduler_loop(rtB, dispatch_task, udB, worker_id=WORKER_B)
    if servedB1 != 1:
        red("T: first drive must serve exactly 1 slice")
    if hT.state() != TaskControlBlock.WAITING:
        red("T must be WAITING after its own park")
    if hT.tcb()[].owner_worker() != WORKER_B:
        red("T must be stamped owner_worker == WORKER_B at first dispatch")

    # ---- arm_remote from a NEUTRAL context: each lands on its OWNER's -----
    # ----     heap, never the caller's, regardless of who calls it ---------
    if min_deadline(wtA) != NO_DEADLINE or min_deadline(wtB) != NO_DEADLINE:
        red("both heaps must start empty (NO_DEADLINE)")
    var gen_u = arm_remote[IntResult](table, hU, UInt64(500))   # U due at t=500
    var gen_t = arm_remote[IntResult](table, hT, UInt64(200))   # T due at t=200 (earlier)
    if wtA.heap.size() != 1:
        red("U's arm must land on worker A's heap (size 1), got " + String(wtA.heap.size()))
    if wtB.heap.size() != 1:
        red("T's arm must land on worker B's heap (size 1), got " + String(wtB.heap.size()))
    if wtA.deliveries != 1 or wtB.deliveries != 1:
        red("each arm_remote must deliver EXACTLY ONE singleton wake (got "
            + String(wtA.deliveries) + ", " + String(wtB.deliveries) + ")")
    if gen_u == 0 or gen_t == 0:
        red("arm_remote must return a nonzero generation token")

    # ---- neither worker's own dispatch address changed (no migration) -----
    if hU.tcb()[].owner_worker() != WORKER_A or hT.tcb()[].owner_worker() != WORKER_B:
        red("owner_worker must be UNCHANGED by arming a remote deadline")

    # ---- service worker A's heap early: nothing due yet --------------------
    var woke_a0 = service_worker_timers[IntResult](rtA, wtA, UInt64(0))
    if woke_a0 != 0:
        red("worker A must wake nobody before any deadline elapses")
    if hU.state() != TaskControlBlock.WAITING:
        red("U must still be WAITING")

    # ---- advance to T's (earlier) deadline; ONLY worker B's pass wakes T --
    var now1 = UInt64(200)
    var woke_b1 = service_worker_timers[IntResult](rtB, wtB, now1)
    if woke_b1 != 1:
        red("worker B's service pass must wake exactly T, woke " + String(woke_b1))
    if hT.state() != TaskControlBlock.RUNNABLE:
        red("T must be RUNNABLE after worker B services its own heap")
    if hT.tcb()[].owner_worker() != WORKER_B:
        red("T must still be owned by WORKER_B after the cross-worker wake (no migration)")
    if not wtB.heap.is_empty():
        red("worker B's heap must be empty after popping T's due entry")
    # worker A's heap (U's entry) must be COMPLETELY untouched by worker B's
    # pass -- "each worker services only its own heap; no cross-worker
    # duplicate delivery" (issue #86).
    if wtA.heap.size() != 1:
        red("worker A's heap must still hold U's entry untouched, size="
            + String(wtA.heap.size()))
    if hU.state() != TaskControlBlock.WAITING:
        red("U must remain WAITING -- worker B's pass must never touch it")
    var woke_a1 = service_worker_timers[IntResult](rtA, wtA, now1)
    if woke_a1 != 0:
        red("worker A's pass at t=200 must wake nobody (U is due at t=500)")

    # ---- advance to U's (later) deadline; worker A's pass wakes U alone ---
    var now2 = UInt64(500)
    var woke_a2 = service_worker_timers[IntResult](rtA, wtA, now2)
    if woke_a2 != 1:
        red("worker A's service pass must wake exactly U, woke " + String(woke_a2))
    if hU.state() != TaskControlBlock.RUNNABLE:
        red("U must be RUNNABLE after worker A services its own heap")
    if hU.tcb()[].owner_worker() != WORKER_A:
        red("U must still be owned by WORKER_A (no migration)")
    if not wtA.heap.is_empty():
        red("worker A's heap must be empty after popping U's due entry")

    # ---- both tasks resume and complete on their OWN worker's runtime -----
    var servedA2 = scheduler_loop(rtA, dispatch_task, udA, worker_id=WORKER_A)
    if servedA2 != 1 or not hU.is_completed():
        red("U must complete on its own resume drive")
    var servedB2 = scheduler_loop(rtB, dispatch_task, udB, worker_id=WORKER_B)
    if servedB2 != 1 or not hT.is_completed():
        red("T must complete on its own resume drive")
    if rtA.pending() != 0 or rtB.pending() != 0:
        red("leftover runnables after both tasks complete")

    # ---- next_park_deadline_ns: bounded by the nearest own timer ----------
    var fresh_wt = WorkerTimerHandle(WORKER_A)
    if next_park_deadline_ns(fresh_wt, 1000, 5000) != 6000:
        red("an empty heap must bound the park deadline by the full backstop")
    _ = fresh_wt.heap.arm(9, 9, UInt64(2000))
    if next_park_deadline_ns(fresh_wt, 1000, 5000) != 2000:
        red("a nearer own timer must shorten the park deadline below the backstop")
    if next_park_deadline_ns(fresh_wt, 3000, 5000) != 3000:
        red("an already-elapsed deadline must return `now` (park returns immediately)")

    print("T40 worker timer delivery: PASS")
