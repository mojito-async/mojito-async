# mojito_async/test/unit/t31_queues_stale_skip.mojo
#
# A2.2 (issue #68) — the A1 skip-on-stale-duplicate invariant survives the
# queue split (local deque + remote-ready queue).
#
# Acceptance (issue #68 verification, E2):
#   1. A popped record whose TCB is no longer RUNNABLE is SKIPPED (counted
#      via rt.skipped()) and NEVER dispatched, whether it came from the
#      LOCAL deque or the REMOTE queue.
#   2. A stale duplicate NEVER double-dispatches a task: with one LIVE
#      record (RUNNABLE) and one stale duplicate for the SAME TCB in the
#      other queue, the live record is dispatched EXACTLY ONCE and the stale
#      duplicate is skipped (enqueue-once is only logical cross-worker).
#   3. A live record enqueued after a stale one still runs exactly once
#      (a skip never eats subsequent work).
#   4. Both queues drain to quiet after the drive (pending() == 0).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, execute


def red(what: String) raises -> None:
    print("T31 queues stale-skip: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _ptr(mut t: TB) -> UnsafePointer[TB, MutAnyOrigin]:
    return UnsafePointer[TB, MutAnyOrigin](to=t)


def body_one(ud: BytePtr) raises -> IntResult:
    return IntResult(1)


# --- dispatcher: counts every dispatch (a stale dispatch is a red) --------

struct CountScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var runs: UnsafePointer[Int, MutAnyOrigin]
    var id_seen: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.runs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.id_seen = self.runs


def dispatch_count(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[CountScene]()
    sc[].runs[] = sc[].runs[] + 1
    sc[].id_seen[] = tid
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    _ = execute(h, body_one, ud)
    return 1


def _completed() raises -> TB:
    """A TCB whose task already ran to COMPLETED (a stale duplicate's TCB)."""
    var t = TB.create()
    t.transition(TaskControlBlock.RUNNABLE)
    t.transition(TaskControlBlock.RUNNING)
    t.transition(TaskControlBlock.COMPLETED)
    return t


def main() raises:
    var rt = create()
    var buf = stack_allocation[8, Int]()
    for zi in range(8):
        buf[zi] = 0
    var sc = CountScene()
    sc.runs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.id_seen = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    var scp = UnsafePointer[CountScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # ---- 1a. stale LOCAL record: popped, skipped, never dispatched -------
    var stale1 = _completed()
    rt.enqueue_local(Int(_ptr(stale1)), 111)
    var served = scheduler_loop(rt, dispatch_count, ud)
    if served != 0:
        red("stale local record must never dispatch (served " + String(served) + ")")
    if rt.skipped() != 1:
        red("stale local record not counted via rt.skipped()")
    if buf[0] != 0:
        red("dispatcher ran on a stale local record")
    if rt.pending() != 0:
        red("local queue not quiet after stale skip")

    # ---- 1b. stale REMOTE record: skipped, never dispatched --------------
    var stale2 = _completed()
    rt.push_remote(Int(_ptr(stale2)), 222)
    served = scheduler_loop(rt, dispatch_count, ud)
    if served != 0:
        red("stale remote record must never dispatch")
    if rt.skipped() != 2:
        red("stale remote record not counted via rt.skipped()")
    if buf[0] != 0:
        red("dispatcher ran on a stale remote record")
    if rt.pending() != 0:
        red("remote queue not quiet after stale skip")

    # ---- 2. enqueue-once cross-worker: ONE live record + one stale twin --
    var live = TB.create()
    live.transition(TaskControlBlock.RUNNABLE)
    rt.push_remote(Int(_ptr(live)), 333)   # the live wake (E5's route)
    rt.enqueue_local(Int(_ptr(live)), 333) # stale duplicate, same TCB
    served = scheduler_loop(rt, dispatch_count, ud)
    if served != 1:
        red("a single live record must dispatch exactly once "
            + "(served " + String(served) + ")")
    if buf[0] != 1 or buf[1] != 333:
        red("the one dispatch must be the live record (id 333)")
    if rt.skipped() != 3:
        red("stale duplicate not skipped (double-dispatch would have run "
            + "the task twice)")
    if rt.pending() != 0:
        red("queues not quiet after the live+stale drive")

    # ---- 3. live-after-stale: a skip never eats subsequent work ----------
    var stale3 = _completed()
    rt.enqueue_local(Int(_ptr(stale3)), 444)
    var live2 = TB.create()
    live2.transition(TaskControlBlock.RUNNABLE)
    rt.enqueue_local(Int(_ptr(live2)), 555)
    served = scheduler_loop(rt, dispatch_count, ud)
    if served != 1:
        red("live-after-stale must still dispatch once")
    if buf[1] != 555:
        red("live-after-stale dispatched the wrong record")
    if rt.skipped() != 4 or rt.pending() != 0:
        red("skip accounting or quiet invariant broken after live-after-stale")

    print("T31 queues stale-skip: PASS")