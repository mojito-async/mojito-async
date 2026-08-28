# mojito_async/test/unit/t31_local_queue.mojo
#
# A2.2 (issue #68) — per-worker LocalDeque + RemoteReadyQueue acceptance.
#
# Acceptance (issue #68 verification, E2):
#   1. LocalDeque owner end: push N records, pop_back returns them LIFO
#      (last pushed first — spawn locality); every pushed record is popped
#      EXACTLY once; pop_back on an empty deque raises.
#   2. steal_front (the thief end) reads the OPPOSITE end (oldest record
#      first) and never overlaps the owner's LIFO end.
#   3. RemoteReadyQueue: owner pop is FIFO (wake order); a record pushed
#      remotely is popped exactly once; pop on an empty queue raises.
#   4. Worker-level N=1 parity: a task spawned on a worker (via the runtime's
#      local deque) runs on that worker; the drive serves LOCAL records
#      before REMOTE records (spec §21), then the runtime is quiet.
#   5. owner_worker is stamped at first run (E5 surface; E2 reserves the
#      field): after one driven slice the TCB reports the driving worker id
#      (identity threaded by value — b2 has no TLS — exactly like
#      scheduler.wake_target_worker).
#   6. Two-worker isolation (MANUAL enqueues — the E1 WorkerPool is the
#      sibling lane; pool integration is E1-owned): a record pushed into
#      worker B's RemoteReadyQueue (simulating a remote wake) is popped
#      EXACTLY once by B's owner pop; worker A never observes it.  E4/E5
#      wire the real cross-worker paths; this driver proves the queues.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.queue import LocalDeque, RemoteReadyQueue, TaskRecord
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import make_worker
from mojito_async.task import JoinHandle, execute, spawn


def red(what: String) raises -> None:
    print("T31 local queue: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _ptr(mut t: TB) -> UnsafePointer[TB, MutAnyOrigin]:
    return UnsafePointer[TB, MutAnyOrigin](to=t)


def _scratch() -> BytePtr:
    return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x11)


def body_one(ud: BytePtr) raises -> IntResult:
    return IntResult(1)


# --- dispatcher: execute a popped record to completion + log its id -------

struct LogScene(ImplicitlyCopyable, ImplicitlyDeletable):
    var count: UnsafePointer[Int, MutAnyOrigin]
    var log0: UnsafePointer[Int, MutAnyOrigin]
    var log1: UnsafePointer[Int, MutAnyOrigin]
    var log2: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.count = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.log0 = self.count
        self.log1 = self.count
        self.log2 = self.count


def dispatch_log(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[LogScene]()
    var i = sc[].count[]
    if i == 0:
        sc[].log0[] = tid
    elif i == 1:
        sc[].log1[] = tid
    else:
        sc[].log2[] = tid
    sc[].count[] = i + 1
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    _ = execute(h, body_one, ud)
    return 1


def main() raises:
    # ---- 1/2. LocalDeque: owner LIFO end + thief's opposite end ----------
    var dq = LocalDeque()
    dq.push_back(TaskRecord(0x1000, 10))
    dq.push_back(TaskRecord(0x2000, 20))
    dq.push_back(TaskRecord(0x3000, 30))
    if dq.count() != 3:
        red("local deque count != 3 after 3 pushes")
    # owner pop is LIFO (spawn locality: last pushed runs first).
    var r1 = dq.pop_back()
    if r1.task_id != 30 or r1.tcb_addr != 0x3000:
        red("pop_back must return the LAST pushed (LIFO owner end)")
    var r2 = dq.pop_back()
    if r2.task_id != 20:
        red("pop_back LIFO order broken")
    # thief's steal_front reads the OPPOSITE end (oldest first).
    var f1 = dq.steal_front()
    if f1.task_id != 10:
        red("steal_front must read the opposite (oldest) end")
    if dq.count() != 0:
        red("local deque not drained exactly-once")
    var empty = LocalDeque()
    var raised = False
    try:
        _ = empty.pop_back()
    except Error:
        raised = True
    if not raised:
        red("pop_back on an empty deque must raise")
    var raised2 = False
    try:
        _ = empty.steal_front()
    except Error:
        raised2 = True
    if not raised2:
        red("steal_front on an empty deque must raise")

    # ---- 3. RemoteReadyQueue: FIFO owner pop, exactly-once ---------------
    var rr = RemoteReadyQueue()
    rr.push(TaskRecord(0x1111, 1))
    rr.push(TaskRecord(0x2222, 2))
    if rr.count() != 2:
        red("remote queue count != 2 after 2 pushes")
    var w1 = rr.pop()
    var w2 = rr.pop()
    if w1.task_id != 1 or w2.task_id != 2:
        red("remote queue must pop FIFO (wake order)")
    if rr.count() != 0:
        red("remote queue not drained exactly-once")
    var raised3 = False
    try:
        _ = rr.pop()
    except Error:
        raised3 = True
    if not raised3:
        red("pop on an empty remote queue must raise")

    # ---- 4. Worker N=1 parity + local-before-remote drive order ----------
    var wa = make_worker()
    var rta = wa.runtime()
    var buf = stack_allocation[16, Int]()
    for zi in range(16):
        buf[zi] = 0
    var sc = LogScene()
    sc.count = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 0 * 8)
    sc.log0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    sc.log1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 2 * 8)
    sc.log2 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    var scp = UnsafePointer[LogScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # A spawns X locally (A's local deque); a simulated remote wake pushes Y
    # (RUNNABLE, E5 would route it here) onto A's RemoteReadyQueue.
    var tcb_x = TB.create()
    var h_x = spawn(rta, _ptr(tcb_x), 0)
    var tcb_y = TB.create()
    tcb_y.transition(TaskControlBlock.RUNNABLE)
    var ptr_y = _ptr(tcb_y)
    rta.push_remote(Int(ptr_y), 777)
    if rta.pending() != 2:
        red("worker A must hold 2 runnable records (1 local + 1 remote)")

    # Drive with the explicit worker identity 1 (b2 has no TLS).
    var served = scheduler_loop(rta, dispatch_log, ud, worker_id=1)
    if served != 2:
        red("drive served " + String(served) + " != 2")
    if not h_x.is_completed():
        red("locally spawned task did not complete on its own worker")
    # spec §21: LOCAL deque is drained before the REMOTE queue.
    if buf[1] != h_x.id() or buf[2] != 777:
        red("drive order must be local-first then remote "
            + "(got " + String(buf[1]) + ", " + String(buf[2]) + ")")

    # ---- 5. owner_worker stamped at first run (E5 surface) ---------------
    if tcb_x.owner_worker() != 1:
        red("owner_worker not stamped at first run on worker 1")
    if tcb_y.owner_worker() != 1:
        red("owner_worker not stamped for the remote-woken record")

    # ---- 6. Two-worker isolation (manual enqueues; E1 pool not this lane)
    var wb = make_worker()
    var rtb = wb.runtime()
    # worker A's side pushes a wake record into worker B's RemoteReadyQueue
    # (the E5 cross-worker wake route; manual here — no pool in this lane).
    rtb.push_remote(Int(ptr_y), 777)
    if rtb.pending() != 1:
        red("worker B must see exactly one remotely pushed record")
    var rec_b = rtb.pop_remote()
    if rec_b.tcb_addr != Int(ptr_y):
        red("B's owner pop did not observe the remote record")
    if rec_b.task_id != 777:
        red("B's popped record lost its task id")
    if rtb.pending() != 0:
        red("B must pop the remote record exactly once")
    var raised4 = False
    try:
        _ = rtb.pop_remote()
    except Error:
        raised4 = True
    if not raised4:
        red("B's second pop_remote must raise (exactly-once)")
    if rta.pending() != 0:
        red("worker A must not observe B's remote queue")

    print("T31 local queue: PASS")