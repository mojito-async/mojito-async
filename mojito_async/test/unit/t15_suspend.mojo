# mojito_async/test/unit/t15_suspend.mojo
#
# A1.1 (issue #33) — _suspend_current park + resume_current, and worker
# reuse: while a task is WAITING, another RUNNABLE task executes on the SAME
# worker before the parked task resumes.
#
# Acceptance:
#   - _suspend_current parks a RUNNING task to WAITING (fresh wait epoch) and
#     it drops OFF the runnable queue;
#   - resume_current delivers readiness ONCE (WAITING -> RUNNABLE +
#     re-enqueue), preserving the task's scheduler id;
#   - worker reuse: driving the single-worker scheduler loop, B runs entirely
#     inside A's park window (order A_START < A_PARK < B_RUN < A_RESUME).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import (
    _suspend_current,
    resume_current,
    scheduler_loop,
)
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn


def red(what: String) raises -> None:
    print("T15 suspend: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

comptime EV_A_START = Int(1)
comptime EV_A_PARK = Int(2)
comptime EV_B_RUN = Int(3)
comptime EV_A_RESUME = Int(4)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var seq: UnsafePointer[Int, MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var a_id: UnsafePointer[Int, MutAnyOrigin]
    var b_id: UnsafePointer[Int, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = self.seq
        self.a_id = self.seq
        self.b_id = self.seq
        self.phase = self.seq


def rec(mut sc: Scene, ev: Int):
    var i = sc.n[]
    sc.seq[i] = ev
    sc.n[] = i + 1


def a_start(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_A_START)
    return IntResult(1)


def a_finish(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    rec(sc[], EV_A_RESUME)
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
            _ = a_start(ud)
            _suspend_current(rt, ha)
            rec(sc[], EV_A_PARK)
            sc[].phase[] = 1
        else:
            _ = execute(ha, a_finish, ud)
        return 1
    raise Error("unexpected task id in dispatcher")


def main() raises:
    var rt = create()
    var buf = stack_allocation[24, Int]()
    var sc = Scene()
    sc.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 8 * 8)
    sc.a_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 9 * 8)
    sc.b_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 10 * 8)
    sc.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 11 * 8)
    buf[8] = 0
    buf[11] = 0
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb_a = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    var tcb_b = TB.create()
    var h_b = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    buf[9] = h_a.id()
    buf[10] = h_b.id()

    var slices = scheduler_loop(rt, dispatch, ud)
    if slices != 2:
        red("expected to serve A and B records, got " + String(slices))
    if h_b.state() != TaskControlBlock.COMPLETED:
        red("B did not reach COMPLETED inside A's park window")
    if h_a.state() != TaskControlBlock.WAITING:
        red("A should be WAITING after park, is " + String(h_a.state()))

    resume_current(rt, h_a)
    if h_a.state() != TaskControlBlock.RUNNABLE:
        red("resume did not make A RUNNABLE")
    if rt.pending() != 1:
        red("resume did not re-enqueue A exactly once")

    var slices2 = scheduler_loop(rt, dispatch, ud)
    if slices2 != 1:
        red("second drive expected one record, got " + String(slices2))
    if not h_a.is_completed():
        red("resumed A did not reach COMPLETED")

    if buf[0] != EV_A_START or buf[1] != EV_A_PARK or buf[2] != EV_B_RUN:
        red("interleave broken (B must run inside A's park window)")
    if buf[3] != EV_A_RESUME:
        red("A_RESUME not 4th event")
    if buf[8] != 4:
        red("expected exactly 4 events, got " + String(buf[8]))

    var stale = False
    try:
        resume_current(rt, h_a)
    except Error:
        stale = True
    if not stale:
        red("resuming a COMPLETED task did not raise")

    print("T15 suspend: PASS")