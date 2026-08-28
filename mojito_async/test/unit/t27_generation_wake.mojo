# mojito_async/test/unit/t27_generation_wake.mojo
#
# A1.3 (issue #51, fold T5) — generation consumption on the wake path.
#
# The wait generation is stamped at WAITING commit; unpark_current must
# CONSUME it so a stale/duplicate wake from a PREVIOUS epoch cannot re-
# transition a task that already woke (cross-worker producer ordering for
# EPIC #2).  This driver proves the single-worker geometry:
#
#   - a task parks TWICE (episode 1 -> generation bump to 2, episode 2 ->
#     generation bump to 3);
#   - a stale wake issued with episode-1's captured generation, after the
#     task has re-parked on episode 2, is REJECTED: the task stays WAITING
#     and nothing is re-enqueued (no double-run);
#   - the correct (current-epoch) wake then transitions WAITING -> RUNNABLE
#     exactly once and the task reaches COMPLETED exactly once;
#   - enqueue-once holds and the A1 loud surface is preserved (a wake on a
#     COMPLETED task with no matching live epoch still raises).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import park_current, unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


def red(what: String) raises -> None:
    print("T27 generation wake: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

comptime EV_A_1 = Int(1)
comptime EV_A_2 = Int(2)
comptime EV_A_DONE = Int(3)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Shared scene: event log + how many times A's body entered (exactly
    once per wake)."""

    var seq: UnsafePointer[Int, MutAnyOrigin]
    var n: UnsafePointer[Int, MutAnyOrigin]
    var a_id: UnsafePointer[Int, MutAnyOrigin]
    var a_slices: UnsafePointer[Int, MutAnyOrigin]  # body-entry count

    def __init__(out self):
        self.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.n = self.seq
        self.a_id = self.seq
        self.a_slices = self.seq


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def body_ep1(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    var i = sc[].n[]
    sc[].seq[i] = EV_A_1
    sc[].n[] = i + 1
    return IntResult(1)


def body_ep2(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    var i = sc[].n[]
    sc[].seq[i] = EV_A_2
    sc[].n[] = i + 1
    return IntResult(2)


def body_done(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    var i = sc[].n[]
    sc[].seq[i] = EV_A_DONE
    sc[].n[] = i + 1
    return IntResult(3)


# A parks on every entry except the LAST: slices==0 body EP1 then park
# (episode 1); slices==1 body EP2 then park (episode 2); slices==2 body DONE
# (reaches COMPLETED).  Each park claims a fresh WAITING epoch.
def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid != sc[].a_id[]:
        raise Error("unexpected task id in t27 dispatcher")
    var ha = _handle(tcb_addr, tid)
    var s = sc[].a_slices[]
    sc[].a_slices[] = s + 1
    if s == 0:
        claim_running(ha)
        _ = body_ep1(ud)
        park_current(rt, ha)  # episode 1: WAITING, generation -> 2
        return 1
    if s == 1:
        claim_running(ha)
        _ = body_ep2(ud)
        park_current(rt, ha)  # episode 2: WAITING, generation -> 3
        return 1
    _ = execute(ha, body_done, ud)  # RUNNABLE -> RUNNING -> COMPLETED
    return 1


def main() raises:
    var failures = List[String]()
    var rt = create()
    var buf = stack_allocation[16, Int]()
    var sc = Scene()
    sc.seq = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    sc.a_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 2 * 8)
    sc.a_slices = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 3 * 8)
    buf[1] = 0
    buf[3] = 0
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb_a = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    buf[2] = h_a.id()

    # --- episode 1: A parks (gen -> 2) -----------------------------------
    var served = scheduler_loop(rt, dispatch, ud)
    if served != 1:
        failures.append("episode 1 served " + String(served) + ", expected 1")
    if h_a.state() != TaskControlBlock.WAITING:
        failures.append("A must be WAITING after episode-1 park (state "
                        + String(h_a.state()) + ")")
    var gen_e1 = tcb_a.generation()
    if gen_e1 != 2:
        failures.append("episode-1 park did not bump generation to 2 (got "
                        + String(gen_e1) + ")")

    # --- wake A (current epoch), A parks again -> gen 3 ---------------------
    unpark_current(rt, h_a)
    if h_a.state() != TaskControlBlock.RUNNABLE:
        failures.append("wake-1 did not make A RUNNABLE")
    var served2 = scheduler_loop(rt, dispatch, ud)
    if served2 != 1:
        failures.append("episode 2 served " + String(served2) + ", expected 1")
    if h_a.state() != TaskControlBlock.WAITING:
        failures.append("A must be WAITING after episode-2 park (state "
                        + String(h_a.state()) + ")")
    if tcb_a.generation() != 3:
        failures.append("episode-2 park did not bump to 3 (got "
                        + String(tcb_a.generation()) + ")")
    if rt.pending() != 0:
        failures.append("queue not quiet after episode 2")

    # --- STALE wake: episode-1's captured generation vs current ----------
    var before_enq = rt.enqueued()
    unpark_current(rt, h_a, required_gen=gen_e1)  # stale: must be REJECTED
    if h_a.state() != TaskControlBlock.WAITING:
        failures.append("stale wake re-transitioned A (state "
                        + String(h_a.state()) + ")")
    if rt.pending() != 0:
        failures.append("stale wake enqueued A (double-run; pending "
                        + String(rt.pending()) + ")")
    if rt.enqueued() != before_enq:
        failures.append("stale wake changed the enqueue counter")

    # --- current-epoch wake: A reaches COMPLETED exactly once ------------
    unpark_current(rt, h_a)
    if h_a.state() != TaskControlBlock.RUNNABLE:
        failures.append("current wake did not make A RUNNABLE")
    var served3 = scheduler_loop(rt, dispatch, ud)
    if served3 != 1:
        failures.append("final served " + String(served3) + ", expected 1")
    if not h_a.is_completed():
        failures.append("A did not complete after the current wake")
    if sc.a_slices[] != 3:
        failures.append("A body ran " + String(sc.a_slices[])
                        + " times, expected exactly 3 (each wake once)")
    if sc.n[] != 3:
        failures.append("expected 3 events, got " + String(sc.n[]))

    # --- A1 loud surface: wake on a COMPLETED task still raises -----------
    var stale = False
    try:
        unpark_current(rt, h_a)
    except Error:
        stale = True
    if not stale:
        failures.append("waking a COMPLETED task did not raise")

    if len(failures) == 0:
        print("T27 generation wake: PASS")
    else:
        print("T27 generation wake: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)