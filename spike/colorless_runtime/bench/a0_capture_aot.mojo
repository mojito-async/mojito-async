# A0.10 measurement capture for SPIKE_REPORT data section (scratch driver).
# CLOCK_MONOTONIC_RAW ~42ns tick. Emits JSONL rows to stdout.
from runtime import Runtime, create
from spawn import JoinHandle, execute, spawn
from task import ResultValue, TaskControlBlock
from measure import jsonl_row, make_clock_source
from mojito_spike import BytePtr
from std.memory import stack_allocation


@extern("exit")
def _c_exit(code: Int32) abi("C"): ...


struct IRes(ResultValue):
    var v: Int

    def __init__(out self):
        self.v = 0

    def __init__(out self, v_: Int):
        self.v = v_


def child_step(ud: BytePtr) raises -> IRes:
    return IRes(7)


def main() raises:
    var clock = make_clock_source()
    var rt = create()

    var ud = BytePtr(unsafe_from_address=8)  # thunk payload unused

    var tcb = stack_allocation[1, TaskControlBlock[IRes]]()
    var hs = stack_allocation[64, JoinHandle[IRes]]()
    var tcbs = stack_allocation[64, TaskControlBlock[IRes]]()

    # warmup (single cell, joined immediately)
    var w = 0
    while w < 200:
        tcb[0] = TaskControlBlock[IRes]()
        var h = spawn[IRes](rt, tcb, 0)
        _ = execute(h, child_step, ud)
        _ = h.join()
        w += 1

    # M1: spawn + work-first execute + completed join round trip
    var N = 5000
    var t0 = clock.now()
    var i = 0
    while i < N:
        tcb[0] = TaskControlBlock[IRes]()
        var h = spawn[IRes](rt, tcb, 0)
        _ = execute(h, child_step, ud)
        _ = h.join()
        i += 1
    var t1 = clock.now()
    print(jsonl_row("m1_spawn_execute_join_roundtrip_ns", t1 - t0, N))

    # M2: completed-join fast path across M FRESH handles (one-shot join)
    var M = 64
    i = 0
    while i < M:
        (tcbs + i)[0] = TaskControlBlock[IRes]()
        hs[i] = spawn[IRes](rt, tcbs + i, 0)
        _ = execute(hs[i], child_step, ud)
        i += 1
    t0 = clock.now()
    var j = 0
    while j < M:
        _ = hs[j].join()
        j += 1
    t1 = clock.now()
    print(jsonl_row("m2_completed_join_fastpath_total_ns", t1 - t0, M))

    # M3: TCB lifecycle chain cost per task
    t0 = clock.now()
    var k = 0
    while k < N:
        tcb[0] = TaskControlBlock[IRes]()
        tcb[0].transition(TaskControlBlock.RUNNABLE)
        tcb[0].transition(TaskControlBlock.RUNNING)
        tcb[0].transition(TaskControlBlock.COMPLETED)
        k += 1
    t1 = clock.now()
    print(jsonl_row("m3_tcb_lifecycle_total_ns", t1 - t0, N))

    _c_exit(0)