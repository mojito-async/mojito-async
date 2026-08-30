# mojito_async/test/stress/t15_select_stress.mojo
#
# A5.6 select model/race acceptance battery (issue #94) — repeated-select
# stress: K selecting tasks fan out across M=3 shared channels PLUS a
# per-task DEADLINE branch (a live TimerHeap entry each, far enough in the
# future that none ever fires), then K producers each deliver exactly one
# item — proving, AT SCALE, every invariant the small unit drivers (t25-
# t29) already proved individually:
#
#   - every selecting task is woken EXACTLY ONCE and receives EXACTLY ONE
#     item (no double delivery, no lost delivery — spec §75.2 item 15's
#     "both_delivered"/"no_winner" forbidden outcomes, at scale);
#   - the winning claim cancels that task's still-armed heap timer
#     (issue #91's `_cancel_armed_timer`) — the heap drains to EXACTLY
#     ZERO entries as every task resolves, never leaking one;
#   - every OTHER channel registration for a resolved task is unregistered
#     (issue #92 step 3) — all three channels' wait queues end EMPTY;
#   - zero leftovers overall: no deferred wakes, no pending runnables, no
#     unconsumed results (the same ZERO-leftover discipline every A1/A5
#     stress driver enforces, issue #37).
#
# Pure Mojo (`mojo run`), extern-free, def-only, deterministic (virtual
# clock; no wall-clock waiting).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from std.memory import stack_allocation
from mojito_async.channel import Channel, make_channel
from mojito_async.channel.select import (
    SelectBranch,
    SelectState,
    deadline_branch,
    recv_branch,
    select,
)
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Deadline
from mojito_async.time.timer_heap import TimerHeap


def red(what: String) raises -> None:
    print("T15 select stress: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]
comptime K = Int(200)   # selecting tasks / producers
comptime M = Int(3)     # shared channels


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


def _complete(h: JoinHandle[IntResult], res: Int) raises:
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    h.tcb()[].mark_result(IntResult(res))


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var chan_0: UnsafePointer[Channel[Int], MutAnyOrigin]
    var chan_1: UnsafePointer[Channel[Int], MutAnyOrigin]
    var chan_2: UnsafePointer[Channel[Int], MutAnyOrigin]
    var branches: UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin]  # K-element array
    var states: UnsafePointer[SelectState, MutAnyOrigin]           # K-element array
    var winners: UnsafePointer[Int, MutAnyOrigin]                  # K-element array
    var values: UnsafePointer[Int, MutAnyOrigin]                   # K-element array
    var run_counts: UnsafePointer[Int, MutAnyOrigin]               # K-element array
    var now_ticks: Int

    def __init__(out self):
        self.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.chan_2 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.branches = UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](unsafe_from_address=1)
        self.states = UnsafePointer[SelectState, MutAnyOrigin](unsafe_from_address=1)
        self.winners = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.values = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.run_counts = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.now_ticks = 0


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var idx = tid - 1  # tasks spawned 1..K in order -> tid-1 is the slot index
    if idx < 0 or idx >= K:
        raise Error("T15: unexpected task id " + String(tid))
    var h = _handle(tcb_addr, tid)
    claim_running(h)
    sc[].run_counts[idx] = sc[].run_counts[idx] + 1
    var out = select[Int, IntResult](
        rt, h,
        UnsafePointer[List[SelectBranch[Int]], MutAnyOrigin](to=sc[].branches[idx])[],
        UnsafePointer[SelectState, MutAnyOrigin](to=sc[].states[idx])[],
        sc[].now_ticks,
    )
    if h.state() == TaskControlBlock.WAITING:
        return 1  # parked; the embedding driver re-enters on resume
    sc[].winners[idx] = out.index
    if out.has_value():
        sc[].values[idx] = out.recv_value()
    else:
        sc[].values[idx] = -999
    _complete(h, out.index)
    return 1


def drain_all(
    mut rt: Runtime,
    chan0: UnsafePointer[Channel[Int], MutAnyOrigin],
    chan1: UnsafePointer[Channel[Int], MutAnyOrigin],
    chan2: UnsafePointer[Channel[Int], MutAnyOrigin],
) raises:
    for c in range(3):
        var chan = chan0
        if c == 1:
            chan = chan1
        elif c == 2:
            chan = chan2
        while chan[].to_wake_len() > 0:
            var wr = chan[].pop_to_wake()
            if wr.task_id == 0:
                break
            unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))


def main() raises:
    var heap = TimerHeap()
    var hp = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    var clock = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0]))

    var chan0 = make_channel[Int](K)
    var chan1 = make_channel[Int](K)
    var chan2 = make_channel[Int](K)
    var chp0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0)
    var chp1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1)
    var chp2 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan2)

    # Per-task branch lists / states: a far-future deadline (never fires
    # this run) rides alongside the three RECV branches so every task's
    # park exercises a REAL heap arm (issue #91) that the winning channel
    # claim must cancel.
    var branches_store = List[List[SelectBranch[Int]]]()
    var states_store = List[SelectState]()
    for _ in range(K):
        var b = List[SelectBranch[Int]]()
        b.append(recv_branch[Int](chp0))
        b.append(recv_branch[Int](chp1))
        b.append(recv_branch[Int](chp2))
        b.append(deadline_branch[Int](hp, clock, Deadline(60_000)))  # 60s: never due
        branches_store.append(b^)
        states_store.append(SelectState())
    var branches_ptr = branches_store.unsafe_ptr()
    var states_ptr = states_store.unsafe_ptr()

    var winners = List[Int]()
    var values = List[Int]()
    var run_counts = List[Int]()
    for _ in range(K):
        winners.append(-1)
        values.append(-1)
        run_counts.append(0)

    var sc = Scene()
    sc.chan_0 = chp0
    sc.chan_1 = chp1
    sc.chan_2 = chp2
    sc.branches = branches_ptr
    sc.states = states_ptr
    sc.winners = winners.unsafe_ptr()
    sc.values = values.unsafe_ptr()
    sc.run_counts = run_counts.unsafe_ptr()
    sc.now_ticks = 0
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var rt = create()
    var pool = List[TB]()
    for _ in range(K):
        pool.append(TB.create())
    var pool_ptr = pool.unsafe_ptr()
    for i in range(K):
        _ = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=pool_ptr[i]), 0)

    # ---- wave 1: every task parks (registers on all 3 channels + arms a heap timer) ----
    var served1 = scheduler_loop(rt, dispatch, ud)
    if served1 != K:
        red("initial drive served " + String(served1) + ", expected " + String(K))
    if chan0.recv_waiters_len() != K or chan1.recv_waiters_len() != K or chan2.recv_waiters_len() != K:
        red("every task must register on all three channels")
    if heap.size() != K:
        red("expected " + String(K) + " armed timers, got " + String(heap.size()))

    # ---- wave 2: K producers deliver exactly one item each, scattered round-robin ----
    var expected_sum = 0
    for i in range(K):
        var item = 1000 + i
        expected_sum += item
        var target = chp0
        if i % M == 1:
            target = chp1
        elif i % M == 2:
            target = chp2
        if not target[].try_send(item):
            red("producer " + String(i) + " send failed")
        drain_all(rt, chp0, chp1, chp2)
        var served = scheduler_loop(rt, dispatch, ud)
        if served != 1:
            red("resume drive " + String(i) + " served " + String(served) + ", expected 1")

    # ---- sweeps: zero leftovers, exactly-once delivery, heap fully drained ----
    if chan0.recv_waiters_len() != 0 or chan1.recv_waiters_len() != 0 or chan2.recv_waiters_len() != 0:
        red("leftover channel registrations after the stress round")
    if chan0.to_wake_len() != 0 or chan1.to_wake_len() != 0 or chan2.to_wake_len() != 0:
        red("leftover deferred wakes after the stress round")
    if not heap.is_empty():
        red("leftover heap entries after the stress round (expected all cancelled)")
    if rt.pending() != 0:
        red("leftover runnables after the stress round")

    var seen_sum = 0
    var completed = 0
    for i in range(K):
        if run_counts[i] != 2:
            red("task " + String(i) + " ran " + String(run_counts[i])
                + " times, expected exactly 2 (park + resume)")
        if winners[i] < 0 or winners[i] > 2:
            red("task " + String(i) + " has an invalid/timeout winner: " + String(winners[i]))
        if values[i] < 1000:
            red("task " + String(i) + " has no valid delivered value")
        seen_sum += values[i]
        completed += 1
    if completed != K:
        red("expected " + String(K) + " completions, got " + String(completed))
    if seen_sum != expected_sum:
        red("delivered value sum " + String(seen_sum) + " != expected "
            + String(expected_sum) + " (lost or duplicated delivery)")

    print("T15 select stress: PASS (K=" + String(K) + " tasks, M=" + String(M) + " channels)")
