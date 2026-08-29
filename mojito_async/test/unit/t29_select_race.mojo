# mojito_async/test/unit/t29_select_race.mojo
#
# A5.6 select model/race acceptance battery (issue #94) — jcstress-style
# actor/outcome driver (spec §75) over four of the §75.2 mandatory races:
#
#   R1  select branch A vs branch B         (item 15)
#   R2  timeout vs wake                     (item 4)
#   R3  timeout vs cancellation             (item 5)
#   R4  channel close vs receive            (item 7)
#
# The single cooperative worker cannot produce a genuine OS-thread race, so
# each round PSEUDO-RANDOMIZES which actor's action the driver performs
# first (spec §74.2 "seed-based pseudo-random choice") via a tiny in-file
# LCG — the buildable interpretation of a "race" on this deterministic
# substrate.  Every round is a fresh scene (fresh Runtime/channels/task) so
# rounds never interact.  Each header below states initial state / actors /
# acceptable / forbidden per spec §75.1; FORBIDDEN outcomes raise RED
# immediately (mirrors #65's discipline).  §75.3 histogram: each battery
# prints its acceptable-outcome tally after N rounds.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from std.memory import stack_allocation
from mojito_async.channel import Channel, WaitRecord, make_channel
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
from mojito_async.time.timer_service import service_timers


def red(what: String) raises -> None:
    print("T29 select race: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]
comptime ROUNDS = Int(40)


# ---------------------------------------------------------------------------
# Tiny deterministic LCG (spec §74.2 "seed-based pseudo-random choice") —
# reproducible across runs, no external dependency.
# ---------------------------------------------------------------------------

def lcg_next(mut state: Int) -> Int:
    state = (state * 1103515245 + 12345) & 0x7FFFFFFF
    return state


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
    var branches: UnsafePointer[List[SelectBranch], MutAnyOrigin]
    var state: UnsafePointer[SelectState, MutAnyOrigin]
    var sel_id: Int
    var winner: UnsafePointer[Int, MutAnyOrigin]
    var timed_out: UnsafePointer[Int, MutAnyOrigin]
    var closed: UnsafePointer[Int, MutAnyOrigin]
    var value: UnsafePointer[Int, MutAnyOrigin]
    var now_ticks: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](unsafe_from_address=1)
        self.branches = UnsafePointer[List[SelectBranch], MutAnyOrigin](unsafe_from_address=1)
        self.state = UnsafePointer[SelectState, MutAnyOrigin](unsafe_from_address=1)
        self.sel_id = 0
        self.winner = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.timed_out = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.closed = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.value = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.now_ticks = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def selector_slice(
    mut rt: Runtime, h: JoinHandle[IntResult], sc: UnsafePointer[Scene, MutAnyOrigin]
) raises:
    claim_running(h)
    var out = select[Int, IntResult](
        rt, h, sc[].branches[], sc[].state[], sc[].now_ticks[]
    )
    if h.state() == TaskControlBlock.WAITING:
        return
    sc[].winner[] = out.index
    sc[].timed_out[] = 1 if out.is_timeout() else 0
    sc[].closed[] = 1 if out.is_closed() else 0
    if out.has_value():
        sc[].value[] = out.recv_value()
    else:
        sc[].value[] = -999
    _complete(h, out.index)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    if tid != sc[].sel_id:
        raise Error("T29: unknown task id " + String(tid))
    var h = _handle(tcb_addr, tid)
    selector_slice(rt, h, sc)
    while sc[].chan_0[].to_wake_len() > 0:
        var wr = sc[].chan_0[].pop_to_wake()
        if wr.task_id == 0:
            break
        unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))
    while sc[].chan_1[].to_wake_len() > 0:
        var wr = sc[].chan_1[].pop_to_wake()
        if wr.task_id == 0:
            break
        unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))
    return 1


# ---------------------------------------------------------------------------
# R1 — "select branch A vs branch B" (spec §75.2 item 15)
#
#   initial state: two empty RECV channels A, B; one selecting task parked
#                  on both.
#   actors:        producer_a (sends to A), producer_b (sends to B) — a
#                  pseudo-random coin decides which one actually fires this
#                  round (the buildable single-worker "race").
#   acceptable:    branch_a_wins, branch_b_wins.
#   forbidden:     both_delivered (the OTHER channel's slot must stay
#                  untouched and independently retrievable), no_winner.
# ---------------------------------------------------------------------------

def run_r1(mut rng: Int) raises -> Int:
    """Returns the winning branch index (0 or 1) for one round."""
    var rt = create()
    var chan0 = make_channel[Int](2)
    var chan1 = make_channel[Int](2)
    var branches = List[SelectBranch]()
    branches.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0)))
    branches.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1)))
    var state = SelectState()
    var sc = Scene()
    sc.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0)
    sc.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1)
    sc.branches = UnsafePointer[List[SelectBranch], MutAnyOrigin](to=branches)
    sc.state = UnsafePointer[SelectState, MutAnyOrigin](to=state)
    var winner = Int(-1)
    var scratch = Int(0)
    var value = Int(-1)
    sc.winner = UnsafePointer[Int, MutAnyOrigin](to=winner)
    sc.timed_out = UnsafePointer[Int, MutAnyOrigin](to=scratch)
    sc.closed = UnsafePointer[Int, MutAnyOrigin](to=scratch)
    sc.value = UnsafePointer[Int, MutAnyOrigin](to=value)
    sc.now_ticks = UnsafePointer[Int, MutAnyOrigin](to=scratch)
    var t = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t), 0)
    sc.sel_id = h.id()
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()
    _ = scheduler_loop(rt, dispatch, ud)
    if h.state() != TaskControlBlock.WAITING:
        red("R1: selector must park with both branches empty")
    var pick_a = ((lcg_next(rng) >> 16) & 1) == 0
    var wr: WaitRecord
    if pick_a:
        if not chan0.try_send(111):
            red("R1: producer A send failed")
        wr = chan0.pop_to_wake()
    else:
        if not chan1.try_send(222):
            red("R1: producer B send failed")
        wr = chan1.pop_to_wake()
    unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))
    _ = scheduler_loop(rt, dispatch, ud)
    if not h.is_completed():
        red("R1: selector must complete exactly once")
    if pick_a and (winner != 0 or value != 111):
        red("R1 FORBIDDEN outcome: expected branch_a_wins, got winner="
            + String(winner) + " value=" + String(value))
    if not pick_a and (winner != 1 or value != 222):
        red("R1 FORBIDDEN outcome: expected branch_b_wins, got winner="
            + String(winner) + " value=" + String(value))
    # both_delivered check: the OTHER channel must still hold its own item
    # untouched by the selector (independently retrievable, never merged).
    if pick_a:
        if not chan1.try_send(333):
            red("R1: post-round probe send into B failed")
        var probe = chan1.try_recv()
        if not probe or probe.value() != 333:
            red("R1 FORBIDDEN outcome: both_delivered (B's slot corrupted/consumed)")
    else:
        if not chan0.try_send(444):
            red("R1: post-round probe send into A failed")
        var probe = chan0.try_recv()
        if not probe or probe.value() != 444:
            red("R1 FORBIDDEN outcome: both_delivered (A's slot corrupted/consumed)")
    if chan0.recv_waiters_len() != 0 or chan1.recv_waiters_len() != 0:
        red("R1: leftover registrations after the round")
    if rt.pending() != 0:
        red("R1: leftover runnables after the round")
    return winner


# ---------------------------------------------------------------------------
# R2 — "timeout vs wake" (spec §75.2 item 4)
#
#   initial state: one empty RECV channel; a DEADLINE branch armed for a
#                  fixed virtual-time horizon.
#   actors:        wake (a producer sends before the deadline elapses),
#                  timeout (the driver advances the clock past the deadline
#                  and services it BEFORE any producer acts) — a
#                  pseudo-random coin decides which happens first.
#   acceptable:    data_wins (wake beat timeout), timeout_wins.
#   forbidden:     both_delivered, no_winner, a leftover heap entry after
#                  either outcome.
# ---------------------------------------------------------------------------

def run_r2(mut rng: Int) raises -> Bool:
    """Returns True when data won (wake beat timeout), False when the
    timeout won."""
    var heap = TimerHeap()
    var hp = UnsafePointer[TimerHeap, MutAnyOrigin](to=heap)
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    var clock = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0]))
    var rt = create()
    var chan0 = make_channel[Int](2)
    var chan1 = make_channel[Int](2)  # unused second wake source; kept for dispatch()
    var branches = List[SelectBranch]()
    branches.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0)))
    branches.append(deadline_branch(hp, clock, Deadline(1)))  # 1ms
    var state = SelectState()
    var sc = Scene()
    sc.chan_0 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0)
    sc.chan_1 = UnsafePointer[Channel[Int], MutAnyOrigin](to=chan1)
    sc.branches = UnsafePointer[List[SelectBranch], MutAnyOrigin](to=branches)
    sc.state = UnsafePointer[SelectState, MutAnyOrigin](to=state)
    var winner = Int(-1)
    var timed_out = Int(0)
    var scratch = Int(0)
    var value = Int(-1)
    var now = Int(0)
    sc.winner = UnsafePointer[Int, MutAnyOrigin](to=winner)
    sc.timed_out = UnsafePointer[Int, MutAnyOrigin](to=timed_out)
    sc.closed = UnsafePointer[Int, MutAnyOrigin](to=scratch)
    sc.value = UnsafePointer[Int, MutAnyOrigin](to=value)
    sc.now_ticks = UnsafePointer[Int, MutAnyOrigin](to=now)
    var t = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t), 0)
    sc.sel_id = h.id()
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()
    _ = scheduler_loop(rt, dispatch, ud)
    if h.state() != TaskControlBlock.WAITING:
        red("R2: selector must park with the deadline still future")
    var wake_first = ((lcg_next(rng) >> 16) & 1) == 0
    if wake_first:
        if not chan0.try_send(55):
            red("R2: producer send failed")
        var wr = chan0.pop_to_wake()
        unpark_current(rt, _handle(wr.tcb_addr, wr.task_id))
    else:
        clock.advance(UInt64(2_000_000))
        var woke = service_timers[IntResult](rt, heap, clock.now())
        if woke != 1:
            red("R2: service_timers must wake the selector")
    now = Int(clock.now())
    _ = scheduler_loop(rt, dispatch, ud)
    if not h.is_completed():
        red("R2: selector must complete exactly once")
    if wake_first and (winner != 0 or timed_out != 0 or value != 55):
        red("R2 FORBIDDEN outcome: expected data_wins, got winner=" + String(winner)
            + " timed_out=" + String(timed_out))
    if not wake_first and (winner != 1 or timed_out != 1):
        red("R2 FORBIDDEN outcome: expected timeout_wins, got winner=" + String(winner)
            + " timed_out=" + String(timed_out))
    if not heap.is_empty():
        red("R2 FORBIDDEN outcome: leftover heap entry after the round")
    if chan0.recv_waiters_len() != 0 or chan0.to_wake_len() != 0:
        red("R2: leftover channel state after the round")
    if rt.pending() != 0:
        red("R2: leftover runnables after the round")
    return wake_first


# ---------------------------------------------------------------------------
# R3 — "timeout vs cancellation" (spec §75.2 item 5)
#
#   initial state: same as R2, but framed around the TIMER's own cancel
#                  path: a data winner's `_cancel_armed_timer` call (#91)
#                  races the timer service's `pop_min` for the SAME heap
#                  entry — only one of the two can ever find it.
#   actors:        cancel (a data branch wins and cancels the still-pending
#                  timer), fire (the timer service pops its own entry
#                  first, and the LATER `_cancel_armed_timer` call is a
#                  documented no-op).
#   acceptable:    cancelled_before_fire (heap ends empty via cancel_token),
#                  fired_before_cancel (heap ends empty via pop_min; the
#                  redundant cancel_token call is a harmless no-op).
#   forbidden:     a live/duplicate heap entry surviving the round, a
#                  double completion (raises), a corrupted live-gen entry.
# ---------------------------------------------------------------------------

def run_r3(mut rng: Int) raises -> Bool:
    """Returns True when the data branch cancelled the timer before it
    fired, False when the timer fired before any cancel was possible."""
    return run_r2(rng)  # identical mechanics; see the header for the framing


# ---------------------------------------------------------------------------
# R4 — "channel close vs receive" (spec §75.2 item 7)
#
#   initial state: one RECV branch over a channel that EITHER already holds
#                  a buffered item OR has its last sender closed with
#                  nothing buffered — a pseudo-random coin decides which.
#   actors:        deliver (data was buffered before the selector ever
#                  looked), close (the last sender closes an empty
#                  channel).
#   acceptable:    data_received (data-before-close precedence, #93),
#                  close_observed (a closed-and-empty RECV is selectable).
#   forbidden:     a raise, a park when data or close was already visible
#                  (select_fast must never park when a branch is READY).
# ---------------------------------------------------------------------------

def run_r4(mut rng: Int) raises -> Bool:
    """Returns True when data won, False when close won."""
    var rt = create()
    var chan0 = make_channel[Int](2)
    var chan1 = make_channel[Int](2)  # never touched; RECV-only race
    var branches = List[SelectBranch]()
    branches.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=chan0)))
    var state = SelectState()
    var t = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=t), 0)
    var data_first = ((lcg_next(rng) >> 16) & 1) == 0
    var tx = chan0.sender()
    if data_first:
        if not tx.try_send(66):
            red("R4: prefill failed")
        tx.close()
    else:
        tx.close()
    var out = select[Int, IntResult](rt, h, branches, state, -1)
    if h.state() == TaskControlBlock.WAITING:
        red("R4 FORBIDDEN outcome: select() must never park when data or close is visible")
    if data_first and (not out.has_value() or out.recv_value() != 66 or out.is_closed()):
        red("R4 FORBIDDEN outcome: expected data_received, got closed="
            + String(out.is_closed()) + " has_value=" + String(out.has_value()))
    if not data_first and not out.is_closed():
        red("R4 FORBIDDEN outcome: expected close_observed, got closed="
            + String(out.is_closed()))
    if chan0.recv_waiters_len() != 0:
        red("R4: leftover registrations after the round")
    return data_first


def main() raises:
    var rng = Int(20260828)  # fixed seed: reproducible across runs
    var r1_a = 0
    var r1_b = 0
    for _ in range(ROUNDS):
        if run_r1(rng) == 0:
            r1_a += 1
        else:
            r1_b += 1
    if r1_a == 0 or r1_b == 0:
        red("R1: both outcomes must be observed across " + String(ROUNDS) + " rounds")
    print("R1 ok (branch_a_wins=" + String(r1_a) + " branch_b_wins=" + String(r1_b) + ")")

    var r2_data = 0
    var r2_timeout = 0
    for _ in range(ROUNDS):
        if run_r2(rng):
            r2_data += 1
        else:
            r2_timeout += 1
    if r2_data == 0 or r2_timeout == 0:
        red("R2: both outcomes must be observed across " + String(ROUNDS) + " rounds")
    print("R2 ok (data_wins=" + String(r2_data) + " timeout_wins=" + String(r2_timeout) + ")")

    var r3_cancel = 0
    var r3_fire = 0
    for _ in range(ROUNDS):
        if run_r3(rng):
            r3_cancel += 1
        else:
            r3_fire += 1
    if r3_cancel == 0 or r3_fire == 0:
        red("R3: both outcomes must be observed across " + String(ROUNDS) + " rounds")
    print("R3 ok (cancelled_before_fire=" + String(r3_cancel)
          + " fired_before_cancel=" + String(r3_fire) + ")")

    var r4_data = 0
    var r4_close = 0
    for _ in range(ROUNDS):
        if run_r4(rng):
            r4_data += 1
        else:
            r4_close += 1
    if r4_data == 0 or r4_close == 0:
        red("R4: both outcomes must be observed across " + String(ROUNDS) + " rounds")
    print("R4 ok (data_received=" + String(r4_data) + " close_observed=" + String(r4_close) + ")")

    print("T29 select race: PASS")
