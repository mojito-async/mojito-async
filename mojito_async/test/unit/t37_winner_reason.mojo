# mojito_async/test/unit/t37_winner_reason.mojo
#
# A4.4 (issue #58) — generation-claim exactly-one-winner: the WAKE-REASON
# STAMP pairwise winner matrix.
#
# Background: `unpark_current`'s claim (TaskControlBlock.wake_claim) already
# guarantees the WAITING -> RUNNABLE edge fires EXACTLY ONCE per generation
# (A0-T12, proven by t27_generation_wake.mojo and the t34c racing-duplicate
# driver) — that deliverable was already promoted from the spike.  The gap
# this driver closes: NOTHING previously recorded WHICH cause won the race.
# `unpark_current` gained an optional `win_reason` parameter (park.mojo) that
# — ONLY on a successful claim, INSIDE the same owner remote-queue guard that
# serializes the claim itself — stamps the winning cause onto the embedded
# WaitNode's `_reason` cell (spec §25/§29.2).  Stamping inside the SAME
# critical section as the claim (rather than the caller pre-stamping before
# calling unpark_current) is what makes the reason atomic with the claim: a
# racing SECOND cause's `win_reason` can never clobber the actual winner's
# label, because the fast top-level `state == RUNNABLE -> return` guard
# (park.mojo) makes every losing call a complete no-op before it ever
# touches the WaitNode.
#
# This driver exercises the four wake causes named by the issue — READY,
# CANCEL, TIMER, CLOSED (mojito_async.runtime.join_handle.SuspendReason) —
# against every pairwise combination the issue lists, in BOTH orderings:
#   (READY, CANCEL), (READY, TIMER), (READY, CLOSED),
#   (CANCEL, TIMER), (TIMER, CLOSED)
# For each ordered pair (first, second) fired at the SAME live generation:
#   - the FIRST call claims: state -> RUNNABLE, reason() == first, exactly
#     one new enqueue (rt.enqueued() advances by 1);
#   - the SECOND call (same required_gen, now stale — the task already left
#     WAITING) is a QUIET NO-OP: state stays RUNNABLE, reason() STILL ==
#     first (never overwritten), rt.enqueued() UNCHANGED.
# 10 scenarios (5 pairs x 2 orders) run against ONE task that re-parks a
# fresh epoch between each — deterministic, single-worker (no genuine OS
# race; the issue explicitly scopes this driver to the single-worker
# harness, matching the spike's t9_cancel_races.mojo / t1_event_protocol.mojo
# geometry).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.park import park_current, unpark_current
from mojito_async.runtime.join_handle import SuspendReason
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn


def red(what: String) raises -> None:
    print("T37 winner reason: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]
comptime N_SCENARIOS = Int(10)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """slice@0, a_id@1."""

    var slice: UnsafePointer[Int, MutAnyOrigin]
    var a_id: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.slice = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.a_id = self.slice


def body_done(ud: BytePtr) raises -> IntResult:
    return IntResult(1)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """A parks on every entry except the LAST (N_SCENARIOS park episodes,
    one fresh generation per scenario), then completes."""
    var sc = ud.bitcast[Scene]()
    if tid != sc[].a_id[]:
        raise Error("unexpected task id in t37 dispatcher")
    var h = _handle(tcb_addr, tid)
    var s = sc[].slice[]
    sc[].slice[] = s + 1
    if s < N_SCENARIOS:
        claim_running(h)
        park_current(rt, h)
        return 1
    _ = execute(h, body_done, ud)
    return 1


def _name(reason: Int) -> String:
    if reason == SuspendReason.READY:
        return "READY"
    if reason == SuspendReason.CANCEL:
        return "CANCEL"
    if reason == SuspendReason.TIMER:
        return "TIMER"
    if reason == SuspendReason.CLOSED:
        return "CLOSED"
    return "?(" + String(reason) + ")"


def main() raises:
    var failures = List[String]()
    var rt = create()
    var buf = stack_allocation[4, Int]()
    var sc = Scene()
    sc.slice = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf))
    sc.a_id = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(buf) + 1 * 8)
    buf[0] = 0
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    var tcb_a = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    buf[1] = h_a.id()

    # The 5 pairwise combinations the issue lists, both orderings.
    var firsts = List[Int]()
    var seconds = List[Int]()
    firsts.append(SuspendReason.READY); seconds.append(SuspendReason.CANCEL)
    firsts.append(SuspendReason.CANCEL); seconds.append(SuspendReason.READY)
    firsts.append(SuspendReason.READY); seconds.append(SuspendReason.TIMER)
    firsts.append(SuspendReason.TIMER); seconds.append(SuspendReason.READY)
    firsts.append(SuspendReason.READY); seconds.append(SuspendReason.CLOSED)
    firsts.append(SuspendReason.CLOSED); seconds.append(SuspendReason.READY)
    firsts.append(SuspendReason.CANCEL); seconds.append(SuspendReason.TIMER)
    firsts.append(SuspendReason.TIMER); seconds.append(SuspendReason.CANCEL)
    firsts.append(SuspendReason.TIMER); seconds.append(SuspendReason.CLOSED)
    firsts.append(SuspendReason.CLOSED); seconds.append(SuspendReason.TIMER)

    for i in range(N_SCENARIOS):
        var served = scheduler_loop(rt, dispatch, ud)
        if served != 1:
            failures.append("scenario " + String(i) + ": served "
                            + String(served) + ", expected 1")
        if h_a.state() != TaskControlBlock.WAITING:
            failures.append("scenario " + String(i)
                            + ": A must be WAITING before the race (state "
                            + String(h_a.state()) + ")")
        var gen = tcb_a.generation()
        var first = firsts[i]
        var second = seconds[i]
        var enq_before = rt.enqueued()

        unpark_current(rt, h_a, required_gen=gen, win_reason=first)
        if h_a.state() != TaskControlBlock.RUNNABLE:
            failures.append("scenario " + String(i) + " (" + _name(first)
                            + " vs " + _name(second)
                            + "): winner's claim did not make A RUNNABLE")
        if tcb_a.wait_node()[].reason() != first:
            failures.append("scenario " + String(i) + " (" + _name(first)
                            + " vs " + _name(second)
                            + "): winner reason not stamped (got "
                            + _name(tcb_a.wait_node()[].reason()) + ")")
        if rt.enqueued() != enq_before + 1:
            failures.append("scenario " + String(i)
                            + ": winner claim must enqueue exactly once")

        # the loser: same live generation, arrives SECOND -> quiet no-op.
        var enq_after_first = rt.enqueued()
        unpark_current(rt, h_a, required_gen=gen, win_reason=second)
        if h_a.state() != TaskControlBlock.RUNNABLE:
            failures.append("scenario " + String(i)
                            + ": loser must not change state")
        if tcb_a.wait_node()[].reason() != first:
            failures.append("scenario " + String(i) + ": loser (" + _name(second)
                            + ") CLOBBERED the winner's (" + _name(first)
                            + ") stamped reason — got "
                            + _name(tcb_a.wait_node()[].reason()))
        if rt.enqueued() != enq_after_first:
            failures.append("scenario " + String(i)
                            + ": loser must not enqueue (duplicate wake)")

    # drain the final RUNNABLE resume -> COMPLETED.
    var served_last = scheduler_loop(rt, dispatch, ud)
    if served_last != 1:
        failures.append("final drain served " + String(served_last)
                        + ", expected 1")
    if not h_a.is_completed():
        failures.append("A did not complete after the winner matrix")

    if len(failures) == 0:
        print("T37 winner reason: PASS")
    else:
        print("T37 winner reason: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        raise Error("T37 winner reason: FAIL")
