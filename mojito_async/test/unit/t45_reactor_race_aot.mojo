# mojito_async/test/unit/t45_reactor_race_aot.mojo
#
# A7.7 (issue #81) — readiness/cancellation/timeout race battery for the
# reactor spine (G1/G2, issues #75/#76 — merged PR #131), EXTENDING
# t40_io_token_aot.mojo's Part A (stale-generation drop) and Part B
# (cancel-then-reuse, zero double-enqueue) rather than duplicating them —
# read t40 first if editing this file; the two together are the "one
# table, multiple consumers" battery issue #81 point 5 asks for.
#
# This driver covers the bullets t40 does NOT already exercise:
#   - readiness arriving BEFORE the waiter attaches vs AFTER (both orders;
#     issue #81 bullet 1 "readiness arriving before park vs after commit"
#     — reactor.mojo's own CALL CONTRACT names this exact window:
#     `attach_waiter`'s already-`IO_OP_READY` branch);
#   - two pending ops sharing the SAME op table, one readiness wake each —
#     only the op whose fd actually became ready wakes; the other stays
#     parked (issue #81 bullet 3);
#   - readiness vs deadline expiry, BOTH orders, LCG-randomized per round
#     (issue #81 bullet 1) — G6/#80's real timer<->reactor interlock isn't
#     merged yet, so this composes the ALREADY-MERGED primitives the way
#     #80 documents it will (TimerHeap + service_timers racing
#     Reactor.poll on the SAME task): whichever `unpark_current` call
#     claims first wins (C6), the loser is a safe no-op because
#     `unpark_current` fast-returns on an already-RUNNABLE task regardless
#     of which side's generation it was carrying — exactly the
#     t29_select_race.mojo R2/R3 "redundant call is a harmless no-op"
#     precedent, applied to the reactor instead of channel/select;
#   - readiness vs a concurrent cancel_op, BOTH orders, LCG-randomized
#     (issue #81 bullet 1, the FIRST-listed race: "readiness arriving
#     against a concurrent cancel") — uses the REAL reactor/cancel.mojo
#     cancel_op (issue #80, merged), the same primitive net/tcp_stream.mojo
#     and net/tcp_listener.mojo's close/cancel paths call;
#   - cancel_op vs timeout, BOTH orders, LCG-randomized (issue #81
#     bullet 1) — both sides now the REAL merged primitives:
#     reactor/cancel.mojo's cancel_op racing time/timer_service's
#     service_timers, exactly the composition #80's own module docstring
#     documents (C6 exactly-one-winner via unpark_current's RUNNABLE
#     fast-return, no extra locking in this module);
#   - a higher-iteration storm (N=120, vs t40's N=6) mixing all three
#     termination causes {ready, cancel, timeout} across the SAME shared
#     IoOpTable, asserting it ends fully coherent (live_count() == 0, no
#     leaked slot, no double-enqueue) — the issue's "high-iteration
#     storm" tightening once the deterministic set passes.
#
# The harness style mirrors t29_select_race.mojo exactly: a tiny in-file
# LCG for spec-\u00a774.2 "seed-based pseudo-random choice" actor ordering, a
# per-round run_rN() returning the observed winner, and a fixed ROUNDS
# loop in main() asserting BOTH outcomes are observed with a printed
# histogram; FORBIDDEN outcomes red() immediately (t65's discipline, per
# the park-race battery this issue mirrors).
#
# t42_io_cancel_deadline_aot.mojo already covers cancel_op/
# service_io_deadlines/cancel_and_close deterministically (single order
# each, plus timer-vs-ready both orders); t42_tcp_accept_aot.mojo already
# covers close-while-pending deterministically.  This file's job is the
# LCG-randomized BOTH-ORDERS histogram battery (issue #81's actual ask)
# over the primitives those files exercise once — read them first if
# editing this file to avoid re-deriving their scenarios here.
#
# AOT-only: imports reactor/poller.mojo's vendor/mojito_sys_io externs
# (dylib mjs_poller_* symbols), so this runs via the run.sh unit AOT loop
# (modular/modular#6971).  One local libc extern (`pipe`), matching the
# house "local libc externs run AOT-only" convention.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation

from mojito_async.reactor.io_token import IoOpKind
from mojito_async.reactor.cancel import cancel_op
from mojito_async.reactor.reactor import Reactor, make_reactor
from mojito_async.runtime.join_handle import SuspendReason
from mojito_async.runtime.park import (
    park_commit,
    park_prepare,
    park_validate,
)
from mojito_async.runtime.runtime import Nil, Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import claim_running, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Duration, from_millis
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.time.timer_service import service_timers
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoInterest


@extern("pipe")
def c_pipe(fds: UnsafePointer[Int32, MutAnyOrigin]) abi("C") -> Int32:
    ...


def red(what: String) raises -> None:
    print("T45 reactor race: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[Nil]
comptime ROUNDS = Int(40)
comptime N_STORM = Int(120)


def lcg_next(mut state: Int) -> Int:
    state = (state * 1103515245 + 12345) & 0x7FFFFFFF
    return state


# ---------------------------------------------------------------------------
# Scenario 1 — readiness BEFORE attach_waiter vs AFTER (both explicit
# orders; issue #81 bullet 1).
# ---------------------------------------------------------------------------


def run_before_attach() raises:
    """Write + drain the pipe BEFORE the task's park ever attaches a
    waiter: reactor.mojo's CALL CONTRACT says attach_waiter's own
    already-IO_OP_READY check must deliver the wake immediately instead of
    losing it — the task must still reach RUNNABLE/COMPLETED with exactly
    one wake, never a hang."""
    var fds = stack_allocation[2, Int32]()
    if c_pipe(fds) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds[0]
    var wfd = fds[1]
    var reactor = make_reactor()
    var rt = create()
    var tcb = TB.create()
    var h = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    claim_running(h)
    var handle = NativeIoHandle(rfd)
    var token = reactor.register_op(handle, IoInterest.READABLE, IoOpKind.READ)

    # Readiness FIRST: a real write, then a real poll drains it into
    # IO_OP_READY — all before the task ever parks.
    var wf = FileDescriptor(Int(wfd))
    wf.write("A")
    var pre_ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if len(pre_ready) != 1 or pre_ready[0].slot != token.slot:
        red("before-attach: setup write must be observed before the task parks")

    var enq_before = rt.enqueued()
    park_prepare(h)
    if park_validate(h):
        red("before-attach: op-table readiness must not be confused with "
            "park.mojo's own TCB-level early-wake latch")
    park_commit(h, SuspendReason.IO)
    if h.state() != TaskControlBlock.WAITING:
        red("before-attach: park_commit must land WAITING before attach")
    reactor.attach_waiter(rt, token, Int(h.tcb()), h.id(), h.tcb()[].generation())
    if h.state() != TaskControlBlock.RUNNABLE:
        red("before-attach: attach_waiter must wake IMMEDIATELY on an "
            "already-ready slot, never leave the task parked")
    if rt.enqueued() != enq_before + 1:
        red("before-attach: exactly one enqueue, never zero or two")
    reactor.unregister(token)
    if reactor.live_count() != 0:
        red("before-attach: unregister must release the slot")


def run_after_attach() raises:
    """The normal order: attach_waiter runs on a merely-REGISTERED (not
    yet ready) slot, then a LATER real write + poll delivers the wake —
    the baseline this suite's other scenarios build on."""
    var fds = stack_allocation[2, Int32]()
    if c_pipe(fds) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds[0]
    var wfd = fds[1]
    var reactor = make_reactor()
    var rt = create()
    var tcb = TB.create()
    var h = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    claim_running(h)
    var handle = NativeIoHandle(rfd)
    var token = reactor.register_op(handle, IoInterest.READABLE, IoOpKind.READ)
    park_prepare(h)
    if park_validate(h):
        red("after-attach: nothing should be latched before any readiness exists")
    park_commit(h, SuspendReason.IO)
    reactor.attach_waiter(rt, token, Int(h.tcb()), h.id(), h.tcb()[].generation())
    if h.state() != TaskControlBlock.WAITING:
        red("after-attach: task must still be parked — nothing written yet")

    var enq_before = rt.enqueued()
    var wf = FileDescriptor(Int(wfd))
    wf.write("B")
    var ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if len(ready) != 1 or ready[0].slot != token.slot:
        red("after-attach: expected exactly the registered slot to become ready")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("after-attach: reactor.poll's wake must land the task RUNNABLE")
    if rt.enqueued() != enq_before + 1:
        red("after-attach: exactly one enqueue, never zero or two")
    reactor.unregister(token)
    if reactor.live_count() != 0:
        red("after-attach: unregister must release the slot")


# ---------------------------------------------------------------------------
# Scenario 2 — two pending ops share ONE table; only the ready op's owner
# wakes (issue #81 bullet 3).
# ---------------------------------------------------------------------------


def run_two_pending_share_table() raises:
    var fds_a = stack_allocation[2, Int32]()
    if c_pipe(fds_a) != 0:
        red("setup: pipe(2) failed")
    var rfd_a = fds_a[0]
    var wfd_a = fds_a[1]
    var fds_b = stack_allocation[2, Int32]()
    if c_pipe(fds_b) != 0:
        red("setup: pipe(2) failed")
    var rfd_b = fds_b[0]
    var wfd_b = fds_b[1]
    var reactor = make_reactor()
    var rt = create()

    var tcb_a = TB.create()
    var h_a = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    claim_running(h_a)
    var token_a = reactor.register_and_park[Nil](
        rt, h_a, NativeIoHandle(rfd_a), IoInterest.READABLE, IoOpKind.READ
    )
    var tcb_b = TB.create()
    var h_b = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    claim_running(h_b)
    var token_b = reactor.register_and_park[Nil](
        rt, h_b, NativeIoHandle(rfd_b), IoInterest.READABLE, IoOpKind.READ
    )
    if h_a.state() != TaskControlBlock.WAITING or h_b.state() != TaskControlBlock.WAITING:
        red("two-pending: both ops must be parked before either fd is written")
    if reactor.live_count() != 2:
        red("two-pending: expected two live table slots")

    # Only B's fd becomes ready — A must remain untouched.
    var wf_b = FileDescriptor(Int(wfd_b))
    wf_b.write("Z")
    var ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if len(ready) != 1 or ready[0].slot != token_b.slot:
        red("two-pending: exactly B's slot must be reported ready, got "
            + String(len(ready)) + " ready token(s)")
    if h_b.state() != TaskControlBlock.RUNNABLE:
        red("two-pending: B must wake")
    if h_a.state() != TaskControlBlock.WAITING:
        red("two-pending: A (never written) must stay parked — no cross-slot wake")
    reactor.unregister(token_b)

    # A's own turn: write A's fd, A wakes, B (already gone) never re-fires.
    var wf_a = FileDescriptor(Int(wfd_a))
    wf_a.write("Y")
    var ready2 = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if len(ready2) != 1 or ready2[0].slot != token_a.slot:
        red("two-pending: A's own delivery must be reported on its own turn")
    if h_a.state() != TaskControlBlock.RUNNABLE:
        red("two-pending: A must wake on its own readiness")
    reactor.unregister(token_a)
    if reactor.live_count() != 0:
        red("two-pending: both slots must be released at the end")


# ---------------------------------------------------------------------------
# Scenario 3 — readiness vs deadline, both orders (LCG round; issue #81
# bullet 1).  Composes the ALREADY-MERGED TimerHeap/service_timers with the
# ALREADY-MERGED Reactor — exactly the composition #80's design doc says
# G6 will wire automatically once it lands.
# ---------------------------------------------------------------------------


def run_r_ready_vs_timeout(mut rng: Int) raises -> Bool:
    """Returns True when readiness won, False when the timeout won."""
    var fds = stack_allocation[2, Int32]()
    if c_pipe(fds) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds[0]
    var wfd = fds[1]
    var reactor = make_reactor()
    var rt = create()
    var heap = TimerHeap()
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    var clock = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0]))

    var tcb = TB.create()
    var h = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    claim_running(h)
    var token = reactor.register_and_park[Nil](
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if h.state() != TaskControlBlock.WAITING:
        red("ready-vs-timeout: task must park before either side fires")
    _ = heap.arm(h.id(), Int(h.tcb()), clock.now() + UInt64(1_000_000))

    var enq_before = rt.enqueued()
    var ready_wins = ((lcg_next(rng) >> 16) & 1) == 0
    if ready_wins:
        var wf = FileDescriptor(Int(wfd))
        wf.write("R")
        var ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
        if len(ready) != 1 or ready[0].slot != token.slot:
            red("ready-vs-timeout: readiness delivery must resolve the registered slot")
    else:
        clock.advance(UInt64(2_000_000))
        var woke = service_timers[Nil](rt, heap, clock.now())
        if woke != 1:
            red("ready-vs-timeout: service_timers must wake the parked task")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("ready-vs-timeout: exactly one side must claim the task")
    if rt.enqueued() != enq_before + 1:
        red("ready-vs-timeout: exactly one enqueue (no double-wake)")

    # Drain the LOSER harmlessly: the redundant call must be a no-op
    # (t29_select_race.mojo R3 precedent) since the task is already
    # RUNNABLE regardless of which side calls unpark_current second.
    if ready_wins:
        clock.advance(UInt64(2_000_000))
        _ = service_timers[Nil](rt, heap, clock.now())
    else:
        var wf2 = FileDescriptor(Int(wfd))
        wf2.write("L")
        _ = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if rt.enqueued() != enq_before + 1:
        red("ready-vs-timeout: the loser's redundant delivery must NOT enqueue again")
    if not heap.is_empty():
        red("ready-vs-timeout: the timer entry must be drained either way")
    reactor.unregister(token)
    if reactor.live_count() != 0:
        red("ready-vs-timeout: the reactor slot must be released either way")
    return ready_wins


# ---------------------------------------------------------------------------
# Scenario 3b — readiness vs a concurrent cancel_op, both orders (LCG
# round; issue #81 bullet 1's FIRST-listed race).  Uses the REAL
# reactor/cancel.mojo cancel_op (issue #80, merged) \u2014 the same primitive
# net/tcp_stream.mojo's close_current/cancel paths call.
# ---------------------------------------------------------------------------


def run_r_ready_vs_cancel(mut rng: Int) raises -> Bool:
    """Returns True when readiness won, False when cancel_op won."""
    var fds = stack_allocation[2, Int32]()
    if c_pipe(fds) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds[0]
    var wfd = fds[1]
    var reactor = make_reactor()
    var rt = create()

    var tcb = TB.create()
    var h = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    claim_running(h)
    var token = reactor.register_and_park[Nil](
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if h.state() != TaskControlBlock.WAITING:
        red("ready-vs-cancel: task must park before either side fires")

    var enq_before = rt.enqueued()
    var ready_wins = ((lcg_next(rng) >> 16) & 1) == 0
    if ready_wins:
        var wf = FileDescriptor(Int(wfd))
        wf.write("R")
        var ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
        if len(ready) != 1 or ready[0].slot != token.slot:
            red("ready-vs-cancel: readiness delivery must resolve the registered slot")
        if h.state() != TaskControlBlock.RUNNABLE:
            red("ready-vs-cancel: readiness must claim the task")
        # cancel_op racing in AFTER readiness already won: cancel_op
        # pre-checks WAITING (module docstring) and finds the task already
        # RUNNABLE \u2014 a benign loss, never a raise, never a double-enqueue.
        var won_after = cancel_op(reactor, rt, token, Int(h.tcb()), h.id())
        if won_after:
            red("ready-vs-cancel: cancel_op must not win after readiness already claimed")
        reactor.unregister(token)
    else:
        var won = cancel_op(reactor, rt, token, Int(h.tcb()), h.id())
        if not won:
            red("ready-vs-cancel: cancel_op must win against a still-WAITING waiter")
        if h.state() != TaskControlBlock.RUNNABLE:
            red("ready-vs-cancel: cancel_op must claim the task")
        # A later readiness for the same (now-unregistered) fd must be a
        # provable no-op (G2: the slot is FREE, drain_ready drops it).
        var wf2 = FileDescriptor(Int(wfd))
        wf2.write("L")
        var late = reactor.poll(rt, Optional[Duration](from_millis(50)))
        if len(late) != 0:
            red("ready-vs-cancel: a stale delivery arrived after cancel_op unregistered")
    if rt.enqueued() != enq_before + 1:
        red("ready-vs-cancel: exactly one enqueue, never zero or two")
    if reactor.live_count() != 0:
        red("ready-vs-cancel: the reactor slot must be released either way")
    return ready_wins


# ---------------------------------------------------------------------------
# Scenario 4 — cancel_op vs timeout, both orders (LCG round; issue #81
# bullet 1).  Both sides are the REAL merged primitives (reactor/
# cancel.mojo's cancel_op vs time/timer_service's service_timers).
# ---------------------------------------------------------------------------


def run_r_cancel_vs_timeout(mut rng: Int) raises -> Bool:
    """Returns True when cancel won, False when the timeout won."""
    var fds = stack_allocation[2, Int32]()
    if c_pipe(fds) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds[0]
    var wfd = fds[1]
    var reactor = make_reactor()
    var rt = create()
    var heap = TimerHeap()
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    var clock = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0]))

    var tcb = TB.create()
    var h = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    claim_running(h)
    var token = reactor.register_and_park[Nil](
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    _ = heap.arm(h.id(), Int(h.tcb()), clock.now() + UInt64(1_000_000))

    var enq_before = rt.enqueued()
    var cancel_wins = ((lcg_next(rng) >> 16) & 1) == 0
    if cancel_wins:
        var won = cancel_op(reactor, rt, token, Int(h.tcb()), h.id())
        if not won:
            red("cancel-vs-timeout: cancel_op must win against a still-WAITING waiter")
    else:
        clock.advance(UInt64(2_000_000))
        var woke = service_timers[Nil](rt, heap, clock.now())
        if woke != 1:
            red("cancel-vs-timeout: service_timers must wake the parked task")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("cancel-vs-timeout: exactly one side must claim the task")
    if rt.enqueued() != enq_before + 1:
        red("cancel-vs-timeout: exactly one enqueue (no double-wake)")

    if cancel_wins:
        clock.advance(UInt64(2_000_000))
        _ = service_timers[Nil](rt, heap, clock.now())
    else:
        var won_late = cancel_op(reactor, rt, token, Int(h.tcb()), h.id())
        if won_late:
            red("cancel-vs-timeout: cancel_op must not win after the timeout already claimed")
    if rt.enqueued() != enq_before + 1:
        red("cancel-vs-timeout: the loser's redundant delivery must NOT enqueue again")
    if not heap.is_empty():
        red("cancel-vs-timeout: the timer entry must be drained either way")
    reactor.unregister(token)
    if reactor.live_count() != 0:
        red("cancel-vs-timeout: the reactor slot must be released either way")
    return cancel_wins


# ---------------------------------------------------------------------------
# Scenario 5 — high-iteration storm mixing {ready, cancel, timeout} across
# the SHARED table (issue #81 point 5 / "high-iteration randomized storm").
# ---------------------------------------------------------------------------


def run_storm(mut rng: Int) raises:
    var reactor = make_reactor()
    var rt = create()
    var heap = TimerHeap()
    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    var clock = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0]))
    var n_ready = 0
    var n_cancel = 0
    var n_timeout = 0

    for i in range(N_STORM):
        var fds = stack_allocation[2, Int32]()
        if c_pipe(fds) != 0:
            red("setup: pipe(2) failed")
        var rfd = fds[0]
        var wfd = fds[1]
        var tcb = TB.create()
        var h = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
        claim_running(h)
        var token = reactor.register_and_park[Nil](
            rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
        )
        if h.state() != TaskControlBlock.WAITING:
            red("storm " + String(i) + ": task must park")
        _ = heap.arm(h.id(), Int(h.tcb()), clock.now() + UInt64(1_000_000))
        var enq_before = rt.enqueued()
        var pick = lcg_next(rng) % 3
        if pick == 0:
            var wf = FileDescriptor(Int(wfd))
            wf.write("S")
            var ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
            if len(ready) != 1 or ready[0].slot != token.slot:
                red("storm " + String(i) + ": expected the registered slot ready")
            n_ready += 1
        elif pick == 1:
            var won = cancel_op(reactor, rt, token, Int(h.tcb()), h.id())
            if not won:
                red("storm " + String(i) + ": cancel_op must win against a still-WAITING waiter")
            n_cancel += 1
        else:
            clock.advance(UInt64(2_000_000))
            var woke = service_timers[Nil](rt, heap, clock.now())
            if woke != 1:
                red("storm " + String(i) + ": timer must fire")
            n_timeout += 1
        if h.state() != TaskControlBlock.RUNNABLE:
            red("storm " + String(i) + ": task never reached RUNNABLE (lost wakeup)")
        if rt.enqueued() != enq_before + 1:
            red("storm " + String(i) + ": exactly one enqueue, never a double-wake")
        # Drain whichever side did NOT win — always a safe no-op once the
        # task is already RUNNABLE.
        clock.advance(UInt64(2_000_000))
        _ = service_timers[Nil](rt, heap, clock.now())
        reactor.unregister(token)
        if reactor.live_count() != 0:
            red("storm " + String(i) + ": table slot must be released before the next round")

    if n_ready == 0 or n_cancel == 0 or n_timeout == 0:
        red("storm: all three termination causes must be observed across "
            + String(N_STORM) + " rounds (got ready=" + String(n_ready)
            + " cancel=" + String(n_cancel) + " timeout=" + String(n_timeout) + ")")
    if not heap.is_empty():
        red("storm: timer heap must be fully drained at the end")
    print("T45 storm ok (ready=" + String(n_ready) + " cancel=" + String(n_cancel)
          + " timeout=" + String(n_timeout) + ")")


def main() raises:
    run_before_attach()
    run_after_attach()
    print("T45 scenario 1 ok (before-attach + after-attach both deliver exactly once)")

    run_two_pending_share_table()
    print("T45 scenario 2 ok (two pending ops share one table; only the ready owner wakes)")

    var rng = Int(20260829)  # fixed seed: reproducible across runs
    var r3_ready = 0
    var r3_timeout = 0
    for _ in range(ROUNDS):
        if run_r_ready_vs_timeout(rng):
            r3_ready += 1
        else:
            r3_timeout += 1
    if r3_ready == 0 or r3_timeout == 0:
        red("scenario 3: both outcomes must be observed across " + String(ROUNDS) + " rounds")
    print("T45 scenario 3 ok (ready_wins=" + String(r3_ready) + " timeout_wins=" + String(r3_timeout) + ")")

    var r3b_ready = 0
    var r3b_cancel = 0
    for _ in range(ROUNDS):
        if run_r_ready_vs_cancel(rng):
            r3b_ready += 1
        else:
            r3b_cancel += 1
    if r3b_ready == 0 or r3b_cancel == 0:
        red("scenario 3b: both outcomes must be observed across " + String(ROUNDS) + " rounds")
    print("T45 scenario 3b ok (ready_wins=" + String(r3b_ready) + " cancel_wins=" + String(r3b_cancel) + ")")

    var r4_cancel = 0
    var r4_timeout = 0
    for _ in range(ROUNDS):
        if run_r_cancel_vs_timeout(rng):
            r4_cancel += 1
        else:
            r4_timeout += 1
    if r4_cancel == 0 or r4_timeout == 0:
        red("scenario 4: both outcomes must be observed across " + String(ROUNDS) + " rounds")
    print("T45 scenario 4 ok (cancel_wins=" + String(r4_cancel) + " timeout_wins=" + String(r4_timeout) + ")")

    run_storm(rng)

    print("T45 reactor race: PASS")
