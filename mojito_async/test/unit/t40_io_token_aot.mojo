# mojito_async/test/unit/t40_io_token_aot.mojo
#
# A7.2 reactor lane (issue #76) — operation tokens/generations acceptance
# driver.
#
# Part A (pure table mechanics, matches the issue's own prescribed
# verification: "fakes a late completion with a stale generation and
# asserts no wake"): register, unregister, reuse the slot with a bumped
# generation, feed a LATE delivery carrying the stale (pre-reuse) wire
# token straight into `drain_ready`, and assert it is PROVABLY DROPPED —
# no ready token, the reused slot's state untouched.  Then prove the LIVE
# occupant's own wire token still delivers correctly (op_kind resolved
# from the table, not the stale decode).
#
# Part B ("a second driver interleaves cancellation then reuse; a race
# battery shows zero double-enqueue"): a REAL Reactor over real pipe(2)
# fds and REAL parked tasks (task.spawn/claim_running + the two-phase
# park kernel via Reactor.register_and_park), driving `rt.enqueued()` —
# the same "exactly one winner" counter t37_winner_reason.mojo and
# t34c_duplicate_wake_aot.mojo already use — through N iterations of
# {register+park, cancel-wake (independent of the reactor, mirroring a
# real CancellationToken delivery), unregister, reuse the freed slot for
# a FRESH task, deliver real readiness}.  Every iteration must produce
# EXACTLY one enqueue for the cancelled task and EXACTLY one enqueue for
# the reused-slot task — never zero (a lost wakeup) and never two (a
# double-enqueue of the same task).
#
# AOT-only: imports the vendor/mojito_sys_io externs (dylib mjs_poller_*
# symbols) via reactor/poller.mojo, so this runs via the run.sh unit AOT
# loop (modular/modular#6971).  One local libc extern (`pipe`) is
# declared here directly, matching the house "local libc externs run
# AOT-only" convention; writes to the pipe use the builtin
# `FileDescriptor` (declaring a second `write` extern collides with the
# stdlib's own internal `write` binding at LLVM lowering).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import Span, stack_allocation

from mojito_async.reactor.io_op_table import IO_OP_READY, IoOpTable
from mojito_async.reactor.io_token import IoOpKind
from mojito_async.reactor.poller import drain_ready
from mojito_async.reactor.reactor import Reactor, make_reactor
from mojito_async.runtime.join_handle import SuspendReason
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.runtime import Nil, Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import claim_running, spawn
from mojito_async.time.deadline import Duration, from_millis
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoEvent, IoInterest


@extern("pipe")
def c_pipe(fds: UnsafePointer[Int32, MutAnyOrigin]) abi("C") -> Int32:
    ...

def red(what: String) raises -> None:
    print("T40 io token: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[Nil]
comptime N_BATTERY = Int(6)


def main() raises:
    # =======================================================================
    # Part A — stale generation delivery is provably dropped.
    # =======================================================================
    var table = IoOpTable()
    var tok1 = table.allocate(IoOpKind.READ, Int32(5), UInt32(1))
    if tok1.generation != 1:
        red("first allocation of a fresh slot must start at generation 1")
    if not table.release(tok1):
        red("release of a freshly-allocated live token must succeed")
    if table.is_live(tok1):
        red("a released token must not report live")

    var tok2 = table.allocate(IoOpKind.WRITE, Int32(9), UInt32(2))
    if tok2.slot != tok1.slot:
        red("expected the freelist to reuse the just-released slot")
    if tok2.generation != tok1.generation + 1:
        red("slot reuse must bump the generation exactly once")

    # A LATE delivery carrying tok1's stale wire bits must be dropped.
    var stale_events = List[IoEvent]()
    var se = IoEvent()
    se.token = tok1.encode()
    se.fd = 5
    se.events = 1
    stale_events.append(se)
    var stale_span = Span[IoEvent, MutAnyOrigin](stale_events)
    var stale_ready = drain_ready(stale_span, table)
    if len(stale_ready) != 0:
        red("a stale-generation delivery must be dropped, not delivered")
    var reused_entry = table.get(tok2.slot)
    if reused_entry.state == IO_OP_READY:
        red("a stale delivery must not mark the NEW occupant ready")
    if reused_entry.op_kind != IoOpKind.WRITE:
        red("the stale delivery must not have mutated the new occupant's op_kind")

    # The LIVE occupant's own wire token must still deliver correctly.
    var live_events = List[IoEvent]()
    var le = IoEvent()
    le.token = tok2.encode()
    le.fd = 9
    le.events = 2
    live_events.append(le)
    var live_span = Span[IoEvent, MutAnyOrigin](live_events)
    var live_ready = drain_ready(live_span, table)
    if len(live_ready) != 1:
        red("the live occupant's own delivery must be reported")
    if live_ready[0].slot != tok2.slot or live_ready[0].generation != tok2.generation:
        red("delivered token does not match the live occupant")
    if live_ready[0].op_kind != IoOpKind.WRITE:
        red("op_kind must resolve from the table entry, not the wire bits")

    # =======================================================================
    # Part B — cancellation then reuse: a race battery shows zero
    # double-enqueue (real Reactor, real parked tasks).
    # =======================================================================
    var reactor = make_reactor()
    var rt = create()

    for i in range(N_BATTERY):
        # --- task A: park on a fresh pipe, then CANCEL before any write ---
        var fds_a = stack_allocation[2, Int32]()
        if c_pipe(fds_a) != 0:
            red("iteration " + String(i) + ": pipe(2) failed (A)")
        var rfd_a = fds_a[0]

        var tcb_a = TB.create()
        var h_a = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
        claim_running(h_a)
        var handle_a = NativeIoHandle(rfd_a)
        var token_a = reactor.register_and_park[Nil](
            rt, h_a, handle_a, IoInterest.READABLE, IoOpKind.READ
        )
        if h_a.state() != TaskControlBlock.WAITING:
            red("iteration " + String(i) + ": task A must be WAITING after "
                "register_and_park (nothing written yet)")
        var enq_before_cancel = rt.enqueued()
        var gen_a = h_a.tcb()[].generation()
        # A cancellation delivery is INDEPENDENT of the reactor's own
        # readiness path (a CancellationToken observer calls unpark_current
        # directly, exactly like wake_cancelled) — the reactor never saw
        # this op become ready.
        unpark_current[Nil](rt, h_a, required_gen=gen_a, win_reason=SuspendReason.CANCEL)
        if h_a.state() != TaskControlBlock.RUNNABLE:
            red("iteration " + String(i) + ": cancel must move task A to RUNNABLE")
        if rt.enqueued() != enq_before_cancel + 1:
            red("iteration " + String(i) + ": cancel must enqueue EXACTLY once")
        # A duplicate cancel/readiness racing in AFTER the winning claim
        # must be a quiet no-op (H2) — zero double-enqueue.
        unpark_current[Nil](rt, h_a, required_gen=gen_a, win_reason=SuspendReason.READY)
        if rt.enqueued() != enq_before_cancel + 1:
            red("iteration " + String(i)
                + ": a losing duplicate wake must NOT enqueue again")
        reactor.unregister(token_a)
        if reactor.live_count() != 0:
            red("iteration " + String(i) + ": cancelled op must release its slot")

        # --- task B: reuse the just-freed slot for a DIFFERENT op ----------
        var fds_b = stack_allocation[2, Int32]()
        if c_pipe(fds_b) != 0:
            red("iteration " + String(i) + ": pipe(2) failed (B)")
        var rfd_b = fds_b[0]
        var wfd_b = fds_b[1]

        var tcb_b = TB.create()
        var h_b = spawn[Nil](rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
        claim_running(h_b)
        var handle_b = NativeIoHandle(rfd_b)
        var token_b = reactor.register_and_park[Nil](
            rt, h_b, handle_b, IoInterest.READABLE, IoOpKind.READ
        )
        if token_b.slot != token_a.slot:
            red("iteration " + String(i)
                + ": expected the freelist to reuse the cancelled slot")
        if token_b.generation != token_a.generation + 1:
            red("iteration " + String(i) + ": slot reuse must bump the generation")
        if h_b.state() != TaskControlBlock.WAITING:
            red("iteration " + String(i) + ": task B must be WAITING")

        # A real write makes the reused slot ready; the reactor's poll must
        # wake task B EXACTLY once — never task A (already RUNNABLE, long
        # gone) and never twice.
        var enq_before_ready = rt.enqueued()
        var wf_b = FileDescriptor(Int(wfd_b))
        wf_b.write("A")

        var deadline_tries = 0
        while True:
            var ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
            if len(ready) > 0:
                if len(ready) != 1:
                    red("iteration " + String(i)
                        + ": expected exactly one ready op, got "
                        + String(len(ready)))
                if ready[0].slot != token_b.slot or ready[0].generation != token_b.generation:
                    red("iteration " + String(i) + ": readiness delivered the WRONG token")
                break
            deadline_tries += 1
            if deadline_tries > 3:
                red("iteration " + String(i) + ": poll never observed the write")
        if h_b.state() != TaskControlBlock.RUNNABLE:
            red("iteration " + String(i) + ": task B must be RUNNABLE after readiness")
        if rt.enqueued() != enq_before_ready + 1:
            red("iteration " + String(i)
                + ": readiness wake must enqueue EXACTLY once (zero double-enqueue)")
        reactor.unregister(token_b)
        if reactor.live_count() != 0:
            red("iteration " + String(i) + ": ready op must release its slot")

    print("T40 io token: PASS")
