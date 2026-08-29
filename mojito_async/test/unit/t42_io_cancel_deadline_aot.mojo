# mojito_async/test/unit/t42_io_cancel_deadline_aot.mojo
#
# A7.6 (issue #80) — I/O cancellation + deadline integration acceptance
# driver, exercised directly against reactor/cancel.mojo + the real
# Reactor/TimerHeap (no TcpStream dependency — #79's net/ layer lands
# separately and reuses these exact primitives).
#
# Acceptance (issue #80):
#   A. cancel_op unregisters the op-table slot and wakes the parked owner
#      with a decoded CancellationError (reusing runtime/park.mojo's
#      is_cancel_wake/raise_if_cancel_wake, since CancelRequest.CANCELLED
#      == SuspendReason.CANCEL) — and a later readiness for the same fd is
#      not delivered (no leaked wake after the op is gone).
#   B. service_io_deadlines fires the TIMEOUT path (is_timeout_wake /
#      raise_if_timeout_wake) when the deadline is the sole due signal.
#   C. readiness beats an armed-but-not-yet-due deadline; cancelling the
#      still-armed heap entry afterward is clean (heap.cancel True).
#   D. C6 exactly-one-winner, TIMER-then-READY ordering: a deadline fires
#      first (claims the wake), a LATER reactor.poll() that also observes
#      readiness for the same (still-registered) token is a provable
#      no-op — no double enqueue (rt.pending() unchanged), no stamp
#      overwrite (is_timeout_wake stays True).
#   E. C6 exactly-one-winner, READY-then-TIMER ordering (the reverse):
#      readiness fires first; a LATER service_io_deadlines() over the same
#      due heap entry is a provable no-op (woke == 0, no re-stamp).
#   F. cancel_and_close unregisters + wakes with the CLOSED reason
#      (is_closed_wake/raise_if_closed_wake); the disposal-hook flavor
#      never leaks a wake either.
#
# AOT-only: imports the vendor/mojito_sys_io externs transitively through
# reactor/reactor.mojo, so this runs via the run.sh unit AOT loop
# (`*_aot.mojo`, modular/modular#6971 — the b2 JIT cannot resolve dylib
# symbols through an imported module).  One local libc extern (`pipe`) is
# declared here directly, matching t39_reactor_aot.mojo's precedent;
# writes use the builtin `FileDescriptor` (a second `write` extern
# collides with the stdlib's own internal binding at LLVM lowering).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation

from mojito_async.cancellation import is_cancellation
from mojito_async.integration.sys import IntResult
from mojito_async.reactor.cancel import (
    cancel_and_close,
    cancel_op,
    is_closed_wake,
    is_timeout_wake,
    raise_if_closed_wake,
    raise_if_timeout_wake,
    service_io_deadlines,
)
from mojito_async.reactor.io_token import IoOpKind
from mojito_async.reactor.reactor import Reactor, make_reactor
from mojito_async.runtime.park import is_cancel_wake, raise_if_cancel_wake
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.time.deadline import Duration, from_millis
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoInterest


@extern("pipe")
def c_pipe(fds: UnsafePointer[Int32, MutAnyOrigin]) abi("C") -> Int32:
    ...


def red(what: String) raises -> None:
    print("T42 io cancel/deadline: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


# ---------------------------------------------------------------------------
# Scenario A — cancel_op: unregister + CANCELLED wake; no leaked wake after.
# ---------------------------------------------------------------------------


def scenario_cancel(mut rt: Runtime, mut reactor: Reactor) raises:
    var fds_a = stack_allocation[2, Int32]()
    if c_pipe(fds_a) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds_a[0]
    var wfd = fds_a[1]
    var tcb_a = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    claim_running(h)
    var token = reactor.register_and_park(
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if h.state() != TaskControlBlock.WAITING:
        red("A: register_and_park did not park on an empty pipe")

    var won = cancel_op(reactor, rt, token, Int(h.tcb()), h.id())
    if not won:
        red("A: cancel_op must win against a still-WAITING waiter")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("A: cancel_op did not deliver the wake")
    claim_running(h)
    if not is_cancel_wake(h):
        red("A: is_cancel_wake did not observe the CANCELLED stamp")
    var raised = False
    try:
        raise_if_cancel_wake(h)
    except e:
        raised = True
        if not is_cancellation(e):
            red("A: raise_if_cancel_wake used the wrong error naming")
    if not raised:
        red("A: raise_if_cancel_wake did not raise after a cancel-won wake")
    if reactor.live_count() != 0:
        red("A: cancel_op did not release the op-table slot")

    # No leaked wake: a repeat cancel_op against the same (now-gone)
    # waiter is a no-op (state is no longer WAITING).
    var pending_before = rt.pending()
    var won_again = cancel_op(reactor, rt, token, Int(h.tcb()), h.id())
    if won_again:
        red("A: cancel_op must not win twice against the same waiter")
    if rt.pending() != pending_before:
        red("A: a losing repeat cancel double-enqueued")

    # Writing to the pipe now must not resurrect a delivery — nothing is
    # registered on this fd anymore.
    var wf = FileDescriptor(Int(wfd))
    wf.write("x")
    var ready = reactor.poll(rt, Optional[Duration](from_millis(50)))
    if len(ready) != 0:
        red("A: a stale delivery arrived after cancel_op unregistered the op")

    print("T42 scenario A (cancel_op): PASS")


# ---------------------------------------------------------------------------
# Scenario B — service_io_deadlines: the sole-signal TIMEOUT path.
# ---------------------------------------------------------------------------


def scenario_timeout(mut rt: Runtime, mut reactor: Reactor) raises:
    var fds_b = stack_allocation[2, Int32]()
    if c_pipe(fds_b) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds_b[0]
    var tcb_b = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    claim_running(h)
    var token = reactor.register_and_park(
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if h.state() != TaskControlBlock.WAITING:
        red("B: register_and_park did not park on an empty pipe")

    var heap = TimerHeap()
    _ = heap.arm(h.id(), Int(h.tcb()), UInt64(0))  # already due at now=0
    var woke = service_io_deadlines[IntResult](rt, heap, UInt64(1))
    if woke != 1:
        red("B: service_io_deadlines did not wake the due waiter")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("B: service_io_deadlines did not deliver the wake")
    claim_running(h)
    if not is_timeout_wake(h):
        red("B: is_timeout_wake did not observe the TIMEOUT stamp")
    var raised = False
    try:
        raise_if_timeout_wake(h)
    except e:
        raised = True
        if "TimeoutError" not in String(e):
            red("B: raise_if_timeout_wake used the wrong error naming: " + String(e))
    if not raised:
        red("B: raise_if_timeout_wake did not raise after a timeout-won wake")
    if is_timeout_wake(h):
        red("B: raise_if_timeout_wake did not clear the stamp (re-stamp-safety)")

    # The redrive's job (not the timer service's): release the still-live
    # op-table slot for the abandoned attempt.
    reactor.unregister(token)
    if reactor.live_count() != 0:
        red("B: unregister after a timeout did not release the slot")

    print("T42 scenario B (service_io_deadlines): PASS")


# ---------------------------------------------------------------------------
# Scenario C — readiness beats an armed-but-not-due deadline.
# ---------------------------------------------------------------------------


def scenario_readiness_beats_far_deadline(mut rt: Runtime, mut reactor: Reactor) raises:
    var fds_c = stack_allocation[2, Int32]()
    if c_pipe(fds_c) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds_c[0]
    var wfd = fds_c[1]
    var tcb_c = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_c), 0)
    claim_running(h)
    var token = reactor.register_and_park(
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if h.state() != TaskControlBlock.WAITING:
        red("C: register_and_park did not park on an empty pipe")

    var heap = TimerHeap()
    var gen = heap.arm(h.id(), Int(h.tcb()), UInt64(10_000_000_000))  # far future

    var wf = FileDescriptor(Int(wfd))
    wf.write("y")
    var ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if len(ready) != 1:
        red("C: expected exactly one ready op after the write")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("C: readiness did not deliver the wake")
    claim_running(h)
    if is_timeout_wake(h):
        red("C: readiness-won resume must not observe a TIMEOUT stamp")
    if is_cancel_wake(h):
        red("C: readiness-won resume must not observe a CANCELLED stamp")

    if not heap.cancel_token(h.id(), gen):
        red("C: cancelling the now-irrelevant far-future deadline failed")
    if heap.has_due(UInt64(20_000_000_000)):
        red("C: a cancelled deadline must not remain due")

    reactor.unregister(token)
    print("T42 scenario C (readiness beats far deadline): PASS")


# ---------------------------------------------------------------------------
# Scenario D — C6 exactly-one-winner, TIMER-then-READY ordering.
# ---------------------------------------------------------------------------


def scenario_timer_then_ready(mut rt: Runtime, mut reactor: Reactor) raises:
    var fds_d = stack_allocation[2, Int32]()
    if c_pipe(fds_d) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds_d[0]
    var wfd = fds_d[1]
    var tcb_d = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_d), 0)
    claim_running(h)
    var token = reactor.register_and_park(
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if h.state() != TaskControlBlock.WAITING:
        red("D: register_and_park did not park on an empty pipe")

    var heap = TimerHeap()
    _ = heap.arm(h.id(), Int(h.tcb()), UInt64(0))  # already due

    # data is ALSO available before either service call runs.
    var wf = FileDescriptor(Int(wfd))
    wf.write("z")

    var woke = service_io_deadlines[IntResult](rt, heap, UInt64(1))
    if woke != 1:
        red("D: the timer must win when it services first")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("D: the timer's wake did not land")
    if not is_timeout_wake(h):
        red("D: the timer winner was not stamped TIMEOUT")

    var pending_before = rt.pending()
    var ready = reactor.poll(rt, Optional[Duration](from_millis(50)))
    if len(ready) != 1:
        red("D: the READY event itself must still be reported by poll (it "
            "is the op-table slot's job to skip the wake, not poll's job "
            "to hide the event) — got " + String(len(ready)))
    if h.state() != TaskControlBlock.RUNNABLE:
        red("D: a losing readiness delivery corrupted the timer winner's state")
    if rt.pending() != pending_before:
        red("D: a losing readiness delivery double-enqueued")
    if not is_timeout_wake(h):
        red("D: a losing readiness delivery overwrote the TIMEOUT stamp")

    reactor.unregister(token)
    print("T42 scenario D (C6 timer-then-ready): PASS")


# ---------------------------------------------------------------------------
# Scenario E — C6 exactly-one-winner, READY-then-TIMER ordering (reverse).
# ---------------------------------------------------------------------------


def scenario_ready_then_timer(mut rt: Runtime, mut reactor: Reactor) raises:
    var fds_e = stack_allocation[2, Int32]()
    if c_pipe(fds_e) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds_e[0]
    var wfd = fds_e[1]
    var tcb_e = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_e), 0)
    claim_running(h)
    var token = reactor.register_and_park(
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if h.state() != TaskControlBlock.WAITING:
        red("E: register_and_park did not park on an empty pipe")

    var heap = TimerHeap()
    _ = heap.arm(h.id(), Int(h.tcb()), UInt64(0))  # already due

    var wf = FileDescriptor(Int(wfd))
    wf.write("w")
    var ready = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if len(ready) != 1:
        red("E: expected exactly one ready op after the write")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("E: readiness did not deliver the wake")
    if is_timeout_wake(h):
        red("E: readiness-won resume must not observe a TIMEOUT stamp")

    var pending_before = rt.pending()
    var woke = service_io_deadlines[IntResult](rt, heap, UInt64(1))
    if woke != 0:
        red("E: a losing timer service call must not count a wake")
    if is_timeout_wake(h):
        red("E: a losing timer service call must not stamp TIMEOUT")
    if rt.pending() != pending_before:
        red("E: a losing timer service call double-enqueued")

    reactor.unregister(token)
    print("T42 scenario E (C6 ready-then-timer): PASS")


# ---------------------------------------------------------------------------
# Scenario F — cancel_and_close: CLOSED wake, never leaks.
# ---------------------------------------------------------------------------


def scenario_cancel_and_close(mut rt: Runtime, mut reactor: Reactor) raises:
    var fds_f = stack_allocation[2, Int32]()
    if c_pipe(fds_f) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds_f[0]
    var wfd = fds_f[1]
    var tcb_f = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_f), 0)
    claim_running(h)
    var token = reactor.register_and_park(
        rt, h, NativeIoHandle(rfd), IoInterest.READABLE, IoOpKind.READ
    )
    if h.state() != TaskControlBlock.WAITING:
        red("F: register_and_park did not park on an empty pipe")

    var won = cancel_and_close(reactor, rt, token, Int(h.tcb()), h.id())
    if not won:
        red("F: cancel_and_close must win against a still-WAITING waiter")
    if h.state() != TaskControlBlock.RUNNABLE:
        red("F: cancel_and_close did not deliver the wake")
    claim_running(h)
    if not is_closed_wake(h):
        red("F: is_closed_wake did not observe the CLOSED stamp")
    var raised = False
    try:
        raise_if_closed_wake(h)
    except e:
        raised = True
        if "ClosedError" not in String(e):
            red("F: raise_if_closed_wake used the wrong error naming: " + String(e))
    if not raised:
        red("F: raise_if_closed_wake did not raise after a close-won wake")
    if reactor.live_count() != 0:
        red("F: cancel_and_close did not release the op-table slot")

    var pending_before = rt.pending()
    var won_again = cancel_and_close(reactor, rt, token, Int(h.tcb()), h.id())
    if won_again:
        red("F: cancel_and_close must not win twice against the same waiter")
    if rt.pending() != pending_before:
        red("F: a losing repeat cancel_and_close double-enqueued")

    var wf = FileDescriptor(Int(wfd))
    wf.write("v")
    var ready = reactor.poll(rt, Optional[Duration](from_millis(50)))
    if len(ready) != 0:
        red("F: a stale delivery arrived after cancel_and_close unregistered the op")

    print("T42 scenario F (cancel_and_close): PASS")


def main() raises:
    var rt = create()
    var reactor = make_reactor()
    scenario_cancel(rt, reactor)
    scenario_timeout(rt, reactor)
    scenario_readiness_beats_far_deadline(rt, reactor)
    scenario_timer_then_ready(rt, reactor)
    scenario_ready_then_timer(rt, reactor)
    scenario_cancel_and_close(rt, reactor)
    print("T42 io cancel/deadline: PASS")
