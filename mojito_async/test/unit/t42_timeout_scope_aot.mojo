# mojito_async/test/unit/t42_timeout_scope.mojo
#
# A6.1 (issue #84) — deadline-scoped timeout core acceptance: a child
# completing before the deadline wins with no cancellation applied; at
# expiry every child (including a NESTED subscope's children) is
# cancelled and the parked owner is woken EXACTLY ONCE with a
# TimeoutError; a child failure that already cancelled the scope beats a
# LATER timeout (the driver's idempotency guard, never a double
# mark/wake); `refresh_timeout` re-arms without a stale token ever
# disturbing the live deadline.
#
# Pure Mojo (Scope + TimerHeap + TimeoutScope, no fiber/stack externs),
# JIT-runnable like t13_scope.mojo/t29_cancel_tree_aot.mojo's sibling
# lanes.  Deterministic virtual time throughout (spec §76.5): every
# "deadline" is an ABSOLUTE tick on one shared `TimerHeap`, driven by
# `timeout_scope_driver` at explicit `now` values — no wall-clock wait.
#
# COMPILER QUIRK (found this session, issue #84): nesting a Scope under
# TWO DIFFERENT parent roots in the SAME function — i.e. two independent
# `open_timeout`/`make_nested_scope` trees both alive in one `def` body —
# corrupts the allocator under b2 (reproduced with the PLAIN `scope.mojo`
# API alone, no timeout_scope involved: `make_nested_scope` under root A
# then under a DIFFERENT root B, in the same function, crashes with
# "Attempt to free invalid pointer"; the SAME root reused for both nested
# scopes, or each root's tree confined to its OWN function that returns
# before the next begins, does not).  WORKAROUND: each scenario below is
# its own `def sceneN(mut rt, mut heap, mut registry) raises` — the
# shared `TimerHeap`/`TimeoutRegistry`/`Runtime` cross scenario
# boundaries as `mut` params (this module's own §76.5 virtual-time
# threading convention), but each scenario's OWN root Scope + TimeoutScope
# tree is constructed and consumed within its own function frame, never
# alongside a SIBLING scenario's tree in one frame.
#
# Root/ancestor Scope cells and every TCB whose address is stored (Int)
# in the TimerHeap/TimeoutRegistry and reconstructed inside
# `timeout_scope_driver` (a generic function several calls removed from
# each scenario) are placed in `stack_allocation` cells — matching this
# suite's existing convention (t20/t21 already `stack_allocation` any
# state that must survive a similar round trip) — while the TimeoutScope/
# nested Scope built via an OUT-PARAM factory (`open_timeout`/
# `make_nested_scope`) stays a plain `var` binding (never an indexed
# `buf[i] = factory(...)`, which breaks the factory's in-place placement
# guarantee — see timeout_scope.mojo header).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation
from std.collections import List
from mojito_async.integration.sys import IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle
from mojito_async.scope import Scope, make_scope, make_nested_scope
from mojito_async.time.timeout_scope import (
    TimeoutRegistry,
    is_timeout_error,
    make_timeout_registry,
    open_timeout,
    refresh_timeout,
    timeout_scope_driver,
)
from mojito_async.time.timer_heap import TimerHeap


def red(what: String) raises -> None:
    print("T42 timeout scope: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def running(mut t: TB) raises:
    t.transition(TaskControlBlock.RUNNABLE)
    t.transition(TaskControlBlock.RUNNING)


def waiting(mut t: TB) raises:
    t.transition(TaskControlBlock.RUNNABLE)
    t.transition(TaskControlBlock.RUNNING)
    t.transition(TaskControlBlock.PARKING)
    t.transition(TaskControlBlock.WAITING)


# =============================================================================
# 1. Child runs to completion before the deadline: no cancellation is
#    applied, and the caller retires the timer (sleep's own discipline,
#    t21) — a later expiry pass is a clean no-op.
# =============================================================================
def scene1_normal_completion(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log1 = List[Int]()
    var log1p = UnsafePointer[List[Int], MutAnyOrigin](to=log1)
    var root1_buf = stack_allocation[1, Scope]()
    root1_buf[0] = make_scope(100, log1p, True)
    var root1p = UnsafePointer[Scope, MutAnyOrigin](to=root1_buf[0])

    var owner1_buf = stack_allocation[1, TB]()
    owner1_buf[0] = TB.create()
    waiting(owner1_buf[0])
    var oh1 = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](to=owner1_buf[0]), 9001
    )

    var ts1 = open_timeout[IntResult](
        101, root1p, oh1, UInt64(1000), heap, registry, log1p, True
    )
    if heap.size() != 1:
        red("open_timeout did not arm exactly one deadline")
    if not registry.is_registered(101):
        red("open_timeout did not register the scope-deadline arm")
    if ts1.timer_id() != 101 or ts1.handle() != 101:
        red("TimeoutScope timer_id/handle mismatch")

    var child1_buf = stack_allocation[1, TB]()
    child1_buf[0] = TB.create()
    running(child1_buf[0])
    _ = ts1.scope_ptr()[].register[IntResult](
        UnsafePointer[TB, MutAnyOrigin](to=child1_buf[0]), 102, 0
    )
    child1_buf[0].transition(TaskControlBlock.COMPLETED)
    child1_buf[0].mark_result(IntResult(42))

    # the scope closes cleanly: no live children, no cancellation.
    ts1.scope_ptr()[].close(rt)
    if not heap.cancel_token(ts1.timer_id(), ts1.gen()):
        red("cancel_token failed to retire the completed scope's timer")
    if heap.size() != 0:
        red("cancelled timer still pending")

    var woke1 = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(5000))
    if woke1 != 0:
        red("driver woke something after the timer was retired")
    if owner1_buf[0].is_failed():
        red("owner spuriously marked failed after normal completion")
    if owner1_buf[0].state() != TaskControlBlock.WAITING:
        red("owner state disturbed after normal completion")


# =============================================================================
# 2. At deadline expiry every child registered under the deadline
#    scope — INCLUDING a nested subscope's children — is cancelled;
#    the owner is woken EXACTLY ONCE with a TimeoutError.
# =============================================================================
def scene2_expiry_cancels_nested(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log2 = List[Int]()
    var log2p = UnsafePointer[List[Int], MutAnyOrigin](to=log2)
    var root2_buf = stack_allocation[1, Scope]()
    root2_buf[0] = make_scope(200, log2p, True)
    var root2p = UnsafePointer[Scope, MutAnyOrigin](to=root2_buf[0])

    var owner2_buf = stack_allocation[1, TB]()
    owner2_buf[0] = TB.create()
    waiting(owner2_buf[0])
    var oh2 = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](to=owner2_buf[0]), 9002
    )

    var ts2 = open_timeout[IntResult](
        201, root2p, oh2, UInt64(2000), heap, registry, log2p, True
    )

    var d1_buf = stack_allocation[1, TB]()
    d1_buf[0] = TB.create()
    running(d1_buf[0])
    _ = ts2.scope_ptr()[].register[IntResult](
        UnsafePointer[TB, MutAnyOrigin](to=d1_buf[0]), 202, 0
    )

    var nested2 = make_nested_scope(203, ts2.scope_ptr(), log2p, True)
    var nested2p = UnsafePointer[Scope, MutAnyOrigin](to=nested2)

    var d2_buf = stack_allocation[1, TB]()
    d2_buf[0] = TB.create()
    running(d2_buf[0])
    _ = nested2p[].register[IntResult](
        UnsafePointer[TB, MutAnyOrigin](to=d2_buf[0]), 204, 0
    )

    var woke2 = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(2000))
    if woke2 != 1:
        red("driver did not wake exactly the owner: woke=" + String(woke2))
    if d1_buf[0].state() != TaskControlBlock.CANCELLED:
        red("direct child not cancelled at expiry")
    if d2_buf[0].state() != TaskControlBlock.CANCELLED:
        red("nested child not cancelled at expiry")
    if not ts2.is_timed_out():
        red("scope not marked cancelled at expiry")
    if not nested2p[].is_cancelled():
        red("nested subscope not marked cancelled at expiry")
    if not owner2_buf[0].is_failed():
        red("owner not marked failed at expiry")
    if not is_timeout_error(Error(owner2_buf[0].error())):
        red("owner failure is not a TimeoutError: " + owner2_buf[0].error())
    if owner2_buf[0].state() != TaskControlBlock.RUNNABLE:
        red("owner not woken (RUNNABLE) at expiry")
    if not heap.is_empty():
        red("expired timer left armed")

    # idempotency: driving again at (or past) the same now is a clean no-op
    # (heap already drained — nothing due, nothing re-processed).
    var woke2b = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(2000))
    if woke2b != 0:
        red("second drive at the same tick re-processed the drained heap")


# =============================================================================
# 3. A child failure that ALREADY cancelled the scope beats a LATER
#    timeout: the driver's idempotency guard is a clean no-op — never
#    a double mark, never clobbers the real primary error.
# =============================================================================
def scene3_failure_beats_timeout(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log3 = List[Int]()
    var log3p = UnsafePointer[List[Int], MutAnyOrigin](to=log3)
    var root3_buf = stack_allocation[1, Scope]()
    root3_buf[0] = make_scope(300, log3p, True)
    var root3p = UnsafePointer[Scope, MutAnyOrigin](to=root3_buf[0])

    var owner3_buf = stack_allocation[1, TB]()
    owner3_buf[0] = TB.create()
    waiting(owner3_buf[0])
    var oh3 = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](to=owner3_buf[0]), 9003
    )

    var ts3 = open_timeout[IntResult](
        301, root3p, oh3, UInt64(3000), heap, registry, log3p, True
    )

    # a child failure elsewhere already recorded the PRIMARY error and
    # drove the cancel (the with_scope/record_failure path this module
    # composes with, not reproduced here — simulated directly per the
    # module's own documented precedence: "child failure beats a later
    # timeout" is `Scope.is_cancelled()` already true).
    ts3.scope_ptr()[].request_cancel_all()
    owner3_buf[0].mark_failed("boom: a real child failure, not a timeout")

    var woke3 = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(3000))
    if woke3 != 0:
        red("driver woke the owner despite an already-cancelled scope")
    if owner3_buf[0].error() != "boom: a real child failure, not a timeout":
        red("driver clobbered the real primary error: " + owner3_buf[0].error())
    if is_timeout_error(Error(owner3_buf[0].error())):
        red("owner's real failure was overwritten with a TimeoutError")
    if owner3_buf[0].state() != TaskControlBlock.WAITING:
        red("driver disturbed an owner it should have left alone")
    if not heap.is_empty():
        red("the already-resolved arm was not drained from the heap")


# =============================================================================
# 4. refresh_timeout re-arms; a stale token can never disturb the live
#    deadline, and the old deadline never fires after a refresh.
# =============================================================================
def scene4_refresh_timeout(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log4 = List[Int]()
    var log4p = UnsafePointer[List[Int], MutAnyOrigin](to=log4)
    var root4_buf = stack_allocation[1, Scope]()
    root4_buf[0] = make_scope(400, log4p, True)
    var root4p = UnsafePointer[Scope, MutAnyOrigin](to=root4_buf[0])

    var owner4_buf = stack_allocation[1, TB]()
    owner4_buf[0] = TB.create()
    waiting(owner4_buf[0])
    var oh4 = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](to=owner4_buf[0]), 9004
    )

    var ts4 = open_timeout[IntResult](
        401, root4p, oh4, UInt64(4000), heap, registry, log4p, True
    )
    var old_gen4 = ts4.gen()

    refresh_timeout(ts4, heap, registry, UInt64(6000))
    if ts4.gen() == old_gen4:
        red("refresh_timeout did not grant a fresh generation")
    if heap.live_gen(ts4.timer_id()) != ts4.gen():
        red("heap's live generation not updated by refresh_timeout")

    # a stale cancel with the OLD token must not touch the live (refreshed)
    # arm — it either no-ops (already removed) or, if it somehow matched,
    # would be a correctness bug; assert the live gen survives untouched.
    _ = heap.cancel_token(ts4.timer_id(), old_gen4)
    if heap.live_gen(ts4.timer_id()) != ts4.gen():
        red("a stale cancel_token disturbed the live refreshed deadline")

    # driving at the OLD deadline (4000) never fires the refreshed timer.
    var woke4a = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(4000))
    if woke4a != 0:
        red("the OLD deadline fired after a refresh")
    if owner4_buf[0].is_failed():
        red("owner marked failed by a stale (pre-refresh) deadline")

    # driving at the NEW deadline (6000) fires exactly once.
    var woke4b = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(6000))
    if woke4b != 1:
        red("the refreshed deadline did not fire: woke=" + String(woke4b))
    if not owner4_buf[0].is_failed():
        red("owner not marked failed by the refreshed deadline")
    if not is_timeout_error(Error(owner4_buf[0].error())):
        red("refreshed-deadline failure is not a TimeoutError")
    if not ts4.is_timed_out():
        red("scope not cancelled by the refreshed deadline")


def main() raises:
    var rt = create()
    var heap = TimerHeap()
    var registry = make_timeout_registry()

    scene1_normal_completion(rt, heap, registry)
    scene2_expiry_cancels_nested(rt, heap, registry)
    scene3_failure_beats_timeout(rt, heap, registry)
    scene4_refresh_timeout(rt, heap, registry)

    print("T42 timeout scope: PASS")
