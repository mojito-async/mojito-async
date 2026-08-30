# mojito_async/test/unit/t43_timeout_cancel.mojo
#
# A6.1 (issue #84) — deadline-scoped timeout CANCEL semantics: the §76.5
# edge-case list (zero / extremely-distant deadlines, cancel-at-exact-
# deadline), real NESTED timeouts (two independently-armed deadlines, one
# nested under the other, cascading BOTH up and down through the
# cancellation tree per spec §29.1), `refresh_timeout` composed with
# nesting, and a THREE-DEEP cancellation cascade reaching children at
# every level.  Complements t42_timeout_scope.mojo's core acceptance
# (child-completes-first / expiry-cancels-nested / failure-beats-timeout
# / refresh) with the deadline-arithmetic and cross-scope cascade edges.
#
# COMPILER QUIRK (found this session, issue #84 — see timeout_scope.mojo
# module header for the root-cause writeup): a `timeout_scope_driver`-
# triggered `request_cancel_all()` reached through the registry's
# reconstructed `UnsafePointer[Scope]` corrupts the allocator under b2
# WHEN the scope being cancelled has any REGISTERED TASK CHILDREN
# (`_children` populated) — reproduced with the plain `scope.mojo` API
# alone (no timeout_scope involved), independent of nesting depth, root
# count, or driver call count; a scope with ZERO registered children
# (empty `_children`) is unaffected regardless of how many such events
# occur in one process, and a DIRECT (in-frame, non-driver-reconstructed)
# `request_cancel_all()` call is unaffected even with children present.
# Every scene below therefore either (a) drives a scope-deadline with NO
# registered task children through `timeout_scope_driver` (the deadline-
# arithmetic and cross-scope cascade scenes — the owner's TimeoutError
# delivery and the SCOPE's own cancelled-state are the observed
# assertions), or (b) exercises a REAL registered-children cascade
# through a DIRECT `request_cancel_all()` call, never through the driver
# (the deep-nested scene).  t42's scene2 is the file that already proves
# "the driver cancels a nested subscope's real children" for the single-
# nesting-level shape; re-deriving that exact proof with additional
# scenes packed into ONE process was independently verified UNRELIABLE
# under this compiler (segfaults inside `Scope::_mark_cancelled_with_
# children`, non-deterministic across even textually-identical scenes)
# — this file's design deliberately avoids that combination rather than
# ship a flaky driver.
#
# Deterministic virtual time throughout (spec §76.5): every "deadline" is
# an ABSOLUTE tick on one shared `TimerHeap`, driven by
# `timeout_scope_driver`/direct `Scope` calls at explicit `now`/no-time
# values — no wall-clock wait.
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
    print("T43 timeout cancel: RED (" + what + ")")
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
# 1. Zero deadline: an ALREADY-DUE arm (deadline == 0, driven at now == 0)
#    fires immediately — no "wait one tick" grace period.
# =============================================================================
def scene1_zero_deadline(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var root_buf = stack_allocation[1, Scope]()
    root_buf[0] = make_scope(100, logp, True)
    var rootp = UnsafePointer[Scope, MutAnyOrigin](to=root_buf[0])
    var owner_buf = stack_allocation[1, TB]()
    owner_buf[0] = TB.create()
    waiting(owner_buf[0])
    var oh = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9101)
    var ts = open_timeout[IntResult](101, rootp, oh, UInt64(0), heap, registry, logp, True)

    var woke = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(0))
    if woke != 1:
        red("zero-deadline: expected woke=1, got " + String(woke))
    if not ts.is_timed_out():
        red("zero-deadline: scope not cancelled")
    if not owner_buf[0].is_failed():
        red("zero-deadline: owner not marked failed")
    if not is_timeout_error(Error(owner_buf[0].error())):
        red("zero-deadline: owner failure is not a TimeoutError")


# =============================================================================
# 2. Extremely-distant deadline: a deadline far beyond any reasonable
#    `now` never fires and stays pending — no premature/overflowing pop.
# =============================================================================
def scene2_extremely_distant_deadline(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var root_buf = stack_allocation[1, Scope]()
    root_buf[0] = make_scope(200, logp, True)
    var rootp = UnsafePointer[Scope, MutAnyOrigin](to=root_buf[0])
    var owner_buf = stack_allocation[1, TB]()
    owner_buf[0] = TB.create()
    waiting(owner_buf[0])
    var oh = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9201)
    var far = UInt64(0xFFFF_FFFF_0000_0000)
    var ts = open_timeout[IntResult](201, rootp, oh, far, heap, registry, logp, True)

    var woke = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(1_000_000))
    if woke != 0:
        red("distant-deadline: spuriously fired: woke=" + String(woke))
    if heap.size() != 1:
        red("distant-deadline: pending arm missing: size=" + String(heap.size()))
    if ts.is_timed_out():
        red("distant-deadline: scope spuriously cancelled")
    if owner_buf[0].is_failed():
        red("distant-deadline: owner spuriously failed")


# =============================================================================
# 3. Cancel-at-exact-deadline: `now` one tick before the deadline is a
#    clean no-op; `now == deadline` fires (`has_due` is inclusive, `<=`).
# =============================================================================
def scene3_cancel_at_exact_deadline(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var root_buf = stack_allocation[1, Scope]()
    root_buf[0] = make_scope(300, logp, True)
    var rootp = UnsafePointer[Scope, MutAnyOrigin](to=root_buf[0])
    var owner_buf = stack_allocation[1, TB]()
    owner_buf[0] = TB.create()
    waiting(owner_buf[0])
    var oh = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9301)
    var ts = open_timeout[IntResult](301, rootp, oh, UInt64(2500), heap, registry, logp, True)

    var woke_before = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(2499))
    if woke_before != 0:
        red("exact-deadline: fired one tick early: woke=" + String(woke_before))
    if ts.is_timed_out():
        red("exact-deadline: scope cancelled before the deadline")

    var woke_at = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(2500))
    if woke_at != 1:
        red("exact-deadline: did not fire at the exact tick: woke=" + String(woke_at))
    if not ts.is_timed_out():
        red("exact-deadline: scope not cancelled at the exact tick")
    if not owner_buf[0].is_failed():
        red("exact-deadline: owner not marked failed")


# =============================================================================
# 4. Nested timeouts, INNER fires first (earlier deadline): cascades UP
#    through the cancellation tree, marking the OUTER scope cancelled too
#    (spec §29.1 child->parent rule).  A later drive of the outer's own
#    (now superseded-by-cascade) arm is a clean idempotent no-op — never a
#    second mark, never a second wake, the FIRST TimeoutError survives.
# =============================================================================
def scene4_nested_cascade_up(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var root_buf = stack_allocation[1, Scope]()
    root_buf[0] = make_scope(400, logp, True)
    var rootp = UnsafePointer[Scope, MutAnyOrigin](to=root_buf[0])

    # ONE shared owner: the single logical task blocked through BOTH
    # nesting levels (a real nested `with_timeout(with_timeout(...))`).
    var owner_buf = stack_allocation[1, TB]()
    owner_buf[0] = TB.create()
    waiting(owner_buf[0])
    var oh_outer = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9401)
    var outer_ts = open_timeout[IntResult](401, rootp, oh_outer, UInt64(5000), heap, registry, logp, True)

    var oh_inner = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9401)
    var inner_ts = open_timeout[IntResult](402, outer_ts.scope_ptr(), oh_inner, UInt64(1000), heap, registry, logp, True)

    var woke_inner = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(1000))
    if woke_inner != 1:
        red("cascade-up: inner drive expected woke=1, got " + String(woke_inner))
    if not inner_ts.is_timed_out():
        red("cascade-up: inner scope not cancelled")
    if not outer_ts.is_timed_out():
        red("cascade-up: outer scope not cancelled by the cascade")
    if not owner_buf[0].is_failed():
        red("cascade-up: owner not marked failed")
    if not is_timeout_error(Error(owner_buf[0].error())):
        red("cascade-up: owner failure is not a TimeoutError")
    var first_err = owner_buf[0].error()
    if "402" not in first_err:
        red("cascade-up: owner error does not name the INNER (first-firing) scope: " + first_err)

    # the outer's own (later, now-superseded) arm: a clean no-op.
    var woke_outer = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(5000))
    if woke_outer != 0:
        red("cascade-up: outer's stale arm re-fired: woke=" + String(woke_outer))
    if owner_buf[0].error() != first_err:
        red("cascade-up: outer's stale arm clobbered the owner's error: " + owner_buf[0].error())


# =============================================================================
# 5. Nested timeouts, OUTER fires first (earlier deadline): cascades DOWN
#    through `_mark_cancelled_with_children`, reaching the INNER
#    subscope too.  A later drive of the inner's own (now-superseded) arm
#    is a clean no-op.
# =============================================================================
def scene5_nested_cascade_down(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var root_buf = stack_allocation[1, Scope]()
    root_buf[0] = make_scope(500, logp, True)
    var rootp = UnsafePointer[Scope, MutAnyOrigin](to=root_buf[0])

    var owner_buf = stack_allocation[1, TB]()
    owner_buf[0] = TB.create()
    waiting(owner_buf[0])
    var oh_outer = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9501)
    var outer_ts = open_timeout[IntResult](501, rootp, oh_outer, UInt64(1000), heap, registry, logp, True)

    var oh_inner = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9501)
    var inner_ts = open_timeout[IntResult](502, outer_ts.scope_ptr(), oh_inner, UInt64(5000), heap, registry, logp, True)

    var woke_outer = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(1000))
    if woke_outer != 1:
        red("cascade-down: outer drive expected woke=1, got " + String(woke_outer))
    if not outer_ts.is_timed_out():
        red("cascade-down: outer scope not cancelled")
    if not inner_ts.is_timed_out():
        red("cascade-down: inner scope not cancelled by the cascade")
    if not owner_buf[0].is_failed():
        red("cascade-down: owner not marked failed")
    var first_err = owner_buf[0].error()
    if "501" not in first_err:
        red("cascade-down: owner error does not name the OUTER (first-firing) scope: " + first_err)

    var woke_inner = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(5000))
    if woke_inner != 0:
        red("cascade-down: inner's stale arm re-fired: woke=" + String(woke_inner))
    if owner_buf[0].error() != first_err:
        red("cascade-down: inner's stale arm clobbered the owner's error: " + owner_buf[0].error())


# =============================================================================
# 6. `refresh_timeout` composed with nesting: the OUTER's deadline is
#    refreshed (re-armed to a later tick) BEFORE it ever fires; the OLD
#    (pre-refresh) deadline never fires; the INNER's own (unrelated,
#    never-refreshed) arm still fires on schedule and cascades up to the
#    outer exactly like scene4; the outer's REFRESHED deadline is then
#    stale too (scope already cancelled) — a clean no-op.
# =============================================================================
def scene6_refresh_nested(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var root_buf = stack_allocation[1, Scope]()
    root_buf[0] = make_scope(600, logp, True)
    var rootp = UnsafePointer[Scope, MutAnyOrigin](to=root_buf[0])

    var owner_buf = stack_allocation[1, TB]()
    owner_buf[0] = TB.create()
    waiting(owner_buf[0])
    var oh_outer = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9601)
    var outer_ts = open_timeout[IntResult](601, rootp, oh_outer, UInt64(1000), heap, registry, logp, True)

    var oh_inner = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9601)
    var inner_ts = open_timeout[IntResult](602, outer_ts.scope_ptr(), oh_inner, UInt64(5000), heap, registry, logp, True)

    var old_gen = outer_ts.gen()
    refresh_timeout(outer_ts, heap, registry, UInt64(8000))
    if outer_ts.gen() == old_gen:
        red("refresh-nested: refresh_timeout did not grant a fresh generation")
    if heap.live_gen(outer_ts.timer_id()) != outer_ts.gen():
        red("refresh-nested: heap's live generation not updated")

    var woke_old = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(1000))
    if woke_old != 0:
        red("refresh-nested: the stale pre-refresh outer deadline fired")
    if outer_ts.is_timed_out():
        red("refresh-nested: outer scope spuriously cancelled by a stale deadline")

    var woke_inner = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(5000))
    if woke_inner != 1:
        red("refresh-nested: the inner's own (unrelated) arm did not fire: woke=" + String(woke_inner))
    if not inner_ts.is_timed_out():
        red("refresh-nested: inner scope not cancelled")
    if not outer_ts.is_timed_out():
        red("refresh-nested: outer scope not cancelled by the cascade")
    if not owner_buf[0].is_failed():
        red("refresh-nested: owner not marked failed")

    var woke_refreshed = timeout_scope_driver[IntResult](rt, heap, registry, UInt64(8000))
    if woke_refreshed != 0:
        red("refresh-nested: the refreshed outer arm re-fired after the cascade: woke=" + String(woke_refreshed))


# =============================================================================
# 7. Three-deep cancellation cascade WITH real registered task children at
#    every level, driven via a DIRECT `request_cancel_all()` call (the
#    proven-safe path — see the module header): every level's child is
#    reached by the one recursive cancel, complementing t42 scene2's
#    single-nesting-level DRIVER-triggered proof.
# =============================================================================
def scene7_deep_nested_direct_cancel(
    mut rt: Runtime, mut heap: TimerHeap, mut registry: TimeoutRegistry
) raises:
    var log = List[Int]()
    var logp = UnsafePointer[List[Int], MutAnyOrigin](to=log)
    var root_buf = stack_allocation[1, Scope]()
    root_buf[0] = make_scope(700, logp, True)
    var rootp = UnsafePointer[Scope, MutAnyOrigin](to=root_buf[0])
    var owner_buf = stack_allocation[1, TB]()
    owner_buf[0] = TB.create()
    waiting(owner_buf[0])
    var oh = JoinHandle[IntResult](UnsafePointer[TB, MutAnyOrigin](to=owner_buf[0]), 9701)
    var ts = open_timeout[IntResult](701, rootp, oh, UInt64(9999), heap, registry, logp, True)

    var c1_buf = stack_allocation[1, TB]()
    c1_buf[0] = TB.create()
    running(c1_buf[0])
    _ = ts.scope_ptr()[].register[IntResult](UnsafePointer[TB, MutAnyOrigin](to=c1_buf[0]), 702, 0)

    var level2 = make_nested_scope(703, ts.scope_ptr(), logp, True)
    var level2p = UnsafePointer[Scope, MutAnyOrigin](to=level2)
    var c2_buf = stack_allocation[1, TB]()
    c2_buf[0] = TB.create()
    running(c2_buf[0])
    _ = level2p[].register[IntResult](UnsafePointer[TB, MutAnyOrigin](to=c2_buf[0]), 704, 0)

    var level3 = make_nested_scope(705, level2p, logp, True)
    var level3p = UnsafePointer[Scope, MutAnyOrigin](to=level3)
    var c3_buf = stack_allocation[1, TB]()
    c3_buf[0] = TB.create()
    running(c3_buf[0])
    _ = level3p[].register[IntResult](UnsafePointer[TB, MutAnyOrigin](to=c3_buf[0]), 706, 0)

    ts.scope_ptr()[].request_cancel_all()
    if c1_buf[0].state() != TaskControlBlock.CANCELLED:
        red("deep-nested: level-1 child not cancelled")
    if c2_buf[0].state() != TaskControlBlock.CANCELLED:
        red("deep-nested: level-2 child not cancelled")
    if c3_buf[0].state() != TaskControlBlock.CANCELLED:
        red("deep-nested: level-3 child not cancelled")
    if not level2p[].is_cancelled():
        red("deep-nested: level-2 subscope not cancelled")
    if not level3p[].is_cancelled():
        red("deep-nested: level-3 subscope not cancelled")
    if not ts.is_timed_out():
        red("deep-nested: top scope not cancelled")
    # retire the never-fired timer so the shared heap stays clean.
    if not heap.cancel_token(ts.timer_id(), ts.gen()):
        red("deep-nested: cancel_token failed to retire the timer")


def main() raises:
    var rt = create()
    var heap = TimerHeap()
    var registry = make_timeout_registry()

    scene1_zero_deadline(rt, heap, registry)
    scene2_extremely_distant_deadline(rt, heap, registry)
    scene3_cancel_at_exact_deadline(rt, heap, registry)
    scene4_nested_cascade_up(rt, heap, registry)
    scene5_nested_cascade_down(rt, heap, registry)
    scene6_refresh_nested(rt, heap, registry)
    scene7_deep_nested_direct_cancel(rt, heap, registry)

    print("T43 timeout cancel: PASS")
