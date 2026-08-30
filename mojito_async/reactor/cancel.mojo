# mojito_async/reactor/cancel.mojo
#
# A7.6 (issue #80) — I/O cancellation + deadline integration.
#
# Composes primitives already merged/live in this tree (runtime/park.mojo's
# C6 exactly-one-winner kernel, reactor/reactor.mojo's Reactor, time/
# timer_heap.mojo's TimerHeap) into the three seams the network lanes
# (connect/accept #77/#78, read/write #79) need to make a pending reactor
# op cancellable and deadline-aware:
#
#   cancel_op(reactor, rt, token, waiter_tcb, waiter_task_id) -> Bool
#       unregister the op-table slot + wake the parked owner with the
#       CANCELLED reason (spec §17/§41.18).
#   cancel_and_close(reactor, rt, token, waiter_tcb, waiter_task_id) -> Bool
#       the disposal-hook flavor (issue #80 deliverable 3): identical to
#       cancel_op but stamps CLOSED instead of CANCELLED, so a TcpStream/
#       TcpListener drop can distinguish "the user cancelled the wait" from
#       "the descriptor itself is gone" on the waiter's next re-entry.  The
#       actual socket close() is the net/ caller's job (this module knows
#       nothing about sockets — reactor/ never depends on net/, only the
#       reverse); this composes the REACTOR half of that disposal hook.
#   service_io_deadlines[R](rt, heap, now) -> Int
#       the timer-heap servicing hook for I/O DEADLINES specifically (issue
#       #80 point 2).  Deliberately a SEPARATE small hook from time/
#       timer_service.service_timers rather than a parameter threaded
#       through it: a plain sleep() timeout leaves WaitNode._reason
#       UNCHANGED (service_timers' documented contract, so `sleep_current`
#       callers see no behavior change), but an I/O deadline MUST be
#       distinguishable from an ordinary readiness wake on the waiter's
#       next re-entry — this hook stamps SuspendReason.TIMER as the win
#       reason via unpark_current's existing `win_reason` parameter (A4.4,
#       issue #58 — "every EXISTING wake producer omits it and is
#       unaffected"), so composing it alongside Reactor.poll/service_io in
#       the SAME driver tick costs the existing lanes nothing.
#
# CancelRequest (issue #80 deliverable 2) reuses the A0.11 SuspendReason
# vocabulary VERBATIM (CANCEL/TIMER/CLOSED were already reserved codes;
# runtime/join_handle.mojo's SuspendReason docstring documents the
# "_reason also carries the winning wake cause" double-duty this relies
# on) rather than inventing a parallel enum that would need its own
# translation layer at every call site — the win_reason a waiter observes
# on resume via WaitNode.reason() literally IS a CancelRequest code.
#
# C6 EXACTLY-ONE-WINNER (issue #80 point 1): cancel_op/cancel_and_close and
# service_io_deadlines never invent their own claim discipline — they route
# every wake through the SAME unpark_current the reactor's own Reactor.poll
# uses.  unpark_current's top-level `if h.state() == RUNNABLE: return` plus
# its generation-claim guard are what make "whichever of {cancel, timeout,
# readiness, close} reaches unpark_current FIRST is the sole winner" true
# for free — this module adds no additional locking.  cancel_op/
# cancel_and_close additionally pre-check `h.state() == WAITING` BEFORE
# calling unpark_current (mirrors sync/mutex.mojo's cancel_lock_wait,
# which removes the waiter from its OWN FIFO before waking it): a task
# that already resumed via a competing winner is left COMPLETELY alone —
# this module never calls unpark_current against a non-WAITING task, which
# also sidesteps unpark_current's documented "a FRESH (required_gen=0)
# wake against a COMPLETED/CANCELLED task raises" surface (that loud path
# exists for genuinely illegal wakes, not benign lost cancel/close races).
#
# CLOSE-BEFORE-COMPLETION (issue #80 point 3, "close claims the generation
# BEFORE poller.unregister"): Reactor.unregister releases the op-table slot
# to FREE immediately (reactor/io_op_table.mojo's release()); reactor/
# poller.mojo's drain_ready drops any delivery whose slot is FREE or whose
# generation no longer matches BEFORE Reactor.poll ever reaches a wake
# attempt — so a poll() that observes readiness for an fd THIS module just
# unregistered is already a provable no-op by construction, regardless of
# whether unregister or the wake call runs first here.
from mojito_async.reactor.reactor import Reactor
from mojito_async.reactor.io_token import IoToken
from mojito_async.runtime.join_handle import JoinHandle, SuspendReason
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.runtime import Nil, Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.time.timer_heap import TimerHeap


struct CancelRequest:
    """Terminal wake reasons for a pending reactor op (issue #80
    deliverable 2).  Aliases of the EXISTING SuspendReason codes (A0.11
    vocabulary) — see the module docstring for why no parallel enum is
    introduced."""

    comptime CANCELLED = SuspendReason.CANCEL
    comptime TIMEOUT = SuspendReason.TIMER
    comptime CLOSED = SuspendReason.CLOSED


def _reconstruct_handle(tcb_addr: Int, task_id: Int) -> JoinHandle[Nil]:
    """Same erased-wake technique as reactor.reactor._reconstruct_handle
    (and time.timer_service.service_timers' bare-(tcbaddr,id) precedent): a
    JoinHandle[Nil] over a raw (tcb_addr, task_id) pair is safe to hand to
    unpark_current regardless of the waiter's REAL result type — only the
    T-independent TCB_Prefix fields are ever touched."""
    return JoinHandle[Nil](
        UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](
            unsafe_from_address=tcb_addr
        ),
        task_id,
    )


def _cancel_with_reason(
    mut reactor: Reactor,
    mut rt: Runtime,
    token: IoToken,
    waiter_tcb: Int,
    waiter_task_id: Int,
    reason: Int,
) raises -> Bool:
    """Shared body for cancel_op/cancel_and_close: pre-check WAITING (see
    module docstring's C6 section), release the op-table slot, and deliver
    the wake with `reason` as the win_reason so the stamp is written INSIDE
    unpark_current's guard — only if the claim actually succeeds (A4.4,
    issue #58).  Returns True iff THIS call found the waiter still WAITING
    and won the race (never calls unpark_current otherwise — a benign loss
    to a competing winner, not an error)."""
    var h = _reconstruct_handle(waiter_tcb, waiter_task_id)
    if h.state() != TaskControlBlock.WAITING:
        return False
    reactor.unregister(token)
    unpark_current[Nil](rt, h, win_reason=reason)
    return True


def cancel_op(
    mut reactor: Reactor,
    mut rt: Runtime,
    token: IoToken,
    waiter_tcb: Int,
    waiter_task_id: Int,
) raises -> Bool:
    """Cancel a pending reactor op (issue #80 deliverable 1): unregister
    `token`'s slot (drops the OS interest, frees the slot so a LATER
    readiness delivery decodes as stale — G2) then wake the parked owner
    with the CANCELLED reason.  The network lanes decode the reason via
    `is_cancel_wake`/`raise_if_cancel_wake` (runtime/park.mojo — the SAME
    predicates the mutex/semaphore/channel cancel lanes already use, since
    CancelRequest.CANCELLED == SuspendReason.CANCEL)."""
    return _cancel_with_reason(
        reactor, rt, token, waiter_tcb, waiter_task_id, CancelRequest.CANCELLED
    )


def cancel_and_close(
    mut reactor: Reactor,
    mut rt: Runtime,
    token: IoToken,
    waiter_tcb: Int,
    waiter_task_id: Int,
) raises -> Bool:
    """The disposal-hook flavor (issue #80 deliverable 3): identical to
    cancel_op but stamps CLOSED — the reason a TcpStream/TcpListener
    close-while-pending funnels through so the woken waiter's redrive can
    raise a decoded ClosedError instead of a bare CancellationError.  The
    caller closes the actual descriptor itself (this module never depends
    on net/); see `is_closed_wake`/`raise_if_closed_wake` below."""
    return _cancel_with_reason(
        reactor, rt, token, waiter_tcb, waiter_task_id, CancelRequest.CLOSED
    )


# ---------------------------------------------------------------------------
# Post-resume winner decode — mirrors runtime/park.mojo's
# is_cancel_wake/raise_if_cancel_wake exactly, for the two reasons this
# module owns (park.mojo already ships the CANCELLED decode pair; network
# lanes reuse THOSE for cancel_op's outcome and use the pair below only for
# TIMEOUT/CLOSED).
# ---------------------------------------------------------------------------


def is_timeout_wake[R: ResultValue](h: JoinHandle[R]) -> Bool:
    """True when this waiter's most recent wake was the TIMEOUT winner
    (service_io_deadlines below beat readiness/cancel/close)."""
    return h.tcb()[].wait_node()[].reason() == CancelRequest.TIMEOUT


def raise_if_timeout_wake[R: ResultValue](h: JoinHandle[R]) raises:
    """Post-resume winner check + raise (issue #80): if the deadline won
    this wait, clear the stamp (re-stamp-safety, mirrors park.mojo's
    raise_if_cancel_wake) and raise a decoded TimeoutError; a no-op when
    any other reason won."""
    if is_timeout_wake(h):
        h.tcb()[].wait_node()[].set_reason(SuspendReason.NONE)
        raise Error("TimeoutError: I/O operation deadline expired")


def is_closed_wake[R: ResultValue](h: JoinHandle[R]) -> Bool:
    """True when this waiter's most recent wake was the CLOSED winner
    (cancel_and_close beat readiness/cancel/timeout)."""
    return h.tcb()[].wait_node()[].reason() == CancelRequest.CLOSED


def raise_if_closed_wake[R: ResultValue](h: JoinHandle[R]) raises:
    """Post-resume winner check + raise: if a close won this wait, clear
    the stamp and raise a decoded ClosedError; a no-op when any other
    reason won."""
    if is_closed_wake(h):
        h.tcb()[].wait_node()[].set_reason(SuspendReason.NONE)
        raise Error("ClosedError: I/O descriptor closed while the op was pending")


# ---------------------------------------------------------------------------
# Deadline integration (issue #80 point 2 / deliverable 4)
# ---------------------------------------------------------------------------


def service_io_deadlines[R: ResultValue](
    mut rt: Runtime, mut heap: TimerHeap, now: UInt64
) raises -> Int:
    """The I/O-deadline servicing hook: pop every timer due at `now` in
    deadline order, skip stale generations (heap.live_gen mismatch — a
    superseded/cancelled arm, exactly like time.timer_service.
    service_timers), and wake each still-WAITING owner with the TIMEOUT
    winner reason.  Returns the number of tasks woken.

    Deliberately a SEPARATE hook from service_timers (see module
    docstring) — service_timers' plain-sleep contract leaves `_reason`
    UNCHANGED on purpose, this hook's whole job is to CHANGE it so a
    redriven read/write/connect/accept can tell a deadline apart from
    ordinary readiness.  A driver composes this alongside Reactor.poll/
    service_io in the same tick (see test/unit/t42_io_cancel_deadline.mojo
    for the composition), exactly like drive_step composes scheduler_loop
    + service_timers today."""
    var woke = 0
    while heap.has_due(now):
        var e = heap.pop_min()
        if heap.live_gen(e.id) != e.gen:
            continue  # stale generation — superseded/cancelled arm, drop
        var h = JoinHandle[R](
            UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
                unsafe_from_address=e.tcbaddr
            ),
            e.id,
        )
        if h.state() == TaskControlBlock.WAITING:
            unpark_current(rt, h, 0, SuspendReason.TIMER)
            woke += 1
    return woke
