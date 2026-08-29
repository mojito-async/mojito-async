# mojito_async/reactor/reactor.mojo
#
# A7.1/A7.2 reactor lane (issues #75/#76) — the reactor core: op registry,
# owner-worker routing, readiness-to-wake mapping, cancellation/timer
# hooks.  `Reactor` is the ONE module the scheduler and network lanes
# (connect/accept/read/write, issues #77-#80) talk to; it owns op
# registration, generation tokens, cancellation races, and mapping native
# readiness to task wakeups — NEVER syscalls (those live one layer down,
# vendor/mojito_sys_io + reactor/poller.mojo).
#
# CALL CONTRACT (the API 3 sibling A7 lanes build on — document changes
# here first):
#
#   var token = reactor.register_and_park(rt, h, handle, interests, kind)
#   # <task resumes here, woken by readiness/cancel, or never parked at
#   #  all because it was already ready inside the PREPARE/VALIDATE
#   #  window>
#   # ... re-attempt the syscall ...
#   reactor.unregister(token)
#
# `register_and_park` composes THREE steps and is the RECOMMENDED entry
# point for every network lane:
#   1. `register_op` — arms the OS-level interest and allocates an op-
#      table slot (issue #75 deliverable 3: "register/unregister, both
#      composed").  Does not touch task state.
#   2. Park the CURRENT task through the TWO-PHASE `park_prepare`/
#      `park_validate`/`park_commit` kernel (runtime/park.mojo, A2.5 issue
#      #71) — matching sync/mutex.mojo's contended `lock()` EXACTLY, never
#      the legacy single-phase `park_current`.  This is NOT about the
#      reactor's own readiness delivery (see point 3 below, which is self-
#      contained) — it is what keeps a CONCURRENT wake from another
#      source racing into the PARKING window from being lost: a
#      cancellation delivered on another worker while this op is
#      registering (`wake_cancelled`, runtime/park.mojo's cancellation
#      seam) calls `unpark_current` on this exact TCB, and only the two-
#      phase VALIDATE re-check catches a wake that landed between PARKING
#      and the WAITING commit (the same lost-wakeup bug A4.1/issue #55
#      fixed for mutex).
#   3. `attach_waiter` — stamps the waiter identity on the op-table slot,
#      called AFTER the park genuinely commits to WAITING (never before:
#      the generation that must be captured is the FRESH WAITING epoch —
#      `park_commit` bumps it on exactly that transition — attaching
#      earlier would capture a stale pre-park generation and reject every
#      future wake).  `attach_waiter` closes its OWN lost-wakeup window:
#      if `service_io`'s poll already delivered readiness for this slot
#      BEFORE the waiter was attached (the reactor genuinely running on
#      its own OS thread, per issue #75's architecture — not just this
#      package's single-threaded test drivers), attach_waiter finds the
#      slot already `IO_OP_READY` and wakes immediately instead of losing
#      the event.  This mechanism is INDEPENDENT of the two-phase park
#      kernel's own early-wake latch — the op table is its own self-
#      contained readiness latch, so the "readiness raced registration"
#      window is closed regardless of which task state it lands in.
#
# `register_op`/`attach_waiter`/`unregister` remain available separately
# for callers that need finer control (e.g. a non-blocking poll-only
# accept that never parks).
#
# ERASED WAKE (owner-worker routing, spec §19.2): the op table stores only
# raw `(tcb_addr, task_id)` Ints per waiter — the SAME address-erasure
# TaskRecord/scope.mojo already use — because a heterogeneous mix of task
# result types R can all be waiting on I/O at once and the table is one
# flat, non-generic slab.  Rather than re-deriving park.mojo's two-phase
# wake algorithm against a raw `TCB_Prefix` pointer (a second copy of
# unpark_current's logic to keep in lockstep forever), this module
# reconstructs a `JoinHandle[Nil]` over the SAME tcb address and calls the
# CANONICAL `unpark_current` — EXACTLY the technique
# mojito_async/time/timer_service.mojo's `service_timers` already uses
# (`JoinHandle[R]` built from a bare `(tcbaddr, id)` pair pulled off the
# timer heap).  This is safe regardless of the real task's R: every method
# `unpark_current` calls (`state`, `transition`, `wake_claim`,
# `owner_runtime`, `early_readiness`/`clear_early_readiness`,
# `claimed_epoch`, `wait_node`) delegates straight to `TCB_Prefix`, the
# FIRST member of `TaskControlBlock[T]` at a T-INDEPENDENT offset (see
# task_control_block.mojo's layout-contract header) — `unpark_current`
# never reads or writes the T-typed result tail, so a `JoinHandle[Nil]`
# reinterpretation touches EXACTLY the same bytes a correctly-typed
# `JoinHandle[R]` would.
from mojito_async.reactor.io_op_table import (
    IO_OP_ARMED,
    IO_OP_FREE,
    IO_OP_READY,
    IO_OP_REGISTERED,
    IoOpTable,
)
from mojito_async.reactor.io_token import IoOpKind, IoToken, invalid_token
from mojito_async.reactor.poller import NativePoller, create_poller, drain_ready
from mojito_async.runtime.join_handle import JoinHandle, SuspendReason
from mojito_async.runtime.park import park_commit, park_prepare, park_validate, unpark_current
from mojito_async.runtime.runtime import Nil, Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.time.deadline import Duration
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoEvent, IoInterest


def _reconstruct_handle(tcb_addr: Int, task_id: Int) -> JoinHandle[Nil]:
    """Rebuild a `JoinHandle[Nil]` over a raw erased `(tcb_addr, task_id)`
    pair — see the module docstring's ERASED WAKE section for why `Nil`
    (an arbitrary `ResultValue`) is safe here regardless of the waiting
    task's REAL result type."""
    return JoinHandle[Nil](
        UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](
            unsafe_from_address=tcb_addr
        ),
        task_id,
    )


struct Reactor(Movable):
    """The reactor core (issues #75/#76): one `NativePoller` + one
    `IoOpTable`.  Owns op registration, generation tokens, and readiness-
    to-wake mapping; never issues a syscall directly (that is
    `reactor/poller.mojo`'s job, one layer down)."""

    var _poller: NativePoller
    var _table: IoOpTable

    def __init__(out self, var poller: NativePoller):
        self._poller = poller^
        self._table = IoOpTable()


    def live_count(self) -> Int:
        """Number of currently-registered op-table slots (observability)."""
        return self._table.live_count()

    # --- op registry (issue #75 deliverable 3 / #76 deliverable 1) --------

    def register_op(
        mut self, handle: NativeIoHandle, interests: IoInterest, op_kind: Int
    ) raises -> IoToken:
        """Arm the OS-level interest for `handle` and allocate a fresh
        op-table slot; does NOT park.  Raises whatever the native poller
        raises (a real registration failure) or `IoOpTable.allocate`'s
        table-full error.  On a poller-registration failure the table
        slot is released again (never leaked)."""
        var token = self._table.allocate(op_kind, handle.get(), interests.bits)
        try:
            self._poller.register(handle, interests, token.encode())
        except e:
            _ = self._table.release(token)
            raise Error(String(e))
        return token

    def attach_waiter(
        mut self,
        mut rt: Runtime,
        token: IoToken,
        waiter_tcb: Int,
        waiter_task_id: Int,
        required_gen: Int,
    ) raises:
        """Stamp the waiter identity on `token`'s slot (called AFTER the
        caller's own park commits to WAITING — see the module docstring's
        CALL CONTRACT).  A stale token (already released/reused) is a
        silent no-op — the caller already lost the race to a cancel/close
        on the same token, nothing to attach to.  If the slot is ALREADY
        `IO_OP_READY` (service_io's poll delivered readiness before this
        attach ran), the wake is delivered IMMEDIATELY instead of being
        lost — this is the lost-wakeup-window close the module docstring
        promises."""
        if not self._table.is_live(token):
            return
        var e = self._table.get(token.slot)
        e.waiter_tcb = waiter_tcb
        e.waiter_task_id = waiter_task_id
        e.waiter_gen = required_gen
        var already_ready = e.state == IO_OP_READY
        if not already_ready:
            e.state = IO_OP_ARMED
        self._table.set(token.slot, e)
        if already_ready:
            unpark_current[Nil](
                rt, _reconstruct_handle(waiter_tcb, waiter_task_id), required_gen
            )

    def register_and_park[R: ResultValue](
        mut self,
        mut rt: Runtime,
        h: JoinHandle[R],
        handle: NativeIoHandle,
        interests: IoInterest,
        op_kind: Int,
    ) raises -> IoToken:
        """The RECOMMENDED entry point (see the module docstring's CALL
        CONTRACT): register `handle`, then park the CURRENT task via the
        two-phase kernel (`park_prepare`/`park_validate`/`park_commit` —
        NOT the legacy single-phase `park_current`, so a concurrent
        cancellation wake landing in the PARKING window is never lost,
        exactly like sync/mutex.mojo's contended `lock()`), attaching the
        waiter to the op-table slot only after the park genuinely commits
        to WAITING.  Returns the live `IoToken` in every case — including
        when `park_validate` finds an early wake and the task never
        actually left RUNNING (a concurrent cancel/close raced the
        register): the caller always owns `unregister(token)` and decides
        from its own post-resume state (cancelled? readable? writable?)
        what happened."""
        var token = self.register_op(handle, interests, op_kind)
        park_prepare(h)
        if park_validate(h):
            # An early wake (e.g. cancellation) landed in the PARKING
            # window before this task could genuinely park: close the
            # window without ever entering WAITING.  No waiter is
            # attached to the op-table slot in this branch (attach only
            # ever happens post-commit) — the caller's checkpoint/state
            # inspection after this call decides what to do with `token`.
            park_commit(h)
        else:
            park_commit(h, SuspendReason.IO)
            self.attach_waiter(
                rt, token, Int(h.tcb()), h.id(), h.tcb()[].generation()
            )
        return token

    def unregister(mut self, token: IoToken) raises:
        """Release `token`'s op-table slot and drop the OS-level interest
        (the cancellation hook: a checkpoint that observes cancellation
        calls this to release both).  Idempotent / race-safe: a stale or
        already-released token is a silent no-op (matches the native
        poller's own already-closed-handle contract) — issue #76's "slot
        reuse after a cancelled/closed op never reuses an old token"
        holds because release() only ever frees the slot this SPECIFIC
        token still names."""
        if not self._table.is_live(token):
            return
        var e = self._table.get(token.slot)
        var handle = NativeIoHandle(e.fd)
        _ = self._table.release(token)
        # never raise on an already-gone fd — the table release above is
        # the source of truth for "did this unregister do anything"; a
        # raising unregister here would turn a benign double-cancel race
        # into a loud failure.
        try:
            self._poller.unregister(handle)
        except e2:
            pass

    # --- timer/cancellation integration seam (issue #75 deliverable) ------

    def wake(mut self) raises:
        """Interrupt a blocked `poll()`/`service_io` wait promptly with
        zero events — the cancellation/fairness shim issue #75 names (a
        cancelling worker or a newly-armed nearer timer deadline calls
        this to make the reactor thread re-evaluate its wait bound)."""
        self._poller.wake()

    # --- the poll/drain/wake cycle (issue #75 deliverables 2 & 4) ---------

    def poll(
        mut self, mut rt: Runtime, timeout: Optional[Duration]
    ) raises -> List[IoToken]:
        """One reactor tick: `wait()` the native poller (bounded by
        `timeout` — the caller passes the nearest timer-heap deadline or
        None to block indefinitely, EPIC #6's timer-hook seam), map
        delivered events back to op-table entries via `drain_ready`, and
        wake every entry that has a waiter attached.  Returns the tokens
        that became ready (whether or not a waiter was attached yet —
        `attach_waiter`'s already-ready check picks up the rest).  Timeout
        expiry is success with an empty list; `wake()` ends a blocked wait
        promptly with an empty list too (issue #75 acceptance)."""
        var buf = List[IoEvent]()
        for _ in range(IoOpTable.CAPACITY):
            buf.append(IoEvent())
        var span = Span[IoEvent, MutAnyOrigin](buf)
        var n = self._poller.wait(span, timeout)
        var ready = drain_ready(Span[IoEvent, MutAnyOrigin](buf)[0:n], self._table)
        for i in range(len(ready)):
            var tok = ready[i]
            var e = self._table.get(tok.slot)
            if e.waiter_tcb != 0:
                unpark_current[Nil](
                    rt,
                    _reconstruct_handle(e.waiter_tcb, e.waiter_task_id),
                    e.waiter_gen,
                )
        return ready^


def make_reactor() raises -> Reactor:
    """Module-level factory (b2 has no static methods): a Reactor over the
    platform-shipped readiness backend (`create_poller`)."""
    return Reactor(create_poller())


def service_io(
    mut rt: Runtime, mut reactor: Reactor, timeout: Optional[Duration]
) raises -> Int:
    """The scheduler-loop servicing hook — mirrors
    `mojito_async.time.timer_service.service_timers` exactly (poll, drain,
    wake).  Returns the number of ops that became ready this tick
    (`Reactor.poll`'s result length), matching `service_timers`'s "woken"
    count convention so a caller composing both hooks (a future
    `drive_step`-style step) can sum them the same way."""
    var ready = reactor.poll(rt, timeout)
    return len(ready)
