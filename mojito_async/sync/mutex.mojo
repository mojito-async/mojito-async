# mojito_async/sync/mutex.mojo
#
# A1.2 (issue #34) — task-aware `Mutex[T]` + `MutexGuard`, over the A1.1
# park/wake path (spec §34).  Parks the CURRENT task on contention instead of
# blocking the worker thread.  Composes with the A1.1 single-worker
# cooperative scheduler: `lock` is a dispatcher-level operation (the runtime
# resumes a parked task by re-dispatching it), so the fast path returns
# acquisition directly and the slow path publishes the waiter, parks, and
# relies on the FIFO handoff from a later unlock to grant acquisition.
#
# Semantics (spec §34.1–34.3):
#   - fast path: uncontended CAS UNLOCKED -> LOCKED — no allocation, no park;
#     on the single cooperative worker a plain state check is atomic within a
#     worker slice (no invisible preemption), mirroring the spec's atomic
#     model without a spur.
#   - slow path: publish the current task as an embedded FIFO waiter; park it
#     via the A1.1 `park_current`; the caller is free to drive other
#     tasks.  A later unlock grants the head waiter (per-waiter GRANT marker
#     in its embedded WaitNode), re-dispatches it; its lock() claims the
#     marker and acquires without re-checking the contended state.
#   - unlock: FIFO handoff to ONE waiter (spec §34.3 — no thundering herd).
#     `_locked` stays held through the handoff window so a new acquirer
#     cannot steal the lock ahead of the granted waiter.
#   - FIFO fairness: waiters are handed the lock in arrival order.
#
# Lost-wakeup safety: within one dispatcher slice there is no interleaving, so
# publish+park on the slow path is atomic with respect to other tasks; a
# release therefore always finds its waiter already parked (WAITING) and the
# A1.1 `unpark_current` delivers readiness exactly once per epoch.
#
# Mojo 1.0.0b2 (def-only) constraints honored: `def` only; generic methods
# parameterized on the caller's ResultValue R; module-level factories; the
# slow-path waiter FIFO is a Deque of (addr,id) with no per-suspension
# allocation on the fast path.
from std.collections import Deque
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.task import JoinHandle
from mojito_async.runtime.park import park_current, unpark_current, raise_if_cancel_wake, wake_cancelled
from mojito_async.cancellation import CancellationToken


comptime WAITER_GRANTED = Int(1)


def _waiter_handle[R: ResultValue](tcb_addr: Int, tid: Int) -> JoinHandle[R]:
    """Reconstruct a waiter's one-shot handle from the queued (addr, id)."""
    return JoinHandle[R](
        UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=tcb_addr
        ),
        tid,
    )


def _remove_at_index(mut q: Deque[Int], idx: Int) raises:
    """Remove the element at `idx` (0-based from the front), preserving the
    relative FIFO order of every OTHER element (issue #57's cancel_lock_wait:
    a cancelled waiter may sit anywhere in the queue, not just the head).
    Deque has no native middle-removal, so this is an O(n) full rotate —
    acceptable at the same complexity already budgeted for these small
    waiter queues elsewhere in this file family."""
    var n = len(q)
    for i in range(n):
        var v = q.popleft()
        if i != idx:
            q.append(v)


# ---------------------------------------------------------------------------
# Mutex[T]
# ---------------------------------------------------------------------------

struct Mutex[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](Movable, ImplicitlyDeletable):
    """Task-aware mutex owning + guarding a value of type `T` (spec §34).

    State:
      _locked — UNLOCKED(False) / LOCKED(True); a handoff keeps it True while
                the granted waiter is about to claim.
      _value  — the protected value.
      _w_tcb/_w_id — FIFO of parked waiters (tcb_addr and task id).

    The waiter's own TCB[].wait_node()._next carries the GRANT marker this
    lock sets when it hands ownership over in unlock(); the resumed task's
    lock() claims (clears) it and returns True.  The marker is per-waiter, so
    several waiters can be granted in separate unlock calls without a shared
    slot.
    """

    var _locked: Bool
    var _value: Self.T
    var _w_tcb: Deque[Int]
    var _w_id: Deque[Int]

    def __init__(out self, initial: Self.T):
        self._locked = False
        self._value = initial
        self._w_tcb = Deque[Int]()
        self._w_id = Deque[Int]()

    # --- queries -----------------------------------------------------------

    def value(mut self) -> UnsafePointer[Self.T, MutAnyOrigin]:
        """Mutable access to the protected value (authorized while held)."""
        return UnsafePointer[Self.T, MutAnyOrigin](to=self._value)

    def is_locked(self) -> Bool:
        return self._locked

    def waiter_count(self) -> Int:
        return len(self._w_tcb)

    # --- fast path (spec §34.1) --------------------------------------------

    def try_lock(mut self) -> Bool:
        """Uncontended acquire: CAS UNLOCKED -> LOCKED.

        Single cooperative worker => a plain state check is atomic within a
        slice.  No allocation, no scheduler lookup, no park."""
        if self._locked:
            return False
        self._locked = True
        return True

    # --- lock / slow path (spec §34.2) --------------------------------------

    def lock[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Task-aware acquire.  Returns True when THIS call owns the lock.

        Fast path returns immediately.  On contention: publish the caller as
        an embedded FIFO waiter, park it via the A1.1 `park_current`, and
        return False — the caller's dispatcher is free to drive other tasks.
        A later unlock grants the head waiter, which is re-dispatched, enters
        lock() again, claims its GRANT marker and returns True.
        """
        # 1) claim an outstanding grant handed over by a prior unlock()
        var dbg_marker = h.tcb()[].wait_node()[].next()
        if dbg_marker == WAITER_GRANTED:
            h.tcb()[].wait_node()[].set_next(0)
            return True
        # 2) fast path
        if self.try_lock():
            return True
        # 3) contended slow path
        self._w_tcb.append(Int(h.tcb()))
        self._w_id.append(h.id())
        park_current(rt, h)
        return False

    # --- unlock / handoff (spec §34.3) -------------------------------------

    def unlock[R: ResultValue](mut self, mut rt: Runtime) raises -> Bool:
        """Release and, when a waiter exists, hand off to ONE FIFO waiter.

        True when ownership was handed off; False when released cold.  The
        lock STAYS held across the handoff window so a new acquirer cannot
        steal it before the granted waiter runs and claims (no herd)."""
        if len(self._w_tcb) == 0:
            self._locked = False
            return False
        var tcb = self._w_tcb.popleft()
        var tid = self._w_id.popleft()
        var hw = _waiter_handle[R](tcb, tid)
        hw.tcb()[].wait_node()[].set_next(WAITER_GRANTED)
        unpark_current(rt, hw)
        return True

    def holds_grant[R: ResultValue](self, h: JoinHandle[R]) -> Bool:
        """Diagnostics: does `h` carry an outstanding GRANT marker?"""
        return h.tcb()[].wait_node()[].next() == WAITER_GRANTED

    # --- token-aware acquire (A4.3, issue #57) ------------------------------

    def lock_cancellable[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], token: CancellationToken
    ) raises -> Bool:
        """Token-aware acquire.  Identical to `lock()` on every readiness
        path (fast CAS, GRANT-marker re-entry, contended park); the ONLY
        addition is the C6 winner check this waiter's own resume carries:
        `raise_if_cancel_wake` fires ONLY when THIS waiter's `cancel_lock_
        wait` won the race (never when readiness/GRANT won — the mutex is
        left exactly as `lock()` would leave it).  A pre-park check
        (`token.checkpoint()` via raise_if_cancel_wake's sibling) is
        deliberately NOT duplicated here: `lock()`'s fast CAS/GRANT re-entry
        paths never park, so there is nothing to pre-empt; a caller that
        wants the "already requested" case to refuse before EVER contending
        should check `token.is_cancellation_requested()` itself before
        calling in (mirrors park_cancellable's contract at the primitive
        boundary)."""
        raise_if_cancel_wake(h)
        if token.is_cancellation_requested():
            raise Error("CancellationError: mutex lock cancelled")
        return self.lock(rt, h)

    def cancel_lock_wait[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Cancel a parked `lock_cancellable` waiter (issue #57): removes
        `h`'s task id from the FIFO wait queue — preserving the ORDER of
        every OTHER queued waiter — and delivers a CANCEL wake.  Returns
        True iff THIS call found + removed + woke the waiter (it won the
        race); False when the id was never queued or a concurrent `unlock`
        already popped it (readiness won — no ghost entry, no double
        wake)."""
        var idx = -1
        for i in range(len(self._w_id)):
            if self._w_id[i] == h.id():
                idx = i
                break
        if idx == -1:
            return False
        _remove_at_index(self._w_tcb, idx)
        _remove_at_index(self._w_id, idx)
        wake_cancelled(rt, h)
        return True


# ---------------------------------------------------------------------------
# MutexGuard
# ---------------------------------------------------------------------------

struct MutexGuard[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](Movable):
    """RAII-style handle to an acquired Mutex: exposes the protected value
    and releases the lock.  In the cooperative single-worker model release is
    always explicit (the body runs at a dispatcher boundary), so `release()`
    performs the FIFO handoff."""

    var _mtx: UnsafePointer[Mutex[Self.T], MutAnyOrigin]

    def __init__(out self, m: UnsafePointer[Mutex[Self.T], MutAnyOrigin]):
        self._mtx = m

    def value(mut self) -> UnsafePointer[Self.T, MutAnyOrigin]:
        return self._mtx[].value()

    def release[R: ResultValue](mut self, mut rt: Runtime) raises -> Bool:
        return self._mtx[].unlock(rt)