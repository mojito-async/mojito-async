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
#   - slow path: publish the current task as an embedded FIFO waiter; park
#     it via the TWO-PHASE `park_prepare`/`park_validate`/`park_commit`
#     kernel (A4.1, issue #55 — promoted from the single-phase `park_current`
#     used through A2.5); the caller is free to drive other tasks.  A later
#     unlock grants the head waiter (per-waiter GRANT marker in its
#     embedded WaitNode), re-dispatches it; its lock() claims the marker
#     and acquires without re-checking the contended state.
#   - unlock: FIFO handoff to ONE waiter (spec §34.3 — no thundering herd).
#     `_locked` stays held through the handoff window so a new acquirer
#     cannot steal the lock ahead of the granted waiter.
#   - FIFO fairness: waiters are handed the lock in arrival order.
#
# Lost-wakeup safety (A4.1, issue #55): on the A1 single cooperative worker,
# publish+park was atomic with respect to other tasks — a release always
# found its waiter already parked.  On the A2 M:N scheduler a cross-worker
# unlock() can race into the PARKING window BETWEEN `park_prepare` and the
# WAITING commit; the single-phase `park_current` never consulted the
# early-wake latch there, so that race silently dropped the wake.  The
# two-phase kernel closes it: `park_validate` re-checks the latch before
# `park_commit` decides WAITING vs. an immediate RUNNABLE unwind, so the
# grant is never lost regardless of which worker's unlock() wins the race.
# `unpark_current` still delivers readiness exactly once per epoch and
# routes to the OWNER worker's remote-ready queue (spec §19.2).
#
# Mojo 1.0.0b2 (def-only) constraints honored: `def` only; generic methods
# parameterized on the caller's ResultValue R; module-level factories; the
# slow-path waiter FIFO is a Deque of (addr,id) with no per-suspension
# allocation on the fast path.
from std.collections import Deque
from mojito_async.runtime.queue import SpinLock
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.task import JoinHandle, claim_running
from mojito_async.runtime.park import (
    park_commit,
    park_prepare,
    park_validate,
    unpark_current,
    raise_if_cancel_wake,
    wake_cancelled,
)
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

struct Mutex[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable]:
    """Task-aware mutex owning + guarding a value of type `T` (spec §34).

    State:
      _guard  — SpinLock (A4.1, issue #55) serializing every read/write of
                `_locked` and the waiter FIFO: on the A2 M:N scheduler two
                REAL worker OS threads can call try_lock()/lock()/unlock()
                on the SAME Mutex concurrently, and a plain check-then-set
                on `_locked` (correct only on the A1 single cooperative
                worker, where no interleaving happens inside a slice) let
                two callers both observe UNLOCKED and both set LOCKED — an
                undetected double acquisition (empirically reproduced by
                t38's cross-worker stress: a corrupted final counter with
                zero hangs, zero raises).  Same SpinLock as queue.mojo's
                LocalDeque/RemoteReadyQueue guard; this struct therefore
                drops the Movable/ImplicitlyDeletable conformances the
                SpinLock's Atomic cannot support (mirrors Runtime/Worker,
                which embed the identical guard for the identical reason).
      _locked — UNLOCKED(False) / LOCKED(True); a handoff keeps it True while
                the granted waiter is about to claim.
      _value  — the protected value.
      _w_tcb/_w_id — FIFO of parked waiters (tcb_addr and task id).

    The waiter's own TCB[].wait_node()._next carries the GRANT marker this
    lock sets when it hands ownership over in unlock(); the resumed task's
    lock() claims (clears) it and returns True.  The marker is per-waiter, so
    several waiters can be granted in separate unlock calls without a shared
    slot.  The marker itself needs no guard: only the resumed owner-task
    ever reads/clears it, strictly after the wake claim that resumed it
    (happens-before via the SAME claim the guard below serializes).
    """

    var _guard: SpinLock
    var _locked: Bool
    var _value: Self.T
    var _w_tcb: Deque[Int]
    var _w_id: Deque[Int]

    def __init__(out self, initial: Self.T):
        self._guard = SpinLock()
        self._locked = False
        self._value = initial
        self._w_tcb = Deque[Int]()
        self._w_id = Deque[Int]()

    # --- queries -----------------------------------------------------------

    def value(mut self) -> UnsafePointer[Self.T, MutAnyOrigin]:
        """Mutable access to the protected value (authorized while held)."""
        return UnsafePointer[Self.T, MutAnyOrigin](to=self._value)

    def is_locked(mut self) -> Bool:
        self._guard.lock()
        var v = self._locked
        self._guard.unlock()
        return v

    def waiter_count(mut self) -> Int:
        self._guard.lock()
        var n = len(self._w_tcb)
        self._guard.unlock()
        return n

    # --- fast path (spec §34.1) --------------------------------------------

    def try_lock(mut self) -> Bool:
        """Uncontended acquire: GUARDED compare-and-set UNLOCKED -> LOCKED
        (A4.1, issue #55).  A plain check-then-set was correct only on the
        A1 single cooperative worker (no interleaving inside a slice); on
        the A2 M:N scheduler two REAL worker OS threads calling try_lock()
        concurrently could both observe UNLOCKED and both set LOCKED — an
        undetected double acquisition, empirically reproduced by t38's
        cross-worker stress before this fix.  No allocation, no scheduler
        lookup, no park."""
        self._guard.lock()
        if self._locked:
            self._guard.unlock()
            return False
        self._locked = True
        self._guard.unlock()
        return True

    # --- lock / slow path (spec §34.2) --------------------------------------

    def lock[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Task-aware acquire.  Returns True when THIS call owns the lock.

        Fast path returns immediately.  On contention: publish the caller
        as an embedded FIFO waiter, then park via the TWO-PHASE PREPARE/
        VALIDATE/COMMIT kernel (A2.5, issue #71) — NOT the single-phase
        `park_current` — so a cross-worker `unlock()` that races into the
        PARKING window is never lost (A4.1, issue #55: `park_current` never
        consults the early-wake latch, so a foreign release landing between
        PARKING and the WAITING commit was silently dropped).  VALIDATE
        re-checks the latch BEFORE committing to WAITING; a grant delivered
        inside that window unwinds PARKING -> RUNNABLE in THIS SAME call
        (the task never sleeps) and this call re-claims RUNNING and
        consumes the grant marker `unlock()` already stamped.  Returns
        False only when the park genuinely commits to WAITING — the
        caller's dispatcher is then free to drive other tasks.  A later
        unlock grants the head waiter, which is re-dispatched, enters
        lock() again, claims its GRANT marker (step 1) and returns True.
        """
        # 1) claim an outstanding grant handed over by a prior unlock() —
        # only the resumed owner-task itself ever reaches this after being
        # granted (happens-before via the wake claim that resumed it), so
        # no guard is needed for this read/clear.
        var dbg_marker = h.tcb()[].wait_node()[].next()
        if dbg_marker == WAITER_GRANTED:
            h.tcb()[].wait_node()[].set_next(0)
            return True
        # 2) fast path / 3) publish as a FIFO waiter — ONE guarded critical
        # section (A4.1, issue #55): checking `_locked` and, on contention,
        # appending to the FIFO must be atomic with respect to a concurrent
        # unlock() on another worker — two separately-guarded calls here
        # would let a release land BETWEEN them and pop an empty FIFO,
        # missing this waiter entirely (a genuine lost wakeup distinct from
        # the PARKING-window one below).
        self._guard.lock()
        if not self._locked:
            self._locked = True
            self._guard.unlock()
            return True
        self._w_tcb.append(Int(h.tcb()))
        self._w_id.append(h.id())
        self._guard.unlock()
        # two-phase park (issue #55): NOT the single-phase `park_current`,
        # exactly as documented above.
        park_prepare(h)
        if park_validate(h):
            # An early wake fired in the PREPARE/COMMIT window (A0-T11).
            # It may be a GRANT (unlock() popped+stamped this waiter) OR a
            # CANCEL (cancel_lock_wait removed it from the FIFO and called
            # wake_cancelled, which latches early_readiness without setting
            # the GRANT marker).  Close the window unconditionally; then
            # inspect the marker to distinguish the two cases.
            _ = park_commit(h)
            claim_running(h)
            if not self.holds_grant(h):
                # Cancel early-wake: no grant was stamped.  Re-enqueue this
                # task so the caller's dispatcher re-dispatches it; the
                # re-entry will see the CANCEL reason and raise.
                h.tcb()[].transition(TaskControlBlock.RUNNABLE)
                rt.enqueue_local(Int(h.tcb()), h.id())
                return False
            h.tcb()[].wait_node()[].set_next(0)
            return True
        if not park_commit(h):
            # Early wake landed between park_validate and park_commit.
            # Same grant-vs-cancel ambiguity: check the marker.
            claim_running(h)
            if not self.holds_grant(h):
                h.tcb()[].transition(TaskControlBlock.RUNNABLE)
                rt.enqueue_local(Int(h.tcb()), h.id())
                return False
            h.tcb()[].wait_node()[].set_next(0)
            return True
        return False

    # --- unlock / handoff (spec §34.3) -------------------------------------

    def unlock[R: ResultValue](mut self, mut rt: Runtime) raises -> Bool:
        """Release and, when a waiter exists, hand off to ONE FIFO waiter.

        True when ownership was handed off; False when released cold.  The
        lock STAYS held across the handoff window so a new acquirer cannot
        steal it before the granted waiter runs and claims (no herd).  The
        `_locked`/FIFO check-and-pop is ONE guarded critical section (A4.1,
        issue #55) — see lock()'s matching section for why."""
        self._guard.lock()
        if len(self._w_tcb) == 0:
            self._locked = False
            self._guard.unlock()
            return False
        var tcb = self._w_tcb.popleft()
        var tid = self._w_id.popleft()
        self._guard.unlock()
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
        wake).  The FIFO search + removal is ONE guarded critical section
        (A4.1, issue #55, extended to this A4.3 path during the A4 merge):
        `_w_tcb`/`_w_id` are shared mutable state a concurrent `unlock()`
        on another worker can pop from at the same instant; scanning/
        splicing them unguarded would race the same Deque storage `unlock`
        already serializes through `self._guard`."""
        self._guard.lock()
        var idx = -1
        for i in range(len(self._w_id)):
            if self._w_id[i] == h.id():
                idx = i
                break
        if idx == -1:
            self._guard.unlock()
            return False
        _remove_at_index(self._w_tcb, idx)
        _remove_at_index(self._w_id, idx)
        self._guard.unlock()
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