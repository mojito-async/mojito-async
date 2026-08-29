# mojito_async/sync/rwlock.mojo
#
# A4.8 (issue #66) — task-aware `RWLock[T]` + `ReadGuard`/`WriteGuard`, over
# the A1.1 park/wake path and the Mutex (issue #34) FIFO publish+park+handoff
# pattern extended to a shared-read exclusion.  Same choreography as
# mutex.mojo/semaphore.mojo: `read`/`write` are dispatcher-level operations
# (the runtime resumes a parked task by re-dispatching it), so the fast path
# returns acquisition directly and the slow path publishes the caller as a
# FIFO waiter, parks it via `park_current`, and relies on a later release to
# grant it back via the per-waiter GRANT marker its own re-entry claims.
#
# Arbitration policy — WRITER-PREFERENCE (documented; issue #66 accepts
# either writer-preference or a documented fair policy):
#   - a NEW reader takes the fast path only when no writer HOLDS and none
#     WAITS (`try_read`): a waiting writer blocks new readers from cutting
#     in ahead of it — the classic RW-lock miss (continuous readers starve
#     the writer) this policy exists to avoid.
#   - a NEW writer takes the fast path only when neither a writer holds nor
#     any reader holds (`try_write`).
#   - `unlock_write` hands off to the FIFO-head WRITER when one waits
#     (preference held even over readers that queued earlier); otherwise it
#     drains and grants EVERY waiting reader in one pass — they never
#     conflict with each other, so there is no reason to wake them one at a
#     time.
#   - `unlock_read` (the LAST reader out) grants the FIFO-head WRITER when
#     one waits.  INVARIANT: a reader can only ever be QUEUED while a writer
#     holds or waits (see `try_read`), so reaching zero readers never leaves
#     a reader to grant here — only a writer, or nobody.
#
# Cancellation (issue #66 acceptance: "a canceled waiter does not leak the
# RW grant to the wrong waiter"): `cancel_read_wait`/`cancel_write_wait` let
# a caller pull ITS OWN still-queued (not yet granted) waiter record out of
# the FIFO before abandoning the wait, so a later release skips straight to
# the next live waiter instead of granting a slot to (or leaving an off-by-
# one gap for) an entry nobody will ever claim.  Callers MUST call this
# BEFORE the WaitNode carries a GRANT marker (i.e. while still logically
# WAITING) — on the single cooperative worker this is deterministic (no
# race window): the park.mojo header notes publish+park is atomic within a
# dispatcher slice, and by the same argument so is publish+cancel.  Mutex
# and Semaphore never needed per-waiter cancellation (their waiters are
# never individually pulled pre-grant); RWLock is the first sync primitive
# to need it, since a slow reader queue can be long-lived under writer
# preference.
#
# Mojo 1.0.0b2 (def-only) constraints honored: `def` only; generic guard
# methods parameterized over the caller's ResultValue R; module-level
# factories; the slow-path waiter FIFOs are parallel Deques of (addr, id)
# with no per-suspension allocation on the fast path.
from std.collections import Deque
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.task import JoinHandle
from mojito_async.runtime.park import park_current, unpark_current


comptime RW_GRANTED = Int(1)


def _rw_waiter_handle[R: ResultValue](tcb_addr: Int, tid: Int) -> JoinHandle[R]:
    """Reconstruct a waiter's one-shot handle from the queued (addr, id)."""
    return JoinHandle[R](
        UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=tcb_addr
        ),
        tid,
    )


# ---------------------------------------------------------------------------
# RWLock[T]
# ---------------------------------------------------------------------------

struct RWLock[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](Movable, ImplicitlyDeletable):
    """Task-aware reader-writer lock owning + guarding a value of type `T`
    (issue #66).  Writer-preference arbitration (see module docstring).

    State:
      _readers        — count of CURRENTLY HELD read grants (0 when free or
                         exclusively write-locked).
      _writer_locked  — a writer holds exclusively.
      _value          — the protected value.
      _r_tcb/_r_id    — FIFO of parked reader waiters (tcb_addr, task id).
      _w_tcb/_w_id    — FIFO of parked writer waiters (tcb_addr, task id).
    """

    var _readers: Int
    var _writer_locked: Bool
    var _value: Self.T
    var _r_tcb: Deque[Int]
    var _r_id: Deque[Int]
    var _w_tcb: Deque[Int]
    var _w_id: Deque[Int]

    def __init__(out self, initial: Self.T):
        self._readers = 0
        self._writer_locked = False
        self._value = initial
        self._r_tcb = Deque[Int]()
        self._r_id = Deque[Int]()
        self._w_tcb = Deque[Int]()
        self._w_id = Deque[Int]()

    # --- queries -------------------------------------------------------------

    def value(mut self) -> UnsafePointer[Self.T, MutAnyOrigin]:
        """Access to the protected value.  Authorized while WRITE held for
        mutation, or while READ held for read-only use by convention — b2
        has no const-view pointer type to enforce the read-only half."""
        return UnsafePointer[Self.T, MutAnyOrigin](to=self._value)

    def is_write_locked(self) -> Bool:
        return self._writer_locked

    def reader_count(self) -> Int:
        return self._readers

    def reader_waiter_count(self) -> Int:
        return len(self._r_tcb)

    def writer_waiter_count(self) -> Int:
        return len(self._w_tcb)

    # --- fast path -------------------------------------------------------------

    def try_read(mut self) -> Bool:
        """Uncontended shared acquire: succeeds only when no writer HOLDS
        and none WAITS (writer preference — a new reader never cuts in
        front of an already-waiting writer).  No allocation, no park; a
        plain state check is atomic within a worker slice on the single
        cooperative worker."""
        if self._writer_locked or len(self._w_tcb) != 0:
            return False
        self._readers += 1
        return True

    def try_write(mut self) -> Bool:
        """Uncontended exclusive acquire: succeeds only when neither a
        writer holds nor any reader holds.  No allocation, no park."""
        if self._writer_locked or self._readers != 0:
            return False
        self._writer_locked = True
        return True

    # --- read / write (slow path) -----------------------------------------------

    def read[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Task-aware shared acquire.  Returns True when THIS call owns a
        read grant.

        1) claim an outstanding grant handed over by a prior release
           (marker check — covers BOTH a direct writer-preference grant and
           a batch reader-drain grant, since only one is ever pending per
           waiter).
        2) fast path (`try_read`).
        3) contended slow path: publish as a FIFO reader waiter and park;
           the caller's dispatcher is free to drive other tasks.  A later
           `unlock_write` drains the reader FIFO and grants every one of
           them at once — readers never contend with each other."""
        if h.tcb()[].wait_node()[].next() == RW_GRANTED:
            h.tcb()[].wait_node()[].set_next(0)
            return True
        if self.try_read():
            return True
        self._r_tcb.append(Int(h.tcb()))
        self._r_id.append(h.id())
        park_current(rt, h)
        return False

    def write[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Task-aware exclusive acquire.  Returns True when THIS call owns
        the write lock.  Fast path (`try_write`) returns immediately; on
        contention the caller is enqueued FIFO among writer waiters and
        parked; a later unlock hands off to the FIFO-head writer (writer
        preference — spec see module docstring)."""
        if h.tcb()[].wait_node()[].next() == RW_GRANTED:
            h.tcb()[].wait_node()[].set_next(0)
            return True
        if self.try_write():
            return True
        self._w_tcb.append(Int(h.tcb()))
        self._w_id.append(h.id())
        park_current(rt, h)
        return False

    # --- unlock / handoff --------------------------------------------------------

    def unlock_read[R: ResultValue](mut self, mut rt: Runtime) raises -> Bool:
        """Release ONE read grant.  Permissive like Mutex/Semaphore: callers
        are trusted to release only what they acquired (no double-release
        guard).  When the reader count reaches zero, hand off to the FIFO-
        head writer if one waits (the only thing that CAN be queued per the
        class invariant — see module docstring).  True when a writer was
        granted; False otherwise (still shared-held, or released cold with
        nobody waiting)."""
        self._readers -= 1
        if self._readers > 0:
            return False
        if len(self._w_tcb) == 0:
            return False
        var tcb = self._w_tcb.popleft()
        var tid = self._w_id.popleft()
        self._writer_locked = True
        var hw = _rw_waiter_handle[R](tcb, tid)
        hw.tcb()[].wait_node()[].set_next(RW_GRANTED)
        unpark_current(rt, hw)
        return True

    def unlock_write[R: ResultValue](mut self, mut rt: Runtime) raises -> Int:
        """Release the exclusive lock.  Writer preference: hand off to the
        FIFO-head writer if one waits (returns 1, and the lock stays write-
        locked across the handoff window — mirrors Mutex.unlock); otherwise
        drain and grant EVERY waiting reader in this one pass (returns the
        count granted, 0 when nobody waits and the lock goes cold)."""
        self._writer_locked = False
        if len(self._w_tcb) != 0:
            var tcb = self._w_tcb.popleft()
            var tid = self._w_id.popleft()
            self._writer_locked = True
            var hw = _rw_waiter_handle[R](tcb, tid)
            hw.tcb()[].wait_node()[].set_next(RW_GRANTED)
            unpark_current(rt, hw)
            return 1
        var n = len(self._r_tcb)
        for _ in range(n):
            var tcb = self._r_tcb.popleft()
            var tid = self._r_id.popleft()
            self._readers += 1
            var hr = _rw_waiter_handle[R](tcb, tid)
            hr.tcb()[].wait_node()[].set_next(RW_GRANTED)
            unpark_current(rt, hr)
        return n

    # --- cancellation: pull a still-queued waiter out of the FIFO ---------------

    def cancel_read_wait[R: ResultValue](mut self, h: JoinHandle[R]) raises -> Bool:
        """Remove `h`'s OWN still-queued reader waiter record before it is
        granted (issue #66: a cancelled waiter must not leak a grant to the
        wrong task).  True when a matching record was found and removed;
        False when `h` was not queued (already granted — the marker is
        already set and it is no longer in this FIFO — or never enqueued).
        MUST be called before observing a GRANT marker on `h` (see module
        docstring); rebuilds the FIFO to preserve the remaining waiters'
        order (Deque has no arbitrary-index erase)."""
        var target = Int(h.tcb())
        var tid = h.id()
        var n = len(self._r_tcb)
        var kept_tcb = Deque[Int]()
        var kept_id = Deque[Int]()
        var found = False
        for _ in range(n):
            var t = self._r_tcb.popleft()
            var i = self._r_id.popleft()
            if (not found) and t == target and i == tid:
                found = True
                continue
            kept_tcb.append(t)
            kept_id.append(i)
        self._r_tcb = kept_tcb^
        self._r_id = kept_id^
        return found

    def cancel_write_wait[R: ResultValue](mut self, h: JoinHandle[R]) raises -> Bool:
        """Writer counterpart of `cancel_read_wait`."""
        var target = Int(h.tcb())
        var tid = h.id()
        var n = len(self._w_tcb)
        var kept_tcb = Deque[Int]()
        var kept_id = Deque[Int]()
        var found = False
        for _ in range(n):
            var t = self._w_tcb.popleft()
            var i = self._w_id.popleft()
            if (not found) and t == target and i == tid:
                found = True
                continue
            kept_tcb.append(t)
            kept_id.append(i)
        self._w_tcb = kept_tcb^
        self._w_id = kept_id^
        return found

    def is_granted[R: ResultValue](self, h: JoinHandle[R]) -> Bool:
        """Diagnostics: does `h` carry an outstanding GRANT marker?"""
        return h.tcb()[].wait_node()[].next() == RW_GRANTED


# ---------------------------------------------------------------------------
# ReadGuard / WriteGuard
# ---------------------------------------------------------------------------

struct ReadGuard[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](Movable):
    """RAII-style handle to an acquired READ grant: exposes read-only-by-
    convention access to the protected value and releases the shared hold.
    In the cooperative single-worker model release is always explicit (the
    body runs at a dispatcher boundary), so `release()` performs the
    reader-count decrement + writer handoff (mirrors MutexGuard)."""

    var _rw: UnsafePointer[RWLock[Self.T], MutAnyOrigin]

    def __init__(out self, rw: UnsafePointer[RWLock[Self.T], MutAnyOrigin]):
        self._rw = rw

    def value(mut self) -> UnsafePointer[Self.T, MutAnyOrigin]:
        return self._rw[].value()

    def release[R: ResultValue](mut self, mut rt: Runtime) raises -> Bool:
        return self._rw[].unlock_read[R](rt)


struct WriteGuard[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](Movable):
    """RAII-style handle to an acquired WRITE grant: exposes mutable access
    to the protected value and releases exclusive ownership.  `release()`
    hands off to a waiting writer (preference) or drains every waiting
    reader (mirrors MutexGuard)."""

    var _rw: UnsafePointer[RWLock[Self.T], MutAnyOrigin]

    def __init__(out self, rw: UnsafePointer[RWLock[Self.T], MutAnyOrigin]):
        self._rw = rw

    def value(mut self) -> UnsafePointer[Self.T, MutAnyOrigin]:
        return self._rw[].value()

    def release[R: ResultValue](mut self, mut rt: Runtime) raises -> Int:
        return self._rw[].unlock_write[R](rt)
