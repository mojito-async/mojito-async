# mojito_async/sync/semaphore.mojo
#
# A1.2 (issue #34) — task-aware `Semaphore` + `Permit`, over the A1.1
# park/wake path (spec §36).  Parks the CURRENT task when permits are
# exhausted instead of blocking the worker.  Composes with the A1.1
# cooperative scheduler exactly like Mutex: `acquire` is a dispatcher-level
# operation (fast path returns immediately; on exhaustion it publishes the
# waiter, parks via `park_current`, and is re-entered on resume with a
# GRANT marker that a `release` set).
#
# Fairness (documented, spec §36): STRICT FIFO.  A waiter never overtakes an
# earlier one: `acquire` takes the fast path ONLY when the wait queue is
# empty, and `release` grants the head waiter whenever a new batch fits; a
# head that cannot yet be satisfied keeps the FIFO held so later waiters do
# not starve it.  In the cooperative single-worker there is no OS-thread
# priority inversion to consider.
#
# Lost-wakeup safety mirrors mutex.mojo: publish+park on the slow path is
# atomic within a worker slice, so a `release` always finds its head waiter
# already parked, and the A1.1 `resume_current_direct` delivers readiness
# exactly once per epoch.
#
# Mojo 1.0.0b2 (def-only): `def` only; generic methods parameterized over the
# caller's ResultValue R; module-level factories; slow-path FIFO is a Deque
# of parallel addr/id/n with no fast-path allocation.
from std.collections import Deque
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.task import JoinHandle
from mojito_async.runtime.park import park_current, unpark_current, raise_if_cancel_wake, wake_cancelled
from mojito_async.cancellation import CancellationToken


comptime PERMIT_GRANTED = Int(1)


def _perm_waiter_handle[R: ResultValue](
    tcb_addr: Int, tid: Int
) -> JoinHandle[R]:
    return JoinHandle[R](
        UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=tcb_addr
        ),
        tid,
    )


def _remove_at_index(mut q: Deque[Int], idx: Int) raises:
    """Remove the element at `idx` (0-based from the front), preserving the
    relative FIFO order of every OTHER element (issue #57's cancel_acquire_
    wait: a cancelled waiter may sit anywhere in the queue).  O(n) rotate —
    Deque has no native middle-removal (mirrors sync/mutex.mojo's helper)."""
    var n = len(q)
    for i in range(n):
        var v = q.popleft()
        if i != idx:
            q.append(v)


# ---------------------------------------------------------------------------
# Semaphore
# ---------------------------------------------------------------------------

struct Semaphore(Movable):
    """Counting semaphore with FIFO wakeup (spec §36).

    State:
      _permits — available permits (>= 0).
      _w_tcb/_w_id/_w_n — FIFO of parked waiters: addr, task id, requested n.

    RAII: `acquire` returns a bearer handle; `Permit.release(rt)` returns the
    permits and wakes waiters FIFO.
    """

    var _permits: Int
    var _w_tcb: Deque[Int]
    var _w_id: Deque[Int]
    var _w_n: Deque[Int]

    def __init__(out self, permits: Int):
        self._permits = permits
        self._w_tcb = Deque[Int]()
        self._w_id = Deque[Int]()
        self._w_n = Deque[Int]()

    def available(self) -> Int:
        return self._permits

    def waiter_count(self) -> Int:
        return len(self._w_tcb)

    # --- fast path ----------------------------------------------------------

    def try_acquire(mut self, n: Int = 1) -> Bool:
        """Attempt a non-parking acquire of `n` permits.

        Succeeds only when NO waiter is queued (FIFO fairness: a new caller
        never overtakes an earlier waiter) and `n` permits are available.
        """
        if len(self._w_tcb) != 0:
            return False
        if self._permits < n:
            return False
        self._permits -= n
        return True

    def acquire[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], n: Int = 1
    ) raises -> Bool:
        """Task-aware acquire of `n` permits (spec §36).  True => got them.

        Fast path (no waiters ahead, enough permits) returns immediately.
        Otherwise the caller is enqueued as a FIFO waiter and parked; a later
        release grants it and re-dispatches it, at which point the GRANT
        marker makes this re-entry acquire without re-checking the queue.
        """
        if h.tcb()[].wait_node()[].next() == PERMIT_GRANTED:
            h.tcb()[].wait_node()[].set_next(0)
            return True
        if self.try_acquire(n):
            return True
        self._w_tcb.append(Int(h.tcb()))
        self._w_id.append(h.id())
        self._w_n.append(n)
        park_current(rt, h)
        return False

    def release[R: ResultValue](mut self, mut rt: Runtime, n: Int = 1) raises -> Bool:
        """Return `n` permits and, FIFO, grant the head waiter if it now fits.

        Each call wakes at most one waiter; a head that still cannot fit keeps
        the FIFO held (a later waiter never jumps) so N calls drain the queue
        in arrival order.  Returns True when a waiter was granted+woken.
        """
        self._permits += n
        if len(self._w_tcb) != 0:
            var need = self._w_n[0]
            if self._permits >= need:
                var tcb = self._w_tcb.popleft()
                var tid = self._w_id.popleft()
                _ = self._w_n.popleft()
                self._permits -= need
                var hw = _perm_waiter_handle[R](tcb, tid)
                hw.tcb()[].wait_node()[].set_next(PERMIT_GRANTED)
                unpark_current(rt, hw)
                return True
        return False

    def is_granted[R: ResultValue](self, h: JoinHandle[R]) -> Bool:
        return h.tcb()[].wait_node()[].next() == PERMIT_GRANTED

    # --- token-aware acquire (A4.3, issue #57) ------------------------------

    def acquire_cancellable[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], token: CancellationToken, n: Int = 1
    ) raises -> Bool:
        """Token-aware acquire.  Identical to `acquire()` on every readiness
        path (fast path, GRANT-marker re-entry, contended park); the ONLY
        addition is the C6 winner check this waiter's own resume carries:
        raises ONLY when THIS waiter's `cancel_acquire_wait` won the race
        (never when readiness/GRANT won — the semaphore is left exactly as
        `acquire()` would leave it, no permit leaked)."""
        raise_if_cancel_wake(h)
        if token.is_cancellation_requested():
            raise Error("CancellationError: semaphore acquire cancelled")
        return self.acquire(rt, h, n)

    def cancel_acquire_wait[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Cancel a parked `acquire_cancellable` waiter (issue #57): removes
        `h` from the three parallel FIFO queues (addr/id/n) — preserving the
        ORDER of every OTHER queued waiter — and delivers a CANCEL wake.
        Returns True iff THIS call found + removed + woke the waiter; False
        when the id was never queued or a concurrent `release` already
        popped it (readiness won — no ghost entry, no permit double-count)."""
        var idx = -1
        for i in range(len(self._w_id)):
            if self._w_id[i] == h.id():
                idx = i
                break
        if idx == -1:
            return False
        _remove_at_index(self._w_tcb, idx)
        _remove_at_index(self._w_id, idx)
        _remove_at_index(self._w_n, idx)
        wake_cancelled(rt, h)
        return True


# ---------------------------------------------------------------------------
# Permit
# ---------------------------------------------------------------------------

struct Permit(Movable):
    """RAII-ish handle to an acquired semaphore slot (spec §36).

    `release(rt)` returns the held permit and (re)wakes FIFO waiters.
    """

    var _sem: UnsafePointer[Semaphore, MutAnyOrigin]
    var _n: Int

    def __init__(
        out self, s: UnsafePointer[Semaphore, MutAnyOrigin], n: Int = 1
    ):
        self._sem = s
        self._n = n

    def release[R: ResultValue](mut self, mut rt: Runtime) raises -> Bool:
        return self._sem[].release[R](rt, self._n)