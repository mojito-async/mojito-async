# mojito_async/sync/condvar.mojo
#
# A4.6 (issue #60) — task-aware `Condvar` over the A1 park/wake kernel
# (spec §A5 "richer synchronization").  Composes with `Mutex[T]` (A1.2,
# issue #34): `wait` releases the caller's held lock to the mutex's own
# FIFO, parks the caller on the condvar's FIFO, and on wake re-acquires the
# lock through the mutex's proven WAITER_GRANTED handoff — no thundering
# herd, no double-lock.
#
# Winner protocol (spec §23-§25/§29.2, "C6" generation-owner rule): C3 (two-
# phase park kernel, issue #56) and C6 (generation-claim winner, issue #58)
# are sibling A4 sub-issues NOT YET landed on `runtime/park.mojo` as of this
# lane's branch point, so this module carries its OWN self-contained winner
# claim rather than depend on an unmerged sibling branch (see the shared
# lane context: draft PRs branch off `origin/main` and integrate later).
# The claim reuses the EXACT mechanism mutex.mojo/semaphore.mojo already
# use for their own GRANT markers — the embedded WaitNode's `_next` int
# field, consumed exactly once (`set_next(0)`), never concurrently written
# by two callers on the single cooperative worker (spec §24's allocation-
# free wait cell; publish+park is atomic within a dispatcher slice, park.py
# module header).  Exactly one of notify_one/notify_all/cancel_waiter/
# timeout_waiter ever stamps a given epoch's marker, so exactly one winner
# reaches `wait`'s resume branch (mirrors the C6 matrix the module docstring
# above describes without needing the shared kernel plumbing yet).
#
# Public winner causes (`resolve_winner`'s return / `wait`'s cause cell):
#   WINNER_READY      — notify_one/notify_all signalled this waiter.
#   WINNER_CANCELLED  — `cancel_waiter` claimed it first; `wait` re-acquires
#                        the lock, THEN raises CancellationError (the caller
#                        already holds the lock again to unwind through, per
#                        the issue's "re-acquire the mutex, and surface the
#                        winner cause exactly once").
#   WINNER_TIMEOUT    — `timeout_waiter` claimed it first (a caller-armed
#                        deadline, e.g. via time/timer_heap.mojo, fired and
#                        the driver called timeout_waiter); `wait` returns
#                        True with the cause cell holding WINNER_TIMEOUT —
#                        no raise (a timeout is not automatically an error).
#
# Lost-signal safety (issue #60 "signal-with-no-waiter-is-lossless"):
# notify_one/notify_all on an EMPTY wait FIFO are a documented no-op (return
# False / 0) — the signal is dropped, never queued for a future waiter.
# The standard condvar predicate-recheck protocol is the caller's
# responsibility (`while not pred(): cv.wait(...)`), matching every other
# primitive in this package (the primitive parks; predicate loops live at
# the call site, spec A5/condvar semantics).
#
# FIFO ordering: notify_one always wakes the LONGEST-waiting task (Deque
# popleft, spec's arrival order); notify_all wakes every CURRENTLY waiting
# task, one enqueue each (no duplicates — each waiter is popped exactly
# once off the FIFO before being stamped+woken).
#
# Mojo 1.0.0b2 (def-only) discipline matches mutex.mojo/semaphore.mojo:
# `def` only; generic methods parameterized over the caller's ResultValue
# R; module-level factories/helpers (no static methods); the FIFO wait set
# is parallel Deque[Int] of (tcb_addr, task_id) — no per-suspension
# allocation on the fast path.  `resolve_winner`/`release_waiter` are
# MODULE-LEVEL so sync/barrier.mojo (issue #59) can reuse the identical
# FIFO-division + winner-claim mechanics instead of re-deriving them (issue
# #59's explicit ask: "reuse the SAME Condvar wait/notify-all path").
from std.collections import Deque
from mojito_async.cancellation import CancellationError
from mojito_async.runtime.queue import SpinLock
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.task import JoinHandle, claim_running
from mojito_async.runtime.park import park_commit, park_prepare, park_validate, unpark_current
from mojito_async.sync.mutex import Mutex


# ---------------------------------------------------------------------------
# Winner causes (shared with sync/barrier.mojo)
# ---------------------------------------------------------------------------

comptime WINNER_READY = Int(1)
comptime WINNER_CANCELLED = Int(2)
comptime WINNER_TIMEOUT = Int(3)

# `wait`'s caller-owned cause/phase cell (an UnsafePointer[Int] the caller
# zero-initializes before the FIRST wait() call and threads through every
# re-entry, exactly like the SceneA/SceneB phase cells in t21_mutex.mojo /
# t22_semaphore.mojo): 0 = not started; -1 = parked, awaiting a winner
# marker; a WINNER_* value = resolved, (re)acquiring the lock.
comptime PHASE_INIT = Int(0)
comptime PHASE_PARKED = Int(-1)


def _cv_waiter_handle[R: ResultValue](tcb_addr: Int, tid: Int) -> JoinHandle[R]:
    """Reconstruct a queued waiter's one-shot handle from (addr, id)."""
    return JoinHandle[R](
        UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=tcb_addr
        ),
        tid,
    )


# ---------------------------------------------------------------------------
# Shared FIFO winner-claim helpers (reused by Barrier, issue #59)
# ---------------------------------------------------------------------------

def resolve_winner[R: ResultValue](h: JoinHandle[R]) raises -> Int:
    """Consume-once: read + clear the resumed waiter's WaitNode marker.

    Raises when the marker is 0 (a resume with no stamped winner is a lost
    wakeup — the loud-failure convention this codebase uses elsewhere for
    an impossible state, e.g. park.mojo's `unpark_current` illegal-
    transition raise)."""
    var marker = h.tcb()[].wait_node()[].next()
    if marker == 0:
        raise Error(
            "Condvar: resumed with no winner marker stamped (lost wakeup)"
        )
    h.tcb()[].wait_node()[].set_next(0)
    return marker


def notify_marker[R: ResultValue](
    mut rt: Runtime, tcb_addr: Int, tid: Int, reason: Int
) raises:
    """Stamp `reason` on one waiter's WaitNode and deliver readiness once
    (park.mojo's `unpark_current` enqueue-once contract)."""
    var hw = _cv_waiter_handle[R](tcb_addr, tid)
    hw.tcb()[].wait_node()[].set_next(reason)
    unpark_current(rt, hw)


def release_waiter[R: ResultValue](
    mut w_tcb: Deque[Int],
    mut w_id: Deque[Int],
    mut rt: Runtime,
    h: JoinHandle[R],
    reason: Int,
) raises -> Bool:
    """FIFO-preserving removal of ONE specific waiter (by task id) from a
    Deque-of-(tcb_addr, task_id) wait set: pop every entry once, re-append
    every non-matching one in its original relative order, and stamp+wake
    the match (first occurrence only — task ids are unique per waiter).
    Returns True when `h` was found and released; False when it was not
    queued (already resolved by a racing notify/cancel/timeout — the C6
    "exactly one winner" property falls out naturally: a second release
    attempt on an already-departed waiter is a pure no-op, never a double
    wake)."""
    var n = len(w_tcb)
    var found = False
    for _ in range(n):
        var tcb = w_tcb.popleft()
        var tid = w_id.popleft()
        if tid == h.id() and not found:
            found = True
            notify_marker[R](rt, tcb, tid, reason)
        else:
            w_tcb.append(tcb)
            w_id.append(tid)
    return found


# ---------------------------------------------------------------------------
# Condvar
# ---------------------------------------------------------------------------

struct Condvar:
    """Task-aware condition variable (spec §A5).

    State:
      _guard   — SpinLock serializing the waiter FIFO: on the A2 M:N
                  scheduler a concurrent notify_one/notify_all can pop from
                  the same Deque storage that wait()'s PHASE_INIT branch is
                  appending to, and a producer holding the mutex can call
                  notify_one on an empty FIFO while the consumer is between
                  mutex.unlock() and the append — both are data races and
                  lost wakeups respectively (issue #148).  The guard is
                  acquired BEFORE mutex.unlock() in wait() so that a
                  notifier that arrives after the unlock is serialized
                  AFTER the append — no lost notify.  Same SpinLock pattern
                  as Mutex/RWLock/Barrier (A4.1, #55/#122/#148).
      _w_tcb/_w_id — FIFO of parked waiters (tcb_addr, task id),
                  identical shape to Mutex/Semaphore's own wait deques.

    `wait` is a multi-step contract like Mutex.lock/Semaphore.acquire: the
    caller's dispatch loop re-invokes it after each park/wake edge, threading
    a caller-owned `cause` cell (zero-initialized before the first call) that
    both tracks wait()'s internal phase AND, once resolved, carries the
    winner cause the caller reads back.
    """

    var _guard: SpinLock
    var _w_tcb: Deque[Int]
    var _w_id: Deque[Int]

    def __init__(out self):
        self._guard = SpinLock()
        self._w_tcb = Deque[Int]()
        self._w_id = Deque[Int]()

    def waiter_count(self) -> Int:
        return len(self._w_tcb)

    # --- wait (spec §A5: releases the lock, parks, re-acquires on wake) ----

    def wait[
        R: ResultValue,
        L: Movable & ImplicitlyCopyable & ImplicitlyDeletable,
    ](
        mut self,
        mut rt: Runtime,
        mut lock: Mutex[L],
        h: JoinHandle[R],
        cause: UnsafePointer[Int, MutAnyOrigin],
    ) raises -> Bool:
        """Wait on this condvar while composing with `lock` (held by the
        caller on entry).  `cause` MUST be zero-initialized (PHASE_INIT)
        before the first call for a given wait cycle.

        Returns True once the lock is fully re-acquired and the wait is
        settled — `cause[]` then holds WINNER_READY or WINNER_TIMEOUT.
        Returns False while still pending (parked on the condvar OR
        re-parked on the mutex's own FIFO); the caller's dispatch loop must
        re-invoke wait() after the next park/wake edge.  Raises
        CancellationError-as-Error (AFTER re-acquiring the lock) when a
        racing `cancel_waiter` claimed the winner marker first.

        Lost-wakeup safety (issue #148): _guard is acquired BEFORE
        mutex.unlock() so a producer that acquires the mutex and calls
        notify_one AFTER our unlock cannot find an empty FIFO — it blocks
        on _guard until we have appended, then delivers the wake.  The
        two-phase park_prepare/park_validate/park_commit kernel (NOT the
        single-phase park_current) then closes the PARKING-window race:
        a notifier that arrives after _guard.unlock() but before park_commit
        sets the early-readiness latch; park_validate re-checks it before
        park_commit decides WAITING vs. an immediate unwind (A0-T11)."""
        if cause[] == PHASE_INIT:
            # Acquire condvar _guard BEFORE mutex.unlock() — a producer
            # holding the mutex and then calling notify_one must block on
            # _guard until we have appended to the FIFO; otherwise it sees
            # an empty FIFO between our unlock and our append (lost notify).
            self._guard.lock()
            _ = lock.unlock[R](rt)
            self._w_tcb.append(Int(h.tcb()))
            self._w_id.append(h.id())
            cause[] = PHASE_PARKED
            self._guard.unlock()
            # Two-phase park (issue #148): NOT the single-phase park_current.
            park_prepare(h)
            if park_validate(h):
                _ = park_commit(h)
                claim_running(h)
                cause[] = resolve_winner[R](h)
                var got = lock.lock[R](rt, h)
                if not got:
                    return False
                if cause[] == WINNER_CANCELLED:
                    raise Error(CancellationError("CancellationError: Condvar.wait cancelled").message)
                return True
            if not park_commit(h):
                claim_running(h)
                cause[] = resolve_winner[R](h)
                var got = lock.lock[R](rt, h)
                if not got:
                    return False
                if cause[] == WINNER_CANCELLED:
                    raise Error(CancellationError("CancellationError: Condvar.wait cancelled").message)
                return True
            return False
        if cause[] == PHASE_PARKED:
            # Resumed: exactly one of notify_one/notify_all/cancel_waiter/
            # timeout_waiter stamped the winner marker before waking us.
            cause[] = resolve_winner[R](h)
        # cause[] now holds the resolved winner (READY/CANCELLED/TIMEOUT):
        # (re)acquire the lock — reuses Mutex's own WAITER_GRANTED handoff,
        # so a mutex-side park here is transparent to this state machine
        # (Mutex.lock consumes ITS OWN marker on the matching re-entry).
        var got = lock.lock[R](rt, h)
        if not got:
            return False
        if cause[] == WINNER_CANCELLED:
            raise Error(CancellationError("CancellationError: Condvar.wait cancelled").message)
        return True

    # --- notify (spec §A5: FIFO release) ------------------------------------

    def notify_one[R: ResultValue](mut self, mut rt: Runtime) raises -> Bool:
        """Wake the LONGEST-waiting task.  A signal with no waiters is
        dropped safely (returns False) — lossless per the predicate-recheck
        protocol documented above the FIFO.

        Guarded by _guard (issue #148): notify_one and wait()'s PHASE_INIT
        append are ONE serialized pair — a notify on an empty FIFO while a
        concurrent append is in-flight would silently drop the signal."""
        self._guard.lock()
        if len(self._w_tcb) == 0:
            self._guard.unlock()
            return False
        var tcb = self._w_tcb.popleft()
        var tid = self._w_id.popleft()
        self._guard.unlock()
        notify_marker[R](rt, tcb, tid, WINNER_READY)
        return True

    def notify_all[R: ResultValue](mut self, mut rt: Runtime) raises -> Int:
        """Wake EVERY currently-waiting task, one enqueue each (no
        duplicates — the FIFO drains to empty).  Returns the count woken."""
        var n = 0
        while self.notify_one[R](rt):
            n += 1
        return n

    # --- cancel / timeout (C6 winner: token/deadline exits) ----------------

    def cancel_waiter[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Claim `h`'s winner marker as CANCELLED and remove it from the
        FIFO.  Idempotent: False when `h` already left the FIFO (a racing
        notify/timeout won).

        The FIFO search + removal is ONE guarded critical section (issue
        #148): _w_tcb/_w_id are shared with notify_one/notify_all on
        another worker."""
        self._guard.lock()
        var n = len(self._w_tcb)
        var found = False
        for _ in range(n):
            var tcb = self._w_tcb.popleft()
            var tid = self._w_id.popleft()
            if tid == h.id() and not found:
                found = True
                notify_marker[R](rt, tcb, tid, WINNER_CANCELLED)
            else:
                self._w_tcb.append(tcb)
                self._w_id.append(tid)
        self._guard.unlock()
        return found

    def timeout_waiter[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Claim `h`'s winner marker as TIMEOUT and remove it from the FIFO.
        Idempotent: False when `h` already left the FIFO.

        Guarded by _guard (issue #148) — mirrors cancel_waiter."""
        self._guard.lock()
        var n = len(self._w_tcb)
        var found = False
        for _ in range(n):
            var tcb = self._w_tcb.popleft()
            var tid = self._w_id.popleft()
            if tid == h.id() and not found:
                found = True
                notify_marker[R](rt, tcb, tid, WINNER_TIMEOUT)
            else:
                self._w_tcb.append(tcb)
                self._w_id.append(tid)
        self._guard.unlock()
        return found
