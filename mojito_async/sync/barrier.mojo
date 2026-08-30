# mojito_async/sync/barrier.mojo
#
# A4.5 (issue #59) — task-aware `Barrier` with FIFO gate release, layered
# over sync/condvar.mojo's shared winner-claim + FIFO-division helpers
# (issue #59 dependency: "Condvar (sibling) provides the wait/notify
# mechanics reused here; otherwise barrier duplicates them").  A Barrier
# gates N tasks at one rendezvous point: each `wait` counts an arrival for
# the CURRENT phase; the Nth arrival releases every waiter of that phase
# EXACTLY ONCE (spec Phase A5) and the barrier resets for the next phase —
# a cyclic barrier, reusable across many phases.
#
# Winner protocol: reuses condvar.mojo's WINNER_READY/CANCELLED/TIMEOUT
# marker convention and `resolve_winner`/`notify_marker` module functions
# (same C6-style exactly-one-winner claim on the WaitNode's `_next` field)
# — see condvar.mojo's header for why this is a self-contained claim rather
# than a dependency on the not-yet-landed C3/C6 park-kernel sub-issues.
# cancel_waiter/timeout_waiter inline the FIFO-preserving single-waiter
# removal (formerly `release_waiter`) under Barrier._guard (issue #148).
#
# Cancellation/timeout consistency (issue #59 acceptance): a waiter removed
# via cancel_waiter/timeout_waiter must NOT corrupt the phase for the
# others.  A parked waiter's `wait()` already counted its own arrival
# against `_target` (the current phase's working target, which starts at
# `_base_target` each phase); cancelling it undoes BOTH its counted arrival
# (`_count -= 1`) AND the phase's expectation that it will ever arrive
# (`_target -= 1`) — the "remaining arrivals needed" (`_target - _count`)
# is therefore UNCHANGED by a cancel, so the other waiters still meet the
# (now smaller) target with no partial and no duplicate release.  The next
# phase (after any release) always starts fresh at `_base_target` — "a
# later phase gate requires everyone again" (issue #59 acceptance).
#
# Cross-worker safety (issue #148): the A1 cooperative single-worker model
# made publish+park atomic within a dispatcher slice (no lost wakeups).
# The A2 M:N scheduler uses REAL OS threads: two arrivals on two workers
# mutate the same _w_tcb/_w_id Deques concurrently (data race + lost
# wakeup).  The fix: a SpinLock guard on all _count/_target/_phase/_w_tcb/
# _w_id access, and the two-phase park_prepare/park_validate/park_commit
# kernel (NOT the single-phase park_current) for the PARKING-window race
# (mirrors Mutex #55 / RWLock #122 / Channel #128 / #137).
#
# Mojo 1.0.0b2 (def-only) discipline matches mutex/semaphore/condvar: `def`
# only; generic methods parameterized over the caller's ResultValue R;
# module-level factory `_br_waiter_handle`... no — Barrier reuses
# condvar.mojo's `notify_marker`/`resolve_winner` surface directly; the
# wait FIFO is a parallel Deque[Int] of (tcb_addr, task_id), no per-
# suspension allocation on the fast path.
from std.collections import Deque
from mojito_async.cancellation import CancellationError
from mojito_async.runtime.queue import SpinLock
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle, claim_running
from mojito_async.runtime.park import park_commit, park_prepare, park_validate
from mojito_async.sync.condvar import (
    PHASE_INIT,
    PHASE_PARKED,
    WINNER_CANCELLED,
    WINNER_READY,
    WINNER_TIMEOUT,
    notify_marker,
    resolve_winner,
)


# ---------------------------------------------------------------------------
# Barrier
# ---------------------------------------------------------------------------

struct Barrier:
    """Cyclic task-aware barrier (spec Phase A5): N tasks rendezvous per
    phase; the Nth `wait` releases every waiter of that phase exactly once
    and the barrier resets for the next phase.

    State:
      _guard       — SpinLock serializing every read/write of `_count`,
                      `_target`, `_phase`, and the waiter FIFO: on the A2
                      M:N scheduler two REAL worker OS threads calling wait()
                      concurrently mutate the same Deque storage, so an
                      unguarded append and popleft on the same two deques
                      from two threads is memory corruption (issue #148).
                      Same SpinLock pattern as Mutex/RWLock (A4.1, #55/#122).
      _base_target — the original N this Barrier was constructed with; every
                      FRESH phase's working target starts here.
      _target      — the CURRENT phase's working target; a cancel/timeout
                      shrinks it (see module header) without disturbing the
                      "remaining arrivals needed" invariant.
      _count       — arrivals counted so far in the current phase.
      _phase       — monotonic phase/epoch counter, bumped on every release
                      (and by `reset`); observable so callers can assert a
                      later phase actually required a fresh N arrivals.
      _w_tcb/_w_id — FIFO of parked waiters (tcb_addr, task id) — reused
                      verbatim shape from condvar.mojo's own wait FIFO.
    """
    var _guard: SpinLock
    var _base_target: Int
    var _target: Int
    var _count: Int
    var _phase: Int
    var _w_tcb: Deque[Int]
    var _w_id: Deque[Int]

    def __init__(out self, target: Int):
        self._guard = SpinLock()
        self._base_target = target
        self._target = target
        self._count = 0
        self._phase = 0
        self._w_tcb = Deque[Int]()
        self._w_id = Deque[Int]()

    # --- queries -------------------------------------------------------------

    def phase(self) -> Int:
        """Monotonic phase/epoch counter; bumps on every release/reset."""
        return self._phase

    def target(self) -> Int:
        """The CURRENT phase's working target (may be < base_target after a
        cancel/timeout this phase; restored to base_target on release)."""
        return self._target

    def base_target(self) -> Int:
        return self._base_target

    def waiter_count(self) -> Int:
        return len(self._w_tcb)

    # --- wait (spec Phase A5) -------------------------------------------------

    def wait[R: ResultValue](
        mut self,
        mut rt: Runtime,
        h: JoinHandle[R],
        cause: UnsafePointer[Int, MutAnyOrigin],
    ) raises -> Bool:
        """Arrive at the barrier and wait for the rest of this phase.

        `cause` MUST be zero-initialized (PHASE_INIT) before the first call
        for a given wait cycle — same multi-step contract as
        Condvar.wait/Mutex.lock: the caller's dispatch loop re-invokes this
        after each park/wake edge.

        Returns True once settled — `cause[]` then holds WINNER_READY (this
        phase's target was reached; every waiter including the Nth arriver
        is released) or WINNER_TIMEOUT.  Returns False while still parked;
        re-invoke after the next wake.  Raises CancellationError-as-Error
        when a racing `cancel_waiter` claimed this waiter's marker first —
        the barrier itself stays healthy (see module header): the phase
        counter is decremented in lockstep so the other waiters still meet
        the (now smaller) target.

        Lost-wakeup safety (issue #148): the count-increment, FIFO append,
        and FIFO drain are ONE guarded critical section (Barrier._guard),
        so a releasing arrival cannot pop from the FIFO while another
        arrival is mid-append (data race / lost entr(y|ies) on the A2 M:N
        scheduler with TWO REAL worker OS threads).  The two-phase
        park_prepare/park_validate/park_commit kernel (NOT the single-phase
        park_current) then closes the lost-wakeup window between releasing
        the guard and committing to WAITING: a notifier that lands in that
        window sets the early-readiness latch, and park_validate re-checks
        it before park_commit decides WAITING vs. an immediate unwind."""
        if cause[] == PHASE_INIT:
            self._guard.lock()
            self._count += 1
            if self._count >= self._target:
                # Last arrival this phase: release every OTHER waiter FIFO,
                # reset for the next phase, and complete this call directly
                # (the Nth arriver never parks — spec Phase A5).
                self._count = 0
                self._target = self._base_target
                self._phase += 1
                cause[] = WINNER_READY
                var n = len(self._w_tcb)
                for _ in range(n):
                    var tcb = self._w_tcb.popleft()
                    var tid = self._w_id.popleft()
                    notify_marker[R](rt, tcb, tid, WINNER_READY)
                self._guard.unlock()
                return True
            self._w_tcb.append(Int(h.tcb()))
            self._w_id.append(h.id())
            cause[] = PHASE_PARKED
            self._guard.unlock()
            # Two-phase park (issue #148): NOT the single-phase park_current.
            # The guard is released first so the releasing arrival can drain
            # the FIFO; park_validate catches a notifier that lands between
            # unlock and the WAITING commit (the A0-T11 lost-wakeup window).
            park_prepare(h)
            if park_validate(h):
                _ = park_commit(h)
                claim_running(h)
                cause[] = resolve_winner[R](h)
                if cause[] == WINNER_CANCELLED:
                    raise Error(
                        CancellationError("CancellationError: Barrier.wait cancelled").message
                    )
                return True
            if not park_commit(h):
                claim_running(h)
                cause[] = resolve_winner[R](h)
                if cause[] == WINNER_CANCELLED:
                    raise Error(
                        CancellationError("CancellationError: Barrier.wait cancelled").message
                    )
                return True
            return False
        if cause[] == PHASE_PARKED:
            cause[] = resolve_winner[R](h)
        if cause[] == WINNER_CANCELLED:
            raise Error(
                CancellationError("CancellationError: Barrier.wait cancelled").message
            )
        return True

    # --- cancel / timeout (leaves the phase healthy for the others) --------

    def cancel_waiter[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Claim `h`'s marker as CANCELLED, remove it from this phase's
        FIFO, and shrink `_target` in lockstep with `_count` so the
        "remaining arrivals needed" invariant is unchanged for the other
        waiters (module header).  Idempotent: False when `h` already left
        the FIFO (a racing release/timeout won).

        The FIFO search + removal is ONE guarded critical section (issue
        #148): _w_tcb/_w_id are shared with wait()'s release path on
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
        if found:
            self._count -= 1
            self._target -= 1
        self._guard.unlock()
        return found

    def timeout_waiter[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Claim `h`'s marker as TIMEOUT, remove it from this phase's FIFO,
        and shrink `_target` in lockstep with `_count` (see cancel_waiter).
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
        if found:
            self._count -= 1
            self._target -= 1
        self._guard.unlock()
        return found

    # --- reset (explicit next-phase / abort-current-phase) -------------------

    def reset(mut self) raises:
        """Abort the CURRENT phase and start a fresh one at `base_target`,
        bumping `_phase`.  Refuses while any waiter is still parked (a
        silent reset would strand them un-woken): cancel outstanding
        waiters first if a hard reset is truly needed."""
        if len(self._w_tcb) != 0:
            raise Error(
                "Barrier.reset: "
                + String(len(self._w_tcb))
                + " waiter(s) still parked — cancel them first"
            )
        self._count = 0
        self._target = self._base_target
        self._phase += 1
