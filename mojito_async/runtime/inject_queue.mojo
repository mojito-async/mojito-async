# mojito_async/runtime/inject_queue.mojo
#
# A2.3 (issue #69) — the global injection queue with bounded backpressure.
#
# The MPSC intake for tasks submitted OUTSIDE any worker's local deque:
# external spawn entries, cross-worker/foreign enqueues, reactor/timer
# submissions.  Spec §18 topology / §21 scheduler loop: every worker drains
# the shared injection queue between its local and remote-ready deques, so
# global work is dequeued identically on every worker — WITHOUT a global lock
# on the per-worker local hot path (worker-local enqueues never take this
# lock; acceptance "no global lock on the local hot path").
#
# P0 (issue step 2) — "a bounded ring guarded by NativeMutex (MPSC under one
# lock), correct first, replace after scheduler semantics stabilize (same
# rule as §20's deque progression)".  The locking primitive here is a
# cross-OS-thread spinlock over std.atomic (seq_cst CAS on a word inside the
# struct — extern-free, JIT-safe): every push/pop/wake-claim is a short
# critical section, so the spin is never held long enough for a worker to
# "block on injection" (ADR-009: workers never block on injection; a full
# injection queue must not wedge a worker — pop is non-blocking, and a
# producer that hits capacity gets try_push=False / a clear full error and
# retries on its next loop iteration, spec §86 backpressure).
#
# Dequeue fairness (issue step 5): workers poll injection in a BOUNDED manner
# each loop slice (see scheduler_loop's `inject_budget`) so one busy worker
# cannot starve global intake.
#
# Producer-side wake contract (issue step 6, PARKING-LOT-ADAPTER): a wake
# from another thread must reach the right queue exactly once per epoch
# (claim-once-per-epoch, spec §25).  `push_wake` below restores that calling
# convention against the spike event.mojo PREPARE/VALIDATE/COMMIT phases,
# reusing park.mojo's `unpark_current` claim-once discipline
# (TaskControlBlock.wake_claim + enqueue-once):
#
#   PREPARE  — the parked task's WAITING commit (park_current, park.mojo)
#              published the embedded WaitNode + fresh epoch generation; that
#              is the producer's claim target (established, not re-derived).
#   VALIDATE — this lane's job, UNDER THE INJECTION LOCK: re-check the
#              waiter is still WAITING at the captured generation; a stale
#              or duplicate wake (generation already consumed / state moved
#              on) is REJECTED with no enqueue (wake_claim returns False).
#   COMMIT   — claim-once succeeded => enqueue the wake record exactly once.
#
# Seams named for the follow-on lanes:
#   - E2 (#68) consumes the enqueue_local branch of Runtime.enqueue_global
#     (see runtime.mojo) with per-worker deques.
#   - E5 (started-fiber affinity) owns the full two-phase PROMOTION and the
#     owner-affinity wake ROUTING: an owner-affinity wake for a STARTED task
#     must reach the owner worker's REMOTE-READY queue — NOT this queue
#     (injection is for UNSTARTED tasks, stealable per §19.1; started wakes
#     are NOT theft-eligible).  `push_wake` is the producer-side contract the
#     E5 lane retargets; the claim-once guard is shared.
#   - E7 sets the fairness budget; this lane only guarantees the bounded poll
#     drains the queue across workers.
#
# Mojo 1.0.0b2 (def-only) constraints honored: `def` only; no function-typed
# fields (backpressure is a count/capacity, not a callback); no module-level
# mutable globals (the ring lives in the struct); externs stay in leaves —
# this module is EXTERN-FREE (the lock is std.atomic, no pthread), so the
# JIT unit drivers keep linking without the dylib (#6971).  Generic
# `push_wake` resolves at the concrete call site (module scope), never inside
# a generic struct method (the b2 safety rule verified during A1.2).
from std.atomic import Atomic
from std.collections import Deque

from mojito_async.runtime.queue import TaskRecord
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock


# ---------------------------------------------------------------------------
# InjectQueue — bounded MPSC ring (one lock), producers any OS thread,
# consumers all workers.
# ---------------------------------------------------------------------------

struct InjectQueue:
    """Shared bounded MPSC injection queue (spec §18/§21, issue #69).

    State:
      _lock     — cross-OS-thread spin word (0 free / 1 held).  Every
                  operation is a short critical section; workers never block
                  here (ADR-009) and LOCAL enqueues never touch it.
      _cap      — hard bound (spec §86: limits for externally submitted
                  unbounded work).
      _data     — the bounded ring (std Deque ring buffer); under `_lock`.
      _attempts/_rejected — observable backpressure ledger: every push/
                  try_push attempt counts; a capacity rejection counts.

    API:
      push(record)      — any thread; RAISES a clear full error at capacity
                          (never blocks — the caller retries next sweep).
      try_push(record)  — any thread; False at capacity (backpressure).
      pop()             — any worker; non-blocking; None when empty.
      capacity()        — the bound.
      pending()         — records currently queued (observability; locked).
      rejected()        — capacity rejections observed (backpressure driver).
    """

    var _lock: Int64
    var _cap: Int
    var _data: Deque[TaskRecord]
    var _attempts: Int
    var _rejected: Int
    # Accepted (enqueued) records — counted UNDER the queue's own lock, so
    # external producer threads can race safely without touching any Runtime
    # field (b2: atomic RMW on a Runtime field inside the fiber-crossing
    # enqueue path miscompiles the A1 fiber drive, verified vs t26; the
    # queue's lock is already held by push, so the count is free here).
    var _accepted: Int

    def __init__(out self, capacity: Int):
        self._lock = 0
        self._cap = capacity
        self._data = Deque[TaskRecord]()
        self._attempts = 0
        self._rejected = 0
        self._accepted = 0

    # --- locking ------------------------------------------------------------

    def _lock_acquire(mut self):
        var p = UnsafePointer[Int64, MutAnyOrigin](to=self._lock)
        var expected: Int64 = 0
        while not Atomic.compare_exchange(p, expected, 1):
            expected = 0

    def _lock_release(mut self):
        var p = UnsafePointer[Int64, MutAnyOrigin](to=self._lock)
        _ = Atomic.store(p, 0)

    # --- queries ------------------------------------------------------------

    def capacity(self) -> Int:
        return self._cap

    def pending(mut self) -> Int:
        """Records currently queued (all workers may consume them)."""
        self._lock_acquire()
        var n = len(self._data)
        self._lock_release()
        return n

    def rejected(self) -> Int:
        """Capacity rejections seen so far (backpressure evidence)."""
        return self._rejected

    def accepted(self) -> Int:
        """Records ACCEPTED since construction (counted under this queue's
        own lock — the cross-thread-safe enqueue accounting; Runtime.enqueued
        folds it in)."""
        return self._accepted

    def attempts(self) -> Int:
        return self._attempts

    # --- producer side (any OS thread) --------------------------------------

    def push(mut self, rec: TaskRecord) raises:
        """Enqueue one runnable record.  RAISES a clear full error at
        capacity instead of blocking any worker (ADR-009): the caller (the
        scheduler's external-spawn intake) retries on its next loop
        iteration.  Push is a short critical section over the ring (P0: MPSC
        under one lock — correct first, §20 queue progression); during the
        hold no worker can be wedged: the critical section is bounded, and a
        full queue fails the push instead of waiting."""
        self._lock_acquire()
        self._attempts += 1
        if len(self._data) >= self._cap:
            self._rejected += 1
            self._lock_release()
            raise Error(
                "InjectQueue.push: queue full (capacity " + String(self._cap)
                + ") — apply backpressure and retry on the next sweep"
            )
        self._data.append(rec)
        self._accepted += 1
        self._lock_release()

    def try_push(mut self, rec: TaskRecord) -> Bool:
        """Backpressure probe (spec §86): True when the record was accepted;
        False at capacity — nothing enqueued, nothing raised, the producer
        applies backpressure and retries on the next sweep."""
        self._lock_acquire()
        self._attempts += 1
        if len(self._data) >= self._cap:
            self._rejected += 1
            self._lock_release()
            return False
        self._data.append(rec)
        self._accepted += 1
        self._lock_release()
        return True

    # --- consumer side (any worker) -----------------------------------------

    def try_pop(mut self, mut out: TaskRecord) -> Bool:
        """Non-blocking dequeue (FIFO): True when `out` was filled with the
        head record; False when the queue is empty (`out` untouched).  This
        by-ref form is the b2-safe spelling used by the scheduler's bounded
        poll (no Optional through generic loops — the b2 compiler
        miscompiles Optional[TaskRecord] truthiness in generics, verified by
        probe).  Non-blocking: a FULL injection queue never wedges a worker
        (ADR-009) — pop keeps draining and the worker services its local/
        remote deques, retrying injection next loop iteration (issue 3)."""
        self._lock_acquire()
        if len(self._data) == 0:
            self._lock_release()
            return False
        # Non-empty under the lock => popleft cannot fail; b2 requires the
        # raising call be guarded.
        var rec = TaskRecord(0, 0)
        try:
            rec = self._data.popleft()
        except Error:
            rec = TaskRecord(0, 0)
        self._lock_release()
        out = rec
        return True

    def pop(mut self) -> Optional[TaskRecord]:
        """Dequeue one record (FIFO); None when empty.  Convenience wrapper
        over `try_pop` for non-generic callers (the issue's public spelling);
        the scheduler poll uses the by-ref `try_pop` (b2-safe)."""
        var rec = TaskRecord(0, 0)
        if self.try_pop(rec):
            return Optional[TaskRecord](rec)
        return Optional[TaskRecord]()


# ---------------------------------------------------------------------------
# push_wake — producer-side PARKING-LOT-ADAPTER wake leg (issue #69 step 6)
# ---------------------------------------------------------------------------

def push_wake[R: ResultValue](
    mut q: InjectQueue,
    tcb_addr: Int,
    task_id: Int,
    required_gen: Int,
) raises -> Bool:
    """Deliver a wake for a parked task EXACTLY ONCE per epoch.

    The producer-side calling convention restored from the spike
    event.mojo PREPARE/VALIDATE/COMMIT pipeline (A0.7, issue #16): the
    waiter's WAITING commit already PREPAREd the embedded node; this leg
    VALIDATEs (under the injection lock) that the task is still WAITING at
    the captured generation and COMMITs — claim-once via the shared
    `wake_claim` guard (park.mojo `unpark_current` discipline, spec §25) —
    then enqueues the wake record exactly once.

    Returns True iff a wake was accepted (enqueued).  A stale/duplicate
    wake (generation already consumed, or the task already moved off
    WAITING) returns False and enqueues NOTHING — double-wake guarded.

    NOTE — routing seam: this lane carries the wake over the INJECTION
    queue (the shared intake any worker drains).  E5's started-fiber
    affinity retargets the enqueue for owner-affinity wakes of STARTED
    tasks to the owner worker's REMOTE-READY queue (NOT theft-eligible);
    the claim-once guard above is the invariant both targets share.

    VALIDATE + COMMIT are atomic under the injection lock: the capacity
    check, the `wake_claim` (park.mojo's claim-once guard), and the enqueue
    happen in ONE critical section, so a concurrent worker pop can never
    observe a claimed-but-not-yet-enqueued wake, and a full queue never
    consumes the claim (the waiter stays WAITING; the producer retries on
    its next sweep — backpressure, ADR-009).
    """
    q._lock_acquire()
    if len(q._data) >= q._cap:
        # No room for the wake record: do NOT claim — the waiter stays
        # WAITING and the wake is simply not delivered yet (the producer
        # retries next sweep).  Never blocks, never corrupts state.
        q._lock_release()
        return False
    var checker = UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
        unsafe_from_address=tcb_addr
    )
    var claimed = checker[].wake_claim(required_gen)
    if claimed:
        q._data.append(TaskRecord(tcb_addr, task_id))
        q._accepted += 1
    q._lock_release()
    return claimed