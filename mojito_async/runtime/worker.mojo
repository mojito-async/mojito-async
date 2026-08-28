# mojito_async/runtime/worker.mojo
#
# A1.1 runtime (issue #33) — the single worker and its drive surface.
#
# `Worker` names the "calling thread IS the worker" invariant (spec §22:
# scheduler re-entry; b2 has no TLS, so the worker is an explicit value the
# caller drives rather than thread-local state).  On ONE worker everything
# is cooperative: a Worker owns a Runtime and offers the two motions the
# lanes/drivers need — run the root task, and drive the runnable queue with
# a statically-known dispatcher (scheduler_loop).  It deliberately does NOT
# own task bodies (b2 cannot store them); callers pass dispatchers where
# bodies are known, exactly like the spike's drivers.
#
# Extern-free, allocation-discipline kept: the Worker holds no task storage;
# every TaskControlBlock cell is caller-allocated.
from std.atomic import Atomic, Ordering
from std.collections import Deque
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.queue import TaskRecord
from mojito_async.runtime.runtime import Nil, Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock


# ===========================================================================
# E4-OWNED (issue #70): A2.4 unstarted-task stealing
# ===========================================================================
#
# The P0 LocalDeque below is the E2-OWNED PROTOTYPE of #68's LocalDeque
# (runtime/queue.mojo).  The A2 lane merge order is E1..E8 and THIS PR merges
# AFTER #68, so the production call sites use the ISSUE-BODY API contract:
#
#     LocalDeque.push / pop / steal_front / len / is_empty
#
# This base ships no LocalDeque yet (A1 FifoQueue only; queue.mojo is #68's
# file), so the P0 shape below stands in with IDENTICAL call-site names —
# the #68 merge replaces this struct (and Worker's `_local` field type)
# mechanically.
#
# P0 deque semantics (spec §20 — "simpler locked deque" phase): owner push
# at the BACK, owner pop at the FRONT (FIFO, matching the A1 runtime queue),
# thieves steal_front at the BACK — the OPPOSITE end from the owner's FIFO
# pop, so owner-drain and thief-steal never contend on the same records
# ("prototype steal_front against the A1 FifoQueue's tail").  A per-deque
# spinlock (the §20 P0 lock) makes pop / steal_front ATOMIC with respect to
# every worker.
#
# b2 toolchain note: the lock is a VALUE `Atomic` field (not a pointer) —
# pointer-dereferenced atomic ops inside loops ICE the b2 kgen (verified
# probe); the value-field shape below compiles and runs.  The struct is a
# PLAIN struct (no Movable/Copyable traits): synthesized moves over the
# non-movable Atomic/Deque fields are deliberately not requested; LocalDeque
# is default-constructed in place (caller-allocated, never copied/moved).
struct LocalDeque:
    """E2-OWNED P0 prototype (issue #68): spinlock-guarded FIFO task deque.

    push        — owner enqueue (FIFO tail).
    pop         — owner dequeue (FIFO head); None when empty.
    steal_front — THIEF take from the tail (spec §20 opposite end; the
                  youngest never-yet-served work); None when empty; atomic
                  with owner operations under the same guard.
    len/is_empty — size queries (caller-owned: only meaningful without
                  concurrent mutation).
    """

    var _lock: Atomic[DType.int64]
    var _data: Deque[TaskRecord]

    def __init__(out self):
        self._lock = Atomic[DType.int64](0)
        self._data = Deque[TaskRecord]()

    def _acquire(mut self):
        while True:
            var expected = Int64(0)
            if self._lock.compare_exchange[
                success_ordering=Ordering.ACQUIRE,
                failure_ordering=Ordering.ACQUIRE,
            ](expected, Int64(1)):
                return

    def _release(mut self):
        self._lock.store[ordering=Ordering.RELEASE](Int64(0))

    def push(mut self, t: TaskRecord) raises:
        self._acquire()
        self._data.append(t)
        self._release()

    def pop(mut self) raises -> Optional[TaskRecord]:
        self._acquire()
        var out = Optional[TaskRecord]()
        if len(self._data) > 0:
            out = Optional[TaskRecord](self._data.popleft())
        self._release()
        return out

    def steal_front(mut self) raises -> Optional[TaskRecord]:
        self._acquire()
        var out = Optional[TaskRecord]()
        if len(self._data) > 0:
            out = Optional[TaskRecord](self._data.pop())
        self._release()
        return out

    def len(self) -> Int:
        return len(self._data)

    def is_empty(self) -> Bool:
        return len(self._data) == 0


struct Worker:
    """One cooperative worker = one Runtime + run/drive entry points.

    E1-OWNED / E2-OWNED (issues #67/#68) fields below are the M:N pool
    seams the worker loop restructure consumes: `_local` is the worker's
    local runnable deque (LocalDeque prototype until #68 merges), `_index`
    is the pool identity, `_peers`/`_n_workers` are the peer registry
    (#67's pool wires them).  The E4 (issue #70) steal surface reads them;
    `_steal_cursor` is E4's round-robin rotation state.
    """

    var _runtime: Runtime

    # --- E2-OWNED (issue #68): local runnable deque ---
    var _local: LocalDeque
    # --- E1-OWNED (issue #67): pool identity + peers ---
    var _index: Int
    var _peers: UnsafePointer[UnsafePointer[Worker, MutAnyOrigin], MutUntrackedOrigin]
    var _n_workers: Int
    # --- E4-OWNED (issue #70): round-robin probe rotation ---
    var _steal_cursor: Int

    def __init__(out self):
        self._runtime = create()
        self._local = LocalDeque()
        self._index = 0
        self._peers = UnsafePointer[
            UnsafePointer[Worker, MutAnyOrigin], MutUntrackedOrigin
        ](unsafe_from_address=1)
        self._n_workers = 0
        self._steal_cursor = 0

    # --- E4 (issue #70): the steal probe surface ---------------------------

    def pop_local(mut self) raises -> Optional[TaskRecord]:
        """The §21 `pop_local` motion over the worker's local deque (the
        only runnable source in the A1 base; #68 restructures the loop and
        adds the remote-ready queue).  Included here because the E4 steal
        probe sits exactly after local/remote/inject in the §21 order."""
        return self._local.pop()

    def request_steal[R: ResultValue = Nil](
        mut self, target: Int
    ) raises -> Optional[TaskRecord]:
        """Probe ONE peer worker's local deque (issue #70 step 1).

        Under the TARGET deque's guard, steal_front takes the youngest
        record and the popped record's TCB is checked IMMEDIATELY — still
        under the guard — so stealability is determined ATOMICALLY with the
        steal itself (no window in which a started fiber could slip between
        read and pop).

        A record whose TCB is already STARTED (spec §19.1/§19.2: a started
        task is worker-affine; e.g. a re-enqueued yield of a started task)
        is RETURNED to the owner's deque — never run on a non-owner worker
        (invariant: no started fiber migrates) — and None is returned so
        the caller picks another peer.  On a successful steal this worker
        runtime's task_steals_total (§71) is bumped exactly once; a failed
        probe (empty deque, or a STARTED record returned) bumps nothing.

        ADR-007-safe: only never-run tasks are ever removed, so no live
        stack is touched by a steal.
        """
        if target < 0 or target >= self._n_workers or target == self._index:
            return Optional[TaskRecord]()
        var peer = self._peers[target]
        var rec = peer[]._local.steal_front()
        if not rec:
            return Optional[TaskRecord]()
        var r = rec.value()
        var checker = UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=r.tcb_addr
        )
        if checker[].is_started():
            # Started task — return it to the OWNER's deque (it re-runs
            # there; nothing is lost or duplicated) and report the probe as
            # failed so the caller picks another peer.
            peer[]._local.push(r)
            return Optional[TaskRecord]()
        self._runtime.note_steal()
        return rec

    def try_steal_unstarted[R: ResultValue = Nil](
        mut self
    ) raises -> Optional[TaskRecord]:
        """The §21 steal probe: poll PEER workers' local deques in
        round-robin starting from own_index+1 (locality-aware order;
        ADR-007-safe — only unstarted tasks are ever removed), skipping
        self, CAPTED at ONE full probe round so an idle worker never spins
        on empty deques — the empty-round outcome hands control back to the
        caller, which yields to the E6 idle sleep path (issue #70 step 3;
        see the §21 loop banner in scheduler.mojo).

        Returns the first record a peer yielded to a steal (STARTED-
        guarded), or None when the round found nothing stealable.  Each
        successful steal bumps this worker runtime's task_steals_total
        exactly once (issue #70 step 5: nothing on a failed probe — no fake
        counters).
        """
        if self._n_workers <= 1:
            return Optional[TaskRecord]()
        # Preskew the rotation off self: a round starts at a peer (never at
        # this worker), then walks around — one probe per peer, capped at a
        # single round so an idle worker does not spin on empty deques.
        var start = self._steal_cursor
        if start == self._index:
            start = (start + 1) % self._n_workers
        for k in range(self._n_workers - 1):
            var pidx = (start + k) % self._n_workers
            if pidx == self._index:
                continue
            var rec = self.request_steal[R](pidx)
            if rec:
                # Advance the rotation past the successful peer.
                self._steal_cursor = (pidx + 1) % self._n_workers
                return rec
        # One complete round, nothing stealable — rotate and yield to E6.
        self._steal_cursor = (start + 1) % self._n_workers
        return Optional[TaskRecord]()

    # --- access ----------------------------------------------------------

    def runtime(mut self) -> ref Runtime:
        return self._runtime

    def handle(mut self) -> UnsafePointer[Runtime, MutAnyOrigin]:
        return UnsafePointer[Runtime, MutAnyOrigin](to=self._runtime)

    # --- entry points -------------------------------------------------------

    def run_root[T: def() raises -> None](mut self, task: T) raises:
        """Execute the ROOT task synchronously on this worker (runtime.run):
        full TCB lifecycle on the calling thread, errors preserved and
        re-raised."""
        self._runtime.run(task)

    def drive[F: def(mut Runtime, Int, Int, BytePtr) raises -> Int](
        mut self, dispatcher: F, ud: BytePtr
    ) raises -> Int:
        """Single-worker scheduler loop: drives the runnable queue to quiet
        with the given statically-known dispatcher (same bound as
        scheduler_loop).  Returns the number of records served."""
        return scheduler_loop(self._runtime, dispatcher, ud)

    def shutdown(mut self) -> None:
        self._runtime.shutdown()


# Module-level factory (b2 has no static methods).
def make_worker() -> Worker:
    return Worker()