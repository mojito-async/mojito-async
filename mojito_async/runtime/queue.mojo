# mojito_async/runtime/queue.mojo
#
# A1.1 runtime (issue #33) — FIFO runnable queue + task record payload.
# A2.2 (issue #68) — per-worker LocalDeque + RemoteReadyQueue replacing the
# A1 shared FIFO on each worker's Runtime (the FIFO itself survives as the
# E3-owned injection intake; see the banner under FifoQueue).
#
# Queue-ownership model (spec §18-21, Appendix B; issue #68 deliverables):
#   - LocalDeque      — the worker's LOCAL runnable deque for UNSTARTED
#                       tasks.  Owner push_back/pop_back (LIFO on the owner
#                       end — spawn locality, issue #68 step 2); thieves
#                       steal_front from the OPPOSITE end (spec §20, P0:
#                       spinlock-guarded; P1 Chase-Lev lock-free is an
#                       explicit FOLLOW-ON — do NOT start lock-free).
#   - RemoteReadyQueue — per-worker queue for wakes of STARTED fibers (spec
#                       §19.2): ANY worker pushes a wake, the OWNER worker
#                       pops it (FIFO — wake order).  E5 wires the actual
#                       remote routing; this lane establishes the queue.
#   - FifoQueue       — the A1 shared FIFO, RETAINED (not deleted) as the
#                       global/injection intake until #69's inject_queue.mojo
#                       replaces that path.
#
# Memory ordering (issue #68 step 4 / App. B queue-ownership): every
# mutation goes through the struct's SpinLock; std.atomic compare_exchange
# and store default to SEQUENTIAL consistency (see std atomic.mojo: the
# default Ordering is SEQUENTIAL), which is strictly stronger than the
# required release/acquire pair: a remote push publishes with release
# semantics before the owning worker's acquire-locked pop can observe it,
# and the owner's local push/pop are properly ordered under the same guard.
#
# False sharing (issue #68 step 5 / spec §64): each per-worker queue struct
# is @align(128) (one cache line) so adjacent workers' deques/remote queues
# never share a line.
#
# Mojo 1.0.0b2 (def-only) constraints honored (see task_control_block.mojo):
#   `def` only; no static methods (module-level factories); b2 has no TLS,
#   so worker identity is threaded by VALUE (scheduler_loop's worker_id) —
#   never stored here.
from std.atomic import Atomic
from std.collections import Deque


struct TaskRecord(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Type-erased runnable-task record (data only).

    tcb_addr — address of the task's caller-allocated TaskControlBlock cell.
    task_id  — the scheduler id that enqueued it (parent or wake).
    """

    var tcb_addr: Int
    var task_id: Int

    def __init__(out self, tcb_addr: Int, task_id: Int):
        self.tcb_addr = tcb_addr
        self.task_id = task_id


# ---------------------------------------------------------------------------
# E3-OWNED: injection intake (issue #68/69)
# ---------------------------------------------------------------------------
# The A1 shared FIFO stays as the GLOBAL/INJECTION intake until #69's
# inject_queue.mojo replaces it: Runtime.enqueue() below keeps routing to
# THIS queue, and scheduler_loop carries the `# E3-OWNED: injection intake`
# seam where #69's bounded injection poll drops in.  #69 fills this seam;
# this lane only banners it.
struct FifoQueue[T: Movable & ImplicitlyDeletable & ImplicitlyCopyable](Sized):
    """FIFO runnable-task queue (E3 injection intake; A1 legacy).  Sized so
    `len(self)` works."""

    var data: Deque[Self.T]

    def __init__(out self):
        self.data = Deque[Self.T]()

    def __len__(self) -> Int:
        return len(self.data)

    def is_empty(self) -> Bool:
        return len(self.data) == 0

    def push(mut self, t: Self.T):
        self.data.append(t)

    def pop(mut self) raises -> Self.T:
        if self.is_empty():
            raise Error("FifoQueue: pop from an empty queue")
        return self.data.popleft()

    def clear(mut self) raises:
        while not self.is_empty():
            _ = self.data.popleft()


# ---------------------------------------------------------------------------
# SpinLock — the P0 queue guard (issue #68 step 2)
# ---------------------------------------------------------------------------
# Minimal spinning lock for the P0 locked-deque guard (NativeMutex /
# mojito-sys S3.1 lands in the vendor lane; a spinlock keeps the guard
# dependency-free and bounded — spec §1501 allows short non-suspending
# spinlocks for internal scheduler structures).  compare_exchange/store
# default to SEQUENTIAL ordering, which delivers the release/acquire
# publication the queue contract requires (see module header).

struct SpinLock:
    """Tiny test-and-set spinlock (uncontended owner ops only — E4 stealing
    adds contention on the thief end, still under this same guard until the
    P1 Chase-Lev follow-on).  NOT copyable/movable (its Atomic guard cannot
    be copied); LocalDeque/RemoteReadyQueue embed it by value like Runtime
    embeds its queues."""

    var _flag: Atomic[DType.int64]

    def __init__(out self):
        self._flag = Atomic[DType.int64](0)

    def lock(mut self):
        while True:
            var expected: Int64 = 0
            if self._flag.compare_exchange(expected, 1):
                return

    def unlock(mut self):
        self._flag.store(0)

    def try_lock(mut self) -> Bool:
        var expected: Int64 = 0
        return self._flag.compare_exchange(expected, 1)


# ---------------------------------------------------------------------------
# LocalDeque — per-worker local runnable deque (P0, spec §20)
# ---------------------------------------------------------------------------

@align(128)
struct LocalDeque:
    """Per-worker LOCAL work-stealing deque for unstarted tasks (P0:
    spinlock-guarded; P1 Chase-Lev is a FOLLOW-ON, issue #68 — do NOT start
    lock-free).

    Owner end = BACK: push_back / pop_back (LIFO — spawn locality: the last
    task spawned is the one whose data is hottest).  Thief end = FRONT:
    steal_front reads the OPPOSITE end so owner and thieves never contend
    on the same end (spec §20).  E4 wires real steal probes; this lane
    establishes the structure and proves the ends (t31).

    @align(128): the per-worker deque sits on its own cache line
    (issue #68 step 5 / spec §64) so N workers' deques never false-share.
    """

    var _guard: SpinLock
    var _data: Deque[TaskRecord]

    def __init__(out self):
        self._guard = SpinLock()
        self._data = Deque[TaskRecord]()

    def push_back(mut self, rec: TaskRecord):
        """Owner push (spawn residency): the LIFO end."""
        self._guard.lock()
        self._data.append(rec)
        self._guard.unlock()

    def pop_back(mut self) raises -> TaskRecord:
        """Owner pop: the LIFO end (last pushed runs first — spawn
        locality).  Raises on an empty deque."""
        self._guard.lock()
        if len(self._data) == 0:
            self._guard.unlock()
            raise Error("LocalDeque.pop_back: pop from an empty deque")
        var rec = self._data.pop()
        self._guard.unlock()
        return rec

    def try_pop_back(mut self) raises -> Optional[TaskRecord]:
        """Atomic check-and-pop at the owner's LIFO end: check and pop in a
        SINGLE critical section, eliminating the TOCTOU window that existed
        between a separate is_empty() and pop_back() call (issue #144).
        Returns None on an empty deque.  The `raises` annotation is required
        because Deque.pop() is a raising call; under the guard the check
        ensures the pop path is unreachable."""
        self._guard.lock()
        if len(self._data) == 0:
            self._guard.unlock()
            return Optional[TaskRecord]()
        var rec = self._data.pop()
        self._guard.unlock()
        return Optional[TaskRecord](rec)

    def steal_front(mut self) raises -> TaskRecord:
        """Thief pop: the FRONT — the OPPOSITE end from the owner's LIFO
        pops (spec §20).  E4 wires cross-worker stealing; t31 proves this
        primitive reads the oldest record.  Raises on an empty deque."""
        self._guard.lock()
        if len(self._data) == 0:
            self._guard.unlock()
            raise Error("LocalDeque.steal_front: steal from an empty deque")
        var rec = self._data.popleft()
        self._guard.unlock()
        return rec

    def try_steal_front(mut self) raises -> Optional[TaskRecord]:
        """Atomic check-and-steal at the thief's FRONT end: check and pop in a
        SINGLE critical section, eliminating the TOCTOU window that existed
        between a separate is_empty() and steal_front() call (issue #144).
        Returns None on an empty deque.  The `raises` annotation is required
        because Deque.popleft() is a raising call; under the guard the check
        ensures the pop path is unreachable."""
        self._guard.lock()
        if len(self._data) == 0:
            self._guard.unlock()
            return Optional[TaskRecord]()
        var rec = self._data.popleft()
        self._guard.unlock()
        return Optional[TaskRecord](rec)

    def is_empty(mut self) -> Bool:
        self._guard.lock()
        var e = len(self._data) == 0
        self._guard.unlock()
        return e

    def count(mut self) -> Int:
        self._guard.lock()
        var n = len(self._data)
        self._guard.unlock()
        return n


# ---------------------------------------------------------------------------
# RemoteReadyQueue — per-worker remote-ready queue (spec §19.2 / §21)
# ---------------------------------------------------------------------------

@align(128)
struct RemoteReadyQueue:
    """Per-worker remote-ready queue for wakes of STARTED fibers.

    ANY worker pushes a wake (push); the OWNER worker pops (pop, FIFO —
    wake order).  A wake delivered here is popped exactly once by the
    owner; stale non-RUNNABLE records are skipped by the scheduler loop and
    never double-dispatched (t31).  E5 routes owner-affine wakes here; this
    lane establishes the queue with manual enqueues proving isolation.

    @align(128) + spinlock guard: same cache-line isolation and
    release/acquire publication discipline as LocalDeque (issue #68
    steps 4-5).
    """

    var _guard: SpinLock
    var _data: Deque[TaskRecord]
    # issue #203: accepted (enqueued) records — counted UNDER this queue's
    # own lock, the exact same shape InjectQueue._accepted already uses
    # (inject_queue.mojo) for the identical problem.  push_remote is the
    # cross-thread producer path (ANY worker may push, issue #203's own
    # title) and Runtime.push_remote used to bump a plain `Runtime`
    # `_enqueued` field right after this push, unguarded — a lost-update
    # race against the owner thread's own concurrent enqueue_local.  b2:
    # atomic RMW directly on a `Runtime` field is proven to miscompile the
    # fiber-crossing enqueue path (verified vs t26, see runtime.mojo's own
    # comment beside `_enqueued` and issue #69's original commit), so the
    # fix does not put an atomic on Runtime — it counts here, under the
    # lock `push` already holds, exactly like InjectQueue's `_accepted`.
    var _accepted: Int

    def __init__(out self):
        self._guard = SpinLock()
        self._data = Deque[TaskRecord]()
        self._accepted = 0

    def push(mut self, rec: TaskRecord):
        """Producer-side push: any worker may deliver a wake here.  The
        acceptance count is bumped in the SAME critical section as the
        append (issue #203) — free here, and the only cross-thread-safe
        place to count a cross-thread producer's write without an atomic
        RMW on a `Runtime` field."""
        self._guard.lock()
        self._data.append(rec)
        self._accepted += 1
        self._guard.unlock()

    def accepted(self) -> Int:
        """Records ACCEPTED since construction (counted under this queue's
        own lock — the cross-thread-safe enqueue accounting; Runtime.enqueued
        folds it in, mirroring InjectQueue.accepted())."""
        return self._accepted

    def pop(mut self) raises -> TaskRecord:
        """Owner pop: FIFO (wake order).  Raises on an empty queue."""
        self._guard.lock()
        if len(self._data) == 0:
            self._guard.unlock()
            raise Error("RemoteReadyQueue.pop: pop from an empty queue")
        var rec = self._data.popleft()
        self._guard.unlock()
        return rec

    def is_empty(mut self) -> Bool:
        self._guard.lock()
        var e = len(self._data) == 0
        self._guard.unlock()
        return e

    def count(mut self) -> Int:
        self._guard.lock()
        var n = len(self._data)
        self._guard.unlock()
        return n