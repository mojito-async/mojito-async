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
# A2.2 (issue #68) — per-worker run-queue accessors (E2-owned).  The queue
# STATE lives on this worker's Runtime (_local LocalDeque + _remote
# RemoteReadyQueue, runtime/queue.mojo — the ONE LocalDeque in the tree);
# Worker exposes thin refs so callers can probe/drive them without reaching
# into Runtime internals.
#
# E1-OWNED (issue #67): the WORKER POOL, thread_entry, NativeThread and
# TlsKey bindings are the sibling lane's; the entry points below
# (run_root/drive/shutdown) stay untouched by that lane.
#
# E4-OWNED (issue #70): the unstarted-task steal probe surface.  The probe
# runs over queue.mojo's LocalDeque (owner push_back/pop_back at the back,
# thief steal_front at the front — spec §20 opposite ends); a STARTED
# record popped under the target deque's guard is RETURNED to the owner
# (push_back) — never run off-owner (ADR-006/007).
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.queue import LocalDeque, RemoteReadyQueue, TaskRecord
from mojito_async.runtime.runtime import Nil, Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.vendor.mojito_sys import (
    NativeThread,
    NativeTlsKey,
    make_native_thread,
    tls_get,
)


# ===========================================================================
# E4-OWNED (issue #70): A2.4 unstarted-task stealing
# ===========================================================================
#
# The steal surface below probes PEER workers' LOCAL deques (queue.mojo's
# LocalDeque — owner push_back/pop_back LIFO at the BACK, thief steal_front
# at the FRONT, spec §20; started-fiber wakes ride the remote-ready queue,
# spec §19.2, and are never theft-eligible).  Stealability is decided
# ATOMICALLY with the steal, under the target deque's guard: the popped
# record's TCB is checked immediately; a STARTED record is RETURNED to the
# owner's deque (it re-runs there — nothing lost) and the probe reports
# failure so the caller picks another peer.  Each successful steal bumps
# the stealing worker runtime's task_steals_total exactly once (spec §71);
# a failed probe bumps nothing — no fake counters.


struct Worker:
    """One worker = one Runtime + (A1) run/drive entry points + (A2.1)
    per-OS-thread identity (id, the owned NativeThread handle once the pool
    spawns it, the current_worker TLS key written at thread entry, issue
    #67) + the M:N pool seams the worker-loop restructure consumes: `_local`
    is this worker's local runnable deque (queue.mojo's LocalDeque, issue
    #68), `_index` is the pool identity, `_peers`/`_n_workers` are the peer
    registry (#67's pool wires them), and `_steal_cursor` is E4's (issue
    #70) round-robin probe rotation.  The A1 motions are UNCHANGED: on ONE
    worker everything is cooperative — run the root task, or drive the
    runnable queue with a statically-known dispatcher.

    Extern-free, allocation-discipline kept: the Worker holds no task
    storage; every TaskControlBlock cell is caller-allocated.
    """

    var _runtime: Runtime
    var _id: Int
    var _thread: NativeThread
    var _started: Bool
    var _tls_current_worker: NativeTlsKey

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
        self._id = 0
        self._thread = make_native_thread()
        self._started = False
        self._tls_current_worker = NativeTlsKey()
        self._local = LocalDeque()
        self._index = 0
        self._peers = UnsafePointer[
            UnsafePointer[Worker, MutAnyOrigin], MutUntrackedOrigin
        ](unsafe_from_address=1)
        self._n_workers = 0
        self._steal_cursor = 0

    def __init__(out self, id: Int):
        self._runtime = create()
        self._id = id
        self._thread = make_native_thread()
        self._started = False
        self._tls_current_worker = NativeTlsKey()
        self._local = LocalDeque()
        self._index = 0
        self._peers = UnsafePointer[
            UnsafePointer[Worker, MutAnyOrigin], MutUntrackedOrigin
        ](unsafe_from_address=1)
        self._n_workers = 0
        self._steal_cursor = 0

    # --- E4 (issue #70): the steal probe surface ---------------------------

    def pop_local(mut self) raises -> Optional[TaskRecord]:
        """The §21 `pop_local` motion over the worker's local deque (owner
        LIFO end — the only runnable source in the A1 base; #68 restructures
        the loop and adds the remote-ready queue).  Included here because
        the E4 steal probe sits exactly after local/remote/inject in the
        §21 order.  None when the deque is empty."""
        if self._local.is_empty():
            return Optional[TaskRecord]()
        return Optional[TaskRecord](self._local.pop_back())

    def request_steal[R: ResultValue = Nil](
        mut self, target: Int
    ) raises -> Optional[TaskRecord]:
        """Probe ONE peer worker's local deque (issue #70 step 1).

        Under the TARGET deque's guard, steal_front takes the oldest
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
        if peer[]._local.is_empty():
            return Optional[TaskRecord]()
        var r = peer[]._local.steal_front()
        var checker = UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=r.tcb_addr
        )
        if checker[].is_started():
            # Started task — return it to the OWNER's deque (it re-runs
            # there; nothing is lost or duplicated) and report the probe as
            # failed so the caller picks another peer.
            peer[]._local.push_back(r)
            return Optional[TaskRecord]()
        self._runtime.note_steal()
        return Optional[TaskRecord](r)

    def try_steal_unstarted[R: ResultValue = Nil](
        mut self
    ) raises -> Optional[TaskRecord]:
        """The §21 steal probe: poll PEER workers' local deques in
        round-robin starting from own_index+1 (locality-aware order;
        ADR-007-safe — only unstarted tasks are ever removed), skipping
        self, CAPPED at ONE full probe round so an idle worker never spins
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

    def runtime(mut self) -> UnsafePointer[Runtime, MutAnyOrigin]:
        """This worker's Runtime (b2 pointer-return idiom; deref at the call
        site — `w.runtime()[].enqueue_local(...)`)."""
        return UnsafePointer[Runtime, MutAnyOrigin](to=self._runtime)

    def handle(mut self) -> UnsafePointer[Runtime, MutAnyOrigin]:
        return UnsafePointer[Runtime, MutAnyOrigin](to=self._runtime)

    def local_queue(mut self) -> UnsafePointer[LocalDeque, MutAnyOrigin]:
        """This worker's LOCAL runnable deque (owner push/pop; the scheduler
        drains it before the remote queue, spec §21)."""
        return self._runtime.local_queue()

    def remote_queue(mut self) -> UnsafePointer[RemoteReadyQueue, MutAnyOrigin]:
        """This worker's REMOTE-ready queue (any worker pushes a wake, the
        owner pops — spec §19.2)."""
        return self._runtime.remote_queue()

    def id(mut self) -> Int:
        """Distinct per-worker id (0 for an unpooled A1 worker; the pool
        numbers its workers 0..N-1)."""
        return self._id

    def thread(mut self) -> NativeThread:
        """The OS thread this worker runs on once the pool spawned it."""
        return self._thread

    def started(mut self) -> Bool:
        """True once the pool spawned this worker's OS thread."""
        return self._started

    def tls_key(mut self) -> NativeTlsKey:
        return self._tls_current_worker

    def mark_started(mut self, t: NativeThread, key: NativeTlsKey):
        """Pool-owned: bind the spawned thread + the current_worker TLS key."""
        self._thread = t
        self._tls_current_worker = key
        self._started = True

    def tls_worker_ptr(mut self) -> BytePtr:
        """Read THIS OS thread's current_worker slot (coarse entry-only
        value; address 0 when unset).  Only the worker thread itself reads
        its own slot — the pool side never calls this."""
        return tls_get(self._tls_current_worker)

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