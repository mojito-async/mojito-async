# mojito_async/runtime/runtime.mojo
#
# A1.1 runtime (issue #33) — the colorless one-worker scheduler core.
#
# A2.2 (issue #68) — per-worker run queues: this Runtime is ONE worker's
# scheduler state.  The A1 shared FIFO (_ready) is REPLACED on the runnable
# hot path by the per-worker _local LocalDeque (owner push/pop, LIFO spawn
# locality) + _remote RemoteReadyQueue (any worker pushes a wake, the owner
# pops it — spec §19.2/§21).  _ready itself is RETAINED as the E3-OWNED
# injection intake (#69 replaces it with inject_queue.mojo; see the banner
# under enqueue()).  The companion enqueue split:
#
#   enqueue_local / pop_local  — this worker's own deque (spawn, yield);
#   push_remote    / pop_remote — remote-ready wakes (unpark_current; E5
#                                 routes owner-affine wakes here);
#   enqueue()                  — E3-OWNED injection intake (unchanged path).
#
# Productionized from spike/colorless_runtime/runtime.mojo (A0.2 + A0.6,
# issues #11, #15); semantics carried forward.  `Runtime` owns this worker's
# run queues, the shutdown flag, the task-id allocator, and the observable
# scheduling counters.  `run[T: def() -> None]` executes the ROOT task on
# the CALLING thread (work-first, spec §88): it creates the root TCB, walks
# NEW -> RUNNABLE -> RUNNING, invokes the task, and marks COMPLETED on both
# the normal and the raising path (root tasks have no joiners, so run() is
# their joiner — the error message is preserved and re-raised).
#
# INV kept (spec §13/A0-T1): run() creates NO OS thread and performs NO
# hidden blocking — everything executes synchronously on the caller's stack.
#
# Mojo 1.0.0b2 (def-only) constraints honored (see task_control_block.mojo):
#   no `fn`, no `async`/`await`, no module-level mutable globals; first-class
#   `def` values are nominal, so the task argument is a generic constrained
#   to the `def()` callable trait; no static methods (module `create()` is
#   the constructor surface); EXTERN-FREE (modular/modular#6971: extern calls
#   stay in the embedding *_aot drivers).
from mojito_async.runtime.queue import FifoQueue, LocalDeque, RemoteReadyQueue, TaskRecord
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from std.atomic import Atomic
from mojito_async.runtime.inject_queue import InjectQueue


# ---------------------------------------------------------------------------
# Nil result (root tasks return nothing)
# ---------------------------------------------------------------------------

struct Nil(ResultValue):
    """Void stand-in for the root TCB's result slot (b2 has no unit type
    usable as a ResultValue; the root's TCB carries Nil and never marks a
    result)."""

    var _tag: Int

    def __init__(out self):
        self._tag = 0


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

struct Runtime:
    """One worker's scheduler core.

    State:
<<<<<<< HEAD
      _ready     — E3-OWNED injection intake: the A1 global FIFO, retained
                     for global/injection submits until #69's inject queue
                     replaces it (NOT on the local hot path).
      _local     — this worker's LOCAL work-stealing deque (unstarted
                     tasks; owner push_back/pop_back — LIFO spawn locality).
      _remote    — this worker's REMOTE-ready queue (wakes of STARTED
                     fibers; any worker pushes, the owner pops — FIFO).
=======
      _ready     — FIFO of RUNNABLE TaskRecords (mutex-free: single worker,
                     no cross-thread handoff exists).
      _inject    — A2.3 (issue #69) shared bounded MPSC injection queue: the
                     global intake for non-worker-local spawns/foreign
                     enqueues; drained by every worker (see enqueue_global
                     and scheduler_loop's bounded poll).
>>>>>>> origin/a2/69-inject
      _shutdown  — latched by shutdown(); run()/spawn refuse afterwards.
      _scope     — root scope handle (Int cell handle; refined upward).
      _next_id   — monotonic task-id allocator (ids start at 1; 0 = none).
      counters   — observable scheduling effects: _tasks_started/_completed
                     count ROLE-task executions by run(); _enqueued counts
                     runnable registrations (spawn/wake/local/remote).
      _fiber_drives / _fiber_switches — A1.5 (issue #53) fiber-path toggle:
                     drives count scheduler slices that drove a task's FIBER;
                     switches count the actual ms_ctx_switch calls (2 per
                     fiber drive: switch-in + switch-out at the park or the
                     completion trampoline).  CHEAP (non-parking) tasks never
                     touch these — the fast-path regression guard asserts a
                     non-parking run observes 0.
    """

    comptime NO_SCOPE = Int(0)
    # A2.3 (issue #69): default bound for the shared injection queue (spec
    # §86 — limits for externally submitted unbounded work).
    comptime INJECT_CAPACITY = Int(1024)

    var _ready: FifoQueue[TaskRecord]
    var _local: LocalDeque
    var _remote: RemoteReadyQueue
    var _shutdown: Bool
    var _scope: Int
    var _next_id: Int
    var _tasks_started: Int
    var _tasks_completed: Int
    var _enqueued: Int
    # Cross-thread safety (A2.3): the INJECTED path counts accepted records
    # inside InjectQueue's own lock (push already holds it — free there,
    # and NO atomic RMW on a Runtime field: b2 miscompiles that inside the
    # fiber-crossing enqueue path, verified vs t26).  `enqueued()` folds
    # local `_enqueued` (A1 paths, plain +=, worker-local) + inject
    # `accepted()`.  External producer threads therefore never touch any
    # Runtime scalar concurrently.
    var _skipped: Int
    var _fiber_drives: Int
    var _fiber_switches: Int
    # A2.3 (issue #69): the shared global injection queue — the MPSC intake
    # for tasks submitted outside any worker's local deque (external spawn
    # entry, cross-worker/foreign enqueues, reactor/timer submissions).
    # DISTINCT from the A1 `_ready` FIFO: `_ready` stays the LOCAL runnable
    # queue of this Runtime (single worker today; per-worker deque lane is
    # E2/#68), while `_inject` is the global bounded intake any worker
    # drains (see scheduler_loop's bounded poll).  Worker-local enqueues
    # NEVER take this queue's lock.
    var _inject: InjectQueue

    def __init__(out self):
        self._ready = FifoQueue[TaskRecord]()
        self._local = LocalDeque()
        self._remote = RemoteReadyQueue()
        self._shutdown = False
        self._scope = Self.NO_SCOPE
        self._next_id = 1
        self._tasks_started = 0
        self._tasks_completed = 0
        self._enqueued = 0
        self._skipped = 0
        self._fiber_drives = 0
        self._fiber_switches = 0
        self._inject = InjectQueue(Self.INJECT_CAPACITY)

    # --- root-task execution (A0-T1) ----------------------------------------

    def run[T: def() raises -> None](mut self, task: T) raises:
        """Execute the ROOT task on the CALLING thread (work-first, spec §88).

        Full TCB lifecycle on the spot: NEW -> RUNNABLE -> RUNNING, invoke,
        then RUNNING -> COMPLETED.  On a raising task the TCB still reaches
        COMPLETED and the error message is preserved and re-raised: the root
        has no joiner, so run() itself is the joiner.

        Raises on a shut-down runtime; re-raises root-task errors prefixed
        "runtime.run: root task raised:".
        """
        if self._shutdown:
            raise Error("runtime.run: runtime is shut down")
        var root = TaskControlBlock[Nil]()
        root.transition(TaskControlBlock.RUNNABLE)
        root.transition(TaskControlBlock.RUNNING)
        self._tasks_started += 1
        try:
            task()
        except e:
            root.transition(TaskControlBlock.COMPLETED)
            self._tasks_completed += 1
            raise Error("runtime.run: root task raised: " + String(e))
        root.transition(TaskControlBlock.COMPLETED)
        self._tasks_completed += 1

    # --- runnable-queue services (spawn.mojo, scheduler, drivers) -----------

    def next_id(mut self) -> Int:
        """Allocate a monotonically increasing task id (never 0)."""
        var id = self._next_id
        self._next_id += 1
        return id

    # E3-OWNED: injection intake (issue #68/69) — global/injection submits
    # keep riding the A1 FIFO path until #69's inject_queue.mojo replaces
    # it; #69 fills this seam (and scheduler_loop's `# E3-OWNED: injection
    # intake` poll slot).  NOT on the local hot path.
    def enqueue(mut self, tcb_addr: Int, task_id: Int) raises:
        """Register a RUNNABLE task record on the E3 injection intake FIFO
        (A1 semantics, unchanged; #69 owns this path's replacement)."""
        if self._shutdown:
            raise Error("runtime.enqueue: runtime is shut down")
        self._ready.push(TaskRecord(tcb_addr, task_id))
        self._bump_enqueued()

    def enqueue_local(mut self, tcb_addr: Int, task_id: Int) raises:
        """Register a RUNNABLE record on THIS worker's local deque (unstarted
        tasks: spawn, yield).  Owner push_back — LIFO spawn locality (issue
        #68).  No global lock on this path: only this worker's deque guard."""
        if self._shutdown:
            raise Error("runtime.enqueue_local: runtime is shut down")
        self._local.push_back(TaskRecord(tcb_addr, task_id))
        self._enqueued += 1

    def push_remote(mut self, tcb_addr: Int, task_id: Int) raises:
        """Deliver a wake to THIS worker's remote-ready queue (STARTED-fiber
        wakes, spec §19.2/§21).  ANY worker may push; the OWNER pops.  E5
        routes owner-affine wakes here; unpark_current already uses this as
        the post-wake enqueue target (issue #68, #39)."""
        if self._shutdown:
            raise Error("runtime.push_remote: runtime is shut down")
        self._remote.push(TaskRecord(tcb_addr, task_id))
        self._enqueued += 1

    def pop_local(mut self) raises -> TaskRecord:
        """Dequeue the next LOCAL record (owner LIFO end); raises on an
        empty deque."""
        return self._local.pop_back()

    def pop_remote(mut self) raises -> TaskRecord:
        """Dequeue the next REMOTE-ready record (owner FIFO pop); raises on
        an empty queue."""
        return self._remote.pop()

    def pop_ready(mut self) raises -> TaskRecord:
        """A1-compat pop of the runnable record the scheduler would serve
        next (the LOCAL deque — t14/t18 manual pops)."""
        return self._local.pop_back()

    def has_local(mut self) -> Bool:
        return not self._local.is_empty()

    def has_remote(mut self) -> Bool:
        return not self._remote.is_empty()

    def has_ready(mut self) -> Bool:
        """A1-compat probe: is there LOCAL work?  (The scheduler now uses
        has_local/has_remote; kept for the A1 callers.)"""
        return self.has_local()

    def local_queue(mut self) -> UnsafePointer[LocalDeque, MutAnyOrigin]:
        """This worker's local deque (E2 accessor; b2 pointer-return idiom —
        deref at the call site, e.g. E4 steal probes)."""
        return UnsafePointer[LocalDeque, MutAnyOrigin](to=self._local)

    def remote_queue(mut self) -> UnsafePointer[RemoteReadyQueue, MutAnyOrigin]:
        """This worker's remote-ready queue (E2 accessor; pointer-return)."""
        return UnsafePointer[RemoteReadyQueue, MutAnyOrigin](to=self._remote)

    def pending(mut self) -> Int:
        """Number of currently RUNNABLE records across all run paths (E3
        intake + local deque + remote queue)."""
        return len(self._ready) + self._local.count() + self._remote.count()

    # --- A2.3 global injection intake (issue #69) ----------------------------

    def enqueue_global(mut self, tcb_addr: Int, task_id: Int, current_worker: Int) raises:
        """Spawn-policy classification for the RUNNABLE registration of a
        task submitted OUTSIDE any worker's local deque.

        `current_worker` is the known worker identity when the spawn hails
        from a task already running on worker W (b2 has no TLS, so worker
        identity is threaded by value; 0 = no known current worker):
          - current_worker != 0 -> enqueue_local(W) — the per-worker deque
            lane is E2/#68's; until it lands this routes through the A1 FIFO
            `_ready` (the single-worker local queue), so a worker-local
            enqueue NEVER takes the injection lock (acceptance: no global
            lock on the local hot path).  [E2-OWNED seam]
          - current_worker == 0 -> _inject.push — the shared bounded MPSC
            intake; the record is an INJECTION (UNSTARTED, stealable per
            §19.1) any worker may run.  At capacity `_inject.push` raises a
            clear full error instead of blocking any worker (ADR-009); the
            caller retries on its next loop iteration (spec §86).
        Registration counts against `_enqueued` exactly once either way.
        """
        if self._shutdown:
            raise Error("runtime.enqueue_global: runtime is shut down")
        if current_worker == 0:
            # INJECTED (UNSTARTED, stealable per §19.1): any worker may run
            # this record — it lands on the shared bounded MPSC intake.  At
            # capacity `_inject.push` raises a clear full error instead of
            # blocking any worker (ADR-009); the caller retries on its next
            # loop iteration (spec §86 backpressure).  The ACCEPTED record
            # is counted inside the queue's own lock (accepted()); the local
            # `_enqueued` counter stays untouched here.
            self._inject.push(TaskRecord(tcb_addr, task_id))
        else:
            # enqueue_local(W): the per-worker deque lane is E2/#68's — this
            # branch consumes that seam when it lands; until then it routes
            # through the A1 FIFO `_ready` (the single-worker local queue).
            # A worker-local enqueue NEVER takes the injection lock here
            # (acceptance: no global lock on the local hot path).
            # [E2-OWNED]
            self._ready.push(TaskRecord(tcb_addr, task_id))
            self._bump_enqueued()

    def inject_queue(mut self) -> UnsafePointer[InjectQueue, MutAnyOrigin]:
        """The shared injection queue (drain seam for scheduler_loop)."""
        return UnsafePointer[InjectQueue, MutAnyOrigin](to=self._inject)

    def inject_pending(mut self) -> Int:
        """Records currently waiting in the global injection queue."""
        return self._inject.pending()

    def inject_capacity(self) -> Int:
        """The injection queue bound (spec §86 limits)."""
        return self._inject.capacity()

    def inject_rejected(self) -> Int:
        """Capacity rejections the injection queue has observed (backpressure
        evidence — the bounded intake shed load cleanly instead of blocking a
        worker)."""
        return self._inject.rejected()

    # --- observability -------------------------------------------------------

    def tasks_started(self) -> Int:
        return self._tasks_started

    def tasks_completed(self) -> Int:
        return self._tasks_completed

    def enqueued(self) -> Int:
        """Total runnable registrations: local (A1 paths) + accepted into
        the shared injection queue (counted under the queue's own lock —
        cross-thread-safe for external producers)."""
        return self._enqueued + self._inject.accepted()


    def _bump_enqueued(mut self):
        self._enqueued += 1

    def _bump_skipped(mut self):
        self._skipped += 1

    def note_skipped(mut self):
        """Count a popped RUNNABLE record that was skipped (its TCB was not
        RUNNABLE — stale duplicate) by the scheduler loop."""
        self._skipped += 1

    def skipped(self) -> Int:
        """Number of stale/duplicate records the scheduler loop skipped."""
        return self._skipped

    # --- A1.5 fiber-path toggle (issue #53) ---------------------------------

    def note_fiber_drive(mut self):
        """Count one fiber-backed dispatch slice (a record driven on a task
        fiber; the cheap path never calls this)."""
        self._fiber_drives += 1

    def note_fiber_switch(mut self):
        """Count one fiber stack switch (ms_ctx_switch; the cheap path never
        calls this)."""
        self._fiber_switches += 1

    def fiber_drives(self) -> Int:
        """Fiber-backed dispatch slices served (A1.5 seam; issue #53)."""
        return self._fiber_drives

    def fiber_switches(self) -> Int:
        """Actual fiber stack switches (issue #53 cheap-path guard: a
        non-parking run must observe 0)."""
        return self._fiber_switches

    def scope_handle(self) -> Int:
        return self._scope

    def set_scope_handle(mut self, h: Int):
        self._scope = h

    # --- lifecycle -----------------------------------------------------------

    def shutdown(mut self) -> None:
        """Latch the shutdown flag (idempotent)."""
        self._shutdown = True

    def is_shutdown(self) -> Bool:
        return self._shutdown


def create() -> Runtime:
    """Module-level factory (b2 has no static methods).  Mirrors the
    mojito-sys convention of exposing module-level constructors."""
    return Runtime()

def run[T: def() raises -> None](mut rt: Runtime, task: T) raises:
    """Module-level `run`: execute the ROOT task on the calling thread.

    Convenience wrapper mirroring the spikes `runtime.run(task)`: the root
    TCB lifecycle is NEW -> RUNNABLE -> RUNNING -> COMPLETED on the calling
    thread (errors preserved and re-raised prefixed "runtime.run: root task
    raised:").  Synchronous, no OS thread, no hidden blocking.
    """
    rt.run(task)