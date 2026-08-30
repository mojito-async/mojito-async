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
# A2.7 (issue #73) — the FAIRNESS surface (spec §21/§67/§71): the worker
# loop's fairness budget counts CONSECUTIVELY LOCALLY-SOURCED slices
# (slices_local) against Budget.K; on hitting K it sweeps remote-ready
# (slices_remote), the injection intake (slices_inject), and the caller's
# timer/reactor service callback (service_sweeps), then resumes local work
# (budget_resets).  The kill-0 starvation watch counts a task that exceeded
# the budget without an intervening cooperative handoff (yields(), any park)
# or service sweep — starvation_events; never-yielding CPU-bound user code
# is the documented §67 cooperative limitation (NO preemption in MVP, §68),
# measured rather than hidden.  The E6 idle-sleep handoff: a worker that
# parks its OS thread MUST do so only AFTER fair_scheduler_loop returned on
# a fully-serviced drain (slices_* + service_sweeps visible).
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
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.idle import announce_work, complete_work, wake_one as idle_wake_one


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
      _ready     — A1 legacy global FIFO (mutex-free: single worker, no
                     cross-thread handoff); retained ONLY for the A1
                     `enqueue()` intake, which #69's inject queue replaced
                     on every live path (NOT on the local hot path).
      _local     — this worker's LOCAL work-stealing deque (unstarted
                     tasks; owner push_back/pop_back — LIFO spawn locality).
      _remote    — this worker's REMOTE-ready queue (wakes of STARTED
                     fibers; any worker pushes, the owner pops — FIFO).
      _inject    — A2.3 (issue #69) shared bounded MPSC injection queue: the
                     global intake for non-worker-local spawns/foreign
                     enqueues; drained by every worker (see enqueue_global
                     and scheduler_loop's bounded poll).
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
      _steal_total — E4 (issue #70): successful unstarted-task steals
                     (spec §71 `task_steals_total`); exact, one bump each.
      _slices_* / _budget_resets / _service_sweeps / _yields /
      _starvation_events — A2.7 (issue #73) fairness observability: the
                     fair loop (scheduler.mojo) classifies every served
                     slice and counts budget/service/starvation events here.
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
    # E4 (issue #70) — successful unstarted-task steals (spec §71
    # `task_steals_total`).  Bumped exactly once per successful steal; a
    # failed probe (empty deque or a STARTED record returned to its owner)
    # bumps nothing — no fake counters.
    var _steal_total: Int
    # E6/M2/#112 (PR #106/#107 folds; issue #112 item 2) — the idle-
    # accounting block + the pool NativeEvent handle for the producer-side
    # wake budget (runtime/idle.mojo).  OPTIONAL fields: the pool allocates
    # the block/event and arms each worker runtime's cell via arm_acct()/
    # arm_event() once the E6 NativeEvent idle path exists (worker_pool.mojo
    # _workers_reinit); until then `_acct` carries the address-1 SENTINEL
    # and `_event` carries 0, and every read/signal is guarded exactly like
    # the acct readers'/idle.wake_one's pattern, so a cloned/moved Runtime
    # (worker cells are rebuilt per start) never announces or signals
    # against a sentinel.  ANNOUNCE + SIGNAL are both wired in
    # enqueue_global (per accepted record) and enqueue_local/push_remote —
    # item 2's wake-path fix: before this fold only announce_work fired
    # (M2-partial), so an idle-parked owner could stall up to the full
    # IDLE_PARK_SLICE_NS backstop (~2s) instead of waking within the
    # event's latency.
    var _acct: BytePtr
    var _event: Int
    # A2.7 (issue #73) — fairness counters (spec §21/§67/§71): work-class
    # slice accounting + budget/service/starvation observability.  The
    # starve-watch and budget state LIVE in the fair loop (scheduler.mojo);
    # the runtime only OBSERVES the drives' effects here.
    var _slices_local: Int
    var _slices_remote: Int
    var _slices_inject: Int
    var _budget_resets: Int
    var _service_sweeps: Int
    var _yields: Int
    var _starvation_events: Int
    # issue #144: worker fault counter — errors that escaped fair_scheduler_loop
    # are caught by pool_worker_loop_scheduled's catch-count-continue guard and
    # tallied here so diagnostics can observe them without losing the thread.
    # Thread safety: safe to read only after join_all() completes, or under
    # external synchronization.
    var _worker_faults: Int

    # M9 (review fold, issue #73): PER-SLICE COUNTER COST, documented and
    # bounded.  Every served slice costs exactly ONE non-atomic Int
    # increment on a worker-LOCAL field (note_slice_{local,remote,inject}
    # from the fair loop — the scheduler touches no other counter on the
    # slice path; budget_resets/service_sweeps/yields/starvation_events
    # bump only on their events, never per slice).  The block is GROUPED
    # at the TAIL of the struct (M9): the seven Ints share one cache line
    # pair that the hot queue fields (_ready/_local/_remote/_inject) never
    # share, so the per-slice write stays off the dispatch hot line — no
    # atomic RMW, no lock, no cross-thread contention (all seven are
    # touched by exactly one worker thread; the pool's acct atomics for the
    # #72 idle counters live on a separate heap block for that reason).
    # Cost envelope: one plain store (worker-local, line-resident) per
    # dispatch slice — negligible vs the dispatch itself; observable only
    # in slice-heavy microbenchmarks and never on a foreign/atomic path.
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
        self._steal_total = 0
        self._acct = BytePtr(unsafe_from_address=1)
        self._event = 0
        self._slices_local = 0
        self._slices_remote = 0
        self._slices_inject = 0
        self._budget_resets = 0
        self._service_sweeps = 0
        self._yields = 0
        self._starvation_events = 0
        self._worker_faults = 0
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
        # E6/M2/#112 producer-side wake budget: announce the accepted LOCAL
        # unit into the idle acct when the pool armed one, then SIGNAL the
        # pool event (issue #112 item 2 — before this fold only the
        # announce fired; an idle-parked owner could stall up to the full
        # IDLE_PARK_SLICE_NS backstop instead of waking within the event's
        # latency).
        self._announce_and_wake()

    def enqueue_local_stolen(mut self, tcb_addr: Int, task_id: Int) raises:
        """Re-enqueue a STOLEN unstarted record onto THIS worker's local deque
        WITHOUT announcing new work (issue #150).  The task was already
        announced on the source worker when it was first seeded; stealing is a
        TRANSFER — the pending counter must not grow again.  enqueue_local
        would call _announce_and_wake and create an unmatched +1 per steal
        that the scheduler's complete_dispatched never balances (the task is
        dispatched and completed only once).  This path pushes and bumps
        _enqueued for observability but never touches the acct."""
        if self._shutdown:
            raise Error("runtime.enqueue_local_stolen: runtime is shut down")
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
        # E6/M2/#112 producer-side wake budget: a REMOTE wake is the
        # classic cross-worker producer — announce the accepted unit AND
        # signal the pool event (the woken owner may be an idle-parked
        # sleeper; issue #112 item 2 closes the wake-path gap this exact
        # call site named).
        self._announce_and_wake()

    # --- E6/M2/#112 idle-acct seam (PR #106/#107 folds; issue #112) -------

    def arm_acct(mut self, acct: BytePtr):
        """Pool-owned (the E6 lane calls this per worker cell once the
        NativeEvent idle path exists): arm this runtime's idle-accounting
        block.  `0`/the address-1 sentinel are refused — the acct readers'
        guard (`_acct_guarded`, runtime/idle.mojo) treats <= 1 as
        unarmed."""
        if Int(acct) > 1:
            self._acct = acct

    def arm_event(mut self, event: Int):
        """Pool-owned (issue #112 item 2, paired with arm_acct): arm this
        runtime's pool NativeEvent handle so the wake paths below can
        SIGNAL an idle-parked owner, not just announce work for it.  `0`
        (the NULL-CAPABLE raw-handle convention, vendor/mojito_sys.mojo
        header) is refused/left unarmed — idle.wake_one's own guard treats
        0 identically, so this is belt-and-suspenders against a Runtime
        that is armed with an acct block but never wired to a real pool
        event (e.g. a caller-owned acct block in a unit test)."""
        if event != 0:
            self._event = event

    def pool_acct(mut self) -> BytePtr:
        """This runtime's armed idle-accounting block; the address-1
        sentinel while unarmed."""
        return self._acct

    def pool_event(mut self) -> Int:
        """This runtime's armed pool NativeEvent handle; 0 while unarmed."""
        return self._event

    def _announce_and_wake(mut self):
        """E6/M2/#112 producer-side wake budget: announce ONE accepted
        runnable record into the idle acct when the pool armed one, THEN
        signal the pool event via idle.wake_one (item 2: the SIGNAL half —
        idle.wake_one's own guard fires the OS-level signal only when a
        worker is actually parked AND the event is armed, so an unarmed
        Runtime, or one with no parked sleeper, announces/signals nothing).
        Both fields are OPTIONAL (default = the address-1 sentinel / 0) and
        every read is guarded exactly like the acct readers' pattern; an
        unarmed runtime announces and signals nothing."""
        if Int(self._acct) > 1:
            announce_work(self._acct, 1)
            idle_wake_one(self._acct, self._event)

    def _complete_dispatched(mut self) raises:
        """Symmetric drain pair for _announce_and_wake: one work unit that was
        announced on enqueue has now been dispatched to completion, parking, or
        re-enqueue (yield_now announces again on the re-enqueue, making the
        pair symmetric: announce → dispatch → complete for every scheduler
        iteration).  Decrements the pending counter so workers' pre-park
        re-check does not see phantom work once the queues are empty.  No-op
        when the acct block is unarmed (the address-1 sentinel) — the same
        guard as _announce_and_wake so unarmed unit-test runtimes are safe."""
        if Int(self._acct) > 1:
            complete_work(self._acct, 1)

    def pop_local(mut self) raises -> TaskRecord:
        """Dequeue the next LOCAL record (owner LIFO end); raises on an
        empty deque."""
        return self._local.pop_back()

    def try_pop_local(mut self) raises -> Optional[TaskRecord]:
        """Atomic check-and-pop of the local deque: check and pop in a SINGLE
        critical section, eliminating the TOCTOU race between a separate
        has_local() and pop_local() call (issue #144).  Returns None on an
        empty deque.  The `raises` annotation is required because the
        underlying Deque.pop() is raising; under the guard the empty check
        ensures the pop path is unreachable."""
        return self._local.try_pop_back()

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

    def has_inject(mut self) -> Bool:
        """True when the E3 injection intake holds a record (A2.7 fairness-
        budget drain probe, issue #73; scheduler.fair_scheduler_loop polls
        this between budget windows).  With #69's InjectQueue merged, the
        intake IS the shared bounded injection queue (the A1 `_ready` FIFO
        is only the legacy `enqueue()` path — never a live intake)."""
        return self._inject.pending() > 0

    def pop_inject(mut self) raises -> TaskRecord:
        """Dequeue the next injection-intake record; raises on an empty
        intake.  The A2.7 fair loop drains the intake to quiet between
        budget windows (spec §21 'service reactor/timers'); records come
        from the shared bounded InjectQueue (issue #69)."""
        var rec = TaskRecord(0, 0)
        if self._inject.try_pop(rec):
            return rec
        raise Error("runtime.pop_inject: injection intake is empty")

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
          - current_worker != 0 -> enqueue_local(W) — the record lands on
            this worker's LOCAL deque (E2/#68's owner push_back, spec §21),
            so a worker-local enqueue NEVER takes the injection lock
            (acceptance: no global lock on the local hot path).  [E2 seam
            consumed: #68's LocalDeque replaced the A1 `_ready` FIFO]
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
            # E6/M2/#112 producer-side wake budget: announce PER ACCEPTED
            # RECORD — a push that raised at capacity announces NOTHING,
            # so the bounded wake budget is never over-spent (a rejected
            # unit produces no wake entitlement) — THEN signal the pool
            # event (item 2): at most ONE signal per accepted record,
            # fired only when idle.wake_one's own guard finds a parked
            # sleeper.
            self._announce_and_wake()
        else:
            # enqueue_local(W): E2/#68's per-worker deque lane — the record
            # lands on THIS worker's LOCAL deque (owner push_back, LIFO
            # spawn locality; the scheduler drains it first, spec §21).  A
            # worker-local enqueue NEVER takes the injection lock here
            # (acceptance: no global lock on the local hot path).  [E2 seam
            # consumed: #68's LocalDeque replaced the A1 `_ready` FIFO]
            self._local.push_back(TaskRecord(tcb_addr, task_id))
            self._enqueued += 1
            # E6/M2/#112 producer-side wake budget: announce the accepted
            # LOCAL unit AND signal the pool event (item 2 — the wake
            # signal for this announced local unit; idle.wake_one bounds
            # the budget on acct_parked > 0).
            self._announce_and_wake()

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

    # --- A2.7 fairness observability (issue #73; spec §21/§67/§71) ----------

    def note_slice_local(mut self):
        """Count one LOCALLY-SOURCED task slice served by the fair loop
        (spawn/yield residency on this worker's own deque)."""
        self._slices_local += 1

    def note_slice_remote(mut self):
        """Count one REMOTE-ready slice served (a wake of a STARTED task,
        spec §19.2 — delivered but deferred at most K local slices)."""
        self._slices_remote += 1

    def note_slice_inject(mut self):
        """Count one INJECTION-intake slice served (the E3 seam; issue #69's
        inject queue drains here)."""
        self._slices_inject += 1

    def note_budget_reset(mut self):
        """Count one fairness-budget RESET: the fair loop hit Budget.K
        locally-sourced slices and serviced the other work classes before
        resuming local work (spec §21 'run at most K ready tasks then
        service reactor/timers')."""
        self._budget_resets += 1

    def note_service_sweep(mut self):
        """Count one timer/reactor service pass between budget drains (the
        caller's service callback — timer_service sweep and/or a reactor
        nonblocking poll)."""
        self._service_sweeps += 1

    def note_yield(mut self):
        """Count one COOPERATIVE yield (yield_now runnable re-registration):
        the task's own handoff — a fresh budget window for the starve-watch."""
        self._yields += 1

    def note_starvation(mut self):
        """Bump the kill-0 starve counter: a task ran more than Budget.K
        CONSECUTIVE slices without a yield/park or an intervening service
        sweep — the documented §67 cooperative limitation (no preemption in
        MVP, §68), measured rather than hidden.  Surfaced to the benchmarks
        (E8, issue #74)."""
        self._starvation_events += 1

    def slices_local(self) -> Int:
        """Locally-sourced slices served (spawn/yield residency)."""
        return self._slices_local

    def slices_remote(self) -> Int:
        """Remote-ready slices served (STARTED-task wakes, spec §19.2)."""
        return self._slices_remote

    def slices_inject(self) -> Int:
        """Injection-intake slices served (E3 seam)."""
        return self._slices_inject

    def budget_resets(self) -> Int:
        """Fairness-budget resets (deferred local work after K local slices)."""
        return self._budget_resets

    def service_sweeps(self) -> Int:
        """Timer/reactor service passes between budget drains."""
        return self._service_sweeps

    def yields(self) -> Int:
        """Cooperative yield_now re-registrations observed."""
        return self._yields

    def starvation_events(self) -> Int:
        """Kill-0 starve counter: never-yielding tasks that held a worker
        past Budget.K consecutive slices (spec §67/§71)."""
        return self._starvation_events

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

    # --- E4 (issue #70): steal observability (spec §71) ----------------------

    def note_steal(mut self):
        """Count one successful unstarted-task steal (issue #70 step 5)."""
        self._steal_total += 1


    def note_worker_fault(mut self):
        """Count one fault that escaped fair_scheduler_loop (issue #144):
        a loop-body error that the embedder's catch-count-continue guard
        swallowed to keep the worker thread alive.  Visible via
        worker_faults_total()."""
        self._worker_faults += 1

    def worker_faults_total(self) -> Int:
        """Number of fair_scheduler_loop errors caught and continued by the
        embedder's worker-thread fault guard (issue #144)."""
        return self._worker_faults
    def task_steals_total(self) -> Int:
        """Successful unstarted-task steals on this runtime (spec §71
        `task_steals_total`).  Exact: one bump per steal, zero on failed
        probes; a started-fiber steal is never counted (the STARTED guard
        returns the record before the counter is reached)."""
        return self._steal_total

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