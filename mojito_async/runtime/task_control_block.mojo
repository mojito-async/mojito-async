# mojito_async/runtime/task_control_block.mojo
#
# A1.1 runtime (issue #33) — pure-Mojo TaskControlBlock + task state machine.
#
# Productionized from spike/colorless_runtime/task.mojo (A0.3, issue #12);
# semantics carried forward VERBATIM (relayout only).  This is the A0.5 task
# state machine with the embedded allocation-free WaitNode (spec §24) and the
# consume-once result slot.  State NAME set matches the spike exactly
# (NEW/RUNNABLE/RUNNING/PARKING/WAITING/COMPLETED/CANCELLED) so sibling lanes
# and the A0 suite stay coherent.
#
# Mojo 1.0.0b2 dialect notes (spike conventions):
#   - `def`, not `fn`; `comptime` for constants.
#   - Mutable instance methods take `mut self`; constructors `out self`.
#   - def parameters pass by immutable value, so `take_result()` returns a
#     copy guarded by a consume-once flag; true move-out is the A1 JoinHandle.
#   - `raise` accepts only the builtin `Error`; IllegalTransitionError is
#     carried in the message.

# ---------------------------------------------------------------------------
# Result slot constraint
# ---------------------------------------------------------------------------

trait ResultValue(ImplicitlyCopyable, ImplicitlyDeletable):
    """Constraint for the TCB result slot: copyable, deletable,
    default-constructible.  Conform with a zero-argument `__init__`."""

    def __init__(out self): ...


# ---------------------------------------------------------------------------
# IllegalTransitionError (error model)
# ---------------------------------------------------------------------------

struct IllegalTransitionError:
    """Named error model for illegal task-state transitions (spec A0.5)."""

    var message: String

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# WaitNode
# ---------------------------------------------------------------------------

struct WaitNode(ImplicitlyCopyable, ImplicitlyDeletable):
    """Embedded intrusive wait node (spec §24).  One reusable node lives in
    each TCB so park/wake performs no per-suspension allocation.  `_generation`
    claims a waiter epoch at WAITING commit; `_reason` is why the task waits
    (intrusive list link `_next`)."""

    var _generation: Int
    var _reason: Int
    var _next: Int

    def __init__(out self):
        self._generation = 0
        self._reason = 0
        self._next = 0

    def generation(self) -> Int:
        return self._generation

    def reason(self) -> Int:
        return self._reason

    def next(self) -> Int:
        return self._next

    def set_reason(mut self, r: Int):
        self._reason = r

    def set_next(mut self, n: Int):
        self._next = n


# ---------------------------------------------------------------------------
# TaskControlBlock
# ---------------------------------------------------------------------------

struct TaskControlBlock[T: ResultValue](ImplicitlyCopyable, ImplicitlyDeletable):
    """Task control block + A0.5 task state machine (pure Mojo, no C).

    State constants (comptime Int).  The encoded machine (spec A0.5):

        NEW -> RUNNABLE -> RUNNING -> PARKING -> WAITING -> RUNNABLE
                       \\                      `- RUNNABLE (early wake)
                        \\-> COMPLETED
                         `-> CANCELLED -> COMPLETED

    Allowed transitions (any other pair raises IllegalTransitionError):

        NEW -> RUNNABLE
        RUNNABLE -> RUNNING
        RUNNING -> PARKING
        PARKING -> RUNNABLE      (early wake)
        PARKING -> WAITING
        WAITING -> RUNNABLE      (readiness or cancellation; one edge)
        RUNNING -> COMPLETED
        RUNNING -> CANCELLED
        CANCELLED -> COMPLETED
    """

    comptime NEW = Int(0)
    comptime RUNNABLE = Int(1)
    comptime RUNNING = Int(2)
    comptime PARKING = Int(3)
    comptime WAITING = Int(4)
    comptime COMPLETED = Int(5)
    comptime CANCELLED = Int(6)

    var _state: Int
    # Task generation: starts at 1; bumped on WAITING commit (PARKING->WAITING)
    # so stale wakeups from an earlier epoch are rejected (spec §25).
    var _generation: Int
    # Embedded waiter — allocation-free park/wake (spec §24).
    var _wait: WaitNode
    # Result slot + consume-once guard.
    var _result: Self.T
    var _has_result: Bool
    # Structured-concurrency links.  Cell handles (0 = none).
    var _parent: Int
    var _scope: Int
    # A2.5 (issue #71) — STARTED latch: True once the task has EVER entered
    # user code (the first RUNNABLE -> RUNNING transition — latched in
    # `_apply`, never unlatched).  A started task is worker-affine (spec
    # §19.2 / ADR-006): even when a later yield or wake returns it to
    # RUNNABLE and re-enqueues it, the latch stays True so the steal guard
    # (E4's is_pre_start consumption, issue #70) can tell "never ran" from
    # "ran, then re-queued".  The stealability test is therefore "latched
    # False", not "state==RUNNABLE".
    var _started: Bool
    # Owner-affinity (A2.2, issue #68 — E2 reserves the field for E5):
    # worker id stamped at FIRST RUN by the scheduler loop.  0 = not
    # started / not pinned (matches scheduler.wake_target_worker's
    # unpinned sentinel).  E5 reads this to route remote-ready wakes to
    # the OWNER worker (spec §19.2).
    var _owner_worker: Int
    # A2.5 (issue #71) — owner RUNTIME address + the two-phase early-wake
    # readiness latch.  `_owner_runtime` is the address of the owner
    # worker's Runtime cell (0 = none), stamped at first run by the
    # scheduler loop: the cross-worker wake route target, so a wake NEVER
    # needs a global worker registry (b2 has no function-typed fields and
    # park.mojo is JIT-importable — it cannot depend on the pool).  `_early`
    # is the PREPARE/VALIDATE/COMMIT early-wake latch (spec §23.2 / A0-T11):
    # a wake delivered while the task is still RUNNING/PARKING (before the
    # WAITING commit) latches it; park_validate() re-checks it and
    # park_commit() consumes it (unwind to RUNNABLE, never WAITING, never a
    # generation bump).  GUARD: every read/write of `_early` (and of the
    # claim decision it feeds) happens under the OWNER worker's remote-ready
    # queue spinlock — the one lock every wake path already serializes
    # through (issue #68 memory-ordering banner) — so the latch/claim and
    # the parker's commit are atomic with respect to each other.
    var _owner_runtime: Int
    var _early: Bool
    def __init__(out self):
        self._state = TaskControlBlock.NEW
        self._generation = 1
        self._wait = WaitNode()
        self._result = Self.T()
        self._has_result = False
        self._parent = 0
        self._scope = 0
        self._started = False
        self._owner_worker = 0
        self._owner_runtime = 0
        self._early = False
    # --- construction ------------------------------------------------------

    @staticmethod
    def create() -> Self:
        """Fresh TCB: state NEW, generation 1, no result, no parent/scope."""
        return Self()

    # --- state machine -----------------------------------------------------

    @staticmethod
    def _is_allowed(from_: Int, to: Int) -> Bool:
        """Central transition table (spec A0.5); single source of truth."""
        if from_ == TaskControlBlock.NEW:
            return to == TaskControlBlock.RUNNABLE
        if from_ == TaskControlBlock.RUNNABLE:
            return to == TaskControlBlock.RUNNING
        if from_ == TaskControlBlock.RUNNING:
            return (
                to == TaskControlBlock.PARKING
                or to == TaskControlBlock.COMPLETED
                or to == TaskControlBlock.CANCELLED
            )
        if from_ == TaskControlBlock.PARKING:
            return (
                to == TaskControlBlock.RUNNABLE or to == TaskControlBlock.WAITING
            )
        if from_ == TaskControlBlock.WAITING:
            return to == TaskControlBlock.RUNNABLE
        if from_ == TaskControlBlock.COMPLETED:
            return False
        if from_ == TaskControlBlock.CANCELLED:
            return to == TaskControlBlock.COMPLETED
        return False

    def _apply(mut self, to: Int):
        self._state = to
        if to == TaskControlBlock.RUNNING:
            # A2.5 (issue #71): the first body entry latches STARTED (never
            # unlatches — a re-queued started task stays observable; spec
            # §14.1/§19.1, ADR-006).
            self._started = True
        if to == TaskControlBlock.WAITING:
            self._generation += 1
            self._wait._generation = self._generation

    def transition(mut self, to: Int) raises:
        """Validate and perform a transition; raises
        IllegalTransitionError-as-Error for non-allowed pairs."""
        if not Self._is_allowed(self._state, to):
            var what = (
                "IllegalTransitionError: illegal transition "
                + String(self._state)
                + " -> "
                + String(to)
            )
            var err = IllegalTransitionError(what)
            raise Error(err.message)
        self._apply(to)

    def conditional_transition(mut self, from_: Int, to: Int) -> Bool:
        """Ordered check-then-store transition (NOT compare-and-swap): applies
        `from_` -> `to` only when the current state equals `from_` AND the
        pair is legal.  Never raises, and — unlike a real CAS — claims no M:N
        atomic ordering.  A true compare-exchange (or the acquire/consume
        generation discipline in `wake_claim`) is the A2/EPIC#2 seam; on the
        single cooperative worker there is no interleaving inside a dispatch
        slice, so the plain check is exact for today."""
        if self._state == from_ and Self._is_allowed(from_, to):
            self._apply(to)
            return True
        return False

    def wake_claim(mut self, required_gen: Int = 0) -> Bool:
        """Consume the waiter generation on the WAITING -> RUNNABLE wake
        edge (spec §23, T5 issue #51): transition WAITING -> RUNNABLE and
        claim the epoch ONLY when the expected generation matches the current
        one.  `required_gen` is the generation a producer captured at WAITING
        commit; pass 0 to always claim when WAITING (today's single worker:
        the epoch is trivially current).  Returns False — WITHOUT touching
        state — when the state is not WAITING, or when a stale `required_gen`
        from a previous epoch does not match the current generation (a cross-
        worker producer, EPIC#2, must not let a stale wake re-transition a
        task that already woke).  Never raises."""
        if self._state != TaskControlBlock.WAITING:
            return False
        if required_gen != 0 and self._generation != required_gen:
            return False
        self._apply(TaskControlBlock.RUNNABLE)
        return True

    # --- queries -----------------------------------------------------------

    def state(self) -> Int:
        return self._state

    def generation(self) -> Int:
        return self._generation

    def wait_node(mut self) -> UnsafePointer[WaitNode, MutAnyOrigin]:
        """The embedded node BY POINTER: callers (wait-list, cancellation)
        stamp reason/next in place — a by-value return would mutate a
        discarded temporary."""
        return UnsafePointer[WaitNode, MutAnyOrigin](to=self._wait)

    def parent_id(self) -> Int:
        return self._parent

    def scope_handle(self) -> Int:
        return self._scope

    def set_parent_id(mut self, id: Int):
        self._parent = id

    def set_scope_handle(mut self, h: Int):
        self._scope = h

    def owner_worker(self) -> Int:
        """The worker that first ran this task (0 = not started / not
        pinned; A2.2 issue #68, E5 surface)."""
        return self._owner_worker

    def set_owner_worker(mut self, worker_id: Int):
        """Stamp the first-run worker (called by the scheduler loop at
        dispatch; A2.2 issue #68)."""
        self._owner_worker = worker_id

    def owner_runtime(self) -> Int:
        """Address of the owner worker's Runtime cell (0 = none; stamped
        with owner_worker at first dispatch — A2.5 issue #71)."""
        return self._owner_runtime

    def set_owner_runtime(mut self, addr: Int):
        """Stamp the owner Runtime address (scheduler loop, first dispatch;
        A2.5 issue #71)."""
        self._owner_runtime = addr

    def early_readiness(self) -> Bool:
        """Two-phase early-wake latch (A0-T11): True once a wake was
        delivered while the task was RUNNING/PARKING (before the WAITING
        commit).  Guarded by the OWNER's remote-ready queue spinlock;
        consumed by park_commit (the unwind-vs-WAITING decision)."""
        return self._early

    def set_early_readiness(mut self):
        self._early = True

    def clear_early_readiness(mut self):
        self._early = False

    def is_completed(self) -> Bool:
        """Query the A0.5 machine: COMPLETED (the run/join paths)."""
        return self._state == TaskControlBlock.COMPLETED

    def is_cancelled(self) -> Bool:
        """Query the A0.5 machine: CANCELLED (the cancellation paths)."""
        return self._state == TaskControlBlock.CANCELLED

    def is_waiting(self) -> Bool:
        """Query the A0.5 machine: WAITING (the park paths)."""
        return self._state == TaskControlBlock.WAITING

    # --- STARTED latch consumption (A2.5 issue #71; E4 #70 shares) ---------

    def is_started(self) -> Bool:
        """Spec §14.1 `started`, TCB form: True exactly once this task has
        entered user code (latched at the first RUNNABLE -> RUNNING).
        A started task is worker-affine and NEVER stealable (spec §19.2 /
        ADR-006), even while its yield re-enqueue leaves it RUNNABLE."""
        return self._started

    def is_pre_start(self) -> Bool:
        """Spec §19.1: True while the task has NEVER entered user code —
        the exact stealability predicate (NEW/RUNNABLE + never ran)."""
        return not self._started

    # --- result slot -------------------------------------------------------

    def mark_result(mut self, val: Self.T):
        """Stash the task result.  Repeat calls overwrite; take_result()
        enforces consume-once."""
        self._result = val
        self._has_result = True

    def has_result_pending(self) -> Bool:
        """True while a result is stored and not yet consumed."""
        return self._has_result

    def take_result(mut self) raises -> Self.T:
        """Consume-once result.  Raises when no result is available."""
        if not self._has_result:
            raise Error(
                "TaskControlBlock.take_result: no result (never marked or "
                "already consumed)"
            )
        var out = self._result
        self._has_result = False
        return out

# ---------------------------------------------------------------------------
# Error predicates (decode the documented message prefixes)
# ---------------------------------------------------------------------------

def is_illegal_transition(e: Error) -> Bool:
    """True when `e` is an IllegalTransitionError (message begins with the
    documented prefix)."""
    return String(e).startswith("IllegalTransitionError: illegal transition ")


def is_cancellation_error(e: Error) -> Bool:
    """True when `e` is a cancellation checkpoint error."""
    return String(e).startswith("CancellationError")