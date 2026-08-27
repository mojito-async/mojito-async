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

    def __init__(out self):
        self._state = TaskControlBlock.NEW
        self._generation = 1
        self._wait = WaitNode()
        self._result = Self.T()
        self._has_result = False
        self._parent = 0
        self._scope = 0

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

    def try_transition(mut self, from_: Int, to: Int) -> Bool:
        """CAS-style transition for race tests: applies `from_` to `to` only
        when current state equals `from_` AND the pair is legal.  Never
        raises."""
        if self._state == from_ and Self._is_allowed(from_, to):
            self._apply(to)
            return True
        return False

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

    def is_completed(self) -> Bool:
        return self._state == TaskControlBlock.COMPLETED

    def is_cancelled(self) -> Bool:
        return self._state == TaskControlBlock.CANCELLED

    def is_waiting(self) -> Bool:
        return self._state == TaskControlBlock.WAITING

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
    stable "IllegalTransitionError:" prefix)."""
    return "IllegalTransitionError:" in String(e)
