# spike/colorless_runtime/task.mojo
#
# A0.3 (issue #12) — minimal pure-Mojo TaskControlBlock + task state machine.
#
# This lane is deliberately free of context switching and of the vendored C
# substrate: it proves the A0.5 task state machine (ownership of every legal
# transition) and the TCB metadata layout before the fiber/queue lanes build
# on top.  Runs with `mojo run` alone — no dylib, no -Xlinker.
#
# Mojo 1.0.0b2 dialect notes (see spike/colorless_runtime/vendor/mojito-sys/
# mojito_spike.mojo for the same conventions):
#   - `def`, not `fn`; `alias`/`fn` are removed.
#   - Mutable instance methods take `mut self`; constructors take `out self`.
#   - `comptime NAME = value` for compile-time constants and type aliases.
#   - Parameters are passed by immutable value: a generic value can only be
#     *copied* into a slot.  `take_result()` therefore returns a copy guarded
#     by a consume-once flag; true move-out semantics belong to the A0.6
#     JoinHandle lane (spec §9.1 linear result).
#   - `raise` accepts only the builtin `Error` (no user error classes yet);
#     the named IllegalTransitionError model is carried in the Error message.

# ---------------------------------------------------------------------------
# Result slot constraint
# ---------------------------------------------------------------------------

trait ResultValue(ImplicitlyCopyable, ImplicitlyDeletable):
    """Minimal constraint for the TCB result slot in this lane.

    b2 passes method parameters by immutable value, so the result slot holds
    a *copyable, default-constructible* value.  `take_result()` returns a
    copy and raises on the second call (consume-once).  The full spec pushes
    large/movable results into cold state handled by JoinHandle (A0.6).

    Conform by providing a zero-argument ``__init__``:

        struct TRes(ResultValue):
            var v: Int
            def __init__(out self): self.v = 0
            def __init__(out self, v: Int): self.v = v
    """

    def __init__(out self): ...


# ---------------------------------------------------------------------------
# IllegalTransitionError (error model)
# ---------------------------------------------------------------------------

struct IllegalTransitionError:
    """Named error model for illegal task-state transitions (spec A0.5).

    b2 (1.0.0b2) only supports raising the builtin ``Error`` and has no
    user-extensible error classes, so ``TaskControlBlock.transition()``
    raises ``Error`` whose message is built by this type:

        "IllegalTransitionError: <from> -> <to>"

    Keep the identifier so the raised condition is nameable in code and in
    test diagnostics; the catch side detects it as ``Error`` raised by
    ``transition()`` (the only raise that method performs).
    """

    var message: String

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# WaitNode
# ---------------------------------------------------------------------------

struct WaitNode(ImplicitlyCopyable, ImplicitlyDeletable):
    """Embedded intrusive wait node (spec §24, minimal form).

    One reusable node lives inside each TCB so park/wake performs no
    per-suspension allocation.

    ``_generation`` — waiter epoch.  Claimed when the task commits to
    WAITING; a stale wakeup from an earlier epoch is rejected by comparing
    against the TCB's current generation (spec §25 'A wake operation MUST be
    idempotent with respect to a waiter generation').

    ``_reason`` — why the task waits (readiness, cancellation, ...).  Open
    set; scheduler lanes (A0.7) define and stamp the values.

    ``_next`` — intrusive list link (parked/waiting task chain).

    Links are Int handles for now (allocation-free, single-worker spike);
    later lanes may switch to UnsafePointer[WaitNode, MutUntrackedOrigin].
    """

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

struct TaskControlBlock[T: ResultValue] (ImplicitlyCopyable, ImplicitlyDeletable):
    """Task control block + A0.5 task state machine (pure Mojo, no C).

    State constants (comptime Int).  The encoded state machine (spec A0.5):

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
        WAITING -> RUNNABLE      (readiness)
        WAITING -> RUNNABLE      (cancellation — same edge, two causes)
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

    # --- state -------------------------------------------------------------
    var _state: Int
    # Task generation: starts at 1; bumped on every park commit (WAITING
    # entry) so stale wakeups from a previous wait epoch are rejected.
    var _generation: Int
    # Embedded waiter — allocation-free park/wake (spec §24).
    var _wait: WaitNode
    # Result slot: copyable value + consume-once flag (see ResultValue).
    var _result: Self.T
    var _has_result: Bool
    # Structured-concurrency links.  Int handles for this lane: parent TCB
    # handle (0 = none) and scope handle (Int for now, per A0_PLAN).
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
        """Perform a validated transition, claiming a fresh wait epoch when
        the task commits to WAITING (stale-wakeup defense, spec §24/§25)."""
        self._state = to
        if to == TaskControlBlock.WAITING:
            self._generation += 1
            self._wait._generation = self._generation

    def transition(mut self, to: Int) raises:
        """Validate and perform NEW -> `to`.  Raises IllegalTransitionError
        (modeled as Error; see IllegalTransitionError doc) when the pair is
        not in the A0.5 table."""
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
        """CAS-style transition for race tests (spec A0.5).

        Applies `from_ -> to` only when the current state equals `from_` AND
        the pair is legal; returns True on success, False otherwise.  Never
        raises.  (Single-worker spike: no true atomic; the semantic is the
        point.)"""
        if self._state == from_ and Self._is_allowed(from_, to):
            self._apply(to)
            return True
        return False

    # --- queries -----------------------------------------------------------

    def state(self) -> Int:
        return self._state

    def generation(self) -> Int:
        return self._generation

    def wait_node(self) -> WaitNode:
        return self._wait

    def parent_id(self) -> Int:
        return self._parent

    def scope_handle(self) -> Int:
        return self._scope

    def is_completed(self) -> Bool:
        return self._state == TaskControlBlock.COMPLETED

    def is_cancelled(self) -> Bool:
        return self._state == TaskControlBlock.CANCELLED

    # --- result slot -------------------------------------------------------

    def mark_result(mut self, val: Self.T):
        """Stash the task result.  Repeat calls overwrite; take_result()
        enforces consume-once."""
        self._result = val
        self._has_result = True

    def take_result(mut self) raises -> Self.T:
        """Consume-once result read.  Raises when no result is available —
        either never marked or already taken (second take_result raises)."""
        if not self._has_result:
            raise Error(
                "TaskControlBlock.take_result: no result (never marked or "
                "already consumed)"
            )
        self._has_result = False
        return self._result