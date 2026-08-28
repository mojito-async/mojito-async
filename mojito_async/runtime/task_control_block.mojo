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
# A3.2 (#42 decision, issue #61) — UNIFORM-PREFIX RELAYOUT.  The R-free
# bookkeeping moves into the non-generic `TCB_Prefix` struct (state,
# generation, wait node, consume-once flag, preserved-failure stamp, parent
# and scope links) and `TaskControlBlock[T]` becomes
# `{ _pre: TCB_Prefix; _result: Self.T }` — the T-typed result slot is the
# TAIL.  Because the prefix is a named non-generic struct at offset 0,
# an erased registry (the #42 non-generic Scope) may read and drive the
# prefix of ANY TaskControlBlock[T] through an
# `UnsafePointer[TCB_Prefix, MutAnyOrigin]` reinterpretation: the layout
# guarantee is structural (first member at offset 0), no casting through a
# canonical R is needed, and the failed/error stamp (`mark_failed`) makes
# per-child FAILURE visible erased — the linchpin for the #63/#64 grouped
# first-error lanes.  The public method surface of TaskControlBlock is
# UNCHANGED (every prefix method is delegated), so existing callers compile
# without edits.
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
# ScopeChild — the non-generic Scope's typed-boundary constraint (#42)
# ---------------------------------------------------------------------------

trait ScopeChild(ResultValue):
    """Constraint for children of the non-generic Scope (#42 decision,
    issue #61): a ResultValue outcome slot PLUS a comptime TAG used by the
    erased registry (mojito_async.scope.TaskRecord) to tag-check every
    boundary cast.  Conform with a zero-argument `__init__` and
    `comptime TAG = Int(...)` (unique within any one registry).  Lives here
    (not in scope.mojo) so leaf ResultValue types (e.g.
    integration/sys.IntResult) can conform without importing scope.mojo."""

    comptime TAG: Int


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
# TCB_Prefix — the R-free uniform prefix (A3.2, #42 decision)
# ---------------------------------------------------------------------------

struct TCB_Prefix(ImplicitlyCopyable, ImplicitlyDeletable):
    """R-free task bookkeeping + A0.5 state machine (non-generic).

    Holds everything an ERASED observer may read or drive without knowing the
    child's result type: the state machine (constants, transition table),
    generation, the embedded wait node, the consume-once result flag, the
    preserved-failure stamp (`_failed`/`_err` — set by execute()'s exception
    path; read by the scope's erased stricture and the #63 grouped joins)
    and the structured-concurrency links.

    Layout contract (#42 decision pt 6): this struct is the FIRST member of
    `TaskControlBlock[T]`, and `_result: T` is the tail — so the prefix
    offsets are IDENTICAL for every T instantiation (C layout: first member
    at offset 0).  Erased access is an UnsafePointer[TCB_Prefix]
    reinterpretation of the TCB address; the t29 mixed-type driver is the
    churn fixture proving prefix ops on Profile AND Activity cells through
    one non-generic Scope.
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
    # Result-slot consume-once guard (the T-typed value lives in the TCB tail).
    var _has_result: Bool
    # Preserved-failure stamp (#42: erased-visible child failure).
    var _failed: Bool
    var _err: String
    # Structured-concurrency links.  Cell handles (0 = none).
    var _parent: Int
    var _scope: Int

    def __init__(out self):
        self._state = TCB_Prefix.NEW
        self._generation = 1
        self._wait = WaitNode()
        self._has_result = False
        self._failed = False
        self._err = ""
        self._parent = 0
        self._scope = 0

    # --- state machine -----------------------------------------------------

    @staticmethod
    def _is_allowed(from_: Int, to: Int) -> Bool:
        """Central transition table (spec A0.5); single source of truth."""
        if from_ == TCB_Prefix.NEW:
            return to == TCB_Prefix.RUNNABLE
        if from_ == TCB_Prefix.RUNNABLE:
            return to == TCB_Prefix.RUNNING
        if from_ == TCB_Prefix.RUNNING:
            return (
                to == TCB_Prefix.PARKING
                or to == TCB_Prefix.COMPLETED
                or to == TCB_Prefix.CANCELLED
            )
        if from_ == TCB_Prefix.PARKING:
            return (
                to == TCB_Prefix.RUNNABLE or to == TCB_Prefix.WAITING
            )
        if from_ == TCB_Prefix.WAITING:
            return to == TCB_Prefix.RUNNABLE
        if from_ == TCB_Prefix.COMPLETED:
            return False
        if from_ == TCB_Prefix.CANCELLED:
            return to == TCB_Prefix.COMPLETED
        return False

    def _apply(mut self, to: Int):
        self._state = to
        if to == TCB_Prefix.WAITING:
            self._generation += 1
            self._wait._generation = self._generation

    def transition(mut self, to: Int) raises:
        """Validate and perform a transition; raises
        IllegalTransitionError-as-Error for non-allowed pairs."""
        if not TCB_Prefix._is_allowed(self._state, to):
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
        if self._state == from_ and TCB_Prefix._is_allowed(from_, to):
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
        if self._state != TCB_Prefix.WAITING:
            return False
        if required_gen != 0 and self._generation != required_gen:
            return False
        self._apply(TCB_Prefix.RUNNABLE)
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

    def is_completed(self) -> Bool:
        return self._state == TCB_Prefix.COMPLETED

    def is_cancelled(self) -> Bool:
        return self._state == TCB_Prefix.CANCELLED

    def is_waiting(self) -> Bool:
        return self._state == TCB_Prefix.WAITING

    # --- result-flag + failure stamp ---------------------------------------

    def has_result_pending(self) -> Bool:
        """True while a result is stored and not yet consumed."""
        return self._has_result

    def mark_result_flag(mut self):
        self._has_result = True

    def clear_result_flag(mut self):
        self._has_result = False

    def mark_failed(mut self, msg: String):
        """Record the preserved child failure on the PREFIX (erased-visible:
        the non-generic Scope's prefix reads and the #63/#64 grouped joins
        see it without knowing T).  Called by execute()'s exception path in
        parallel with the JoinHandle-level fail()."""
        self._failed = True
        self._err = msg

    def is_failed(self) -> Bool:
        return self._failed

    def error(self) -> String:
        return self._err


# ---------------------------------------------------------------------------
# TaskControlBlock
# ---------------------------------------------------------------------------

struct TaskControlBlock[T: ResultValue](ImplicitlyCopyable, ImplicitlyDeletable):
    """Task control block + A0.5 task state machine (pure Mojo, no C).

    Layout (A3.2, #42 decision): `_pre: TCB_Prefix` (R-free uniform prefix,
    FIRST member) + `_result: Self.T` (typed TAIL).  The state machine,
    generation discipline, wait node, consume-once flag, failure stamp and
    structured-concurrency links live in `_pre`; every prefix method is
    delegated so the public surface is unchanged.

    State constants mirror TCB_Prefix (comptime aliases; MUST stay in
    lockstep — the t29 churn fixture exercises erased prefix ops on mixed
    types to catch drift).

    Encoded machine (spec A0.5):

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

    comptime NEW = TCB_Prefix.NEW
    comptime RUNNABLE = TCB_Prefix.RUNNABLE
    comptime RUNNING = TCB_Prefix.RUNNING
    comptime PARKING = TCB_Prefix.PARKING
    comptime WAITING = TCB_Prefix.WAITING
    comptime COMPLETED = TCB_Prefix.COMPLETED
    comptime CANCELLED = TCB_Prefix.CANCELLED

    var _pre: TCB_Prefix
    # Typed result TAIL: only the typed boundary (mark_result/take_result /
    # close_typed[T]) touches it.
    var _result: Self.T

    def __init__(out self):
        self._pre = TCB_Prefix()
        self._result = Self.T()

    # --- construction ------------------------------------------------------

    @staticmethod
    def create() -> Self:
        """Fresh TCB: state NEW, generation 1, no result, no parent/scope."""
        return Self()

    # --- state machine (delegated to the prefix) ---------------------------

    def transition(mut self, to: Int) raises:
        self._pre.transition(to)

    def conditional_transition(mut self, from_: Int, to: Int) -> Bool:
        return self._pre.conditional_transition(from_, to)

    def wake_claim(mut self, required_gen: Int = 0) -> Bool:
        return self._pre.wake_claim(required_gen)

    # --- queries (delegated) ----------------------------------------------

    def state(self) -> Int:
        return self._pre.state()

    def generation(self) -> Int:
        return self._pre.generation()

    def wait_node(mut self) -> UnsafePointer[WaitNode, MutAnyOrigin]:
        return self._pre.wait_node()

    def parent_id(self) -> Int:
        return self._pre.parent_id()

    def scope_handle(self) -> Int:
        return self._pre.scope_handle()

    def set_parent_id(mut self, id: Int):
        self._pre.set_parent_id(id)

    def set_scope_handle(mut self, h: Int):
        self._pre.set_scope_handle(h)

    def is_completed(self) -> Bool:
        return self._pre.is_completed()

    def is_cancelled(self) -> Bool:
        return self._pre.is_cancelled()

    def is_waiting(self) -> Bool:
        return self._pre.is_waiting()

    def has_result_pending(self) -> Bool:
        return self._pre.has_result_pending()

    def is_failed(self) -> Bool:
        return self._pre.is_failed()

    def error(self) -> String:
        return self._pre.error()

    def mark_failed(mut self, msg: String):
        """Record the preserved child failure on the prefix (#42 erased
        visibility).  Called by execute()'s exception path alongside the
        JoinHandle-level fail()."""
        self._pre.mark_failed(msg)

    # --- result slot (typed tail) -----------------------------------------

    def mark_result(mut self, val: Self.T):
        """Stash the task result.  Repeat calls overwrite; take_result()
        enforces consume-once."""
        self._result = val
        self._pre.mark_result_flag()

    def take_result(mut self) raises -> Self.T:
        """Consume-once result.  Raises when no result is available."""
        if not self._pre.has_result_pending():
            raise Error(
                "TaskControlBlock.take_result: no result (never marked or "
                "already consumed)"
            )
        var out = self._result
        self._pre.clear_result_flag()
        return out


# ---------------------------------------------------------------------------
# Error predicates (decode the documented message prefixes)
# ---------------------------------------------------------------------------

def is_illegal_transition(e: Error) -> Bool:
    """True when `e` is an IllegalTransitionError (message begins with the
    stable "IllegalTransitionError:" prefix)."""
    return "IllegalTransitionError:" in String(e)