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

# Issue #143: Atomic[DType.int64] from std.atomic provides acquire-load /
# release-store semantics that prevent LICM hoisting of _state/_generation/
# _claim_epoch reads out of foreign-thread spin loops (the LICM-class root
# cause documented in issue #143).  Unlike @extern("C") symbols (which crash
# the b2 JIT via modular/modular#6971 when transitively imported), Atomic
# lowers through MLIR to native atomic IR (LDAR/STLR on arm64) — JIT-safe.
from std.atomic import Atomic, Ordering

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
    (intrusive list link `_next`).

    Issue #190: `_next` (the cross-worker winner/GRANT marker — the shared
    convention mutex.mojo's module header documents, reused verbatim by
    semaphore.mojo/rwlock.mojo/condvar.mojo/barrier.mojo) and `_reason` (the
    wake cause, double-duty as the winner cause per SuspendReason's
    docstring) are Atomic[DType.int64] with explicit acquire/release
    ordering, matching issue #143's treatment of TaskControlBlock._state/
    _generation/_claim_epoch: one worker's notify/wake leg stamps them and a
    DIFFERENT worker's resumed task reads them back (park.mojo's
    park_validate/park_commit, condvar.mojo's resolve_winner/notify_marker)
    across a SpinLock-guarded critical section — a plain field is not
    guaranteed fresh across that boundary at Mojo's default `-O` (the
    LICM-class miscompilation #143 documents); #175 hit exactly this gap in
    t60_barrier_cross_worker_aot before this fix closed it at the source.
    `_generation` stays plain storage here: WaitNode.generation() is only
    ever read back by the SAME owner worker that stamped it at WAITING
    commit (cancel.mojo/timer_service.mojo/timeout_scope.mojo pass it as a
    snapshot `required_gen` into unpark_current, which re-validates against
    TCB_Prefix's OWN atomic `_generation` before claiming — the field these
    callers actually race on is already #143-covered)."""

    var _generation: Int
    var _reason: Int64  # atomic acquire/release (issue #190, matches #143)
    var _next: Int64    # atomic acquire/release (issue #190, matches #143)

    def __init__(out self):
        self._generation = 0
        self._reason = Int64(0)
        self._next = Int64(0)

    def generation(self) -> Int:
        return self._generation

    def reason(mut self) -> Int:
        return Int(Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._reason)))

    def next(mut self) -> Int:
        return Int(Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._next)))

    def set_reason(mut self, r: Int):
        Atomic[DType.int64].store[ordering=Ordering.RELEASE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._reason), Int64(r))

    def set_next(mut self, n: Int):
        Atomic[DType.int64].store[ordering=Ordering.RELEASE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._next), Int64(n))


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

    var _state: Int64        # atomic acquire/release (issue #143)
    # Task generation: starts at 1; bumped on WAITING commit (PARKING->WAITING)
    # so stale wakeups from an earlier epoch are rejected (spec §25).
    var _generation: Int64   # atomic acquire/release (issue #143)
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
    # the parker's commit are atomic with respect to each other.  Issue
    # #190: the guard alone does not stop the optimizer from caching a
    # plain field read across the lock/unlock boundary at default `-O`
    # (the same LICM-class gap #143 closed for _state/_generation/
    # _claim_epoch — #175 hit the WaitNode half of it in
    # t60_barrier_cross_worker_aot), so `_early` is Atomic[DType.uint8]
    # (0/1 latch) with explicit acquire/release ordering, read/written
    # through the same UnsafePointer-cast pattern #143 established.
    var _owner_runtime: Int
    var _early: UInt8  # atomic acquire/release (issue #190, matches #143)
    # H2 (PR #109) — CLAIMED-EPOCH marker: the generation of the last
    # successful wake claim (0 = no claim consumed yet).  The WAKE leg
    # stamps it exactly when wake_claim() consumes a WAITING epoch; a
    # later wake carrying the SAME required_gen is by definition a
    # DUPLICATE of that claim — quiet no-op in every task state (RUNNING,
    # PARKING, RUNNABLE, COMPLETED, CANCELLED), even though the generation
    # counter itself may still equal required_gen (it only bumps at the
    # NEXT WAITING commit).  GUARD: read/written under the OWNER worker's
    # remote-ready queue spinlock, alongside `_early` and the claim
    # decision (issue #68 memory-ordering banner).
    var _claim_epoch: Int64  # atomic acquire/release (issue #143)
    def __init__(out self):
        self._state = Int64(TCB_Prefix.NEW)
        self._generation = Int64(1)
        self._wait = WaitNode()
        self._has_result = False
        self._failed = False
        self._err = ""
        self._parent = 0
        self._scope = 0
        self._started = False
        self._owner_worker = 0
        self._owner_runtime = 0
        self._early = UInt8(0)
        self._claim_epoch = Int64(0)
    # --- construction ------------------------------------------------------

    @staticmethod
    def create() -> Self:
        """Fresh TCB: state NEW, generation 1, no result, no parent/scope."""
        return Self()

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
        Atomic[DType.int64].store[ordering=Ordering.RELEASE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._state), Int64(to))
        if to == TaskControlBlock.RUNNING:
            # A2.5 (issue #71): the first body entry latches STARTED (never
            # unlatches — a re-queued started task stays observable; spec
            # §14.1/§19.1, ADR-006).
            self._started = True
        if to == TaskControlBlock.WAITING:
            var new_gen = self._generation + Int64(1)
            Atomic[DType.int64].store[ordering=Ordering.RELEASE](
                UnsafePointer[Int64, MutAnyOrigin](to=self._generation), new_gen)
            self._wait._generation = Int(new_gen)

    def transition(mut self, to: Int) raises:
        """Validate and perform a transition; raises
        IllegalTransitionError-as-Error for non-allowed pairs."""
        # Issue #190: read the CURRENT state through the atomic getter, not
        # the plain field — matches wake_claim below; a foreign-thread wake
        # (unpark_current's loud-raise tail) can reach this same check.
        var cur = self.state()
        if not TCB_Prefix._is_allowed(cur, to):
            var what = (
                "IllegalTransitionError: illegal transition "
                + String(cur)
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
        if self.state() == from_ and TCB_Prefix._is_allowed(from_, to):
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
        task that already woke).  Never raises.

        H2 (PR #109): a SUCCESSFUL claim stamps `_claim_epoch` = the claimed
        generation (the current one at claim time) — the DUPLICATE detector
        for unpark_current: any later wake carrying that same required_gen
        is a duplicate of this claim and must no-op quietly in every state
        (the generation counter alone cannot tell, since it only bumps at
        the NEXT WAITING commit).

        Issue #190: this is the OTHER half of the same LICM-class gap the
        WaitNode fields closed — `wake_claim` is the one place a REMOTE
        thread's `unpark_current` reads TCB_Prefix's own `_state`/
        `_generation` (under the owner's remote-ready queue guard, the same
        guard the WAITING-committing side holds in `_apply`/`transition`).
        #143 made the STORAGE atomic and added atomic external getters
        (`state()`/`generation()`), but this method's own checks still read
        the plain fields directly, leaving exactly the gap #143 intended to
        close open on the read side a foreign thread actually exercises on
        every cross-worker wake claim.  Routed through the atomic getters
        below instead."""
        if self.state() != TaskControlBlock.WAITING:
            return False
        if required_gen != 0 and self.generation() != required_gen:
            return False
        self._apply(TaskControlBlock.RUNNABLE)
        Atomic[DType.int64].store[ordering=Ordering.RELEASE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._claim_epoch),
            Int64(self.generation()))
        return True

    # --- queries -----------------------------------------------------------

    def state(mut self) -> Int:
        return Int(Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._state)))

    def generation(mut self) -> Int:
        return Int(Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._generation)))

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

    def early_readiness(mut self) -> Bool:
        """Two-phase early-wake latch (A0-T11): True once a wake was
        delivered while the task was RUNNING/PARKING (before the WAITING
        commit).  Guarded by the OWNER's remote-ready queue spinlock;
        consumed by park_commit (the unwind-vs-WAITING decision)."""
        return Atomic[DType.uint8].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[UInt8, MutAnyOrigin](to=self._early)) != 0

    def set_early_readiness(mut self):
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](
            UnsafePointer[UInt8, MutAnyOrigin](to=self._early), UInt8(1))

    def clear_early_readiness(mut self):
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](
            UnsafePointer[UInt8, MutAnyOrigin](to=self._early), UInt8(0))

    def claimed_epoch(mut self) -> Int:
        """H2 (PR #109): the generation of the last consumed wake claim."""
        return Int(Atomic[DType.int64].load[ordering=Ordering.ACQUIRE](
            UnsafePointer[Int64, MutAnyOrigin](to=self._claim_epoch)))
    def is_completed(self) -> Bool:
        return self._state == Int64(TaskControlBlock.COMPLETED)

    def is_cancelled(self) -> Bool:
        return self._state == Int64(TaskControlBlock.CANCELLED)

    def is_waiting(self) -> Bool:
        return self._state == Int64(TaskControlBlock.WAITING)

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

    def state(mut self) -> Int:
        return self._pre.state()

    def generation(mut self) -> Int:
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

    def owner_worker(self) -> Int:
        """The worker that first ran this task (0 = not started / not
        pinned; A2.2 issue #68, E5 surface)."""
        return self._pre.owner_worker()

    def set_owner_worker(mut self, worker_id: Int):
        """Stamp the first-run worker (called by the scheduler loop at
        dispatch; A2.2 issue #68)."""
        self._pre.set_owner_worker(worker_id)

    def owner_runtime(self) -> Int:
        """Address of the owner worker's Runtime cell (0 = none; stamped
        with owner_worker at first dispatch — A2.5 issue #71)."""
        return self._pre.owner_runtime()

    def set_owner_runtime(mut self, addr: Int):
        """Stamp the owner Runtime address (scheduler loop, first dispatch;
        A2.5 issue #71)."""
        self._pre.set_owner_runtime(addr)

    def early_readiness(mut self) -> Bool:
        """Two-phase early-wake latch (A0-T11): True once a wake was
        delivered while the task was RUNNING/PARKING (before the WAITING
        commit).  Guarded by the OWNER's remote-ready queue spinlock;
        consumed by park_commit (the unwind-vs-WAITING decision)."""
        return self._pre.early_readiness()

    def set_early_readiness(mut self):
        self._pre.set_early_readiness()

    def clear_early_readiness(mut self):
        self._pre.clear_early_readiness()

    def claimed_epoch(mut self) -> Int:
        """H2 (PR #109): the generation of the last consumed wake claim."""
        return self._pre.claimed_epoch()

    # --- STARTED latch consumption (A2.5 issue #71; E4 #70 shares) ---------

    def is_started(self) -> Bool:
        """Spec §14.1 `started`, TCB form: True exactly once this task has
        entered user code (latched at the first RUNNABLE -> RUNNING).
        A started task is worker-affine and NEVER stealable (spec §19.2 /
        ADR-006), even while its yield re-enqueue leaves it RUNNABLE."""
        return self._pre.is_started()

    def is_pre_start(self) -> Bool:
        """Spec §19.1: True while the task has NEVER entered user code —
        the exact stealability predicate (NEW/RUNNABLE + never ran)."""
        return self._pre.is_pre_start()

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
    documented prefix)."""
    return String(e).startswith("IllegalTransitionError: illegal transition ")


def is_cancellation_error(e: Error) -> Bool:
    """True when `e` is a cancellation checkpoint error."""
    return String(e).startswith("CancellationError")
