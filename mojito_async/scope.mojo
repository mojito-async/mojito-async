# mojito_async/scope.mojo
#
# A1.1 runtime (issue #33) — structured-concurrency scope with a
# JOIN-INTEGRATED close.
#
# A3.2 (issue #61) — #42 DECISION (ADR-015, full text lands with the A3
# merge): Scope becomes NON-GENERIC.  Summary of the adopted strategy:
#
#   * `struct Scope` (NO type parameters) with typed access ONLY at typed
#     call sites: `scope.spawn[T](rt, tcb, parent_id) -> JoinHandle[T]`
#     per child (spec §8 spelling), `scope.register[T](...)`,
#     `scope.lookup[T](...) -> ptr TCB_Prefix`, `scope.close_typed[T](...)`.
#   * The registry ADDRESS-ERASES children into TaskRecord{addr, id, tag}
#     cells (the proven house TaskRecord pattern); tag = the child's
#     comptime ScopeChild.TAG, stamped at the (typed) registration boundary.
#   * Any boundary cast is COMPTIME-TAG-CHECKED: lookup[T] / close_typed[T]
#     verify record.tag == T.TAG and raise the deterministic
#     `ScopeTagMismatch` (message prefix "ScopeTagMismatch:") otherwise.
#   * close() is ERASED VALIDATE-ONLY (decision pt 4): it checks every child
#     COMPLETED through the R-free TCB_Prefix (see
#     runtime/task_control_block.mojo: the prefix struct is the layout
#     guarantee for erased access — first member at offset 0, T-typed result
#     TAIL), then marks closed and drops the registry; it NEVER consumes an
#     untyped result.  HOMOGENEOUS scopes keep the join-integrated typed
#     reap through close_typed[T] (tag-checked, consume-once).  MIXED scopes
#     close validate-only and reap by parent handles — documented.
#   * Failure policy seam: `request_cancel_all()` drives the erased prefix —
#     RUNNING children are transitioned CANCELLED, WAITING children are
#     woken via wake_claim; NEW/RUNNABLE children are untouched (their
#     cancellation requires the #54 flag tree).  Best-effort (never raises
#     on a child the machine cannot cancel); the full tree propagation is
#     lane #54.
#   * Spec §66 (results not Copyable) is DECOUPLED from this decision
#     (un-landable under any strategy on b2; future work).
#
# A1.1 semantics carried forward: close REFUSES (ChildrenStillLive, spec
# A0-T13/T14) while a genuinely-live (not-yet-COMPLETED) child or an open
# direct subscope remains; the nested-scope ordering (inner-before-outer,
# parent refuses close while a subscope is open) and the `drop_children`
# abort escape hatch are kept verbatim.
#
# NEW root ergonomic (spec §13/§113, issue #61): `with_scope(rt, body, ud)`
# — creates the root scope, runs `body(rt, scope, ud)`, closes (joins) it on
# normal return, and on a body error propagates the FIRST error after
# cancellation-requesting siblings (§8.2 default policy).  b2 has no `with`
# context manager or TLS, so the runtime is threaded explicitly; the §113
# prototype shape is transcribed onto this surface (see t29_with_scope).
#
# Mojo 1.0.0b2 dialect: `def` only; no static methods -> module factories
# make_scope / make_nested_scope; Scope holds List fields so it is NOT
# ImplicitlyCopyable — callers operate through UnsafePointer; absent optional
# pointers are Optional (b2 pointers carry no null).
from std.collections import List
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.join_handle import JoinHandle
from mojito_async.runtime.task_control_block import (
    ScopeChild,
    TCB_Prefix,
    TaskControlBlock,
)


# ---------------------------------------------------------------------------
# ChildrenStillLive (error model)
# ---------------------------------------------------------------------------

struct ChildrenStillLive:
    """Named error model for refusing scope exit with live children
    (spec A0-T13/T14).  Carried in the message."""

    var message: String

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# ScopeTagMismatch (error model — the #42 negative test)
# ---------------------------------------------------------------------------

struct ScopeTagMismatch:
    """Named error model for a comptime-tag-checked boundary cast that named
    the WRONG child type: `lookup[T]`/`close_typed[T]` on a registry entry
    recorded under a different ScopeChild.TAG.  Deterministic (#42 pt 3)."""

    var message: String

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# CancelHook — injected cancellation callback (failure-policy seam, #64)
# ---------------------------------------------------------------------------
#
# Retired as a Scope TYPE PARAMETER by the #42 non-generic conversion (issue
# #61) — request_cancel_all() above now drives sibling cancellation directly
# through the erased TCB_Prefix for the with_scope root ergonomic.  The
# A3.5 failure policy (issue #64) still wants a COOPERATIVE, flag-observable
# cancellation signal (the shipped mojito_async.cancellation_adapter
# CancelFlagHook) at the moment a primary error is RECORDED, so the trait
# survives as a PER-CALL generic constraint on record_failure[H] below —
# the same pattern #61 already uses for register[T]/spawn[T]/lookup[T]: the
# STRUCT stays non-generic; only the method is parametrically polymorphic
# over the caller-supplied hook.

trait CancelHook(ImplicitlyCopyable, Movable, ImplicitlyDeletable):
    """Injection point for the sibling cancellation policy (spec A0-T14)."""

    def request_cancel(mut self, scope_handle: Int, child_handle: Int) raises:
        ...


# ---------------------------------------------------------------------------
# ScopeChild is defined in runtime/task_control_block.mojo (re-exported here
# via the import above) so leaf ResultValue types can conform without
# importing scope.mojo (avoids a scope.mojo <-> integration/sys.mojo cycle).
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# TaskRecord — erased registry cell (the house pattern)
# ---------------------------------------------------------------------------

struct TaskRecord(ImplicitlyCopyable, Movable, ImplicitlyDeletable):
    """Address-erased child cell: the TCB's raw address, the runtime task id
    (registry key), and the comptime tag of the child's STATIC type.  Typed
    access (join/reap/lookup) happens ONLY at typed call sites; every cast
    across the erased boundary is tag-checked.  Trivially copyable (three
    scalar Ints, no owned resource) — swap-remove in unregister()/close()
    copies cells by value, same as the pre-#42 parallel-list registry."""

    var addr: Int
    var id: Int
    var tag: Int

    def __init__(out self, a: Int, i: Int, t: Int):
        self.addr = a
        self.id = i
        self.tag = t


# ---------------------------------------------------------------------------
# Scope — non-generic structured-concurrency scope
# ---------------------------------------------------------------------------

struct Scope(Movable, ImplicitlyDeletable):
    """Structured-concurrency scope owning an ADDRESS-ERASED child registry
    (#42 decision): `_children` TaskRecord cells append only on register
    growth.  `_open` gates register/close; `_parent` link and
    `_open_subscopes` enforce inner-before-outer.  `_order_log` records close
    order for tests/diagnostics.  No type parameters: typed access is
    per-call-site (`spawn[T]`, `register[T]`, `lookup[T]`, `close_typed[T]`).
    """

    var _handle: Int
    var _open: Bool
    var _children: List[TaskRecord]
    # Close-order log shared with sibling scopes (records handle on close).
    var _order_log: Optional[UnsafePointer[List[Int], MutAnyOrigin]]
    # Parent-scope link (empty when root).
    var _parent: Optional[UnsafePointer[Scope, MutAnyOrigin]]
    # Open direct subscopes.
    var _open_subscopes: Int
    # Failure record (issue #64): first-RECORDED failure is the primary
    # (handle + message); later failures are suppressed.  `_failed` counts
    # EVERY recorded failure (no error is lost); `_raised` marks the primary
    # consumed-on-raise (exactly-once).
    var _primary_handle: Int
    var _primary_msg: String
    var _suppressed: Int
    var _failed: Int
    var _raised: Bool

    def __init__(
        out self,
        handle: Int,
        order_log: Optional[UnsafePointer[List[Int], MutAnyOrigin]],
        parent: Optional[UnsafePointer[Scope, MutAnyOrigin]],
    ):
        self._handle = handle
        self._open = True
        self._children = List[TaskRecord]()
        self._order_log = order_log
        self._parent = parent
        self._open_subscopes = 0
        self._primary_handle = 0
        self._primary_msg = ""
        self._suppressed = 0
        self._failed = 0
        self._raised = False
        if self._parent:
            self._parent.value()[]._open_subscopes += 1

    # --- queries -----------------------------------------------------------

    def handle(self) -> Int:
        return self._handle

    def is_open(self) -> Bool:
        return self._open

    def live_child_count(self) -> Int:
        return len(self._children)

    def is_registered(self, child_handle: Int) -> Bool:
        for i in range(len(self._children)):
            if self._children[i].id == child_handle:
                return True
        return False

    def open_subscopes(self) -> Int:
        return self._open_subscopes

    # --- failure-policy queries (issue #64) --------------------------------

    def has_primary_error(self) -> Bool:
        """True while a primary error is recorded and not yet raised."""
        return self._failed > 0 and not self._raised

    def failed_count(self) -> Int:
        """Total RECORDED failures (primary + suppressed): no error is lost."""
        return self._failed

    def suppressed_count(self) -> Int:
        """Failures recorded after the primary (kept observable)."""
        return self._suppressed

    def has_live_unfinished(self) -> Bool:
        """True when a registered child is not yet COMPLETED (genuinely live;
        the close() join would have nothing to consume).  Erased read: the
        R-free prefix is the same struct for every T."""
        for i in range(len(self._children)):
            var pre = UnsafePointer[TCB_Prefix, MutAnyOrigin](
                unsafe_from_address=self._children[i].addr
            )
            if not pre[].is_completed():
                return True
        return False

    # --- registry (typed boundary stamps tag + addr; erased storage) -------

    def register[T: ScopeChild](
        mut self,
        child: UnsafePointer[TaskControlBlock[T], MutAnyOrigin],
        task_id: Int,
        parent_task_id: Int,
    ) raises -> Int:
        """Register a child under its RUNTIME task id (the registry key).
        Refuses a closed scope and a child that already names another scope.
        Returns the task id (so the caller's JoinHandle id == registry key —
        is_registered(handle.id()) is exact, not coincidental)."""
        if not self._open:
            var err = ChildrenStillLive(
                "ScopeClosed: register into closed scope "
                + String(self._handle)
            )
            raise Error(err.message)
        var prior = child[].scope_handle()
        if prior != 0 and prior != self._handle:
            var err = ChildrenStillLive(
                "ChildrenStillLive: child already names scope "
                + String(prior)
            )
            raise Error(err.message)
        child[].set_scope_handle(self._handle)
        child[].set_parent_id(parent_task_id)
        self._children.append(TaskRecord(Int(child), task_id, T.TAG))
        return task_id

    def unregister(mut self, child_id: Int) raises:
        """Remove a child by its runtime task id (swap-remove).  Validates
        the child still names THIS scope; refuses unknown children."""
        for i in range(len(self._children)):
            if self._children[i].id == child_id:
                var pre = UnsafePointer[TCB_Prefix, MutAnyOrigin](
                    unsafe_from_address=self._children[i].addr
                )
                if pre[].scope_handle() != self._handle:
                    var err = ChildrenStillLive(
                        "UnknownChild: child "
                        + String(child_id)
                        + " does not name scope "
                        + String(self._handle)
                    )
                    raise Error(err.message)
                var last = len(self._children) - 1
                self._children[i] = self._children[last]
                _ = self._children.pop(last)
                return
        var err = ChildrenStillLive(
            "UnknownChild: unregister of unknown child "
            + String(child_id)
            + " from scope "
            + String(self._handle)
        )
        raise Error(err.message)

    # --- typed spawn (per-child, spec §8) ----------------------------------

    def spawn[T: ScopeChild](
        mut self,
        mut rt: Runtime,
        tcb: UnsafePointer[TaskControlBlock[T], MutAnyOrigin],
        parent_id: Int,
    ) raises -> JoinHandle[T]:
        """SCOPE-AWARE spawn (issue #40/#61): AUTO-REGISTERS the child in
        this scope and enqueues it as a NEW RUNNABLE task; returns the TYPED
        single-owner JoinHandle[T] (typed access at the typed call site, #42
        pt 2).  INV-3 inspection: registration is STRUCTURAL here — the child
        cannot become a task without also becoming a member of this scope,
        because register() (the registry's single owner) stamps scope_handle
        and parent on the child, refuses a CLOSED scope (ScopeClosed), and
        refuses a child that already names a DIFFERENT scope.  Work-first:
        spawn only REGISTERS the child as runnable; its first entry happens
        when execute() or a scheduler trampoline reaches it."""
        if rt.is_shutdown():
            raise Error("mojito_async.scope.spawn: runtime is shut down")
        tcb[].transition(TaskControlBlock.RUNNABLE)
        var id = rt.next_id()
        _ = self.register[T](tcb, id, parent_id)
        rt.enqueue(Int(tcb), id)
        return JoinHandle[T](tcb, id)

    # --- typed boundary cast (comptime-tag-checked) ------------------------

    def lookup[T: ScopeChild](
        mut self, child_id: Int
    ) raises -> UnsafePointer[TCB_Prefix, MutAnyOrigin]:
        """The comptime-tag-checked ERASED read: resolve a child's R-free
        prefix by id.  The tag recorded at registration MUST equal T.TAG —
        any wrong-type cast raises ScopeTagMismatch deterministically (the
        #42 negative test) instead of misreading memory."""
        for i in range(len(self._children)):
            if self._children[i].id == child_id:
                if self._children[i].tag != T.TAG:
                    var err = ScopeTagMismatch(
                        "ScopeTagMismatch: child "
                        + String(child_id)
                        + " recorded under tag "
                        + String(self._children[i].tag)
                        + ", lookup named tag "
                        + String(T.TAG)
                    )
                    raise Error(err.message)
                return UnsafePointer[TCB_Prefix, MutAnyOrigin](
                    unsafe_from_address=self._children[i].addr
                )
        var err = ChildrenStillLive(
            "UnknownChild: lookup of unknown child "
            + String(child_id)
            + " from scope "
            + String(self._handle)
        )
        raise Error(err.message)

    # --- failure policy seam (erased; #54 lands the flag tree) -------------

    def request_cancel_all(mut self) raises:
        """Request cancellation of every currently-registered child through
        the ERASED R-free prefix (#42 pt 2): RUNNING children are
        transitioned CANCELLED; WAITING children are woken (wake_claim,
        WAITING -> RUNNABLE) so their next checkpoint observes the request;
        NEW/RUNNABLE children are LEFT UNTOUCHED (the machine has no edge to
        cancel them, and their cancellation needs the #54 flag tree).
        Best-effort — a child the machine cannot cancel is skipped, never an
        error: this is the §8.2 failure-policy seam with_scope drives on a
        body error."""
        for i in range(len(self._children)):
            var pre = UnsafePointer[TCB_Prefix, MutAnyOrigin](
                unsafe_from_address=self._children[i].addr
            )
            var st = pre[].state()
            if st == TaskControlBlock.RUNNING:
                try:
                    pre[].transition(TaskControlBlock.CANCELLED)
                except Error:
                    _ = 0  # best-effort: never fail the sweep on a race
            elif st == TaskControlBlock.WAITING:
                _ = pre[].wake_claim()

    # --- failure policy (issue #64: record primary, cancel siblings via a
    # per-call CancelHook, raise once at the boundary) ----------------------

    def record_failure[H: CancelHook](
        mut self, child_handle: Int, msg: String, mut hook: H
    ) raises:
        """Record a child failure under the first-error failure policy.

        FIRST-RECORDED wins (documented ordering: first-RECORDED, not
        first-finished): the first record_failure becomes the PRIMARY error
        and cancels every sibling through the caller-supplied CancelHook —
        the cancel-tree #54-ready COOPERATIVE seam, distinct from the
        best-effort erased request_cancel_all() above — once, in registry
        order, skipping the failed child itself.  Later failures are
        recorded-but-not-primary (suppressed) and never re-cancel siblings;
        `failed_count` totals every record so no error is lost.  Refuses an
        unknown child."""
        if not self.is_registered(child_handle):
            var err = ChildrenStillLive(
                "UnknownChild: record_failure of unknown child "
                + String(child_handle)
                + " from scope "
                + String(self._handle)
            )
            raise Error(err.message)
        if self._failed == 0:
            self._primary_handle = child_handle
            self._primary_msg = msg
            for i in range(len(self._children)):
                var sid = self._children[i].id
                if sid != child_handle:
                    hook.request_cancel(self._handle, sid)
        else:
            self._suppressed += 1
        self._failed += 1

    def raise_primary(mut self) raises:
        """Deferred raise surface (first_error-style): raise the recorded
        primary error exactly once.  No-op when no primary is recorded or it
        was already consumed by an earlier raise/close."""
        if self._failed > 0 and not self._raised:
            self._raised = True
            raise Error(self._primary_msg)

    # --- containment -------------------------------------------------------

    def drop_children(mut self):
        """Containment escape hatch: drop every child reference without
        individual unregistration (abort paths / scope teardown)."""
        while len(self._children) > 0:
            _ = self._children.pop(len(self._children) - 1)

    # --- close (erased validate-only + typed reap variant) ------------------

    def _validate_exit(mut self) raises:
        """Shared validations: scope open, every registered child COMPLETED
        (erased prefix), no open direct subscope.  On ANY violation raise
        ChildrenStillLive BEFORE consuming anything (nothing is joined, no
        result is taken, the scope stays open — a caller can fix the
        violation and retry)."""
        if not self._open:
            var err = ChildrenStillLive(
                "DoubleClose: double close of scope " + String(self._handle)
            )
            raise Error(err.message)
        for i in range(len(self._children)):
            var pre = UnsafePointer[TCB_Prefix, MutAnyOrigin](
                unsafe_from_address=self._children[i].addr
            )
            if not pre[].is_completed():
                var err = ChildrenStillLive(
                    "ChildrenStillLive: scope "
                    + String(self._handle)
                    + " exit refused with an unfinished live child"
                )
                raise Error(err.message)
        if self._open_subscopes != 0:
            var err = ChildrenStillLive(
                "ChildrenStillLive: scope "
                + String(self._handle)
                + " exit refused with "
                + String(self._open_subscopes)
                + " open subscopes"
            )
            raise Error(err.message)

    def _close_bookkeeping(mut self):
        self._open = False
        self.drop_children()
        if self._order_log:
            self._order_log.value()[].append(self._handle)
        if self._parent:
            self._parent.value()[]._subscope_closed()

    def close(mut self, mut rt: Runtime) raises:
        """ERASED VALIDATE-ONLY close (#42 decision pt 4): verify the scope
        is open, every registered child is COMPLETED, and no open direct
        subscope remains (two-phase validate-then-consume; on ANY violation
        nothing is consumed and the scope stays open).  Then mark closed and
        drop the registry.  The scope's registry is erased, so close()
        NEVER consumes results: callers reap typed results through their
        JoinHandles (or use close_typed[T] on homogeneous scopes for the
        join-integrated typed reap).  `rt` is RESERVED for the engine-driven
        join of a later lane.

        Phase 3 (issue #64): after closing, raise the recorded primary error
        exactly once (raise_primary — consumed on raise); a later close then
        refuses with DoubleClose instead of re-raising it."""
        self._validate_exit()
        self._close_bookkeeping()
        self.raise_primary()

    def close_typed[T: ScopeChild](mut self, mut rt: Runtime) raises:
        """TYPED join-integrated close for HOMOGENEOUS scopes (#42 pt 4):
        validate as close(), then tag-check EVERY registered child against
        T.TAG — ANY mismatch raises ScopeTagMismatch BEFORE anything is
        consumed (the #42 negative test) — then consume-once take_result of
        each settled child through the typed boundary.  The scope is the
        final joiner for children never individually reaped.

        Phase 3 (issue #64): after closing, raise the recorded primary error
        exactly once (raise_primary — consumed on raise)."""
        self._validate_exit()
        for i in range(len(self._children)):
            if self._children[i].tag != T.TAG:
                var err = ScopeTagMismatch(
                    "ScopeTagMismatch: typed reap of scope "
                    + String(self._handle)
                    + " named tag "
                    + String(T.TAG)
                    + " but child "
                    + String(self._children[i].id)
                    + " is recorded under tag "
                    + String(self._children[i].tag)
                    + " (mixed scope: validate-only close + reap-by-handle)"
                )
                raise Error(err.message)
        for i in range(len(self._children)):
            var pre = UnsafePointer[TCB_Prefix, MutAnyOrigin](
                unsafe_from_address=self._children[i].addr
            )
            if pre[].has_result_pending():
                var tc = UnsafePointer[TaskControlBlock[T], MutAnyOrigin](
                    unsafe_from_address=self._children[i].addr
                )
                _ = tc[].take_result()
        self._close_bookkeeping()
        self.raise_primary()

    def _subscope_closed(mut self):
        self._open_subscopes -= 1


# ---------------------------------------------------------------------------
# Module-level factories (b2: no static methods)
# ---------------------------------------------------------------------------

def _opt_log(
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
) -> Optional[UnsafePointer[List[Int], MutAnyOrigin]]:
    if has_log:
        return Optional[UnsafePointer[List[Int], MutAnyOrigin]](order_log)
    return Optional[UnsafePointer[List[Int], MutAnyOrigin]]()


def make_scope(
    handle: Int,
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
) -> Scope:
    """Root scope: no parent link, zero open subscopes."""
    var no_parent = Optional[UnsafePointer[Scope, MutAnyOrigin]]()
    return Scope(handle, _opt_log(order_log, has_log), no_parent)


def make_nested_scope(
    handle: Int,
    parent: UnsafePointer[Scope, MutAnyOrigin],
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
) raises -> Scope:
    """Nested scope: registers an open subscope of `parent`, so the parent
    cannot close until this scope closes (inner-before-outer)."""
    if not parent[].is_open():
        var err = ChildrenStillLive(
            "ChildrenStillLive: parent scope "
            + String(parent[].handle())
            + " already closed"
        )
        raise Error(err.message)
    var with_log = _opt_log(order_log, has_log)
    var with_parent = Optional[UnsafePointer[Scope, MutAnyOrigin]](parent)
    var s = Scope(handle, with_log, with_parent)
    return s^


# ---------------------------------------------------------------------------
# with_scope — the §13/§113 root ergonomic (issue #61)
# ---------------------------------------------------------------------------

def with_scope[
    F: def(mut Runtime, UnsafePointer[Scope, MutAnyOrigin], BytePtr) raises -> None
](mut rt: Runtime, body: F, ud: BytePtr) raises:
    """Create the ROOT scope, run `body(rt, scope, ud)`, and join it.

    Root-scope ergonomics (spec §13/§113): the b2 surface for the spec's
    `with Scope() as scope:` prototype — b2 has no context manager and no
    TLS, so with_scope threads the runtime explicitly and the body receives
    BOTH the mutable runtime and the root scope pointer.

    Failure policy (§8.2 default, issue #61 acceptance): when the body
    raises, with_scope RECORDS the primary error, cancellation-requests the
    registered siblings (request_cancel_all — erased prefix, best-effort),
    and then closes the scope; if the close REFUSES (live children cannot be
    driven to completion in-library on b2 — the embedding scheduler loop is
    the driver's job), the registry is dropped (abort escape hatch).  The
    PRIMARY error is ALWAYS re-raised untouched — teardown errors never mask
    it.  On normal return the scope is closed (validate-only; the body's own
    joins reaped the results) and ChildrenStillLive surfaces if the body
    leaked live children.
    """
    if rt.is_shutdown():
        raise Error("with_scope: runtime is shut down")
    var h = rt.scope_handle()
    if h == 0:
        h = rt.next_id()
        rt.set_scope_handle(h)
    var order_log = List[Int]()
    var s = make_scope(h, UnsafePointer[List[Int], MutAnyOrigin](to=order_log), False)
    var sp = UnsafePointer[Scope, MutAnyOrigin](to=s)
    var err = ""
    try:
        body(rt, sp, ud)
    except e:
        err = String(e)
        try:
            sp[].request_cancel_all()
        except Error:
            _ = 0  # best-effort: never mask the primary error
        try:
            sp[].close(rt)
        except Error:
            sp[].drop_children()
        raise Error(err)
    sp[].close(rt)


# ---------------------------------------------------------------------------
# Error predicates (decode the documented message prefixes)
# ---------------------------------------------------------------------------

def is_children_still_live(e: Error) -> Bool:
    """True when `e` is a ChildrenStillLive refusal (message begins with the
    stable "ChildrenStillLive:" prefix)."""
    return "ChildrenStillLive:" in String(e)


def is_scope_tag_mismatch(e: Error) -> Bool:
    """True when `e` is a ScopeTagMismatch (message begins with the stable
    "ScopeTagMismatch:" prefix)."""
    return "ScopeTagMismatch:" in String(e)