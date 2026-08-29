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
# A3.4 (issue #63) — GROUPED joins over a scope's HETEROGENEOUS children,
# built directly on the #61/#42 non-generic Scope (no with_scope body
# wrapper required):
#   * `join_all()` — a WAIT BARRIER (spec §8): raises ChildrenStillLive
#     while any registered child has not reached COMPLETED, silent once
#     every child has.  b2 cannot express a single call returning a
#     heterogeneous collection of typed results (different R per child),
#     so the grouped ergonomic is this barrier — each child still joins
#     with its OWN static type at its own `handle.join()` call site, side
#     by side for mixed R (§96 fan-out shape, §113 join-in-loop shape).
#   * `first_error(rt)` — the §8.2 default failure policy driven directly
#     off the erased registry: scan for the FIRST COMPLETED+FAILED child
#     (mark_failed's erased stamp), cancel siblings
#     (request_cancel_all), join-all siblings (close(rt), falling back to
#     drop_children() on refusal — the with_scope abort escape hatch), then
#     raise the primary error.  READ-ONLY over the failed child's result:
#     the owning JoinHandle's own join() still re-raises the identical
#     error afterward (never a double-join).
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

    def join_all(self) raises:
        """Grouped-join WAIT BARRIER (spec §8 `scope.join_all()`, issue
        #63): confirms every REGISTERED child has reached COMPLETED through
        the erased R-free prefix (the same read `close()`'s
        `_validate_exit` uses) — VALIDATE-ONLY: it never closes the scope,
        drops the registry, or consumes any child's result.

        b2 has no in-library scheduler to BLOCK a caller until completion
        (COOPERATIVE POLICY, spec §88): join_all() is a synchronous check,
        not a park — callers drive children to COMPLETED (execute() / the
        embedding scheduler loop) BEFORE calling it.

        This is the GROUPED ergonomic §96/§113 ask for: b2 cannot express a
        single call that returns a heterogeneous collection of typed
        results (`different R per child`), so the grouped join is this
        BARRIER — each child still joins with its OWN static type at its
        own `handle.join()` call site, side by side for mixed R (§96's
        `profile.join()`, `posts.join()`, `permissions.join()` shape).

        Raises ChildrenStillLive (documented prefix, decode via
        is_children_still_live) naming the FIRST unfinished child; nothing
        is consumed on refusal — a caller may finish driving and retry."""
        for i in range(len(self._children)):
            var pre = UnsafePointer[TCB_Prefix, MutAnyOrigin](
                unsafe_from_address=self._children[i].addr
            )
            if not pre[].is_completed():
                var err = ChildrenStillLive(
                    "ChildrenStillLive: join_all of scope "
                    + String(self._handle)
                    + " found unfinished child "
                    + String(self._children[i].id)
                )
                raise Error(err.message)

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

    def first_error(mut self, mut rt: Runtime) raises:
        """Grouped FIRST-ERROR join (spec §8.2 default failure policy,
        issue #63): implements the policy DIRECTLY on the Scope — no
        with_scope body wrapper required.  Scans registered children in
        REGISTRATION order for the FIRST one whose erased R-free prefix is
        COMPLETED and FAILED (is_failed() — #42's mark_failed erased
        stamp, set by execute()'s exception path independent of any
        per-handle join).  A silent no-op (no raise) when no registered
        child has failed yet — a non-failing poll a caller may retry after
        driving more children.

        On the FIRST failure, drives the §8.2 sequence exactly as
        with_scope's body-error path does:
          1. record the primary error (the failed child's PRESERVED
             message, read — never consumed — from the erased prefix);
          2. cancel sibling tasks (request_cancel_all, erased-prefix,
             best-effort; the found child is already COMPLETED so it is a
             no-op for it);
          3. join all siblings (attempt close(rt); on refusal — a live
             child the cancellation request could not drive to COMPLETED
             in-library, e.g. a NEW/RUNNABLE child or one whose own
             cooperative checkpoint has not yet observed the cancellation —
             fall back to drop_children(), the with_scope abort escape
             hatch, so the refusal never masks the primary error);
          4. raise the primary error.

        READ-ONLY over the failed child's result: first_error() never
        calls take_result()/join() on it, so the owning JoinHandle's OWN
        consume-once join() still re-raises the IDENTICAL error afterward
        — first_error() and a handle's join() are independent readers of
        the same preserved failure stamp, never a double-join (issue #63
        exit criterion: no child is joined twice)."""
        var found_addr = 0
        for i in range(len(self._children)):
            var pre = UnsafePointer[TCB_Prefix, MutAnyOrigin](
                unsafe_from_address=self._children[i].addr
            )
            if pre[].is_completed() and pre[].is_failed():
                found_addr = self._children[i].addr
                break
        if found_addr == 0:
            return
        var msg = UnsafePointer[TCB_Prefix, MutAnyOrigin](
            unsafe_from_address=found_addr
        )[].error()
        try:
            self.request_cancel_all()
        except Error:
            _ = 0  # best-effort: never mask the primary error
        try:
            self.close(rt)
        except Error:
            self.drop_children()
        raise Error(msg)

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
        join of a later lane."""
        self._validate_exit()
        self._close_bookkeeping()

    def close_typed[T: ScopeChild](mut self, mut rt: Runtime) raises:
        """TYPED join-integrated close for HOMOGENEOUS scopes (#42 pt 4):
        validate as close(), then tag-check EVERY registered child against
        T.TAG — ANY mismatch raises ScopeTagMismatch BEFORE anything is
        consumed (the #42 negative test) — then consume-once take_result of
        each settled child through the typed boundary.  The scope is the
        final joiner for children never individually reaped."""
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