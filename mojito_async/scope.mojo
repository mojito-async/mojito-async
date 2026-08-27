# mojito_async/scope.mojo
#
# A1.1 runtime (issue #33) — structured-concurrency scope with a
# JOIN-INTEGRATED close.
#
# Productionized from spike/colorless_runtime/scope.mojo (A0.9, issue #18).
# A1.1 extends the A0 "drained-registry-only" close into a *join-integrated*
# close (spec §112 Epic B / C7): close() now JOINS any registered child whose
# outcome is settled (consume-once take of its COMPLETED result) instead of
# requiring the registry to be pre-drained; it REFUSES (ChildrenStillLive,
# spec A0-T13/T14) only while a genuinely-live (not-yet-COMPLETED) child or an
# open direct subscope remains.  The spike's nested-scope ordering
# (inner-before-outer, parent refuses close while a subscope is open) and the
# `drop_children` abort escape hatch are kept verbatim.
#
# Failure policy flows through an INJECTED CancelHook (trait slot); real
# cancellation is in cancellation.mojo (A1.1 exposes the token; the tree
# propagation is later).
#
# Mojo 1.0.0b2 dialect: `def` only; no static methods -> module factories
# make_scope / make_nested_scope; Scope holds List fields so it is NOT
# ImplicitlyCopyable — callers operate through UnsafePointer; absent optional
# pointers are Optional (b2 pointers carry no null).
from std.collections import List
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock


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
# CancelHook — injected cancellation callback (failure policy seam)
# ---------------------------------------------------------------------------

trait CancelHook(ImplicitlyCopyable, Movable, ImplicitlyDeletable):
    """Injection point for the sibling cancellation policy (spec A0-T14)."""

    def request_cancel(mut self, scope_handle: Int, child_handle: Int) raises:
        ...


# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------

struct Scope[R: ResultValue, H: CancelHook](Movable, ImplicitlyDeletable):
    """Structured-concurrency scope owning a child-task registry.

    `_child_ids` / `_child_ptrs` parallel lists (registry allocates only on
    register growth).  `_open` gates register/close; `_parent` link and
    `_open_subscopes` enforce inner-before-outer.  `_order_log` records close
    order for tests/diagnostics.  Handles start at 1 and increment per scope
    (0 = "no child").
    """

    var _handle: Int
    var _open: Bool
    var _next_child_id: Int
    var _child_ids: List[Int]
    var _child_ptrs: List[UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin]]
    # Injected failure-policy callback.
    var _hook: Self.H
    # Close-order log shared with sibling scopes (records handle on close).
    var _order_log: Optional[UnsafePointer[List[Int], MutAnyOrigin]]
    # Parent-scope link (empty when root).
    var _parent: Optional[UnsafePointer[Self, MutAnyOrigin]]
    # Open direct subscopes.
    var _open_subscopes: Int

    def __init__(
        out self,
        hook: Self.H,
        handle: Int,
        order_log: Optional[UnsafePointer[List[Int], MutAnyOrigin]],
        parent: Optional[UnsafePointer[Self, MutAnyOrigin]],
    ):
        self._handle = handle
        self._open = True
        self._next_child_id = 1
        self._child_ids = List[Int]()
        self._child_ptrs = List[
            UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin]
        ]()
        self._hook = hook
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
        return len(self._child_ids)

    def is_registered(self, child_handle: Int) -> Bool:
        for i in range(len(self._child_ids)):
            if self._child_ids[i] == child_handle:
                return True
        return False

    def open_subscopes(self) -> Int:
        return self._open_subscopes

    def has_live_unfinished(self) -> Bool:
        """True when a registered child is not yet COMPLETED (genuinely live;
        the close() join would have nothing to consume)."""
        for i in range(len(self._child_ptrs)):
            if not self._child_ptrs[i][].is_completed():
                return True
        return False

    # --- registry ----------------------------------------------------------

    def register(
        mut self,
        child: UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin],
        parent_task_id: Int,
    ) raises -> Int:
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
        var cid = self._next_child_id
        self._next_child_id += 1
        child[].set_scope_handle(self._handle)
        child[].set_parent_id(parent_task_id)
        self._child_ids.append(cid)
        self._child_ptrs.append(child)
        return cid

    def unregister(mut self, child_handle: Int) raises:
        for i in range(len(self._child_ids)):
            if self._child_ids[i] == child_handle:
                var tcb_ptr = self._child_ptrs[i]
                if tcb_ptr[].scope_handle() != self._handle:
                    var err = ChildrenStillLive(
                        "UnknownChild: child "
                        + String(child_handle)
                        + " does not name scope "
                        + String(self._handle)
                    )
                    raise Error(err.message)
                var last = len(self._child_ids) - 1
                self._child_ids[i] = self._child_ids[last]
                self._child_ptrs[i] = self._child_ptrs[last]
                _ = self._child_ids.pop(last)
                _ = self._child_ptrs.pop(last)
                return
        var err = ChildrenStillLive(
            "UnknownChild: unregister of unknown child "
            + String(child_handle)
            + " from scope "
            + String(self._handle)
        )
        raise Error(err.message)

    # --- failure policy ----------------------------------------------------

    def request_cancel_all(mut self) raises:
        for i in range(len(self._child_ids)):
            self._hook.request_cancel(self._handle, self._child_ids[i])

    # --- containment -------------------------------------------------------

    def drop_children(mut self):
        """Containment escape hatch: drop every child reference without
        individual unregistration (abort paths / scope teardown)."""
        while len(self._child_ids) > 0:
            _ = self._child_ids.pop(len(self._child_ids) - 1)
            _ = self._child_ptrs.pop(len(self._child_ptrs) - 1)

    # --- close (join-integrated) -------------------------------------------

    def close(mut self, mut rt: Runtime) raises:
        """A1.1 JOIN-INTEGRATED close (spec): joins every registered child
        whose outcome is settled (consume-once take_result), then closes the
        scope.

        TWO-PHASE (validate-then-consume):
          Phase 1 - VALIDATE: every registered child must be COMPLETED and no
          open direct subscope may remain; on ANY violation raise
          ChildrenStillLive BEFORE consuming anything (nothing is joined, no
          result is taken, the scope stays open -- a caller can fix the
          violation and retry).
          Phase 2 - CONSUME: take_result on each settled child (the scope is
          the final joiner for children never individually reaped), then mark
          closed and drop all child references.

        Double-close raises.  Records the close into the shared order log
        (when participating).  `rt` is RESERVED for the engine-driven join of
        a later lane (when close() may drive pending children to completion);
        A1.1 validates instead of driving, so it is unused here.
        """
        # ---- Phase 1: validate (no consumption on failure) -----------------
        if not self._open:
            var err = ChildrenStillLive(
                "DoubleClose: double close of scope " + String(self._handle)
            )
            raise Error(err.message)
        for i in range(len(self._child_ptrs)):
            if not self._child_ptrs[i][].is_completed():
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
        # ---- Phase 2: consume settled results (join), then close -----------
        for i in range(len(self._child_ptrs)):
            var c = self._child_ptrs[i]
            if c[].has_result_pending():
                _ = c[].take_result()
        self._open = False
        self.drop_children()
        if self._order_log:
            self._order_log.value()[].append(self._handle)
        if self._parent:
            self._parent.value()[]._subscope_closed()

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


def make_scope[R: ResultValue, H: CancelHook](
    hook: H,
    handle: Int,
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
) -> Scope[R, H]:
    """Root scope: no parent link, zero open subscopes."""
    var no_parent = Optional[UnsafePointer[Scope[R, H], MutAnyOrigin]]()
    return Scope[R, H](hook, handle, _opt_log(order_log, has_log), no_parent)


def make_nested_scope[R: ResultValue, H: CancelHook](
    hook: H,
    handle: Int,
    parent: UnsafePointer[Scope[R, H], MutAnyOrigin],
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
) raises -> Scope[R, H]:
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
    var with_parent = Optional[UnsafePointer[Scope[R, H], MutAnyOrigin]](parent)
    var s = Scope[R, H](hook, handle, with_log, with_parent)
    return s^

# ---------------------------------------------------------------------------
# Error predicates (decode the documented message prefixes)
# ---------------------------------------------------------------------------

def is_children_still_live(e: Error) -> Bool:
    """True when `e` is a ChildrenStillLive refusal (message begins with the
    stable "ChildrenStillLive:" prefix)."""
    return "ChildrenStillLive:" in String(e)
