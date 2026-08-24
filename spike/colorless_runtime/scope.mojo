# spike/colorless_runtime/scope.mojo
#
# A0.9 (issue #18) — pure-Mojo scope containment + nested scopes.
#
# Spec A0-T13/T14 at unit level (fibers NOT required here).  One `Scope`
# owns a registry of child tasks (TaskControlBlock handles); structured
# concurrency properties:
#
#   - Registry: register()/unregister() stamp and verify the scope handle
#     on each child TCB via set_scope_handle()/scope_handle(); the parent
#     task id round-trips through set_parent_id().
#   - Structured exit: close() requires a drained registry AND no open
#     subscopes, else raises ChildrenStillLive (modeled as Error, same
#     pattern as IllegalTransitionError in task.mojo).  close() drops all
#     child references; double-close raises.
#   - Structured order: nested scopes are independent Scope objects (no
#     global state — nesting works by construction).  A subscope registers
#     itself with its parent at creation; the parent refuses close() while
#     a subscope is open, so inner-before-outer ordering is enforced, not
#     merely conventional.  An optional shared order log records close
#     order for tests/diagnostics.
#
#   - Failure policy: cancellation flows through an INJECTED CancelHook
#     (trait slot).  request_cancel_all() invokes the hook once per live
#     child; the "first failed child cancels siblings" policy is driven by
#     callers (unregister the failed child, then request_cancel_all()).
#     Real cancel.mojo integration lands later — this module does not
#     import it.
#
# Join choice (documented per lane brief): close() does NOT join children;
# it REQUIRES a drained registry (every child already unregistered by its
# joiner).  Per-child join callbacks belong to the A0.6 JoinHandle lane;
# duplicating them here would fork responsibility.  drop_children() is the
# containment escape hatch for abort paths.
#
# Mojo 1.0.0b2 dialect notes (same conventions as task.mojo/queue.mojo):
#   - `def` only; NO static methods on structs -> module-level factories
#     make_scope / make_nested_scope.
#   - def parameters pass by immutable value -> Scope holds List fields and
#     is therefore NOT ImplicitlyCopyable; callers operate through
#     UnsafePointer[Scope, MutAnyOrigin] (see t13_scope.mojo).
#   - UnsafePointer is non-nullable -> absent optional pointers (close-order
#     log, parent link) are modeled with Optional[UnsafePointer].

from std.collections import List
from task import ResultValue, TaskControlBlock

# ---------------------------------------------------------------------------
# ChildrenStillLive (error model)
# ---------------------------------------------------------------------------

struct ChildrenStillLive:
    """Named error model for refusing scope exit with live children
    (spec A0-T13/T14).  b2 raises only builtin Error; like
    IllegalTransitionError in task.mojo, the named condition is carried in
    the message ("ChildrenStillLive: ...") so it stays nameable in code and
    test diagnostics."""

    var message: String

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# CancelHook — injected cancellation callback slot
# ---------------------------------------------------------------------------

trait CancelHook(ImplicitlyCopyable, Movable, ImplicitlyDeletable):
    """Injection point for the failure policy (spec A0-T14).

    The scope never cancels children directly; it calls the injected hook
    once per live child.  The real implementation (cancel.mojo integration)
    lands in a later lane; tests inject recording stubs."""

    def request_cancel(mut self, scope_handle: Int, child_handle: Int) raises:
        ...



# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------

struct Scope[T: ResultValue, H: CancelHook](Movable, ImplicitlyDeletable):
    """Structured-concurrency scope owning a child-task registry.

    Type parameters:
        T — result-slot type of the registered TaskControlBlock[T].
        H — injected CancelHook implementation.

    Child storage is two parallel Lists (handle id + TCB pointer), so the
    spike registry allocates only on register growth.  Handles start at 1
    and increment monotonically per scope (0 is reserved for "no child").

    Nesting: a subscope created via make_nested_scope registers an open-
    subscope count with its parent object; the parent's close() refuses
    while any subscope remains open.  No global state anywhere — nesting
    composes to arbitrary depth.
    """

    var _handle: Int
    var _open: Bool
    var _next_child_id: Int
    var _child_ids: List[Int]
    var _child_ptrs: List[UnsafePointer[TaskControlBlock[Self.T], MutAnyOrigin]]
    # Injected failure-policy callback.
    var _hook: Self.H
    # Close-order log shared with sibling scopes (records _handle on close);
    # empty Optional when the scope does not participate in order recording.
    var _order_log: Optional[UnsafePointer[List[Int], MutAnyOrigin]]
    # Parent-scope link for nesting (empty when root).
    var _parent: Optional[UnsafePointer[Self, MutAnyOrigin]]
    # Open direct subscopes (decremented by each subscope's close()).
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
            UnsafePointer[TaskControlBlock[Self.T], MutAnyOrigin]
        ]()
        self._hook = hook
        self._order_log = order_log
        self._parent = parent
        self._open_subscopes = 0
        # Every construction path maintains the parent's open-subscope
        # invariant (previously only make_nested_scope did): a directly
        # constructed parent-linked scope is counted too, so the parent's
        # close() refusal arithmetic can never go negative.
        if self._parent:
            self._parent.value()[]._open_subscopes += 1

    # --- queries -----------------------------------------------------------

    def handle(self) -> Int:
        return self._handle

    def is_open(self) -> Bool:
        return self._open

    def live_child_count(self) -> Int:
        return len(self._child_ids)

    def live_children(self) -> Int:
        return len(self._child_ids)

    def is_registered(self, child_handle: Int) -> Bool:
        for i in range(len(self._child_ids)):
            if self._child_ids[i] == child_handle:
                return True
        return False

    def open_subscopes(self) -> Int:
        return self._open_subscopes

    # --- registry ----------------------------------------------------------

    def register(
        mut self,
        child: UnsafePointer[TaskControlBlock[Self.T], MutAnyOrigin],
        parent_task_id: Int,
    ) raises -> Int:
        """Adopt a child task: assign its handle, stamp the scope handle
        and parent task id onto the TCB, and add it to the registry.
        Returns the assigned child handle."""
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
        """Release one child (joined, failed, or migrated).  Raises when
        the handle is unknown to this scope."""
        for i in range(len(self._child_ids)):
            if self._child_ids[i] == child_handle:
                # Round-trip check: the child must still name this scope.
                var tcb_ptr = self._child_ptrs[i]
                if tcb_ptr[].scope_handle() != self._handle:
                    var err = ChildrenStillLive(
                        "UnknownChild: child "
                        + String(child_handle)
                        + " does not name scope "
                        + String(self._handle)
                    )
                    raise Error(err.message)
                # Swap-remove: registry order carries no semantics (every
                # survivor gets cancel-requested), so removal is O(1).
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
        """Fire the injected cancel hook once per LIVE child, in registry
        order.  Sibling-cancellation policy: callers unregister the failed
        child first, then call this — the hook then reaches exactly the
        surviving siblings."""
        for i in range(len(self._child_ids)):
            self._hook.request_cancel(self._handle, self._child_ids[i])

    # --- containment -------------------------------------------------------

    def drop_children(mut self):
        """Containment escape hatch: drop every child reference without
        individual unregistration (abort paths / scope teardown)."""
        while len(self._child_ids) > 0:
            _ = self._child_ids.pop(len(self._child_ids) - 1)
            _ = self._child_ptrs.pop(len(self._child_ptrs) - 1)

    def close(mut self) raises:
        """Structured exit.  Refuses (ChildrenStillLive) while any child or
        direct subscope is still live; otherwise marks the scope closed,
        drops all child references (nothing outlives scope storage),
        records the close into the shared order log (inner-before-outer is
        therefore observable AND enforced), and detaches from the parent.
        Double-close raises."""
        if not self._open:
            var err = ChildrenStillLive(
                "DoubleClose: double close of scope "
                + String(self._handle)
            )
            raise Error(err.message)
        if len(self._child_ids) != 0 or self._open_subscopes != 0:
            var err = ChildrenStillLive(
                "ChildrenStillLive: scope "
                + String(self._handle)
                + " exit refused with "
                + String(len(self._child_ids))
                + " live children and "
                + String(self._open_subscopes)
                + " open subscopes"
            )
            raise Error(err.message)
        self._open = False
        self.drop_children()
        if self._order_log:
            self._order_log.value()[].append(self._handle)
        if self._parent:
            self._parent.value()[]._subscope_closed()

    def _subscope_closed(mut self):
        self._open_subscopes -= 1


# ---------------------------------------------------------------------------
# Module-level factories (b2: no static methods on structs)
# ---------------------------------------------------------------------------

def _opt_log(
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
) -> Optional[UnsafePointer[List[Int], MutAnyOrigin]]:
    if has_log:
        return Optional[UnsafePointer[List[Int], MutAnyOrigin]](order_log)
    return Optional[UnsafePointer[List[Int], MutAnyOrigin]]()


def make_scope[T: ResultValue, H: CancelHook](
    hook: H,
    handle: Int,
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
) -> Scope[T, H]:
    """Root scope: no parent link, zero open subscopes."""
    var no_parent = Optional[UnsafePointer[Scope[T, H], MutAnyOrigin]]()
    return Scope[T, H](hook, handle, _opt_log(order_log, has_log), no_parent)


def make_nested_scope[T: ResultValue, H: CancelHook](
    hook: H,
    handle: Int,
    parent: UnsafePointer[Scope[T, H], MutAnyOrigin],
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
) raises -> Scope[T, H]:
    """Nested scope: registers as an open subscope of `parent`, so the
    parent cannot close until this scope closes (inner-before-outer)."""
    if not parent[].is_open():
        var err = ChildrenStillLive(
            "ChildrenStillLive: parent scope "
            + String(parent[].handle())
            + " already closed"
        )
        raise Error(err.message)
    var with_log = _opt_log(order_log, has_log)
    var with_parent = Optional[UnsafePointer[Scope[T, H], MutAnyOrigin]](parent)
    var s = Scope[T, H](hook, handle, with_log, with_parent)
    return s^
