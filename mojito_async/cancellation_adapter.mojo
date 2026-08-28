# mojito_async/cancellation_adapter.mojo
#
# A1.2-cancel (issue #41) — handle<->flag adapter for a scope's failure
# policy seam.
#
# A3.2 (#42 decision, issue #61) DECOUPLES this adapter from Scope: the
# non-generic Scope's request_cancel_all() now drives the ERASED TCB_Prefix
# directly (RUNNING -> CANCELLED, WAITING woken) and has no injected-hook
# seam.  This module stands ALONE as a handle<->flag adapter over
# cancellation.mojo's CancelFlag/CancellationToken trees — useful for a
# caller that wants (scope handle, child handle) pairs (e.g. the real task
# ids a Scope's register[T]() hands out) to resolve to CancelFlag cells and
# drive tree-propagated requests.  Full flag-tree wiring INTO Scope is lane
# #54 (decision pt 5).
#
# Two pieces, both CALLER-OWNED (b2: no module-level mutable globals, no
# statics):
#
#   CancelFlagRegistry — a caller-allocation-free mapping, in mirror-parallel
#   Lists, of handle -> scope flag and (scope, child) -> per-child flag.
#   Registering a CHILD also links that child flag under the scope flag via
#   make_child_flag (read-through: cancelling the scope is observed by the
#   child checkpoint).  The registry never allocates CancelFlag cells —
#   callers own the flags and pass their pointers in.
#
#   CancelFlagHook — request_cancel(scope_handle, child_handle) resolves the
#   child flag in the registry and calls request() on it (tree propagation
#   through the child flag's parent link).  Unknown scope / unknown child
#   REFUSE deterministically with documented message prefixes.
#
#   Registration is SYMMETRIC: register_child mutates the caller's child flag
#   (links it under the scope flag); unregister_child / unregister_scope
#   reverse that mutation by calling flag.clear_parent() on each unregistered
#   child, so an unregistered flag stops reading through to a retired scope.
#   The registry does not otherwise own the flag cells.
#
# Error messages use the stable prefixes:
#   "CancelAdapter: unknown scope <n>"
#   "CancelAdapter: duplicate scope <n>"
#   "CancelAdapter: unknown child (<n>, <m>)"
#   "CancelAdapter: duplicate child (<n>, <m>)"
# Prefixes are decoded by the `is_*` predicates below.
#
# Mojo 1.0.0b2 dialect: `def` only; module factories; List-backed registry
# (Scope-style parallel lists, allocates only on register growth); pure
# builtin `Error` raises.

from mojito_async.cancellation import CancelFlag


# ---------------------------------------------------------------------------
# CancelFlagRegistry — handle<->flag mapping (caller-owned)
# ---------------------------------------------------------------------------

struct CancelFlagRegistry(Movable, ImplicitlyDeletable):
    """Maps scope handle -> scope flag and (scope, child) -> child flag.

    Backed by parallel Lists (allocates only on register growth) for clean
    O(n) lookup consistent with Scope's own registry.  The registry does NOT
    own the CancelFlag cells: callers allocate flags and pass pointers in;
    unregister never frees a cell.
    """

    var _scope_ids: List[Int]
    var _scope_flags: List[UnsafePointer[CancelFlag, MutAnyOrigin]]
    var _child_scope_ids: List[Int]
    var _child_ids: List[Int]
    var _child_flags: List[UnsafePointer[CancelFlag, MutAnyOrigin]]

    def __init__(out self):
        self._scope_ids = List[Int]()
        self._scope_flags = List[UnsafePointer[CancelFlag, MutAnyOrigin]]()
        self._child_scope_ids = List[Int]()
        self._child_ids = List[Int]()
        self._child_flags = List[UnsafePointer[CancelFlag, MutAnyOrigin]]()

    # --- scope slot --------------------------------------------------------

    def has_scope(self, scope_handle: Int) -> Bool:
        for i in range(len(self._scope_ids)):
            if self._scope_ids[i] == scope_handle:
                return True
        return False

    def register_scope(
        mut self, scope_handle: Int, flag: UnsafePointer[CancelFlag, MutAnyOrigin]
    ) raises:
        """Bind a scope handle to its scope flag.  Refuses a duplicate."""
        if self.has_scope(scope_handle):
            raise Error(
                "CancelAdapter: duplicate scope " + String(scope_handle)
            )
        self._scope_ids.append(scope_handle)
        self._scope_flags.append(flag)

    def unregister_scope(mut self, scope_handle: Int) raises:
        """Drop a scope slot and every child mapped under it.  Refuses an
        unknown scope.  Never frees flag cells (caller-owned)."""
        var found = False
        for i in range(len(self._scope_ids)):
            if self._scope_ids[i] == scope_handle:
                found = True
                var last = len(self._scope_ids) - 1
                self._scope_ids[i] = self._scope_ids[last]
                self._scope_flags[i] = self._scope_flags[last]
                _ = self._scope_ids.pop(last)
                _ = self._scope_flags.pop(last)
                break
        if not found:
            raise Error(
                "CancelAdapter: unknown scope " + String(scope_handle)
            )
        # drop every child mapped under this scope (severing its parent link
        # so the child flag stops reading through to the scope — symmetry)
        var j = 0
        while j < len(self._child_scope_ids):
            if self._child_scope_ids[j] == scope_handle:
                self._child_flags[j][].clear_parent()
                var last = len(self._child_scope_ids) - 1
                self._child_scope_ids[j] = self._child_scope_ids[last]
                self._child_ids[j] = self._child_ids[last]
                self._child_flags[j] = self._child_flags[last]
                _ = self._child_scope_ids.pop(last)
                _ = self._child_ids.pop(last)
                _ = self._child_flags.pop(last)
            else:
                j += 1

    def scope_flag_ptr(
        self, scope_handle: Int
    ) raises -> UnsafePointer[CancelFlag, MutAnyOrigin]:
        for i in range(len(self._scope_ids)):
            if self._scope_ids[i] == scope_handle:
                return self._scope_flags[i]
        raise Error("CancelAdapter: unknown scope " + String(scope_handle))

    # --- child slot ---------------------------------------------------------

    def has_child(self, scope_handle: Int, child_handle: Int) -> Bool:
        for i in range(len(self._child_scope_ids)):
            if (
                self._child_scope_ids[i] == scope_handle
                and self._child_ids[i] == child_handle
            ):
                return True
        return False

    def register_child(
        mut self,
        scope_handle: Int,
        child_handle: Int,
        flag: UnsafePointer[CancelFlag, MutAnyOrigin],
    ) raises:
        """Bind a child handle to its child flag, LINKED under the scope flag
        (make_child_flag read-through: cancelling the scope cancels the child
        checkpoint).  Refuses an unknown scope or a duplicate child."""
        if not self.has_scope(scope_handle):
            raise Error(
                "CancelAdapter: unknown scope " + String(scope_handle)
            )
        if self.has_child(scope_handle, child_handle):
            raise Error(
                "CancelAdapter: duplicate child ("
                + String(scope_handle)
                + ", "
                + String(child_handle)
                + ")"
            )
        # link the (caller-supplied) child flag under the scope flag
        flag[].set_parent(self.scope_flag_ptr(scope_handle))
        self._child_scope_ids.append(scope_handle)
        self._child_ids.append(child_handle)
        self._child_flags.append(flag)

    def unregister_child(mut self, scope_handle: Int, child_handle: Int) raises:
        """Drop a child mapping (symmetry with register_child).

        register_child mutates the caller-supplied child flag by linking it
        under the scope flag; unregister_child reverses that mutation, calling
        flag.clear_parent() so the child flag stops reading through to the
        scope.  Refuses an unknown child.  Never frees the flag cell
        (caller-owned)."""
        for i in range(len(self._child_scope_ids)):
            if (
                self._child_scope_ids[i] == scope_handle
                and self._child_ids[i] == child_handle
            ):
                self._child_flags[i][].clear_parent()
                var last = len(self._child_scope_ids) - 1
                self._child_scope_ids[i] = self._child_scope_ids[last]
                self._child_ids[i] = self._child_ids[last]
                self._child_flags[i] = self._child_flags[last]
                _ = self._child_scope_ids.pop(last)
                _ = self._child_ids.pop(last)
                _ = self._child_flags.pop(last)
                return
        raise Error(
            "CancelAdapter: unknown child ("
            + String(scope_handle)
            + ", "
            + String(child_handle)
            + ")"
        )

    def child_flag_ptr(
        self, scope_handle: Int, child_handle: Int
    ) raises -> UnsafePointer[CancelFlag, MutAnyOrigin]:
        for i in range(len(self._child_scope_ids)):
            if (
                self._child_scope_ids[i] == scope_handle
                and self._child_ids[i] == child_handle
            ):
                return self._child_flags[i]
        raise Error(
            "CancelAdapter: unknown child ("
            + String(scope_handle)
            + ", "
            + String(child_handle)
            + ")"
        )


# ---------------------------------------------------------------------------
# CancelFlagHook — standalone handle-resolving cancel callback (#42:
# decoupled from Scope; see module header)
# ---------------------------------------------------------------------------

struct CancelFlagHook(Movable, ImplicitlyDeletable):
    """Resolves (scope, child) handles to the registered child flag and
    requests it (cancelling that child, and — through the child's parent
    link — propagating downward into the child's own descendants).  Refuses
    unknown handles."""

    var _registry: UnsafePointer[CancelFlagRegistry, MutAnyOrigin]

    def __init__(
        out self, registry: UnsafePointer[CancelFlagRegistry, MutAnyOrigin]
    ):
        self._registry = registry

    def request_cancel(mut self, scope_handle: Int, child_handle: Int) raises:
        self._registry[].child_flag_ptr(scope_handle, child_handle)[].request()


# ---------------------------------------------------------------------------
# Module-level factories (b2: no static methods)
# ---------------------------------------------------------------------------

def make_cancel_flag_registry() -> CancelFlagRegistry:
    """Fresh, empty, caller-owned handle<->flag registry."""
    return CancelFlagRegistry()


def make_cancel_flag_hook(
    registry: UnsafePointer[CancelFlagRegistry, MutAnyOrigin],
) -> CancelFlagHook:
    """A cancel-adapter bound to `registry`.  request_cancel(scope,
    child) fires the registered child flag."""
    return CancelFlagHook(registry)


# ---------------------------------------------------------------------------
# Error predicates (decode the documented message prefixes)
# ---------------------------------------------------------------------------

def is_unknown_scope(e: Error) -> Bool:
    return "CancelAdapter: unknown scope " in String(e)


def is_unknown_child(e: Error) -> Bool:
    return "CancelAdapter: unknown child (" in String(e)


def is_duplicate_scope(e: Error) -> Bool:
    return "CancelAdapter: duplicate scope " in String(e)


def is_duplicate_child(e: Error) -> Bool:
    return "CancelAdapter: duplicate child (" in String(e)
