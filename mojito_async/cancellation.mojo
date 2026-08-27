# mojito_async/cancellation.mojo
#
# A1.1 runtime (issue #33) — cooperative cancellation flag + checkpoints.
#
# Productionized from spike/colorless_runtime/cancel.mojo (A0.8, issue #17):
# a CancelFlag is the per-scope/task cancellation cell consumed by
# cooperative checkpoints.  A1.1 adds the public `CancellationToken` surface
# (spec §7.1/§29): token.request() is the idempotent request; checkpoints
# observe a request DOWNWARD through parent links; `checkpoint()` raises
# CancellationError-as-Error only when requested.
#
#   - request() is idempotent
#   - is_cancellation_requested() observes the local flag OR any ancestor
#     flag (child propagation: cancelling a scope cancels its children)
#   - checkpoint() raises CancellationError ONLY when requested; the raise
#     stamps an observation on the raising flag (no reset after observe)
#   - parent links are UnsafePointer[CancelFlag, MutAnyOrigin]; presence
#     tracked via Optional (b2 pointers carry no null check).
#
# The full cancellation TREE (spec §29.1) is a later lane; A1.1 keeps the
# proven flag + checkpoint discipline out of the scheduler core.
#
# NOTE (b2): a module-level `def checkpoint(token)` free function is NOT
# provided because it collides with the method name `CancellationToken.
# checkpoint` inside this module; the cooperative checkpoint is exposed as a
# method (and via `token.checkpoint()` used by consumers).
#
# Mojo 1.0.0b2 dialect: `def` only; module-level factories; the only builtin
# raise is `Error` (named conditions carried in the message).

# ---------------------------------------------------------------------------
# CancellationError (error model)
# ---------------------------------------------------------------------------

struct CancellationError:
    """Named error model for cooperative-cancellation checkpoints."""

    var message: String

    def __init__(out self):
        self.message = "CancellationError"

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# CancelFlag — the cancellation cell
# ---------------------------------------------------------------------------

struct CancelFlag(ImplicitlyCopyable, ImplicitlyDeletable):
    """Cooperative cancellation cell per scope/task.  `_requested` set by
    request(); `_observed` stamped when a checkpoint raises; `_parent` link
    for downward propagation (a child request never cancels the parent).
    Spike reset policy: reset() clears a pre-observation request; reset after
    observe raises."""

    var _requested: Bool
    var _observed: Bool
    var _consumed_through: Bool
    var _parent: Optional[UnsafePointer[CancelFlag, MutAnyOrigin]]

    def __init__(out self):
        self._requested = False
        self._observed = False
        self._consumed_through = False
        self._parent = Optional[UnsafePointer[CancelFlag, MutAnyOrigin]]()

    # --- requests ----------------------------------------------------------

    def request(mut self):
        self._requested = True

    def is_requested(self) -> Bool:
        if self._requested:
            return True
        if self._parent:
            return self._parent.value()[].is_requested()
        return False

    def observed(self) -> Bool:
        return self._observed

    # --- checkpoints -------------------------------------------------------

    def checkpoint(mut self) raises:
        if self.is_requested():
            self._observed = True
            self._stamp_chain()
            raise Error("CancellationError: checkpoint observed cancellation")

    def _stamp_chain(mut self):
        self._consumed_through = True
        if self._parent:
            self._parent.value()[]._stamp_chain()

    # --- reset -------------------------------------------------------------

    def reset(mut self) raises:
        if self._observed or self._consumed_through:
            raise Error("CancellationError.reset: no reset after observe")
        self._requested = False

    # --- parent ------------------------------------------------------------

    def set_parent(mut self, parent: UnsafePointer[CancelFlag, MutAnyOrigin]):
        self._parent = Optional[UnsafePointer[CancelFlag, MutAnyOrigin]](parent)


# ---------------------------------------------------------------------------
# CancellationToken — the public surface wrapping a CancelFlag cell
# ---------------------------------------------------------------------------

struct CancellationToken(ImplicitlyCopyable, ImplicitlyDeletable):
    """The public cancellation token (spec §7.1/§29).  Wraps a caller-owned
    CancelFlag cell; request()/is_cancellation_requested()/checkpoint() are
    the cooperative surface.  Caller-allocates the flag cell (no hidden
    allocation on the checkpath)."""

    var _flag: UnsafePointer[CancelFlag, MutAnyOrigin]

    def __init__(out self, flag: UnsafePointer[CancelFlag, MutAnyOrigin]):
        self._flag = flag

    def request(mut self):
        """Request cancellation.  Idempotent."""
        self._flag[].request()

    def is_cancellation_requested(self) -> Bool:
        return self._flag[].is_requested()

    def is_requested(self) -> Bool:
        return self._flag[].is_requested()

    def observed(self) -> Bool:
        return self._flag[].observed()

    def checkpoint(mut self) raises:
        """Cooperative cancellation point: raises CancellationError-as-Error
        when cancellation is requested here or through an ancestor."""
        self._flag[].checkpoint()

    def flag(self) -> UnsafePointer[CancelFlag, MutAnyOrigin]:
        return self._flag


# ---------------------------------------------------------------------------
# Module-level factories
# ---------------------------------------------------------------------------

def make_cancel_flag() -> CancelFlag:
    """Fresh root flag: not requested, not observed, no parent."""
    return CancelFlag()


def make_child_flag(
    parent: UnsafePointer[CancelFlag, MutAnyOrigin],
) -> CancelFlag:
    """Child flag linked to `parent` (a live scope/test-local CancelFlag).

    Read-through propagation at query time: later parent requests are
    observed without re-linking."""
    var child = CancelFlag()
    child.set_parent(parent)
    return child