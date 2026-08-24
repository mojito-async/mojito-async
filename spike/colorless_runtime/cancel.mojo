# spike/colorless_runtime/cancel.mojo
#
# A0.8 (issue #17) — cooperative cancellation flag + checkpoints.
#
# Pure Mojo (no dylib, no fibers): a CancelFlag is the per-scope/task
# cancellation cell consumed by cooperative checkpoints.  The scheduler
# lane stamps CANCELLED transitions; this module owns the *flag* semantics:
#
#   - request() is idempotent
#   - is_requested() observes the local flag OR any ancestor flag
#     (child propagation: cancelling a scope cancels its children)
#   - checkpoint() raises CancellationError ONLY when requested; the raise
#     stamps an observation on the flag that raised
#   - reset() clears a request BEFORE any observation; after a checkpoint
#     has observed, reset() raises — spike policy "no reset after observe"
#     (a sibling may already have acted on the cancellation, so un-doing it
#     would break exactly-once winner accounting in the race harness).
#
# Mojo 1.0.0b2 dialect notes (same conventions as task.mojo):
#   - `def` only; NO static methods -> module-level factories below.
#   - b2 only supports raising the builtin `Error`; CancellationError is a
#     named message carrier so the raised condition stays nameable.
#   - Parent links are UnsafePointer[CancelFlag, MutAnyOrigin] into a live
#     parent (scope object or test local); presence tracked by `_has_parent`
#     because b2 pointers carry no null check.

# ---------------------------------------------------------------------------
# CancellationError (error model)
# ---------------------------------------------------------------------------

struct CancellationError:
    """Named error model for cooperative-cancellation checkpoints.

    b2 raises only the builtin ``Error``, so ``checkpoint()`` raises
    ``Error`` whose message is built by this type:

        "CancellationError: <detail>"

    Keep the identifier so the raised condition is nameable in code and in
    test diagnostics; the catch side detects plain ``Error`` raised by
    ``checkpoint()`` (the only raise it performs when requested).
    """

    var message: String

    def __init__(out self):
        self.message = "CancellationError"

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# CancelFlag
# ---------------------------------------------------------------------------

struct CancelFlag(ImplicitlyCopyable, ImplicitlyDeletable):
    """Cooperative cancellation cell per scope/task (spec A0-T9).

    Fields:
        _requested   set by request(); never auto-cleared (see reset()).
        _observed    stamped by checkpoint() at the moment it raises; the
                     observation belongs to THIS flag only — a child
                     observing through an ancestor does not mark the
                     ancestor observed.
        _parent      Optional link to the enclosing scope's flag for
                     propagation.  Requests propagate DOWNWARD only
                     (parent requested -> child observes); a child request
                     never cancels the parent.

    Spike reset policy: reset() clears a pre-observation request; once a
    checkpoint has observed (raised), reset() raises.  There is deliberately
    no post-observation reset path in the spike.
    """

    var _requested: Bool
    var _observed: Bool
    var _parent: Optional[UnsafePointer[CancelFlag, MutAnyOrigin]]

    def __init__(out self):
        self._requested = False
        self._observed = False
        self._parent = Optional[UnsafePointer[CancelFlag, MutAnyOrigin]]()

    # --- requests ----------------------------------------------------------

    def request(mut self):
        """Request cancellation. Idempotent by construction."""
        self._requested = True

    def is_requested(self) -> Bool:
        """Local request OR any ancestor request (downward propagation)."""
        if self._requested:
            return True
        if self._parent:
            return self._parent.value()[].is_requested()
        return False

    def observed(self) -> Bool:
        return self._observed

    # --- checkpoints -------------------------------------------------------

    def checkpoint(mut self) raises:
        """Cooperative cancellation point.

        Silent when not requested.  When cancellation is observable (here or
        through an ancestor) raises CancellationError-as-Error and stamps
        the observation on THIS flag before raising.
        """
        if self.is_requested():
            self._observed = True
            var err = CancellationError("checkpoint observed cancellation")
            raise Error(err.message)

    # --- reset -------------------------------------------------------------

    def reset(mut self) raises:
        """Clear a request that no checkpoint has observed yet.

        Raises when the flag already observed a cancellation (spike policy:
        no reset after observe).  Note reset() clears only the LOCAL
        request; an ancestor's request keeps propagating down.
        """
        if self._observed:
            raise Error(
                "CancellationError.reset: no reset after observe "
                "(spike policy)"
            )
        self._requested = False


# ---------------------------------------------------------------------------
# Module-level factories (b2: no static methods in structs)
# ---------------------------------------------------------------------------

def make_cancel_flag() -> CancelFlag:
    """Fresh root flag: not requested, not observed, no parent."""
    return CancelFlag()


def make_child_flag(parent: UnsafePointer[CancelFlag, MutAnyOrigin]) -> CancelFlag:
    """Child flag linked to `parent` (a live scope/test-local CancelFlag).

    Propagation is read-through at query time: the child holds the pointer,
    so later requests on the parent are observed without re-linking.
    """
    var child = CancelFlag()
    child._parent = Optional[UnsafePointer[CancelFlag, MutAnyOrigin]](parent)
    return child
