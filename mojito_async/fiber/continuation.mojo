# mojito_async/fiber/continuation.mojo
#
# A1.2 (issue #50) — one-shot continuation over a bound fiber.
#
# This is the SEMANTIC layer between the wrapped Fiber (issue #49, owned by
# FiberBind / mojito_async/fiber/fiber.mojo) and a driver's TaskControlBlock:
# it formalizes the once-shot continuation interpretation (spec §14.2) as a
# small state machine on top of a representation-independent `FiberMotion`
# carrier, and binds the single `user` payload that a task body reaches (the
# ms_ctx_make side channel, spec §14.1).
#
# Design constraints (all honored):
#   - EXTERN-free, def-only, module-level factories (b2; #6971 keeps extern
#     call sites in the embedding *_aot drivers).  This module never calls
#     ms_* bounds — the register switch is provided by a FiberMotion
#     implementation supplied by the caller (an *_aot driver inlines the raw
#     ms_ctx / ms_stack choreography exactly like spike t2/t4; a JIT driver
#     uses a hosted Motion).
#   - Representation independence (§14.3): the state machine addresses the
#     carrier through the `FiberMotion` trait (has_resumed / is_suspended /
#     finished), NOT raw stack/context/layout fields.  A compiler-generated
#     continuation can later replace the fiber without touching this layer.
#   - No public accessor exposes NativeStack, NativeContext, stack size,
#     stack address, or context layout (§14.2/§14.3).  `carrier()` returns
#     the internal Motion VALUE so a host can drive/teardown the actual
#     fiber; it never leaks a raw pointer.
#
# Once-shot state machine (NEW / STARTED / SUSPENDED / RESUMED_ONCE /
# COMPLETED), seeded from the spec §14.1 `started: Bool`:
#
#     NEW --start()--> STARTED --resume()--> (running) --suspend()--> SUSPENDED
#                                                    |                       |
#                                              complete()             single wake
#                                                    |                       |
#                                                    v                       v
#                                               COMPLETED <----- RESUMED_ONCE+
#                                      (terminal)      --suspend()--> SUSPENDED
#
#   - start()     claims the once-shot entry (`started`); a second start of
#                 an already-started continuation raises loudly.
#   - resume()    enters / re-enters the carrier.  Legal from STARTED (the
#                 first RUNNING entry) or SUSPENDED (the single winning wake
#                 of one episode, spec §14.2).  A second resume while the
#                 continuation is already running (a second entry into an
#                 already-started continuation) raises LOUDLY — never a
#                 silent double-run.
#   - suspend()   parks a running continuation (STARTED / RESUMED_ONCE) into
#                 SUSPENDED, opening the window for exactly one wake.
#   - complete()  marks the continuation COMPLETED (terminal); every verb
#                 afterwards raises.
#
# The "single winning wake" invariant (spec §14.2) is enforced by the
# ledger: between a resume() and the next suspend(), another resume() is
# rejected, so at most one entry per suspension episode is delivered.  The
# carrier's own finished() tells the state machine when the body unwound, so
# a resume() that reaches completion lands in COMPLETED instead of
# RESUMED_ONCE.

from mojito_async.integration.sys import BytePtr


# ---------------------------------------------------------------------------
# FiberMotion — minimal representation-independent carrier (spec §14.3)
# ---------------------------------------------------------------------------

trait FiberMotion(ImplicitlyCopyable, ImplicitlyDeletable):
    """Carrier contract the continuation drives.

    Mirrors the Fiber surface the runtime needs, but stays raw-pointer free:
    an orchestration driver provides a concrete Motion that drives the real
    registers/stack via the vendored choreography; a hosted Motion exercises
    the state machine with no machine code."""

    def has_resumed(self) -> Bool: ...
    def is_suspended(self) -> Bool: ...
    # True once the fiber body ran to completion (returned / unwound).
    def finished(self) -> Bool: ...

    def resume(mut self) raises:
        """Switch into the carrier (first entry or re-entry)."""
        ...

    def suspend(mut self) raises:
        """Switch out of the carrier back to the driver."""
        ...


# ---------------------------------------------------------------------------
# FiberContinuation — the once-shot semantic state machine
# ---------------------------------------------------------------------------

struct FiberContinuation[F: FiberMotion](ImplicitlyCopyable, ImplicitlyDeletable):
    comptime NEW = Int(0)
    comptime STARTED = Int(1)
    comptime SUSPENDED = Int(2)
    comptime RESUMED_ONCE = Int(3)
    comptime COMPLETED = Int(4)

    var _fiber: Self.F
    var _user: BytePtr
    var _state: Int
    var _started: Bool  # spec §14.1 `started`

    def __init__(out self, fiber: Self.F, user: BytePtr):
        self._fiber = fiber
        self._user = user
        self._state = Self.NEW
        self._started = False

    # --- queries -------------------------------------------------------------

    def state(self) -> Int:
        return self._state

    def label(self) -> String:
        if self._state == Self.NEW:
            return "NEW"
        if self._state == Self.STARTED:
            return "STARTED"
        if self._state == Self.SUSPENDED:
            return "SUSPENDED"
        if self._state == Self.RESUMED_ONCE:
            return "RESUMED_ONCE"
        return "COMPLETED"

    def is_started(self) -> Bool:
        return self._started

    def is_suspended(self) -> Bool:
        return self._state == Self.SUSPENDED

    def is_completed(self) -> Bool:
        return self._state == Self.COMPLETED

    def user_payload(self) -> BytePtr:
        return self._user

    def carrier(self) -> Self.F:
        return self._fiber

    # --- once ledger ---------------------------------------------------------

    def start(mut self) raises:
        """Claim the once-shot continuation: NEW -> STARTED.  A second start
        of an already-started continuation raises loudly."""
        if self._started:
            raise Error(
                "FiberContinuation.start: continuation already started "
                "(once-shot); state=" + self.label()
            )
        if self._state != Self.NEW:
            raise Error(
                "FiberContinuation.start: cannot start a non-NEW continuation "
                "(state=" + self.label() + ")"
            )
        self._started = True
        self._state = Self.STARTED

    def _require_running(self, verb: String) raises:
        if self._state != Self.STARTED and self._state != Self.RESUMED_ONCE:
            raise Error(
                "FiberContinuation." + verb + ": continuation not running "
                "(state=" + self.label() + ")"
            )

    def suspend(mut self) raises:
        """Park a running continuation into SUSPENDED.  Legal only from a
        SUSPENDED-capable running state (STARTED or RESUMED_ONCE); the
        single winning wake comes next."""
        self._require_running("suspend")
        self._fiber.suspend()
        self._state = Self.SUSPENDED

    def resume(mut self) raises:
        """Enter / re-enter the continuation.  Legal from STARTED (the first
        RUNNING entry) or SUSPENDED (the single winning wake of this
        episode).  A second resume while the continuation is already running
        (a second entry into an already-started continuation) raises loudly
        — never a silent double-run.  When the body unwinds on this entry,
        the continuation lands in COMPLETED; otherwise RESUMED_ONCE."""
        if self._state == Self.NEW or self._state == Self.COMPLETED:
            raise Error(
                "FiberContinuation.resume: cannot enter a "
                + self.label() + " continuation"
            )
        if self._state == Self.RESUMED_ONCE:
            raise Error(
                "FiberContinuation.resume: second entry into an already "
                "running continuation (state=RESUMED_ONCE) rejected"
            )
        self._fiber.resume()
        if self._fiber.finished():
            self._state = Self.COMPLETED
        else:
            self._state = Self.RESUMED_ONCE

    def complete(mut self) raises:
        """Mark the continuation COMPLETED (terminal).  Legal while the body
        unwinds to COMPLETED; idempotent once terminal."""
        if self._state == Self.STARTED or self._state == Self.RESUMED_ONCE:
            self._state = Self.COMPLETED
        elif self._state == Self.COMPLETED:
            return
        else:
            raise Error(
                "FiberContinuation.complete: cannot complete a "
                + self.label() + " continuation"
            )


# ---------------------------------------------------------------------------
# Module-level factory (b2 has no static methods)
# ---------------------------------------------------------------------------

def make_continuation[F: FiberMotion](
    fiber: F, user: BytePtr
) -> FiberContinuation[F]:
    return FiberContinuation[F](fiber, user)


# ---------------------------------------------------------------------------
# Error predicate (decode the documented message prefixes)
# ---------------------------------------------------------------------------

def is_continuation_error(e: Error) -> Bool:
    """True when `e` is a continuation once/resume/suspend error."""
    var m = String(e)
    return (
        "FiberContinuation.start:" in m
        or "FiberContinuation.resume:" in m
        or "FiberContinuation.suspend:" in m
        or "FiberContinuation.complete:" in m
    )