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
# A1 consensus fold T2 (PR #96) — the fold that REAL-CARRIER-grounded this
# layer.  The pre-fold continuation was disconnected from the real Fiber:
# resume() flipped the ledger AFTER the entering switch, so a real fiber
# could never replay a second episode (an in-body suspend() saw the ledger
# still at SUSPENDED and REJECTED itself), a driver-side suspend() on a
# parked real fiber silently rewound registers, and no real unwind ever made
# the state machine honest.  This fold:
#   1. resume() sets the ledger to RESUMED_ONCE (running) BEFORE the entering
#      switch; after the carrier returns it RECONCILES against the carrier's
#      REAL signals: finished() -> COMPLETED, is_suspended() -> SUSPENDED,
#      else -> RESUMED_ONCE.  No post-switch blind write.
#   2. suspend() raises unless the carrier reports the fiber ACTUALLY RUNNING
#      (has_resumed() && !is_suspended()): a driver-side suspend of a parked
#      or never-entered continuation raises loudly — never a silent
#      register rewind.
#   3. FiberMotion is implemented FOR the real Fiber (see fiber.mojo): the
#      trait gains the completion seam (mark_completed/finished) and Fiber
#      conforms, so the reconcile reads finished()/is_suspended() from the
#      moving carrier.
#   4. complete() is the carrier-unwind seam: the body calls it as its last
#      act before unwinding; it moves the ledger to COMPLETED AND marks the
#      carrier (finished()), so the driver-side resume() reconcile agrees.
#
# Once-shot state machine (NEW / STARTED / SUSPENDED / RESUMED_ONCE /
# COMPLETED), seeded from the spec §14.1 `started: Bool`:
#
#     NEW --start()--> STARTED --resume()--> (running) --suspend()--> SUSPENDED
#                                                 |                       |
#                                            complete()           single wake
#                                                 |                       |
#                                                 v                       v
#                                            COMPLETED <------ RESUMED_ONCE+
#                                     (terminal)     --suspend()--> SUSPENDED
#
#   - start()     claims the once-shot entry (`started`); a second start of
#                 an already-started continuation raises loudly.
#   - resume()    enters / re-enters the carrier.  Legal from STARTED (the
#                 first RUNNING entry) or SUSPENDED (the single winning wake
#                 of one episode, spec §14.2).  A second resume while the
#                 continuation is already running (state == RESUMED_ONCE, set
#                 BEFORE the entering switch) raises LOUDLY — never a silent
#                 double-run.  After the carrier returns, the ledger
#                 reconciles with the carrier's real finished()/is_suspended().
#   - suspend()   parks a running continuation (STARTED / RESUMED_ONCE) into
#                 SUSPENDED, opening the window for exactly one wake.  It is
#                 the BODY-side verb (in-fiber); it never writes the ledger
#                 for a register-switch carrier (that call returns only on
#                 the NEXT resume, in the wrong context) — the park is
#                 recorded by the driver-side resume() reconcile reading
#                 carrier.is_suspended().  A hosted/no-switch carrier returns
#                 from suspend() immediately parked, so the ledger follows
#                 is_suspended() right there.
#   - complete()  marks the continuation COMPLETED (terminal) AND marks the
#                 carrier (finished()); every verb afterwards raises.  Called
#                 by the body as its last act before unwinding.
#
# The "single winning wake" invariant (spec §14.2) is enforced by the
# ledger: between a resume() and the next suspension, another resume() is
# rejected, so at most one entry per suspension episode is delivered.  The
# carrier's own finished() tells the state machine when the body unwound, so
# a resume() that reaches completion lands in COMPLETED instead of
# RESUMED_ONCE.
#
# Unlimited representation independence (§14.3): the state machine addresses
# the carrier through the `FiberMotion` trait (has_resumed / is_suspended /
# finished / mark_completed / resume / suspend), NOT raw stack / context /
# layout fields.  No public accessor exposes NativeStack, NativeContext,
# stack size, stack address, or context layout.  A compiler-generated
# continuation can later replace the fiber without touching this layer.
#
# OWNERSHIP (fold T2 / #5, Mojo 1.0.0b2-safe): b2 passes ctor/function
# parameters by immutable reference, so a Movable value (Fiber) CANNOT be
# transferred INTO a struct-field constructor in this dialect — copying it
# would itself be the copy-safety bug (a copy aliases the same stack/block
# and double-frees).  The continuation therefore PINS the driver's own,
# already-MOVED carrier handle through a stable UnsafePointer: the driver
# owns exactly one Fiber (its `var f = make_fiber(...)`), the continuation
# is a NON-OWNING state-machine/viewer over the pinned carrier, and there is
# exactly one destroy path (the driver's handle).  No copy is ever taken
# (Fiber is Movable, not ImplicitlyCopyable — compile-enforced), so the
# aliasing the fold bans cannot be written.  The driver MUST keep the pinned
# carrier alive for the continuation's lifetime (scope-of-borrow contract,
# identical to motioasync Scope/SeamCell pointer ownership).
from mojito_async.integration.sys import BytePtr


# ---------------------------------------------------------------------------
# FiberMotion — minimal representation-independent carrier (spec §14.3)
# ---------------------------------------------------------------------------

trait FiberMotion(Movable, ImplicitlyDeletable):
    """Carrier contract the continuation drives.

    Mirrors the Fiber surface the runtime needs, but stays raw-pointer free.
    extended by Movable+ImplicitlyDeletable because a carrier is a
    single-owner execution resource (the real Fiber conforms; a hosted
    motion for unit tests conforms too)."""

    def has_resumed(self) -> Bool:
        """True once the carrier has taken at least one entry (first
        resume()), i.e. a fiber body is live."""
        ...

    def is_suspended(self) -> Bool:
        """True while the carrier is parked, waiting for the next resume."""
        ...

    # True once the fiber body ran to completion (returned / unwound /
    # mark_completed was called).
    def finished(self) -> Bool: ...

    # Driver/carrier completion seam: record that the body unwound, so
    # finished() becomes true.
    def mark_completed(mut self): ...

    def resume(mut self) raises:
        """Switch into the carrier (first entry or re-entry)."""
        ...

    def suspend(mut self) raises:
        """Switch out of the carrier back to the driver."""
        ...


# ---------------------------------------------------------------------------
# FiberContinuation — the once-shot semantic state machine
# ---------------------------------------------------------------------------

struct FiberContinuation[F: FiberMotion](Movable, ImplicitlyDeletable):
    comptime NEW = Int(0)
    comptime STARTED = Int(1)
    comptime SUSPENDED = Int(2)
    comptime RESUMED_ONCE = Int(3)
    comptime COMPLETED = Int(4)

    # Pinned (non-owning) carrier: the DRIVER owns the single Fiber handle and
    # must keep it alive for this continuation's lifetime.  See ownership note
    # in the module doc.
    var _fiber: UnsafePointer[Self.F, MutAnyOrigin]
    var _user: BytePtr
    var _state: Int
    var _started: Bool  # spec §14.1 `started`

    def __init__(
        out self, fiber: UnsafePointer[Self.F, MutAnyOrigin], user: BytePtr
    ):
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

    # Carrier-truth forwarders (the driver may reconcile the ledger against
    # what the carrier actually reports).
    def carrier_has_resumed(self) -> Bool:
        return self._fiber[].has_resumed()

    def carrier_is_suspended(self) -> Bool:
        return self._fiber[].is_suspended()

    def carrier_finished(self) -> Bool:
        return self._fiber[].finished()

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
        """Park a running continuation into SUSPENDED.

        BODY-side verb (in-fiber): legal only from a SUSPENDED-capable running
        state (STARTED or RESUMED_ONCE) AND while the carrier reports the
        fiber actually running (has_resumed && !is_suspended) — fold T2.  A
        driver-side suspend of a parked or never-entered continuation raises
        LOUDLY and never silently rewinds saved registers.

        For a register-switch carrier this call returns only on the NEXT
        resume (in the wrong context), so the ledger is NOT written here for
        a switching carrier — the driver-side resume() reconcile records the
        park by reading carrier.is_suspended().  For a hosted/no-switch
        carrier the call returns immediately, parked, and the ledger follows
        is_suspended() right here."""
        self._require_running("suspend")
        if not (
            self._fiber[].has_resumed() and not self._fiber[].is_suspended()
        ):
            raise Error(
                "FiberContinuation.suspend: carrier is not running "
                "(has_resumed=" + self._b(self._fiber[].has_resumed())
                + ", is_suspended=" + self._b(self._fiber[].is_suspended())
                + "); state=" + self.label()
            )
        self._fiber[].suspend()
        # Hosted/no-switch carrier returns here parked -> record it.  A
        # register-switch carrier returns here only on the NEXT resume, when
        # it is no longer suspended (the re-entering resume cleared the
        # latch) -> no write; that reconcile owns the transition.
        if self._fiber[].is_suspended():
            self._state = Self.SUSPENDED

    def resume(mut self) raises:
        """Enter / re-enter the continuation.

        Legal from STARTED (the first RUNNING entry) or SUSPENDED (the single
        winning wake of this episode).  Fold T1: the ledger is set to
        RESUMED_ONCE (running) BEFORE the entering switch, so an in-body
        suspend() during this episode sees the continuation running (never a
        self-reject).  A second resume while already running raises LOUDLY —
        never a silent double-run.  After the carrier returns the ledger
        RECONCILES against the carrier's real signals:
        finished() -> COMPLETED; is_suspended() -> SUSPENDED; else (a
        no-op/hosted carrier still running) -> RESUMED_ONCE."""
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
        # Fold T1: flip the ledger BEFORE the entering switch.
        self._state = Self.RESUMED_ONCE
        self._fiber[].resume()
        # Reconcile against the carrier's real signals (no post-switch blind
        # write): the carrier owns the truth about what happened in the
        # episode.
        if self._fiber[].finished():
            self._state = Self.COMPLETED
        elif self._fiber[].is_suspended():
            self._state = Self.SUSPENDED
        else:
            self._state = Self.RESUMED_ONCE

    def complete(mut self) raises:
        """Mark the continuation COMPLETED (terminal) and the carrier
        finished (the carrier-unwind seam).  Legal while the body unwinds to
        COMPLETED from a running (STARTED / RESUMED_ONCE) state; idempotent
        once terminal."""
        if self._state == Self.STARTED or self._state == Self.RESUMED_ONCE:
            self._state = Self.COMPLETED
            self._fiber[].mark_completed()
        elif self._state == Self.COMPLETED:
            return
        else:
            raise Error(
                "FiberContinuation.complete: cannot complete a "
                + self.label() + " continuation"
            )

    def _b(self, v: Bool) -> String:
        if v:
            return "true"
        return "false"


# ---------------------------------------------------------------------------
# Module-level factory (b2 has no static methods)
# ---------------------------------------------------------------------------

def make_continuation[F: FiberMotion](
    fiber: UnsafePointer[F, MutAnyOrigin], user: BytePtr
) -> FiberContinuation[F]:
    """Pin the driver's MOVED carrier handle under a non-owning continuation.

    `fiber` must be a stable pointer to the caller's single owning carrier
    value (e.g. `to=f` after `var f = make_fiber(...)`), kept alive for the
    continuation's lifetime.  No copy is taken (Fiber is non-copyable); the
    owner destroys it (fold T2/#5 — single destroy path, no aliasing)."""
    return FiberContinuation[F](fiber, user)


# ---------------------------------------------------------------------------
# Error predicate (decode the documented message prefixes)
# ---------------------------------------------------------------------------

def is_continuation_error(e: Error) -> Bool:
    """True when `e` is a continuation once/resume/suspend/complete error."""
    var m = String(e)
    return (
        "FiberContinuation.start:" in m
        or "FiberContinuation.resume:" in m
        or "FiberContinuation.suspend:" in m
        or "FiberContinuation.complete:" in m
    )