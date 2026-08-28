# mojito_async/test/unit/t23_continuation.mojo
#
# A1.2 (issue #50) — one-shot continuation: SEMANTIC-layer unit driver.
#
# This driver exercises the state machine of mojito_async/fiber/
# continuation.mojo (NEW / STARTED / SUSPENDED / RESUMED_ONCE / COMPLETED)
# over a representation-independent FiberMotion carrier (spec §14.3).  It is
# EXTERN-FREE (JIT-legal, #6971) so it drives an HONEST HOSTED motion (no
# machine code): the carrier LATCHES the park/unpark/completion signals the
# ledger reconciles on, so the state machine runs under the same carrier
# contract a register-switch carrier honors.  The sibling *_aot driver
# (t28_continuation_fiber_aot) proves the same state machine over the REAL
# ms_ctx_switch.
#
# Acceptance (issue #50 + A1 consensus fold T2):
#   - resume() sets the ledger BEFORE the entering switch (RESUMED_ONCE /
#     running): while the carrier is running the continuation reads as
#     RESUMED_ONCE, so an in-body suspend() during an episode never
#     self-rejects; after the carrier returns, the ledger reconciles against
#     the carrier's REAL signals: finished() -> COMPLETED, is_suspended() ->
#     SUSPENDED, else -> RESUMED_ONCE (no post-switch blind write);
#   - resume() of an already-running continuation (double-resume) raises
#     LOUDLY, never a silent second entry;
#   - suspend() raises unless the CARRIER reports the fiber actually running
#     (has_resumed && !is_suspended): a driver-side suspend of a parked or
#     never-entered continuation raises loudly, never a silent register
#     rewind;
#   - state transitions are exact: NEW -> STARTED -> (running) -> SUSPENDED
#     -> (running) -> SUSPENDED -> COMPLETED (the full re-park cycle);
#   - once COMPLETED the continuation is TERMINAL: every verb raises
#     (complete() stays idempotent), and the carrier agrees (finished());
#   - `start()` is one-shot: a second start raises;
#   - the single `user` payload binding survives the whole episode;
#   - no raw stack / context / layout accessor is reachable via the public
#     surface (the semantic module never touches one).
#
# Verdict: exit 0 + "PASS"; any failure prints "RED (...)" and raises
# (exit 1).
from mojito_async.integration.sys import BytePtr
from mojito_async.fiber import (
    FiberContinuation,
    FiberMotion,
    is_continuation_error,
    make_continuation,
)


def red(what: String) raises -> None:
    print("T23 continuation: RED (" + what + ")")
    raise Error(what)


# ---------------------------------------------------------------------------
# Hosted FiberMotion — a register-free carrier (spec §14.3) that lets the
# semantic state machine run under JIT.  It is HONEST about the three
# signals the ledger reconciles on: resume() counts entries and clears the
# park latch; suspend() parks; finished() turns true once the scripted body
# completed (`_finish_after` resumes) or complete() marked it.  No raw
# stack/context anywhere.
# ---------------------------------------------------------------------------

struct HostedMotion(FiberMotion, Movable, ImplicitlyCopyable, ImplicitlyDeletable):
    var _resumes: Int
    var _finish_after: Int
    var _parked: Bool
    var _completed: Bool

    def __init__(out self, finish_after: Int = 0):
        self._resumes = 0
        self._finish_after = finish_after
        self._parked = False
        self._completed = False

    def has_resumed(self) -> Bool:
        return self._resumes > 0

    def is_suspended(self) -> Bool:
        return self._parked

    def finished(self) -> Bool:
        return self._completed or (
            self._finish_after > 0 and self._resumes >= self._finish_after
        )

    def mark_completed(mut self):
        self._completed = True

    def resume(mut self) raises:
        self._resumes += 1
        self._parked = False  # re-entering the body

    def suspend(mut self) raises:
        self._parked = True  # the body yielded back to the driver


def make_hosted(finish_after: Int = 0) -> HostedMotion:
    return HostedMotion(finish_after)


def main() raises:
    var failures = List[String]()

    # --- construction: NEW, un-started, carrier not yet entered ------------
    var motion = make_hosted()
    var user = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x5EED)
    var cont = make_continuation(
        UnsafePointer[HostedMotion, MutAnyOrigin](to=motion), user
    )
    if cont.state() != FiberContinuation.NEW:
        failures.append("fresh continuation not NEW")
    if cont.is_started():
        failures.append("fresh continuation must be un-started")
    if cont.is_completed() or cont.is_suspended():
        failures.append("fresh continuation must not be completed/suspended")
    if Int(cont.user_payload()) != 0x5EED:
        failures.append("user payload binding lost")
    if cont.carrier_has_resumed():
        failures.append("carrier must not report resumed before the first entry")

    # --- one-shot start -----------------------------------------------------
    cont.start()
    if cont.state() != FiberContinuation.STARTED:
        failures.append("start must move to STARTED")
    if not cont.is_started():
        failures.append("start must set the once-shot/started flag")

    # second start must be REJECTED loudly (one-shot, no silent re-entry)
    var dbl_start_rejected = False
    try:
        cont.start()
    except e:
        dbl_start_rejected = is_continuation_error(e)
    if not dbl_start_rejected:
        failures.append("double start was NOT rejected")

    # --- fold T2: suspend BEFORE any entry raises (carrier not running) ----
    var pre_suspend_rejected = False
    try:
        cont.suspend()
    except e:
        pre_suspend_rejected = is_continuation_error(e)
    if not pre_suspend_rejected:
        failures.append(
            "driver-side suspend before the first entry was NOT rejected"
        )

    # --- fold T1: the ledger flips BEFORE the entering switch --------------
    # (The hosted carrier is RUNNING right after resume(); the reconcile has
    # no finish/park signal yet, so it leaves the ledger at RESUMED_ONCE —
    # the honest "running" state an in-body suspend() is legal from.)
    cont.resume()
    if cont.state() != FiberContinuation.RESUMED_ONCE:
        failures.append(
            "resume must leave the ledger RUNNING (RESUMED_ONCE) while the "
            "carrier is running"
        )
    if not cont.carrier_has_resumed():
        failures.append("carrier must report resumed after the entry")
    if cont.carrier_is_suspended():
        failures.append("carrier must not report parked right after the entry")

    # A second resume while running = double-resume: REJECT loudly
    var dbl_resume_rejected = False
    try:
        cont.resume()
    except e:
        dbl_resume_rejected = is_continuation_error(e)
    if not dbl_resume_rejected:
        failures.append(
            "double resume of a running continuation was NOT rejected"
        )

    # resume of a NEW continuation also rejected (cannot enter without start)
    var motion2 = make_hosted()
    var fresh2 = make_continuation(
        UnsafePointer[HostedMotion, MutAnyOrigin](to=motion2),
        UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
    )
    var resume_new_rejected = False
    try:
        fresh2.resume()
    except e:
        resume_new_rejected = is_continuation_error(e)
    if not resume_new_rejected:
        failures.append("resume of a NEW continuation was NOT rejected")

    # --- suspend: the in-body park (carrier latches parked; the ledger
    #     follows the carrier's is_suspended() signal) ----------------------
    cont.suspend()
    if cont.state() != FiberContinuation.SUSPENDED:
        failures.append("suspend must move to SUSPENDED")
    if not cont.is_suspended():
        failures.append("is_suspended must be true after suspend")
    if not cont.carrier_is_suspended():
        failures.append("carrier must report parked after suspend")

    # --- fold T2: suspend of the PARKED continuation raises (no rewind) ----
    var parked_suspend_rejected = False
    try:
        cont.suspend()
    except e:
        parked_suspend_rejected = is_continuation_error(e)
    if not parked_suspend_rejected:
        failures.append(
            "driver-side suspend of a parked continuation was NOT rejected"
        )

    # --- the single winning wake: back to RUNNING --------------------------
    cont.resume()
    if cont.state() != FiberContinuation.RESUMED_ONCE:
        failures.append(
            "the winning wake must leave the ledger RUNNING (RESUMED_ONCE)"
        )

    # --- full re-park cycle: start -> park -> wake -> re-park -> COMPLETED -
    # Same episode script the real-carrier driver (t28) executes: the mock
    # body completes on its third resume, so the final reconcile reads
    # finished() -> COMPLETED.
    var mc = make_hosted(3)
    var cont_cycle = make_continuation(
        UnsafePointer[HostedMotion, MutAnyOrigin](to=mc), user
    )
    cont_cycle.start()
    if cont_cycle.state() != FiberContinuation.STARTED:
        failures.append("cycle start must move to STARTED")
    cont_cycle.resume()   # episode 1 enters: REFRESHED_ONCE (running)
    if cont_cycle.state() != FiberContinuation.RESUMED_ONCE:
        failures.append("cycle episode 1 must be RUNNING (RESUMED_ONCE)")
    cont_cycle.suspend()  # body parks: SUSPENDED
    if cont_cycle.state() != FiberContinuation.SUSPENDED:
        failures.append("cycle park 1 must move to SUSPENDED")
    cont_cycle.resume()   # episode 2 enters: RESUMED_ONCE
    if cont_cycle.state() != FiberContinuation.RESUMED_ONCE:
        failures.append("cycle episode 2 must be RUNNING (RESUMED_ONCE)")
    cont_cycle.suspend()  # body re-parks: SUSPENDED
    if cont_cycle.state() != FiberContinuation.SUSPENDED:
        failures.append("cycle re-park must move back to SUSPENDED")
    cont_cycle.resume()   # episode 3: the body finishes -> COMPLETED
    if cont_cycle.state() != FiberContinuation.COMPLETED:
        failures.append(
            "the resume that runs the body to completion must move to "
            "COMPLETED (state=" + cont_cycle.label() + ")"
        )
    if not cont_cycle.carrier_finished():
        failures.append(
            "carrier must report finished() after the body completed"
        )
    if not cont_cycle.is_completed():
        failures.append("ledger must report COMPLETED at the end of the cycle")

    # --- terminal: every verb raises ---------------------------------------
    var resume_post = False
    try:
        cont_cycle.resume()
    except e:
        resume_post = is_continuation_error(e)
    if not resume_post:
        failures.append("resume of a COMPLETED continuation was NOT rejected")

    var suspend_post = False
    try:
        cont_cycle.suspend()
    except e:
        suspend_post = is_continuation_error(e)
    if not suspend_post:
        failures.append("suspend of a COMPLETED continuation was NOT rejected")

    var start_post = False
    try:
        cont_cycle.start()
    except e:
        start_post = is_continuation_error(e)
    if not start_post:
        failures.append("start of a COMPLETED continuation was NOT rejected")

    # complete() is idempotent on the terminal state (no raise, still
    # COMPLETED, carrier stays finished)
    try:
        cont_cycle.complete()
    except e:
        failures.append("complete on COMPLETED must be idempotent, not raise")
    if cont_cycle.state() != FiberContinuation.COMPLETED:
        failures.append("complete must keep COMPLETED terminal")
    if not cont_cycle.carrier_finished():
        failures.append("carrier finished() must survive the idempotent complete")

    # --- complete() from a running-but-not-parked state is legal and marks
    #     the carrier (the carrier-unwind seam) -----------------------------
    var m2 = make_hosted()
    var cont2 = make_continuation(
        UnsafePointer[HostedMotion, MutAnyOrigin](to=m2),
        UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
    )
    cont2.start()
    try:
        cont2.complete()
    except e:
        failures.append("complete from STARTED must be legal, not raise")
    if cont2.state() != FiberContinuation.COMPLETED:
        failures.append("complete from STARTED must go COMPLETED")
    if not cont2.carrier_finished():
        failures.append("complete() must mark the carrier finished")

    # --- suspend from a non-running state raises ----------------------------
    var suspend_new_rejected = False
    try:
        fresh2.suspend()
    except e:
        suspend_new_rejected = is_continuation_error(e)
    if not suspend_new_rejected:
        failures.append("suspend of a NEW continuation was NOT rejected")

    # --- the single user payload survives the whole episode -----------------
    if Int(cont_cycle.user_payload()) != 0x5EED:
        failures.append("user payload lost")

    if len(failures) != 0:
        for m in failures:
            print("  - " + m)
        red("state machine violated " + String(len(failures)) + " invariant(s)")

    print("T23 continuation: PASS")