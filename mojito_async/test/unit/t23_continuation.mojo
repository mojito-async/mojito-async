# mojito_async/test/unit/t23_continuation.mojo
#
# A1.2 (issue #50) — one-shot continuation over a bound fiber.
#
# This driver exercises the SEMANTIC layer of mojito_async/fiber/
# continuation.mojo: the once-shot state machine (NEW / STARTED / SUSPENDED
# / RESUMED_ONCE / COMPLETED) over a representation-independent FiberMotion
# carrier (spec §14.3).  It is EXTERN-FREE (JIT-legal, #6971) so it drives a
# HOSTED motion (no machine code); a sibling *_aot driver proves the same
# state machine over the REAL ms_ctx_switch by inlining the vendored
# choreography (spike t2/t4 shape), but this lane's semantic drivers stay
# linkage-free so they green under the production harness regardless of the
# vendored dylib wiring (a FiberBind concern).
#
# Acceptance (issue #50):
#   - exactly one resume per suspension episode; a resume of a
#     started-but-not-parked continuation (double-resume) raises LOUDLY,
#     never a silent second entry;
#   - state transitions are exact: NEW -> STARTED -> SUSPENDED ->
#     RESUMED_ONCE (-> SUSPENDED for a re-entry edge) -> COMPLETED;
#   - once COMPLETED the continuation is TERMINAL: every verb raises
#     (complete() stays idempotent);
#   - the single `user` payload binding survives the whole episode
#     (ms_ctx_make side channel);
#   - `start()` is one-shot: a second start raises;
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
# semantic state machine run under JIT.  It counts resumes and, once
# `_finish_after` resumes have happened, reports finished() (a body that
# completed).  No raw stack/context anywhere.
# ---------------------------------------------------------------------------

struct HostedMotion(FiberMotion, ImplicitlyCopyable, ImplicitlyDeletable):
    var _resumes: Int
    var _finish_after: Int

    def __init__(out self, finish_after: Int = 0):
        self._resumes = 0
        self._finish_after = finish_after

    def has_resumed(self) -> Bool:
        return self._resumes > 0

    def is_suspended(self) -> Bool:
        return False  # a hosted carrier has no live register suspension

    def finished(self) -> Bool:
        return self._finish_after > 0 and self._resumes >= self._finish_after

    def start(mut self) raises:
        pass

    def resume(mut self) raises:
        self._resumes += 1

    def suspend(mut self) raises:
        pass


def make_hosted(finish_after: Int = 0) -> HostedMotion:
    return HostedMotion(finish_after)


def main() raises:
    var failures = List[String]()

    # --- construction: NEW, un-started -------------------------------------
    var motion = make_hosted()
    var user = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x5EED)
    var cont = make_continuation(motion, user)
    if cont.state() != FiberContinuation.NEW:
        failures.append("fresh continuation not NEW")
    if cont.is_started():
        failures.append("fresh continuation must be un-started")
    if cont.is_completed() or cont.is_suspended():
        failures.append("fresh continuation must not be completed/suspended")
    if Int(cont.user_payload()) != 0x5EED:
        failures.append("user payload binding lost")

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

    # --- first suspension: STARTED -> SUSPENDED ----------------------------
    cont.suspend()
    if cont.state() != FiberContinuation.SUSPENDED:
        failures.append("suspend from STARTED must move to SUSPENDED")
    if not cont.is_suspended():
        failures.append("is_suspended must be true after suspend")

    # --- single winning wake: SUSPENDED -> RESUMED_ONCE ---------------------
    cont.resume()
    if cont.state() != FiberContinuation.RESUMED_ONCE:
        failures.append("resume must move to RESUMED_ONCE")

    # A second resume while not suspended = double-resume: REJECT loudly
    var dbl_resume_rejected = False
    try:
        cont.resume()
    except e:
        dbl_resume_rejected = is_continuation_error(e)
    if not dbl_resume_rejected:
        failures.append("double resume of a started continuation was NOT rejected")

    # resume of a NEW continuation also rejected (cannot enter without start)
    var fresh2 = make_continuation(
        make_hosted(), UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
    )
    var resume_new_rejected = False
    try:
        fresh2.resume()
    except e:
        resume_new_rejected = is_continuation_error(e)
    if not resume_new_rejected:
        failures.append("resume of a NEW continuation was NOT rejected")

    # --- re-entry edge: SUSPENDED again, then COMPLETED ----------------------
    # The next resume drives the body to completion (finish_after == 2 total
    # resumes: the first resumed the body, the second lets it complete).
    var cont_finish = make_continuation(make_hosted(2), user)
    cont_finish.start()
    cont_finish.suspend()
    cont_finish.resume()
    if cont_finish.state() != FiberContinuation.RESUMED_ONCE:
        failures.append("first resume of finishing continuation must be RESUMED_ONCE")
    cont_finish.suspend()
    if cont_finish.state() != FiberContinuation.SUSPENDED:
        failures.append("second suspend must move back to SUSPENDED")
    cont_finish.resume()
    if cont_finish.state() != FiberContinuation.COMPLETED:
        failures.append(
            "resume that runs the body to completion must move to COMPLETED "
            "(state=" + cont_finish.label() + ")"
        )

    # --- terminal: every verb raises ---------------------------------------
    var resume_post = False
    try:
        cont_finish.resume()
    except e:
        resume_post = is_continuation_error(e)
    if not resume_post:
        failures.append("resume of a COMPLETED continuation was NOT rejected")

    var suspend_post = False
    try:
        cont_finish.suspend()
    except e:
        suspend_post = is_continuation_error(e)
    if not suspend_post:
        failures.append("suspend of a COMPLETED continuation was NOT rejected")

    var start_post = False
    try:
        cont_finish.start()
    except e:
        start_post = is_continuation_error(e)
    if not start_post:
        failures.append("start of a COMPLETED continuation was NOT rejected")

    # complete is idempotent on the terminal state (no raise, still COMPLETED)
    try:
        cont_finish.complete()
    except e:
        failures.append("complete on COMPLETED must be idempotent, not raise")
    if cont_finish.state() != FiberContinuation.COMPLETED:
        failures.append("complete must keep COMPLETED terminal")

    # --- single user payload survives everything ---------------------------
    if Int(cont_finish.user_payload()) != 0x5EED:
        failures.append("user payload lost")

    # --- complete() from a running-but-not-parked state is legal ------------
    var cont2 = make_continuation(
        make_hosted(), UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
    )
    cont2.start()
    try:
        cont2.complete()
    except e:
        failures.append("complete from STARTED must be legal, not raise")
    if cont2.state() != FiberContinuation.COMPLETED:
        failures.append("complete from STARTED must go COMPLETED")

    # --- suspend from a non-running state raises ----------------------------
    var suspend_new_rejected = False
    try:
        fresh2.suspend()
    except e:
        suspend_new_rejected = is_continuation_error(e)
    if not suspend_new_rejected:
        failures.append("suspend of a NEW continuation was NOT rejected")

    if len(failures) != 0:
        for m in failures:
            print("  - " + m)
        red("state machine violated " + String(len(failures)) + " invariant(s)")

    print("T23 continuation: PASS")